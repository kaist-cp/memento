//! Checkpoint

use crossbeam_utils::CachePadded;

use super::Timestamp;
use crate::pmem::{
    ll::persist_obj,
    ralloc::{Collectable, GarbageCollection},
    PoolHandle, CACHE_LINE_SHIFT,
};

/// Checkpoint memento
#[derive(Debug)]
pub struct Checkpoint<T: Default + Clone + Collectable> {
    saved: [CachePadded<(T, Timestamp)>; 2],
}

unsafe impl<T: Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        Self {
            saved: [
                CachePadded::new((T::default(), Timestamp::from(0))),
                CachePadded::new((T::default(), Timestamp::from(0))),
            ],
        }
    }
}

impl<T: Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(chk: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
        let (_, latest) = chk.stale_latest_idx();

        // Record the one with max timestamp among checkpoints
        if chk.saved[latest].1 > pool.exec_info.chk_info {
            pool.exec_info.chk_info = chk.saved[latest].1;
        }

        if chk.saved[latest].1 > Timestamp::from(0) {
            T::filter(&mut chk.saved[latest].0, tid, gc, pool);
        }
    }
}

/// Error of checkpoint containing existing/new value
#[derive(Debug)]
pub struct CheckpointError<T> {
    /// Existing value
    pub current: T,

    /// New value
    pub new: T,
}

impl<T> Checkpoint<T>
where
    T: Default + Clone + Collectable,
{
    /// Checkpoint
    pub fn checkpoint<const REC: bool, F: FnOnce() -> T>(
        &mut self,
        val_func: F,
        tid: usize,
        pool: &PoolHandle,
    ) -> T {
        if REC {
            if let Some(v) = self.peek(tid, pool) {
                return v;
            }
        }

        let new = val_func();
        let (stale, _) = self.stale_latest_idx();

        // Normal run
        let t = pool.exec_info.exec_time();
        if std::mem::size_of::<(T, Timestamp)>() <= 1 << CACHE_LINE_SHIFT {
            self.saved[stale] = CachePadded::new((new.clone(), t));
            persist_obj(&*self.saved[stale], true);
        } else {
            self.saved[stale].0 = new.clone();
            persist_obj(&self.saved[stale].0, true);
            self.saved[stale].1 = t;
            persist_obj(&self.saved[stale].1, true);
        }

        pool.exec_info.local_max_time.store(tid, t);
        new
    }

    #[inline]
    fn is_valid(&self, idx: usize, tid: usize, pool: &PoolHandle) -> bool {
        self.saved[idx].1 > pool.exec_info.local_max_time.load(tid)
    }

    #[inline]
    fn stale_latest_idx(&self) -> (usize, usize) {
        if self.saved[0].1 < self.saved[1].1 {
            (0, 1)
        } else {
            (1, 0)
        }
    }

    /// Peek
    pub fn peek(&self, tid: usize, pool: &PoolHandle) -> Option<T> {
        let (_, latest) = self.stale_latest_idx();

        if self.is_valid(latest, tid, pool) {
            pool.exec_info
                .local_max_time
                .store(tid, self.saved[latest].1);
            Some((self.saved[latest].0).clone())
        } else {
            None
        }
    }

    /// Clear
    #[inline]
    pub fn clear(&mut self) {
        self.saved = [
            CachePadded::new((T::default(), Timestamp::from(0))),
            CachePadded::new((T::default(), Timestamp::from(0))),
        ];
        persist_obj(&*self.saved[0], false);
        persist_obj(&*self.saved[1], false);
    }
}
