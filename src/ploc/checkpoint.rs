//! Checkpoint

use std::sync::atomic::Ordering;

use crossbeam_utils::CachePadded;

use super::{Handle, Timestamp};
use crate::{
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle, CACHE_LINE_SHIFT,
    },
    Memento,
};

/// Checkpoint memento
#[derive(Debug)]
pub struct Checkpoint<T: Default + Clone + Collectable> {
    saved: [CachePadded<(T, Timestamp)>; 2],
}

unsafe impl<T: Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Default + Clone + Collectable> Memento for Checkpoint<T> {
    /// Clear
    #[inline]
    fn clear(&mut self) {
        self.saved = [
            CachePadded::new((T::default(), Timestamp::from(0))),
            CachePadded::new((T::default(), Timestamp::from(0))),
        ];
        persist_obj(&*self.saved[0], false);
        persist_obj(&*self.saved[1], false);
    }
}

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
        if chk.saved[latest].1 > pool.exec_info.chk_max_time {
            pool.exec_info.chk_max_time = chk.saved[latest].1;
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
    pub fn checkpoint<F: FnOnce() -> T>(&mut self, val_func: F, handle: &Handle) -> T {
        if handle.rec.load(Ordering::Relaxed) {
            if let Some(v) = self.peek(handle) {
                return v;
            }
            handle.rec.store(false, Ordering::Relaxed);
        }

        let new = val_func();
        let (stale, _) = self.stale_latest_idx();

        // Normal run
        let t = handle.pool.exec_info.exec_time();
        if std::mem::size_of::<(T, Timestamp)>() <= 1 << CACHE_LINE_SHIFT {
            self.saved[stale] = CachePadded::new((new.clone(), t));
            persist_obj(&*self.saved[stale], true);
        } else {
            self.saved[stale].0 = new.clone();
            persist_obj(&self.saved[stale].0, true);
            self.saved[stale].1 = t;
            persist_obj(&self.saved[stale].1, true);
        }

        handle.local_max_time.store(t);
        new
    }

    #[inline]
    fn is_valid(&self, idx: usize, handle: &Handle) -> bool {
        self.saved[idx].1 > handle.local_max_time.load()
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
    pub fn peek(&self, handle: &Handle) -> Option<T> {
        let (_, latest) = self.stale_latest_idx();

        if self.is_valid(latest, handle) {
            handle.local_max_time.store(self.saved[latest].1);
            Some((self.saved[latest].0).clone())
        } else {
            None
        }
    }
}

/// Test
pub mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::{
        pmem::{ralloc::Collectable, rdtscp, RootObj},
        test_utils::tests::*,
        Memento,
    };

    const NR_COUNT: usize = 100_000;

    struct Checkpoints {
        chks: [Checkpoint<usize>; NR_COUNT],
    }

    impl Memento for Checkpoints {
        fn clear(&mut self) {
            for i in 0..NR_COUNT {
                self.chks[i].clear();
            }
        }
    }

    impl Default for Checkpoints {
        fn default() -> Self {
            Self {
                chks: array_init::array_init(|_| Default::default()),
            }
        }
    }

    impl Collectable for Checkpoints {
        fn filter(m: &mut Self, tid: usize, gc: &mut GarbageCollection, pool: &mut PoolHandle) {
            for i in 0..NR_COUNT {
                Checkpoint::filter(&mut m.chks[i], tid, gc, pool);
            }
        }
    }

    impl RootObj<Checkpoints> for TestRootObj<DummyRootObj> {
        #[allow(unused_variables)]
        fn run(&self, chks: &mut Checkpoints, handle: &Handle) {
            #[cfg(not(feature = "pmcheck"))] // TODO: Remove
            let testee = unsafe { TESTER.as_ref().unwrap().testee(true, handle) };

            let mut items = (0..NR_COUNT).collect_vec();

            for seq in 0..NR_COUNT {
                let i = chks.chks[seq].checkpoint(|| rdtscp() as usize % items.len(), handle);

                let val = items.remove(i);
                #[cfg(not(feature = "pmcheck"))] // TODO: Remove
                testee.report(seq, TestValue::new(handle.tid, val))
            }
        }
    }

    #[test]
    fn checkpoints() {
        const FILE_NAME: &str = "checkpoint";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<DummyRootObj>, Checkpoints>(FILE_NAME, FILE_SIZE, 1, NR_COUNT);
    }

    /// Test checkpoint for psan
    #[cfg(feature = "pmcheck")]
    pub fn chks() {
        const FILE_NAME: &str = "checkpoint";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

        run_test::<TestRootObj<DummyRootObj>, Checkpoints>(FILE_NAME, FILE_SIZE, 1, NR_COUNT);
    }
}
