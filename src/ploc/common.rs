//! Atomic Update Common

// TODO: Alloc도 memento가 될 수도 있음

use std::{marker::PhantomData, sync::atomic::AtomicUsize};

use crossbeam_epoch::Guard;
use crossbeam_utils::CachePadded;

use crate::{
    pepoch::PShared,
    pmem::{
        ll::persist_obj,
        ralloc::{Collectable, GarbageCollection},
        PoolHandle,
    },
    Memento,
};

/// TODO: doc
pub trait Node: Sized {
    /// TODO: doc
    fn ack(&self);

    /// TODO: doc
    fn acked(&self) -> bool;

    /// TODO: doc
    fn owner(&self) -> &AtomicUsize;
}

/// TODO: doc
pub trait NodeUnOpt: Sized {
    /// TODO: doc
    fn ack_unopt(&self);

    /// TODO: doc
    fn acked_unopt(&self) -> bool;

    /// TODO: doc
    fn owner_unopt(&self) -> &AtomicUsize;
}

/// TODO: doc
pub trait DeallocNode<T, N: Node> {
    /// TODO: doc
    fn dealloc(&self, target: PShared<'_, N>, guard: &Guard, pool: &PoolHandle);
}

/// TODO(doc)
pub trait Invalid {
    /// TODO(doc)
    fn invalidate(&mut self);

    /// TODO(doc)
    fn is_invalid(&self) -> bool;
}

/// TODO(doc)
#[derive(Debug)]
pub struct Checkpoint<T: Invalid + Default + Clone + Collectable> {
    saved: CachePadded<T>,
    _marker: PhantomData<*const T>,
}

unsafe impl<T: Invalid + Default + Clone + Collectable + Send + Sync> Send for Checkpoint<T> {}
unsafe impl<T: Invalid + Default + Clone + Collectable + Send + Sync> Sync for Checkpoint<T> {}

impl<T: Invalid + Default + Clone + Collectable> Default for Checkpoint<T> {
    fn default() -> Self {
        let mut t = T::default();
        t.invalidate();

        Self {
            saved: CachePadded::from(t),
            _marker: Default::default(),
        }
    }
}

impl<T: Invalid + Default + Clone + Collectable> Collectable for Checkpoint<T> {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl<T> Memento for Checkpoint<T>
where
    T: 'static + Invalid + Default + Clone + Collectable,
{
    type Object<'o> = ();
    type Input<'o> = T;
    type Output<'o> = T;
    type Error<'o> = !;

    fn run<'o>(
        &'o mut self,
        (): Self::Object<'o>,
        chk: Self::Input<'o>,
        rec: bool,
        _: &'o Guard,
        _: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error<'o>> {
        if rec {
            if let Some(saved) = self.result() {
                return Ok(saved);
            }
        }

        // Normal run
        self.saved = CachePadded::from(chk.clone());
        persist_obj(&self.saved, true);
        Ok(chk)
    }

    fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {
        self.saved.invalidate();
        persist_obj(&self.saved, true);
    }
}

impl<T: Invalid + Default + Clone + Collectable> Checkpoint<T> {
    #[inline]
    fn result<'g>(&self) -> Option<T> {
        if self.saved.is_invalid() {
            None
        } else {
            Some((*self.saved).clone())
        }
    }
}

// /// Input으로 주어지는 `save_loc`은 `no_read()`로 세팅되어 있어야 함
// #[derive(Debug)]
// pub struct Load<T: Collectable> {
//     _marker: PhantomData<*const N>,
// }

// unsafe impl<T: Collectable + Send + Sync> Send for Checkpoint<T> {}
// unsafe impl<T: Collectable + Send + Sync> Sync for Checkpoint<T> {}

// impl<T: Collectable> Default for Checkpoint<T> {
//     fn default() -> Self {
//         Self {
//             _marker: Default::default(),
//         }
//     }
// }

// impl<T: Collectable> Collectable for Checkpoint<T> {
//     fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
// }

// impl<T> Memento for Checkpoint<T>
// where
//     N: 'static + Node,
// {
//     type Object<'o> = ();
//     type Input<'o> = (&'o PAtomic<T>, &'o PAtomic<T>);
//     type Output<'o> = PShared<'o, N>;
//     type Error<'o> = !;

//     fn run<'o>(
//         &'o mut self,
//         (): Self::Object<'o>,
//         (save_loc, point): Self::Input<'o>, // TODO: point는 object로, save_loc은 Load가 들고 있기
//         rec: bool,
//         guard: &'o Guard,
//         _: &'static PoolHandle,
//     ) -> Result<Self::Output<'o>, Self::Error<'o>> {
//         if rec {
//             if let Some(saved) = self.result(save_loc, guard) {
//                 return Ok(saved);
//             }
//         }

//         // Normal run
//         let p = point.load(Ordering::SeqCst, guard);
//         save_loc.store(p, Ordering::Relaxed);
//         persist_obj(save_loc, true);
//         Ok(p)
//     }

//     fn reset(&mut self, _: &Guard, _: &'static PoolHandle) {}
// }

// impl<T: Collectable> Checkpoint<T> {
//     #[inline]
//     fn result<'g>(&self, save_loc: &PAtomic<T>, guard: &'g Guard) -> Option<PShared<'g, N>> {
//         let saved = save_loc.load(Ordering::Relaxed, guard);

//         if saved == Self::no_read() {
//             None
//         } else {
//             Some(saved)
//         }
//     }

//     /// `Read`가 읽은 적이 없다는 걸 표시하기 위한 포인터
//     #[inline]
//     pub fn no_read<'g, T>() -> PShared<'g, T> {
//         const NO_READ: usize = usize::MAX - u32::MAX as usize;
//         unsafe { PShared::<T>::from_usize(NO_READ) }
//     }
// }
