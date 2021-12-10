//! Persistent Spin Lock

use std::sync::atomic::AtomicUsize;

use crate::pmem::{Collectable, GarbageCollection, PoolHandle};

/// TODO(doc)
// #[derive(Debug)]
// pub struct Lock<T: Clone> {
//     delete: Delete<Queue<T>, Node<MaybeUninit<T>>, Self>,
// }

// impl<T: Clone> Default for Lock<T> {
//     fn default() -> Self {
//         Self {
//             delete: Default::default(),
//         }
//     }
// }

// unsafe impl<T: Clone + Send + Sync> Send for Lock<T> {}

// impl<T: Clone> Collectable for Lock<T> {
//     fn filter(try_deq: &mut Self, gc: &mut GarbageCollection, pool: &PoolHandle) {
//         Delete::filter(&mut try_deq.delete, gc, pool);
//     }
// }

// impl<T: 'static + Clone> Memento for Lock<T> {
//     type Object<'o> = &'o Queue<T>;
//     type Input<'o> = ();
//     type Output<'o> = Option<T>;
//     type Error<'o> = TryFail;

//     fn run<'o>(
//         &mut self,
//         queue: Self::Object<'o>,
//         (): Self::Input<'o>,
//         rec: bool,
//         guard: &'o Guard,
//         pool: &'static PoolHandle,
//     ) -> Result<Self::Output<'o>, Self::Error<'o>> {
//         self.delete
//             .run(&queue.head, (PShared::null(), queue), rec, guard, pool)
//             .map(|ret| {
//                 ret.map(|popped| {
//                     let next = unsafe { popped.deref(pool) }
//                         .next
//                         .load(Ordering::SeqCst, guard); // TODO(opt): next를 다시 load해서 성능 저하
//                     let next_ref = unsafe { next.deref(pool) };
//                     unsafe { guard.defer_pdestroy(popped) };
//                     unsafe { (*next_ref.data.as_ptr()).clone() }
//                 })
//             })
//             .map_err(|_| TryFail)
//     }

//     fn reset(&mut self, guard: &Guard, pool: &'static PoolHandle) {
//         self.delete.reset(guard, pool);
//     }
// }

/// TODO(doc)
#[derive(Debug)]
pub struct SpinLock {
    inner: AtomicUsize,
}

impl Default for SpinLock {
    fn default() -> Self {
        Self {
            inner: AtomicUsize::new(SpinLock::RELEASED),
        }
    }
}

impl Collectable for SpinLock {
    fn filter(_: &mut Self, _: &mut GarbageCollection, _: &PoolHandle) {}
}

impl SpinLock {
    const RELEASED: usize = 0;
}
