//! Default function of pepoch module

use crate::pepoch::guard::Guard;
use crate::plocation::pool::PoolHandle;
use std::marker::PhantomData;

/// TODO: doc, impl
///
/// # Example
///
/// lifetime: &T < Guard
///
/// ```compile_fail
/// # use compositional_persistent_object::plocation::pool::*;
/// # use compositional_persistent_object::pepoch::*;
/// # use compositional_persistent_object::utils::tests::DummyRootOp;
/// # use std::sync::atomic::Ordering;
/// # let pool: &PoolHandle = unsafe { Pool::open::<DummyRootOp>("foo.pool", 8 * 1024 * 1024 *1024) }.unwrap();
///
/// // Guard 및 Shared 포인터 얻기
/// let guard = pin(&pool);
/// let shared = PAtomic::new(1234, &pool).load(Ordering::SeqCst, &guard);
///
/// // Reference 얻기
/// let val_ref = unsafe { shared.deref(&pool) };
/// drop(guard);
///
/// // Guard가 drop되었으니 참조 불가
/// let val = *val_ref; // compile error
/// ```
///
/// PoolHandle이 global하기 때문에 lifetime Guard < PoolHandle은 강제 못함.
pub fn pin(_: &PoolHandle) -> Guard<'_> {
    Guard {
        _marker: PhantomData,
    }
}

/// TODO: doc, impl
///
/// # Safety
///
/// TODO
pub unsafe fn unprotected(_: &PoolHandle) -> &Guard<'_> {
    static UNPROTECTED: Guard<'_> = Guard {
        _marker: PhantomData,
    };
    &UNPROTECTED
}
