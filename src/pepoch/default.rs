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
/// # let pool: PoolHandle<DummyRootOp> = unsafe { Pool::open("foo.pool", 8 * 1024 * 1024 *1024) }.unwrap();
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
/// lifetime: Guard < PoolHandle
///
/// ```compile_fail
/// # use compositional_persistent_object::plocation::pool::*;
/// # use compositional_persistent_object::pepoch::*;
/// # use compositional_persistent_object::utils::tests::DummyRootOp;
/// # let pool: PoolHandle<DummyRootOp> = unsafe { Pool::open("foo.pool", 8 * 1024 * 1024 * 1024) }.unwrap();
///
/// let guard = pin(&pool);
/// drop(pool);
///
/// // PoolHandle이 drop되었으니 guard도 사용불가
/// let guard = &guard; // compile error
/// ```
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
