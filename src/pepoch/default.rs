//! Default function of pepoch module

use crate::pepoch::guard::Guard;
use crate::persistent::POp;
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
/// # use compositional_persistent_object::utils::tests::TestRootOp;
/// # use std::sync::atomic::Ordering;
/// # let pool: PoolHandle<TestRootOp> = unsafe { Pool::open("foo.pool") }.unwrap();
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
/// # use compositional_persistent_object::utils::tests::TestRootOp;
/// # let pool: PoolHandle<TestRootOp> = unsafe { Pool::open("foo.pool") }.unwrap();
///
/// let guard = pin(&pool);
/// drop(pool);
///
/// // PoolHandle이 drop되었으니 guard도 사용불가
/// let guard = &guard; // compile error
/// ```
pub fn pin<O: POp>(_: &PoolHandle<O>) -> Guard<'_> {
    Guard {
        _marker: PhantomData,
    }
}

/// TODO: doc, impl
///
/// # Safety
///
/// TODO
pub unsafe fn unprotected<O: POp>(_: &PoolHandle<O>) -> &Guard<'_> {
    static UNPROTECTED: Guard<'_> = Guard {
        _marker: PhantomData,
    };
    &UNPROTECTED
}
