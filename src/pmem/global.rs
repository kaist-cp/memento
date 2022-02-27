//! A global object that will hold information about the pool
//! (e.g. the starting address of the pool, metadata of the pool) while the pool is open.

use super::pool::PoolHandle;

static mut GLOBAL_POOL: Option<PoolHandle> = None;

/// Set the global pool handle
pub fn init(pool: PoolHandle) {
    unsafe {
        GLOBAL_POOL = Some(pool);
    }
}

/// Clear the global pool handle
pub fn clear() {
    unsafe {
        GLOBAL_POOL = None;
    }
}

/// Load the global pool handle
pub fn global_pool() -> Option<&'static mut PoolHandle> {
    unsafe { GLOBAL_POOL.as_mut() }
}
