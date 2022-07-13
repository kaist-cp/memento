//! Persistent Memory Pool
//!
//! A memory "pool" that maps files to virtual addresses as a persistent heap and manages those memory areas.

use std::alloc::Layout;
use std::ffi::{c_void, CString};
use std::io::Error;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fs, mem};

use crate::ploc::{CASCheckpointArr, ExecInfo, NR_MAX_THREADS};
use crate::pmem::global::global_pool;
use crate::pmem::ll::persist_obj;
use crate::pmem::ptr::PPtr;
use crate::pmem::{global, ralloc::*};
use crate::*;
use crossbeam_epoch::{self as epoch};
use crossbeam_utils::CachePadded;
use std::thread;

#[cfg(feature = "simulate_tcrash")]
use crate::test_utils::tests::{disable_killed, UNIX_TIDS};

// indicating at which root of Ralloc the metadata, root obj, and root mementos are located.
enum RootIdx {
    RootObj,       // root obj
    CASCheckpoint, // cas general checkpoint
    NrMemento,     // number of root mementos
    MementoStart,  // start index of root memento(s)
}

lazy_static::lazy_static! {
    static ref BARRIER_WAIT: [AtomicBool; NR_MAX_THREADS+1] =
        array_init::array_init(|_| AtomicBool::new(false));
}

/// PoolHandle
///
/// # Example
///
/// ```no_run
/// # use memento::pmem::pool::*;
/// # use memento::*;
/// # use memento::test_utils::tests::{DummyRootObj as MyRootObj, DummyRootMemento as MyRootMemento};
/// # use crossbeam_epoch::{self as epoch};
/// // create new pool and get pool handle
/// let pool_handle = Pool::create::<MyRootObj, MyRootMemento>("foo.pool", 8 * 1024 * 1024 * 1024, 1).unwrap();
///
/// // run root memento(s)
/// pool_handle.execute::<MyRootObj, MyRootMemento>();
/// ```
#[derive(Debug)]
pub struct PoolHandle {
    start: usize,

    len: usize,

    /// Detectable execution information per thread
    pub(crate) exec_info: ExecInfo,
}

impl PoolHandle {
    /// start address of pool
    #[inline]
    pub fn start(&self) -> usize {
        self.start
    }

    /// end address of pool
    #[inline]
    pub fn end(&self) -> usize {
        self.start() + self.len
    }

    /// Start main program of pool by running root memento(s)
    ///
    /// O: root obj
    /// M: root memento(s)
    #[allow(box_pointers)]
    pub fn execute<O, M>(&'static self)
    where
        O: RootObj<M> + Send + Sync + 'static,
        M: Collectable + Default + Send + Sync,
    {
        // get root obj
        let root_obj = unsafe {
            (RP_get_root_c(RootIdx::RootObj as u64) as *const O)
                .as_ref()
                .unwrap()
        };

        // get number of root memento(s)
        let nr_memento = unsafe { *(RP_get_root_c(RootIdx::NrMemento as u64) as *mut usize) };

        // repeat until `tid` thread succeeds the `tid`th memento
        let mut handles = Vec::new();
        for tid in 1..=nr_memento {
            // get `tid`th root mement
            let m_addr =
                unsafe { RP_get_root_c(RootIdx::MementoStart as u64 + tid as u64) as usize };

            let th = thread::spawn(move || {
                let h = thread::spawn(move || {
                    loop {
                        self.exec_info.local_max_time[tid].store(0, Ordering::Relaxed);

                        struct Args<O: 'static> {
                            m_addr: usize,
                            tid: usize,
                            nr_memento: usize,
                            pool_handle: &'static PoolHandle,
                            root_obj: &'static O,
                        }

                        extern "C" fn thread_start<O, M>(arg: *mut c_void) -> *mut c_void
                        where
                            O: RootObj<M> + Send + Sync + 'static,
                            M: Collectable + Default + Send + Sync,
                        {
                            // Decompose arguments
                            let args = unsafe { (arg as *mut Args<O>).as_mut() }.unwrap();
                            let (m_addr, tid, nr_memento, pool_handle, root_obj) = (
                                args.m_addr,
                                args.tid,
                                args.nr_memento,
                                args.pool_handle,
                                args.root_obj,
                            );
                            let root_mmt = unsafe { (m_addr as *mut M).as_mut().unwrap() };

                            // Old Guard
                            let guard = unsafe { epoch::old_guard(tid) };

                            // Barrier
                            pool_handle.barrier_wait(tid, nr_memento);

                            // Run memento
                            #[cfg(feature = "simulate_tcrash")]
                            {
                                // check if already finished.
                                if UNIX_TIDS[tid].load(Ordering::SeqCst) == -1 {
                                    return ptr::null_mut();
                                }
                            }

                            let _ = root_obj.run(root_mmt, tid, &guard, pool_handle);

                            #[cfg(feature = "simulate_tcrash")]
                            disable_killed(tid);

                            ptr::null_mut()
                        }

                        let mut native: libc::pthread_t = unsafe { mem::zeroed() };
                        let attr: libc::pthread_attr_t = unsafe { mem::zeroed() };
                        let mut args = Args {
                            m_addr,
                            tid,
                            nr_memento,
                            pool_handle: self,
                            root_obj,
                        };

                        // Run memento
                        unsafe {
                            let _err = libc::pthread_create(
                                &mut native,
                                &attr,
                                thread_start::<O, M>,
                                &mut args as *const _ as *mut _,
                            );
                        }

                        // Join
                        // - Exit on success, re-run memento on failure
                        // - The guard used in case of failure is also not cleaned up. A guard that loses its owner should be used well by the thread created in the next iteration.
                        let mut status = ptr::null_mut();
                        let _ = unsafe { libc::pthread_join(native, &mut status) };
                        match status as *const _ as usize {
                            0 => break,
                            _ => {
                                thread::sleep(std::time::Duration::from_secs(1));
                                println!("PANIC: Root memento No.{} re-executed.", tid);

                                #[cfg(feature = "simulate_tcrash")]
                                if tid == 1 {
                                    println!("Stop testing becuase Thread 1 panicked. Maybe there is a assertion bug.");
                                    unsafe {
                                        libc::exit(10);
                                    }
                                }
                            }
                        }
                    }
                });
                let _ = h.join();
            });
            handles.push(th);
        }

        while !handles.is_empty() {
            let _ = handles.pop().unwrap().join();
        }
    }

    fn barrier_wait(&self, tid: usize, nr_memento: usize) {
        // To guarantee that Ralloc's thread-local free list `TCache` was initialized before the thread crash simulation.
        #[cfg(feature = "simulate_tcrash")]
        let _dummy_alloc = self.alloc::<usize>();

        let _ = BARRIER_WAIT[tid].store(true, Ordering::SeqCst);
        for other in 1..=nr_memento {
            loop {
                if BARRIER_WAIT[other].load(Ordering::SeqCst) {
                    break;
                }
            }
        }
    }

    /// unsafe get root
    ///
    /// It is useful to check the object in the pool directly
    ///
    /// # Safety
    ///
    /// Carefully use `ix`
    pub unsafe fn get_root(&self, ix: u64) -> *mut c_void {
        RP_get_root_c(ix)
    }

    /// alloc
    #[inline]
    pub fn alloc<T>(&self) -> PPtr<T> {
        let ptr = self.pool().alloc(mem::size_of::<T>());
        PPtr::from(ptr as usize - self.start())
    }

    /// allocate according to the layout and return pointer pointing to it as `T`
    ///
    /// # Safety
    ///
    /// Carefully check `T` and `layout`
    #[inline]
    pub unsafe fn alloc_layout<T>(&self, layout: Layout) -> PPtr<T> {
        let ptr = self.pool().alloc(layout.size());
        PPtr::from(ptr as usize - self.start())
    }

    /// free
    #[inline]
    pub fn free<T>(&self, pptr: PPtr<T>) {
        let addr_abs = self.start() + pptr.into_offset();
        self.pool().free(addr_abs as *mut u8);
    }

    /// deallocate as much as the layout size from the offset address
    ///
    /// # Safety
    ///
    /// Carefully check `offset` and `layout`
    #[inline]
    pub unsafe fn free_layout(&self, offset: usize, _layout: Layout) {
        // NOTE: Ralloc's free does not receive a size, so just pass the address to deallocate.
        let addr_abs = self.start() + offset;
        self.pool().free(addr_abs as *mut u8);
    }

    #[inline]
    fn pool(&self) -> &Pool {
        unsafe { &*(self.start() as *const Pool) }
    }

    /// check if the `raw` addr is in range of pool
    #[inline]
    pub fn valid(&self, raw: usize) -> bool {
        raw >= self.start() && raw < self.end()
    }
}

impl Drop for PoolHandle {
    fn drop(&mut self) {
        unsafe { RP_close() }
    }
}

/// Pool
#[derive(Debug)]
pub struct Pool {}

impl Pool {
    /// Create pool
    ///
    /// Create and initialize a file to be used as a pool and return its handle.
    ///
    /// # Errors
    ///
    /// * Fail if file already exists in `filepath`
    /// * Fail if `size` is not more than `1GB` and less than `1TB` (forced by Ralloc)
    pub fn create<O, M>(
        filepath: &str,
        size: usize,
        nr_memento: usize, // number of root memento(s)
    ) -> Result<&'static PoolHandle, Error>
    where
        O: RootObj<M>,
        M: Collectable + Default,
    {
        if Path::new(&(filepath.to_owned() + "_basemd")).exists() {
            return Err(Error::new(
                std::io::ErrorKind::AlreadyExists,
                "File already exist.",
            ));
        }
        fs::create_dir_all(Path::new(filepath).parent().unwrap())?;

        global::clear();

        // create fil and initialze its content to pool layout of Ralloc
        let filepath = CString::new(filepath).expect("CString::new failed");
        let is_reopen = unsafe { RP_init(filepath.as_ptr(), size as u64) };
        assert_eq!(is_reopen, 0);

        unsafe {
            // set general cas checkpoint
            let cas_chk_arr = RP_malloc(mem::size_of::<[CASCheckpointArr; 2]>() as u64)
                as *mut [CASCheckpointArr; 2];
            cas_chk_arr.write(array_init::array_init(|_| {
                array_init::array_init(|_| CachePadded::new(AtomicU64::new(0)))
            }));
            persist_obj(cas_chk_arr.as_mut().unwrap(), true);
            let _prev = RP_set_root(cas_chk_arr as *mut c_void, RootIdx::CASCheckpoint as u64);
            let chk_ref = cas_chk_arr.as_ref().unwrap();

            // set global pool
            global::init(PoolHandle {
                start: RP_mmapped_addr(),
                len: size,
                exec_info: ExecInfo::from(chk_ref),
            });

            let pool = global_pool().unwrap();

            // set root obj
            let o_ptr = RP_malloc(mem::size_of::<O>() as u64) as *mut O;
            o_ptr.write(O::pdefault(pool));
            persist_obj(o_ptr.as_mut().unwrap(), true);
            let _prev = RP_set_root(o_ptr as *mut c_void, RootIdx::RootObj as u64);

            // set number of root mementos
            let nr_memento_ptr = RP_malloc(mem::size_of::<usize>() as u64) as *mut usize;
            nr_memento_ptr.write(nr_memento);
            persist_obj(nr_memento_ptr.as_mut().unwrap(), true);
            let _prev = RP_set_root(nr_memento_ptr as *mut c_void, RootIdx::NrMemento as u64);

            // set root memento(s): 1 ~ nr_memento
            for i in 1..nr_memento + 1 {
                let root_ptr = RP_malloc(mem::size_of::<M>() as u64) as *mut M;
                root_ptr.write(M::default());
                persist_obj(root_ptr.as_mut().unwrap(), true);
                let _prev = RP_set_root(
                    root_ptr as *mut c_void,
                    RootIdx::MementoStart as u64 + i as u64,
                );
            }

            // Initialize shared volatile variables
            lazy_static::initialize(&BARRIER_WAIT);
            epoch::init();

            Ok(pool)
        }
    }

    /// Open pool
    ///
    /// mapping the file to the persistent heap and return its handler with root type `O`
    ///
    /// # Safety
    ///
    /// * it must be loaded with the same type as the root op type (i.e. `O`) specified during `Pool::create`.
    ///
    /// # Errors
    ///
    /// * Fail if file does not exist in `filepath`
    /// * Fail if not called with the same size as the size specified during `Pool::create` (forced by Ralloc)
    pub unsafe fn open<O, M>(filepath: &str, size: usize) -> Result<&'static PoolHandle, Error>
    where
        O: RootObj<M>,
        M: Collectable + Default,
    {
        if !Path::new(&(filepath.to_owned() + "_basemd")).exists() {
            return Err(Error::new(std::io::ErrorKind::NotFound, "File not found."));
        }

        global::clear();

        // open file
        let filepath = CString::new(filepath).expect("CString::new failed");
        let is_reopen = RP_init(filepath.as_ptr(), size as u64);
        assert_eq!(is_reopen, 1);

        // get the starting address of the mapped address and set the global pool
        let chk_ref = (RP_get_root_c(RootIdx::CASCheckpoint as u64)
            as *const [CASCheckpointArr; 2])
            .as_ref()
            .unwrap();

        global::init(PoolHandle {
            start: RP_mmapped_addr(),
            len: size,
            exec_info: ExecInfo::from(chk_ref),
        });

        // run GC of Ralloc
        {
            unsafe extern "C" fn root_filter<T: Collectable>(
                ptr: *mut ::std::os::raw::c_char,
                tid: usize,
                gc: &mut GarbageCollection,
            ) {
                RP_mark(
                    gc,
                    ptr,
                    tid.wrapping_sub(RootIdx::MementoStart as usize),
                    Some(T::filter_inner),
                );
            }

            // set filter function of root obj
            RP_set_root_filter(Some(root_filter::<O>), RootIdx::RootObj as u64);

            // set filter function of root memento(s)
            let nr_memento = *(RP_get_root_c(RootIdx::NrMemento as u64) as *mut usize);
            for tid in 1..nr_memento + 1 {
                RP_set_root_filter(
                    Some(root_filter::<M>),
                    RootIdx::MementoStart as u64 + tid as u64,
                );
            }

            // call GC of Ralloc
            let _is_gc_executed = RP_recover();
        }

        let pool = global_pool().unwrap();
        pool.exec_info.set_info();

        // Initialize shared volatile variables
        lazy_static::initialize(&BARRIER_WAIT);
        epoch::init();

        Ok(pool)
    }

    /// TODO(doc)
    pub fn remove(filepath: &str) -> Result<(), Error> {
        fs::remove_file(&(filepath.to_owned() + "_basemd"))?;
        fs::remove_file(&(filepath.to_owned() + "_desc"))?;
        fs::remove_file(&(filepath.to_owned() + "_sb"))?;
        Ok(())
    }

    #[inline]
    fn alloc(&self, size: usize) -> *mut u8 {
        let addr_abs = unsafe { RP_malloc(size as u64) };
        addr_abs as *mut u8
    }

    #[inline]
    fn free(&self, ptr: *mut u8) {
        unsafe { RP_free(ptr as *mut c_void) }
    }
}

/// Root object of pool
pub trait RootObj<M: Collectable + Default>: PDefault + Collectable {
    /// Root object's default run function with a root memento
    fn run(&self, mmt: &mut M, tid: usize, guard: &Guard, pool: &PoolHandle);
}

#[cfg(test)]
mod tests {
    use crossbeam_epoch::Guard;
    use env_logger as _;
    use log::{self as _, debug};

    use crate::pmem::pool::*;
    use crate::test_utils::tests::*;

    impl RootObj<CheckInv> for DummyRootObj {
        fn run(&self, mmt: &mut CheckInv, _: usize, _: &Guard, _: &PoolHandle) {
            if mmt.flag {
                debug!("check inv");
                assert_eq!(mmt.value, 42);
            } else {
                debug!("update");
                mmt.value = 42;
                mmt.flag = true;
            }
        }
    }

    #[derive(Default)]
    struct CheckInv {
        value: usize,
        flag: bool,
    }

    impl Collectable for CheckInv {
        fn filter(_: &mut Self, _: usize, _: &mut GarbageCollection, _: &mut PoolHandle) {
            // no-op
        }
    }

    const FILE_NAME: &str = "check_inv";
    const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;

    // check flag=1 => value=42
    #[test]
    fn check_inv() {
        env_logger::init();

        run_test::<DummyRootObj, CheckInv>(FILE_NAME, FILE_SIZE, 1);
    }
}
