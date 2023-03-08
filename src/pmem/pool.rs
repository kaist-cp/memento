//! Persistent Memory Pool
//!
//! A memory "pool" that maps files to virtual addresses as a persistent heap and manages those memory areas.

use std::alloc::Layout;
use std::ffi::{c_void, CString};
use std::io::Error;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fs, mem};

use crate::ploc::{CasHelpArr, CasHelpDescArr, ExecInfo, Handle, NR_MAX_THREADS};
use crate::pmem::global::global_pool;
use crate::pmem::ll::persist_obj;
use crate::pmem::ptr::PPtr;
use crate::pmem::{alloc::*, global};
use crate::*;
use crossbeam_epoch::{self as epoch};
use test_utils::thread;

// indicating at which root of Ralloc the metadata, root obj, and root mementos are located.
pub(crate) enum RootIdx {
    RootObj,        // root obj
    CASHelpArr,     // cas help array
    CASHelpDescArr, // cas help descriptor array
    NrMemento,      // number of root mementos
    MementoStart,   // start index of root memento(s)
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
        M: Memento + Send + Sync,
    {
        // get root obj
        let root_obj = unsafe {
            (PMEMAllocator::get_root(RootIdx::RootObj as u64) as *const O)
                .as_ref()
                .unwrap()
        };

        // get number of root memento(s)
        let nr_memento =
            unsafe { *(PMEMAllocator::get_root(RootIdx::NrMemento as u64) as *mut usize) };

        // repeat until `tid` thread succeeds the `tid`th memento
        let mut handles = Vec::new();
        for tid in 1..=nr_memento {
            // get `tid`th root mement
            let m_addr = unsafe {
                PMEMAllocator::get_root(RootIdx::MementoStart as u64 + tid as u64) as usize
            };

            let th = thread::spawn(move || {
                let h = thread::spawn(move || {
                    loop {
                        // Run memento
                        let mh = thread::spawn(move || {
                            let handle = Handle::new(tid, unsafe { epoch::old_guard(tid) }, self);
                            let root_mmt = unsafe { (m_addr as *mut M).as_mut().unwrap() };

                            // Barrier
                            handle.pool.barrier_wait(handle.tid, nr_memento);

                            // Run memento
                            root_obj.run(root_mmt, &handle);
                        });

                        // Join
                        // - Exit on success, re-run memento on failure
                        // - The guard used in case of failure is also not cleaned up.
                        //   A guard that loses its owner should be used well by the thread created in the next iteration.
                        if let Ok(_) = mh.join() {
                            break;
                        }

                        println!("[pool::execute] Thread {tid} re-executed.");
                    }
                });
                let _ = h.join();
            });
            handles.push(th);
        }

        while let Some(h) = handles.pop() {
            let _ = h.join();
        }
    }

    fn barrier_wait(&self, tid: usize, nr_memento: usize) {
        // Initialize Ralloc's thread local structures
        #[cfg(feature = "tcrash")]
        let _a = self.alloc::<usize>();

        BARRIER_WAIT[tid].store(true, Ordering::SeqCst);
        for other in 1..=nr_memento {
            while !BARRIER_WAIT[other].load(Ordering::SeqCst) {
                std::hint::spin_loop();
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
        PMEMAllocator::get_root(ix)
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
        assert!(self.valid(addr_abs));
        self.pool().free(addr_abs as *mut u8, mem::size_of::<T>());
    }

    /// deallocate as much as the layout size from the offset address
    ///
    /// # Safety
    ///
    /// Carefully check `offset` and `layout`
    #[inline]
    pub unsafe fn free_layout(&self, offset: usize, layout: Layout) {
        // NOTE: Ralloc's free does not receive a size, so just pass the address to deallocate.
        let addr_abs = self.start() + offset;
        self.pool().free(addr_abs as *mut u8, layout.size());
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
        unsafe { PMEMAllocator::close(self.start, self.len) }
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
    pub fn create<O: RootObj<M>, M: Memento>(
        filepath: &str,
        size: usize,
        nr_memento: usize, // number of root memento(s)
    ) -> Result<&'static PoolHandle, Error> {
        if Pool::is_valid(filepath) {
            return Err(Error::new(
                std::io::ErrorKind::AlreadyExists,
                "File already exist.",
            ));
        }
        fs::create_dir_all(Path::new(filepath).parent().unwrap())?;

        global::clear();

        // create fil and initialze its content to pool layout of Ralloc
        let filepath_c = CString::new(filepath).expect("CString::new failed");
        let is_reopen = unsafe { PMEMAllocator::create(filepath_c.as_ptr(), size as u64) };
        assert_eq!(is_reopen, 0);

        unsafe {
            // set general cas checkpoint
            let cas_help_arr =
                PMEMAllocator::malloc(mem::size_of::<CasHelpArr>() as u64) as *mut CasHelpArr;
            cas_help_arr.write(CasHelpArr::default());
            persist_obj(cas_help_arr.as_mut().unwrap(), true);
            let _prev =
                PMEMAllocator::set_root(cas_help_arr as *mut c_void, RootIdx::CASHelpArr as u64);
            let chk_ref = cas_help_arr.as_ref().unwrap();

            // set cas help descriptor
            let cas_help_desc_arr = PMEMAllocator::malloc(mem::size_of::<CasHelpDescArr>() as u64)
                as *mut CasHelpDescArr;
            cas_help_desc_arr.write(CasHelpDescArr::default());
            persist_obj(cas_help_desc_arr.as_mut().unwrap(), true);
            let _prev = PMEMAllocator::set_root(
                cas_help_desc_arr as *mut c_void,
                RootIdx::CASHelpDescArr as u64,
            );
            let desc_ref = cas_help_desc_arr.as_ref().unwrap();

            // set global pool
            global::init(PoolHandle {
                start: PMEMAllocator::mmapped_addr(),
                len: size,
                exec_info: ExecInfo::from((chk_ref, desc_ref)),
            });

            let pool = global_pool().unwrap();

            // set root obj
            let o_ptr = PMEMAllocator::malloc(mem::size_of::<O>() as u64) as *mut O;
            let tmp_handle = Handle::new(1, epoch::pin(), pool);
            tmp_handle.rec.store(false, Ordering::SeqCst);
            o_ptr.write(O::pdefault(&tmp_handle));
            persist_obj(o_ptr.as_mut().unwrap(), true);
            let _prev = PMEMAllocator::set_root(o_ptr as *mut c_void, RootIdx::RootObj as u64);

            // set number of root mementos
            let nr_memento_ptr =
                PMEMAllocator::malloc(mem::size_of::<usize>() as u64) as *mut usize;
            nr_memento_ptr.write(nr_memento);
            persist_obj(nr_memento_ptr.as_mut().unwrap(), true);
            let _prev =
                PMEMAllocator::set_root(nr_memento_ptr as *mut c_void, RootIdx::NrMemento as u64);

            // set root memento(s): 1 ~ nr_memento
            for i in 1..nr_memento + 1 {
                let root_ptr = PMEMAllocator::malloc(mem::size_of::<M>() as u64) as *mut M;
                root_ptr.write(M::default());
                persist_obj(root_ptr.as_mut().unwrap(), true);
                let _prev = PMEMAllocator::set_root(
                    root_ptr as *mut c_void,
                    RootIdx::MementoStart as u64 + i as u64,
                );
            }

            // Initialize shared volatile variables
            lazy_static::initialize(&BARRIER_WAIT);
            epoch::init();

            // Mark pool file as valid
            Pool::mark_valid(filepath)?;
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
    pub unsafe fn open<O: RootObj<M>, M: Memento>(
        filepath: &str,
        size: usize,
    ) -> Result<&'static PoolHandle, Error> {
        if !Pool::is_valid(filepath) {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "Pool is not valid.",
            ));
        }

        global::clear();

        // open file
        let filepath = CString::new(filepath).expect("CString::new failed");
        let is_reopen = PMEMAllocator::open(filepath.as_ptr(), size as u64);
        assert_eq!(is_reopen, 1);

        // get the starting address of the mapped address and set the global pool
        let chk_ref = (PMEMAllocator::get_root(RootIdx::CASHelpArr as u64) as *const CasHelpArr)
            .as_ref()
            .unwrap();
        let desc_ref = (PMEMAllocator::get_root(RootIdx::CASHelpDescArr as u64)
            as *const CasHelpDescArr)
            .as_ref()
            .unwrap();

        global::init(PoolHandle {
            start: PMEMAllocator::mmapped_addr(),
            len: size,
            exec_info: ExecInfo::from((chk_ref, desc_ref)),
        });

        // run GC of Ralloc
        {
            // set filter function of root obj
            PMEMAllocator::set_root_filter::<O>(RootIdx::RootObj as u64);

            // set filter function of root memento(s)
            let nr_memento = *(PMEMAllocator::get_root(RootIdx::NrMemento as u64) as *mut usize);
            for tid in 1..nr_memento + 1 {
                PMEMAllocator::set_root_filter::<M>(RootIdx::MementoStart as u64 + tid as u64);
            }

            // call GC of Ralloc
            let _is_gc_executed = PMEMAllocator::recover();
        }

        let pool = global_pool().unwrap();
        pool.exec_info.set_info();

        // Initialize shared volatile variables
        lazy_static::initialize(&BARRIER_WAIT);
        epoch::init();

        Ok(pool)
    }

    /// Remove pool
    pub fn remove(filepath: &str) -> Result<(), Error> {
        // _basedmd, _desc, _sb are pool files created by Ralloc
        fs::remove_file(filepath.to_owned())?;
        fs::remove_file(filepath.to_owned() + "_basemd")?;
        fs::remove_file(filepath.to_owned() + "_desc")?;
        fs::remove_file(filepath.to_owned() + "_sb")?;
        Ok(())
    }

    fn is_valid(filepath: &str) -> bool {
        Path::new(&(filepath.to_owned() + "_valid")).exists()
    }

    fn mark_valid(filepath: &str) -> Result<(), Error> {
        fs::write(Path::new(&(filepath.to_owned() + "_valid")), "1")?;
        Ok(())
    }

    #[inline]
    fn alloc(&self, size: usize) -> *mut u8 {
        let addr_abs = unsafe { PMEMAllocator::malloc(size as u64) };
        addr_abs as *mut u8
    }

    #[inline]
    fn free(&self, ptr: *mut u8, len: usize) {
        unsafe { PMEMAllocator::free(ptr as *mut c_void, len) }
    }
}

/// Root object of pool
pub trait RootObj<M: Memento>: PDefault + Collectable {
    /// Root object's default run function with a root memento
    fn run(&self, mmt: &mut M, handle: &Handle);
}

/// Test
pub mod test {
    use crate::pmem::pool::*;
    use crate::test_utils::tests::*;
    use mmt_derive::Collectable;

    impl RootObj<CheckInv> for DummyRootObj {
        fn run(&self, mmt: &mut CheckInv, _: &Handle) {
            if mmt.flag {
                assert_eq!(mmt.value, 42);
            } else {
                mmt.value = 42;
                mmt.flag = true;
            }
        }
    }

    #[derive(Default, Collectable, Memento)]
    struct CheckInv {
        value: usize,
        flag: bool,
    }

    // check flag=1 => value=42
    // #[cfg(not(feature = "pmcheck"))]
    #[test]
    fn check_inv() {
        const FILE_NAME: &str = "check_inv";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        run_test::<DummyRootObj, CheckInv>(FILE_NAME, FILE_SIZE, 1, 1);
    }

    /// check flag=1 => value=42
    /// TODO chek inv for pmcheck
    #[cfg(feature = "pmcheck")]
    pub fn check_invaa() {
        const FILE_NAME: &str = "check_inv";
        const FILE_SIZE: usize = 8 * 1024 * 1024 * 1024;
        run_test::<DummyRootObj, CheckInv>(FILE_NAME, FILE_SIZE, 1, 1);
    }
}
