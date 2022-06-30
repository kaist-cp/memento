use std::{ops::DerefMut, sync::atomic::Ordering};

use super::P;
use crate::common::{pick, TestNOps, QUEUE_INIT_SIZE, TOTAL_NOPS};
use corundum::{default::*, ptr::Ptr};
use crossbeam_utils::thread;

#[derive(Debug, Default)]
#[repr(align(128))]
struct Node {
    val: usize, // TODO: generic
    next: Option<Ptr<Node, P>>,
}

impl Node {
    fn new(val: usize) -> Self {
        Self { val, next: None }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // TODO: debugging feature
        // println!("drop node: {:?}", self);
    }
}

/// Corundum Queue
#[derive(Debug)]
pub struct CrndmQueue {
    mutex: Parc<PMutex<usize>>,
    head: PRefCell<Option<Ptr<Node, P>>>,
    tail: PRefCell<Option<Ptr<Node, P>>>,
}

unsafe impl Sync for CrndmQueue {}
unsafe impl Send for CrndmQueue {}

impl RootObj<P> for CrndmQueue {
    fn init(journal: &Journal) -> Self {
        Self {
            mutex: Parc::new(PMutex::new(0), journal),
            head: PRefCell::new(None),
            tail: PRefCell::new(None),
        }
    }
}

impl CrndmQueue {
    pub fn enqueue(&self, val: usize) {
        P::transaction(|j| {
            // Lock and get reference
            let _lock = self.mutex.lock(j);
            let mut head_ref = self.head.borrow_mut(j);
            let mut tail_ref = self.tail.borrow_mut(j);

            // Allocate node and convert to sharable ptr (leak Pbox)
            let node = Pbox::new(Node::new(val), j);
            let node_ptr = unsafe { Ptr::from_raw(Pbox::into_raw(node)) };

            // Enqueue
            if head_ref.is_none() && tail_ref.is_none() {
                *head_ref = Some(node_ptr);
            } else {
                tail_ref.unwrap().next = Some(node_ptr);
            }
            *tail_ref = Some(node_ptr);
        })
        .unwrap();
    }

    pub fn dequeue(&self) -> Option<usize> {
        P::transaction(|j| {
            // Lock and get reference
            let _lock = self.mutex.lock(j);
            let mut head_ref = self.head.borrow_mut(j);
            let mut tail_ref = self.tail.borrow_mut(j);

            // Empty
            if head_ref.is_none() {
                return None;
            }

            // Not Empty
            let mut head = head_ref.unwrap();
            let val = head.val;
            let next = head.next;
            *head_ref = next;
            if head_ref.is_none() {
                *tail_ref = None;
            }
            drop(unsafe { Pbox::<Node>::from_raw(head.deref_mut() as *mut Node) });
            Some(val)
        })
        .unwrap()
    }

    pub fn size(&self) -> usize {
        P::transaction(|_| {
            let mut size = 0;
            let mut ptr = self.head.as_ref();
            loop {
                if ptr.is_none() {
                    break;
                }
                size += 1;
                let node = ptr.as_ref().unwrap();
                ptr = &node.next;
            }
            size
        })
        .unwrap()
    }

    /// print all elements of queue
    // TODO: debugging feature
    pub fn print_all(&self) {
        print!("print_all: ");
        P::transaction(|_| {
            let mut ptr = self.head.as_ref();
            loop {
                if ptr.is_none() {
                    break;
                }
                let node = ptr.as_ref().unwrap();
                print!("{}, ", node.val);
                ptr = &node.next;
            }
            println!();
        })
        .unwrap();
    }
}

#[derive(Root, Debug)]
pub struct TestCrndmQueue {
    queue: CrndmQueue,
}
impl TestNOps for TestCrndmQueue {}

impl TestCrndmQueue {
    pub fn get_nops(&self, nr_thread: usize, duration: f64, prob: Option<u32>) -> usize {
        let q = &self.queue;

        // initailize
        for i in 0..unsafe { QUEUE_INIT_SIZE } {
            q.enqueue(i);
        }

        // Create nr_threads and execute
        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                let _ = scope.spawn(move |_| {
                    let ops = if let Some(prob) = prob {
                        // prob
                        self.test_nops(
                            &|tid, _| {
                                if pick(prob) {
                                    q.enqueue(tid);
                                } else {
                                    let _ = q.dequeue();
                                }
                            },
                            tid,
                            duration,
                            unsafe { crossbeam_epoch::unprotected() },
                        )
                    } else {
                        // pair
                        self.test_nops(
                            &|tid, _| {
                                q.enqueue(tid);
                                let _ = q.dequeue();
                            },
                            tid,
                            duration,
                            unsafe { crossbeam_epoch::unprotected() },
                        )
                    };

                    let _ = TOTAL_NOPS.fetch_add(ops, Ordering::SeqCst);
                });
            }
        })
        .unwrap();

        TOTAL_NOPS.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod test {
    use super::CrndmQueue;
    use super::P;
    use corundum::default::*;
    use corundum::open_flags::{O_1GB, O_CF};
    use crossbeam_utils::thread;
    use memento::test_utils::tests::get_test_abs_path;

    const FILE_NAME: &str = "crndm_enqdeq.pool";
    const COUNT: usize = 100_000;

    #[test]
    fn enq_deq() {
        let filepath = get_test_abs_path(FILE_NAME);
        let queue = P::open::<CrndmQueue>(&filepath, O_1GB | O_CF).unwrap();

        for i in 0..COUNT {
            queue.enqueue(i);
        }
        for i in 0..COUNT {
            assert_eq!(queue.dequeue(), Some(i));
        }
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn enq_deq_concur() {
        let filepath = get_test_abs_path(FILE_NAME);
        let queue = P::open::<CrndmQueue>(&filepath, O_1GB | O_CF).unwrap();
        let q = &*queue;

        #[allow(box_pointers)]
        thread::scope(|scope| {
            let _ = scope.spawn(move |_| {
                for i in 0..COUNT {
                    q.enqueue(i);
                }
            });
            let _ = scope.spawn(move |_| {
                for i in 0..COUNT {
                    loop {
                        if let Some(v) = q.dequeue() {
                            assert_eq!(v, i);
                            break;
                        }
                    }
                }
            });
        })
        .unwrap();
        assert_eq!(q.dequeue(), None);
    }
}
