use std::sync::atomic::Ordering;

use atomic::Atomic;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::Backoff;

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let queue = Box::into_raw(Box::new(ArrayQueue::<T>::new(cap)));
    let counter = Box::into_raw(Box::new(Counter::default()));

    (
        Sender {
            msgs: queue,
            counter,
        },
        Receiver {
            msgs: queue,
            counter,
        },
    )
}

pub struct Sender<T> {
    msgs: *mut ArrayQueue<T>,
    counter: *mut Counter,
}

impl<T> Sender<T> {
    unsafe fn msgs(&self) -> &ArrayQueue<T> {
        self.msgs.as_ref().unwrap()
    }

    unsafe fn counter(&self) -> &Counter {
        self.counter.as_ref().unwrap()
    }

    /// Blocks the current thread until a message is sent or the channel is disconnected.
    pub fn send(&self, msg: T) -> Result<(), T> {
        let backoff = Backoff::new();
        let mut msg = msg;
        loop {
            // return Err if there is no receiver
            if unsafe { self.counter() }.peek().1 == 0 {
                return Err(msg);
            }

            match unsafe { self.msgs() }.push(msg) {
                Ok(_) => return Ok(()),
                Err(m) => {
                    msg = m;
                    backoff.snooze();
                }
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let _ = unsafe { self.counter() }.add(1, 0);

        Self {
            msgs: self.msgs.clone(),
            counter: self.counter.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        unsafe {
            if self.counter().add(-1, 0) == (0, 0) {
                // Drop channel components if i am the last one.
                drop(Box::from_raw(self.msgs));
                drop(Box::from_raw(self.counter));
            }
        }
    }
}

pub struct Receiver<T> {
    msgs: *mut ArrayQueue<T>,
    counter: *mut Counter,
}

impl<T> Receiver<T> {
    unsafe fn msgs(&self) -> &ArrayQueue<T> {
        self.msgs.as_ref().unwrap()
    }

    unsafe fn counter(&self) -> &Counter {
        self.counter.as_ref().unwrap()
    }

    pub fn recv(&self) -> Result<T, ()> {
        let backoff = Backoff::new();
        loop {
            match unsafe { self.msgs() }.pop() {
                Some(msg) => return Ok(msg),
                None => {
                    // return Err if there is no sender and no messages in the buffer.
                    if unsafe { self.counter() }.peek().0 == 0 {
                        return Err(());
                    }
                    backoff.snooze()
                }
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        unsafe {
            if self.counter().add(0, -1) == (0, 0) {
                // Drop channel components if i am the last one.
                drop(Box::from_raw(self.msgs));
                drop(Box::from_raw(self.counter));
            }
        }
    }
}

#[derive(Default)]
struct Counter {
    // left: number of sender
    // right: number of receiver (n<=1 in this implementation because it is mpsc)
    sender_recver: Atomic<(u32, u32)>,
}

impl Counter {
    fn peek(&self) -> (u32, u32) {
        self.sender_recver.load(Ordering::SeqCst)
    }

    fn add(&self, left: i32, right: i32) -> (u32, u32) {
        let backoff = Backoff::new();
        loop {
            let old = self.sender_recver.load(Ordering::SeqCst); // (cnt_sender, cnt_recver)
            let new = (old.0 + left as u32, old.1 + right as u32); // (cnt_sender + left, cnt_recver + right);

            if self
                .sender_recver
                .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return new;
            }

            backoff.snooze();
        }
    }
}
