use std::sync::atomic::Ordering;

use atomic::Atomic;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::Backoff;

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let queue = Box::into_raw(Box::new(ArrayQueue::<T>::new(cap)));
    let counter = Box::into_raw(Box::new(Counter::new()));

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

unsafe impl<T> Send for Sender<T> {}
unsafe impl<T> Sync for Sender<T> {}

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
    // right: number of receiver (always n<=1 in this implementation because it is mpsc)
    sender_recver: Atomic<(i32, i32)>,
}

impl Counter {
    fn new() -> Self {
        Counter {
            sender_recver: Atomic::new((1, 1)),
        }
    }

    fn peek(&self) -> (i32, i32) {
        self.sender_recver.load(Ordering::SeqCst)
    }

    fn add(&self, left: i32, right: i32) -> (i32, i32) {
        let backoff = Backoff::new();
        loop {
            let old = self.sender_recver.load(Ordering::SeqCst); // (cnt_sender, cnt_recver)
            let new = (old.0 + left, old.1 + right); // (cnt_sender + left, cnt_recver + right);

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

/// Tests from https://doc.rust-lang.org/std/sync/mpsc/
#[cfg(test)]
mod test {
    use crate::bounded;

    /// Simple usage
    #[test]
    fn simple() {
        use std::thread;

        // Create a simple streaming channel
        let (tx, rx) = bounded(1024);
        thread::spawn(move || {
            tx.send(10).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 10);
    }

    /// Shared usage
    #[test]
    fn shared() {
        use std::thread;

        // Create a shared channel that can be sent along from many threads
        // where tx is the sending half (tx for transmission), and rx is the receiving
        // half (rx for receiving).
        let (tx, rx) = bounded(1024);
        for i in 0..10 {
            let tx = tx.clone();
            thread::spawn(move || {
                tx.send(i).unwrap();
            });
        }

        for _ in 0..10 {
            let j = rx.recv().unwrap();
            assert!(0 <= j && j < 10);
        }
    }

    /// Propagating panics
    #[test]
    fn prop_panic() {
        // The call to recv() will return an error because the channel has already
        // hung up (or been deallocated)
        let (tx, rx) = bounded::<i32>(1024);
        drop(tx);
        assert!(rx.recv().is_err());
    }

    /// Synchronous channels
    #[test]
    fn sync_chan() {
        use std::thread;

        let (tx, rx) = bounded(1024);
        thread::spawn(move || {
            // This will wait for the parent thread to start receiving
            tx.send(53).unwrap();
        });
        rx.recv().unwrap();
    }

    /// Unbounded receive loop
    #[test]
    fn unbounded_recv_loop() {
        use std::thread;

        let (tx, rx) = bounded(3);

        for _ in 0..3 {
            // It would be the same without thread and clone here
            // since there will still be one `tx` left.
            let tx = tx.clone();
            // cloned tx dropped within thread
            thread::spawn(move || tx.send("ok").unwrap());
        }

        // Drop the last sender to stop `rx` waiting for message.
        // The program will not complete if we comment this out.
        // **All** `tx` needs to be dropped for `rx` to have `Err`.
        drop(tx);

        // Unbounded receiver waiting for all senders to complete.
        while let Ok(msg) = rx.recv() {
            println!("{msg}");
        }

        println!("completed");
    }
}
