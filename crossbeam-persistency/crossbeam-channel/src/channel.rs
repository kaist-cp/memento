use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let queue = Box::into_raw(Box::new((ArrayQueue::<T>::new(cap))));
    let cnt_sender = Box::into_raw(Box::new(AtomicUsize::new(1)));
    let cnt_recver = Box::into_raw(Box::new(AtomicUsize::new(1)));

    (
        Sender {
            msgs: queue,
            cnt: cnt_sender,
            cnt_recver,
        },
        Receiver {
            msgs: queue,
            cnt: cnt_recver,
            cnt_sender,
        },
    )
}

pub struct Sender<T> {
    msgs: *mut ArrayQueue<T>,
    cnt: *mut AtomicUsize,
    cnt_recver: *mut AtomicUsize,
}

impl<T> Sender<T> {
    unsafe fn inner(&self) -> &ArrayQueue<T> {
        todo!()
    }

    /// Attempts to send a message into the channel without blocking.
    pub fn try_send(&self, msg: T) -> Result<(), T> {
        unsafe { self.inner() }.push(msg)
    }

    /// Blocks the current thread until a message is sent or the channel is disconnected.
    pub fn send(&self, msg: T) {
        // let backoff = Backoff;
        let mut msg = msg;
        while let Err(m) = self.try_send(msg) {
            msg = m
            // TODO: backoff
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let _ = unsafe { self.cnt.as_ref() }
            .unwrap()
            .fetch_add(1, Ordering::SeqCst);
        Self {
            msgs: self.msgs.clone(),
            cnt: self.cnt.clone(),
            cnt_recver: self.cnt_recver.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let cnt_prev = unsafe { self.cnt.as_ref() }
            .unwrap()
            .fetch_sub(1, Ordering::SeqCst);
        if cnt_prev == 1
            && unsafe { self.cnt_recver.as_ref() }
                .unwrap()
                .load(Ordering::SeqCst)
                == 0
        {
            // I am the owner who can drop this channel
            todo!("drop queue, cnt_sender, cnt_recver")
        }
    }
}

pub struct Receiver<T> {
    msgs: *const ArrayQueue<T>,
    cnt: *const AtomicUsize,
    cnt_sender: *const AtomicUsize,
}

impl<T> Receiver<T> {
    unsafe fn inner(&self) -> &ArrayQueue<T> {
        todo!()
    }

    /// Attempts to receive a message from the channel without blocking.
    ///
    /// This method will either receive a message from the channel immediately or return an error
    /// if the channel is empty.
    pub fn try_recv(&self) -> Result<T, ()> {
        unsafe { self.inner() }.pop().ok_or(())
    }

    pub fn recv(&self) -> Result<T, ()> {
        // let backoff = Backoff;
        loop {
            if let Ok(msg) = self.try_recv() {
                return Ok(msg);
            }
            // TODO: backoff
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let _ = unsafe { self.cnt.as_ref() }
            .unwrap()
            .fetch_add(1, Ordering::SeqCst);
        Self {
            msgs: self.msgs.clone(),
            cnt: self.cnt.clone(),
            cnt_sender: self.cnt_sender.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let cnt_prev = unsafe { self.cnt.as_ref() }
            .unwrap()
            .fetch_sub(1, Ordering::SeqCst);
        if cnt_prev == 1
            && unsafe { self.cnt_sender.as_ref() }
                .unwrap()
                .load(Ordering::SeqCst)
                == 0
        {
            // I am the owner who can drop this channel
            todo!("drop queue, cnt_sender, cnt_recver")
        }
    }
}
