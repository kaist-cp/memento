//! Persistent ticket lock

use std::sync::atomic::AtomicUsize;

use crate::persistent::PersistentOp;

/// TODO: doc
#[derive(Debug, Clone)] // 문제: Guard를 Clone으로 해야된다는 건 비직관적
pub struct Guard<'l> {
    lock: &'l TicketLock
    // TODO: token
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        // TODO: 구현
        // TODO: clone 된 guard에 대처
        //       - ticket lock의 curr와 같은지 확인
        //       - overflow wrap되어 한바퀴 돈 ticket을 unlock하는 불상사 대비
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct Lock {
    // TODO: 구현
}

impl<'l> PersistentOp<'l> for Lock {
    type Object = &'l TicketLock;
    type Input = ();
    type Output = Guard<'l>;

    fn run(&mut self, lock: Self::Object, _: Self::Input) -> Self::Output {
        // TODO: 구현
        Guard { lock }
    }

    fn reset(&mut self, nested: bool) {
        // TODO: 구현.
        let _ = nested;
    }
}

/// TODO: doc
#[derive(Debug, Default)]
pub struct TicketLock {
    curr: AtomicUsize,
    next: AtomicUsize
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;

    use super::*;
    use std::collections::VecDeque;

    const NR_THREAD: usize = 4;
    const COUNT: usize = 1_000_000;

    type Queue<T> = VecDeque<T>;

    #[derive(Default)]
    struct Obj {
        queue: Queue<usize>,
        lock: TicketLock,
    }

    #[derive(Default)]
    struct PushPop {
        lock: Lock,
        // TODO: Queue의 push,
        // TODO: Queue의 Pop,
        resetting: bool
    }

    impl<'o> PersistentOp<'o> for PushPop {
        type Object = &'o Obj;
        type Input = usize;
        type Output = Option<usize>;

        // TODO: 쓰임새를 보이는 용도로 VecDequeue의 push_back(), pop_back()를 사용.
        //       이들은 PersistentOp이 아니므로 이 run()은 지금은 idempotent 하지 않음.
        fn run(&mut self, object: Self::Object, input: Self::Input) -> Self::Output {
            if self.resetting {
                self.reset(false);
            }

            // Lock the object
            let _guard = self.lock.run(&object.lock, ());

            // Push & Pop
            let q: &mut Queue<usize> = unsafe { &mut *(&object.queue as *const _ as *mut _) };
            q.push_back(input);
            q.pop_front()
        } // Unlock when `_guard` is dropped

        fn reset(&mut self, nested: bool) {
            if !nested {
                self.resetting = true;
            }

            self.lock.reset(true);
            // TODO: reset Push and Pop

            if !nested {
                self.resetting = false;
            }
        }
    }

    #[test]
    fn push_pop_seq_queue() {
        let obj = Obj::default(); // TODO(persistent location)
        let mut push_pops: Vec<Vec<PushPop>> = (0..NR_THREAD)
            .map(|_| (0..COUNT).map(|_| PushPop::default()).collect())
            .collect(); // TODO(persistent location)

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..NR_THREAD {
                let obj = &obj;
                let push_pops = unsafe {
                    (push_pops.get_unchecked_mut(tid) as *mut Vec<PushPop>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..COUNT {
                        // Check if push_pop acts like an identity function
                        // lock 구현 안 되어 있으므로 assertion 실패함
                        assert_eq!(push_pops[i].run(obj, tid), Some(tid));
                    }
                });
            }
        })
        .unwrap();
    }
}
