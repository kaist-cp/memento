//! Persistent Stack

use crate::persistent::*;

/// Stack의 try push/pop 실패
#[derive(Debug, Clone)]
pub struct TryFail;

/// Persistent stack trait
pub trait Stack<T> {
    /// Try push 연산을 위한 Persistent op.
    /// Try push의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    type TryPush: for<'s> POp<Object<'s> = &'s Self, Input = T, Output<'s> = Result<(), TryFail>>;

    /// Push 연산을 위한 Persistent op.
    /// 반드시 push에 성공함.
    type Push: for<'s> POp<Object<'s> = &'s Self, Input = T, Output<'s> = ()>;

    /// Try pop 연산을 위한 Persistent op.
    /// Try pop의 결과가 `TryFail`일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `TryFail`이 됨.
    /// Try pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type TryPop: for<'s> POp<Object<'s> = &'s Self, Input = (), Output<'s> = Result<Option<T>, TryFail>>;

    /// Pop 연산을 위한 Persistent op.
    /// 반드시 pop에 성공함.
    /// pop의 결과가 `None`(empty)일 경우, 재시도 시 stack의 상황과 관계없이 언제나 `None`이 됨.
    type Pop: for<'s> POp<Object<'s> = &'s Self, Input = (), Output<'s> = Option<T>>;
}

/// push_pop을 반복하는 Concurrent stack test
///
/// - Job: 자신의 tid로 1회 push하고 그 뒤 1회 pop을 함
/// - 여러 스레드가 Job을 반복
/// - 마지막에 지금까지의 모든 pop의 결과물이 각 tid값의 정확한 누적 횟수를 가지는지 체크
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crossbeam_utils::thread;

    pub(crate) fn test_push_pop<S>(nr_thread: usize, cnt: usize)
    where
        S: Stack<usize> + Default + Sync,
        S::Push: Send,
        S::Pop: Send,
    {
        let s = S::default(); // TODO(persistent location)
        let mut pushes: Vec<Vec<S::Push>> = (0..nr_thread)
            .map(|_| (0..cnt).map(|_| S::Push::default()).collect())
            .collect(); // TODO(persistent location)
        let mut pops: Vec<Vec<S::Pop>> = (0..nr_thread)
            .map(|_| (0..cnt).map(|_| S::Pop::default()).collect())
            .collect(); // TODO(persistent location)

        // 아래 로직은 idempotent 함

        #[allow(box_pointers)]
        thread::scope(|scope| {
            for tid in 0..nr_thread {
                let s = &s;
                let pushes = unsafe {
                    (pushes.get_unchecked_mut(tid) as *mut Vec<S::Push>)
                        .as_mut()
                        .unwrap()
                };
                let pops = unsafe {
                    (pops.get_unchecked_mut(tid) as *mut Vec<S::Pop>)
                        .as_mut()
                        .unwrap()
                };

                let _ = scope.spawn(move |_| {
                    for i in 0..cnt {
                        pushes[i].run(s, tid);
                        assert!(pops[i].run(s, ()).is_some());
                    }
                });
            }
        })
        .unwrap();

        // Check empty
        assert!(S::Pop::default().run(&s, ()).is_none());

        // Check results
        let mut results = vec![0_usize; nr_thread];
        for pops in pops.iter_mut() {
            for pop in pops.iter_mut() {
                let ret = pop.run(&s, ()).unwrap();
                results[ret] += 1;
            }
        }

        assert!(results.iter().all(|r| *r == cnt));
    }
}
