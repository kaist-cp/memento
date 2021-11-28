//! Trait collection for persistent objects

use crate::plocation::{pool::PoolHandle, ralloc::Collectable};
use crossbeam_epoch::Guard;
use std::{mem::ManuallyDrop, ptr};

/// Ownership을 얼리기 위한 wrapper.
///
/// - `from()`을 통해 target object의 ownership을 얼림
/// - `own()`을 통해 object의 ownership을 다시 획득
/// - `ManuallyDrop`과 유사. 차이점은 `ManuallyDrop`은 value가 `Clone`일 때에만 `clone()`하지만
///   `Frozen`은 어떤 value든 `clone()` 가능하다는 것임
#[derive(Debug)]
pub struct Frozen<T> {
    value: ManuallyDrop<T>,
}

impl<T> Clone for Frozen<T> {
    fn clone(&self) -> Self {
        Self {
            value: unsafe { ptr::read(&self.value) },
        }
    }
}

impl<T> From<T> for Frozen<T> {
    fn from(item: T) -> Self {
        Self {
            value: ManuallyDrop::new(item),
        }
    }
}

impl<T> Frozen<T> {
    /// object의 ownership을 획득
    ///
    /// # Safety
    ///
    /// 다음 두 조건을 모두 만족할 때에만 safe:
    /// - `own()` 후 object로의 마지막 접근(*t1*)과
    ///   object가 다른 스레드에 넘겨지는 시점 혹은 own한 스레드에서 drop 되는 시점(*t2*) 사이에
    ///   checkpoint(*c*)가 있어야 함.
    ///   + checkpoint(*c*): object가 더 이상 필요하지 않음을 나타낼 수 있는 어떠한 증거든 상관 없음 (e.g. flag, states)
    /// - *c*를 아직 거치지 않았다는 것을 알아야 함.
    ///
    /// # Examples
    ///
    /// ```rust
    ///    use memento::persistent::Frozen;
    ///
    ///    // 이 변수들은 언제나 pmem에서 접근 가능함을 가정
    ///    let src = Frozen::<Box<i32>>::from(Box::new(42)); // TODO: use `PBox`
    ///    let mut data = 0;
    ///    let mut flag = false;
    ///
    ///    {
    ///        // `src`로부터 메시지를 받아와 data에 저장하는 로직
    ///        // 이 로직은 crash 이전이나 이후 모두 안전함
    ///        if !flag { // Checking if the checkpoint c has not yet passed
    ///            let msg = src.clone(); // Cloning a `Frozen` object from somewhere.
    ///            let x = unsafe { msg.own() }; // This is always safe because `flag` shows that the inner value of `msg` is still valid.
    ///            data = *x; // The last access to `x` (t1)
    ///            flag = true; // Checkpointing that `msg` is no longer needed. (c)
    ///            // x is dropped (t2), no more valid.
    ///        }
    ///        assert_eq!(data, 42);
    ///    }
    /// ```
    pub unsafe fn own(self) -> T {
        ManuallyDrop::into_inner(self.value)
    }
}

/// op을 exactly-once 실행하기 위한 trait
///
/// # Safety
///
/// * 초기화 혹은 `reset()` 후 다음 `reset()` 전까지 `Memento`은 *반드시* 한 object에 대해서만 `run()`을 수행해야 함.
/// * `Memento`는 자신 혹은 자신이 사용한 `Guard`가 Drop 될 때 *반드시* `reset()` 되어있는 상태여야 함.
pub trait Memento: Default + Collectable {
    /// Persistent op의 target object
    type Object<'o>;

    /// Persistent op의 input type
    type Input;

    /// Persistent op의 output type
    type Output<'o>: Clone
    where
        Self: 'o;

    /// Persistent op이 적용되지 않았을 때 발생하는 Error type
    type Error;

    /// Persistent op 동작 함수 (idempotent)
    ///
    /// - `Ok`를 반환한 적이 있는 op은 같은 input에 대해 언제나 같은 Output을 반환
    /// - `Err`를 반환한 op은 `reset()` 없이 다시 호출 가능
    /// - Input을 매번 인자로 받아 불필요한 백업을 하지 않음
    /// - Pre-crash op이 충분히 진행됐을 경우 Post-crash 재실행시의 input이 op 결과에 영향을 끼치지 않을 수도 있음.
    ///   즉, post-crash의 functional correctness는 보장하지 않음. (이러한 동작이 safety를 해치지 않음.)
    ///
    /// ## Argument
    /// * `PoolHandle` - 메모리 관련 operation(e.g. `deref`, `alloc`)을 어느 풀에서 할지 알기 위해 필요
    fn run<'o>(
        &'o mut self,
        object: Self::Object<'o>,
        input: Self::Input,
        guard: &mut Guard,
        pool: &'static PoolHandle,
    ) -> Result<Self::Output<'o>, Self::Error>;

    /// 새롭게 op을 실행하도록 재사용하기 위해 리셋 (idempotent)
    ///
    /// 어떤 op들의 `reset()`은 1개 이하의 instruction으로 수행될 수도 있고, 어떤 op들은
    /// 그보다 많은 instruction을 요구할 수도 있다. 후자의 경우 reset 하고 있음을 나타내는 flag를 통해
    /// reset 도중에 crash가 났을 때에도 이후에 reset 하다가 crash 났음을 알 수 있게 해야만 한다.
    ///
    /// `nested`: 상위 op의 `reset()`에서 하위 op을 `reset()`을 호출할 경우 이미 상위 op의 reset 중임을
    /// 나타내는 flag가 켜져있으므로 하위 op의 reset이 따로 reset flag를 설정할 필요가 없다. 이를 위해 하위
    /// op의 `reset()` 호출 시 `nested`를 `true`로 해주어 내부에서 별도로 reset flag를 설정할 필요가 없도록
    /// 알려줄 수 있다.
    fn reset(&mut self, nested: bool, guard: &mut Guard, pool: &'static PoolHandle);

    /// Recovery할 때만 필요한 로직을 포함하는 memento가 해당 로직을 수행하도록 셋팅
    ///
    /// 프로그램 시작할 때 root에서 set_recovery를 호출
    fn set_recovery(&mut self, pool: &'static PoolHandle);
}

/// TODO: doc
pub trait PDefault: Collectable {
    /// TODO: doc
    fn pdefault(pool: &'static PoolHandle) -> Self;
}
