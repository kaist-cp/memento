//! Trait collection for persistent objects

/// op를 exactly-once 실행하기 위한 Client trait
pub trait PersistentClient: Default {
    /// 새로운 op을 실행을 위해 info 재사용하기 위해 idempotent 하게 리셋
    fn reset(&mut self);
}

/// Persistent obj을 사용하기 위한 Persistent op trait
pub trait PersistentOp<C: PersistentClient> {
    /// Persistent op의 input type
    type Input;

    /// Persistent op의 output type
    type Output;

    /// Persistent op 호출 함수
    /// - client의 타입을 통해 op을 구분 (e.g. 기존 `Queue.push()`는 `Queue.persistent_op(&mut PushClient)`와 같이 호출)
    /// - Input을 매번 인자로 받아 불필요한 백업을 하지 않음
    ///   + 참고: Post-crash input과 pre-crash(client-tracked) input이 다른 경우의 정책:
    ///     * Pre-crash(client-tracked) input을 기반으로 동작함
    ///     * safe하기만 하면 되므로 상관 없음. functional correctness는 보장하지 않음.
    /// - 같은 client에 대해 언제나 같은 Output을 반환 (idempotent)
    fn persistent_op(&self, client: &mut C, input: Self::Input) -> Self::Output;
}
