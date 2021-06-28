/// op를 exactly-once 실행하기 위한 Client trait
pub trait PersistentClient: Default {
    /// 새로운 op을 실행을 위해 info 재사용하기 위해 idempotent 하게 리셋
    fn reset(&mut self);
}
