use central_domain::Timestamp;

/// Time source. Tests substitute a deterministic fake.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}
