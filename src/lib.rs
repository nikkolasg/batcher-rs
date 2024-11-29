use std::collections::HashSet;
use std::fmt::Debug;
use std::mem;

/// Element to batch together
///
/// It can be uniquely represented by a ID.
pub trait Unit: Debug + Clone + Send + Sync {
    type ID: Debug + Eq + std::hash::Hash + Clone + Send + Sync;
    /// Returns the ID associated with this unit.
    fn id(&self) -> Self::ID;
}

/// Batcher is the main trait driving the creation of a batch.
///
/// A batch is a vector of units. A batch can grow by calling `new_unit`.
/// At any point in time, the caller can decide to release the current pending batch and get the
/// list of units.
pub trait Batcher {
    type Unit: Unit;

    /// new_unit adds a new unit to the batch. The return enum indicates whether the batch is
    /// ready.
    fn new_unit(&mut self, unit: Self::Unit) -> Option<Vec<Self::Unit>>;
    /// release the whole pending batch regardless if it is full or not. Sometimes necessary when
    /// for example some timeout expired.
    fn release(self) -> Vec<Self::Unit>;
}

/// An enum implementing various batching strategies. User can implement its own strategy by
/// implementing the `Policy` trait.
pub enum PolicyKind<ID> {
    BySize(usize),
    ByList(HashSet<ID>),
}

/// Policy implementing the batcher trait.
///
/// It uses the PolicyKind to drive its decision whether to release or not the batch at each
/// insertion. The current implementation uses a vector as backend.
pub struct PolicyBatcher<U: Unit> {
    backend: VecBatcher<U>,
    policy: PolicyKind<U::ID>,
}

/// Enum indicating whether a batch is ready or should be further built.
enum BatchStatus {
    KeepBatching,
    ReleaseBatch,
}

/// VecBatcher is a simple implementation of the Batcher trait backed by a vector.
#[derive(Default)]
struct VecBatcher<U> {
    pending: Vec<U>,
}

impl<U: Unit> VecBatcher<U> {
    pub fn new() -> Self {
        Self {
            pending: Default::default(),
        }
    }
    fn new_unit(&mut self, unit: U) {
        self.pending.push(unit);
    }
    fn release(&mut self) -> Vec<U> {
        mem::take(&mut self.pending)
    }
}

impl From<bool> for BatchStatus {
    fn from(value: bool) -> Self {
        match value {
            true => BatchStatus::ReleaseBatch,
            false => BatchStatus::KeepBatching,
        }
    }
}

impl<ID> PolicyKind<ID>
where
    ID: Eq + std::hash::Hash,
{
    fn outcome<U: Unit<ID = ID>>(&self, batch: &VecBatcher<U>) -> BatchStatus {
        match self {
            PolicyKind::BySize(max_size) => BatchStatus::from(batch.pending.len() >= *max_size),
            PolicyKind::ByList(ref set) => BatchStatus::from(
                &batch
                    .pending
                    .iter()
                    .map(|u| u.id())
                    .collect::<HashSet<ID>>()
                    == set,
            ),
        }
    }
}

impl<U: Unit> PolicyBatcher<U> {
    pub fn new(policy: PolicyKind<U::ID>) -> Self {
        Self {
            backend: VecBatcher::new(),
            policy,
        }
    }
}
impl<U> Batcher for PolicyBatcher<U>
where
    U: Unit,
{
    type Unit = U;

    fn new_unit(&mut self, unit: Self::Unit) -> Option<Vec<Self::Unit>> {
        self.backend.new_unit(unit);
        match self.policy.outcome(&self.backend) {
            BatchStatus::KeepBatching => None,
            BatchStatus::ReleaseBatch => Some(self.backend.release()),
        }
    }

    fn release(mut self) -> Vec<Self::Unit> {
        self.backend.release()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use derive_more::From;

    #[derive(From, Clone, Debug, PartialEq)]
    struct TestUnit(usize);
    impl Unit for TestUnit {
        type ID = usize;

        fn id(&self) -> Self::ID {
            self.0
        }
    }

    struct TestCase {
        to_insert: Vec<TestUnit>,
        policy: PolicyKind<usize>,
        expected_outcome: Option<Vec<TestUnit>>,
    }

    #[test]
    fn test_policy_kind() {
        let cases = vec![
            // BySize test with less than what we put
            TestCase {
                to_insert: vec![10.into(), 11.into(), 12.into()],
                policy: PolicyKind::BySize(2),
                expected_outcome: Some(vec![10.into(), 11.into()]),
            },
            // BySize ttest with more than what we put
            TestCase {
                to_insert: vec![10.into(), 11.into(), 12.into()],
                policy: PolicyKind::BySize(4),
                expected_outcome: None,
            },
            // BySize ttest exact number
            TestCase {
                to_insert: vec![10.into(), 11.into(), 12.into()],
                policy: PolicyKind::BySize(4),
                expected_outcome: Some(vec![10.into(), 11.into(), 12.into()]),
            },
            // ByList test with different list
            TestCase {
                to_insert: vec![10.into(), 11.into(), 12.into()],
                policy: PolicyKind::ByList(HashSet::from([10, 9, 12])),
                expected_outcome: None,
            },
            // ByList test with same list
            TestCase {
                to_insert: vec![10.into(), 11.into(), 12.into()],
                policy: PolicyKind::ByList(HashSet::from([10, 11, 12])),
                expected_outcome: Some(vec![10.into(), 11.into(), 12.into()]),
            },
        ];
        for (i, case) in cases.into_iter().enumerate() {
            let mut batcher = PolicyBatcher::new(case.policy);
            assert!(!case.to_insert.is_empty());
            for unit in case.to_insert.iter() {
                match batcher.new_unit(unit.clone()) {
                    None => continue,
                    Some(out) => {
                        assert_eq!(case.expected_outcome, Some(out), "test case {}", i);
                        break;
                    }
                }
            }
        }
    }
}
