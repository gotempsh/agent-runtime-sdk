//! Bounded event journal and replay contract for retained runtimes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use crate::lifecycle::{InvocationId, RuntimeId};
use crate::retained::EventEnvelope;

/// Safe bounded defaults for an in-memory remote-host event tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventJournalLimits {
    /// Maximum retained runtimes.
    pub max_runtimes: usize,
    /// Maximum retained invocations per runtime.
    pub max_invocations_per_runtime: usize,
    /// Maximum retained events per invocation.
    pub max_events_per_invocation: usize,
    /// Maximum events returned by one replay request.
    pub max_replay_events: usize,
}

impl Default for EventJournalLimits {
    fn default() -> Self {
        Self {
            max_runtimes: 128,
            max_invocations_per_runtime: 256,
            max_events_per_invocation: 4_096,
            max_replay_events: 2_048,
        }
    }
}

/// Failure returned while configuring, appending, or replaying an event journal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventJournalError {
    /// A configured limit cannot provide bounded useful behavior.
    #[error("event journal limit `{field}` must be greater than zero")]
    InvalidLimit {
        /// Rejected limit field.
        field: &'static str,
    },
    /// An event uses an unsupported envelope schema.
    #[error("event schema version {version} is not supported")]
    UnsupportedSchema {
        /// Unsupported schema version.
        version: u16,
    },
    /// A first or subsequent event skipped an expected sequence.
    #[error(
        "event sequence gap for runtime {runtime_id}, invocation {invocation_id}: expected {expected}, received {actual}"
    )]
    SequenceGap {
        /// Runtime whose sequence was invalid.
        runtime_id: RuntimeId,
        /// Invocation whose sequence was invalid.
        invocation_id: InvocationId,
        /// Next expected sequence.
        expected: u64,
        /// Received sequence.
        actual: u64,
    },
    /// A sequence was reused with a different event payload.
    #[error(
        "event sequence conflict for runtime {runtime_id}, invocation {invocation_id} at sequence {sequence}"
    )]
    SequenceConflict {
        /// Runtime containing the conflict.
        runtime_id: RuntimeId,
        /// Invocation containing the conflict.
        invocation_id: InvocationId,
        /// Reused sequence.
        sequence: u64,
    },
    /// A requested replay cursor predates retained data.
    #[error(
        "replay window expired for runtime {runtime_id}, invocation {invocation_id}; earliest retained sequence is {earliest_available}"
    )]
    ReplayWindowExpired {
        /// Runtime whose replay window expired.
        runtime_id: RuntimeId,
        /// Invocation whose replay window expired.
        invocation_id: InvocationId,
        /// Earliest sequence still available, or the first sequence after a fully evicted invocation.
        earliest_available: u64,
    },
    /// A replay limit was zero or exceeded the configured maximum.
    #[error("replay limit must be between 1 and {maximum}; received {actual}")]
    InvalidReplayLimit {
        /// Configured maximum replay size.
        maximum: usize,
        /// Requested replay size.
        actual: usize,
    },
    /// Internal journal state violated an invariant instead of panicking.
    #[error("event journal state invariant failed: {message}")]
    StateInvariant {
        /// Static invariant description.
        message: &'static str,
    },
}

/// Result of appending one event envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventAppendOutcome {
    /// The event was appended.
    Appended,
    /// The identical event and sequence already existed.
    AlreadyPresent,
}

/// Cursor-based replay request for one runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReplayRequest {
    /// Runtime whose events should be replayed.
    pub runtime_id: RuntimeId,
    /// Last durably observed sequence for each invocation.
    pub after: BTreeMap<InvocationId, u64>,
    /// Optional invocation filter. An empty set intentionally replays no history.
    pub only_invocations: Option<BTreeSet<InvocationId>>,
    /// Maximum number of envelopes to return.
    pub limit: usize,
}

/// Deterministically ordered replay batch.
#[derive(Debug, Clone, PartialEq)]
pub struct EventReplayBatch {
    /// Ordered envelopes after the requested cursors.
    pub events: Vec<EventEnvelope>,
    /// Last sequence returned for every invocation represented in this batch.
    pub next: BTreeMap<InvocationId, u64>,
    /// Whether more retained events remain after this batch.
    pub truncated: bool,
}

/// Event-tail storage required by a remote runtime host.
#[async_trait]
pub trait EventJournal: Send + Sync {
    /// Appends one ordered envelope idempotently.
    async fn append(
        &self,
        envelope: EventEnvelope,
    ) -> Result<EventAppendOutcome, EventJournalError>;

    /// Replays envelopes after per-invocation durable cursors.
    async fn replay(
        &self,
        request: EventReplayRequest,
    ) -> Result<EventReplayBatch, EventJournalError>;

    /// Removes all retained events for one disposed runtime.
    async fn remove_runtime(&self, runtime_id: &RuntimeId);
}

/// Bounded process-local event journal for embedded and test hosts.
#[derive(Clone)]
pub struct InMemoryEventJournal {
    limits: EventJournalLimits,
    inner: Arc<Mutex<JournalState>>,
}

impl InMemoryEventJournal {
    /// Creates a journal after validating every bound.
    pub fn new(limits: EventJournalLimits) -> Result<Self, EventJournalError> {
        for (field, value) in [
            ("max_runtimes", limits.max_runtimes),
            (
                "max_invocations_per_runtime",
                limits.max_invocations_per_runtime,
            ),
            (
                "max_events_per_invocation",
                limits.max_events_per_invocation,
            ),
            ("max_replay_events", limits.max_replay_events),
        ] {
            if value == 0 {
                return Err(EventJournalError::InvalidLimit { field });
            }
        }
        Ok(Self {
            limits,
            inner: Arc::new(Mutex::new(JournalState::default())),
        })
    }

    fn lock(&self) -> MutexGuard<'_, JournalState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for InMemoryEventJournal {
    fn default() -> Self {
        Self {
            limits: EventJournalLimits::default(),
            inner: Arc::new(Mutex::new(JournalState::default())),
        }
    }
}

#[async_trait]
impl EventJournal for InMemoryEventJournal {
    async fn append(
        &self,
        envelope: EventEnvelope,
    ) -> Result<EventAppendOutcome, EventJournalError> {
        if envelope.schema_version != 1 {
            return Err(EventJournalError::UnsupportedSchema {
                version: envelope.schema_version,
            });
        }
        let mut state = self.lock();
        if !state.runtimes.contains_key(&envelope.runtime_id) {
            state.runtimes.insert(
                envelope.runtime_id.clone(),
                RuntimeJournal::new(self.limits.max_invocations_per_runtime),
            );
            state.runtime_order.push_back(envelope.runtime_id.clone());
            while state.runtime_order.len() > self.limits.max_runtimes {
                if let Some(expired) = state.runtime_order.pop_front() {
                    state.runtimes.remove(&expired);
                }
            }
        }
        let Some(runtime) = state.runtimes.get_mut(&envelope.runtime_id) else {
            return Err(EventJournalError::StateInvariant {
                message: "runtime disappeared before append",
            });
        };
        runtime.append(envelope, self.limits.max_events_per_invocation)
    }

    async fn replay(
        &self,
        request: EventReplayRequest,
    ) -> Result<EventReplayBatch, EventJournalError> {
        if request.limit == 0 || request.limit > self.limits.max_replay_events {
            return Err(EventJournalError::InvalidReplayLimit {
                maximum: self.limits.max_replay_events,
                actual: request.limit,
            });
        }
        let state = self.lock();
        let Some(runtime) = state.runtimes.get(&request.runtime_id) else {
            return Ok(EventReplayBatch {
                events: Vec::new(),
                next: request.after,
                truncated: false,
            });
        };
        runtime.replay(request, self.limits.max_replay_events)
    }

    async fn remove_runtime(&self, runtime_id: &RuntimeId) {
        let mut state = self.lock();
        state.runtimes.remove(runtime_id);
        state
            .runtime_order
            .retain(|candidate| candidate != runtime_id);
    }
}

#[derive(Default)]
struct JournalState {
    runtimes: HashMap<RuntimeId, RuntimeJournal>,
    runtime_order: VecDeque<RuntimeId>,
}

struct RuntimeJournal {
    invocations: HashMap<InvocationId, InvocationJournal>,
    invocation_order: VecDeque<InvocationId>,
    evicted_invocations: HashSet<InvocationId>,
    evicted_order: VecDeque<InvocationId>,
    max_invocations: usize,
}

impl RuntimeJournal {
    fn new(max_invocations: usize) -> Self {
        Self {
            invocations: HashMap::new(),
            invocation_order: VecDeque::new(),
            evicted_invocations: HashSet::new(),
            evicted_order: VecDeque::new(),
            max_invocations,
        }
    }

    fn append(
        &mut self,
        envelope: EventEnvelope,
        max_events: usize,
    ) -> Result<EventAppendOutcome, EventJournalError> {
        let runtime_id = envelope.runtime_id.clone();
        let invocation_id = envelope.invocation_id.clone();
        if self.evicted_invocations.contains(&invocation_id) {
            return Err(EventJournalError::SequenceConflict {
                runtime_id,
                invocation_id,
                sequence: envelope.sequence,
            });
        }
        if !self.invocations.contains_key(&invocation_id) {
            if envelope.sequence != 1 {
                return Err(EventJournalError::SequenceGap {
                    runtime_id,
                    invocation_id,
                    expected: 1,
                    actual: envelope.sequence,
                });
            }
            self.invocations
                .insert(invocation_id.clone(), InvocationJournal::new(max_events));
            self.invocation_order.push_back(invocation_id.clone());
            while self.invocation_order.len() > self.max_invocations {
                if let Some(expired) = self.invocation_order.pop_front() {
                    self.invocations.remove(&expired);
                    if self.evicted_invocations.insert(expired.clone()) {
                        self.evicted_order.push_back(expired);
                    }
                }
            }
            while self.evicted_order.len() > self.max_invocations {
                if let Some(expired) = self.evicted_order.pop_front() {
                    self.evicted_invocations.remove(&expired);
                }
            }
        }
        let Some(invocation) = self.invocations.get_mut(&invocation_id) else {
            return Err(EventJournalError::StateInvariant {
                message: "invocation disappeared before append",
            });
        };
        invocation.append(envelope)
    }

    fn replay(
        &self,
        request: EventReplayRequest,
        maximum: usize,
    ) -> Result<EventReplayBatch, EventJournalError> {
        for (invocation_id, cursor) in &request.after {
            if self.evicted_invocations.contains(invocation_id) {
                return Err(EventJournalError::ReplayWindowExpired {
                    runtime_id: request.runtime_id.clone(),
                    invocation_id: invocation_id.clone(),
                    earliest_available: cursor.saturating_add(1),
                });
            }
            if let Some(invocation) = self.invocations.get(invocation_id) {
                if *cursor < invocation.evicted_through {
                    return Err(EventJournalError::ReplayWindowExpired {
                        runtime_id: request.runtime_id.clone(),
                        invocation_id: invocation_id.clone(),
                        earliest_available: invocation.evicted_through.saturating_add(1),
                    });
                }
            }
        }

        let mut events = self
            .invocations
            .iter()
            .filter(|(invocation_id, _)| {
                request
                    .only_invocations
                    .as_ref()
                    .is_none_or(|selected| selected.contains(*invocation_id))
            })
            .flat_map(|(_, invocation)| invocation.events.iter())
            .filter(|event| {
                event.sequence
                    > request
                        .after
                        .get(&event.invocation_id)
                        .copied()
                        .unwrap_or(0)
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by(|left, right| {
            left.observed_at_unix_ms
                .cmp(&right.observed_at_unix_ms)
                .then_with(|| left.invocation_id.cmp(&right.invocation_id))
                .then_with(|| left.sequence.cmp(&right.sequence))
        });
        let truncated = events.len() > request.limit;
        events.truncate(request.limit.min(maximum));
        let mut next = request.after;
        for event in &events {
            next.insert(event.invocation_id.clone(), event.sequence);
        }
        Ok(EventReplayBatch {
            events,
            next,
            truncated,
        })
    }
}

struct InvocationJournal {
    events: VecDeque<EventEnvelope>,
    evicted_through: u64,
    maximum: usize,
}

impl InvocationJournal {
    fn new(maximum: usize) -> Self {
        Self {
            events: VecDeque::new(),
            evicted_through: 0,
            maximum,
        }
    }

    fn append(&mut self, envelope: EventEnvelope) -> Result<EventAppendOutcome, EventJournalError> {
        let expected = self
            .events
            .back()
            .map_or(self.evicted_through.saturating_add(1), |event| {
                event.sequence.saturating_add(1)
            });
        if envelope.sequence < expected {
            if self
                .events
                .iter()
                .find(|event| event.sequence == envelope.sequence)
                .is_some_and(|event| event == &envelope)
            {
                return Ok(EventAppendOutcome::AlreadyPresent);
            }
            return Err(EventJournalError::SequenceConflict {
                runtime_id: envelope.runtime_id,
                invocation_id: envelope.invocation_id,
                sequence: envelope.sequence,
            });
        }
        if envelope.sequence > expected {
            return Err(EventJournalError::SequenceGap {
                runtime_id: envelope.runtime_id,
                invocation_id: envelope.invocation_id,
                expected,
                actual: envelope.sequence,
            });
        }
        self.events.push_back(envelope);
        while self.events.len() > self.maximum {
            if let Some(expired) = self.events.pop_front() {
                self.evicted_through = expired.sequence;
            }
        }
        Ok(EventAppendOutcome::Appended)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retained::RuntimeEvent;
    use crate::TurnEvent;

    fn runtime_id(value: &str) -> RuntimeId {
        RuntimeId::new(value).expect("valid runtime identifier")
    }

    fn invocation_id(value: &str) -> InvocationId {
        InvocationId::new(value).expect("valid invocation identifier")
    }

    fn event(runtime: &str, invocation: &str, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            schema_version: 1,
            runtime_id: runtime_id(runtime),
            invocation_id: invocation_id(invocation),
            sequence,
            observed_at_unix_ms: sequence,
            event: RuntimeEvent::ProviderEvent {
                event: TurnEvent::TextDelta {
                    text: format!("event-{sequence}"),
                },
            },
        }
    }

    #[tokio::test]
    async fn appends_idempotently_and_rejects_gaps_or_conflicts() {
        let journal = InMemoryEventJournal::default();
        let first = event("runtime-1", "invocation-1", 1);
        assert_eq!(
            journal.append(first.clone()).await.expect("append event"),
            EventAppendOutcome::Appended
        );
        assert_eq!(
            journal.append(first).await.expect("append duplicate"),
            EventAppendOutcome::AlreadyPresent
        );
        assert!(matches!(
            journal.append(event("runtime-1", "invocation-1", 3)).await,
            Err(EventJournalError::SequenceGap {
                expected: 2,
                actual: 3,
                ..
            })
        ));
        let mut conflict = event("runtime-1", "invocation-1", 1);
        conflict.observed_at_unix_ms = 999;
        assert!(matches!(
            journal.append(conflict).await,
            Err(EventJournalError::SequenceConflict { sequence: 1, .. })
        ));
    }

    #[tokio::test]
    async fn reports_an_expired_replay_window_after_event_retention() {
        let journal = InMemoryEventJournal::new(EventJournalLimits {
            max_events_per_invocation: 2,
            ..EventJournalLimits::default()
        })
        .expect("valid journal limits");
        for sequence in 1..=3 {
            journal
                .append(event("runtime-1", "invocation-1", sequence))
                .await
                .expect("append event");
        }
        let replay = journal
            .replay(EventReplayRequest {
                runtime_id: runtime_id("runtime-1"),
                after: BTreeMap::from([(invocation_id("invocation-1"), 0)]),
                only_invocations: None,
                limit: 10,
            })
            .await;
        assert!(matches!(
            replay,
            Err(EventJournalError::ReplayWindowExpired {
                earliest_available: 2,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn replay_filters_invocations_and_reports_bounded_pagination() {
        let journal = InMemoryEventJournal::new(EventJournalLimits {
            max_replay_events: 2,
            ..EventJournalLimits::default()
        })
        .expect("valid journal limits");
        for sequence in 1..=3 {
            journal
                .append(event("runtime-filter", "selected", sequence))
                .await
                .expect("append selected event");
            journal
                .append(event("runtime-filter", "unrelated", sequence))
                .await
                .expect("append unrelated event");
        }
        let selected = invocation_id("selected");
        let first = journal
            .replay(EventReplayRequest {
                runtime_id: runtime_id("runtime-filter"),
                after: BTreeMap::from([(selected.clone(), 0)]),
                only_invocations: Some(BTreeSet::from([selected.clone()])),
                limit: 2,
            })
            .await
            .expect("first replay page");
        assert_eq!(first.events.len(), 2);
        assert!(first
            .events
            .iter()
            .all(|event| event.invocation_id == selected));
        assert!(first.truncated);

        let second = journal
            .replay(EventReplayRequest {
                runtime_id: runtime_id("runtime-filter"),
                after: first.next,
                only_invocations: Some(BTreeSet::from([selected.clone()])),
                limit: 2,
            })
            .await
            .expect("second replay page");
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].sequence, 3);
        assert!(!second.truncated);
    }

    #[tokio::test]
    async fn replays_concurrent_invocations_with_deterministic_cursors() {
        let journal = InMemoryEventJournal::new(EventJournalLimits {
            max_runtimes: 4,
            max_invocations_per_runtime: 64,
            max_events_per_invocation: 128,
            max_replay_events: 4_096,
        })
        .expect("valid journal limits");
        let mut tasks = Vec::new();
        for invocation in 0..32 {
            let journal = journal.clone();
            tasks.push(tokio::spawn(async move {
                for sequence in 1..=100 {
                    let mut envelope = event(
                        "runtime-stress",
                        &format!("invocation-{invocation:02}"),
                        sequence,
                    );
                    envelope.observed_at_unix_ms = sequence * 100 + invocation;
                    journal.append(envelope).await.expect("append stress event");
                }
            }));
        }
        for task in tasks {
            task.await.expect("stress writer task");
        }

        let replay = journal
            .replay(EventReplayRequest {
                runtime_id: runtime_id("runtime-stress"),
                after: BTreeMap::new(),
                only_invocations: None,
                limit: 4_096,
            })
            .await
            .expect("replay stress events");
        assert_eq!(replay.events.len(), 3_200);
        assert!(!replay.truncated);
        assert_eq!(replay.next.len(), 32);
        assert!(replay.next.values().all(|sequence| *sequence == 100));
        assert!(replay
            .events
            .windows(2)
            .all(|window| { window[0].observed_at_unix_ms <= window[1].observed_at_unix_ms }));
    }
}
