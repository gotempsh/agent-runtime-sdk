use crate::{
    ApprovalDecision, ApprovalRequest, InteractionHandler, QuestionAnswer, QuestionRequest,
};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::oneshot;

const DEFAULT_MAX_PENDING_INTERACTIONS: usize = 128;

/// Outcome of resolving an interaction through [`InteractionBroker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InteractionResolution {
    /// A currently waiting provider interaction received the response.
    Delivered,
    /// The response arrived just before the provider registered its waiter.
    Staged,
}

/// Failure returned while resolving an interaction through [`InteractionBroker`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InteractionBrokerError {
    /// The same interaction was resolved more than once.
    #[error("interaction `{id}` was already resolved")]
    AlreadyResolved {
        /// The provider interaction identifier.
        id: String,
    },
    /// The bounded broker has no room for an early response.
    #[error("interaction broker already holds its maximum of {maximum} pending responses")]
    CapacityExceeded {
        /// The maximum number of unresolved interactions retained by the broker.
        maximum: usize,
    },
    /// The runtime stopped waiting before the response could be delivered.
    #[error("interaction `{id}` is no longer waiting for a response")]
    Expired {
        /// The provider interaction identifier.
        id: String,
    },
}

enum Slot<T> {
    Waiting {
        generation: u64,
        sender: oneshot::Sender<T>,
    },
    Staged(T),
}

struct Registry<T> {
    slots: HashMap<String, Slot<T>>,
    resolved: HashSet<String>,
    resolved_order: VecDeque<String>,
    next_generation: u64,
    maximum: usize,
}

impl<T> Registry<T> {
    fn new(maximum: usize) -> Self {
        Self {
            slots: HashMap::new(),
            resolved: HashSet::new(),
            resolved_order: VecDeque::new(),
            next_generation: 1,
            maximum: maximum.max(1),
        }
    }

    fn mark_resolved(&mut self, id: &str) {
        if self.resolved.insert(id.to_string()) {
            self.resolved_order.push_back(id.to_string());
        }
        while self.resolved_order.len() > self.maximum {
            if let Some(expired) = self.resolved_order.pop_front() {
                self.resolved.remove(&expired);
            }
        }
    }
}

enum Registration<T> {
    Ready(T),
    Waiting {
        receiver: oneshot::Receiver<T>,
        generation: u64,
    },
    Duplicate,
}

fn lock<T>(registry: &Mutex<Registry<T>>) -> MutexGuard<'_, Registry<T>> {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn register<T>(registry: &Arc<Mutex<Registry<T>>>, id: &str) -> Registration<T> {
    let mut registry = lock(registry);
    if registry.resolved.contains(id) {
        return Registration::Duplicate;
    }
    match registry.slots.remove(id) {
        Some(Slot::Staged(value)) => {
            registry.mark_resolved(id);
            Registration::Ready(value)
        }
        Some(waiting @ Slot::Waiting { .. }) => {
            registry.slots.insert(id.to_string(), waiting);
            Registration::Duplicate
        }
        None => {
            let generation = registry.next_generation;
            registry.next_generation = registry.next_generation.saturating_add(1);
            let (sender, receiver) = oneshot::channel();
            registry
                .slots
                .insert(id.to_string(), Slot::Waiting { generation, sender });
            Registration::Waiting {
                receiver,
                generation,
            }
        }
    }
}

fn resolve<T>(
    registry: &Arc<Mutex<Registry<T>>>,
    id: &str,
    value: T,
) -> Result<InteractionResolution, InteractionBrokerError> {
    let mut registry = lock(registry);
    if registry.resolved.contains(id) {
        return Err(InteractionBrokerError::AlreadyResolved { id: id.to_string() });
    }
    match registry.slots.remove(id) {
        Some(Slot::Waiting { sender, .. }) => match sender.send(value) {
            Ok(()) => {
                registry.mark_resolved(id);
                Ok(InteractionResolution::Delivered)
            }
            Err(_) => Err(InteractionBrokerError::Expired { id: id.to_string() }),
        },
        Some(staged @ Slot::Staged(_)) => {
            registry.slots.insert(id.to_string(), staged);
            Err(InteractionBrokerError::AlreadyResolved { id: id.to_string() })
        }
        None if registry.slots.len() >= registry.maximum => {
            Err(InteractionBrokerError::CapacityExceeded {
                maximum: registry.maximum,
            })
        }
        None => {
            registry.slots.insert(id.to_string(), Slot::Staged(value));
            Ok(InteractionResolution::Staged)
        }
    }
}

struct WaitingGuard<T> {
    registry: Arc<Mutex<Registry<T>>>,
    id: String,
    generation: u64,
}

impl<T> Drop for WaitingGuard<T> {
    fn drop(&mut self) {
        let mut registry = lock(&self.registry);
        if matches!(
            registry.slots.get(&self.id),
            Some(Slot::Waiting { generation, .. }) if *generation == self.generation
        ) {
            registry.slots.remove(&self.id);
        }
    }
}

async fn wait<T>(registry: &Arc<Mutex<Registry<T>>>, id: &str) -> Option<T> {
    match register(registry, id) {
        Registration::Ready(value) => Some(value),
        Registration::Duplicate => None,
        Registration::Waiting {
            receiver,
            generation,
        } => {
            let _guard = WaitingGuard {
                registry: Arc::clone(registry),
                id: id.to_string(),
                generation,
            };
            receiver.await.ok()
        }
    }
}

/// Bounded, race-safe bridge between provider interactions and an external UI.
///
/// The runtime calls this value through [`InteractionHandler`]. An HTTP or UI
/// boundary resolves the request later through [`Self::resolve_approval`] or
/// [`Self::resolve_question`]. Responses that arrive between event publication
/// and waiter registration are staged safely. Dropped or timed-out waiters are
/// removed automatically.
///
/// Persist and authorize a response in the host application before resolving
/// it here; this broker deliberately owns coordination, not durable storage.
#[derive(Clone)]
pub struct InteractionBroker {
    approvals: Arc<Mutex<Registry<ApprovalDecision>>>,
    questions: Arc<Mutex<Registry<Option<QuestionAnswer>>>>,
}

impl Default for InteractionBroker {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PENDING_INTERACTIONS)
    }
}

impl InteractionBroker {
    /// Create a broker that retains at most `maximum` early responses per kind.
    pub fn new(maximum: usize) -> Self {
        Self {
            approvals: Arc::new(Mutex::new(Registry::new(maximum))),
            questions: Arc::new(Mutex::new(Registry::new(maximum))),
        }
    }

    /// Deliver or stage an approval decision.
    pub fn resolve_approval(
        &self,
        request_id: &str,
        decision: ApprovalDecision,
    ) -> Result<InteractionResolution, InteractionBrokerError> {
        resolve(&self.approvals, request_id, decision)
    }

    /// Deliver or stage an answer to an agent question.
    pub fn resolve_question(
        &self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<InteractionResolution, InteractionBrokerError> {
        resolve(&self.questions, request_id, Some(answer))
    }

    /// Explicitly decline a question instead of leaving the provider waiting.
    pub fn decline_question(
        &self,
        request_id: &str,
    ) -> Result<InteractionResolution, InteractionBrokerError> {
        resolve(&self.questions, request_id, None)
    }
}

#[async_trait]
impl InteractionHandler for InteractionBroker {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        wait(&self.approvals, &request.id)
            .await
            .unwrap_or_else(|| ApprovalDecision::Deny {
                reason: Some("Approval is no longer pending".to_string()),
            })
    }

    async fn answer(&self, request: QuestionRequest) -> Option<QuestionAnswer> {
        wait(&self.questions, &request.id).await.flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn question(id: &str) -> QuestionRequest {
        QuestionRequest::new(id, serde_json::json!([]))
    }

    async fn wait_for_question_waiter(broker: &InteractionBroker, id: &str) {
        for _ in 0..100 {
            if matches!(
                lock(&broker.questions).slots.get(id),
                Some(Slot::Waiting { .. })
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("question waiter `{id}` was not registered");
    }

    #[tokio::test]
    async fn delivers_a_question_answer_to_the_waiting_runtime() {
        let broker = InteractionBroker::default();
        let waiting = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.answer(question("question-1")).await })
        };
        wait_for_question_waiter(&broker, "question-1").await;

        assert_eq!(
            broker
                .resolve_question(
                    "question-1",
                    QuestionAnswer {
                        answers: serde_json::json!({"Fruit":"Banana"}),
                    },
                )
                .unwrap(),
            InteractionResolution::Delivered
        );
        assert_eq!(
            waiting.await.unwrap().unwrap().answers,
            serde_json::json!({"Fruit":"Banana"})
        );
    }

    #[tokio::test]
    async fn preserves_an_answer_that_arrives_before_waiter_registration() {
        let broker = InteractionBroker::default();
        assert_eq!(
            broker
                .resolve_question(
                    "question-early",
                    QuestionAnswer {
                        answers: serde_json::json!({"Fruit":"Plantain"}),
                    },
                )
                .unwrap(),
            InteractionResolution::Staged
        );

        let answer = broker.answer(question("question-early")).await.unwrap();
        assert_eq!(answer.answers, serde_json::json!({"Fruit":"Plantain"}));
    }

    #[tokio::test]
    async fn rejects_a_second_resolution_after_delivery() {
        let broker = InteractionBroker::default();
        let waiting = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.answer(question("question-once")).await })
        };
        wait_for_question_waiter(&broker, "question-once").await;

        broker
            .resolve_question("question-once", QuestionAnswer::selected("Fruit", "Banana"))
            .unwrap();
        assert!(waiting.await.unwrap().is_some());
        assert!(matches!(
            broker.resolve_question(
                "question-once",
                QuestionAnswer::selected("Fruit", "Plantain")
            ),
            Err(InteractionBrokerError::AlreadyResolved { .. })
        ));
    }

    #[tokio::test]
    async fn removes_a_waiter_when_the_runtime_stops_waiting() {
        let broker = InteractionBroker::default();
        let waiting = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.answer(question("question-expired")).await })
        };
        wait_for_question_waiter(&broker, "question-expired").await;
        waiting.abort();
        let _ = waiting.await;

        assert_eq!(
            broker
                .resolve_question(
                    "question-expired",
                    QuestionAnswer {
                        answers: serde_json::json!({"Fruit":"Banana"}),
                    },
                )
                .unwrap(),
            InteractionResolution::Staged
        );
    }
}
