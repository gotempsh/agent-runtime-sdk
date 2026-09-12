//! Deterministic application-owned Agent Relay integration coverage.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use serde_json::json;
use temps_agent_runtime::lifecycle::InvocationId;
use temps_agent_runtime::relay::{
    inbound_turn_input, AgentAddress, AgentAddressPattern, AgentAvailability, AgentDeliveryQuery,
    AgentDeliveryReceipt, AgentDeliveryStatus, AgentDirectoryEntry, AgentDirectoryPage,
    AgentDiscoveryGrant, AgentDiscoveryQuery, AgentIdempotencyKey, AgentMessageEnvelope,
    AgentMessageId, AgentMessageProvenance, AgentMessageRouter, AgentMessagingLimits,
    AgentReceiveGrant, AgentRelayContext, AgentRelayDeliveryState, AgentRelayError,
    AgentRelayErrorKind, AgentRelayMcpBridge, AgentRelayMetadata, AgentRelayResult,
    AgentRelayRetryAdvice, AgentSendGrant, AgentThreadId, MessagingGrant,
    ReplyToAgentMessageRequest, SendAgentMessageRequest, AGENT_MESSAGE_SCHEMA_VERSION,
    AGENT_RELAY_TOOL_REPLY, AGENT_RELAY_TOOL_SEND, AGENT_RELAY_TOOL_STATUS,
};
use temps_agent_runtime::TurnProvenance;

#[derive(Default)]
struct RelayState {
    agents: BTreeMap<AgentAddress, AgentAvailability>,
    envelopes: BTreeMap<AgentMessageId, AgentMessageEnvelope>,
    receipts: BTreeMap<AgentMessageId, AgentDeliveryReceipt>,
    idempotency: HashMap<(AgentAddress, AgentIdempotencyKey), AgentMessageId>,
    ambiguous_once: HashSet<AgentIdempotencyKey>,
    next_message: u64,
    next_thread: u64,
}

#[derive(Default)]
struct MemoryApplicationRouter {
    state: Mutex<RelayState>,
}

impl MemoryApplicationRouter {
    fn state(&self) -> MutexGuard<'_, RelayState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(&self, address: AgentAddress, availability: AgentAvailability) {
        self.state().agents.insert(address, availability);
    }

    fn envelope(&self, message_id: &AgentMessageId) -> AgentMessageEnvelope {
        self.state().envelopes[message_id].clone()
    }

    fn mark_delivered(&self, message_id: &AgentMessageId) {
        let mut state = self.state();
        let receipt = state.receipts.get_mut(message_id).unwrap();
        receipt.status = AgentDeliveryStatus::Delivered;
        receipt.attempts = receipt.attempts.saturating_add(1);
        receipt.updated_at_unix_ms = receipt.updated_at_unix_ms.saturating_add(1);
        receipt.detail = Some("recipient runtime acknowledged the inbound turn".to_owned());
    }

    fn accept_then_make_response_ambiguous(&self, key: AgentIdempotencyKey) {
        self.state().ambiguous_once.insert(key);
    }

    fn enqueue(
        &self,
        context: &AgentRelayContext,
        request: SendAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt> {
        let mut state = self.state();
        if !state.agents.contains_key(&request.recipient) {
            return Err(AgentRelayError::permanent(
                AgentRelayErrorKind::UnauthorizedRecipient,
                "the application directory does not authorize this recipient",
            ));
        }
        let index_key = (context.sender().clone(), request.idempotency_key.clone());
        if let Some(message_id) = state.idempotency.get(&index_key) {
            return Ok(state.receipts[message_id].clone());
        }

        state.next_message = state.next_message.saturating_add(1);
        let message_id = AgentMessageId::new(format!("message-{}", state.next_message)).unwrap();
        let thread_id = request.thread_id.unwrap_or_else(|| {
            state.next_thread = state.next_thread.saturating_add(1);
            AgentThreadId::new(format!("thread-{}", state.next_thread)).unwrap()
        });
        let envelope = AgentMessageEnvelope {
            schema_version: AGENT_MESSAGE_SCHEMA_VERSION,
            message_id: message_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            thread_id: thread_id.clone(),
            reply_to: request.reply_to,
            sender: context.sender().clone(),
            recipient: request.recipient,
            message: request.message,
            created_at_unix_ms: 1_000,
            ttl_seconds: request.ttl_seconds,
            hop_count: request.hop_count,
            hop_limit: request.hop_limit,
        };
        envelope.validate().unwrap();
        let receipt = AgentDeliveryReceipt {
            message_id: message_id.clone(),
            idempotency_key: request.idempotency_key.clone(),
            thread_id,
            status: AgentDeliveryStatus::Queued,
            attempts: 0,
            updated_at_unix_ms: 1_000,
            detail: Some("recipient busy; durable message remains queued".to_owned()),
        };
        state.idempotency.insert(index_key, message_id.clone());
        state.envelopes.insert(message_id.clone(), envelope);
        state.receipts.insert(message_id.clone(), receipt.clone());

        if state.ambiguous_once.remove(&request.idempotency_key) {
            return Err(AgentRelayError {
                kind: AgentRelayErrorKind::Indeterminate,
                retry: AgentRelayRetryAdvice::Reconcile,
                delivery: AgentRelayDeliveryState::PossiblyAccepted,
                message: "broker response ended after durable acceptance; reconcile first"
                    .to_owned(),
                message_id: Some(message_id),
            });
        }
        Ok(receipt)
    }
}

#[async_trait]
impl AgentMessageRouter for MemoryApplicationRouter {
    async fn discover(
        &self,
        _context: &AgentRelayContext,
        query: AgentDiscoveryQuery,
    ) -> AgentRelayResult<AgentDirectoryPage> {
        let agents = self
            .state()
            .agents
            .iter()
            .take(usize::from(query.limit))
            .map(|(address, availability)| AgentDirectoryEntry {
                address: address.clone(),
                display_name: address.to_string(),
                description: Some("deterministic integration fixture".to_owned()),
                availability: *availability,
                metadata: AgentRelayMetadata::default(),
            })
            .collect();
        Ok(AgentDirectoryPage {
            agents,
            next_cursor: None,
        })
    }

    async fn send(
        &self,
        context: &AgentRelayContext,
        request: SendAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt> {
        self.enqueue(context, request)
    }

    async fn reply(
        &self,
        context: &AgentRelayContext,
        request: ReplyToAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt> {
        let original = self
            .state()
            .envelopes
            .get(&request.in_reply_to)
            .cloned()
            .ok_or_else(|| {
                AgentRelayError::permanent(
                    AgentRelayErrorKind::MessageNotFound,
                    "reply target does not exist",
                )
            })?;
        self.enqueue(
            context,
            SendAgentMessageRequest {
                recipient: original.sender,
                idempotency_key: request.idempotency_key,
                thread_id: Some(original.thread_id),
                reply_to: Some(request.in_reply_to),
                message: request.message,
                ttl_seconds: request.ttl_seconds,
                hop_count: request.hop_count,
                hop_limit: request.hop_limit,
            },
        )
    }

    async fn delivery_status(
        &self,
        context: &AgentRelayContext,
        query: AgentDeliveryQuery,
    ) -> AgentRelayResult<AgentDeliveryReceipt> {
        let state = self.state();
        let message_id = match query {
            AgentDeliveryQuery::MessageId(message_id) => message_id,
            AgentDeliveryQuery::IdempotencyKey(key) => state
                .idempotency
                .get(&(context.sender().clone(), key))
                .cloned()
                .ok_or_else(|| {
                    AgentRelayError::permanent(
                        AgentRelayErrorKind::MessageNotFound,
                        "idempotency key does not exist for this sender",
                    )
                })?,
            _ => {
                return Err(AgentRelayError::permanent(
                    AgentRelayErrorKind::InvalidRequest,
                    "unsupported delivery query",
                ))
            }
        };
        state.receipts.get(&message_id).cloned().ok_or_else(|| {
            AgentRelayError::permanent(
                AgentRelayErrorKind::MessageNotFound,
                "message does not exist",
            )
        })
    }
}

fn address(value: &str) -> AgentAddress {
    AgentAddress::new(value).unwrap()
}

fn grant(send_prefix: &str, receive_prefix: &str) -> MessagingGrant {
    MessagingGrant {
        discovery: Some(AgentDiscoveryGrant {
            addresses: vec![AgentAddressPattern::prefix(send_prefix).unwrap()],
            max_results: 10,
        }),
        send: Some(AgentSendGrant {
            recipients: vec![AgentAddressPattern::prefix(send_prefix).unwrap()],
        }),
        receive: Some(AgentReceiveGrant {
            senders: vec![AgentAddressPattern::prefix(receive_prefix).unwrap()],
        }),
        limits: AgentMessagingLimits {
            max_messages_per_minute: None,
            ..AgentMessagingLimits::default()
        },
        ..MessagingGrant::default()
    }
}

#[tokio::test]
async fn two_projects_exchange_durable_messages_only_through_explicit_tools() {
    let router = Arc::new(MemoryApplicationRouter::default());
    let agent_a = address("tenant/project-a/agent-a");
    let agent_b = address("tenant/project-b/agent-b");
    router.register(agent_a.clone(), AgentAvailability::Online);
    router.register(agent_b.clone(), AgentAvailability::Busy);
    let grant_a = grant("tenant/project-b/", "tenant/project-b/");
    let grant_b = grant("tenant/project-a/", "tenant/project-a/");

    let bridge_a = AgentRelayMcpBridge::for_turn(
        agent_a.clone(),
        "turn-capability-a",
        grant_a,
        router.clone(),
    )
    .unwrap();
    assert_eq!(router.state().envelopes.len(), 0);
    let sent = bridge_a
        .call_tool(
            AGENT_RELAY_TOOL_SEND,
            json!({
                "recipient": agent_b,
                "idempotency_key": "a-to-b-1",
                "content": "Review the relay contract and explicitly reply."
            }),
        )
        .await;
    assert!(!sent.is_error);
    assert_eq!(sent.structured_content["status"], "queued");
    let sent_id: AgentMessageId =
        serde_json::from_value(sent.structured_content["message_id"].clone()).unwrap();

    let duplicate = bridge_a
        .call_tool(
            AGENT_RELAY_TOOL_SEND,
            json!({
                "recipient": "tenant/project-b/agent-b",
                "idempotency_key": "a-to-b-1",
                "content": "A retry may differ, but the durable idempotency record wins."
            }),
        )
        .await;
    assert_eq!(duplicate.structured_content["message_id"], sent_id.as_str());
    assert_eq!(router.state().envelopes.len(), 1);

    let envelope_for_b = router.envelope(&sent_id);
    let input_b = inbound_turn_input(
        InvocationId::new("project-b-turn-1").unwrap(),
        &envelope_for_b,
        &grant_b,
        2_000,
    )
    .unwrap();
    assert!(input_b.prompt.contains("untrusted user-level content"));
    let TurnProvenance::Agent(provenance) = input_b.provenance else {
        panic!("recipient turn must carry typed agent provenance")
    };
    assert_eq!(provenance.sender, agent_a);
    router.mark_delivered(&sent_id);

    let bridge_b = AgentRelayMcpBridge::for_inbound_turn(
        address("tenant/project-b/agent-b"),
        "turn-capability-b",
        grant_b,
        AgentMessageProvenance::from(&envelope_for_b),
        router.clone(),
    )
    .unwrap();
    assert_eq!(router.state().envelopes.len(), 1);
    let replied = bridge_b
        .call_tool(
            AGENT_RELAY_TOOL_REPLY,
            json!({
                "in_reply_to": sent_id,
                "idempotency_key": "b-to-a-1",
                "content": "Reviewed. The application still owns durability and authorization."
            }),
        )
        .await;
    assert!(!replied.is_error);
    let reply_id: AgentMessageId =
        serde_json::from_value(replied.structured_content["message_id"].clone()).unwrap();
    assert_eq!(router.state().envelopes.len(), 2);
    let reply = router.envelope(&reply_id);
    assert_eq!(reply.recipient, agent_a);
    assert_eq!(reply.reply_to.as_ref(), Some(&sent_id));
    assert_eq!(reply.hop_count, 1);

    router.mark_delivered(&reply_id);
    let final_status = bridge_b
        .call_tool(
            AGENT_RELAY_TOOL_STATUS,
            json!({"idempotency_key": "b-to-a-1"}),
        )
        .await;
    assert_eq!(final_status.structured_content["status"], "delivered");
}

#[tokio::test]
async fn ambiguous_acceptance_reconciles_without_duplicate_delivery() {
    let router = Arc::new(MemoryApplicationRouter::default());
    let agent_a = address("tenant/project-a/agent-a");
    router.register(agent_a.clone(), AgentAvailability::Online);
    router.register(
        address("tenant/project-b/agent-b"),
        AgentAvailability::Offline,
    );
    router.accept_then_make_response_ambiguous(AgentIdempotencyKey::new("ambiguous-1").unwrap());
    let bridge = AgentRelayMcpBridge::for_turn(
        agent_a,
        "ambiguous-capability",
        grant("tenant/project-b/", "tenant/project-b/"),
        router.clone(),
    )
    .unwrap();

    let ambiguous = bridge
        .call_tool(
            AGENT_RELAY_TOOL_SEND,
            json!({
                "recipient": "tenant/project-b/agent-b",
                "idempotency_key": "ambiguous-1",
                "content": "This is durably accepted before the response is lost."
            }),
        )
        .await;
    assert!(ambiguous.is_error);
    assert_eq!(
        ambiguous.structured_content["error"]["retry"]["kind"],
        "reconcile"
    );
    assert_eq!(
        ambiguous.structured_content["error"]["delivery"],
        "possibly_accepted"
    );

    let reconciled = bridge
        .call_tool(
            AGENT_RELAY_TOOL_STATUS,
            json!({"idempotency_key": "ambiguous-1"}),
        )
        .await;
    assert!(!reconciled.is_error);
    assert_eq!(reconciled.structured_content["status"], "queued");
    assert_eq!(router.state().envelopes.len(), 1);
}

#[tokio::test]
async fn unauthorized_recipient_is_a_typed_permanent_rejection() {
    let router = Arc::new(MemoryApplicationRouter::default());
    let bridge = AgentRelayMcpBridge::for_turn(
        address("tenant/project-a/agent-a"),
        "restricted-capability",
        grant("tenant/project-b/", "tenant/project-b/"),
        router,
    )
    .unwrap();
    let rejected = bridge
        .call_tool(
            AGENT_RELAY_TOOL_SEND,
            json!({
                "recipient": "tenant/project-c/agent-c",
                "idempotency_key": "unauthorized-1",
                "content": "not authorized"
            }),
        )
        .await;
    assert!(rejected.is_error);
    assert_eq!(
        rejected.structured_content["error"]["kind"],
        "unauthorized_recipient"
    );
    assert_eq!(
        rejected.structured_content["error"]["retry"]["kind"],
        "never"
    );
    assert_eq!(
        rejected.structured_content["error"]["delivery"],
        "not_accepted"
    );
}
