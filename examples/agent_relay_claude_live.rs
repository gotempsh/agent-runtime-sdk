//! Live, bounded two-project Claude verification for application-owned Agent Relay.
//!
//! Run only with an authenticated Claude Code installation:
//! `cargo run --example agent_relay_claude_live --features claude`.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use temps_agent_runtime::relay::{
    inbound_turn_request, AgentAddress, AgentAddressPattern, AgentAvailability, AgentDeliveryQuery,
    AgentDeliveryReceipt, AgentDeliveryStatus, AgentDirectoryEntry, AgentDirectoryPage,
    AgentDiscoveryGrant, AgentDiscoveryQuery, AgentMessageEnvelope, AgentMessageId,
    AgentMessageProvenance, AgentMessageRouter, AgentReceiveGrant, AgentRelayContext,
    AgentRelayError, AgentRelayErrorKind, AgentRelayMcpBridge, AgentRelayMcpExposure,
    AgentRelayMetadata, AgentRelayResult, AgentSendGrant, AgentThreadId, MessagingGrant,
    ReplyToAgentMessageRequest, SendAgentMessageRequest, AGENT_MESSAGE_SCHEMA_VERSION,
    AGENT_RELAY_TOOL_REPLY, AGENT_RELAY_TOOL_SEND, AGENT_RELAY_TOOL_STATUS,
};
use temps_agent_runtime::{
    AgentRuntime, EventSink, PermissionMode, Provider, Result, SecretString, ToolCallStatus,
    TurnEvent, TurnRequest,
};

const TOKEN_A: &str = "fixture-capability-a";
const TOKEN_B: &str = "fixture-capability-b";
const MCP_TOKEN_ENV: &str = "AGENT_RELAY_FIXTURE_CAPABILITY";
const TURN_TOKEN_ENV: &str = "AGENT_RELAY_TURN_CAPABILITY";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureCapability {
    address: AgentAddress,
    inbound_message_id: Option<AgentMessageId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureStore {
    capabilities: BTreeMap<String, FixtureCapability>,
    messages: Vec<AgentMessageEnvelope>,
    receipts: Vec<AgentDeliveryReceipt>,
}

impl FixtureStore {
    fn new(agent_a: AgentAddress, agent_b: AgentAddress) -> Self {
        Self {
            capabilities: BTreeMap::from([
                (
                    TOKEN_A.to_owned(),
                    FixtureCapability {
                        address: agent_a,
                        inbound_message_id: None,
                    },
                ),
                (
                    TOKEN_B.to_owned(),
                    FixtureCapability {
                        address: agent_b,
                        inbound_message_id: None,
                    },
                ),
            ]),
            messages: Vec::new(),
            receipts: Vec::new(),
        }
    }

    fn load(path: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    fn save(&self, path: &Path) -> std::io::Result<()> {
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(path, bytes)
    }

    fn receipt(&self, message_id: &AgentMessageId) -> Option<AgentDeliveryReceipt> {
        self.receipts
            .iter()
            .find(|receipt| &receipt.message_id == message_id)
            .cloned()
    }
}

struct FileFixtureRouter {
    store_path: PathBuf,
}

impl FileFixtureRouter {
    fn load(&self) -> AgentRelayResult<FixtureStore> {
        FixtureStore::load(&self.store_path).map_err(|error| AgentRelayError {
            kind: AgentRelayErrorKind::Unavailable,
            retry: temps_agent_runtime::relay::AgentRelayRetryAdvice::Immediate,
            delivery: temps_agent_runtime::relay::AgentRelayDeliveryState::NotAccepted,
            message: format!("could not load fixture store: {error}"),
            message_id: None,
        })
    }

    fn save(&self, store: &FixtureStore) -> AgentRelayResult<()> {
        store
            .save(&self.store_path)
            .map_err(|error| AgentRelayError {
                kind: AgentRelayErrorKind::Indeterminate,
                retry: temps_agent_runtime::relay::AgentRelayRetryAdvice::Reconcile,
                delivery: temps_agent_runtime::relay::AgentRelayDeliveryState::PossiblyAccepted,
                message: format!("could not save fixture store: {error}"),
                message_id: None,
            })
    }

    fn enqueue(
        &self,
        context: &AgentRelayContext,
        request: SendAgentMessageRequest,
    ) -> AgentRelayResult<AgentDeliveryReceipt> {
        let mut store = self.load()?;
        if let Some(existing) = store.messages.iter().find(|message| {
            message.sender == *context.sender()
                && message.idempotency_key == request.idempotency_key
        }) {
            return store.receipt(&existing.message_id).ok_or_else(|| {
                AgentRelayError::permanent(
                    AgentRelayErrorKind::Indeterminate,
                    "fixture idempotency record has no receipt",
                )
            });
        }
        let recipient_exists = store
            .capabilities
            .values()
            .any(|capability| capability.address == request.recipient);
        if !recipient_exists {
            return Err(AgentRelayError::permanent(
                AgentRelayErrorKind::UnauthorizedRecipient,
                "recipient is not in the fixture application directory",
            ));
        }
        let sequence = store.messages.len().saturating_add(1);
        let message_id = AgentMessageId::new(format!("live-message-{sequence}"))
            .expect("fixture message ID is valid");
        let thread_id = request.thread_id.unwrap_or_else(|| {
            AgentThreadId::new(format!("live-thread-{sequence}"))
                .expect("fixture thread ID is valid")
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
            created_at_unix_ms: now_unix_ms(),
            ttl_seconds: request.ttl_seconds,
            hop_count: request.hop_count,
            hop_limit: request.hop_limit,
        };
        envelope.validate().map_err(|error| {
            AgentRelayError::permanent(
                AgentRelayErrorKind::InvalidRequest,
                format!("invalid fixture envelope: {error}"),
            )
        })?;
        let receipt = AgentDeliveryReceipt {
            message_id,
            idempotency_key: request.idempotency_key,
            thread_id,
            status: AgentDeliveryStatus::Queued,
            attempts: 0,
            updated_at_unix_ms: now_unix_ms(),
            detail: Some("durably queued by the live fixture application".to_owned()),
        };
        store.messages.push(envelope);
        store.receipts.push(receipt.clone());
        self.save(&store)?;
        Ok(receipt)
    }
}

#[async_trait]
impl AgentMessageRouter for FileFixtureRouter {
    async fn discover(
        &self,
        _context: &AgentRelayContext,
        query: AgentDiscoveryQuery,
    ) -> AgentRelayResult<AgentDirectoryPage> {
        let store = self.load()?;
        let agents = store
            .capabilities
            .values()
            .take(usize::from(query.limit))
            .map(|capability| AgentDirectoryEntry {
                address: capability.address.clone(),
                display_name: capability.address.to_string(),
                description: Some("live two-project Claude fixture".to_owned()),
                availability: AgentAvailability::Online,
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
            .load()?
            .messages
            .into_iter()
            .find(|message| message.message_id == request.in_reply_to)
            .ok_or_else(|| {
                AgentRelayError::permanent(
                    AgentRelayErrorKind::MessageNotFound,
                    "reply target does not exist in the fixture",
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
        let store = self.load()?;
        match query {
            AgentDeliveryQuery::MessageId(message_id) => store.receipt(&message_id),
            AgentDeliveryQuery::IdempotencyKey(key) => store
                .messages
                .iter()
                .find(|message| {
                    message.sender == *context.sender() && message.idempotency_key == key
                })
                .and_then(|message| store.receipt(&message.message_id)),
            _ => None,
        }
        .ok_or_else(|| {
            AgentRelayError::permanent(
                AgentRelayErrorKind::MessageNotFound,
                "delivery record does not exist in the fixture",
            )
        })
    }
}

struct Transcript(&'static str);

#[async_trait]
impl EventSink for Transcript {
    async fn emit(&self, event: TurnEvent) -> Result<()> {
        if let TurnEvent::ToolCall {
            name,
            status,
            output,
            error,
            ..
        } = event
        {
            let outcome = match status {
                ToolCallStatus::Started => "started",
                ToolCallStatus::Succeeded => output.as_deref().unwrap_or("succeeded"),
                ToolCallStatus::Failed => error.as_deref().unwrap_or("failed"),
                _ => "updated",
            };
            println!("{} tool {name}: {outcome}", self.0);
        }
        Ok(())
    }
}

fn fixture_grant() -> MessagingGrant {
    let fixture_scope = AgentAddressPattern::prefix("fixture/").expect("valid fixture scope");
    MessagingGrant {
        discovery: Some(AgentDiscoveryGrant {
            addresses: vec![fixture_scope.clone()],
            max_results: 10,
        }),
        send: Some(AgentSendGrant {
            recipients: vec![fixture_scope.clone()],
        }),
        receive: Some(AgentReceiveGrant {
            senders: vec![fixture_scope],
        }),
        ..MessagingGrant::default()
    }
}

fn configure_relay(
    request: &mut TurnRequest,
    executable: &Path,
    store_path: &Path,
    capability_token: &str,
    tools: &[&str],
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let exposure = AgentRelayMcpExposure::stdio(
        executable,
        vec![
            "mcp-server".to_owned(),
            store_path.to_string_lossy().into_owned(),
        ],
        BTreeMap::from([(MCP_TOKEN_ENV.to_owned(), TURN_TOKEN_ENV.to_owned())]),
    )?;
    exposure.install(&mut request.launch_context)?;
    request.launch_context.allowed_tools = Some(
        tools
            .iter()
            .map(|tool| exposure.claude_tool_name(tool))
            .collect(),
    );
    request.launch_context.strict_mcp_config = true;
    request.environment.insert(
        TURN_TOKEN_ENV.to_owned(),
        SecretString::new(capability_token),
    );
    request.permission_mode = PermissionMode::FullAccess;
    request.max_turns = Some(6);
    request.timeout = Duration::from_secs(180);
    Ok(())
}

fn mark_delivered(store_path: &Path, message_id: &AgentMessageId) -> std::io::Result<()> {
    let mut store = FixtureStore::load(store_path)?;
    let receipt = store
        .receipts
        .iter_mut()
        .find(|receipt| &receipt.message_id == message_id)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "missing receipt"))?;
    receipt.status = AgentDeliveryStatus::Delivered;
    receipt.attempts = receipt.attempts.saturating_add(1);
    receipt.updated_at_unix_ms = now_unix_ms();
    receipt.detail = Some("recipient Claude turn acknowledged by fixture host".to_owned());
    store.save(store_path)
}

async fn run_mcp_server(
    store_path: PathBuf,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var(MCP_TOKEN_ENV)
        .map_err(|_| format!("{MCP_TOKEN_ENV} was not supplied by the host capability"))?;
    let store = FixtureStore::load(&store_path)?;
    let capability = store
        .capabilities
        .get(&token)
        .cloned()
        .ok_or("host capability token is not authorized")?;
    let router: Arc<dyn AgentMessageRouter> = Arc::new(FileFixtureRouter {
        store_path: store_path.clone(),
    });
    let grant = fixture_grant();
    let bridge = if let Some(message_id) = capability.inbound_message_id {
        let envelope = store
            .messages
            .iter()
            .find(|message| message.message_id == message_id)
            .ok_or("inbound fixture message does not exist")?;
        AgentRelayMcpBridge::for_inbound_turn(
            capability.address,
            "live-inbound-capability",
            grant,
            AgentMessageProvenance::from(envelope),
            router,
        )?
    } else {
        AgentRelayMcpBridge::for_turn(
            capability.address,
            "live-outbound-capability",
            grant,
            router,
        )?
    };

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        let request: Value = serde_json::from_str(&line)?;
        if let Some(response) = bridge.handle_json_rpc(request).await {
            writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
            stdout.flush()?;
        }
    }
    Ok(())
}

async fn run_live() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let executable = std::env::current_exe()?;
    let root = tempfile::tempdir()?;
    let project_a = root.path().join("project-a");
    let project_b = root.path().join("project-b");
    std::fs::create_dir_all(&project_a)?;
    std::fs::create_dir_all(&project_b)?;
    let store_path = root.path().join("relay-store.json");
    let agent_a = AgentAddress::new("fixture/project-a/agent-a")?;
    let agent_b = AgentAddress::new("fixture/project-b/agent-b")?;
    FixtureStore::new(agent_a.clone(), agent_b.clone()).save(&store_path)?;

    let runtime = AgentRuntime::builder().build()?;
    let mut request_a = TurnRequest::new(
        Provider::Claude,
        &project_a,
        format!(
            "Call {AGENT_RELAY_TOOL_SEND} exactly once to recipient `{agent_b}` with idempotency_key `live-a-to-b-1`. Send this content: `Review this relay and explicitly reply using {AGENT_RELAY_TOOL_REPLY}.` Then call {AGENT_RELAY_TOOL_STATUS} with that idempotency key. Do not attempt any other work."
        ),
    );
    configure_relay(
        &mut request_a,
        &executable,
        &store_path,
        TOKEN_A,
        &[AGENT_RELAY_TOOL_SEND, AGENT_RELAY_TOOL_STATUS],
    )?;
    runtime.run(request_a, &Transcript("agent-a"), None).await?;

    let mut store = FixtureStore::load(&store_path)?;
    if store.messages.len() != 1 {
        return Err(format!(
            "Agent A produced {} durable messages; expected exactly one",
            store.messages.len()
        )
        .into());
    }
    let outbound = store.messages[0].clone();
    mark_delivered(&store_path, &outbound.message_id)?;
    store = FixtureStore::load(&store_path)?;
    store
        .capabilities
        .get_mut(TOKEN_B)
        .expect("fixture B capability exists")
        .inbound_message_id = Some(outbound.message_id.clone());
    store.save(&store_path)?;

    let receive_grant = fixture_grant();
    let mut request_b = inbound_turn_request(
        Provider::Claude,
        &project_b,
        &outbound,
        &receive_grant,
        now_unix_ms(),
    )?;
    configure_relay(
        &mut request_b,
        &executable,
        &store_path,
        TOKEN_B,
        &[AGENT_RELAY_TOOL_REPLY, AGENT_RELAY_TOOL_STATUS],
    )?;
    runtime.run(request_b, &Transcript("agent-b"), None).await?;

    let store = FixtureStore::load(&store_path)?;
    if store.messages.len() != 2 {
        return Err(format!(
            "Agent B left {} total durable messages; expected A's message plus one reply",
            store.messages.len()
        )
        .into());
    }
    let reply = &store.messages[1];
    if reply.recipient != agent_a || reply.reply_to.as_ref() != Some(&outbound.message_id) {
        return Err("Agent B reply did not preserve recipient/reply correlation".into());
    }
    mark_delivered(&store_path, &reply.message_id)?;
    let final_store = FixtureStore::load(&store_path)?;
    for receipt in final_store.receipts {
        println!(
            "host receipt {} thread={} status={:?} attempts={}",
            receipt.message_id, receipt.thread_id, receipt.status, receipt.attempts
        );
    }
    println!(
        "live relay verified across {} and {}",
        project_a.display(),
        project_b.display()
    );
    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() == Some(std::ffi::OsStr::new("mcp-server")) {
        let store_path = arguments
            .next()
            .map(PathBuf::from)
            .ok_or("mcp-server mode requires a store path")?;
        return run_mcp_server(store_path).await;
    }
    run_live().await
}
