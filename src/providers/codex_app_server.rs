//! Bidirectional Codex transport built on `codex app-server`.
//!
//! `codex exec --json` streams a turn one way: the process decides everything
//! itself and the SDK can only read. `codex app-server` speaks JSON-RPC 2.0
//! over stdio in both directions, which is what live approvals and user
//! questions need. The mapping here keeps the normalized event stream at
//! parity with the `exec --json` adapter, so an application can switch
//! [`super::codex::CodexTurnMode`] without re-teaching its UI a second
//! vocabulary.
//!
//! The protocol shapes below were taken from
//! `codex app-server generate-json-schema --experimental` (codex-cli 0.155.1):
//! `ThreadStartParams`, `TurnStartParams`, `ServerNotification`,
//! `ServerRequest`, `ToolRequestUserInputParams` and
//! `ToolRequestUserInputResponse`.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::adapter::{AdapterOutput, AdapterState, IdleFrame, InteractionRequest};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    ApprovalDecision, ApprovalRequest, CompactionTrigger, ContextCompaction, Provider,
    ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result, RunStatus, RuntimeError,
    ToolCallStatus, TurnEvent, TurnRequest, Usage,
};

/// Key under which this transport keeps its per-turn protocol state.
const STATE_KEY: &str = "codex.app_server";

/// Locally generated JSON-RPC request identifiers. The app server echoes them
/// back on responses, which is how [`parse_line`] advances the handshake.
const ID_INITIALIZE: u64 = 1;
const ID_THREAD: u64 = 2;
const ID_FORK: u64 = 3;
const ID_TURN: u64 = 4;
/// Written only by [`interrupt`], never awaited by the parser.
const ID_INTERRUPT: u64 = 5;
/// Written only by [`keepalive_probe`] between turns.
const ID_KEEPALIVE: &str = "sdk-idle-keepalive";
/// Prefix of every request id the session-title flow writes. The rest of the
/// id names the flow and the step, so a late answer for an abandoned flow can
/// neither advance a newer one nor be mistaken for a keepalive answer.
const TITLE_ID_PREFIX: &str = "sdk-title-";

/// Model Codex's own TUI titles threads with (`codex-rs/tui/src/app/thread_title.rs`).
const TITLE_MODEL: &str = "gpt-5.6-luna";
/// Longest title the TUI asks for and keeps.
const TITLE_MAX_CHARS: usize = 36;
/// Byte bound of the whole title prompt, instructions included.
const TITLE_PROMPT_MAX_BYTES: usize = 960;
/// Longest structured answer the hidden title turn may return.
const TITLE_RESPONSE_MAX_BYTES: usize = 8 * 1024;
/// `ThreadSource::Feature("thread_title")`, which serializes as its name.
const TITLE_THREAD_SOURCE: &str = "thread_title";
/// `codex_protocol::models::BUILT_IN_PERMISSION_PROFILE_READ_ONLY`.
const READ_ONLY_PERMISSION_PROFILE: &str = ":read-only";

/// Reply sent for a question the turn did not wait on. An empty answer set
/// reads to the model as "the user chose nothing"; this says what actually
/// happened so the turn can keep making progress.
const ASYNC_QUESTION_NOTE: &str = "The user has not answered yet. Their reply will arrive later as a follow-up message; continue with work that does not depend on it.";

/// Reply sent when an application declines, drops, or times out a blocking
/// question. Distinct from [`ASYNC_QUESTION_NOTE`]: no answer is coming.
const DECLINED_QUESTION_NOTE: &str =
    "The user did not answer this question. Continue without it, or state what you need.";

const MAX_TOOL_OUTPUT_CHARS: usize = 4096;

/// Per-turn protocol state retained between output lines.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TurnState {
    /// `thread/start` or `thread/resume` parameters.
    thread_params: Value,
    /// `turn/start` parameters; `threadId` is filled in once known.
    turn_params: Value,
    /// Whether this turn resumes an existing Codex thread.
    resume: bool,
    retained: bool,
    /// Model requested for this turn, used to label context-window usage
    /// before the app server reports the thread's resolved model.
    model: Option<String>,
    /// Thread identifier reported by the app server.
    thread_id: Option<String>,
    /// Turn identifier required by `turn/interrupt`.
    turn_id: Option<String>,
    /// Item identifiers that already streamed incremental text, so the
    /// consolidated `item/completed` copy is not emitted twice.
    streamed: Vec<String>,
    /// Last `error` notification, surfaced when the turn ends badly.
    error_message: Option<String>,
    /// `contextCompaction` item currently in progress.
    open_compaction: Option<String>,
    /// A `contextCompaction` item completed during this turn, so the
    /// deprecated `thread/compacted` notification must not repeat it.
    saw_compaction_item: bool,
    /// Latest active-context occupancy, the pre-compaction size when a
    /// compaction starts.
    last_context_tokens: Option<u64>,
    /// This invocation compacts the thread (`thread/compact/start`) instead of
    /// starting a turn.
    manual_compaction: bool,
    /// Hidden thread that titles a new session, when one was requested.
    title: Option<TitleFlow>,
}

/// Where the session-title flow is. Every step waits on exactly one answer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum TitleStep {
    /// Waiting for the user's thread to open.
    #[default]
    Waiting,
    /// `config/read`, to learn which MCP servers to disable.
    ReadingConfig,
    /// `thread/start` of the hidden thread.
    StartingThread,
    /// `turn/start` on the hidden thread.
    StartingTurn,
    /// The hidden turn is generating the title.
    Running,
    /// Finished, failed, or abandoned. Nothing more is written.
    Done,
}

/// Codex's TUI titles a new thread from a hidden, ephemeral, read-only thread
/// with every tool disabled, then names the visible thread with the result
/// (`codex-rs/tui/src/app/thread_title.rs`, `temporary_structured_request.rs`).
/// This replays it on the turn's own app-server connection. It runs alongside
/// the user's turn and never delays or fails it: every error ends the flow
/// silently, and a turn that ends first abandons it.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct TitleFlow {
    /// Distinguishes this flow's request ids from every other flow's.
    nonce: String,
    /// The bounded prompt, built exactly like the TUI's `thread_title_prompt`.
    prompt: String,
    cwd: String,
    /// MCP servers the hidden thread must not start.
    mcp_servers: Vec<String>,
    /// Model of the user's thread, the fallback when the title model fails.
    main_model: Option<String>,
    /// Model provider of the user's thread.
    model_provider: Option<String>,
    /// Model of the current attempt; `None` is the server's default model.
    model: Option<String>,
    /// The single fallback attempt was already used.
    retried: bool,
    step: TitleStep,
    /// Hidden thread of the current attempt.
    thread_id: Option<String>,
    /// Every hidden thread this flow opened, so frames of an abandoned
    /// attempt stay out of the user's turn too.
    hidden_threads: Vec<String>,
    /// Hidden turn of the current attempt.
    turn_id: Option<String>,
    /// Latest final agent message of the hidden turn.
    response: Option<String>,
    /// The user's thread was named some other way meanwhile; that name wins.
    superseded: bool,
}

pub(super) fn mark_retained(state: &mut AdapterState) {
    let mut turn = load(state);
    turn.retained = true;
    store(state, &turn);
}

fn load(state: &AdapterState) -> TurnState {
    state
        .extensions
        .get(STATE_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

fn store(state: &mut AdapterState, turn: &TurnState) {
    if let Ok(value) = serde_json::to_value(turn) {
        state.extensions.insert(STATE_KEY.to_string(), value);
    }
}

fn protocol(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Protocol {
        provider: Provider::Codex,
        message: message.into(),
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| protocol(format!("could not encode a Codex request: {error}")))
}

fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn reply(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

/// Frames written to stdin before the first output line: the capability
/// handshake that unlocks the experimental `item/tool/requestUserInput`
/// server request.
pub(super) fn handshake() -> Vec<u8> {
    [
        request(
            ID_INITIALIZE,
            "initialize",
            json!({
                "clientInfo": {
                    "name": "temps-agent-runtime",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {"experimentalApi": true},
            }),
        ),
        json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}),
    ]
    .iter()
    .map(ToString::to_string)
    .collect::<Vec<_>>()
    .join("\n")
    .into_bytes()
}

/// Build the thread and turn parameters this transport sends once the app
/// server accepts the handshake.
pub(super) fn prepare_turn(request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
    super::refuse_compaction_instructions(Provider::Codex, &request.prompt)?;
    let controls = super::codex::resolve_controls(request)?;
    let (approval, collaboration, sandbox) =
        controls.unwrap_or(("never", "default", "danger-full-access"));
    let mut thread_params = json!({
        "cwd": request.working_directory.to_string_lossy(),
        "sandbox": sandbox,
        "approvalPolicy": approval,
    });
    if let Some(instructions) = request.launch_context.system_prompt_append.as_deref() {
        thread_params["developerInstructions"] = json!(instructions);
    }
    if let Some(model) = request.model.as_deref() {
        thread_params["model"] = json!(model);
    }
    if let Some(tier) = request.harness_options.get("service_tier") {
        thread_params["serviceTier"] = json!(tier);
    }
    if let Some(resumed) = request.session_id.as_deref() {
        thread_params["threadId"] = json!(resumed);
        // Only metadata is needed to start the next turn. Returning the entire
        // history can exceed the event frame limit on long conversations.
        // The same parameters also cover the active-writer fork fallback.
        thread_params["excludeTurns"] = json!(true);
    }

    // Image attachments become native `localImage` user inputs; every other
    // attachment was already described in the rendered prompt text.
    let mut input = vec![json!({"type": "text", "text": request.prompt})];
    for path in super::codex::image_attachment_paths(request)? {
        input.push(json!({"type": "localImage", "path": path}));
    }
    let mut turn_params = json!({
        "input": input,
        "summary": "detailed",
    });
    if let Some(reasoning) = request.reasoning.as_deref() {
        turn_params["effort"] = json!(reasoning);
    }
    if let Some(tier) = request.harness_options.get("service_tier") {
        turn_params["serviceTier"] = json!(tier);
    }
    // `CollaborationMode.settings.model` is required by the protocol, so plan
    // mode can only be requested natively when the caller pinned a model.
    // Otherwise the read-only sandbox resolved above still keeps the turn from
    // editing the workspace.
    if collaboration == "plan" {
        if let Some(model) = request.model.as_deref() {
            turn_params["collaborationMode"] = json!({
                "mode": "plan",
                "settings": {
                    "model": model,
                    "reasoning_effort": request.reasoning.as_deref().unwrap_or("medium"),
                },
            });
        }
    }

    let manual_compaction = super::is_manual_compaction_prompt(&request.prompt);
    let title = request
        .request_session_title
        .as_deref()
        .filter(|_| !request.continuation && !manual_compaction)
        .map(str::trim)
        .filter(|description| !description.is_empty())
        .map(|description| TitleFlow {
            nonce: title_nonce(),
            prompt: title_prompt(description),
            cwd: request.working_directory.to_string_lossy().into_owned(),
            mcp_servers: request.launch_context.mcp_servers.keys().cloned().collect(),
            ..TitleFlow::default()
        });
    store(
        state,
        &TurnState {
            thread_params,
            turn_params,
            resume: request.session_id.is_some(),
            model: request.model.clone(),
            manual_compaction,
            title,
            ..TurnState::default()
        },
    );
    Ok(())
}

/// Encode `turn/interrupt` for a turn the app server already acknowledged.
pub(super) fn interrupt(state: &AdapterState) -> Option<Vec<u8>> {
    let turn = load(state);
    let (thread_id, turn_id) = (turn.thread_id?, turn.turn_id?);
    encode(&request(
        ID_INTERRUPT,
        "turn/interrupt",
        json!({"threadId": thread_id, "turnId": turn_id}),
    ))
    .ok()
}

/// Start a turn after this app-server connection has already initialized.
pub(super) fn retained_turn_start(state: &AdapterState) -> Result<Vec<u8>> {
    let turn = load(state);
    let method = if turn.resume {
        "thread/resume"
    } else {
        "thread/start"
    };
    encode(&request(ID_THREAD, method, turn.thread_params))
}

/// Ask an idle app server whether `thread_id` still runs background terminals.
///
/// Background terminals are children of the app server and die with it, so a
/// retained process that owns one must outlive its idle timeout. The string id
/// never collides with the numeric ids [`parse_response`] advances on.
pub(super) fn keepalive_probe(thread_id: &str) -> Option<Vec<u8>> {
    encode(&json!({
        "jsonrpc": "2.0",
        "id": ID_KEEPALIVE,
        "method": "thread/backgroundTerminals/list",
        "params": {"threadId": thread_id, "limit": 1},
    }))
    .ok()
}

/// Classify a frame the app server sent while no turn is running.
///
/// Notifications carry output and exit status from background terminals and
/// are harmless. A server request cannot be answered outside a turn.
pub(super) fn classify_idle_frame(line: &str) -> IdleFrame {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return IdleFrame::Unexpected;
    };
    let has_method = value.get("method").is_some();
    match value.get("id") {
        None if has_method => IdleFrame::Background,
        Some(id) if !has_method && id.as_str() == Some(ID_KEEPALIVE) => {
            IdleFrame::KeepaliveResult {
                active: value
                    .pointer("/result/data")
                    .and_then(Value::as_array)
                    .is_some_and(|terminals| !terminals.is_empty()),
            }
        }
        // A session-title flow the finished turn abandoned can still be
        // answered. It never owes the process anything.
        Some(id)
            if !has_method
                && id
                    .as_str()
                    .is_some_and(|id| id.starts_with(TITLE_ID_PREFIX)) =>
        {
            IdleFrame::Background
        }
        _ => IdleFrame::Unexpected,
    }
}

/// Translate one JSON-RPC message from the app server.
pub(super) fn parse_line(line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
    let value: Value = serde_json::from_str(line)
        .map_err(|error| protocol(format!("invalid JSON-RPC message: {error}")))?;
    let mut turn = load(state);
    let mut output = AdapterOutput::default();
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    // The hidden title thread shares this connection. Nothing it sends may
    // reach the user's turn: not its text, usage, errors, nor its end.
    if title_frame(&value, &method, &mut turn, state, &mut output) {
        store(state, &turn);
        return Ok(output);
    }
    // A rename names its thread but no turn, so a retained connection would
    // otherwise drop it as uncorrelated.
    if method == "thread/name/updated" && value.get("id").is_none() {
        notification(&value, &method, &mut turn, state, &mut output);
        store(state, &turn);
        return Ok(output);
    }
    // `thread/compact/start` answers with an empty result; the compaction's
    // turn id first appears on `turn/started`, so a manual compaction adopts
    // it there instead of discarding its own turn as uncorrelated.
    if turn.manual_compaction
        && turn.turn_id.is_none()
        && method == "turn/started"
        && turn.thread_id.is_some()
        && value.pointer("/params/threadId").and_then(Value::as_str) == turn.thread_id.as_deref()
    {
        if let Some(started) = reported_turn_id(&value) {
            turn.turn_id = Some(started.to_string());
            store(state, &turn);
        }
    }
    // Older app servers report a finished compaction only through
    // `thread/compacted`, which names the thread but no turn.
    let own_thread_compacted = turn.manual_compaction
        && method == "thread/compacted"
        && turn.thread_id.is_some()
        && value.pointer("/params/threadId").and_then(Value::as_str) == turn.thread_id.as_deref();
    if own_thread_compacted {
        notification(&value, &method, &mut turn, state, &mut output);
        store(state, &turn);
        return Ok(output);
    }
    if turn.retained
        && !method.is_empty()
        && value.get("id").is_none()
        && reported_turn_id(&value).is_none()
    {
        // Uncorrelated notifications cannot be assigned to an invocation on a
        // reused connection. Process-global state is refreshed explicitly by
        // its dedicated APIs instead of leaking into the active turn stream.
        return Ok(output);
    }
    if turn.retained
        && reported_turn_id(&value)
            .is_some_and(|reported| turn.turn_id.as_deref() != Some(reported))
    {
        return Ok(output);
    }
    if turn_scoped_method(&method) && !belongs_to_active_turn(&value, &turn) {
        // App-server connections can emit delayed frames after a completed
        // turn. A retained connection must never project those frames into a
        // later invocation's fresh parser state.
        return Ok(output);
    }
    if method.is_empty() {
        parse_response(&value, &mut turn, state, &mut output)?;
    } else if value.get("id").is_some() {
        server_request(&value, &method, &mut turn, &mut output)?;
    } else {
        notification(&value, &method, &mut turn, state, &mut output);
    }
    if output.terminal {
        // The title is lost for this turn; the next one asks again. Detach a
        // hidden thread a retained connection would otherwise keep loaded.
        if let Some(flow) = turn.title.as_mut() {
            flow.abandon(&mut output);
        }
    }
    store(state, &turn);
    Ok(output)
}

fn turn_scoped_method(method: &str) -> bool {
    matches!(
        method,
        "item/agentMessage/delta"
            | "item/reasoning/textDelta"
            | "item/reasoning/summaryTextDelta"
            | "item/started"
            | "item/completed"
            | "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "item/tool/requestUserInput"
            | "thread/tokenUsage/updated"
            | "thread/compacted"
            | "turn/completed"
            | "turn/failed"
    )
}

fn belongs_to_active_turn(value: &Value, turn: &TurnState) -> bool {
    if !turn.retained {
        return true;
    }
    let reported = reported_turn_id(value);
    matches!((reported, turn.turn_id.as_deref()), (Some(reported), Some(current)) if reported == current)
}

fn reported_turn_id(value: &Value) -> Option<&str> {
    value
        .pointer("/params/turnId")
        .or_else(|| value.pointer("/params/turn/id"))
        .and_then(Value::as_str)
}

/// Route a frame that belongs to the session-title flow, returning whether it
/// did. A routed frame never reaches the user's turn.
fn title_frame(
    value: &Value,
    method: &str,
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
) -> bool {
    let real_thread = turn.thread_id.clone();
    if method.is_empty() {
        let Some(id) = value
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| id.starts_with(TITLE_ID_PREFIX))
        else {
            return false;
        };
        // An answer for a flow this turn does not own is dropped too.
        if let Some(flow) = turn.title.as_mut() {
            flow.on_response(id, value, output);
        }
        return true;
    }
    let Some(flow) = turn.title.as_mut() else {
        return false;
    };
    let reported = value
        .pointer("/params/threadId")
        .or_else(|| value.pointer("/params/thread/id"))
        .and_then(Value::as_str);
    if !reported.is_some_and(|reported| flow.hidden_threads.iter().any(|own| own == reported)) {
        return false;
    }
    if let Some(id) = value.get("id") {
        // The hidden thread has no tools and no user to ask; whatever it
        // requests is refused so it cannot stall.
        push_frame(
            output,
            &json!({"jsonrpc": "2.0", "id": id, "error": {
                "code": -32000,
                "message": "The session-title thread cannot request approvals or input.",
            }}),
        );
    } else {
        flow.on_notification(method, value, real_thread.as_deref(), state, output);
    }
    true
}

impl TitleFlow {
    /// Request id for one step of the current attempt.
    fn id(&self, step: &str) -> String {
        let attempt = if self.retried { 2 } else { 1 };
        format!("{TITLE_ID_PREFIX}{}-{attempt}-{step}", self.nonce)
    }

    fn send(&self, output: &mut AdapterOutput, step: &str, method: &str, params: Value) {
        push_frame(
            output,
            &json!({"jsonrpc": "2.0", "id": self.id(step), "method": method, "params": params}),
        );
    }

    /// Read the effective configuration for the MCP servers to disable, like
    /// the TUI does before every hidden thread.
    fn read_config(&mut self, output: &mut AdapterOutput) {
        self.step = TitleStep::ReadingConfig;
        self.send(
            output,
            "config",
            "config/read",
            json!({"includeLayers": false, "cwd": self.cwd}),
        );
    }

    /// `thread/start` an ephemeral, read-only thread with every tool, MCP
    /// server and extension disabled (`start_temporary_thread`).
    fn start_thread(&mut self, output: &mut AdapterOutput) {
        self.step = TitleStep::StartingThread;
        self.thread_id = None;
        self.turn_id = None;
        self.response = None;
        let mut config = Map::new();
        for feature in [
            "apps",
            "code_mode",
            "code_mode_only",
            "context_management",
            "current_time_reminder",
            "deferred_executor",
            "enable_fanout",
            "goals",
            "hooks",
            "image_generation",
            "memories",
            "multi_agent",
            "multi_agent_v2",
            "plugins",
            "request_permissions_tool",
            "shell_snapshot",
            "shell_tool",
            "standalone_web_search",
            "token_budget",
            "tool_suggest",
            "unified_exec",
            "view_image",
        ] {
            config.insert(format!("features.{feature}"), json!(false));
        }
        for key in [
            "cloud.skills.enabled",
            "skills.include_instructions",
            "tools.experimental_request_user_input.enabled",
            "tools.update_plan.enabled",
        ] {
            config.insert(key.to_string(), json!(false));
        }
        config.insert("web_search".to_string(), json!("disabled"));
        config.insert(
            "default_permissions".to_string(),
            json!(READ_ONLY_PERMISSION_PROFILE),
        );
        // One dotted key per server merges `enabled = false` into the
        // server's own definition. A nested `mcp_servers` object replaces it
        // instead, which Codex refuses ("invalid transport") for a server
        // that only a `--config` override on the app-server defines, such as
        // the ones an application launches it with.
        if self
            .mcp_servers
            .iter()
            .all(|name| is_plain_server_name(name))
        {
            for name in &self.mcp_servers {
                config.insert(format!("mcp_servers.{name}.enabled"), json!(false));
            }
        } else {
            config.insert(
                "mcp_servers".to_string(),
                Value::Object(
                    self.mcp_servers
                        .iter()
                        .map(|name| (name.clone(), json!({"enabled": false})))
                        .collect(),
                ),
            );
        }
        let mut params = json!({
            "cwd": self.cwd,
            "sandbox": "read-only",
            // Nothing on this connection can answer the hidden thread.
            "approvalPolicy": "never",
            "ephemeral": true,
            "threadSource": TITLE_THREAD_SOURCE,
            "environments": [],
            "dynamicTools": [],
            "runtimeWorkspaceRoots": [],
            "selectedCapabilityRoots": [],
            "config": config,
        });
        if let Some(model) = &self.model {
            params["model"] = json!(model);
        }
        if let Some(provider) = &self.model_provider {
            params["modelProvider"] = json!(provider);
        }
        self.send(output, "thread", "thread/start", params);
    }

    /// `turn/start` the structured title request (`start_structured_turn`).
    fn start_turn(&mut self, output: &mut AdapterOutput) {
        self.step = TitleStep::StartingTurn;
        let mut params = json!({
            "threadId": self.thread_id,
            "input": [{"type": "text", "text": self.prompt}],
            "outputSchema": title_output_schema(),
        });
        if self.model.as_deref() == Some(TITLE_MODEL) {
            params["effort"] = json!("low");
        }
        self.send(output, "turn", "turn/start", params);
    }

    /// Try once more on the user's own model when the title model failed,
    /// otherwise give up silently.
    fn retry_or_finish(&mut self, output: &mut AdapterOutput) {
        self.detach(output);
        if !self.retried
            && self.model.as_deref() == Some(TITLE_MODEL)
            && self.main_model.as_deref() != Some(TITLE_MODEL)
        {
            self.retried = true;
            self.model = self.main_model.clone();
            self.start_thread(output);
        } else {
            self.step = TitleStep::Done;
        }
    }

    /// Best-effort `thread/unsubscribe` of the current hidden thread.
    fn detach(&mut self, output: &mut AdapterOutput) {
        if let Some(thread) = self.thread_id.take() {
            self.send(
                output,
                "unsubscribe",
                "thread/unsubscribe",
                json!({"threadId": thread}),
            );
        }
    }

    /// Stop because the user's turn ended first.
    fn abandon(&mut self, output: &mut AdapterOutput) {
        if self.step != TitleStep::Done {
            self.detach(output);
            self.step = TitleStep::Done;
        }
    }

    fn on_response(&mut self, id: &str, value: &Value, output: &mut AdapterOutput) {
        let failed = value.get("error").is_some();
        let result = value.get("result").unwrap_or(&Value::Null);
        if self.step == TitleStep::ReadingConfig && id == self.id("config") {
            if failed {
                // Fail closed, like the TUI: unknown MCP servers stay enabled
                // otherwise.
                self.step = TitleStep::Done;
                return;
            }
            if let Some(servers) = result
                .pointer("/config/mcp_servers")
                .and_then(Value::as_object)
            {
                self.mcp_servers.extend(servers.keys().cloned());
            }
            self.mcp_servers.sort();
            self.mcp_servers.dedup();
            // The title model is an OpenAI model; another provider's thread
            // titles with its own model straight away.
            self.model = if matches!(self.model_provider.as_deref(), None | Some("openai")) {
                Some(TITLE_MODEL.to_string())
            } else {
                self.main_model.clone()
            };
            self.start_thread(output);
        } else if self.step == TitleStep::StartingThread && id == self.id("thread") {
            if failed {
                self.retry_or_finish(output);
                return;
            }
            let Some(hidden) = result
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .filter(|hidden| !hidden.is_empty())
            else {
                self.step = TitleStep::Done;
                return;
            };
            self.hidden_threads.push(hidden.to_string());
            self.thread_id = Some(hidden.to_string());
            // Fail closed, like the TUI, unless the thread really is read-only.
            if result.pointer("/sandbox/type").and_then(Value::as_str) != Some("readOnly") {
                self.detach(output);
                self.step = TitleStep::Done;
                return;
            }
            self.start_turn(output);
        } else if self.step == TitleStep::StartingTurn && id == self.id("turn") {
            if failed {
                self.retry_or_finish(output);
                return;
            }
            self.turn_id = result
                .pointer("/turn/id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            self.step = TitleStep::Running;
        }
        // `thread/name/set` and `thread/unsubscribe` answers, and answers for
        // a step this flow already left, need nothing.
    }

    fn on_notification(
        &mut self,
        method: &str,
        value: &Value,
        real_thread: Option<&str>,
        state: &mut AdapterState,
        output: &mut AdapterOutput,
    ) {
        if !matches!(self.step, TitleStep::StartingTurn | TitleStep::Running) {
            return;
        }
        let params = value.get("params").unwrap_or(&Value::Null);
        if params.get("threadId").and_then(Value::as_str) != self.thread_id.as_deref() {
            // A frame of an attempt this flow already abandoned.
            return;
        }
        match method {
            "item/completed" => {
                let Some(item) = params.get("item") else {
                    return;
                };
                if item.get("type").and_then(Value::as_str) != Some("agentMessage")
                    || item.get("phase").and_then(Value::as_str) == Some("commentary")
                {
                    return;
                }
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    self.response =
                        (text.len() <= TITLE_RESPONSE_MAX_BYTES).then(|| text.to_string());
                }
            }
            "turn/completed" | "turn/failed" => {
                let completed = params.get("turn").unwrap_or(params);
                if let (Some(reported), Some(own)) = (
                    completed.get("id").and_then(Value::as_str),
                    self.turn_id.as_deref(),
                ) {
                    if reported != own {
                        return;
                    }
                }
                let status = completed
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("completed");
                if method == "turn/failed" || status != "completed" {
                    // The title model can be unavailable to this account
                    // even though the thread and turn were accepted.
                    self.retry_or_finish(output);
                    return;
                }
                if let Some(title) = self.response.as_deref().and_then(parse_title) {
                    if !self.superseded {
                        if let Some(real_thread) = real_thread {
                            // Name the thread in Codex itself, so the Codex
                            // app and `thread/list` show the same title.
                            self.send(
                                output,
                                "name",
                                "thread/name/set",
                                json!({"threadId": real_thread, "name": title}),
                            );
                        }
                        super::report_session_title(&title, state, output);
                    }
                }
                self.detach(output);
                self.step = TitleStep::Done;
            }
            _ => {}
        }
    }
}

/// Queue a title-flow frame. Encoding plain JSON cannot fail in practice, and
/// a title is never worth failing the turn over.
fn push_frame(output: &mut AdapterOutput, frame: &Value) {
    if let Ok(bytes) = encode(frame) {
        output.writes.push(bytes);
    }
}

/// Whether `name` can sit unquoted inside a dotted config key.
fn is_plain_server_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A process-unique flow id. Ids only need to be unique per connection.
fn title_nonce() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .to_string()
}

/// The TUI's `thread_title_output_schema`.
fn title_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "minLength": 1,
                "maxLength": TITLE_MAX_CHARS,
            },
        },
        "required": ["title"],
        "additionalProperties": false,
    })
}

/// The TUI's `thread_title_prompt`: its instructions and the user's
/// description, bounded to [`TITLE_PROMPT_MAX_BYTES`] without splitting a
/// character.
fn title_prompt(description: &str) -> String {
    let prefix = format!(
        "Generate a concise, single-line task title of at most {TITLE_MAX_CHARS} characters \
         and under five words where possible. Start with an imperative verb. Capitalize only \
         the first word unless the user's language, proper nouns, acronyms, or code terms \
         require otherwise. Preserve ticket references exactly. Write in the user's language. \
         Do not use quotes, markdown, or trailing punctuation. Do not answer the request.\
         \n\nUser prompt:\n"
    );
    let remaining = TITLE_PROMPT_MAX_BYTES.saturating_sub(prefix.len());
    let description = description
        .trim()
        .char_indices()
        .take_while(|(index, character)| index + character.len_utf8() <= remaining)
        .map(|(_, character)| character)
        .collect::<String>();
    format!("{prefix}{description}")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GeneratedTitle {
    title: String,
}

/// The TUI's `parse_thread_title`.
fn parse_title(response: &str) -> Option<String> {
    if !response.trim_start().starts_with('{') {
        return None;
    }
    let title = serde_json::from_str::<GeneratedTitle>(response).ok()?.title;
    let normalized = title
        .trim()
        .trim_matches(|character| {
            matches!(
                character,
                '"' | '\'' | '`' | '\u{201C}' | '\u{201D}' | '\u{2018}' | '\u{2019}'
            )
        })
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(['.', '?', '!'])
        .trim_end()
        .to_string();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized.chars().take(TITLE_MAX_CHARS).collect())
}

/// Advance the handshake with the response to one of our own requests.
fn parse_response(
    value: &Value,
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
) -> Result<()> {
    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        return Ok(());
    };
    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Codex rejected a request")
            .to_string();
        // A resumed thread that another process already owns can still be
        // continued from its history through a fork.
        if id == ID_THREAD && turn.resume && message.contains("already has an active writer") {
            output.events.push(TurnEvent::Warning {
                message: "This Codex session is open in another process. Continuing from a copy of its history; the original session stays unchanged.".to_string(),
            });
            output.writes.push(encode(&request(
                ID_FORK,
                "thread/fork",
                turn.thread_params.clone(),
            ))?);
            return Ok(());
        }
        fail(turn, state, output, &message, error.get("code"));
        return Ok(());
    }
    let result = value.get("result").cloned().unwrap_or(Value::Null);
    match id {
        ID_INITIALIZE => {
            let method = if turn.resume {
                "thread/resume"
            } else {
                "thread/start"
            };
            output.writes.push(encode(&request(
                ID_THREAD,
                method,
                turn.thread_params.clone(),
            ))?);
        }
        ID_THREAD | ID_FORK => {
            let thread_id = result
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| protocol("Codex opened a thread without an identifier"))?
                .to_string();
            let named = result
                .pointer("/thread/name")
                .or_else(|| result.pointer("/thread/title"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty());
            if let Some(title) = named {
                state.result.session_title = Some(title.to_string());
            }
            // `turn/completed` does not repeat the model, so record whatever
            // the opened thread reports.
            if let Some(model) = result
                .pointer("/thread/model")
                .or_else(|| result.pointer("/thread/settings/model"))
                .and_then(Value::as_str)
            {
                state.result.model = Some(model.to_string());
            }
            if state.result.session_id.as_deref() != Some(thread_id.as_str()) {
                state.result.session_id = Some(thread_id.clone());
                output.events.push(TurnEvent::SessionStarted {
                    session_id: thread_id.clone(),
                    title: state.result.session_title.clone(),
                });
            }
            turn.thread_id = Some(thread_id.clone());
            if turn.manual_compaction {
                turn.open_compaction = Some("manual".to_string());
                output.events.push(TurnEvent::CompactionStarted {
                    trigger: CompactionTrigger::Manual,
                });
                output.writes.push(encode(&request(
                    ID_TURN,
                    "thread/compact/start",
                    json!({"threadId": thread_id}),
                ))?);
                return Ok(());
            }
            let mut params = turn.turn_params.clone();
            params["threadId"] = json!(thread_id);
            output
                .writes
                .push(encode(&request(ID_TURN, "turn/start", params))?);
            // Written after the user's turn so titling never delays it.
            let requested_model = turn.model.clone();
            if let Some(flow) = turn
                .title
                .as_mut()
                .filter(|flow| flow.step == TitleStep::Waiting)
            {
                if named.is_some() {
                    flow.step = TitleStep::Done;
                } else {
                    flow.main_model = state
                        .result
                        .model
                        .clone()
                        .or_else(|| {
                            result
                                .get("model")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        })
                        .or(requested_model);
                    flow.model_provider = result
                        .get("modelProvider")
                        .or_else(|| result.pointer("/thread/modelProvider"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    flow.read_config(output);
                }
            }
        }
        ID_TURN => {
            turn.turn_id = result
                .pointer("/turn/id")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        _ => {}
    }
    Ok(())
}

/// Answer a server-initiated request: approvals and user questions reach the
/// application, everything else is acknowledged so the turn cannot stall.
fn server_request(
    value: &Value,
    method: &str,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) -> Result<()> {
    let id = value.get("id").cloned().unwrap_or(Value::Null);
    let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
    match method {
        "item/commandExecution/requestApproval"
        | "item/fileChange/requestApproval"
        | "item/permissions/requestApproval" => {
            let request = ApprovalRequest {
                id: interaction_id(&id),
                tool_name: approval_tool_name(method).to_string(),
                description: Some(describe_approval(method, &params)),
                input: params,
            };
            output
                .events
                .push(TurnEvent::ApprovalRequested(request.clone()));
            output.interaction = Some(InteractionRequest::Approval {
                request,
                original: value.clone(),
            });
        }
        "item/tool/requestUserInput" => {
            let request = QuestionRequest::new(interaction_id(&id), questions(&params));
            // `isBlocking: false` means the agent keeps working; answering
            // later is the host's job, so the turn must not wait here.
            if params.get("isBlocking").and_then(Value::as_bool) == Some(false) {
                output
                    .events
                    .push(TurnEvent::AsyncQuestionRequested(request));
                output.writes.push(encode(&reply(
                    &id,
                    answers_for(&params, ASYNC_QUESTION_NOTE),
                ))?);
            } else {
                output
                    .events
                    .push(TurnEvent::QuestionRequested(request.clone()));
                output.interaction = Some(InteractionRequest::Question {
                    request,
                    original: value.clone(),
                });
            }
        }
        // An unanswered server request blocks the app server forever. An empty
        // result is the reference driver's behavior for requests this adapter
        // does not model (MCP elicitation, dynamic tool calls).
        _ => {
            let _ = turn;
            output.writes.push(encode(&reply(&id, json!({})))?);
        }
    }
    Ok(())
}

/// Map a server notification onto the normalized event stream.
fn notification(
    value: &Value,
    method: &str,
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
) {
    let params = value.get("params").cloned().unwrap_or(Value::Null);
    if let Some(usage) = super::codex::codex_account_usage(&params) {
        output.events.push(TurnEvent::AccountUsageUpdated { usage });
    }
    match method {
        "item/agentMessage/delta" => {
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                remember_stream(turn, &params);
                state.result.text.push_str(delta);
                state.saw_text_delta = true;
                output.events.push(TurnEvent::TextDelta {
                    text: delta.to_string(),
                });
            }
        }
        "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
            if let Some(delta) = params.get("delta").and_then(Value::as_str) {
                remember_stream(turn, &params);
                state
                    .result
                    .reasoning
                    .get_or_insert_with(String::new)
                    .push_str(delta);
                output.events.push(TurnEvent::ReasoningDelta {
                    text: delta.to_string(),
                });
            }
        }
        "item/started" | "item/completed" => {
            let completed = method == "item/completed";
            let Some(item) = params.get("item") else {
                return;
            };
            match item.get("type").and_then(Value::as_str) {
                Some("contextCompaction") => {
                    compaction_item(item, completed, turn, output);
                }
                Some("agentMessage") if completed => {
                    if let Some(text) = consolidated(turn, item, "text") {
                        state.result.text.push_str(&text);
                        output.events.push(TurnEvent::TextDelta { text });
                    }
                }
                Some("reasoning") if completed => {
                    if let Some(text) = consolidated(turn, item, "text") {
                        state
                            .result
                            .reasoning
                            .get_or_insert_with(String::new)
                            .push_str(&text);
                        output.events.push(TurnEvent::ReasoningDelta { text });
                    }
                }
                _ => {
                    if let Some(event) = tool_event(item, completed) {
                        output.events.push(event);
                    }
                }
            }
        }
        "thread/name/updated" => {
            // `ThreadNameUpdatedNotification` is `{threadId, threadName}`.
            let own = match (
                params.get("threadId").and_then(Value::as_str),
                turn.thread_id.as_deref(),
            ) {
                (Some(reported), Some(current)) => reported == current,
                _ => !turn.retained,
            };
            if !own {
                return;
            }
            if let Some(name) = params
                .get("threadName")
                .or_else(|| params.get("name"))
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
            {
                if let Some(flow) = turn
                    .title
                    .as_mut()
                    .filter(|flow| flow.step != TitleStep::Done)
                {
                    flow.superseded = true;
                }
                super::report_session_title(name, state, output);
            }
        }
        "warning" => {
            // A warning that names another thread (the hidden title thread
            // before its id is known) is not about this turn.
            let reported = params.get("threadId").and_then(Value::as_str);
            if let (Some(reported), Some(current)) = (reported, turn.thread_id.as_deref()) {
                if reported != current {
                    return;
                }
            }
            if let Some(message) = params
                .get("message")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|message| !message.is_empty())
            {
                output.events.push(TurnEvent::Warning {
                    message: message.to_string(),
                });
            }
        }
        "thread/tokenUsage/updated" => {
            // A notification for another thread (the app server can host
            // several) must not be charged against this turn.
            let reported = params.get("threadId").and_then(Value::as_str);
            if let (Some(reported), Some(current)) = (reported, turn.thread_id.as_deref()) {
                if reported != current {
                    return;
                }
            }
            let model = state
                .result
                .model
                .as_deref()
                .or(turn.model.as_deref())
                .map(str::to_owned);
            let usage = token_usage(&params, model);
            if let Some(used) = usage
                .context_window
                .as_ref()
                .and_then(|context| context.used_tokens)
            {
                turn.last_context_tokens = Some(used);
            }
            if usage != Usage::default() {
                super::merge_usage(&mut state.result.usage, &usage);
                output.events.push(TurnEvent::Usage(usage));
            }
        }
        "thread/compacted" if turn.manual_compaction => {
            // A manual compaction is complete once the thread says so.
            if turn.open_compaction.take().is_some() {
                output.events.push(TurnEvent::CompactionCompleted {
                    compaction: completed_compaction_with(
                        CompactionTrigger::Manual,
                        turn.last_context_tokens,
                    ),
                });
            }
            output.terminal = true;
        }
        "thread/compacted" => {
            // Deprecated in favor of the `contextCompaction` item, but still
            // the only signal from older app servers. Report it only when no
            // item already described this compaction.
            if turn.open_compaction.is_none() && !turn.saw_compaction_item {
                output.events.push(TurnEvent::CompactionCompleted {
                    compaction: completed_compaction(turn.last_context_tokens),
                });
            }
        }
        "error" => {
            let reported = params.get("threadId").and_then(Value::as_str);
            if let (Some(reported), Some(current)) = (reported, turn.thread_id.as_deref()) {
                if reported != current {
                    return;
                }
            }
            turn.error_message = params
                .pointer("/error/message")
                .or_else(|| params.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or(turn.error_message.take());
        }
        "turn/completed" | "turn/failed" => {
            let completed = params.get("turn").unwrap_or(&params);
            // A late notification for an earlier turn must not end this one.
            if let (Some(reported), Some(current)) = (
                completed.get("id").and_then(Value::as_str),
                turn.turn_id.as_deref(),
            ) {
                if reported != current {
                    return;
                }
            }
            output.terminal = true;
            if turn.open_compaction.take().is_some() {
                // A turn cannot end with its compaction still running; the
                // application must see it close instead of spinning forever.
                output.events.push(TurnEvent::CompactionFailed {
                    trigger: if turn.manual_compaction {
                        CompactionTrigger::Manual
                    } else {
                        CompactionTrigger::Automatic
                    },
                    message: Some(turn.error_message.clone().unwrap_or_else(|| {
                        "Codex ended the turn before the compaction finished.".to_string()
                    })),
                });
            }
            if let Some(model) = completed.get("model").and_then(Value::as_str) {
                state.result.model = Some(model.to_string());
            }
            let usage = token_usage(completed, state.result.model.clone());
            if usage != Usage::default() {
                super::merge_usage(&mut state.result.usage, &usage);
                output.events.push(TurnEvent::Usage(usage));
            }
            let status = completed
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("completed");
            if method == "turn/failed" || !matches!(status, "completed" | "succeeded") {
                let message = completed
                    .pointer("/error/message")
                    .or_else(|| params.pointer("/error/message"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| turn.error_message.clone())
                    .unwrap_or_else(|| format!("Codex ended the turn with status `{status}`"));
                fail(
                    turn,
                    state,
                    output,
                    &message,
                    completed.pointer("/error/code"),
                );
            }
        }
        _ => {}
    }
}

/// Translate a `contextCompaction` item into the compaction lifecycle.
///
/// Codex runs automatic compaction inside the turn that crossed its
/// `model_auto_compact_token_limit` (or before it, when the history is already
/// too large) and reports it as an ordinary thread item: `item/started` when
/// the summary request begins and `item/completed` when the history has been
/// replaced. The item carries no token counts, so the pre-compaction size is
/// the latest context occupancy the turn reported; the post-compaction size
/// arrives as the next `thread/tokenUsage/updated` snapshot.
fn compaction_item(
    item: &Value,
    completed: bool,
    turn: &mut TurnState,
    output: &mut AdapterOutput,
) {
    let id = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if !completed {
        if turn.open_compaction.is_none() {
            turn.open_compaction = Some(id);
            output.events.push(TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Automatic,
            });
        }
        return;
    }
    // A manual compaction opened its own lifecycle when it was requested.
    let trigger = if turn.manual_compaction {
        CompactionTrigger::Manual
    } else {
        CompactionTrigger::Automatic
    };
    let was_open = turn.open_compaction.take().is_some();
    if turn.manual_compaction {
        // The compaction is the whole invocation: its completed item ends it,
        // whether or not `turn/completed` or `thread/compacted` follows.
        output.terminal = true;
        if !was_open {
            // `thread/compacted` already reported this manual compaction.
            return;
        }
    }
    turn.saw_compaction_item = true;
    output.events.push(TurnEvent::CompactionCompleted {
        compaction: completed_compaction_with(trigger, turn.last_context_tokens),
    });
}

fn completed_compaction(pre_tokens: Option<u64>) -> ContextCompaction {
    completed_compaction_with(CompactionTrigger::Automatic, pre_tokens)
}

fn completed_compaction_with(
    trigger: CompactionTrigger,
    pre_tokens: Option<u64>,
) -> ContextCompaction {
    ContextCompaction {
        trigger,
        pre_tokens,
        post_tokens: None,
        dropped_tokens: None,
        cumulative_dropped_tokens: None,
        duration_ms: None,
    }
}

/// Record a provider-native terminal failure, exactly like the `exec --json`
/// adapter does, so both transports surface the same typed error.
fn fail(
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
    message: &str,
    code: Option<&Value>,
) {
    let provider_code = code.and_then(|code| {
        code.as_str()
            .map(str::to_owned)
            .or_else(|| code.as_i64().map(|code| code.to_string()))
    });
    let kind = classify_provider_failure(&format!(
        "{} {message}",
        provider_code.as_deref().unwrap_or_default()
    ));
    let mut failure = ProviderTerminalFailure::new(kind, message, DeliveryState::Accepted);
    if let Some(code) = provider_code {
        failure = failure.with_provider_code(format!("codex::{code}"));
    }
    turn.error_message = Some(message.to_string());
    state.terminal_failure = Some(failure);
    state.result.status = RunStatus::Failed;
    output.terminal = true;
}

fn remember_stream(turn: &mut TurnState, params: &Value) {
    if let Some(id) = params.get("itemId").and_then(Value::as_str) {
        if !turn.streamed.iter().any(|seen| seen == id) {
            turn.streamed.push(id.to_string());
        }
    }
}

/// Return the consolidated item text only when it was not already streamed.
fn consolidated(turn: &TurnState, item: &Value, field: &str) -> Option<String> {
    let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
    if turn.streamed.iter().any(|seen| seen == id) {
        return None;
    }
    item.get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn truncate(text: &str) -> String {
    if text.chars().count() <= MAX_TOOL_OUTPUT_CHARS {
        return text.to_string();
    }
    let mut bounded: String = text.chars().take(MAX_TOOL_OUTPUT_CHARS).collect();
    bounded.push_str("… [truncated]");
    bounded
}

/// Map an app-server `ThreadItem` onto the same tool-call vocabulary the
/// `exec --json` adapter emits, so applications see one Codex tool stream.
fn tool_event(item: &Value, completed: bool) -> Option<TurnEvent> {
    let kind = item.get("type")?.as_str()?;
    let failed = matches!(
        item.get("status").and_then(Value::as_str),
        Some("failed" | "interrupted" | "declined")
    );
    let (name, input, output, error) = match kind {
        "commandExecution" => {
            let output = item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .map(truncate);
            let mut error = failed.then(|| {
                item.get("exitCode").and_then(Value::as_i64).map_or_else(
                    || "command execution failed".to_string(),
                    |code| format!("command exited with code {code}"),
                )
            });
            if let (Some(reason), Some(detail)) = (error.as_mut(), output.as_deref()) {
                let detail = detail.trim();
                if !detail.is_empty() {
                    reason.push_str(": ");
                    reason.push_str(detail);
                }
            }
            (
                "command_execution".to_string(),
                item.get("command")
                    .cloned()
                    .map(|command| json!({"command": command})),
                output,
                error,
            )
        }
        "fileChange" => (
            "file_change".to_string(),
            item.get("changes").cloned(),
            item.get("changes")
                .and_then(Value::as_array)
                .map(|changes| {
                    changes
                        .iter()
                        .filter_map(|change| change.get("path").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|paths| !paths.is_empty()),
            failed.then(|| "file change failed".to_string()),
        ),
        "mcpToolCall" => {
            let server = item
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("unknown-server");
            let tool = item
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("unknown-tool");
            (
                format!("mcp__{server}__{tool}"),
                item.get("arguments").cloned(),
                item.get("result")
                    .filter(|value| !value.is_null())
                    .map(|result| truncate(&result.to_string())),
                item.pointer("/error/message")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        item.get("error")
                            .filter(|value| !value.is_null())
                            .map(|error| truncate(&error.to_string()))
                    }),
            )
        }
        "webSearch" => (
            "web_search".to_string(),
            Some(json!({"query": item.get("query")})),
            item.get("results")
                .filter(|value| !value.is_null())
                .map(|results| truncate(&results.to_string())),
            failed.then(|| "web search failed".to_string()),
        ),
        _ => return None,
    };
    Some(TurnEvent::ToolCall {
        id: item.get("id").and_then(Value::as_str).map(str::to_owned),
        name,
        status: if !completed {
            ToolCallStatus::Started
        } else if failed {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Succeeded
        },
        input,
        output,
        error,
        task_id: None,
        images: Vec::new(),
    })
}

/// Normalize `thread/tokenUsage/updated`'s camelCase counters.
///
/// Context occupancy is the *latest* model request (`tokenUsage.last`), not
/// the thread total: a compaction shrinks the active window even though the
/// thread's cumulative totals keep growing. `model` labels the snapshot so an
/// application can show which model's window is being filled.
fn token_usage(params: &Value, model: Option<String>) -> Usage {
    let container = params
        .get("tokenUsage")
        .or_else(|| params.get("usage"))
        .unwrap_or(params);
    let last = container.get("last").unwrap_or(container);
    let token = |name: &str| last.get(name).and_then(Value::as_u64);
    Usage {
        input_tokens: token("inputTokens").or_else(|| token("input_tokens")),
        output_tokens: token("outputTokens").or_else(|| token("output_tokens")),
        cache_creation_input_tokens: token("cacheWriteInputTokens"),
        cache_read_input_tokens: token("cachedInputTokens"),
        context_window: token("totalTokens").map(|used| crate::ContextWindowUsage {
            used_tokens: Some(used),
            limit_tokens: container
                .get("modelContextWindow")
                .and_then(Value::as_u64)
                .filter(|limit| *limit > 0),
            model,
            estimated: false,
        }),
        cost_usd: None,
    }
}

fn interaction_id(id: &Value) -> String {
    id.as_str().map_or_else(|| id.to_string(), str::to_owned)
}

fn approval_tool_name(method: &str) -> &'static str {
    match method {
        "item/commandExecution/requestApproval" => "command_execution",
        "item/fileChange/requestApproval" => "file_change",
        _ => "permissions",
    }
}

fn describe_approval(method: &str, params: &Value) -> String {
    match method {
        "item/commandExecution/requestApproval" => {
            params.get("command").and_then(Value::as_str).map_or_else(
                || "Run a shell command".to_string(),
                |command| format!("Run `{command}`"),
            )
        }
        "item/fileChange/requestApproval" => {
            params.get("grantRoot").and_then(Value::as_str).map_or_else(
                || "Apply file changes".to_string(),
                |root| format!("Apply file changes under {root}"),
            )
        }
        _ => params
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("Grant additional permissions")
            .to_string(),
    }
}

/// Convert `ToolRequestUserInputParams.questions` into portable prompts.
fn questions(params: &Value) -> Value {
    let prompts = params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|question| {
            json!({
                "header": question.get("header").and_then(Value::as_str).unwrap_or_default(),
                "question": question.get("question").and_then(Value::as_str).unwrap_or_default(),
                "multiSelect": false,
                "options": question
                    .get("options")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|option| json!({
                        "label": option.get("label").and_then(Value::as_str).unwrap_or_default(),
                        "description": option
                            .get("description")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();
    json!({"questions": prompts})
}

/// Ordered `(question id, header, question text)` triples from a request.
fn question_ids(params: &Value) -> Vec<(String, String, String)> {
    params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, question)| {
            let text = |field: &str| {
                question
                    .get(field)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            let id = question
                .get("id")
                .and_then(Value::as_str)
                .map_or_else(|| format!("question-{index}"), str::to_owned);
            (id, text("header"), text("question"))
        })
        .collect()
}

/// Reply that answers every question with the same explanatory note.
fn answers_for(params: &Value, note: &str) -> Value {
    let answers = question_ids(params)
        .into_iter()
        .map(|(id, _, _)| (id, json!({"answers": [note]})))
        .collect::<Map<_, _>>();
    json!({"answers": answers})
}

/// Encode an approval decision as a `*RequestApprovalResponse`.
pub(super) fn approval_response(original: &Value, decision: ApprovalDecision) -> Result<Vec<u8>> {
    let wire = match decision {
        ApprovalDecision::Allow => "accept",
        ApprovalDecision::AllowForSession => "acceptForSession",
        ApprovalDecision::Deny { .. } => "decline",
    };
    let id = original.get("id").cloned().unwrap_or(Value::Null);
    encode(&reply(&id, json!({"decision": wire})))
}

/// Encode a `ToolRequestUserInputResponse` from an application answer.
///
/// Answers are keyed by question text, then header, then Codex's own question
/// id, which covers every shape [`QuestionAnswer`] is built with.
pub(super) fn question_response(
    original: &Value,
    answer: Option<QuestionAnswer>,
) -> Result<Vec<u8>> {
    let params = original.get("params").cloned().unwrap_or_else(|| json!({}));
    let id = original.get("id").cloned().unwrap_or(Value::Null);
    let Some(answer) = answer else {
        return encode(&reply(&id, answers_for(&params, DECLINED_QUESTION_NOTE)));
    };
    let answers = question_ids(&params)
        .into_iter()
        .map(|(question_id, header, text)| {
            let selected = [text.as_str(), header.as_str(), question_id.as_str()]
                .into_iter()
                .filter(|key| !key.is_empty())
                .find_map(|key| answer.answers.get(key))
                .map(selected_values)
                .unwrap_or_default();
            (question_id, json!({"answers": selected}))
        })
        .collect::<Map<_, _>>();
    encode(&reply(&id, json!({"answers": answers})))
}

fn selected_values(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned)
            })
            .collect(),
        Value::Null => Vec::new(),
        other => vec![other.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PermissionMode, Provider};

    fn decode(bytes: Vec<u8>) -> Value {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn question_frame(blocking: bool) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": "server-9",
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1",
                "isBlocking": blocking,
                "questions": [{
                    "id": "q1", "header": "Fruit", "question": "Which fruit?",
                    "options": [{"label": "Banana", "description": "Yellow"}]
                }]
            }
        })
    }

    #[test]
    fn approval_decisions_use_the_native_wire_vocabulary() {
        let original = json!({"id": "server-1"});
        for (decision, expected) in [
            (ApprovalDecision::Allow, "accept"),
            (ApprovalDecision::AllowForSession, "acceptForSession"),
            (ApprovalDecision::Deny { reason: None }, "decline"),
        ] {
            let response = decode(approval_response(&original, decision).unwrap());
            assert_eq!(response["id"], json!("server-1"));
            assert_eq!(response["result"]["decision"], json!(expected));
        }
    }

    #[test]
    fn a_question_answer_is_keyed_back_to_the_native_question_id() {
        let original = question_frame(true);
        for key in ["Which fruit?", "Fruit", "q1"] {
            let response = decode(
                question_response(&original, Some(QuestionAnswer::selected(key, "Banana")))
                    .unwrap(),
            );
            assert_eq!(
                response["result"]["answers"]["q1"]["answers"],
                json!(["Banana"]),
                "answer keyed by `{key}` was lost"
            );
        }
    }

    #[test]
    fn an_unanswered_question_says_so_instead_of_returning_an_empty_selection() {
        let response = decode(question_response(&question_frame(true), None).unwrap());
        let answers = &response["result"]["answers"]["q1"]["answers"];
        assert_eq!(answers[0], json!(DECLINED_QUESTION_NOTE));
    }

    #[test]
    fn a_non_blocking_question_is_answered_without_an_interaction() {
        let mut state = AdapterState::default();
        let output = parse_line(&question_frame(false).to_string(), &mut state).unwrap();

        assert!(output.interaction.is_none());
        assert!(matches!(
            output.events.as_slice(),
            [TurnEvent::AsyncQuestionRequested(request)] if request.id == "server-9"
        ));
        let reply = decode(output.writes[0].clone());
        assert_eq!(
            reply["result"]["answers"]["q1"]["answers"][0],
            json!(ASYNC_QUESTION_NOTE)
        );
    }

    #[test]
    fn a_blocking_question_is_delegated_to_the_application() {
        let mut state = AdapterState::default();
        let output = parse_line(&question_frame(true).to_string(), &mut state).unwrap();

        assert!(output.writes.is_empty(), "{:?}", output.writes);
        assert!(matches!(
            output.interaction,
            Some(InteractionRequest::Question { ref request, .. }) if request.id == "server-9"
        ));
    }

    #[test]
    fn tool_items_keep_the_exec_json_tool_vocabulary() {
        let mut state = AdapterState::default();
        let completed = json!({"jsonrpc": "2.0", "method": "item/completed", "params": {"item": {
            "id": "item-2", "type": "commandExecution", "command": "ls",
            "status": "failed", "aggregatedOutput": "denied", "exitCode": 13
        }}});
        let output = parse_line(&completed.to_string(), &mut state).unwrap();

        assert!(matches!(&output.events[0], TurnEvent::ToolCall {
            id: Some(id), name, status: ToolCallStatus::Failed, error: Some(error), ..
        } if id == "item-2" && name == "command_execution"
            && error.contains("code 13") && error.contains("denied")));

        let mcp = json!({"jsonrpc": "2.0", "method": "item/started", "params": {"item": {
            "id": "item-3", "type": "mcpToolCall", "server": "platform", "tool": "deploy",
            "arguments": {"project": 7}, "status": "inProgress"
        }}});
        let output = parse_line(&mcp.to_string(), &mut state).unwrap();
        assert!(matches!(&output.events[0], TurnEvent::ToolCall {
            name, status: ToolCallStatus::Started, input: Some(input), ..
        } if name == "mcp__platform__deploy" && input == &json!({"project": 7})));
    }

    #[test]
    fn a_streamed_message_is_not_replayed_by_its_consolidated_item() {
        let mut state = AdapterState::default();
        parse_line(
            &json!({"jsonrpc": "2.0", "method": "item/agentMessage/delta",
                "params": {"itemId": "item-1", "delta": "Done"}})
            .to_string(),
            &mut state,
        )
        .unwrap();
        let output = parse_line(
            &json!({"jsonrpc": "2.0", "method": "item/completed", "params": {"item": {
                "id": "item-1", "type": "agentMessage", "text": "Done"
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.events.is_empty(), "{:?}", output.events);
        assert_eq!(state.result.text, "Done");
        assert!(state.saw_text_delta);
    }

    fn token_usage_frame(thread: &str, total: u64) -> String {
        json!({"jsonrpc": "2.0", "method": "thread/tokenUsage/updated", "params": {
            "threadId": thread, "turnId": "turn-1",
            "tokenUsage": {
                "last": {"inputTokens": 120, "outputTokens": 34, "cachedInputTokens": 64,
                    "cacheWriteInputTokens": 8, "totalTokens": total},
                "total": {"inputTokens": 900, "outputTokens": 400, "totalTokens": 1300},
                "modelContextWindow": 272_000
            }
        }})
        .to_string()
    }

    fn compaction_item_frame(method: &str) -> String {
        json!({"jsonrpc": "2.0", "method": method, "params": {
            "threadId": "thread-1", "turnId": "turn-1",
            "item": {"type": "contextCompaction", "id": "item-compact-1"}
        }})
        .to_string()
    }

    fn thread_compacted_frame() -> String {
        json!({"jsonrpc": "2.0", "method": "thread/compacted", "params": {
            "threadId": "thread-1", "turnId": "turn-1"
        }})
        .to_string()
    }

    fn compaction_events(state: &mut AdapterState, frames: &[String]) -> Vec<TurnEvent> {
        frames
            .iter()
            .flat_map(|frame| parse_line(frame, state).unwrap().events)
            .filter(|event| {
                matches!(
                    event,
                    TurnEvent::CompactionStarted { .. }
                        | TurnEvent::CompactionCompleted { .. }
                        | TurnEvent::CompactionFailed { .. }
                )
            })
            .collect()
    }

    /// Frame shapes of `codex app-server` 0.155.1 (`ThreadItem::ContextCompaction`
    /// and the deprecated `ContextCompactedNotification`).
    #[test]
    fn reports_an_automatic_compaction_item_once_from_start_to_completion() {
        let mut state = started_turn(None);
        let events = compaction_events(
            &mut state,
            &[
                token_usage_frame("thread-1", 180_000),
                compaction_item_frame("item/started"),
                compaction_item_frame("item/completed"),
                thread_compacted_frame(),
            ],
        );
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(matches!(
            events[0],
            TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Automatic
            }
        ));
        assert!(matches!(
            events[1],
            TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Automatic,
                    pre_tokens: Some(180_000),
                    post_tokens: None,
                    ..
                }
            }
        ));
    }

    #[test]
    fn falls_back_to_the_deprecated_compacted_notification() {
        let mut state = started_turn(None);
        let events = compaction_events(&mut state, &[thread_compacted_frame()]);
        assert!(matches!(
            events.as_slice(),
            [TurnEvent::CompactionCompleted { .. }]
        ));
    }

    #[test]
    fn a_turn_that_ends_mid_compaction_closes_it_as_failed() {
        let mut state = started_turn(None);
        let events = compaction_events(
            &mut state,
            &[
                compaction_item_frame("item/started"),
                json!({"jsonrpc": "2.0", "method": "turn/completed", "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "interrupted"}
                }})
                .to_string(),
            ],
        );
        assert!(matches!(
            events.as_slice(),
            [
                TurnEvent::CompactionStarted { .. },
                TurnEvent::CompactionFailed {
                    trigger: CompactionTrigger::Automatic,
                    message: Some(_),
                }
            ]
        ));
    }

    /// Open a thread so token-usage notifications can be attributed to it.
    fn started_turn(model: Option<&str>) -> AdapterState {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request.model = model.map(str::to_owned);
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        parse_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &mut state).unwrap();
        parse_line(
            r#"{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1"}}}"#,
            &mut state,
        )
        .unwrap();
        state
    }

    #[test]
    fn token_usage_reports_the_active_context_window_against_the_selected_model() {
        let mut state = started_turn(Some("gpt-5-codex"));

        let output = parse_line(&token_usage_frame("thread-1", 154), &mut state).unwrap();

        let [TurnEvent::Usage(usage)] = output.events.as_slice() else {
            panic!("expected one usage event, got {:?}", output.events);
        };
        assert_eq!(usage.input_tokens, Some(120));
        assert_eq!(usage.output_tokens, Some(34));
        assert_eq!(usage.cache_read_input_tokens, Some(64));
        assert_eq!(usage.cache_creation_input_tokens, Some(8));
        let window = usage.context_window.clone().expect("context window");
        assert_eq!(window.used_tokens, Some(154));
        assert_eq!(window.limit_tokens, Some(272_000));
        assert_eq!(window.model.as_deref(), Some("gpt-5-codex"));
        assert!(!window.estimated);
        assert_eq!(state.result.usage.context_window, usage.context_window);
    }

    #[test]
    fn a_later_context_snapshot_replaces_the_earlier_one() {
        let mut state = started_turn(None);
        parse_line(&token_usage_frame("thread-1", 154), &mut state).unwrap();

        parse_line(&token_usage_frame("thread-1", 96), &mut state).unwrap();

        let window = state
            .result
            .usage
            .context_window
            .clone()
            .expect("context window");
        assert_eq!(
            window.used_tokens,
            Some(96),
            "compaction shrinks the active window; usage must not accumulate"
        );
    }

    #[test]
    fn token_usage_for_another_thread_is_not_charged_against_this_turn() {
        let mut state = started_turn(Some("gpt-5-codex"));

        let output = parse_line(&token_usage_frame("thread-other", 999), &mut state).unwrap();

        assert!(output.events.is_empty(), "{:?}", output.events);
        assert_eq!(state.result.usage, Usage::default());
    }

    #[test]
    fn the_model_reported_by_the_thread_labels_the_context_window() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request.model = Some("requested-model".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        parse_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &mut state).unwrap();
        parse_line(
            r#"{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1","model":"resolved-model"}}}"#,
            &mut state,
        )
        .unwrap();

        let output = parse_line(&token_usage_frame("thread-1", 154), &mut state).unwrap();

        let [TurnEvent::Usage(usage)] = output.events.as_slice() else {
            panic!("expected one usage event");
        };
        assert_eq!(
            usage.context_window.as_ref().unwrap().model.as_deref(),
            Some("resolved-model")
        );
    }

    #[test]
    fn a_failed_turn_keeps_the_provider_diagnostic_and_code() {
        let mut state = AdapterState::default();
        let output = parse_line(
            &json!({"jsonrpc": "2.0", "method": "turn/completed", "params": {"turn": {
                "id": "turn-1", "status": "failed",
                "error": {"message": "usage limit reached", "code": "usage_limit"}
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.terminal);
        let failure = state.terminal_failure.unwrap();
        assert_eq!(failure.kind, crate::ProviderProcessErrorKind::RateLimited);
        assert_eq!(failure.provider_code.as_deref(), Some("codex::usage_limit"));
        assert_eq!(state.result.status, RunStatus::Failed);
    }

    #[test]
    fn the_handshake_opts_into_the_experimental_request_user_input_api() {
        let handshake = String::from_utf8(handshake()).unwrap();
        let initialize: Value = serde_json::from_str(handshake.lines().next().unwrap()).unwrap();

        assert_eq!(initialize["method"], json!("initialize"));
        assert_eq!(
            initialize["params"]["capabilities"]["experimentalApi"],
            json!(true)
        );
    }

    #[test]
    fn unrestricted_access_maps_to_the_native_sandbox_and_approval_policy() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request.permission_mode = PermissionMode::FullAccess;
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();

        let turn = load(&state);
        assert_eq!(turn.thread_params["sandbox"], json!("danger-full-access"));
        assert_eq!(turn.thread_params["approvalPolicy"], json!("never"));
        assert_eq!(
            turn.turn_params["input"][0],
            json!({"type": "text", "text": "inspect"})
        );
    }

    #[test]
    fn an_image_attachment_becomes_a_native_local_image_user_input() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "what is this?");
        request.attachments = vec![
            crate::retained::TurnAttachment {
                path: "/tmp/screenshot.png".into(),
                display_name: None,
                media_type: Some("image/png".into()),
            },
            crate::retained::TurnAttachment {
                path: "/tmp/report.pdf".into(),
                display_name: None,
                media_type: Some("application/pdf".into()),
            },
        ];
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();

        let turn = load(&state);
        assert_eq!(
            turn.turn_params["input"],
            json!([
                {"type": "text", "text": "what is this?"},
                {"type": "localImage", "path": "/tmp/screenshot.png"}
            ])
        );
    }

    #[test]
    fn a_manual_compaction_compacts_the_opened_thread_instead_of_starting_a_turn() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "/compact");
        request.session_id = Some("thread-9".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        parse_line(r#"{"id":1,"result":{}}"#, &mut state).unwrap();
        let opened = parse_line(
            r#"{"id":2,"result":{"thread":{"id":"thread-9"}}}"#,
            &mut state,
        )
        .unwrap();
        let compact = decode(opened.writes[0].clone());
        assert_eq!(compact["method"], "thread/compact/start");
        assert_eq!(compact["params"]["threadId"], "thread-9");
        assert!(opened.events.iter().any(|event| matches!(
            event,
            TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Manual
            }
        )));

        let done = parse_line(
            r#"{"jsonrpc":"2.0","method":"thread/compacted","params":{"threadId":"thread-9"}}"#,
            &mut state,
        )
        .unwrap();
        assert!(done.terminal);
        assert!(done.events.iter().any(|event| matches!(
            event,
            TurnEvent::CompactionCompleted { compaction } if compaction.trigger == CompactionTrigger::Manual
        )));
    }

    #[test]
    fn a_retained_manual_compaction_follows_only_its_own_threads_turn() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "/compact");
        request.session_id = Some("thread-9".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        mark_retained(&mut state);
        parse_line(
            r#"{"id":2,"result":{"thread":{"id":"thread-9"}}}"#,
            &mut state,
        )
        .unwrap();
        parse_line(r#"{"id":4,"result":{}}"#, &mut state).unwrap();

        // A late turn from another thread on the shared connection is not
        // this compaction's turn.
        parse_line(
            r#"{"method":"turn/started","params":{"threadId":"thread-other","turn":{"id":"turn-x"}}}"#,
            &mut state,
        )
        .unwrap();
        let stray = parse_line(
            r#"{"method":"item/completed","params":{"threadId":"thread-other","turnId":"turn-x","item":{"type":"contextCompaction","id":"c-x"}}}"#,
            &mut state,
        )
        .unwrap();
        assert!(!stray.terminal);
        assert_eq!(stray.events, [] as [crate::TurnEvent; 0]);

        parse_line(
            r#"{"method":"turn/started","params":{"threadId":"thread-9","turn":{"id":"turn-c"}}}"#,
            &mut state,
        )
        .unwrap();
        let done = parse_line(
            r#"{"method":"item/completed","params":{"threadId":"thread-9","turnId":"turn-c","item":{"type":"contextCompaction","id":"c-1"}}}"#,
            &mut state,
        )
        .unwrap();
        // The completed compaction item ends the invocation by itself.
        assert!(done.terminal);
        assert!(done.events.iter().any(|event| matches!(
            event,
            TurnEvent::CompactionCompleted { compaction } if compaction.trigger == CompactionTrigger::Manual
        )));
    }

    #[test]
    fn a_retained_manual_compaction_also_ends_on_the_older_thread_compacted() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "/compact");
        request.session_id = Some("thread-9".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        mark_retained(&mut state);
        parse_line(
            r#"{"id":2,"result":{"thread":{"id":"thread-9"}}}"#,
            &mut state,
        )
        .unwrap();
        parse_line(r#"{"id":4,"result":{}}"#, &mut state).unwrap();
        let other = parse_line(
            r#"{"method":"thread/compacted","params":{"threadId":"thread-other"}}"#,
            &mut state,
        )
        .unwrap();
        assert!(!other.terminal);
        let done = parse_line(
            r#"{"method":"thread/compacted","params":{"threadId":"thread-9"}}"#,
            &mut state,
        )
        .unwrap();
        assert!(done.terminal);
        assert!(done
            .events
            .iter()
            .any(|event| matches!(event, TurnEvent::CompactionCompleted { .. })));
    }

    #[test]
    fn resume_and_writer_conflict_fork_exclude_historical_turns() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "continue");
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state).unwrap();
        let opened = parse_line(r#"{"id":1,"result":{}}"#, &mut state).unwrap();
        let start = decode(opened.writes[0].clone());
        assert_eq!(start["method"], "thread/start");
        assert!(start["params"].get("excludeTurns").is_none());

        request.session_id = Some("large-thread".into());
        prepare_turn(&request, &mut state).unwrap();
        let opened = parse_line(r#"{"id":1,"result":{}}"#, &mut state).unwrap();
        let resume = decode(opened.writes[0].clone());
        assert_eq!(resume["params"]["excludeTurns"], true);
        let conflict = parse_line(
            r#"{"id":2,"error":{"message":"thread already has an active writer"}}"#,
            &mut state,
        )
        .unwrap();
        let fork = decode(conflict.writes[0].clone());
        assert_eq!(fork["method"], "thread/fork");
        assert_eq!(fork["params"]["threadId"], "large-thread");
        assert_eq!(fork["params"]["excludeTurns"], true);
    }

    #[test]
    fn the_handshake_response_opens_the_requested_thread_and_starts_the_turn() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "continue");
        request.session_id = Some("thread-7".into());
        let mut state = AdapterState::default();
        state.result.session_id = request.session_id.clone();
        prepare_turn(&request, &mut state).unwrap();

        let opened = parse_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &mut state).unwrap();
        let resume = decode(opened.writes[0].clone());
        assert_eq!(resume["method"], json!("thread/resume"));
        assert_eq!(resume["params"]["threadId"], json!("thread-7"));
        assert_eq!(resume["params"]["excludeTurns"], json!(true));

        let started = parse_line(
            r#"{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-7"}}}"#,
            &mut state,
        )
        .unwrap();
        assert!(
            started.events.is_empty(),
            "a resumed thread is not a new session"
        );
        let turn = decode(started.writes[0].clone());
        assert_eq!(turn["method"], json!("turn/start"));
        assert_eq!(turn["params"]["threadId"], json!("thread-7"));

        parse_line(
            r#"{"jsonrpc":"2.0","id":4,"result":{"turn":{"id":"turn-9"}}}"#,
            &mut state,
        )
        .unwrap();
        let interrupt = decode(interrupt(&state).unwrap());
        assert_eq!(interrupt["method"], json!("turn/interrupt"));
        assert_eq!(interrupt["params"]["turnId"], json!("turn-9"));
    }

    #[test]
    fn an_unmodelled_server_request_is_acknowledged_so_the_turn_cannot_stall() {
        let mut state = AdapterState::default();
        let output = parse_line(
            r#"{"jsonrpc":"2.0","id":"server-5","method":"mcpServer/elicitation/request","params":{}}"#,
            &mut state,
        )
        .unwrap();

        assert!(output.interaction.is_none());
        assert_eq!(decode(output.writes[0].clone())["id"], json!("server-5"));
    }

    // Session titles: a hidden thread, the way Codex's TUI titles threads.

    const DESCRIPTION: &str = "Fix the login redirect bug in auth.rs (JIRA-142)";

    fn title_request(description: Option<&str>) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::Codex, "/work/repo", "full prompt");
        request.model = Some("requested-model".into());
        request.request_session_title = description.map(str::to_owned);
        request
    }

    fn writes(output: &AdapterOutput) -> Vec<Value> {
        output.writes.iter().cloned().map(decode).collect()
    }

    fn feed(state: &mut AdapterState, frame: Value) -> AdapterOutput {
        parse_line(&frame.to_string(), state).unwrap()
    }

    /// Answer a request the adapter wrote.
    fn answer(state: &mut AdapterState, request: &Value, result: Value) -> AdapterOutput {
        feed(
            state,
            json!({"jsonrpc": "2.0", "id": request["id"], "result": result}),
        )
    }

    fn reject(state: &mut AdapterState, request: &Value) -> AdapterOutput {
        feed(
            state,
            json!({"jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32600, "message": "model not found"}}),
        )
    }

    /// Prepare `request`, finish the handshake and open the user's thread.
    fn open(request: &TurnRequest, thread: Value) -> (AdapterState, AdapterOutput) {
        let mut state = AdapterState::default();
        prepare_turn(request, &mut state).unwrap();
        parse_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &mut state).unwrap();
        let opened = feed(
            &mut state,
            json!({"jsonrpc": "2.0", "id": 2, "result": {
                "thread": thread, "model": "gpt-5-codex", "modelProvider": "openai"
            }}),
        );
        (state, opened)
    }

    /// Open the user's thread and drive the title flow up to its hidden turn.
    /// Returns the hidden `turn/start` frame.
    fn hidden_turn_started(state: &mut AdapterState, opened: &AdapterOutput) -> Value {
        let config_read = writes(opened)[1].clone();
        let started = answer(state, &config_read, json!({"config": {}}));
        let thread_start = writes(&started)[0].clone();
        let turn = answer(
            state,
            &thread_start,
            json!({"thread": {"id": "title-thread"}, "sandbox": {"type": "readOnly"}}),
        );
        writes(&turn)[0].clone()
    }

    fn hidden(method: &str, params: Value) -> Value {
        let mut params = params;
        params["threadId"] = json!("title-thread");
        json!({"jsonrpc": "2.0", "method": method, "params": params})
    }

    fn hidden_answer(text: &str) -> Value {
        hidden(
            "item/completed",
            json!({"turnId": "title-turn", "item": {
                "id": "title-item", "type": "agentMessage", "text": text, "phase": "final_answer"
            }}),
        )
    }

    fn hidden_completed(status: &str) -> Value {
        hidden(
            "turn/completed",
            json!({"turn": {"id": "title-turn", "status": status, "model": TITLE_MODEL,
                "usage": {"inputTokens": 9, "outputTokens": 3}}}),
        )
    }

    #[test]
    fn a_server_name_that_cannot_be_a_dotted_key_is_disabled_as_a_table() {
        let mut request = title_request(Some(DESCRIPTION));
        for name in ["plain-server", "needs.quotes"] {
            request.launch_context.mcp_servers.insert(
                name.into(),
                crate::McpServerConfig::Stdio {
                    command: "server".into(),
                    args: Vec::new(),
                    environment_from: std::collections::BTreeMap::new(),
                },
            );
        }
        let (state, _) = open(&request, json!({"id": "thread-1"}));
        let mut flow = load(&state).title.expect("a title flow");
        let mut output = AdapterOutput::default();
        flow.start_thread(&mut output);
        let frame = &writes(&output)[0];
        let config = &frame["params"]["config"];
        assert_eq!(
            config["mcp_servers"],
            json!({"needs.quotes": {"enabled": false}, "plain-server": {"enabled": false}})
        );
        assert!(config.get("mcp_servers.plain-server.enabled").is_none());
    }

    #[test]
    fn a_requested_title_runs_on_a_hidden_read_only_thread_and_names_the_users_thread() {
        let mut request = title_request(Some(DESCRIPTION));
        request.launch_context.mcp_servers.insert(
            "temps_fleet".into(),
            crate::McpServerConfig::Stdio {
                command: "temps-fleet".into(),
                args: Vec::new(),
                environment_from: std::collections::BTreeMap::new(),
            },
        );
        let (mut state, opened) = open(&request, json!({"id": "thread-1"}));

        // The user's turn is written first; titling never delays it.
        let opened_writes = writes(&opened);
        assert_eq!(opened_writes.len(), 2, "{opened_writes:?}");
        assert_eq!(opened_writes[0]["method"], "turn/start");
        assert_eq!(opened_writes[0]["params"]["threadId"], "thread-1");
        let config_read = &opened_writes[1];
        assert_eq!(config_read["method"], "config/read");
        assert_eq!(config_read["params"]["cwd"], "/work/repo");
        let id = config_read["id"].as_str().unwrap();
        assert!(id.starts_with(TITLE_ID_PREFIX) && id != ID_KEEPALIVE);

        let started = answer(
            &mut state,
            config_read,
            json!({"config": {"model": "gpt-5-codex", "mcp_servers": {"github": {"url": "x"}}}}),
        );
        assert_eq!(started.events.len(), 0);
        let [thread_start] = writes(&started).try_into().unwrap();
        assert_eq!(thread_start["method"], "thread/start");
        let params = &thread_start["params"];
        assert_eq!(params["model"], TITLE_MODEL);
        assert_eq!(params["modelProvider"], "openai");
        assert_eq!(params["ephemeral"], true);
        assert_eq!(params["sandbox"], "read-only");
        assert_eq!(params["cwd"], "/work/repo");
        assert_eq!(params["threadSource"], "thread_title");
        for empty in [
            "environments",
            "dynamicTools",
            "runtimeWorkspaceRoots",
            "selectedCapabilityRoots",
        ] {
            assert_eq!(params[empty], json!([]), "{empty}");
        }
        let config = &params["config"];
        for feature in ["shell_tool", "unified_exec", "multi_agent", "apps", "hooks"] {
            assert_eq!(config[format!("features.{feature}")], false, "{feature}");
        }
        assert_eq!(config["tools.update_plan.enabled"], false);
        assert_eq!(config["web_search"], "disabled");
        assert_eq!(config["default_permissions"], ":read-only");
        // Merged into each server's own definition, so a server only a
        // `--config` override defines is not replaced by a transport-less one.
        assert_eq!(config["mcp_servers.github.enabled"], false);
        assert_eq!(config["mcp_servers.temps_fleet.enabled"], false);
        assert!(config.get("mcp_servers").is_none());
        assert!(thread_start["id"]
            .as_str()
            .unwrap()
            .starts_with(TITLE_ID_PREFIX));

        let turn = answer(
            &mut state,
            &thread_start,
            json!({"thread": {"id": "title-thread"}, "sandbox": {"type": "readOnly"}}),
        );
        let [turn_start] = writes(&turn).try_into().unwrap();
        assert_eq!(turn_start["method"], "turn/start");
        assert_eq!(turn_start["params"]["threadId"], "title-thread");
        assert_eq!(
            turn_start["params"]["input"],
            json!([{"type": "text", "text": title_prompt(DESCRIPTION)}])
        );
        assert_eq!(turn_start["params"]["outputSchema"], title_output_schema());
        assert_eq!(turn_start["params"]["effort"], "low");

        let accepted = answer(
            &mut state,
            &turn_start,
            json!({"turn": {"id": "title-turn"}}),
        );
        assert!(accepted.writes.is_empty() && accepted.events.is_empty());
        let message = feed(
            &mut state,
            hidden_answer(r#"{"title":"Fix login redirect."}"#),
        );
        assert!(message.writes.is_empty() && message.events.is_empty());

        let done = feed(&mut state, hidden_completed("completed"));
        assert!(!done.terminal);
        assert_eq!(
            done.events,
            [TurnEvent::SessionTitle {
                title: "Fix login redirect".into()
            }]
        );
        let [name, unsubscribe] = writes(&done).try_into().unwrap();
        assert_eq!(name["method"], "thread/name/set");
        assert_eq!(
            name["params"],
            json!({"threadId": "thread-1", "name": "Fix login redirect"})
        );
        assert_eq!(unsubscribe["method"], "thread/unsubscribe");
        assert_eq!(unsubscribe["params"], json!({"threadId": "title-thread"}));
        assert_eq!(
            state.result.session_title.as_deref(),
            Some("Fix login redirect")
        );

        // Codex echoes the rename; it is the same title, so no second event.
        let echo = feed(
            &mut state,
            json!({"jsonrpc": "2.0", "method": "thread/name/updated",
                "params": {"threadId": "thread-1", "threadName": "Fix login redirect"}}),
        );
        assert!(echo.events.is_empty(), "{:?}", echo.events);
        for reply in [name, unsubscribe] {
            let late = answer(&mut state, &reply, json!({}));
            assert!(late.events.is_empty() && late.writes.is_empty());
        }
    }

    #[test]
    fn hidden_thread_frames_never_reach_or_end_the_users_turn() {
        for retained in [false, true] {
            let (mut state, opened) =
                open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
            if retained {
                mark_retained(&mut state);
            }
            hidden_turn_started(&mut state, &opened);
            // The user's `turn/start` has not been answered yet, so its turn
            // id is unknown: nothing but the thread id tells the turns apart.
            let mut events = Vec::new();
            for frame in [
                hidden(
                    "turn/started",
                    json!({"turn": {"id": "title-turn", "status": "inProgress"}}),
                ),
                hidden(
                    "item/started",
                    json!({"turnId": "title-turn", "item": {
                        "id": "cmd", "type": "commandExecution", "command": "ls"
                    }}),
                ),
                hidden(
                    "item/agentMessage/delta",
                    json!({"turnId": "title-turn", "itemId": "title-item", "delta": "{\"ti"}),
                ),
                hidden(
                    "item/reasoning/summaryTextDelta",
                    json!({"turnId": "title-turn", "itemId": "r", "delta": "thinking"}),
                ),
                hidden(
                    "thread/tokenUsage/updated",
                    json!({"turnId": "title-turn", "tokenUsage": {
                        "last": {"inputTokens": 9, "outputTokens": 3, "totalTokens": 12},
                        "modelContextWindow": 128_000
                    }}),
                ),
                hidden(
                    "error",
                    json!({"turnId": "title-turn", "willRetry": false,
                        "error": {"message": "hidden failure"}}),
                ),
                hidden("warning", json!({"message": "hidden warning"})),
                hidden_answer(r#"{"title":"Fix login redirect"}"#),
            ] {
                let output = feed(&mut state, frame);
                assert!(!output.terminal);
                assert!(output.writes.is_empty(), "{:?}", output.writes);
                events.extend(output.events);
            }
            let approval = feed(
                &mut state,
                json!({"jsonrpc": "2.0", "id": "server-7",
                    "method": "item/commandExecution/requestApproval",
                    "params": {"threadId": "title-thread", "turnId": "title-turn",
                        "itemId": "cmd", "command": "rm -rf /"}}),
            );
            assert!(approval.interaction.is_none() && approval.events.is_empty());
            let [refusal] = writes(&approval).try_into().unwrap();
            assert_eq!(refusal["id"], "server-7");
            assert!(refusal.get("error").is_some());

            let done = feed(&mut state, hidden_completed("completed"));
            assert!(!done.terminal, "the hidden turn ended the user's turn");
            events.extend(done.events);
            assert_eq!(
                events,
                [TurnEvent::SessionTitle {
                    title: "Fix login redirect".into()
                }],
                "retained: {retained}"
            );
            assert_eq!(state.result.text.len(), 0);
            assert!(state.result.reasoning.is_none());
            assert_eq!(state.result.usage, Usage::default());
            assert_eq!(state.result.model.as_deref(), None);
            assert!(state.terminal_failure.is_none());
            assert_eq!(load(&state).error_message, None);

            // The user's own turn still runs and ends as before.
            parse_line(
                r#"{"jsonrpc":"2.0","id":4,"result":{"turn":{"id":"turn-1"}}}"#,
                &mut state,
            )
            .unwrap();
            let text = feed(
                &mut state,
                json!({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {
                    "threadId": "thread-1", "turnId": "turn-1", "itemId": "i", "delta": "Done"}}),
            );
            assert_eq!(
                text.events,
                [TurnEvent::TextDelta {
                    text: "Done".into()
                }]
            );
            let end = feed(
                &mut state,
                json!({"jsonrpc": "2.0", "method": "turn/completed", "params": {
                    "threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}}}),
            );
            assert!(end.terminal);
            assert_eq!(state.result.status, RunStatus::Succeeded);
            assert_eq!(
                state.result.session_title.as_deref(),
                Some("Fix login redirect")
            );
        }
    }

    #[test]
    fn no_title_is_requested_without_a_description_or_for_a_named_or_continued_thread() {
        let (_, opened) = open(&title_request(None), json!({"id": "thread-1"}));
        assert_eq!(opened.writes.len(), 1);

        let (_, opened) = open(&title_request(Some("   ")), json!({"id": "thread-1"}));
        assert_eq!(opened.writes.len(), 1);

        let (state, opened) = open(
            &title_request(Some(DESCRIPTION)),
            json!({"id": "thread-1", "name": "Named in the Codex app"}),
        );
        assert_eq!(opened.writes.len(), 1);
        assert_eq!(writes(&opened)[0]["method"], "turn/start");
        assert_eq!(
            state.result.session_title.as_deref(),
            Some("Named in the Codex app")
        );

        let mut request = title_request(Some(DESCRIPTION));
        request.session_id = Some("thread-1".into());
        request.continuation = true;
        let (_, opened) = open(&request, json!({"id": "thread-1"}));
        assert_eq!(opened.writes.len(), 1);

        let mut request = title_request(Some(DESCRIPTION));
        request.prompt = "/compact".into();
        request.session_id = Some("thread-1".into());
        let (_, opened) = open(&request, json!({"id": "thread-1"}));
        assert_eq!(writes(&opened)[0]["method"], "thread/compact/start");
        assert_eq!(opened.writes.len(), 1);
    }

    #[test]
    fn a_failing_title_model_falls_back_once_to_the_threads_model_then_gives_up_silently() {
        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let started = answer(&mut state, &writes(&opened)[1], json!({"config": {}}));
        let first = writes(&started)[0].clone();
        assert_eq!(first["params"]["model"], TITLE_MODEL);

        let retried = reject(&mut state, &first);
        assert!(retried.events.is_empty() && !retried.terminal);
        let [second] = writes(&retried).try_into().unwrap();
        assert_eq!(second["method"], "thread/start");
        assert_eq!(second["params"]["model"], "gpt-5-codex");
        assert_ne!(second["id"], first["id"]);

        // The fallback's turn carries no title-model effort.
        let turn = answer(
            &mut state,
            &second,
            json!({"thread": {"id": "title-thread"}, "sandbox": {"type": "readOnly"}}),
        );
        let turn_start = writes(&turn)[0].clone();
        assert!(turn_start["params"].get("effort").is_none());

        let failed = reject(&mut state, &turn_start);
        assert!(failed.events.is_empty() && !failed.terminal);
        assert!(state.terminal_failure.is_none());
        // Only the hidden thread is detached; nothing is retried again.
        let [unsubscribe] = writes(&failed).try_into().unwrap();
        assert_eq!(unsubscribe["method"], "thread/unsubscribe");
        assert_eq!(load(&state).title.unwrap().step, TitleStep::Done);
    }

    #[test]
    fn a_hidden_turn_the_title_model_cannot_run_retries_on_the_threads_model() {
        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let turn_start = hidden_turn_started(&mut state, &opened);
        answer(
            &mut state,
            &turn_start,
            json!({"turn": {"id": "title-turn"}}),
        );
        let failed = feed(&mut state, hidden_completed("failed"));
        assert!(failed.events.is_empty() && !failed.terminal);
        let [unsubscribe, restart] = writes(&failed).try_into().unwrap();
        assert_eq!(unsubscribe["method"], "thread/unsubscribe");
        assert_eq!(restart["method"], "thread/start");
        assert_eq!(restart["params"]["model"], "gpt-5-codex");
    }

    #[test]
    fn a_thread_that_is_not_read_only_or_unreadable_config_ends_the_title_flow() {
        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let unread = reject(&mut state, &writes(&opened)[1]);
        assert!(unread.writes.is_empty() && unread.events.is_empty());

        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let started = answer(&mut state, &writes(&opened)[1], json!({"config": {}}));
        let writable = answer(
            &mut state,
            &writes(&started)[0],
            json!({"thread": {"id": "title-thread"}, "sandbox": {"type": "dangerFullAccess"}}),
        );
        let [unsubscribe] = writes(&writable).try_into().unwrap();
        assert_eq!(unsubscribe["method"], "thread/unsubscribe");
    }

    #[test]
    fn another_providers_thread_titles_with_its_own_model() {
        let mut state = AdapterState::default();
        prepare_turn(&title_request(Some(DESCRIPTION)), &mut state).unwrap();
        parse_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, &mut state).unwrap();
        let opened = feed(
            &mut state,
            json!({"jsonrpc": "2.0", "id": 2, "result": {
                "thread": {"id": "thread-1"}, "model": "llama3", "modelProvider": "ollama"
            }}),
        );
        let started = answer(&mut state, &writes(&opened)[1], json!({"config": {}}));
        assert_eq!(writes(&started)[0]["params"]["model"], "llama3");
    }

    #[test]
    fn a_turn_that_ends_first_abandons_the_title_and_late_frames_stay_inert() {
        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let turn_start = hidden_turn_started(&mut state, &opened);
        parse_line(
            r#"{"jsonrpc":"2.0","id":4,"result":{"turn":{"id":"turn-1"}}}"#,
            &mut state,
        )
        .unwrap();
        let end = feed(
            &mut state,
            json!({"jsonrpc": "2.0", "method": "turn/completed", "params": {
                "threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed"}}}),
        );
        assert!(end.terminal);
        assert!(end.events.is_empty(), "{:?}", end.events);
        let [unsubscribe] = writes(&end).try_into().unwrap();
        assert_eq!(unsubscribe["params"]["threadId"], "title-thread");

        for late in [
            json!({"jsonrpc": "2.0", "id": turn_start["id"], "result": {"turn": {"id": "title-turn"}}}),
            hidden_answer(r#"{"title":"Too late"}"#),
            hidden_completed("completed"),
        ] {
            let output = feed(&mut state, late);
            assert!(output.events.is_empty() && output.writes.is_empty());
        }
        assert_eq!(state.result.session_title, None);

        // Between turns a retained process drops them as background output,
        // and never reads them as a keepalive answer.
        assert_eq!(
            classify_idle_frame(&json!({"id": turn_start["id"], "result": {}}).to_string()),
            IdleFrame::Background
        );
        assert_eq!(
            classify_idle_frame(&hidden_completed("completed").to_string()),
            IdleFrame::Background
        );
        assert!(matches!(
            classify_idle_frame(&json!({"id": ID_KEEPALIVE, "result": {"data": []}}).to_string()),
            IdleFrame::KeepaliveResult { active: false }
        ));
    }

    #[test]
    fn a_frame_answering_another_flow_is_dropped_without_advancing_this_one() {
        let (mut state, opened) =
            open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
        let foreign = feed(
            &mut state,
            json!({"jsonrpc": "2.0", "id": format!("{TITLE_ID_PREFIX}0-1-config"),
                "result": {"config": {}}}),
        );
        assert!(foreign.writes.is_empty() && foreign.events.is_empty());
        assert_eq!(load(&state).title.unwrap().step, TitleStep::ReadingConfig);
        assert_ne!(
            writes(&opened)[1]["id"],
            json!(format!("{TITLE_ID_PREFIX}0-1-config"))
        );
    }

    #[test]
    fn a_rename_reports_the_session_title_once_and_supersedes_generation() {
        for retained in [false, true] {
            let (mut state, opened) =
                open(&title_request(Some(DESCRIPTION)), json!({"id": "thread-1"}));
            if retained {
                mark_retained(&mut state);
            }
            let turn_start = hidden_turn_started(&mut state, &opened);
            answer(
                &mut state,
                &turn_start,
                json!({"turn": {"id": "title-turn"}}),
            );
            let renamed = json!({"jsonrpc": "2.0", "method": "thread/name/updated",
                "params": {"threadId": "thread-1", "threadName": "  Renamed by hand "}});
            let first = feed(&mut state, renamed.clone());
            assert_eq!(
                first.events,
                [TurnEvent::SessionTitle {
                    title: "Renamed by hand".into()
                }],
                "retained: {retained}"
            );
            assert_eq!(feed(&mut state, renamed).events.len(), 0);
            let other = feed(
                &mut state,
                json!({"jsonrpc": "2.0", "method": "thread/name/updated",
                    "params": {"threadId": "thread-other", "threadName": "Not ours"}}),
            );
            assert_eq!(other.events.len(), 0);

            // The hand-written name wins over the generated one.
            feed(
                &mut state,
                hidden_answer(r#"{"title":"Fix login redirect"}"#),
            );
            let done = feed(&mut state, hidden_completed("completed"));
            assert!(done.events.is_empty(), "{:?}", done.events);
            let [unsubscribe] = writes(&done).try_into().unwrap();
            assert_eq!(unsubscribe["method"], "thread/unsubscribe");
            assert_eq!(
                state.result.session_title.as_deref(),
                Some("Renamed by hand")
            );
        }
    }

    #[test]
    fn the_title_prompt_matches_the_tui_and_stays_within_its_byte_bound() {
        let prompt = title_prompt("  Fix it  ");
        assert_eq!(
            prompt,
            "Generate a concise, single-line task title of at most 36 characters and under \
             five words where possible. Start with an imperative verb. Capitalize only the \
             first word unless the user's language, proper nouns, acronyms, or code terms \
             require otherwise. Preserve ticket references exactly. Write in the user's \
             language. Do not use quotes, markdown, or trailing punctuation. Do not answer \
             the request.\n\nUser prompt:\nFix it"
        );
        let long = title_prompt(&"é".repeat(2_000));
        assert!(long.len() <= TITLE_PROMPT_MAX_BYTES);
        assert!(long.len() > TITLE_PROMPT_MAX_BYTES - 2);
        assert!(long.ends_with('é'));
    }

    #[test]
    fn generated_titles_are_normalized_like_the_tui() {
        assert_eq!(
            parse_title(r#"{"title":"  “Fix   the\nlogin bug?!”  "}"#).as_deref(),
            Some("Fix the login bug")
        );
        assert_eq!(
            parse_title(&format!(r#"{{"title":"{}"}}"#, "a".repeat(50)))
                .unwrap()
                .chars()
                .count(),
            TITLE_MAX_CHARS
        );
        assert_eq!(parse_title(r#"Title: {"title":"x"}"#), None);
        assert_eq!(parse_title(r#"{"title":"x","extra":1}"#), None);
        assert_eq!(parse_title(r#"{"title":" ... "}"#), None);
    }
}
