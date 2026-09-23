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

use crate::adapter::{AdapterOutput, AdapterState, InteractionRequest};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    ApprovalDecision, ApprovalRequest, Provider, ProviderTerminalFailure, QuestionAnswer,
    QuestionRequest, Result, RunStatus, RuntimeError, ToolCallStatus, TurnEvent, TurnRequest,
    Usage,
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

    store(
        state,
        &TurnState {
            thread_params,
            turn_params,
            resume: request.session_id.is_some(),
            model: request.model.clone(),
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
            if let Some(title) = result
                .pointer("/thread/name")
                .or_else(|| result.pointer("/thread/title"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
            {
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
            let mut params = turn.turn_params.clone();
            params["threadId"] = json!(thread_id);
            output
                .writes
                .push(encode(&request(ID_TURN, "turn/start", params))?);
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
            if let Some(name) = params
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|name| !name.is_empty())
            {
                state.result.session_title = Some(name.to_string());
            }
        }
        "warning" => {
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
            if usage != Usage::default() {
                super::merge_usage(&mut state.result.usage, &usage);
                output.events.push(TurnEvent::Usage(usage));
            }
        }
        "error" => {
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

        assert!(output.writes.is_empty());
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

        assert!(output.events.is_empty());
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

        assert!(output.events.is_empty());
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
}
