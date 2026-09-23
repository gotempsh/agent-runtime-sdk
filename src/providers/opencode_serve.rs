//! Bidirectional OpenCode transport built on `opencode serve`.
//!
//! `opencode run --format json` streams a turn one way and, crucially, has no
//! permission enforcement the SDK can rely on: the flags it accepts are
//! `--auto` (approve everything) and `--agent plan`, so every other policy is
//! whatever the user's own `opencode` configuration happens to say. An
//! application cannot ask for "prompt before running a shell command" and be
//! told what actually happened.
//!
//! `opencode serve` fixes both halves. Policy is supplied per turn through
//! `OPENCODE_CONFIG_CONTENT`, which the server reads instead of the ambient
//! configuration, and anything the policy marks `ask` is surfaced as a
//! `permission.asked` event answered over HTTP. That makes permission
//! enforcement a property of the turn rather than of the machine it runs on.
//!
//! This module is the synchronous half of that transport. It sequences the
//! whole protocol — session, subscription, prompt, events, permission answers
//! — as one state machine over newline-delimited frames, exactly like
//! [`super::codex_app_server`]. [`super::opencode_http`] moves the bytes.
//!
//! The wire shapes are those of the reference driver, which read them off a
//! live `opencode 1.4.3 serve` process rather than from the published types:
//! notably the permission event really is `permission.asked`, while the
//! shipped SDK types still declare `permission.updated`. Events are therefore
//! read as loosely-typed JSON, which survives that class of drift by
//! construction.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter::{AdapterOutput, AdapterState, InteractionRequest};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    ApprovalDecision, ApprovalRequest, PermissionMode, Provider, ProviderTerminalFailure,
    QuestionAnswer, QuestionRequest, Result, RunStatus, RuntimeError, ToolCallStatus, TurnEvent,
    TurnRequest,
};

use super::opencode_http::{
    FRAME_ERROR, FRAME_EVENT, FRAME_READY, FRAME_RESPONSE, FRAME_SUBSCRIBED,
};

/// Key under which this transport keeps its per-turn protocol state.
const STATE_KEY: &str = "opencode.serve";

/// Correlation identifiers for the bridge requests this module issues.
const ID_SESSION: u64 = 1;
const ID_PROMPT: u64 = 2;

/// Tool output is bounded to the same budget the other adapters use.
const MAX_TOOL_OUTPUT_CHARS: usize = 4096;

/// Per-turn protocol state retained between frames.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct TurnState {
    /// Loopback port the `opencode serve` child was told to bind.
    pub port: u16,
    /// Workspace passed as the `directory` query parameter.
    directory: String,
    /// Session this turn resumes, if any.
    resume: Option<String>,
    /// Message parts sent as the prompt.
    parts: Vec<Value>,
    /// Native `{providerID, modelID}` selection, when the caller pinned one.
    model: Option<Value>,
    /// Session being prompted.
    session_id: Option<String>,
    /// Whether every permission must be refused without consulting the
    /// application, because the turn asked for a read-only plan.
    plan_mode: bool,
    /// Whether an explicitly empty tool allowlist denied every tool.
    tools_denied: bool,
    /// Message id to role, so only assistant output is streamed.
    roles: std::collections::BTreeMap<String, String>,
    /// Part id to the text already emitted for it.
    emitted: std::collections::BTreeMap<String, String>,
    /// Part id to `text` or `reasoning`.
    part_kinds: std::collections::BTreeMap<String, String>,
    /// Last error reported by the session.
    error_message: Option<String>,
    /// Whether any assistant text or tool call was observed.
    saw_activity: bool,
    /// Require every turn-affecting SSE event to identify this turn's session.
    retained: bool,
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

/// Read back the port chosen for this turn, for `command_for_turn`.
pub(super) fn turn_port(state: &AdapterState) -> Option<u16> {
    let port = load(state).port;
    (port != 0).then_some(port)
}

pub(super) fn mark_retained(state: &mut AdapterState) {
    let mut turn = load(state);
    turn.retained = true;
    store(state, &turn);
}

fn protocol(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Protocol {
        provider: Provider::OpenCode,
        message: message.into(),
    }
}

fn encode(value: &Value) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| protocol(format!("could not encode an OpenCode request: {error}")))
}

/// Percent-encode a query-parameter value.
///
/// A workspace path can contain spaces, `#`, `&` or `?`, any of which would
/// otherwise change which directory the server is told to use.
fn query_escape(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn get(id: u64, path: String) -> Value {
    json!({"id": id, "method": "GET", "path": path})
}

fn post(id: Option<u64>, path: String, body: Value) -> Value {
    json!({"id": id, "method": "POST", "path": path, "body": body})
}

/// Permission policy for one turn, as `OPENCODE_CONFIG_CONTENT` expects it.
///
/// OpenCode's only permission levers are the blanket `edit` and `bash`
/// categories, so every mode is expressed on those two axes. Plan mode denies
/// both outright: OpenCode's read-only tools are gated by neither category, so
/// a plan turn can still look around but can never write or run a shell
/// command — the same "no side effects, ever" guarantee Codex gets from a
/// read-only sandbox.
///
/// This is the whole reason `Serve` mode exists. It is supplied per turn and
/// replaces the user's ambient configuration, so the policy an application
/// asked for is the policy the harness runs under.
pub(super) fn permission_config(request: &TurnRequest) -> Result<String> {
    let plan = is_plan_mode(request);
    let (edit, bash) = if plan {
        ("deny", "deny")
    } else {
        match &request.permission_mode {
            // Reviewing an edit you were never asked about is not a review,
            // so this is "write freely, ask before running a shell command".
            PermissionMode::AcceptEdits => ("allow", "ask"),
            PermissionMode::FullAccess => ("allow", "allow"),
            PermissionMode::Default | PermissionMode::Plan | PermissionMode::Custom(_) => {
                ("ask", "ask")
            }
        }
    };
    let native = request
        .harness_options
        .get("permission_mode")
        .map(String::as_str);
    let (edit, bash) = match native {
        Some("auto") => ("allow", "allow"),
        Some("ask") => ("ask", "ask"),
        Some("default") | None => (edit, bash),
        Some(other) => {
            return Err(RuntimeError::InvalidRequest {
                field: "harness_options.permission_mode",
                message: format!("unsupported OpenCode permission mode `{other}`"),
            })
        }
    };

    let mut config = json!({"permission": {"edit": edit, "bash": bash}});
    if request
        .launch_context
        .allowed_tools
        .as_deref()
        .is_some_and(<[String]>::is_empty)
    {
        // An explicitly empty tool set must be an enforcement boundary, not a
        // prompt-level suggestion. OpenCode's wildcard rule is the only thing
        // that expresses it.
        config["permission"] = json!({"*": "deny"});
    }
    if let Some(servers) = mcp_config(request)? {
        config["mcp"] = servers;
    }
    Ok(config.to_string())
}

/// Whether this turn is a read-only plan turn.
fn is_plan_mode(request: &TurnRequest) -> bool {
    matches!(request.permission_mode, PermissionMode::Plan)
        || request
            .harness_options
            .get("agent")
            .is_some_and(|agent| agent == "plan")
}

/// Translate turn-scoped MCP servers into OpenCode's `mcp` configuration.
///
/// OpenCode distinguishes `local` (a subprocess speaking MCP over stdio) from
/// `remote` (an HTTP endpoint), and takes the command as an argv array, so no
/// shell quoting is involved. Credentials are never serialized here: stdio
/// servers name the harness variables to forward, and remote servers name the
/// variable each header is read from.
fn mcp_config(request: &TurnRequest) -> Result<Option<Value>> {
    let servers = &request.launch_context.mcp_servers;
    if servers.is_empty() {
        return Ok(None);
    }
    let mut configured = serde_json::Map::new();
    for (name, server) in servers {
        let entry = match server {
            crate::McpServerConfig::Stdio {
                command,
                args,
                environment_from,
            } => {
                let Some(command) = command.to_str() else {
                    return Err(RuntimeError::InvalidRequest {
                        field: "launch_context.mcp_servers.command",
                        message: "stdio commands must be valid UTF-8".into(),
                    });
                };
                let mut argv = vec![json!(command)];
                argv.extend(args.iter().map(|argument| json!(argument)));
                let mut entry = json!({"type": "local", "command": argv, "enabled": true});
                if !environment_from.is_empty() {
                    // OpenCode takes an environment map rather than a list of
                    // names to forward, so a reference is expanded by the
                    // server from its own environment. The value never
                    // appears in this configuration.
                    let environment = environment_from
                        .iter()
                        .map(|(target, source)| {
                            (target.clone(), json!(format!("{{env:{source}}}")))
                        })
                        .collect::<serde_json::Map<_, _>>();
                    entry["environment"] = Value::Object(environment);
                }
                entry
            }
            crate::McpServerConfig::Http { url, headers_from } => {
                let mut entry = json!({"type": "remote", "url": url, "enabled": true});
                if !headers_from.is_empty() {
                    let headers = headers_from
                        .iter()
                        .map(|(header, source)| {
                            (header.clone(), json!(format!("{{env:{source}}}")))
                        })
                        .collect::<serde_json::Map<_, _>>();
                    entry["headers"] = Value::Object(headers);
                }
                entry
            }
        };
        configured.insert(name.clone(), entry);
    }
    Ok(Some(Value::Object(configured)))
}

/// Seed the state machine from the validated request.
pub(super) fn prepare_turn(request: &TurnRequest, state: &mut AdapterState, port: u16) {
    let mut parts = Vec::new();
    // The persona/system-prompt prefix is prompt-level for OpenCode:
    // `OPENCODE_CONFIG_CONTENT` has no system-prompt field and the prompt body
    // has no injection field, so there is nowhere else to put it.
    let prompt = match system_prompt_prefix(request) {
        Some(prefix) if !request.prompt.is_empty() => {
            format!("{prefix}\n\n---\n\n{}", request.prompt)
        }
        Some(prefix) => prefix,
        None => request.prompt.clone(),
    };
    if !prompt.is_empty() {
        parts.push(json!({"type": "text", "text": prompt}));
    }
    // Best-effort, and deliberately additive. The reference driver sends a
    // `file` part built from a URL it already has; the SDK only has an
    // execution-host path, and that `file://` spelling has not been confirmed
    // against a live server. So this adapter does *not* advertise
    // `TurnCapabilities::native_image_attachments`: the caller still describes
    // the files in the prompt, and a part OpenCode ignores costs nothing,
    // whereas dropping the attachment silently would lose it.
    for attachment in &request.attachments {
        parts.push(json!({
            "type": "file",
            "mime": attachment.media_type,
            "filename": attachment.display_name.clone().or_else(|| attachment
                .path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())),
            "url": format!("file://{}", attachment.path.display()),
        }));
    }

    store(
        state,
        &TurnState {
            port,
            directory: request.working_directory.to_string_lossy().into_owned(),
            resume: request.session_id.clone(),
            parts,
            model: model_selection(request.model.as_deref()),
            plan_mode: is_plan_mode(request),
            tools_denied: request
                .launch_context
                .allowed_tools
                .as_deref()
                .is_some_and(<[String]>::is_empty),
            ..TurnState::default()
        },
    );
}

/// Render the launch context's system-prompt and tool policy as a prompt
/// prefix, which is the only channel OpenCode offers for either.
fn system_prompt_prefix(request: &TurnRequest) -> Option<String> {
    let context = &request.launch_context;
    let mut sections = Vec::new();
    if let Some(instructions) = context
        .system_prompt_append
        .as_deref()
        .map(str::trim)
        .filter(|instructions| !instructions.is_empty())
    {
        sections.push(instructions.to_string());
    }
    // A non-empty allowlist is advisory here; an empty one is enforced by the
    // wildcard deny rule in `permission_config` instead.
    if let Some(tools) = context
        .allowed_tools
        .as_deref()
        .filter(|tools| !tools.is_empty())
    {
        sections.push(format!(
            "Use only these tools: {}. Do not use any other tool.",
            tools.join(", ")
        ));
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

/// OpenCode takes a split `{providerID, modelID}`, not one identifier.
fn model_selection(model: Option<&str>) -> Option<Value> {
    let model = model?;
    let (provider, id) = model.split_once('/')?;
    (!provider.is_empty() && !id.is_empty()).then(|| json!({"providerID": provider, "modelID": id}))
}

/// Encode a cooperative abort for a turn whose session is known.
pub(super) fn interrupt(state: &AdapterState) -> Option<Vec<u8>> {
    let turn = load(state);
    let session = turn.session_id?;
    encode(&post(
        None,
        format!(
            "/session/{}/abort?directory={}",
            query_escape(&session),
            query_escape(&turn.directory)
        ),
        json!({}),
    ))
    .ok()
}

/// Translate one bridge frame.
pub(super) fn parse_line(line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
    let value: Value = serde_json::from_str(line)
        .map_err(|error| protocol(format!("invalid bridge frame: {error}")))?;
    let mut turn = load(state);
    let mut output = AdapterOutput::default();
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        FRAME_READY => ready(&turn, &mut output)?,
        FRAME_SUBSCRIBED => prompt(&turn, &mut output)?,
        FRAME_RESPONSE => response(&value, &mut turn, state, &mut output)?,
        FRAME_EVENT => {
            if let Some(event) = value.get("event") {
                self::event(event, &mut turn, state, &mut output);
            }
        }
        FRAME_ERROR => {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("The OpenCode server became unreachable.");
            fail(&mut turn, state, &mut output, message, None);
        }
        _ => {}
    }
    store(state, &turn);
    Ok(output)
}

/// Resolve the session as soon as the server answers.
fn ready(turn: &TurnState, output: &mut AdapterOutput) -> Result<()> {
    let directory = query_escape(&turn.directory);
    let frame = match turn.resume.as_deref() {
        // `session.get` finds a session by id without needing the directory to
        // match, so a workspace reached through a symlink cannot fail closed
        // on a spurious "not found" the way listing and filtering would.
        Some(resume) => get(
            ID_SESSION,
            format!("/session/{}?directory={directory}", query_escape(resume)),
        ),
        None => post(
            Some(ID_SESSION),
            format!("/session?directory={directory}"),
            json!({}),
        ),
    };
    output.writes.push(encode(&frame)?);
    Ok(())
}

/// Send the prompt once the event stream is confirmed open.
fn prompt(turn: &TurnState, output: &mut AdapterOutput) -> Result<()> {
    let Some(session) = turn.session_id.as_deref() else {
        return Ok(());
    };
    let mut body = json!({"parts": turn.parts});
    if let Some(model) = &turn.model {
        body["model"] = model.clone();
    }
    output.writes.push(encode(&post(
        Some(ID_PROMPT),
        format!(
            "/session/{}/message?directory={}",
            query_escape(session),
            query_escape(&turn.directory)
        ),
        body,
    ))?);
    Ok(())
}

/// Advance the handshake with the answer to one of our own requests.
fn response(
    value: &Value,
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
) -> Result<()> {
    let status = value.get("status").and_then(Value::as_u64).unwrap_or(0);
    let body = value.get("body").cloned().unwrap_or(Value::Null);
    match value.get("id").and_then(Value::as_u64) {
        Some(ID_SESSION) => {
            if status == 0 || status >= 400 {
                let message = turn.resume.as_deref().map_or_else(
                    || "OpenCode could not start a session for this turn.".to_string(),
                    |_| {
                        "This OpenCode session can't be resumed — it may no longer exist."
                            .to_string()
                    },
                );
                fail(turn, state, output, &message, None);
                return Ok(());
            }
            let session = body.get("id").and_then(Value::as_str);
            // A session with a parent is a subagent session. Prompting it
            // returns success but produces no event activity at all, so the
            // turn would hang until its deadline instead of failing here.
            let parented = body.get("parentID").is_some_and(|parent| !parent.is_null());
            let Some(session) = session.filter(|_| !parented) else {
                fail(
                    turn,
                    state,
                    output,
                    "This OpenCode session can't be resumed — it may be a subagent session or no longer exist.",
                    None,
                );
                return Ok(());
            };
            turn.session_id = Some(session.to_string());
            if let Some(title) = body
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
            {
                state.result.session_title = Some(title.to_string());
            }
            if state.result.session_id.as_deref() != Some(session) {
                state.result.session_id = Some(session.to_string());
                output.events.push(TurnEvent::SessionStarted {
                    session_id: session.to_string(),
                    title: state.result.session_title.clone(),
                });
            }
            // Subscribe before prompting so nothing emitted in the turn's
            // first moments is missed; the prompt follows `@subscribed`.
            output.writes.push(encode(&json!({
                "method": "SUBSCRIBE",
                "path": "/event",
            }))?);
        }
        Some(ID_PROMPT) if (200..300).contains(&status) => {
            output.turn_submitted = true;
        }
        Some(ID_PROMPT) if status == 0 || status >= 400 => {
            let message = body
                .pointer("/data/message")
                .or_else(|| body.get("message"))
                .and_then(Value::as_str)
                .map_or_else(
                    || "OpenCode rejected this turn's prompt.".to_string(),
                    str::to_owned,
                );
            fail(turn, state, output, &message, None);
        }
        _ => {}
    }
    Ok(())
}

/// Map one OpenCode SSE event onto the normalized stream.
fn event(
    event: &Value,
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
) {
    let kind = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let properties = event.get("properties").unwrap_or(&Value::Null);
    // A retained server carries traffic for more than one turn. Fail closed:
    // every event that can mutate or finish a turn must identify the current
    // session, so delayed or unrelated traffic cannot leak across turns.
    let reported = properties.get("sessionID").and_then(Value::as_str);
    if turn.retained {
        if reported
            .zip(turn.session_id.as_deref())
            .is_none_or(|(a, b)| a != b)
        {
            return;
        }
    } else if let (Some(reported), Some(current)) = (reported, turn.session_id.as_deref()) {
        if reported != current {
            return;
        }
    }
    match kind {
        "message.updated" => {
            let info = properties.get("info").unwrap_or(&Value::Null);
            if let (Some(id), Some(role)) = (
                info.get("id").and_then(Value::as_str),
                info.get("role").and_then(Value::as_str),
            ) {
                turn.roles.insert(id.to_string(), role.to_string());
            }
            if info.get("role").and_then(Value::as_str) == Some("assistant") {
                if let Some(cost) = info.get("cost").and_then(Value::as_f64) {
                    state.result.usage.cost_usd = Some(cost);
                }
            }
        }
        "message.part.updated" => {
            let part = properties.get("part").unwrap_or(&Value::Null);
            let message = part.get("messageID").and_then(Value::as_str);
            if message.is_none_or(|id| turn.roles.get(id).map(String::as_str) != Some("assistant"))
            {
                return;
            }
            let Some(part_id) = part.get("id").and_then(Value::as_str) else {
                return;
            };
            match part.get("type").and_then(Value::as_str) {
                Some(kind @ ("text" | "reasoning")) => {
                    let full = part.get("text").and_then(Value::as_str).unwrap_or_default();
                    if let Some(delta) = observe_full(turn, part_id, kind, full) {
                        emit_delta(kind, &delta, state, output);
                    }
                }
                Some("tool") => {
                    if let Some(event) = tool_event(part) {
                        turn.saw_activity = true;
                        output.events.push(event);
                    }
                }
                _ => {}
            }
        }
        "message.part.delta" => {
            let message = properties.get("messageID").and_then(Value::as_str);
            if message.is_none_or(|id| turn.roles.get(id).map(String::as_str) != Some("assistant"))
            {
                return;
            }
            let (Some(part_id), Some(delta)) = (
                properties.get("partID").and_then(Value::as_str),
                properties.get("delta").and_then(Value::as_str),
            ) else {
                return;
            };
            if delta.is_empty() {
                return;
            }
            let kind = observe_delta(turn, part_id, delta);
            emit_delta(&kind, delta, state, output);
        }
        "permission.asked" => permission(properties, turn, output),
        "session.error" => {
            turn.error_message = properties
                .pointer("/error/data/message")
                .or_else(|| properties.pointer("/error/message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or(turn.error_message.take());
        }
        "session.idle" => {
            output.terminal = true;
            if let Some(message) = turn.error_message.clone() {
                fail(turn, state, output, &message, None);
            } else if state.result.text.trim().is_empty() && !turn.saw_activity {
                // A turn that produced no text and no tool call is a real
                // failure, not an empty success: it is what a silently
                // auto-refused edit looked like before permissions were
                // enforced, and reporting it as success hid exactly the
                // problem this transport exists to solve.
                fail(
                    turn,
                    state,
                    output,
                    "OpenCode finished without producing a reply.",
                    None,
                );
            }
        }
        _ => {}
    }
}

/// Surface a permission request, or refuse it outright when the turn's policy
/// says no decision is available to make.
fn permission(properties: &Value, turn: &TurnState, output: &mut AdapterOutput) {
    let Some(id) = properties.get("id").and_then(Value::as_str) else {
        return;
    };
    let kind = properties
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or("tool");
    let patterns = properties
        .get("patterns")
        .and_then(Value::as_array)
        .map(|patterns| {
            patterns
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|patterns| !patterns.is_empty());
    let (tool_name, description) = describe(kind, patterns.as_deref());

    let request = ApprovalRequest {
        id: id.to_string(),
        tool_name,
        description: Some(description),
        input: properties.clone(),
    };
    // Defence in depth. A plan turn and an empty tool allowlist are already
    // denied by the configuration the server was started with, so reaching
    // here means the policy did not hold. Refusing without asking keeps the
    // guarantee even then, and never presents the user a choice that the turn
    // promised would not exist.
    if turn.plan_mode || turn.tools_denied {
        output
            .events
            .push(TurnEvent::ApprovalRequested(request.clone()));
        if let Ok(frame) = answer_frame(
            turn.session_id.as_deref().unwrap_or_default(),
            &turn.directory,
            id,
            ApprovalDecision::Deny { reason: None },
        ) {
            output.writes.push(frame);
        }
        return;
    }
    output
        .events
        .push(TurnEvent::ApprovalRequested(request.clone()));
    // The reply is addressed to a session and a workspace, and
    // `approval_response` sees only this value, so both travel with it.
    output.interaction = Some(InteractionRequest::Approval {
        request,
        original: json!({
            "id": id,
            "session": turn.session_id,
            "directory": turn.directory,
        }),
    });
}

/// Name and describe a permission the way the reference driver does.
fn describe(kind: &str, patterns: Option<&str>) -> (String, String) {
    match (kind, patterns) {
        ("bash", Some(patterns)) => ("Bash".into(), format!("run `{patterns}`")),
        ("bash", None) => ("Bash".into(), "run a shell command".into()),
        ("edit", Some(patterns)) => ("Edit".into(), format!("edit {patterns}")),
        ("edit", None) => ("Edit".into(), "edit files".into()),
        (kind, Some(patterns)) => (kind.into(), format!("use {kind}: {patterns}")),
        (kind, None) => (kind.into(), format!("use {kind}")),
    }
}

/// Encode the HTTP request that answers one permission request.
fn answer_frame(
    session: &str,
    directory: &str,
    permission_id: &str,
    decision: ApprovalDecision,
) -> Result<Vec<u8>> {
    let response = match decision {
        ApprovalDecision::Allow => "once",
        ApprovalDecision::AllowForSession => "always",
        ApprovalDecision::Deny { .. } => "reject",
    };
    encode(&post(
        None,
        format!(
            "/session/{}/permissions/{}?directory={}",
            query_escape(session),
            query_escape(permission_id),
            query_escape(directory)
        ),
        json!({"response": response}),
    ))
}

/// Encode an application's decision for the runtime to write.
///
/// Everything needed to address the reply travels on `original`, which is the
/// only value this is given.
pub(super) fn approval_response(original: &Value, decision: ApprovalDecision) -> Result<Vec<u8>> {
    let text = |field: &str| {
        original
            .get(field)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    answer_frame(&text("session"), &text("directory"), &text("id"), decision)
}

/// OpenCode has no question channel, so nothing is ever encoded for one.
pub(super) fn question_response(
    _request: &QuestionRequest,
    _original: &Value,
    _answer: Option<QuestionAnswer>,
) -> Option<Vec<u8>> {
    None
}

/// A `message.part.updated` carries a part's whole current text; return only
/// the genuinely new suffix so a later resend cannot double-count what a
/// `message.part.delta` already streamed.
fn observe_full(turn: &mut TurnState, part_id: &str, kind: &str, full: &str) -> Option<String> {
    turn.part_kinds
        .insert(part_id.to_string(), kind.to_string());
    let previous = turn.emitted.get(part_id).cloned().unwrap_or_default();
    if !full.starts_with(&previous) {
        return None;
    }
    let delta = full[previous.len()..].to_string();
    if delta.is_empty() {
        return None;
    }
    turn.emitted.insert(part_id.to_string(), full.to_string());
    Some(delta)
}

/// Record an incremental delta against the same tracker, so a later full-value
/// resend does not re-emit it.
fn observe_delta(turn: &mut TurnState, part_id: &str, delta: &str) -> String {
    let kind = turn
        .part_kinds
        .get(part_id)
        .cloned()
        .unwrap_or_else(|| "text".to_string());
    turn.emitted
        .entry(part_id.to_string())
        .or_default()
        .push_str(delta);
    kind
}

fn emit_delta(kind: &str, delta: &str, state: &mut AdapterState, output: &mut AdapterOutput) {
    if kind == "reasoning" {
        state
            .result
            .reasoning
            .get_or_insert_with(String::new)
            .push_str(delta);
        output.events.push(TurnEvent::ReasoningDelta {
            text: delta.to_string(),
        });
    } else {
        state.result.text.push_str(delta);
        state.saw_text_delta = true;
        output.events.push(TurnEvent::TextDelta {
            text: delta.to_string(),
        });
    }
}

fn truncate(text: &str) -> String {
    if text.chars().count() <= MAX_TOOL_OUTPUT_CHARS {
        return text.to_string();
    }
    let mut bounded: String = text.chars().take(MAX_TOOL_OUTPUT_CHARS).collect();
    bounded.push_str("… [truncated]");
    bounded
}

fn tool_event(part: &Value) -> Option<TurnEvent> {
    let id = part.get("callID").and_then(Value::as_str)?;
    let name = part.get("tool").and_then(Value::as_str).unwrap_or("tool");
    let state = part.get("state").unwrap_or(&Value::Null);
    let status = state
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("running");
    let text = state.get("output").and_then(Value::as_str).map(truncate);
    let (status, out, error) = match status {
        "completed" => (ToolCallStatus::Succeeded, text, None),
        "error" => (
            ToolCallStatus::Failed,
            None,
            Some(text.unwrap_or_else(|| "Tool call failed.".to_string())),
        ),
        _ => (ToolCallStatus::Started, None, None),
    };
    Some(TurnEvent::ToolCall {
        id: Some(id.to_string()),
        name: name.to_string(),
        status,
        input: state.get("input").cloned(),
        output: out,
        error,
        task_id: None,
    })
}

/// Record a provider-native terminal failure.
fn fail(
    turn: &mut TurnState,
    state: &mut AdapterState,
    output: &mut AdapterOutput,
    message: &str,
    code: Option<&str>,
) {
    let kind = classify_provider_failure(&format!("{} {message}", code.unwrap_or_default()));
    let mut failure = ProviderTerminalFailure::new(kind, message, DeliveryState::Accepted);
    if let Some(code) = code {
        failure = failure.with_provider_code(format!("opencode::{code}"));
    }
    turn.error_message = Some(message.to_string());
    state.terminal_failure = Some(failure);
    state.result.status = RunStatus::Failed;
    output.terminal = true;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn decode(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn request(mode: PermissionMode) -> TurnRequest {
        let mut request = TurnRequest::new(Provider::OpenCode, "/tmp/work", "do the thing");
        request.permission_mode = mode;
        request
    }

    /// Drive the state machine to the point where a turn is running.
    fn running_turn(mode: PermissionMode) -> AdapterState {
        let request = request(mode);
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);
        parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();
        parse_line(
            &json!({"type": FRAME_RESPONSE, "id": ID_SESSION, "status": 200,
                    "body": {"id": "session-1"}})
            .to_string(),
            &mut state,
        )
        .unwrap();
        parse_line(&json!({"type": FRAME_SUBSCRIBED}).to_string(), &mut state).unwrap();
        state
    }

    fn permission_event(kind: &str) -> String {
        json!({"type": FRAME_EVENT, "event": {
            "type": "permission.asked",
            "properties": {
                "id": "permission-9", "sessionID": "session-1",
                "permission": kind, "patterns": ["rm -rf /"]
            }
        }})
        .to_string()
    }

    #[test]
    fn each_permission_mode_maps_onto_opencodes_two_axes() {
        for (mode, edit, bash) in [
            (PermissionMode::Default, "ask", "ask"),
            (PermissionMode::AcceptEdits, "allow", "ask"),
            (PermissionMode::FullAccess, "allow", "allow"),
            (PermissionMode::Plan, "deny", "deny"),
        ] {
            let config: Value =
                serde_json::from_str(&permission_config(&request(mode.clone())).unwrap()).unwrap();
            assert_eq!(
                config["permission"]["edit"],
                json!(edit),
                "edit for {mode:?}"
            );
            assert_eq!(
                config["permission"]["bash"],
                json!(bash),
                "bash for {mode:?}"
            );
        }
    }

    #[test]
    fn an_empty_tool_allowlist_becomes_a_wildcard_denial() {
        let mut request = request(PermissionMode::FullAccess);
        request.launch_context.allowed_tools = Some(Vec::new());

        let config: Value = serde_json::from_str(&permission_config(&request).unwrap()).unwrap();

        assert_eq!(
            config["permission"],
            json!({"*": "deny"}),
            "an explicitly empty tool set must be enforced, not merely suggested"
        );
    }

    #[test]
    fn a_permission_ask_reaches_the_application_and_its_answer_is_posted() {
        let mut state = running_turn(PermissionMode::Default);

        let output = parse_line(&permission_event("bash"), &mut state).unwrap();

        let Some(InteractionRequest::Approval { request, original }) = output.interaction else {
            panic!("expected an approval interaction, got {:?}", output.events);
        };
        assert_eq!(request.id, "permission-9");
        assert_eq!(request.tool_name, "Bash");
        assert!(request.description.unwrap().contains("rm -rf /"));

        let allow = decode(&approval_response(&original, ApprovalDecision::Allow).unwrap());
        assert_eq!(allow["method"], json!("POST"));
        assert_eq!(allow["body"]["response"], json!("once"));
        assert!(allow["path"]
            .as_str()
            .unwrap()
            .starts_with("/session/session-1/permissions/permission-9"));
    }

    #[test]
    fn a_denied_permission_is_refused_on_the_native_wire_vocabulary() {
        let original =
            json!({"id": "permission-9", "session": "session-1", "directory": "/tmp/work"});

        for (decision, expected) in [
            (ApprovalDecision::Allow, "once"),
            (ApprovalDecision::AllowForSession, "always"),
            (ApprovalDecision::Deny { reason: None }, "reject"),
        ] {
            let frame = decode(&approval_response(&original, decision).unwrap());
            assert_eq!(frame["body"]["response"], json!(expected));
        }
    }

    #[test]
    fn a_plan_turn_refuses_a_permission_without_consulting_the_application() {
        let mut state = running_turn(PermissionMode::Plan);

        let output = parse_line(&permission_event("edit"), &mut state).unwrap();

        assert!(
            output.interaction.is_none(),
            "a plan turn promised no side effects, so there is no decision to offer"
        );
        let refusal = decode(&output.writes[0]);
        assert_eq!(refusal["body"]["response"], json!("reject"));
        assert!(matches!(
            output.events.as_slice(),
            [TurnEvent::ApprovalRequested(request)] if request.id == "permission-9"
        ));
    }

    #[test]
    fn an_empty_tool_allowlist_also_refuses_without_asking() {
        let mut request = request(PermissionMode::FullAccess);
        request.launch_context.allowed_tools = Some(Vec::new());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);
        parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();
        parse_line(
            &json!({"type": FRAME_RESPONSE, "id": ID_SESSION, "status": 200,
                    "body": {"id": "session-1"}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        let output = parse_line(&permission_event("bash"), &mut state).unwrap();

        assert!(output.interaction.is_none());
        assert_eq!(
            decode(&output.writes[0])["body"]["response"],
            json!("reject")
        );
    }

    #[test]
    fn the_prompt_waits_for_the_event_stream_to_be_open() {
        let request = request(PermissionMode::Default);
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);

        parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();
        let opened = parse_line(
            &json!({"type": FRAME_RESPONSE, "id": ID_SESSION, "status": 200,
                    "body": {"id": "session-1"}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert_eq!(
            decode(&opened.writes[0])["method"],
            json!("SUBSCRIBE"),
            "subscribing first is what keeps early events from being missed"
        );
        assert!(
            opened.writes.len() == 1,
            "the prompt must not be sent before the stream is open"
        );

        let prompted =
            parse_line(&json!({"type": FRAME_SUBSCRIBED}).to_string(), &mut state).unwrap();
        let frame = decode(&prompted.writes[0]);
        assert_eq!(frame["method"], json!("POST"));
        assert!(frame["path"].as_str().unwrap().contains("/message"));
        assert_eq!(frame["body"]["parts"][0]["text"], json!("do the thing"));
    }

    #[test]
    fn a_subagent_session_fails_fast_instead_of_hanging() {
        let mut request = request(PermissionMode::Default);
        request.session_id = Some("child-1".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);
        parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();

        let output = parse_line(
            &json!({"type": FRAME_RESPONSE, "id": ID_SESSION, "status": 200,
                    "body": {"id": "child-1", "parentID": "parent-1"}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.terminal);
        assert_eq!(state.result.status, RunStatus::Failed);
        assert!(state
            .terminal_failure
            .unwrap()
            .diagnostic
            .contains("subagent"));
    }

    #[test]
    fn a_server_that_dies_mid_turn_fails_the_turn_with_its_diagnostic() {
        let mut state = running_turn(PermissionMode::Default);

        let output = parse_line(
            &json!({"type": FRAME_ERROR,
                    "message": "OpenCode's event stream ended before the turn finished."})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.terminal);
        assert_eq!(state.result.status, RunStatus::Failed);
        assert!(state
            .terminal_failure
            .unwrap()
            .diagnostic
            .contains("ended before the turn finished"));
    }

    #[test]
    fn only_assistant_text_is_streamed_and_never_counted_twice() {
        let mut state = running_turn(PermissionMode::Default);
        let role = |id: &str, role: &str| {
            json!({"type": FRAME_EVENT, "event": {"type": "message.updated", "properties": {
                "sessionID": "session-1", "info": {"id": id, "role": role}
            }}})
            .to_string()
        };
        parse_line(&role("m-user", "user"), &mut state).unwrap();
        parse_line(&role("m-1", "assistant"), &mut state).unwrap();

        let user = parse_line(
            &json!({"type": FRAME_EVENT, "event": {"type": "message.part.updated", "properties": {
                "sessionID": "session-1",
                "part": {"id": "p-0", "messageID": "m-user", "type": "text", "text": "echo"}
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();
        assert!(
            user.events.is_empty(),
            "the user's own message is not output"
        );

        let first = parse_line(
            &json!({"type": FRAME_EVENT, "event": {"type": "message.part.updated", "properties": {
                "sessionID": "session-1",
                "part": {"id": "p-1", "messageID": "m-1", "type": "text", "text": "Hello"}
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();
        assert_eq!(
            first.events,
            vec![TurnEvent::TextDelta {
                text: "Hello".into()
            }]
        );

        // The same part resent in full must only yield the new suffix.
        let second = parse_line(
            &json!({"type": FRAME_EVENT, "event": {"type": "message.part.updated", "properties": {
                "sessionID": "session-1",
                "part": {"id": "p-1", "messageID": "m-1", "type": "text", "text": "Hello there"}
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();
        assert_eq!(
            second.events,
            vec![TurnEvent::TextDelta {
                text: " there".into()
            }]
        );
        assert_eq!(state.result.text, "Hello there");
    }

    #[test]
    fn another_sessions_events_are_not_charged_to_this_turn() {
        let mut state = running_turn(PermissionMode::Default);

        let output = parse_line(
            &json!({"type": FRAME_EVENT, "event": {"type": "message.part.delta", "properties": {
                "sessionID": "session-other", "messageID": "m-1",
                "partID": "p-1", "delta": "leak"
            }}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.events.is_empty());
        assert_eq!(state.result.text, "");
    }

    #[test]
    fn an_idle_turn_that_produced_nothing_is_a_failure_not_an_empty_success() {
        let mut state = running_turn(PermissionMode::Default);

        let output = parse_line(
            &json!({"type": FRAME_EVENT, "event": {"type": "session.idle",
                    "properties": {"sessionID": "session-1"}}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        assert!(output.terminal);
        assert_eq!(state.result.status, RunStatus::Failed);
    }

    #[test]
    fn stdio_and_remote_mcp_servers_reach_the_config_without_their_secrets() {
        let mut request = request(PermissionMode::Default);
        request.launch_context.mcp_servers.insert(
            "temps_fleet".into(),
            crate::McpServerConfig::Stdio {
                command: "/opt/tools/temps fleet".into(),
                args: vec!["mcp".into(), "serve".into()],
                environment_from: BTreeMap::from([("TOKEN".into(), "FLEET_TOKEN".into())]),
            },
        );
        request.launch_context.mcp_servers.insert(
            "platform".into(),
            crate::McpServerConfig::Http {
                url: "https://relay.example.test/mcp".into(),
                headers_from: BTreeMap::from([("Authorization".into(), "RELAY_TOKEN".into())]),
            },
        );

        let config: Value = serde_json::from_str(&permission_config(&request).unwrap()).unwrap();

        let fleet = &config["mcp"]["temps_fleet"];
        assert_eq!(fleet["type"], json!("local"));
        assert_eq!(
            fleet["command"],
            json!(["/opt/tools/temps fleet", "mcp", "serve"]),
            "an argv array needs no quoting, so a spaced path stays one argument"
        );
        assert_eq!(fleet["environment"]["TOKEN"], json!("{env:FLEET_TOKEN}"));
        assert_eq!(fleet["enabled"], json!(true));

        let platform = &config["mcp"]["platform"];
        assert_eq!(platform["type"], json!("remote"));
        assert_eq!(
            platform["headers"]["Authorization"],
            json!("{env:RELAY_TOKEN}")
        );
        // The permission policy still merges alongside the MCP block.
        assert_eq!(config["permission"]["edit"], json!("ask"));
    }

    #[test]
    fn the_launch_context_is_carried_as_a_prompt_prefix() {
        let mut request = request(PermissionMode::Default);
        request.launch_context.system_prompt_append = Some("Answer in French.".into());
        request.launch_context.allowed_tools = Some(vec!["read".into(), "grep".into()]);
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);

        let turn = load(&state);
        let text = turn.parts[0]["text"].as_str().unwrap();
        assert!(text.starts_with("Answer in French."));
        assert!(text.contains("Use only these tools: read, grep"));
        assert!(text.ends_with("do the thing"));
    }

    #[test]
    fn a_workspace_path_with_separators_cannot_rewrite_the_query() {
        let mut request = TurnRequest::new(Provider::OpenCode, "/tmp/a b&c?d", "hi");
        request.permission_mode = PermissionMode::Default;
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);

        let output = parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();

        let path = decode(&output.writes[0])["path"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(path.contains("%26"), "`&` must be escaped in {path}");
        assert!(path.contains("%3F"), "`?` must be escaped in {path}");
        assert_eq!(
            path.matches('?').count(),
            1,
            "only the query starts a query"
        );
    }

    #[test]
    fn a_running_turn_can_be_interrupted_on_its_own_session() {
        let state = running_turn(PermissionMode::Default);

        let frame = decode(&interrupt(&state).unwrap());

        assert_eq!(frame["method"], json!("POST"));
        assert!(frame["path"]
            .as_str()
            .unwrap()
            .starts_with("/session/session-1/abort"));
    }

    #[test]
    fn a_model_identifier_is_split_into_opencodes_native_selection() {
        let mut request = request(PermissionMode::Default);
        request.model = Some("anthropic/claude-sonnet-4".into());
        let mut state = AdapterState::default();
        prepare_turn(&request, &mut state, 4242);
        parse_line(&json!({"type": FRAME_READY}).to_string(), &mut state).unwrap();
        parse_line(
            &json!({"type": FRAME_RESPONSE, "id": ID_SESSION, "status": 200,
                    "body": {"id": "session-1"}})
            .to_string(),
            &mut state,
        )
        .unwrap();

        let prompted =
            parse_line(&json!({"type": FRAME_SUBSCRIBED}).to_string(), &mut state).unwrap();

        assert_eq!(
            decode(&prompted.writes[0])["body"]["model"],
            json!({"providerID": "anthropic", "modelID": "claude-sonnet-4"})
        );
    }
}
