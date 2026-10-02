use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter::{inspect_executable, resolve_executable, AdapterState};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    AccountCredits, AccountUsageProbeSpec, AccountUsageReport, AccountUsageSnapshot,
    AccountUsageWindow, AccountUsageWindowKind, AdapterOutput, AgentAdapter, AgentTask,
    AgentTaskActivity, AgentTaskActivityKind, AgentTaskUsage, AgentTranscriptEntry, AgentWorkflow,
    AgentWorkflowAgent, AgentWorkflowAgentState, AgentWorkflowPhase, ApprovalDecision,
    ApprovalRequest, AuthenticationProbeSpec, AutoCompactionPolicy, CatalogProbeSpec, CommandSpec,
    CompactionTrigger, ContextCompaction, ContextWindowUsage, HarnessAuthentication,
    HarnessCatalogStatus, HarnessControlGroup, HarnessControlKind, HarnessControlOption,
    HarnessModel, HarnessModelCatalog, HarnessReasoningEffort, InteractionRequest,
    LaunchContextCapabilities, McpServerConfig, PermissionMode, PermissionSupport, Provider,
    ProviderReadiness, ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result, RunStatus,
    RuntimeError, ToolCallStatus, TransportExitStatus, TurnCapabilities, TurnEvent, TurnRequest,
    Usage,
};

const CLAUDE_STATE_KEY: &str = "claude.native_tasks";
const MAX_NATIVE_TASKS: usize = 32;
const MAX_TASK_FIELD_CHARS: usize = 4_000;
/// Agents kept per workflow; the rest are counted in `omitted_agents`.
const MAX_WORKFLOW_AGENTS: usize = 100;
/// Phases kept per workflow.
const MAX_WORKFLOW_PHASES: usize = 32;
/// Most recent script log lines kept per workflow.
const MAX_WORKFLOW_LOGS: usize = 10;
/// Bound for each workflow text field (labels, previews, log lines). Every
/// update carries the whole workflow, so these stay short.
const MAX_WORKFLOW_TEXT_CHARS: usize = 240;
/// A workflow tick that only moves counters (tokens, tool calls, latest
/// tool) is re-emitted once per this many ticks; any other change at once.
const WORKFLOW_COUNTER_TICKS: u32 = 5;
/// How long a retained turn waits for Claude's follow-up answer after its
/// background tasks drain. Claude starts it immediately after the task
/// notification, so this only bounds a notification it does not answer.
const FOLLOW_UP_GRACE: Duration = Duration::from_secs(3);
/// Request ID of the native interrupt the runtime sends to a retained turn.
const INTERRUPT_REQUEST_ID: &str = "temps-agent-runtime-interrupt";
/// Upper bound on user messages one turn may submit, its prompt included.
const MAX_TURN_MESSAGES: usize = 32;

// This helper runs on the selected execution host. It reads Claude Code's
// existing OAuth credential without modifying it and prints only normalized
// quota metadata. The access and refresh tokens never cross stdout or argv.
const CLAUDE_ACCOUNT_USAGE_SCRIPT: &str = r#"
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const childProcess = require("node:child_process");

const marker = "temps_agent_runtime_account_usage";
const maxCredentialBytes = 1024 * 1024;
const maxResponseBytes = 256 * 1024;

function emit(value) {
  process.stdout.write(JSON.stringify({ type: marker, ...value }) + "\n");
}

function unavailable(reason, retryable) {
  emit({ status: "unavailable", reason, retryable });
}

function parseCredential(raw) {
  try {
    const parsed = JSON.parse(raw);
    const oauth = parsed && parsed.claudeAiOauth;
    return oauth && typeof oauth.accessToken === "string" ? oauth : null;
  } catch {
    return null;
  }
}

function fileCredential() {
  const root = process.env.CLAUDE_HOME || path.join(os.homedir(), ".claude");
  const file = path.join(root, ".credentials.json");
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile() || stat.size > maxCredentialBytes) return null;
    return parseCredential(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function keychainCredential() {
  if (process.platform !== "darwin") return null;
  const account = os.userInfo().username;
  const lookups = [
    ["find-generic-password", "-a", account, "-w", "-s", "Claude Code-credentials"],
    ["find-generic-password", "-w", "-s", "Claude Code-credentials"],
  ];
  for (const args of lookups) {
    try {
      const result = childProcess.spawnSync("/usr/bin/security", args, {
        encoding: "utf8",
        timeout: 2000,
        maxBuffer: maxCredentialBytes,
        stdio: ["ignore", "pipe", "ignore"],
      });
      if (result.status === 0) {
        const credential = parseCredential(result.stdout);
        if (credential) return credential;
      }
    } catch {}
  }
  return null;
}

function number(value) {
  if (typeof value !== "number" && (typeof value !== "string" || value.trim() === "")) return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function resetSeconds(value) {
  const numeric = number(value);
  if (numeric !== null) return Math.floor(numeric > 100000000000 ? numeric / 1000 : numeric);
  if (typeof value !== "string") return null;
  const millis = Date.parse(value);
  return Number.isFinite(millis) ? Math.floor(millis / 1000) : null;
}

function planLabel(oauth) {
  if (typeof oauth.subscriptionType !== "string" || !oauth.subscriptionType) return null;
  const subscription = oauth.subscriptionType[0].toUpperCase() + oauth.subscriptionType.slice(1);
  const tier = typeof oauth.rateLimitTier === "string" ? oauth.rateLimitTier.split("_").pop() : null;
  return tier ? `${subscription} ${tier}` : subscription;
}

function window(id, kind, label, value, durationMinutes) {
  if (!value || typeof value !== "object") return null;
  const used = number(value.utilization ?? value.percent);
  if (used === null) return null;
  return {
    id,
    ...(label ? { label } : {}),
    kind,
    used_percent: used,
    duration_minutes: durationMinutes,
    resets_at_unix_seconds: resetSeconds(value.resets_at ?? value.resetsAt),
  };
}

function normalizedWindows(body) {
  const windows = [];
  const known = [
    ["five_hour", "session", null, 300],
    ["seven_day", "weekly", null, 10080],
    ["seven_day_opus", "weekly", "Opus", 10080],
    ["seven_day_sonnet", "weekly", "Sonnet", 10080],
    ["seven_day_omelette", "weekly", "Omelette", 10080],
  ];
  for (const [id, kind, label, duration] of known) {
    const parsed = window(id, kind, label, body[id], duration);
    if (parsed) windows.push(parsed);
  }
  if (Array.isArray(body.limits)) {
    for (const limit of body.limits) {
      if (!limit || limit.kind !== "weekly_scoped") continue;
      const dimension = limit.scope && limit.scope.model ? "model" : "surface";
      const scope = limit.scope && limit.scope[dimension];
      if (!scope || typeof scope !== "object") continue;
      const nativeId = typeof scope.id === "string" && scope.id ? scope.id : null;
      const label = typeof scope.display_name === "string" && scope.display_name
        ? scope.display_name
        : nativeId;
      if (!label) continue;
      const id = `weekly_scoped:${dimension}:${nativeId || label.toLowerCase().replace(/[^a-z0-9]+/g, "_")}`;
      const parsed = window(id, "weekly", label, limit, 10080);
      if (!parsed) continue;
      const existing = windows.findIndex((candidate) => candidate.id === id);
      if (existing === -1) windows.push(parsed);
      else windows[existing] = parsed;
    }
  }
  return windows;
}

async function main() {
  const oauth = fileCredential() || keychainCredential();
  if (!oauth) {
    unavailable("Claude Code is not authenticated on this execution host", false);
    return;
  }
  let response;
  try {
    response = await fetch("https://api.anthropic.com/api/oauth/usage", {
      headers: {
        Authorization: `Bearer ${oauth.accessToken}`,
        Accept: "application/json",
        "anthropic-beta": "oauth-2025-04-20",
      },
      redirect: "error",
      signal: AbortSignal.timeout(10000),
    });
  } catch {
    unavailable("The Claude account-usage endpoint could not be reached", true);
    return;
  }
  if (response.status === 401 || response.status === 403) {
    unavailable("Claude Code authentication is expired or was rejected", false);
    return;
  }
  if (!response.ok) {
    unavailable(`The Claude account-usage endpoint returned HTTP ${response.status}`, response.status >= 500 || response.status === 429);
    return;
  }
  const declaredLength = number(response.headers.get("content-length"));
  if (declaredLength !== null && declaredLength > maxResponseBytes) {
    unavailable("The Claude account-usage response exceeded the SDK limit", false);
    return;
  }
  if (!response.body) {
    unavailable("Claude returned an empty account-usage response", true);
    return;
  }
  const reader = response.body.getReader();
  const chunks = [];
  let responseBytes = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    responseBytes += value.byteLength;
    if (responseBytes > maxResponseBytes) {
      await reader.cancel();
      unavailable("The Claude account-usage response exceeded the SDK limit", false);
      return;
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(responseBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  let body;
  try {
    body = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    unavailable("Claude returned malformed account-usage data", false);
    return;
  }
  const windows = normalizedWindows(body);
  if (windows.length === 0) {
    unavailable("Claude returned no account quota windows", true);
    return;
  }
  emit({
    status: "available",
    usage: {
      provider: "claude",
      plan: planLabel(oauth),
      windows,
      credits: null,
    },
  });
}

main().catch(() => unavailable("Claude account usage could not be read", true));
"#;

fn claude_effort_label(effort: &str) -> &str {
    match effort {
        "off" => "Off",
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "Extra high",
        "max" => "Max",
        "ultracode" => "Ultra code",
        value => value,
    }
}

fn claude_catalog_model(
    model: &Value,
    default_resolved: Option<&str>,
    ultracode_available: bool,
) -> Option<HarnessModel> {
    let id = model.get("value")?.as_str()?;
    let label = model
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(id);
    let native_effort_ids = model
        .get("supportedEffortLevels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let default_effort = native_effort_ids
        .iter()
        .copied()
        .find(|effort| *effort == "high")
        .or_else(|| native_effort_ids.first().copied());
    let resolved = model.get("resolvedModel").and_then(Value::as_str);
    let supports_adaptive_thinking = model
        .get("supportsAdaptiveThinking")
        .and_then(Value::as_bool)
        == Some(true);
    let supports_disabled_thinking = supports_adaptive_thinking
        && !resolved.is_some_and(|model| model.contains("fable") || model.contains("mythos"));
    let mut effort_ids = Vec::with_capacity(native_effort_ids.len() + 2);
    if supports_disabled_thinking {
        effort_ids.push("off");
    }
    effort_ids.extend(native_effort_ids.iter().copied());
    if ultracode_available && native_effort_ids.contains(&"xhigh") {
        effort_ids.push("ultracode");
    }
    Some(HarnessModel {
        id: id.into(),
        label: label.into(),
        description: model
            .get("description")
            .and_then(Value::as_str)
            .filter(|description| !description.is_empty())
            .map(str::to_string),
        context_window_tokens: advertised_context_window(model),
        is_default: default_resolved.is_some() && resolved == default_resolved,
        reasoning_efforts: effort_ids
            .into_iter()
            .map(|effort| HarnessReasoningEffort {
                id: effort.into(),
                label: claude_effort_label(effort).into(),
                description: match effort {
                    "off" => Some("Disable adaptive thinking for this session.".into()),
                    "ultracode" => Some(
                        "Use xhigh effort with Claude Code's dynamic workflow orchestration for this session."
                            .into(),
                    ),
                    _ => None,
                },
                is_default: Some(effort) == default_effort,
            })
            .collect(),
        service_tiers: Vec::new(),
    })
}

fn advertised_context_window(model: &Value) -> Option<u64> {
    if let Some(tokens) = model
        .get("contextWindow")
        .or_else(|| model.get("context_window"))
        .or_else(|| model.get("contextWindowTokens"))
        .and_then(Value::as_u64)
    {
        return Some(tokens);
    }
    ["value", "resolvedModel", "displayName", "description"]
        .into_iter()
        .filter_map(|key| model.get(key).and_then(Value::as_str))
        .find_map(context_window_from_label)
}

fn context_window_from_label(value: &str) -> Option<u64> {
    let normalized = value.to_ascii_lowercase();
    if normalized.contains("[1m]") || normalized.contains("1m context") {
        Some(1_000_000)
    } else if normalized.contains("[200k]") || normalized.contains("200k context") {
        Some(200_000)
    } else {
        None
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ClaudeNativeState {
    tasks: BTreeMap<String, AgentTask>,
    background_task_ids: BTreeSet<String>,
    tool_use_to_task: BTreeMap<String, String>,
    tool_names: BTreeMap<String, String>,
    effective_permission_mode: Option<PermissionMode>,
    permission_mode_before_plan: Option<PermissionMode>,
    result_seen: bool,
    /// This turn is an explicit `/compact` request, so compaction it reports
    /// is manual rather than automatic.
    manual_compaction_turn: bool,
    /// Trigger of the compaction Claude reported as in progress, if any.
    open_compaction: Option<CompactionTrigger>,
    /// Claude reported the open compaction succeeded; its boundary follows.
    open_compaction_succeeded: bool,
    /// This turn runs on a retained process that outlives it, so background
    /// work does not need the turn to keep stdin open.
    retained_turn: bool,
    /// Claude started a follow-up turn of its own (a task notification
    /// woke the parent agent) after this turn's first result.
    follow_up_active: bool,
    /// Every background task finished after the result; Claude normally
    /// answers each notification with a follow-up turn that may still start.
    awaiting_follow_up: bool,
    /// Lifecycle of each user message this turn submitted, keyed by the
    /// message UUID Claude echoes in `command_lifecycle` frames.
    own_commands: BTreeMap<String, String>,
    /// The CLI reports `command_lifecycle`, so turn completion can wait for
    /// this turn's own messages instead of trusting the first `result`.
    lifecycle_seen: bool,
    /// Claude acknowledged the runtime's native interrupt.
    interrupting: bool,
    /// Workflow ticks since each workflow task's snapshot was last emitted.
    #[serde(default)]
    workflow_quiet_ticks: BTreeMap<String, u32>,
    /// Workflow tasks whose counters changed since their snapshot was last
    /// emitted.
    #[serde(default)]
    workflow_unsent: BTreeSet<String>,
}

impl ClaudeNativeState {
    /// Every message this turn submitted has finished (answered or cancelled).
    ///
    /// A message written but not yet reported (`pending`) is unfinished: a
    /// reply Claude was already composing can arrive before it is queued.
    fn own_commands_done(&self) -> bool {
        !self.lifecycle_seen
            || self
                .own_commands
                .values()
                .all(|state| !matches!(state.as_str(), "pending" | "queued" | "started"))
    }

    /// This turn's own exchange is over: Claude has answered every message the
    /// turn submitted and is not composing an answer of its own.
    fn answered(&self) -> bool {
        self.result_seen && !self.follow_up_active && self.own_commands_done()
    }
}

/// Generate a random RFC 4122 version 4 UUID for a submitted user message.
///
/// Claude stores it as the transcript message ID, so it must be unique; it
/// need not be unpredictable.
fn new_message_uuid() -> String {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let mut bytes = [0_u8; 16];
    for (half, chunk) in bytes.chunks_mut(8).enumerate() {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(sequence);
        hasher.write_u128(now);
        hasher.write_usize(half);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Encode one stream-JSON user message carrying `uuid` for lifecycle tracking.
fn user_message_frame(text: &str, uuid: Option<&str>) -> Result<Vec<u8>> {
    let mut frame = json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": text}]
        },
        "parent_tool_use_id": null
    });
    if let Some(uuid) = uuid {
        frame["uuid"] = Value::String(uuid.to_string());
    }
    serde_json::to_vec(&frame).map_err(|error| RuntimeError::Protocol {
        provider: Provider::Claude,
        message: format!("could not encode user message: {error}"),
    })
}

/// Claude's stop request.
///
/// `cancel_queued` (the `interrupt_cancel_queued_v1` capability) cancels the
/// messages queued behind the turn atomically with the abort, so none of them
/// starts afterwards. A CLI without that capability treats the request as a
/// plain interrupt; the turn then stops each queued message as it starts.
fn interrupt_frame() -> Option<Vec<u8>> {
    serde_json::to_vec(&json!({
        "type": "control_request",
        "request_id": INTERRUPT_REQUEST_ID,
        "request": { "subtype": "interrupt", "cancel_queued": true }
    }))
    .ok()
}

fn claude_permission_mode(value: &str) -> PermissionMode {
    match value {
        "default" | "manual" => PermissionMode::Default,
        "acceptEdits" => PermissionMode::AcceptEdits,
        "plan" => PermissionMode::Plan,
        "bypassPermissions" => PermissionMode::FullAccess,
        other => PermissionMode::Custom(other.to_string()),
    }
}

fn reported_permission_mode(value: &Value) -> Option<PermissionMode> {
    [
        "permissionMode",
        "permission_mode",
        "current_permission_mode",
    ]
    .into_iter()
    .find_map(|key| value.get(key).and_then(Value::as_str))
    .map(claude_permission_mode)
}

fn change_permission_mode(
    native: &mut ClaudeNativeState,
    output: &mut AdapterOutput,
    mode: PermissionMode,
) {
    if native.effective_permission_mode.as_ref() == Some(&mode) {
        return;
    }
    if mode == PermissionMode::Plan {
        if native.effective_permission_mode.as_ref() != Some(&PermissionMode::Plan) {
            native.permission_mode_before_plan = native.effective_permission_mode.clone();
        }
    } else if native.effective_permission_mode.as_ref() == Some(&PermissionMode::Plan) {
        native.permission_mode_before_plan = None;
    }
    native.effective_permission_mode = Some(mode.clone());
    output
        .events
        .push(TurnEvent::PermissionModeChanged { mode });
}

/// Claude Code CLI adapter using its bidirectional stream-JSON protocol.
#[derive(Debug, Clone, Default)]
pub struct Claude {
    executable: Option<PathBuf>,
}

impl Claude {
    /// Rebuild the activity recorded in a Claude transcript, such as a
    /// workflow agent's (see [`AgentWorkflow::agent_transcript_path`]), as
    /// the events a live turn would have emitted: assistant text, reasoning
    /// and tool calls with their results, bounded the same way.
    ///
    /// Tool inputs and outputs are returned as Claude recorded them, which
    /// can include secrets a tool read or printed; filter what you store or
    /// log.
    ///
    /// `transcript` is the JSONL file's contents. A transcript is written
    /// while the agent runs, so a line that is not complete JSON is skipped
    /// rather than treated as an error. Only the last `max_entries` entries
    /// are returned.
    #[must_use]
    pub fn transcript_activity(
        &self,
        transcript: &str,
        max_entries: usize,
    ) -> Vec<AgentTranscriptEntry> {
        let mut state = AdapterState::default();
        let mut entries = std::collections::VecDeque::new();
        for line in transcript.lines() {
            let Ok(frame) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if !matches!(
                frame.get("type").and_then(Value::as_str),
                Some("assistant" | "user")
            ) {
                continue;
            }
            let Ok(output) = self.parse_line(line, &mut state) else {
                continue;
            };
            let timestamp = frame
                .get("timestamp")
                .and_then(Value::as_str)
                .filter(|timestamp| timestamp.len() <= 64)
                .map(str::to_owned);
            // A live turn streams reasoning as deltas; a transcript records
            // it only as `thinking` blocks, which may be redacted to "".
            let reasoning = frame
                .pointer("/message/content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("thinking"))
                .filter_map(|block| block.get("thinking").and_then(Value::as_str))
                .filter(|text| !text.is_empty())
                .map(|text| TurnEvent::ReasoningDelta {
                    text: text.to_owned(),
                });
            for event in reasoning.chain(output.events) {
                if !matches!(
                    event,
                    TurnEvent::TextDelta { .. }
                        | TurnEvent::ReasoningDelta { .. }
                        | TurnEvent::ToolCall { .. }
                ) {
                    continue;
                }
                if entries.len() == max_entries {
                    entries.pop_front();
                }
                if max_entries > 0 {
                    entries.push_back(AgentTranscriptEntry {
                        timestamp: timestamp.clone(),
                        event,
                    });
                }
            }
        }
        entries.into()
    }

    /// Use an executable path meaningful inside the selected transport.
    pub fn with_executable(path: impl Into<PathBuf>) -> Self {
        Self {
            executable: Some(path.into()),
        }
    }

    fn resolved(&self) -> Option<PathBuf> {
        resolve_executable(self.executable.as_ref(), "claude")
    }

    fn configured_executable(&self) -> PathBuf {
        self.executable
            .clone()
            .unwrap_or_else(|| PathBuf::from("claude"))
    }
}

fn claude_mcp_config(request: &TurnRequest) -> Result<Option<String>> {
    let context = &request.launch_context;
    if context.mcp_servers.is_empty() && !context.strict_mcp_config {
        return Ok(None);
    }

    let mut servers = serde_json::Map::new();
    for (name, server) in &context.mcp_servers {
        let value = match server {
            McpServerConfig::Stdio {
                command,
                args,
                environment_from,
            } => {
                let environment = environment_from
                    .iter()
                    .map(|(target, source)| {
                        (target.clone(), Value::String(format!("${{{source}}}")))
                    })
                    .collect::<serde_json::Map<_, _>>();
                json!({
                    "type": "stdio",
                    "command": command,
                    "args": args,
                    "env": environment,
                })
            }
            McpServerConfig::Http { url, headers_from } => {
                let headers = headers_from
                    .iter()
                    .map(|(header, source)| {
                        (header.clone(), Value::String(format!("${{{source}}}")))
                    })
                    .collect::<serde_json::Map<_, _>>();
                json!({
                    "type": "http",
                    "url": url,
                    "headers": headers,
                })
            }
        };
        servers.insert(name.clone(), value);
    }

    serde_json::to_string(&json!({ "mcpServers": servers }))
        .map(Some)
        .map_err(|error| RuntimeError::Protocol {
            provider: Provider::Claude,
            message: format!("could not encode MCP launch configuration: {error}"),
        })
}

#[async_trait]
impl AgentAdapter for Claude {
    fn provider(&self) -> Provider {
        Provider::Claude
    }

    fn supports_retained_process(&self) -> bool {
        true
    }

    fn executable(&self) -> PathBuf {
        self.configured_executable()
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            accept_edits: true,
            plan: true,
            full_access: true,
            custom: true,
            live_approvals: true,
            live_questions: true,
        }
    }

    fn launch_context_capabilities(&self) -> LaunchContextCapabilities {
        LaunchContextCapabilities {
            system_prompt_append: true,
            allowed_tools: true,
            stdio_mcp: true,
            http_mcp: true,
            strict_mcp_config: true,
        }
    }

    fn control_groups(&self) -> Vec<HarnessControlGroup> {
        vec![HarnessControlGroup {
            id: "permission_mode".into(),
            label: "Permission".into(),
            kind: HarnessControlKind::Permission,
            options: [
                (
                    "manual",
                    "Always ask",
                    "Ask before edits and commands.",
                    true,
                    false,
                ),
                (
                    "acceptEdits",
                    "Accept file edits",
                    "Allow workspace edits; keep other prompts.",
                    false,
                    false,
                ),
                (
                    "plan",
                    "Plan mode",
                    "Explore and propose changes without editing.",
                    false,
                    false,
                ),
                (
                    "auto",
                    "Auto mode",
                    "Use Claude's background safety classifier.",
                    false,
                    false,
                ),
                (
                    "bypassPermissions",
                    "Bypass",
                    "Skip Claude's permission checks; use only inside isolation.",
                    false,
                    true,
                ),
            ]
            .into_iter()
            .map(
                |(id, label, description, is_default, dangerous)| HarnessControlOption {
                    id: id.into(),
                    label: label.into(),
                    description: description.into(),
                    is_default,
                    dangerous,
                },
            )
            .collect(),
        }]
    }

    fn catalog_probe(&self) -> Option<CatalogProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.extend([
            "--print".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--verbose".into(),
            "--input-format".into(),
            "stream-json".into(),
            "--tools".into(),
            "".into(),
            "--setting-sources=".into(),
        ]);
        command.initial_stdin = Some(
            serde_json::to_vec(&json!({
                "type": "control_request",
                "request_id": "temps-agent-runtime-model-catalog",
                "request": { "subtype": "initialize" }
            }))
            .expect("static Claude catalog request is serializable"),
        );
        Some(CatalogProbeSpec {
            command,
            expected_response_ids: None,
        })
    }

    fn parse_catalog(&self, lines: &[String]) -> Result<HarnessModelCatalog> {
        for line in lines {
            let Ok(frame) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if frame.get("type").and_then(Value::as_str) != Some("control_response")
                || frame
                    .pointer("/response/request_id")
                    .and_then(Value::as_str)
                    != Some("temps-agent-runtime-model-catalog")
            {
                continue;
            }
            if frame.pointer("/response/subtype").and_then(Value::as_str) != Some("success") {
                return Err(RuntimeError::Protocol {
                    provider: Provider::Claude,
                    message: frame
                        .pointer("/response/error")
                        .and_then(Value::as_str)
                        .unwrap_or("Claude Code rejected model discovery")
                        .to_string(),
                });
            }
            let response =
                frame
                    .pointer("/response/response")
                    .ok_or_else(|| RuntimeError::Protocol {
                        provider: Provider::Claude,
                        message: "Claude Code initialization omitted its response body".into(),
                    })?;
            let raw_models = response
                .get("models")
                .and_then(Value::as_array)
                .ok_or_else(|| RuntimeError::Protocol {
                    provider: Provider::Claude,
                    message: "Claude Code initialization did not return a model catalog".into(),
                })?;
            let default_resolved = raw_models
                .iter()
                .find(|model| model.get("value").and_then(Value::as_str) == Some("default"))
                .and_then(|model| model.get("resolvedModel"))
                .and_then(Value::as_str);
            let commands = response
                .get("commands")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let advertises_ultracode = commands.iter().any(|command| {
                command.get("name").and_then(Value::as_str) == Some("effort")
                    && command
                        .get("argumentHint")
                        .and_then(Value::as_str)
                        .is_some_and(|hint| hint.contains("ultracode"))
            });
            let dynamic_workflows_available = commands.iter().any(|command| {
                command
                    .get("description")
                    .and_then(Value::as_str)
                    .is_some_and(|description| description.contains("dynamic workflow"))
            });
            let ultracode_available = advertises_ultracode && dynamic_workflows_available;
            let models = raw_models
                .iter()
                .filter(|model| model.get("value").and_then(Value::as_str) != Some("default"))
                .filter_map(|model| {
                    claude_catalog_model(model, default_resolved, ultracode_available)
                })
                .collect::<Vec<_>>();
            if models.is_empty() {
                return Err(RuntimeError::Protocol {
                    provider: Provider::Claude,
                    message: "Claude Code returned an empty concrete model catalog".into(),
                });
            }
            return Ok(HarnessModelCatalog {
                status: HarnessCatalogStatus::Ready,
                source: "control_initialize".into(),
                models,
                error: None,
            });
        }
        Err(RuntimeError::Protocol {
            provider: Provider::Claude,
            message: "Claude Code exited without its initialization response".into(),
        })
    }

    fn parse_account_usage(&self, lines: &[String]) -> Result<Option<AccountUsageSnapshot>> {
        Ok(lines.iter().find_map(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|frame| claude_account_usage(&frame))
        }))
    }

    fn account_usage_probe(&self) -> Option<AccountUsageProbeSpec> {
        let mut command = CommandSpec::new("node");
        command
            .args
            .extend(["-e".into(), CLAUDE_ACCOUNT_USAGE_SCRIPT.into()]);
        Some(AccountUsageProbeSpec {
            command,
            expected_response_ids: None,
        })
    }

    fn parse_account_usage_probe(&self, lines: &[String]) -> Result<AccountUsageReport> {
        for line in lines {
            let Ok(frame) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if frame.get("type").and_then(Value::as_str)
                != Some("temps_agent_runtime_account_usage")
            {
                continue;
            }
            return match frame.get("status").and_then(Value::as_str) {
                Some("available") => {
                    let usage =
                        frame
                            .get("usage")
                            .cloned()
                            .ok_or_else(|| RuntimeError::Protocol {
                                provider: Provider::Claude,
                                message: "Claude account-usage response omitted its snapshot"
                                    .into(),
                            })?;
                    let usage: AccountUsageSnapshot =
                        serde_json::from_value(usage).map_err(|error| RuntimeError::Protocol {
                            provider: Provider::Claude,
                            message: format!("invalid Claude account-usage snapshot: {error}"),
                        })?;
                    if usage.provider != Provider::Claude {
                        return Err(RuntimeError::Protocol {
                            provider: Provider::Claude,
                            message: "Claude account-usage response named another provider".into(),
                        });
                    }
                    Ok(AccountUsageReport::available(usage))
                }
                Some("unavailable") => {
                    let reason = frame
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("Claude account usage is unavailable")
                        .chars()
                        .take(512)
                        .collect::<String>();
                    Ok(AccountUsageReport::unavailable(
                        Provider::Claude,
                        reason,
                        frame
                            .get("retryable")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    ))
                }
                _ => Err(RuntimeError::Protocol {
                    provider: Provider::Claude,
                    message: "Claude account-usage response has an unknown status".into(),
                }),
            };
        }
        Ok(AccountUsageReport::unavailable(
            Provider::Claude,
            "Claude account-usage helper returned no result",
            true,
        ))
    }

    fn authentication_probe(&self) -> Option<AuthenticationProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command
            .args
            .extend(["auth".into(), "status".into(), "--json".into()]);
        Some(AuthenticationProbeSpec { command })
    }

    fn parse_authentication_probe(
        &self,
        stdout: &[u8],
        stderr: &str,
        status: TransportExitStatus,
    ) -> Result<HarnessAuthentication> {
        if let Ok(value) = serde_json::from_slice::<Value>(stdout) {
            return match value.get("loggedIn").and_then(Value::as_bool) {
                Some(true) => Ok(HarnessAuthentication::authenticated("auth_status")),
                Some(false) if value.get("authMethod").and_then(Value::as_str) == Some("none") => {
                    Ok(HarnessAuthentication::required(
                        "auth_status",
                        "Claude Code is not authenticated on this execution target",
                    ))
                }
                Some(false) => Ok(HarnessAuthentication::rejected(
                    "auth_status",
                    "Claude Code credentials are configured but not usable",
                )),
                None => Err(RuntimeError::Protocol {
                    provider: Provider::Claude,
                    message: "Claude authentication status omitted `loggedIn`".into(),
                }),
            };
        }
        let diagnostic = stderr.trim();
        if !status.success
            && classify_provider_failure(diagnostic)
                == crate::ProviderProcessErrorKind::AuthenticationFailed
        {
            return Ok(HarnessAuthentication::rejected(
                "auth_status",
                if diagnostic.is_empty() {
                    "Claude Code credentials were rejected"
                } else {
                    diagnostic
                },
            ));
        }
        Err(RuntimeError::Protocol {
            provider: Provider::Claude,
            message: "Claude authentication status returned malformed JSON".into(),
        })
    }

    async fn readiness(&self) -> ProviderReadiness {
        inspect_executable(Provider::Claude, self.resolved()).await
    }

    fn prepare_turn(&self, request: &TurnRequest, state: &mut AdapterState) -> Result<()> {
        let mut native = take_native_state(state);
        native.manual_compaction_turn = is_manual_compaction_prompt(&request.prompt);
        native
            .own_commands
            .insert(new_message_uuid(), "pending".to_string());
        put_native_state(state, native);
        Ok(())
    }

    fn command_for_turn(&self, request: &TurnRequest, state: &AdapterState) -> Result<CommandSpec> {
        let mut spec = self.command(request)?;
        // The prompt carries the UUID `prepare_turn` registered, so the
        // `command_lifecycle` frames for it are recognized as this turn's.
        let uuid =
            peek_native_state(state).and_then(|native| native.own_commands.keys().next().cloned());
        if let Some(uuid) = uuid {
            spec.initial_stdin = Some(user_message_frame(&request.prompt, Some(&uuid))?);
        }
        Ok(spec)
    }

    fn encode_user_message(&self, text: &str, state: &mut AdapterState) -> Result<Option<Vec<u8>>> {
        let mut native = take_native_state(state);
        let outcome = if !native.retained_turn {
            // A one-shot process closes stdin once it answers, so there is no
            // channel to deliver a later message on.
            Ok(None)
        } else if native.own_commands.len() >= MAX_TURN_MESSAGES {
            Err(RuntimeError::InvalidRequest {
                field: "message",
                message: format!("a turn accepts at most {MAX_TURN_MESSAGES} user messages"),
            })
        } else {
            let uuid = new_message_uuid();
            native
                .own_commands
                .insert(uuid.clone(), "pending".to_string());
            user_message_frame(text, Some(&uuid)).map(Some)
        };
        put_native_state(state, native);
        outcome
    }

    fn interrupt_request(&self, state: &AdapterState) -> Option<Vec<u8>> {
        // A one-shot process is terminated with its turn; only a retained
        // process survives to unwind cooperatively.
        peek_native_state(state)
            .is_some_and(|native| native.retained_turn)
            .then(interrupt_frame)
            .flatten()
    }

    fn retained_interrupt_settled(&self, state: &AdapterState) -> Option<bool> {
        let native = peek_native_state(state)?;
        // Answered, including every message the turn submitted (an
        // interrupted one ends `cancelled`): nothing of this turn still runs.
        native.retained_turn.then(|| native.answered())
    }

    fn retained_background_work(&self, state: &AdapterState) -> bool {
        peek_native_state(state).is_some_and(|native| {
            !native.background_task_ids.is_empty()
                || native.follow_up_active
                || native.awaiting_follow_up
        })
    }

    fn mark_retained_turn(&self, state: &mut AdapterState) {
        let mut native = take_native_state(state);
        native.retained_turn = true;
        put_native_state(state, native);
    }

    fn retained_handoff_ready(&self, state: &AdapterState) -> bool {
        // Only between exchanges: after this turn's answer, while background
        // tasks run, and never while Claude is composing a follow-up answer
        // that would otherwise be split across two turns.
        state.terminal_failure.is_none()
            && peek_native_state(state).is_some_and(|native| {
                native.retained_turn
                    && native.answered()
                    && !native.awaiting_follow_up
                    && !native.background_task_ids.is_empty()
            })
    }

    fn inherit_retained_handoff(&self, mut previous: AdapterState, next: &mut AdapterState) {
        let previous = take_native_state(&mut previous);
        let mut native = take_native_state(next);
        // Only live work crosses the hand-off. Finished tasks stay with the
        // turn that reported them; carrying them would let a chain of
        // hand-offs fill the bounded task table and hide new subagents.
        native.tasks = previous
            .tasks
            .into_iter()
            .filter(|(task_id, _)| previous.background_task_ids.contains(task_id))
            .collect();
        native.tool_use_to_task = previous
            .tool_use_to_task
            .into_iter()
            .filter(|(_, task_id)| previous.background_task_ids.contains(task_id))
            .collect();
        native.background_task_ids = previous.background_task_ids;
        // Names are dropped once a tool reports its result, so these are the
        // calls still in flight, whose results may reach the next turn.
        native.tool_names = previous.tool_names;
        native.effective_permission_mode = previous.effective_permission_mode;
        native.permission_mode_before_plan = previous.permission_mode_before_plan;
        native.lifecycle_seen = previous.lifecycle_seen;
        put_native_state(next, native);
    }

    fn retained_completion_grace(&self, state: &AdapterState) -> Option<Duration> {
        peek_native_state(state)
            .is_some_and(|native| {
                native.retained_turn
                    && native.awaiting_follow_up
                    && native.background_task_ids.is_empty()
                    && native.own_commands_done()
            })
            .then_some(FOLLOW_UP_GRACE)
    }

    fn turn_capabilities(&self) -> TurnCapabilities {
        TurnCapabilities {
            // Claude reports `system/status` `compacting` when it starts
            // compacting and `compact_boundary` (or a failed compact result)
            // when it ends, for automatic and manual compaction alike.
            compaction_lifecycle: true,
            live_messages: true,
            ..TurnCapabilities::default()
        }
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        let mut spec = CommandSpec::new(self.configured_executable());
        spec.args.extend([
            "--print".into(),
            "--input-format".into(),
            "stream-json".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--permission-prompt-tool".into(),
            "stdio".into(),
            "--verbose".into(),
            "--include-partial-messages".into(),
        ]);
        let permission = request.harness_options.get("permission_mode").map_or_else(
            || match &request.permission_mode {
                PermissionMode::Default => "manual",
                PermissionMode::AcceptEdits => "acceptEdits",
                PermissionMode::Plan => "plan",
                PermissionMode::FullAccess => "bypassPermissions",
                PermissionMode::Custom(value) => value,
            },
            String::as_str,
        );
        if ![
            "manual",
            "acceptEdits",
            "plan",
            "auto",
            "dontAsk",
            "bypassPermissions",
        ]
        .contains(&permission)
        {
            return Err(RuntimeError::InvalidRequest {
                field: "harness_options.permission_mode",
                message: format!("unsupported Claude permission mode `{permission}`"),
            });
        }
        spec.args
            .extend(["--permission-mode".into(), permission.into()]);
        if permission == "bypassPermissions" {
            spec.args.push("--dangerously-skip-permissions".into());
        }
        if let Some(reasoning) = request.reasoning.as_deref() {
            match reasoning {
                "off" => spec.args.extend(["--thinking".into(), "disabled".into()]),
                "ultracode" => {
                    spec.args.extend(["--effort".into(), "xhigh".into()]);
                    spec.args
                        .extend(["--settings".into(), r#"{"ultracode":true}"#.into()]);
                }
                "low" | "medium" | "high" | "xhigh" | "max" => {
                    spec.args.extend(["--effort".into(), reasoning.into()]);
                }
                value => {
                    return Err(RuntimeError::InvalidRequest {
                        field: "reasoning",
                        message: format!("unsupported Claude thinking selection `{value}`"),
                    });
                }
            }
        }
        if let Some(model) = request.model.as_deref() {
            spec.args.extend(["--model".into(), model.into()]);
        }
        if let Some(session) = request.session_id.as_deref() {
            spec.args.extend(["--resume".into(), session.into()]);
        }
        if let Some(max_turns) = request.max_turns {
            spec.args
                .extend(["--max-turns".into(), max_turns.to_string().into()]);
        }
        match request.auto_compaction {
            AutoCompactionPolicy::ProviderDefault => {}
            AutoCompactionPolicy::Automatic => {
                spec.args.extend(["--autocompact".into(), "auto".into()]);
            }
            AutoCompactionPolicy::TokenThreshold { tokens } => {
                spec.args
                    .extend(["--autocompact".into(), tokens.to_string().into()]);
            }
        }
        if let Some(system_prompt) = request.launch_context.system_prompt_append.as_deref() {
            spec.args
                .extend(["--append-system-prompt".into(), system_prompt.into()]);
        }
        if let Some(tools) = request.launch_context.allowed_tools.as_deref() {
            spec.args.extend(["--tools".into(), tools.join(",").into()]);
        }
        if let Some(mcp_config) = claude_mcp_config(request)? {
            spec.args.extend(["--mcp-config".into(), mcp_config.into()]);
            if request.launch_context.strict_mcp_config {
                spec.args.push("--strict-mcp-config".into());
            }
        }
        spec.initial_stdin = Some(
            serde_json::to_vec(&json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": request.prompt}]
                },
                "parent_tool_use_id": null
            }))
            .map_err(|error| RuntimeError::Protocol {
                provider: Provider::Claude,
                message: format!("could not encode user message: {error}"),
            })?,
        );
        spec.interactive_stdin = true;
        Ok(spec)
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        let value: Value = serde_json::from_str(line).map_err(|error| RuntimeError::Protocol {
            provider: Provider::Claude,
            message: format!("invalid stream-JSON record: {error}"),
        })?;
        let mut output = AdapterOutput::default();
        let mut native = take_native_state(state);
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "system" => {
                if native.result_seen
                    && value.get("subtype").and_then(Value::as_str) == Some("init")
                {
                    // A notification woke the parent agent: its answer ends
                    // with another result that belongs to this turn.
                    native.follow_up_active = true;
                    native.awaiting_follow_up = false;
                }
                if let Some(mode) = reported_permission_mode(&value) {
                    change_permission_mode(&mut native, &mut output, mode);
                }
                if state.result.session_title.is_none() {
                    state.result.session_title = ["session_title", "session_name", "title", "name"]
                        .into_iter()
                        .find_map(|key| value.get(key).and_then(Value::as_str))
                        .map(str::trim)
                        .filter(|title| !title.is_empty())
                        .map(str::to_owned);
                }
                if let Some(session_id) = value.get("session_id").and_then(Value::as_str) {
                    if state.result.session_id.as_deref() != Some(session_id) {
                        state.result.session_id = Some(session_id.to_string());
                        output.events.push(TurnEvent::SessionStarted {
                            session_id: session_id.to_string(),
                            title: state.result.session_title.clone(),
                        });
                    }
                }
                if state.result.model.is_none() {
                    state.result.model = value
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                if value.get("subtype").and_then(Value::as_str) == Some("compact_boundary") {
                    native.open_compaction = None;
                    native.open_compaction_succeeded = false;
                    translate_compaction(&value, state, &mut output);
                } else if value.get("subtype").and_then(Value::as_str) == Some("status") {
                    translate_compaction_status(&value, &mut native, &mut output);
                } else {
                    translate_system_task(&value, &mut native, &mut output);
                }
            }
            "stream_event" => {
                let delta = value.pointer("/event/delta");
                match delta
                    .and_then(|delta| delta.get("type"))
                    .and_then(Value::as_str)
                {
                    Some("text_delta") => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("text"))
                            .and_then(Value::as_str)
                        {
                            state.saw_text_delta = true;
                            state.result.text.push_str(text);
                            output.events.push(TurnEvent::TextDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("thinking_delta") => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("thinking"))
                            .and_then(Value::as_str)
                        {
                            state
                                .result
                                .reasoning
                                .get_or_insert_with(String::new)
                                .push_str(text);
                            output.events.push(TurnEvent::ReasoningDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            "assistant" => {
                if state.result.model.is_none() {
                    state.result.model = value
                        .pointer("/message/model")
                        .and_then(Value::as_str)
                        .filter(|model| !model.is_empty() && *model != "<synthetic>")
                        .map(str::to_owned);
                }
                let task_id = task_id_for(
                    &native,
                    value.get("parent_tool_use_id").and_then(Value::as_str),
                );
                if let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) {
                    for block in blocks {
                        match block.get("type").and_then(Value::as_str) {
                            Some("text") if !state.saw_text_delta => {
                                if let Some(text) = block.get("text").and_then(Value::as_str) {
                                    state.result.text.push_str(text);
                                    output.events.push(TurnEvent::TextDelta {
                                        text: text.to_string(),
                                    });
                                }
                            }
                            Some("tool_use") => {
                                let id = block.get("id").and_then(Value::as_str).map(str::to_owned);
                                let name = block
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .unwrap_or("tool")
                                    .to_string();
                                if let Some(id) = &id {
                                    native.tool_names.insert(id.clone(), name.clone());
                                }
                                output.events.push(TurnEvent::ToolCall {
                                    id,
                                    name,
                                    status: ToolCallStatus::Started,
                                    input: block.get("input").cloned(),
                                    output: None,
                                    error: None,
                                    task_id: task_id.clone(),
                                });
                            }
                            _ => {}
                        }
                    }
                }
                let mut usage = super::usage_from(&value);
                // A subagent's message measures the subagent's own context,
                // not the conversation's, so it never sets occupancy.
                let subagent = value
                    .get("parent_tool_use_id")
                    .is_some_and(|parent| !parent.is_null());
                if !subagent {
                    usage.context_window =
                        context_window_usage(&value, state.result.model.as_deref());
                }
                if usage != Usage::default() {
                    super::merge_usage(&mut state.result.usage, &usage);
                    output.events.push(TurnEvent::Usage(usage));
                }
            }
            "user" => {
                if let Some(launch) = value.get("tool_use_result") {
                    if record_workflow_launch(&mut native, launch) {
                        emit_tasks(&native, &mut output);
                    }
                }
                let task_id = task_id_for(
                    &native,
                    value.get("parent_tool_use_id").and_then(Value::as_str),
                );
                if let Some(blocks) = value.pointer("/message/content").and_then(Value::as_array) {
                    for block in blocks {
                        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                            continue;
                        }
                        let Some(tool_use_id) = block.get("tool_use_id").and_then(Value::as_str)
                        else {
                            continue;
                        };
                        let failed = block.get("is_error").and_then(Value::as_bool) == Some(true);
                        let text =
                            tool_result_text(block.get("content"), value.get("tool_use_result"));
                        // Each call reports one result; forgetting it keeps
                        // the map to calls still in flight.
                        let tool_name = native
                            .tool_names
                            .remove(tool_use_id)
                            .unwrap_or_else(|| "tool".to_string());
                        // Claude reports a local_bash task after its Bash
                        // tool_use; task_started links the tool ID to the
                        // task, so the result belongs to that shell task.
                        // Nested subagent calls keep their parent.
                        let owning_task = task_id.clone().or_else(|| {
                            native.tool_use_to_task.get(tool_use_id).and_then(|id| {
                                native
                                    .tasks
                                    .get(id)
                                    .filter(|task| task.kind == "shell")
                                    .map(|_| id.clone())
                            })
                        });
                        output.events.push(TurnEvent::ToolCall {
                            id: Some(tool_use_id.to_string()),
                            name: tool_name.clone(),
                            status: if failed {
                                ToolCallStatus::Failed
                            } else {
                                ToolCallStatus::Succeeded
                            },
                            input: None,
                            output: (!failed).then(|| text.clone()),
                            error: failed.then_some(text),
                            task_id: owning_task,
                        });
                        if !failed {
                            match tool_name.as_str() {
                                "EnterPlanMode" => change_permission_mode(
                                    &mut native,
                                    &mut output,
                                    PermissionMode::Plan,
                                ),
                                "ExitPlanMode" => {
                                    let restored = native
                                        .permission_mode_before_plan
                                        .clone()
                                        .unwrap_or(PermissionMode::Default);
                                    change_permission_mode(&mut native, &mut output, restored);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            "control_request" => {
                let request_id = value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RuntimeError::Protocol {
                        provider: Provider::Claude,
                        message: "control_request omitted request_id".to_string(),
                    })?
                    .to_string();
                let tool_name = value
                    .pointer("/request/tool_name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let input = value
                    .pointer("/request/input")
                    .cloned()
                    .unwrap_or(Value::Null);
                if tool_name == "AskUserQuestion" {
                    let request = QuestionRequest::new(
                        request_id,
                        input
                            .get("questions")
                            .cloned()
                            .unwrap_or_else(|| input.clone()),
                    );
                    output
                        .events
                        .push(TurnEvent::QuestionRequested(request.clone()));
                    output.interaction = Some(InteractionRequest::Question {
                        request,
                        original: value,
                    });
                } else {
                    let request = ApprovalRequest {
                        id: request_id,
                        tool_name: tool_name.clone(),
                        description: input
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        input,
                    };
                    output.events.push(if tool_name == "ExitPlanMode" {
                        TurnEvent::PlanApprovalRequested(request.clone())
                    } else {
                        TurnEvent::ApprovalRequested(request.clone())
                    });
                    output.interaction = Some(InteractionRequest::Approval {
                        request,
                        original: value,
                    });
                }
            }
            "result" => {
                native.result_seen = true;
                native.follow_up_active = false;
                native.awaiting_follow_up = false;
                close_unfinished_compaction(&mut native, &mut output);
                // Claude can emit its terminal result before background Task
                // subagents finish. Keep stdin available for their approvals
                // and continue reading task progress until the native task set
                // clears and the provider exits. A retained turn also waits for
                // every message it submitted: a message sent mid-turn may be
                // answered by this result or by a later exchange.
                output.terminal = native.background_task_ids.is_empty()
                    && (!native.retained_turn || native.own_commands_done());
                let failed = value.get("is_error").and_then(Value::as_bool) == Some(true);
                state.result.status = if failed {
                    let diagnostic = value
                        .get("result")
                        .and_then(Value::as_str)
                        .filter(|message| !message.trim().is_empty())
                        .or_else(|| value.pointer("/error/message").and_then(Value::as_str))
                        .or_else(|| value.get("error").and_then(Value::as_str))
                        // Claude's streaming JSON result can omit `result`
                        // and put the actionable failure only in `errors`.
                        .or_else(|| {
                            value
                                .get("errors")
                                .and_then(Value::as_array)
                                .and_then(|errors| {
                                    errors
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .find(|message| !message.trim().is_empty())
                                })
                        })
                        .unwrap_or("Claude reported an error")
                        .to_string();
                    let provider_code = value
                        .get("subtype")
                        .or_else(|| value.pointer("/error/type"))
                        .or_else(|| value.pointer("/error/code"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let kind = classify_provider_failure(&format!(
                        "{} {diagnostic}",
                        provider_code.as_deref().unwrap_or_default()
                    ));
                    let mut failure =
                        ProviderTerminalFailure::new(kind, diagnostic, DeliveryState::Accepted);
                    if let Some(code) = provider_code {
                        failure = failure.with_provider_code(format!("claude::{code}"));
                    }
                    state.terminal_failure = Some(failure);
                    RunStatus::Failed
                } else {
                    RunStatus::Succeeded
                };
                if !failed && state.result.text.is_empty() {
                    if let Some(text) = value.get("result").and_then(Value::as_str) {
                        state.result.text = text.to_string();
                        if !text.is_empty() {
                            output.events.push(TurnEvent::TextDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                }
                if state.result.session_id.is_none() {
                    state.result.session_id = value
                        .get("session_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                if state.result.model.is_none() {
                    state.result.model = value
                        .get("model")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                let mut usage = super::usage_from(&value);
                usage.context_window = reported_context_limit(
                    &value,
                    state.result.usage.context_window.as_ref(),
                    state.result.model.as_deref(),
                );
                super::merge_usage(&mut state.result.usage, &usage);
                if usage != crate::Usage::default() {
                    output.events.push(TurnEvent::Usage(usage));
                }
            }
            "command_lifecycle" => {
                let command = value.get("command_uuid").and_then(Value::as_str);
                let lifecycle = value.get("state").and_then(Value::as_str);
                if let (Some(command), Some(lifecycle)) = (command, lifecycle) {
                    native.lifecycle_seen = true;
                    if let Some(tracked) = native.own_commands.get_mut(command) {
                        *tracked = bounded(lifecycle);
                        // Stopping a turn stops every message it submitted,
                        // including ones Claude had queued behind the
                        // interrupted exchange.
                        if native.interrupting && lifecycle == "started" {
                            output.writes.extend(interrupt_frame());
                        }
                    } else if lifecycle == "started" && native.result_seen {
                        // A command this turn did not submit: Claude answering
                        // a task notification on its own. Its exchange ends
                        // with a result that belongs to this turn.
                        native.follow_up_active = true;
                        native.awaiting_follow_up = false;
                    }
                    // A turn whose last own message just finished ends here,
                    // unless Claude may still answer drained background work.
                    if native.retained_turn
                        && !native.interrupting
                        && !native.awaiting_follow_up
                        && native.answered()
                        && native.background_task_ids.is_empty()
                    {
                        output.terminal = true;
                    }
                }
            }
            "control_response" => {
                if value
                    .pointer("/response/request_id")
                    .and_then(Value::as_str)
                    == Some(INTERRUPT_REQUEST_ID)
                {
                    native.interrupting = true;
                }
            }
            "rate_limit_event" => {
                if let Some(usage) = claude_account_usage(&value) {
                    output.events.push(TurnEvent::AccountUsageUpdated { usage });
                } else {
                    output.events.push(TurnEvent::Warning {
                        message: "Claude Code reported a rate limit without usage-window metadata"
                            .to_string(),
                    });
                }
            }
            _ => {}
        }
        put_native_state(state, native);
        Ok(output)
    }

    fn approval_response(
        &self,
        request: &ApprovalRequest,
        original: &Value,
        decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        let original_input = original
            .pointer("/request/input")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let response = match decision {
            // Claude Code's control protocol has no session-scoped grant, so
            // a session approval permits exactly this one operation.
            ApprovalDecision::Allow | ApprovalDecision::AllowForSession => {
                json!({"behavior": "allow", "updatedInput": original_input})
            }
            ApprovalDecision::Deny { reason } => json!({
                "behavior": "deny",
                "message": reason.unwrap_or_else(|| "Permission denied".to_string())
            }),
        };
        encode_control_response(&request.id, response).map(Some)
    }

    fn question_response(
        &self,
        request: &QuestionRequest,
        original: &Value,
        answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        let response = if let Some(answer) = answer {
            let mut input = original
                .pointer("/request/input")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if let Some(object) = input.as_object_mut() {
                object.insert("answers".to_string(), answer.answers);
            }
            json!({"behavior": "allow", "updatedInput": input})
        } else {
            json!({"behavior": "deny", "message": "Question was not answered"})
        };
        encode_control_response(&request.id, response).map(Some)
    }
}

fn encode_control_response(request_id: &str, response: Value) -> Result<Vec<u8>> {
    serde_json::to_vec(&json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": response
        }
    }))
    .map_err(|error| RuntimeError::Protocol {
        provider: Provider::Claude,
        message: format!("could not encode control response: {error}"),
    })
}

fn peek_native_state(state: &AdapterState) -> Option<ClaudeNativeState> {
    state
        .extensions
        .get(CLAUDE_STATE_KEY)
        .and_then(|value| ClaudeNativeState::deserialize(value).ok())
}

fn take_native_state(state: &mut AdapterState) -> ClaudeNativeState {
    state
        .extensions
        .remove(CLAUDE_STATE_KEY)
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

fn put_native_state(state: &mut AdapterState, native: ClaudeNativeState) {
    if let Ok(value) = serde_json::to_value(native) {
        state.extensions.insert(CLAUDE_STATE_KEY.to_string(), value);
    }
}

fn context_window_usage(value: &Value, fallback_model: Option<&str>) -> Option<ContextWindowUsage> {
    let usage = value.pointer("/message/usage")?;
    let input = usage.get("input_tokens").and_then(Value::as_u64)?;
    let cache_creation = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let used_tokens = input
        .saturating_add(cache_creation)
        .saturating_add(cache_read)
        .saturating_add(output);
    let model = value
        .pointer("/message/model")
        .and_then(Value::as_str)
        .or(fallback_model)
        .filter(|model| !model.is_empty() && *model != "<synthetic>")
        .map(str::to_owned);
    Some(ContextWindowUsage {
        used_tokens: Some(used_tokens),
        limit_tokens: model.as_deref().and_then(context_window_from_label),
        model,
        estimated: true,
    })
}

/// Completes the turn's last context occupancy with the window Claude Code
/// reports in `result.modelUsage`.
///
/// Per-message usage carries only a bare model id such as `claude-opus-5-5`,
/// which says nothing about its window. The result frame reports the actual
/// `contextWindow` for every model the turn used, subagents included, so the
/// entry for the conversation's own model is selected. Occupancy measured
/// against another model is left alone rather than paired with a window that
/// does not describe it.
fn reported_context_limit(
    value: &Value,
    occupancy: Option<&ContextWindowUsage>,
    conversation_model: Option<&str>,
) -> Option<ContextWindowUsage> {
    let occupancy = occupancy?;
    let model = conversation_model.or(occupancy.model.as_deref())?;
    if occupancy
        .model
        .as_deref()
        .is_some_and(|measured| measured != model)
    {
        return None;
    }
    let models = value.get("modelUsage")?.as_object()?;
    let entry = models.get(model).or_else(|| {
        models
            .values()
            .find(|entry| entry.get("canonicalModel").and_then(Value::as_str) == Some(model))
    })?;
    let limit_tokens = entry.get("contextWindow").and_then(Value::as_u64)?;
    Some(ContextWindowUsage {
        limit_tokens: Some(limit_tokens),
        model: Some(model.to_owned()),
        ..occupancy.clone()
    })
}

fn claude_usage_window(id: &str, value: &Value) -> Option<AccountUsageWindow> {
    let utilization = value.get("utilization").and_then(Value::as_f64)?;
    let (kind, duration_minutes) = match id {
        "five_hour" => (AccountUsageWindowKind::Session, Some(5 * 60)),
        "seven_day" | "seven_day_opus" | "seven_day_sonnet" | "seven_day_overage_included" => {
            (AccountUsageWindowKind::Weekly, Some(7 * 24 * 60))
        }
        _ => (AccountUsageWindowKind::Other, None),
    };
    Some(AccountUsageWindow {
        id: id.to_string(),
        label: None,
        kind,
        // Claude reports a ratio; the SDK contract uses a percentage.
        // Ratios above one intentionally remain percentages above 100.
        used_percent: utilization * 100.0,
        duration_minutes,
        resets_at_unix_seconds: value
            .get("resetsAt")
            .or_else(|| value.get("resets_at"))
            .and_then(Value::as_u64),
    })
}

fn claude_account_usage(value: &Value) -> Option<AccountUsageSnapshot> {
    let info = value
        .get("rate_limit_info")
        .or_else(|| value.get("rateLimitInfo"))?;
    let mut windows = Vec::new();
    if let Some(unified) = info
        .get("unifiedWindows")
        .or_else(|| info.get("unified_windows"))
        .and_then(Value::as_object)
    {
        for id in [
            "five_hour",
            "seven_day",
            "seven_day_opus",
            "seven_day_sonnet",
            "seven_day_overage_included",
        ] {
            if let Some(window) = unified
                .get(id)
                .and_then(|window| claude_usage_window(id, window))
            {
                windows.push(window);
            }
        }
        for (id, window) in unified {
            if windows.iter().any(|known| known.id == *id) {
                continue;
            }
            if let Some(window) = claude_usage_window(id, window) {
                windows.push(window);
            }
        }
    }
    if windows.is_empty() {
        let id = info
            .get("rateLimitType")
            .or_else(|| info.get("rate_limit_type"))
            .and_then(Value::as_str)?;
        if let Some(window) = claude_usage_window(id, info) {
            windows.push(window);
        }
    }
    let credits = info
        .get("overageBalance")
        .or_else(|| info.get("overage_balance"))
        .map(|balance| AccountCredits {
            unlimited: false,
            balance: Some(
                balance
                    .as_str()
                    .map_or_else(|| balance.to_string(), str::to_owned),
            ),
            currency: None,
        });
    (!windows.is_empty() || credits.is_some()).then_some(AccountUsageSnapshot {
        provider: Provider::Claude,
        plan: None,
        windows,
        credits,
    })
}

/// Whether a prompt is Claude Code's native `/compact` command.
fn is_manual_compaction_prompt(prompt: &str) -> bool {
    let prompt = prompt.trim_start();
    prompt
        .strip_prefix("/compact")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
}

/// Maximum characters of a provider compaction diagnostic carried in events.
const MAX_COMPACTION_ERROR_CHARS: usize = 512;

/// Translate Claude's `system/status` frames into the compaction lifecycle.
///
/// Claude Code reports `status: "compacting"` when a compaction starts (and
/// repeats it periodically while a long compaction runs). The compaction ends
/// with a `compact_boundary` on success, or with `status: null` carrying
/// `compact_result: "failed"` and an optional `compact_error`. A
/// `compact_result: "success"` precedes the boundary, so it does not close the
/// compaction by itself.
fn translate_compaction_status(
    value: &Value,
    native: &mut ClaudeNativeState,
    output: &mut AdapterOutput,
) {
    if value.get("status").and_then(Value::as_str) == Some("compacting") {
        if native.open_compaction.is_none() {
            let trigger = if native.manual_compaction_turn {
                CompactionTrigger::Manual
            } else {
                CompactionTrigger::Automatic
            };
            native.open_compaction = Some(trigger);
            native.open_compaction_succeeded = false;
            output.events.push(TurnEvent::CompactionStarted { trigger });
        }
        return;
    }
    match value.get("compact_result").and_then(Value::as_str) {
        Some("success") => {
            if native.open_compaction.is_some() {
                native.open_compaction_succeeded = true;
            }
        }
        Some("failed") => {
            let trigger =
                native
                    .open_compaction
                    .take()
                    .unwrap_or(if native.manual_compaction_turn {
                        CompactionTrigger::Manual
                    } else {
                        CompactionTrigger::Automatic
                    });
            native.open_compaction_succeeded = false;
            let message = value
                .get("compact_error")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|message| !message.is_empty())
                .map(|message| message.chars().take(MAX_COMPACTION_ERROR_CHARS).collect());
            output
                .events
                .push(TurnEvent::CompactionFailed { trigger, message });
        }
        _ => {}
    }
}

/// Close a compaction still open when the turn reaches its terminal result.
///
/// Claude does not always follow a successful compaction with a boundary in
/// the same process (for example when the boundary was already persisted),
/// and a skipped compaction reports no result at all. Either way the
/// application must not show a compaction as running after the turn ended.
fn close_unfinished_compaction(native: &mut ClaudeNativeState, output: &mut AdapterOutput) {
    let Some(trigger) = native.open_compaction.take() else {
        return;
    };
    if std::mem::take(&mut native.open_compaction_succeeded) {
        output.events.push(TurnEvent::CompactionCompleted {
            compaction: ContextCompaction {
                trigger,
                pre_tokens: None,
                post_tokens: None,
                dropped_tokens: None,
                cumulative_dropped_tokens: None,
                duration_ms: None,
            },
        });
    } else {
        output.events.push(TurnEvent::CompactionFailed {
            trigger,
            message: Some("Claude ended the turn before the compaction finished.".to_owned()),
        });
    }
}

fn translate_compaction(value: &Value, state: &mut AdapterState, output: &mut AdapterOutput) {
    let metadata = value
        .get("compactMetadata")
        .or_else(|| value.get("compact_metadata"));
    let trigger = match metadata
        .and_then(|metadata| metadata.get("trigger"))
        .and_then(Value::as_str)
    {
        Some("auto" | "automatic") => CompactionTrigger::Automatic,
        Some("manual") => CompactionTrigger::Manual,
        _ => CompactionTrigger::Unknown,
    };
    let pre_tokens = metadata
        .and_then(|metadata| {
            metadata
                .get("preTokens")
                .or_else(|| metadata.get("pre_tokens"))
        })
        .and_then(Value::as_u64);
    let post_tokens = metadata
        .and_then(|metadata| {
            metadata
                .get("postTokens")
                .or_else(|| metadata.get("post_tokens"))
        })
        .and_then(Value::as_u64);
    let dropped_tokens = metadata
        .and_then(|metadata| {
            metadata
                .get("droppedTokens")
                .or_else(|| metadata.get("dropped_tokens"))
        })
        .and_then(Value::as_u64)
        .or_else(|| Some(pre_tokens?.saturating_sub(post_tokens?)));
    let compaction = ContextCompaction {
        trigger,
        pre_tokens,
        post_tokens,
        dropped_tokens,
        cumulative_dropped_tokens: metadata
            .and_then(|metadata| {
                metadata
                    .get("cumulativeDroppedTokens")
                    .or_else(|| metadata.get("cumulative_dropped_tokens"))
            })
            .and_then(Value::as_u64),
        duration_ms: metadata
            .and_then(|metadata| {
                metadata
                    .get("durationMs")
                    .or_else(|| metadata.get("duration_ms"))
            })
            .and_then(Value::as_u64),
    };
    output.events.push(TurnEvent::CompactionCompleted {
        compaction: compaction.clone(),
    });
    if let Some(used_tokens) = post_tokens {
        let model = state.result.model.clone();
        let context_window = ContextWindowUsage {
            used_tokens: Some(used_tokens),
            limit_tokens: model.as_deref().and_then(context_window_from_label),
            model,
            estimated: false,
        };
        state.result.usage.context_window = Some(context_window.clone());
        output.events.push(TurnEvent::Usage(Usage {
            context_window: Some(context_window),
            ..Usage::default()
        }));
    }
}

fn task_id_for(native: &ClaudeNativeState, tool_use_id: Option<&str>) -> Option<String> {
    tool_use_id.map(|id| {
        native
            .tool_use_to_task
            .get(id)
            .cloned()
            .unwrap_or_else(|| id.to_string())
    })
}

fn task_kind(task_type: Option<&str>, agent_type: Option<&str>) -> String {
    if agent_type.is_some_and(|value| !value.is_empty()) || task_type == Some("local_agent") {
        return "subagent".to_string();
    }
    match task_type {
        Some("local_bash") => "shell".to_string(),
        Some("local_workflow") => "workflow".to_string(),
        Some(value) if !value.is_empty() => bounded(value),
        _ => "task".to_string(),
    }
}

fn bounded(value: &str) -> String {
    if value.chars().count() <= MAX_TASK_FIELD_CHARS {
        value.to_string()
    } else {
        let mut text = value.chars().take(MAX_TASK_FIELD_CHARS).collect::<String>();
        text.push_str("… [truncated]");
        text
    }
}

fn optional_bounded(value: Option<&str>) -> Option<String> {
    value.map(bounded)
}

fn fallback_task(id: &str) -> AgentTask {
    AgentTask {
        id: id.to_string(),
        kind: "task".to_string(),
        description: "Background task".to_string(),
        status: "running".to_string(),
        agent_type: None,
        error: None,
        summary: None,
        workflow: None,
    }
}

fn workflow_text(value: &str) -> String {
    if value.chars().count() <= MAX_WORKFLOW_TEXT_CHARS {
        value.to_string()
    } else {
        let mut text = value
            .chars()
            .take(MAX_WORKFLOW_TEXT_CHARS)
            .collect::<String>();
        text.push('…');
        text
    }
}

fn workflow_field(entry: &Value, key: &str) -> Option<String> {
    entry
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(workflow_text)
}

fn workflow_number(entry: &Value, key: &str) -> Option<u64> {
    entry.get(key).and_then(Value::as_u64)
}

fn workflow_index(entry: &Value, key: &str) -> Option<u32> {
    workflow_number(entry, key).map(|value| u32::try_from(value).unwrap_or(u32::MAX))
}

/// Parse Claude's `workflow_progress` list: phases, agents and script logs,
/// each phase and agent reported once with its latest state.
fn parse_workflow(entries: &[Value], previous: &AgentWorkflow) -> AgentWorkflow {
    let mut workflow = AgentWorkflow {
        name: previous.name.clone(),
        run_id: previous.run_id.clone(),
        transcript_dir: previous.transcript_dir.clone(),
        ..AgentWorkflow::default()
    };
    let mut reported_agents = 0_u32;
    let mut logs = std::collections::VecDeque::new();
    for entry in entries {
        match entry.get("type").and_then(Value::as_str) {
            Some("workflow_phase") if workflow.phases.len() < MAX_WORKFLOW_PHASES => {
                let Some(index) = workflow_index(entry, "index") else {
                    continue;
                };
                workflow.phases.push(AgentWorkflowPhase {
                    index,
                    title: workflow_field(entry, "title")
                        .unwrap_or_else(|| format!("Phase {index}")),
                    kind: workflow_field(entry, "kind"),
                });
            }
            Some("workflow_agent") => {
                let Some(agent) = parse_workflow_agent(entry) else {
                    continue;
                };
                reported_agents = reported_agents.saturating_add(1);
                if workflow.agents.len() < MAX_WORKFLOW_AGENTS {
                    workflow.agents.push(agent);
                }
            }
            Some("workflow_log") => {
                if let Some(message) = workflow_field(entry, "message") {
                    if logs.len() == MAX_WORKFLOW_LOGS {
                        logs.pop_front();
                    }
                    logs.push_back(message);
                }
            }
            _ => {}
        }
    }
    workflow.logs = logs.into();
    workflow.omitted_agents =
        reported_agents.saturating_sub(u32::try_from(workflow.agents.len()).unwrap_or(u32::MAX));
    workflow
}

fn parse_workflow_agent(entry: &Value) -> Option<AgentWorkflowAgent> {
    let index = workflow_index(entry, "index")?;
    let native_state = workflow_field(entry, "state").unwrap_or_else(|| "start".to_string());
    let started_at_ms = workflow_number(entry, "startedAt");
    let blocked = entry.get("blocked").and_then(Value::as_bool) == Some(true);
    let state = match native_state.as_str() {
        _ if blocked => AgentWorkflowAgentState::Blocked,
        "done" => AgentWorkflowAgentState::Completed,
        "error" => AgentWorkflowAgentState::Failed,
        "start" if started_at_ms.is_none() => AgentWorkflowAgentState::Queued,
        _ => AgentWorkflowAgentState::Running,
    };
    let error = match entry.get("error") {
        Some(Value::String(error)) if !error.is_empty() => Some(workflow_text(error)),
        Some(Value::Null) | None => None,
        Some(other) => Some(workflow_text(&other.to_string())),
    };
    Some(AgentWorkflowAgent {
        index,
        label: workflow_field(entry, "label").unwrap_or_else(|| format!("agent {index}")),
        state,
        native_state,
        phase_index: workflow_index(entry, "phaseIndex"),
        phase_title: workflow_field(entry, "phaseTitle"),
        agent_id: workflow_field(entry, "agentId"),
        agent_type: workflow_field(entry, "agentType"),
        model: workflow_field(entry, "model"),
        isolation: workflow_field(entry, "isolation"),
        attempt: workflow_index(entry, "attempt"),
        cached: entry.get("cached").and_then(Value::as_bool) == Some(true),
        tokens: workflow_number(entry, "tokens"),
        tool_calls: workflow_number(entry, "toolCalls"),
        duration_ms: workflow_number(entry, "durationMs"),
        queued_at_ms: workflow_number(entry, "queuedAt"),
        started_at_ms,
        last_progress_at_ms: workflow_number(entry, "lastProgressAt"),
        last_tool_name: workflow_field(entry, "lastToolName"),
        last_tool_summary: workflow_field(entry, "lastToolSummary"),
        prompt_preview: workflow_field(entry, "promptPreview"),
        result_preview: workflow_field(entry, "resultPreview"),
        error,
    })
}

/// Whether two workflow snapshots differ only in counters a viewer can
/// afford to see a few ticks late: tokens, tool calls, durations and the
/// latest tool.
fn workflow_counters_only(previous: &AgentWorkflow, next: &AgentWorkflow) -> bool {
    let without_counters = |workflow: &AgentWorkflow| {
        let mut workflow = workflow.clone();
        for agent in &mut workflow.agents {
            agent.tokens = None;
            agent.tool_calls = None;
            agent.duration_ms = None;
            agent.last_progress_at_ms = None;
            agent.last_tool_name = None;
            agent.last_tool_summary = None;
        }
        workflow
    };
    without_counters(previous) == without_counters(next)
}

/// Agents that are new or changed state between two workflow snapshots.
fn changed_workflow_agents(
    previous: &AgentWorkflow,
    next: &AgentWorkflow,
) -> Vec<AgentWorkflowAgent> {
    next.agents
        .iter()
        .filter(|agent| {
            previous
                .agents
                .iter()
                .find(|earlier| earlier.index == agent.index)
                .is_none_or(|earlier| earlier.state != agent.state)
        })
        .cloned()
        .collect()
}

fn workflow_agent_summary(agent: &AgentWorkflowAgent) -> String {
    let change = match agent.state {
        AgentWorkflowAgentState::Queued => "queued",
        AgentWorkflowAgentState::Running => "started",
        AgentWorkflowAgentState::Completed if agent.cached => "reused a cached result",
        AgentWorkflowAgentState::Completed => "completed",
        AgentWorkflowAgentState::Failed => "failed",
        AgentWorkflowAgentState::Blocked => "was blocked",
    };
    format!("{} {change}", agent.label)
}

/// Record what the Workflow tool reported when it launched a run: its name,
/// run identifier and where it writes each agent's transcript.
fn record_workflow_launch(native: &mut ClaudeNativeState, launch: &Value) -> bool {
    if launch.get("taskType").and_then(Value::as_str) != Some("local_workflow") {
        return false;
    }
    let Some(task_id) = launch
        .get("taskId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return false;
    };
    if !can_track(native, task_id) {
        return false;
    }
    let mut task = native.tasks.get(task_id).cloned().unwrap_or_else(|| {
        let mut task = fallback_task(task_id);
        task.kind = "workflow".to_string();
        task
    });
    let workflow = task.workflow.get_or_insert_with(AgentWorkflow::default);
    if let Some(name) = workflow_field(launch, "workflowName") {
        workflow.name = Some(name);
    }
    if let Some(run_id) = workflow_field(launch, "runId") {
        workflow.run_id = Some(run_id);
    }
    // A path is only useful whole, so it is bounded like any task field
    // rather than shortened like display text.
    if let Some(directory) = launch.get("transcriptDir").and_then(Value::as_str) {
        if !directory.is_empty() && directory.len() <= MAX_TASK_FIELD_CHARS {
            workflow.transcript_dir = Some(directory.to_string());
        }
    }
    native.tasks.insert(task_id.to_string(), task);
    true
}

fn emit_tasks(native: &ClaudeNativeState, output: &mut AdapterOutput) {
    output.events.push(TurnEvent::TasksChanged {
        tasks: native
            .tasks
            .values()
            .take(MAX_NATIVE_TASKS)
            .cloned()
            .collect(),
    });
}

fn task_usage(value: Option<&Value>) -> Option<AgentTaskUsage> {
    let usage = value?.as_object()?;
    Some(AgentTaskUsage {
        total_tokens: usage.get("total_tokens")?.as_u64()?,
        tool_uses: usage.get("tool_uses")?.as_u64()?,
        duration_ms: usage.get("duration_ms")?.as_u64()?,
    })
}

/// Finish a turn whose background work just drained after its result.
///
/// A one-shot process closes stdin now and keeps reading until Claude exits,
/// so a follow-up answer is still captured. A retained process outlives the
/// turn: Claude answers each task notification with a follow-up
/// `init`…`result` exchange, so the turn ends at that result instead of here,
/// or after [`FOLLOW_UP_GRACE`] if Claude stays silent.
fn background_work_drained(native: &mut ClaudeNativeState, output: &mut AdapterOutput) {
    if !native.result_seen || !native.background_task_ids.is_empty() {
        return;
    }
    if native.retained_turn {
        native.awaiting_follow_up = !native.follow_up_active;
    } else {
        output.terminal = true;
    }
}

fn can_track(native: &ClaudeNativeState, task_id: &str) -> bool {
    native.tasks.contains_key(task_id) || native.tasks.len() < MAX_NATIVE_TASKS
}

fn translate_system_task(
    value: &Value,
    native: &mut ClaudeNativeState,
    output: &mut AdapterOutput,
) {
    match value.get("subtype").and_then(Value::as_str) {
        Some("permission_denied") => {
            let id = value
                .get("tool_use_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let name = value
                .get("tool_name")
                .and_then(Value::as_str)
                .unwrap_or("tool")
                .to_string();
            if let Some(id) = &id {
                native.tool_names.insert(id.clone(), name.clone());
            }
            output.events.push(TurnEvent::ToolCall {
                id,
                name,
                status: ToolCallStatus::Failed,
                input: None,
                output: None,
                error: Some(bounded(
                    value
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("Permission denied."),
                )),
                task_id: task_id_for(native, value.get("agent_id").and_then(Value::as_str)),
            });
        }
        Some("task_started") => {
            let Some(task_id) = value.get("task_id").and_then(Value::as_str) else {
                return;
            };
            if task_id.is_empty() || !can_track(native, task_id) {
                return;
            }
            if let Some(tool_use_id) = value.get("tool_use_id").and_then(Value::as_str) {
                if !tool_use_id.is_empty() {
                    native
                        .tool_use_to_task
                        .insert(tool_use_id.to_string(), task_id.to_string());
                }
            }
            let agent_type = optional_bounded(value.get("subagent_type").and_then(Value::as_str));
            let description = bounded(
                value
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("Background task"),
            );
            let task_type = value.get("task_type").and_then(Value::as_str);
            // The Workflow tool's result can arrive first and already record
            // where the run writes its transcripts.
            let mut workflow = native
                .tasks
                .get(task_id)
                .and_then(|task| task.workflow.clone());
            if task_type == Some("local_workflow") {
                let workflow = workflow.get_or_insert_with(AgentWorkflow::default);
                if let Some(name) = value.get("workflow_name").and_then(Value::as_str) {
                    workflow.name = Some(workflow_text(name));
                }
            }
            native.tasks.insert(
                task_id.to_string(),
                AgentTask {
                    id: task_id.to_string(),
                    kind: task_kind(task_type, agent_type.as_deref()),
                    description: description.clone(),
                    status: "running".to_string(),
                    agent_type: agent_type.clone(),
                    error: None,
                    summary: None,
                    workflow,
                },
            );
            if value.get("is_backgrounded").and_then(Value::as_bool) == Some(true) {
                native.background_task_ids.insert(task_id.to_string());
            }
            output.events.push(TurnEvent::TaskActivity {
                activity: AgentTaskActivity {
                    task_id: task_id.to_string(),
                    kind: AgentTaskActivityKind::Started,
                    description: Some(description),
                    status: Some("running".to_string()),
                    agent_type,
                    summary: None,
                    last_tool_name: None,
                    spawn_depth: value
                        .get("spawn_depth")
                        .and_then(Value::as_u64)
                        .map(|depth| u32::try_from(depth).unwrap_or(u32::MAX)),
                    usage: None,
                    workflow_agent: None,
                },
            });
            emit_tasks(native, output);
        }
        Some("task_updated") => {
            let Some(task_id) = value.get("task_id").and_then(Value::as_str) else {
                return;
            };
            if task_id.is_empty() || !can_track(native, task_id) {
                return;
            }
            let patch = value.get("patch").and_then(Value::as_object);
            let mut task = native
                .tasks
                .get(task_id)
                .cloned()
                .unwrap_or_else(|| fallback_task(task_id));
            let description = patch
                .and_then(|patch| patch.get("description"))
                .and_then(Value::as_str)
                .map(bounded);
            let status = patch
                .and_then(|patch| patch.get("status"))
                .and_then(Value::as_str)
                .map(bounded);
            let error = patch
                .and_then(|patch| patch.get("error"))
                .and_then(Value::as_str)
                .map(bounded);
            if let Some(description) = &description {
                task.description.clone_from(description);
            }
            if let Some(status) = &status {
                task.status.clone_from(status);
            }
            if let Some(error) = &error {
                task.error.clone_from(&Some(error.clone()));
            }
            let agent_type = task.agent_type.clone();
            native.tasks.insert(task_id.to_string(), task);
            match patch
                .and_then(|patch| patch.get("is_backgrounded"))
                .and_then(Value::as_bool)
            {
                Some(true) => {
                    native.background_task_ids.insert(task_id.to_string());
                }
                Some(false) => {
                    native.background_task_ids.remove(task_id);
                }
                None => {}
            }
            output.events.push(TurnEvent::TaskActivity {
                activity: AgentTaskActivity {
                    task_id: task_id.to_string(),
                    kind: AgentTaskActivityKind::Updated,
                    description,
                    status,
                    agent_type,
                    summary: None,
                    last_tool_name: None,
                    spawn_depth: None,
                    usage: None,
                    workflow_agent: None,
                },
            });
            emit_tasks(native, output);
        }
        Some("task_progress") => {
            let Some(task_id) = value.get("task_id").and_then(Value::as_str) else {
                return;
            };
            if task_id.is_empty() || !can_track(native, task_id) {
                return;
            }
            let mut task = native
                .tasks
                .get(task_id)
                .cloned()
                .unwrap_or_else(|| fallback_task(task_id));
            let agent_type = optional_bounded(value.get("subagent_type").and_then(Value::as_str))
                .or_else(|| task.agent_type.clone());
            let description = optional_bounded(value.get("description").and_then(Value::as_str));
            let summary = optional_bounded(value.get("summary").and_then(Value::as_str));
            let workflow_progress = value.get("workflow_progress").and_then(Value::as_array);
            if workflow_progress.is_some() {
                task.kind = "workflow".to_string();
            } else {
                task.kind = task_kind(Some(&task.kind), agent_type.as_deref());
            }
            let mut changed = native.tasks.get(task_id).is_none_or(|known| {
                known.kind != task.kind
                    || description
                        .as_ref()
                        .is_some_and(|description| *description != known.description)
                    || known.agent_type != agent_type
                    || (summary.is_some() && known.summary != summary)
            });
            if let Some(description) = &description {
                task.description.clone_from(description);
            }
            task.agent_type.clone_from(&agent_type);
            if let Some(summary) = &summary {
                task.summary.clone_from(&Some(summary.clone()));
            }
            let status = task.status.clone();
            if task.kind == "workflow" {
                // Every tick carries the whole workflow: replace it, and
                // record only agents that changed state, not each tick.
                // A tick that only moves counters, or changes nothing, is
                // sent at most every few ticks.
                let mut counters_changed = false;
                if let Some(entries) = workflow_progress {
                    let previous = task.workflow.take().unwrap_or_default();
                    let next = parse_workflow(entries, &previous);
                    if previous != next {
                        if workflow_counters_only(&previous, &next) {
                            counters_changed = true;
                        } else {
                            changed = true;
                        }
                    }
                    for agent in changed_workflow_agents(&previous, &next) {
                        output.events.push(TurnEvent::TaskActivity {
                            activity: AgentTaskActivity {
                                task_id: task_id.to_string(),
                                kind: AgentTaskActivityKind::Progress,
                                description: None,
                                status: Some(status.clone()),
                                agent_type: agent_type.clone(),
                                summary: Some(workflow_agent_summary(&agent)),
                                last_tool_name: None,
                                spawn_depth: None,
                                usage: task_usage(value.get("usage")),
                                workflow_agent: Some(Box::new(agent)),
                            },
                        });
                    }
                    task.workflow = Some(next);
                }
                native.tasks.insert(task_id.to_string(), task);
                if counters_changed {
                    native.workflow_unsent.insert(task_id.to_string());
                }
                let quiet = native
                    .workflow_quiet_ticks
                    .entry(task_id.to_string())
                    .or_default();
                *quiet += 1;
                let emit = changed
                    || (*quiet >= WORKFLOW_COUNTER_TICKS
                        && native.workflow_unsent.contains(task_id));
                if emit {
                    *quiet = 0;
                    native.workflow_unsent.remove(task_id);
                    emit_tasks(native, output);
                }
                return;
            }
            native.tasks.insert(task_id.to_string(), task);
            output.events.push(TurnEvent::TaskActivity {
                activity: AgentTaskActivity {
                    task_id: task_id.to_string(),
                    kind: AgentTaskActivityKind::Progress,
                    description,
                    status: Some(status),
                    agent_type,
                    summary,
                    last_tool_name: optional_bounded(
                        value.get("last_tool_name").and_then(Value::as_str),
                    ),
                    spawn_depth: None,
                    usage: task_usage(value.get("usage")),
                    workflow_agent: None,
                },
            });
            emit_tasks(native, output);
        }
        Some("task_notification") => {
            let Some(task_id) = value.get("task_id").and_then(Value::as_str) else {
                return;
            };
            if task_id.is_empty() || !can_track(native, task_id) {
                return;
            }
            let mut task = native
                .tasks
                .get(task_id)
                .cloned()
                .unwrap_or_else(|| fallback_task(task_id));
            let status = bounded(
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or(&task.status),
            );
            let summary = optional_bounded(value.get("summary").and_then(Value::as_str));
            task.status.clone_from(&status);
            if let Some(summary) = &summary {
                task.summary.clone_from(&Some(summary.clone()));
            }
            if status == "failed" && task.error.is_none() {
                task.error.clone_from(&summary);
            }
            let agent_type = task.agent_type.clone();
            native.tasks.insert(task_id.to_string(), task);
            native.background_task_ids.remove(task_id);
            background_work_drained(native, output);
            let kind = match status.as_str() {
                "failed" => AgentTaskActivityKind::Failed,
                "stopped" | "killed" => AgentTaskActivityKind::Stopped,
                _ => AgentTaskActivityKind::Completed,
            };
            output.events.push(TurnEvent::TaskActivity {
                activity: AgentTaskActivity {
                    task_id: task_id.to_string(),
                    kind,
                    description: None,
                    status: Some(status),
                    agent_type,
                    summary,
                    last_tool_name: None,
                    spawn_depth: None,
                    usage: task_usage(value.get("usage")),
                    workflow_agent: None,
                },
            });
            emit_tasks(native, output);
        }
        Some("background_tasks_changed") => {
            let mut live_ids = BTreeSet::new();
            if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
                for raw in tasks {
                    let Some(task_id) = raw.get("task_id").and_then(Value::as_str) else {
                        continue;
                    };
                    if task_id.is_empty() || !can_track(native, task_id) {
                        continue;
                    }
                    live_ids.insert(task_id.to_string());
                    let mut task = native
                        .tasks
                        .get(task_id)
                        .cloned()
                        .unwrap_or_else(|| fallback_task(task_id));
                    task.kind = task_kind(
                        raw.get("task_type").and_then(Value::as_str),
                        task.agent_type.as_deref(),
                    );
                    if let Some(description) = raw.get("description").and_then(Value::as_str) {
                        task.description = bounded(description);
                    }
                    task.status = "running".to_string();
                    native.tasks.insert(task_id.to_string(), task);
                }
            }
            for task_id in native.background_task_ids.difference(&live_ids) {
                if let Some(task) = native.tasks.get_mut(task_id) {
                    if !matches!(
                        task.status.as_str(),
                        "completed" | "failed" | "killed" | "stopped" | "ended"
                    ) {
                        task.status = "ended".to_string();
                    }
                }
            }
            native.background_task_ids = live_ids;
            background_work_drained(native, output);
            emit_tasks(native, output);
        }
        _ => {}
    }
}

fn tool_result_text(content: Option<&Value>, tool_use_result: Option<&Value>) -> String {
    if let Some(result) = tool_use_result.and_then(Value::as_object) {
        if let Some(stdout) = result.get("stdout").and_then(Value::as_str) {
            let stderr = result
                .get("stderr")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let combined = format!("{stdout}{stderr}");
            if !combined.trim().is_empty() {
                return bounded(&combined);
            }
        }
        if let Some(file) = result.get("file").and_then(Value::as_object) {
            if let Some(content) = file.get("content").and_then(Value::as_str) {
                return bounded(content);
            }
        }
    }
    if let Some(content) = content.and_then(Value::as_str) {
        return bounded(content);
    }
    bounded(&content.cloned().unwrap_or(Value::Null).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_and_maps_auto_permission_mode() {
        let adapter = Claude::default();
        let group = adapter.control_groups().into_iter().next().unwrap();
        assert!(group.options.iter().any(|option| option.id == "auto"));

        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request
            .harness_options
            .insert("permission_mode".into(), "auto".into());
        let command = adapter.command(&request).unwrap();
        assert!(command
            .args
            .windows(2)
            .any(|args| args == ["--permission-mode", "auto"]));
        assert!(!command
            .args
            .iter()
            .any(|argument| argument == "--dangerously-skip-permissions"));
    }

    #[test]
    fn exposes_claude_interactive_permission_modes_with_native_labels() {
        let adapter = Claude::default();
        let group = adapter.control_groups().into_iter().next().unwrap();
        let options = group
            .options
            .iter()
            .map(|option| (option.id.as_str(), option.label.as_str(), option.is_default))
            .collect::<Vec<_>>();

        assert_eq!(
            options,
            vec![
                ("manual", "Always ask", true),
                ("acceptEdits", "Accept file edits", false),
                ("plan", "Plan mode", false),
                ("auto", "Auto mode", false),
                ("bypassPermissions", "Bypass", false),
            ]
        );
    }

    #[test]
    fn maps_default_permission_to_current_cli_default_mode() {
        let adapter = Claude::with_executable("/bin/sh");
        let request = TurnRequest::new(Provider::Claude, ".", "test");
        let command = adapter.command(&request).unwrap();
        let arguments = command
            .args
            .iter()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(arguments
            .windows(2)
            .any(|pair| pair == ["--permission-mode", "manual"]));
    }

    #[test]
    fn supports_dont_ask_for_explicit_headless_requests_without_advertising_it() {
        let adapter = Claude::with_executable("/bin/sh");
        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request.permission_mode = PermissionMode::Custom("dontAsk".into());

        let command = adapter.command(&request).unwrap();
        let arguments = command
            .args
            .iter()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(arguments
            .windows(2)
            .any(|pair| pair == ["--permission-mode", "dontAsk"]));
        assert!(!adapter.control_groups()[0]
            .options
            .iter()
            .any(|option| option.id == "dontAsk"));
    }

    #[test]
    fn applies_structured_launch_context_without_exposing_mcp_secrets() {
        let adapter = Claude::with_executable("/bin/sh");
        let mut request = TurnRequest::new(Provider::Claude, ".", "test");
        request.environment.insert(
            "FLEET_MCP_TOKEN".into(),
            crate::SecretString::new("super-secret-token"),
        );
        request.launch_context.system_prompt_append =
            Some("Follow Fleet's standing instructions.".into());
        request.launch_context.allowed_tools =
            Some(vec!["Read".into(), "mcp__temps_fleet__create_agent".into()]);
        request.launch_context.strict_mcp_config = true;
        request.launch_context.mcp_servers.insert(
            "temps_fleet".into(),
            McpServerConfig::Stdio {
                command: "/usr/local/bin/temps-fleet".into(),
                args: vec!["mcp-server".into()],
                environment_from: BTreeMap::from([(
                    "TEMPS_FLEET_MCP_PARENT_TOKEN".into(),
                    "FLEET_MCP_TOKEN".into(),
                )]),
            },
        );

        let command = adapter.command(&request).unwrap();
        let arguments = command
            .args
            .iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(arguments.windows(2).any(|pair| {
            pair == [
                "--append-system-prompt",
                "Follow Fleet's standing instructions.",
            ]
        }));
        assert!(arguments
            .windows(2)
            .any(|pair| { pair == ["--tools", "Read,mcp__temps_fleet__create_agent"] }));
        assert!(arguments
            .iter()
            .any(|argument| argument == "--strict-mcp-config"));
        let config = arguments
            .windows(2)
            .find(|pair| pair[0] == "--mcp-config")
            .map(|pair| serde_json::from_str::<Value>(&pair[1]).unwrap())
            .expect("MCP configuration argument");
        assert_eq!(
            config.pointer("/mcpServers/temps_fleet/env/TEMPS_FLEET_MCP_PARENT_TOKEN"),
            Some(&json!("${FLEET_MCP_TOKEN}"))
        );
        assert!(!arguments.join(" ").contains("super-secret-token"));
        assert!(!format!("{request:?}").contains("super-secret-token"));
    }

    #[test]
    fn discovers_claude_native_models_from_the_control_initialization() {
        let adapter = Claude::default();
        let probe = adapter.catalog_probe().unwrap();
        let arguments = probe
            .command
            .args
            .iter()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(arguments.windows(2).any(|pair| pair == ["--tools", ""]));
        assert!(arguments
            .iter()
            .any(|argument| argument == "--setting-sources="));
        let request: Value =
            serde_json::from_slice(probe.command.initial_stdin.as_deref().unwrap()).unwrap();
        assert_eq!(
            request.pointer("/request/subtype"),
            Some(&json!("initialize"))
        );

        let frame = json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": "temps-agent-runtime-model-catalog",
                "response": {
                    "commands": [
                        {
                            "name": "effort",
                            "description": "Set effort level for model usage",
                            "argumentHint": "<low|medium|high|xhigh|max|ultracode|auto>"
                        },
                        {
                            "name": "deep-research",
                            "description": "Deep research harness. (dynamic workflow)",
                            "argumentHint": ""
                        }
                    ],
                    "models": [
                        {
                            "value": "default",
                            "resolvedModel": "claude-sonnet-5",
                            "displayName": "Default (recommended)",
                            "description": "Sonnet 5 · Efficient for routine tasks",
                            "supportedEffortLevels": ["low", "medium", "high", "xhigh", "max"]
                        },
                        {
                            "value": "sonnet",
                            "resolvedModel": "claude-sonnet-5",
                            "displayName": "Sonnet",
                            "description": "Sonnet 5 · Efficient for routine tasks",
                            "supportedEffortLevels": ["low", "medium", "high", "xhigh", "max"],
                            "supportsAdaptiveThinking": true
                        },
                        {
                            "value": "haiku",
                            "resolvedModel": "claude-haiku-4-5-20251001",
                            "displayName": "Haiku",
                            "description": "Haiku 4.5 · Fastest for quick answers"
                        }
                    ]
                }
            }
        });
        let catalog = adapter.parse_catalog(&[frame.to_string()]).unwrap();

        assert_eq!(catalog.status, HarnessCatalogStatus::Ready);
        assert_eq!(catalog.source, "control_initialize");
        assert_eq!(catalog.models.len(), 2);
        assert_eq!(catalog.models[0].id, "sonnet");
        assert_eq!(catalog.models[0].label, "Sonnet");
        assert_eq!(
            catalog.models[0].description.as_deref(),
            Some("Sonnet 5 · Efficient for routine tasks")
        );
        assert!(catalog.models[0].is_default);
        assert!(catalog.models[0]
            .reasoning_efforts
            .iter()
            .any(|effort| effort.id == "high" && effort.is_default));
        assert_eq!(
            catalog.models[0]
                .reasoning_efforts
                .iter()
                .map(|effort| (effort.id.as_str(), effort.label.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("off", "Off"),
                ("low", "Low"),
                ("medium", "Medium"),
                ("high", "High"),
                ("xhigh", "Extra high"),
                ("max", "Max"),
                ("ultracode", "Ultra code"),
            ]
        );
        assert!(
            catalog.models[1].reasoning_efforts.is_empty(),
            "{:?}",
            catalog.models[1].reasoning_efforts
        );
        assert!(catalog.models.iter().all(|model| model.id != "default"));
    }

    #[test]
    fn omits_disabled_thinking_for_models_that_require_adaptive_thinking() {
        let model = json!({
            "value": "fable",
            "resolvedModel": "claude-fable-5",
            "displayName": "Fable",
            "supportedEffortLevels": ["low", "medium", "high", "xhigh", "max"],
            "supportsAdaptiveThinking": true
        });

        let parsed = claude_catalog_model(&model, Some("claude-fable-5"), true).unwrap();

        assert!(!parsed
            .reasoning_efforts
            .iter()
            .any(|effort| effort.id == "off"));
        assert!(parsed
            .reasoning_efforts
            .iter()
            .any(|effort| effort.id == "ultracode"));
    }

    #[test]
    fn reads_context_limits_only_when_the_harness_catalog_advertises_them() {
        let one_million = json!({
            "value": "opus[1m]",
            "resolvedModel": "claude-opus-5[1m]",
            "displayName": "Opus 5 (1M context)"
        });
        let explicit = json!({
            "value": "custom",
            "displayName": "Custom",
            "contextWindowTokens": 320_000
        });
        let unknown = json!({
            "value": "sonnet",
            "displayName": "Sonnet"
        });

        assert_eq!(advertised_context_window(&one_million), Some(1_000_000));
        assert_eq!(advertised_context_window(&explicit), Some(320_000));
        assert_eq!(advertised_context_window(&unknown), None);
    }

    #[test]
    fn maps_special_claude_thinking_choices_to_session_launch_flags() {
        let adapter = Claude::default();
        let mut off = TurnRequest::new(Provider::Claude, ".", "test");
        off.reasoning = Some("off".into());
        let off_command = adapter.command(&off).unwrap();
        assert!(off_command
            .args
            .windows(2)
            .any(|args| args == ["--thinking", "disabled"]));
        assert!(!off_command
            .args
            .iter()
            .any(|argument| argument == "--effort"));

        let mut ultracode = TurnRequest::new(Provider::Claude, ".", "test");
        ultracode.reasoning = Some("ultracode".into());
        let ultracode_command = adapter.command(&ultracode).unwrap();
        assert!(ultracode_command
            .args
            .windows(2)
            .any(|args| args == ["--effort", "xhigh"]));
        assert!(ultracode_command
            .args
            .windows(2)
            .any(|args| args == ["--settings", r#"{"ultracode":true}"#]));
    }

    #[test]
    fn maps_automatic_compaction_policy_to_native_cli_flags() {
        let adapter = Claude::default();
        let mut automatic = TurnRequest::new(Provider::Claude, ".", "test");
        automatic.auto_compaction = AutoCompactionPolicy::Automatic;
        let command = adapter.command(&automatic).unwrap();
        assert!(command
            .args
            .windows(2)
            .any(|args| args == ["--autocompact", "auto"]));

        let mut threshold = TurnRequest::new(Provider::Claude, ".", "test");
        threshold.auto_compaction = AutoCompactionPolicy::TokenThreshold { tokens: 250_000 };
        let command = adapter.command(&threshold).unwrap();
        assert!(command
            .args
            .windows(2)
            .any(|args| args == ["--autocompact", "250000"]));
    }

    #[test]
    fn command_accepts_an_executable_that_exists_only_in_the_transport() {
        let adapter = Claude::with_executable("/remote/bin/claude");
        let request = TurnRequest::new(Provider::Claude, "/remote/workspace", "test");

        let command = adapter.command(&request).unwrap();

        assert_eq!(command.program, PathBuf::from("/remote/bin/claude"));
    }

    #[test]
    fn parses_text_delta_without_duplicating_assistant_message() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let delta = r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}}"#;
        let assistant =
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}"#;
        assert_eq!(
            adapter.parse_line(delta, &mut state).unwrap().events.len(),
            1
        );
        let events = adapter.parse_line(assistant, &mut state).unwrap().events;
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(state.result.text, "hi");
    }

    #[test]
    fn emits_context_occupancy_from_claude_usage_components() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"assistant","message":{"model":"claude-opus-5[1m]","content":[],"usage":{"input_tokens":100,"cache_creation_input_tokens":20,"cache_read_input_tokens":300,"output_tokens":40}}}"#,
                &mut state,
            )
            .unwrap();

        assert!(matches!(
            &output.events[0],
            TurnEvent::Usage(Usage {
                input_tokens: Some(100),
                cache_creation_input_tokens: Some(20),
                cache_read_input_tokens: Some(300),
                output_tokens: Some(40),
                context_window: Some(ContextWindowUsage {
                    used_tokens: Some(460),
                    limit_tokens: Some(1_000_000),
                    estimated: true,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(
            state
                .result
                .usage
                .context_window
                .as_ref()
                .and_then(ContextWindowUsage::remaining_tokens),
            Some(999_540)
        );
    }

    #[test]
    fn the_result_frame_reports_the_context_limit_of_a_bare_model_id() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let message = adapter
            .parse_line(
                r#"{"type":"assistant","message":{"model":"claude-opus-5-5","content":[],"usage":{"input_tokens":2,"cache_creation_input_tokens":20000,"cache_read_input_tokens":349000,"output_tokens":900}}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &message.events[0],
            TurnEvent::Usage(Usage {
                context_window: Some(ContextWindowUsage {
                    limit_tokens: None,
                    ..
                }),
                ..
            })
        ));

        let result = adapter
            .parse_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","usage":{"input_tokens":40,"output_tokens":1800},"modelUsage":{"claude-haiku-4-5":{"contextWindow":200000},"claude-opus-5-5":{"contextWindow":1000000,"canonicalModel":"claude-opus-5-5"}}}"#,
                &mut state,
            )
            .unwrap();

        let reported = result
            .events
            .iter()
            .find_map(|event| match event {
                TurnEvent::Usage(usage) => usage.context_window.clone(),
                _ => None,
            })
            .expect("the result reports the context window");
        assert_eq!(reported.used_tokens, Some(369_902));
        assert_eq!(reported.limit_tokens, Some(1_000_000));
        assert_eq!(reported.model.as_deref(), Some("claude-opus-5-5"));
        assert!(reported.estimated);
        assert_eq!(state.result.usage.context_window, Some(reported));
    }

    #[test]
    fn a_subagent_message_neither_sets_occupancy_nor_selects_the_limit() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        for line in [
            r#"{"type":"system","subtype":"init","session_id":"s","model":"claude-opus-5-5"}"#,
            r#"{"type":"assistant","message":{"model":"claude-opus-5-5","content":[],"usage":{"input_tokens":2,"cache_creation_input_tokens":20000,"cache_read_input_tokens":349000,"output_tokens":900}}}"#,
            r#"{"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"model":"claude-haiku-4-5","content":[],"usage":{"input_tokens":40,"output_tokens":10}}}"#,
        ] {
            adapter.parse_line(line, &mut state).unwrap();
        }
        adapter
            .parse_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","modelUsage":{"claude-haiku-4-5":{"contextWindow":200000},"claude-opus-5-5":{"contextWindow":1000000}}}"#,
                &mut state,
            )
            .unwrap();
        let context = state.result.usage.context_window.unwrap();
        assert_eq!(context.used_tokens, Some(369_902));
        assert_eq!(context.limit_tokens, Some(1_000_000));
        assert_eq!(context.model.as_deref(), Some("claude-opus-5-5"));
    }

    #[test]
    fn a_result_without_model_usage_keeps_the_last_occupancy() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        adapter
            .parse_line(
                r#"{"type":"assistant","message":{"model":"claude-opus-5-5","content":[],"usage":{"input_tokens":10,"output_tokens":5}}}"#,
                &mut state,
            )
            .unwrap();
        adapter
            .parse_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","usage":{"input_tokens":10,"output_tokens":5}}"#,
                &mut state,
            )
            .unwrap();
        let context = state.result.usage.context_window.unwrap();
        assert_eq!(context.used_tokens, Some(15));
        assert_eq!(context.limit_tokens, None);
    }

    #[test]
    fn translates_native_compact_boundary_without_provider_session_identifiers() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        state.result.model = Some("claude-opus-5[1m]".into());
        let output = adapter
            .parse_line(
                r#"{"type":"system","subtype":"compact_boundary","compactMetadata":{"trigger":"auto","preTokens":467465,"postTokens":17781,"durationMs":2400,"cumulativeDroppedTokens":449684,"preservedUUIDs":["secret-provider-id"]}}"#,
                &mut state,
            )
            .unwrap();

        assert_eq!(output.events.len(), 2);
        assert!(matches!(
            &output.events[0],
            TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Automatic,
                    pre_tokens: Some(467_465),
                    post_tokens: Some(17_781),
                    dropped_tokens: Some(449_684),
                    cumulative_dropped_tokens: Some(449_684),
                    duration_ms: Some(2_400),
                }
            }
        ));
        assert!(matches!(
            &output.events[1],
            TurnEvent::Usage(Usage {
                context_window: Some(ContextWindowUsage {
                    used_tokens: Some(17_781),
                    limit_tokens: Some(1_000_000),
                    estimated: false,
                    ..
                }),
                ..
            })
        ));
        assert!(!format!("{:?}", output.events).contains("secret-provider-id"));
    }

    fn compaction_events(events: &[TurnEvent]) -> Vec<&TurnEvent> {
        events
            .iter()
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

    fn parse_all(adapter: &Claude, state: &mut AdapterState, records: &[&str]) -> Vec<TurnEvent> {
        records
            .iter()
            .flat_map(|record| adapter.parse_line(record, state).unwrap().events)
            .collect()
    }

    /// Recorded from Claude Code 2.1.283 `--output-format stream-json`: an
    /// automatic compaction mid-turn reports `status: compacting` (repeated as
    /// a keepalive), a success result, then the native boundary.
    #[test]
    fn reports_automatic_compaction_from_start_status_to_boundary() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        adapter
            .prepare_turn(
                &TurnRequest::new(Provider::Claude, "/workspace", "Refactor the parser"),
                &mut state,
            )
            .unwrap();
        let events = parse_all(
            &adapter,
            &mut state,
            &[
                r#"{"type":"system","subtype":"status","status":"compacting","uuid":"u1","session_id":"s"}"#,
                r#"{"type":"system","subtype":"status","status":"compacting","uuid":"u2","session_id":"s"}"#,
                r#"{"type":"system","subtype":"status","status":null,"compact_result":"success","uuid":"u3","session_id":"s"}"#,
                r#"{"type":"system","subtype":"compact_boundary","session_id":"s","uuid":"u4","compact_metadata":{"trigger":"auto","pre_tokens":180000,"post_tokens":12000}}"#,
                r#"{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}"#,
            ],
        );
        let compaction = compaction_events(&events);
        assert_eq!(compaction.len(), 2, "{compaction:?}");
        assert!(matches!(
            compaction[0],
            TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Automatic
            }
        ));
        assert!(matches!(
            compaction[1],
            TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Automatic,
                    pre_tokens: Some(180_000),
                    post_tokens: Some(12_000),
                    dropped_tokens: Some(168_000),
                    ..
                }
            }
        ));
    }

    #[test]
    fn reports_a_manual_compact_prompt_as_a_manual_compaction() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        adapter
            .prepare_turn(
                &TurnRequest::new(Provider::Claude, "/workspace", "/compact keep the plan"),
                &mut state,
            )
            .unwrap();
        let events = parse_all(
            &adapter,
            &mut state,
            &[
                r#"{"type":"system","subtype":"status","status":"compacting","session_id":"s"}"#,
                r#"{"type":"system","subtype":"compact_boundary","session_id":"s","compact_metadata":{"trigger":"manual","pre_tokens":50000}}"#,
            ],
        );
        let compaction = compaction_events(&events);
        assert!(matches!(
            compaction[0],
            TurnEvent::CompactionStarted {
                trigger: CompactionTrigger::Manual
            }
        ));
        assert!(matches!(
            compaction[1],
            TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Manual,
                    ..
                }
            }
        ));
        assert!(!is_manual_compaction_prompt("/compaction-notes please"));
        assert!(is_manual_compaction_prompt("  /compact"));
    }

    #[test]
    fn closes_a_failed_compaction_with_the_provider_diagnostic() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let events = parse_all(
            &adapter,
            &mut state,
            &[
                r#"{"type":"system","subtype":"status","status":"compacting","session_id":"s"}"#,
                r#"{"type":"system","subtype":"status","status":null,"compact_result":"failed","compact_error":"Conversation too long to summarize","session_id":"s"}"#,
                r#"{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}"#,
            ],
        );
        let compaction = compaction_events(&events);
        assert_eq!(compaction.len(), 2, "{compaction:?}");
        assert!(matches!(
            compaction[1],
            TurnEvent::CompactionFailed {
                trigger: CompactionTrigger::Automatic,
                message: Some(message),
            } if message == "Conversation too long to summarize"
        ));
    }

    #[test]
    fn never_leaves_a_compaction_open_after_the_terminal_result() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let events = parse_all(
            &adapter,
            &mut state,
            &[
                r#"{"type":"system","subtype":"status","status":"compacting","session_id":"s"}"#,
                r#"{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}"#,
            ],
        );
        let compaction = compaction_events(&events);
        assert_eq!(compaction.len(), 2, "{compaction:?}");
        assert!(matches!(compaction[1], TurnEvent::CompactionFailed { .. }));

        let mut state = AdapterState::default();
        let events = parse_all(
            &adapter,
            &mut state,
            &[
                r#"{"type":"system","subtype":"status","status":"compacting","session_id":"s"}"#,
                r#"{"type":"system","subtype":"status","status":null,"compact_result":"success","session_id":"s"}"#,
                r#"{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}"#,
            ],
        );
        let compaction = compaction_events(&events);
        assert!(matches!(
            compaction[1],
            TurnEvent::CompactionCompleted {
                compaction: ContextCompaction {
                    trigger: CompactionTrigger::Automatic,
                    pre_tokens: None,
                    ..
                }
            }
        ));
    }

    #[test]
    fn advertises_the_compaction_lifecycle() {
        assert!(Claude::default().turn_capabilities().compaction_lifecycle);
    }

    #[test]
    fn preserves_a_question_between_pre_and_post_answer_text_events() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let records = [
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Before the question."}}}"#,
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tool-question","name":"AskUserQuestion","input":{"questions":[{"header":"Fruit","question":"Which fruit?","options":[{"label":"Apple","description":"Choose apple"}],"multiSelect":false}]}}]}}"#,
            r#"{"type":"control_request","request_id":"question-fruit","request":{"tool_name":"AskUserQuestion","input":{"questions":[{"header":"Fruit","question":"Which fruit?","options":[{"label":"Apple","description":"Choose apple"}],"multiSelect":false}]}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"After the answer."}}}"#,
        ];
        let events = records
            .into_iter()
            .flat_map(|record| adapter.parse_line(record, &mut state).unwrap().events)
            .collect::<Vec<_>>();

        assert!(matches!(
            &events[0],
            TurnEvent::TextDelta { text } if text == "Before the question."
        ));
        assert!(matches!(
            &events[1],
            TurnEvent::ToolCall { name, status: ToolCallStatus::Started, .. }
                if name == "AskUserQuestion"
        ));
        assert!(matches!(
            &events[2],
            TurnEvent::QuestionRequested(request) if request.id == "question-fruit"
        ));
        assert!(matches!(
            &events[3],
            TurnEvent::TextDelta { text } if text == "After the answer."
        ));
        assert_eq!(state.result.text, "Before the question.After the answer.");
    }

    #[test]
    fn publishes_the_effective_permission_mode_and_tracks_native_plan_transitions() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();

        let initialized = adapter
            .parse_line(
                r#"{"type":"system","subtype":"init","permissionMode":"default"}"#,
                &mut state,
            )
            .unwrap();
        assert_eq!(
            initialized.events,
            vec![TurnEvent::PermissionModeChanged {
                mode: PermissionMode::Default,
            }]
        );

        adapter
            .parse_line(
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"enter-plan","name":"EnterPlanMode","input":{}}]}}"#,
                &mut state,
            )
            .unwrap();
        let entered = adapter
            .parse_line(
                r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"enter-plan","content":"Entered plan mode"}]}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            entered.events.as_slice(),
            [
                TurnEvent::ToolCall {
                    status: ToolCallStatus::Succeeded,
                    ..
                },
                TurnEvent::PermissionModeChanged {
                    mode: PermissionMode::Plan
                }
            ]
        ));

        adapter
            .parse_line(
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"exit-plan","name":"ExitPlanMode","input":{}}]}}"#,
                &mut state,
            )
            .unwrap();
        let exited = adapter
            .parse_line(
                r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"exit-plan","content":"Plan approved"}]}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            exited.events.as_slice(),
            [
                TurnEvent::ToolCall {
                    status: ToolCallStatus::Succeeded,
                    ..
                },
                TurnEvent::PermissionModeChanged {
                    mode: PermissionMode::Default
                }
            ]
        ));
    }

    #[test]
    fn exposes_exit_plan_mode_as_a_typed_plan_approval() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"control_request","request_id":"approve-plan","request":{"tool_name":"ExitPlanMode","input":{"description":"Implement the proposed steps"}}}"#,
                &mut state,
            )
            .unwrap();

        assert!(matches!(
            output.events.as_slice(),
            [TurnEvent::PlanApprovalRequested(request)]
                if request.id == "approve-plan" && request.tool_name == "ExitPlanMode"
        ));
        assert!(matches!(
            output.interaction,
            Some(InteractionRequest::Approval { request, .. })
                if request.id == "approve-plan"
        ));
    }

    #[test]
    fn parses_the_harness_session_title() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"system","session_id":"session-123","session_title":"Inspect the runtime"}"#,
                &mut state,
            )
            .unwrap();

        assert_eq!(
            state.result.session_title.as_deref(),
            Some("Inspect the runtime")
        );
        assert_eq!(
            output.events,
            vec![TurnEvent::SessionStarted {
                session_id: "session-123".into(),
                title: Some("Inspect the runtime".into()),
            }]
        );
    }

    #[test]
    fn repeated_resumed_session_id_is_not_started_again() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        state.result.session_id = Some("session-123".into());

        let output = adapter
            .parse_line(
                r#"{"type":"system","session_id":"session-123"}"#,
                &mut state,
            )
            .unwrap();

        assert!(output.events.is_empty(), "{:?}", output.events);
        assert_eq!(state.result.session_id.as_deref(), Some("session-123"));
    }

    #[test]
    fn question_response_merges_answers_into_original_input() {
        let adapter = Claude::default();
        let original = json!({"request":{"input":{"questions":[{"question":"Ship?"}]}}});
        let encoded = adapter
            .question_response(
                &QuestionRequest {
                    id: "q1".into(),
                    prompts: Vec::new(),
                    questions: json!([]),
                },
                &original,
                Some(QuestionAnswer {
                    answers: json!({"Ship?":"Yes"}),
                }),
            )
            .unwrap()
            .unwrap();
        let value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            value.pointer("/response/response/behavior"),
            Some(&json!("allow"))
        );
        assert_eq!(
            value.pointer("/response/response/updatedInput/answers/Ship?"),
            Some(&json!("Yes"))
        );
    }

    #[test]
    fn local_bash_result_uses_task_started_tool_id_to_identify_shell_task() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let started = adapter
            .parse_line(
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_bash","name":"Bash","input":{"command":"pwd"}}]}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &started.events[0],
            TurnEvent::ToolCall { task_id: None, .. }
        ));
        adapter
            .parse_line(
                r#"{"type":"system","subtype":"task_started","task_id":"shell-1","tool_use_id":"toolu_bash","description":"Check directory","task_type":"local_bash"}"#,
                &mut state,
            )
            .unwrap();
        let completed = adapter
            .parse_line(
                r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_bash","content":"/tmp"}]}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &completed.events[0],
            TurnEvent::ToolCall { task_id: Some(id), output: Some(output), .. }
                if id == "shell-1" && output == "/tmp"
        ));
    }

    #[test]
    fn streams_native_subagent_lifecycle_and_nests_tool_calls() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();

        let started = adapter
            .parse_line(
                r#"{"type":"system","subtype":"task_started","task_id":"agent-native-1","tool_use_id":"toolu_agent","description":"Explore the repo","subagent_type":"Explore","task_type":"local_agent","is_backgrounded":true,"spawn_depth":1}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &started.events[0],
            TurnEvent::TaskActivity { activity }
                if activity.task_id == "agent-native-1"
                    && activity.kind == AgentTaskActivityKind::Started
                    && activity.spawn_depth == Some(1)
        ));
        assert!(matches!(
            &started.events[1],
            TurnEvent::TasksChanged { tasks }
                if tasks.len() == 1
                    && tasks[0].kind == "subagent"
                    && tasks[0].agent_type.as_deref() == Some("Explore")
        ));

        let nested = adapter
            .parse_line(
                r#"{"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"tool_use","id":"toolu_bash","name":"Bash","input":{"command":"pwd"}}]}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &nested.events[0],
            TurnEvent::ToolCall { task_id, .. }
                if task_id.as_deref() == Some("agent-native-1")
        ));

        let progress = adapter
            .parse_line(
                r#"{"type":"system","subtype":"task_progress","task_id":"agent-native-1","description":"Reading manifests","summary":"Found the workspace","last_tool_name":"Read","usage":{"total_tokens":500,"tool_uses":3,"duration_ms":1200}}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &progress.events[0],
            TurnEvent::TaskActivity { activity }
                if activity.kind == AgentTaskActivityKind::Progress
                    && activity.usage == Some(AgentTaskUsage {
                        total_tokens: 500,
                        tool_uses: 3,
                        duration_ms: 1200,
                    })
        ));

        let result = adapter
            .parse_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"Parent finished"}"#,
                &mut state,
            )
            .unwrap();
        assert!(
            !result.terminal,
            "background task keeps stdin and stream alive"
        );

        let finished = adapter
            .parse_line(
                r#"{"type":"system","subtype":"task_notification","task_id":"agent-native-1","status":"completed","summary":"Done"}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &finished.events[0],
            TurnEvent::TaskActivity { activity }
                if activity.kind == AgentTaskActivityKind::Completed
        ));
        assert!(
            finished.terminal,
            "the last completion closes interactive stdin"
        );

        let empty = adapter
            .parse_line(
                r#"{"type":"system","subtype":"background_tasks_changed","tasks":[]}"#,
                &mut state,
            )
            .unwrap();
        assert!(empty.terminal, "the task bookend closes interactive stdin");
        assert!(matches!(
            &empty.events[0],
            TurnEvent::TasksChanged { tasks }
                if tasks.len() == 1 && tasks[0].status == "completed"
        ));
    }

    const BACKGROUND_STARTED: &str = r#"{"type":"system","subtype":"task_started","task_id":"agent-bg-1","tool_use_id":"toolu_agent","description":"Wait for the build","subagent_type":"general-purpose","task_type":"local_agent","is_backgrounded":true}"#;
    const PARENT_RESULT: &str =
        r#"{"type":"result","subtype":"success","is_error":false,"result":"Launched"}"#;
    const BACKGROUND_FINISHED: &str = r#"{"type":"system","subtype":"task_notification","task_id":"agent-bg-1","status":"completed","summary":"Build finished"}"#;
    const BACKGROUND_EMPTY: &str =
        r#"{"type":"system","subtype":"background_tasks_changed","tasks":[]}"#;
    const FOLLOW_UP_INIT: &str = r#"{"type":"system","subtype":"init","session_id":"session-bg"}"#;
    const FOLLOW_UP_TEXT: &str = r#"{"type":"assistant","parent_tool_use_id":null,"message":{"content":[{"type":"text","text":"The build finished."}]}}"#;
    const FOLLOW_UP_RESULT: &str =
        r#"{"type":"result","subtype":"success","is_error":false,"result":"The build finished."}"#;

    fn retained_state(adapter: &Claude) -> AdapterState {
        let mut state = AdapterState::default();
        adapter.mark_retained_turn(&mut state);
        state
    }

    #[test]
    fn retained_turn_waits_for_the_follow_up_answer_after_background_drain() {
        let adapter = Claude::default();
        let mut state = retained_state(&adapter);
        for line in [BACKGROUND_STARTED, PARENT_RESULT] {
            assert!(!adapter.parse_line(line, &mut state).unwrap().terminal);
        }
        // Claude answers the notification with its own init..result exchange
        // after the task set drains; ending at the drain would orphan it on a
        // process that outlives this turn.
        for line in [
            BACKGROUND_FINISHED,
            BACKGROUND_EMPTY,
            FOLLOW_UP_INIT,
            FOLLOW_UP_TEXT,
        ] {
            assert!(
                !adapter.parse_line(line, &mut state).unwrap().terminal,
                "{line}"
            );
        }
        assert!(
            adapter
                .parse_line(FOLLOW_UP_RESULT, &mut state)
                .unwrap()
                .terminal
        );
        assert!(state.result.text.ends_with("The build finished."));
    }

    #[test]
    fn retained_drain_without_a_follow_up_completes_after_a_quiet_grace() {
        let adapter = Claude::default();
        let mut state = retained_state(&adapter);
        for line in [BACKGROUND_STARTED, PARENT_RESULT] {
            adapter.parse_line(line, &mut state).unwrap();
        }
        assert_eq!(adapter.retained_completion_grace(&state), None);
        adapter.parse_line(BACKGROUND_FINISHED, &mut state).unwrap();
        assert_eq!(
            adapter.retained_completion_grace(&state),
            Some(FOLLOW_UP_GRACE)
        );
        adapter.parse_line(FOLLOW_UP_INIT, &mut state).unwrap();
        assert_eq!(
            adapter.retained_completion_grace(&state),
            None,
            "a started follow-up ends with its own result"
        );
    }

    #[test]
    fn retained_turn_hands_off_only_between_exchanges() {
        let adapter = Claude::default();
        let mut state = retained_state(&adapter);
        adapter.parse_line(BACKGROUND_STARTED, &mut state).unwrap();
        assert!(
            !adapter.retained_handoff_ready(&state),
            "the turn has not answered yet"
        );
        adapter.parse_line(PARENT_RESULT, &mut state).unwrap();
        assert!(adapter.retained_handoff_ready(&state));
        adapter.parse_line(FOLLOW_UP_INIT, &mut state).unwrap();
        assert!(
            !adapter.retained_handoff_ready(&state),
            "a follow-up answer in progress is never split across turns"
        );
        adapter.parse_line(FOLLOW_UP_RESULT, &mut state).unwrap();
        assert!(adapter.retained_handoff_ready(&state));
        adapter.parse_line(BACKGROUND_FINISHED, &mut state).unwrap();
        assert!(
            !adapter.retained_handoff_ready(&state),
            "the follow-up for the drained task may already be starting"
        );

        let mut one_shot = AdapterState::default();
        for line in [BACKGROUND_STARTED, PARENT_RESULT] {
            adapter.parse_line(line, &mut one_shot).unwrap();
        }
        assert!(
            !adapter.retained_handoff_ready(&one_shot),
            "a one-shot process exits with its turn and cannot be handed off"
        );

        let mut failed = retained_state(&adapter);
        adapter.parse_line(BACKGROUND_STARTED, &mut failed).unwrap();
        adapter
            .parse_line(
                r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":"tool failed"}"#,
                &mut failed,
            )
            .unwrap();
        assert!(
            !adapter.retained_handoff_ready(&failed),
            "a failed answer must surface as this turn's failure"
        );
    }

    #[test]
    fn handed_off_turn_keeps_tracking_inherited_background_tasks() {
        let adapter = Claude::default();
        let mut previous = retained_state(&adapter);
        for line in [BACKGROUND_STARTED, PARENT_RESULT] {
            adapter.parse_line(line, &mut previous).unwrap();
        }

        let mut next = AdapterState::default();
        adapter
            .prepare_turn(
                &TurnRequest::new(Provider::Claude, ".", "/compact"),
                &mut next,
            )
            .unwrap();
        adapter.mark_retained_turn(&mut next);
        adapter.inherit_retained_handoff(previous, &mut next);
        let native = peek_native_state(&next).unwrap();
        assert!(native.background_task_ids.contains("agent-bg-1"));
        assert!(
            !native.result_seen,
            "the next turn still owes its own answer"
        );
        assert!(
            native.manual_compaction_turn,
            "the next turn keeps its own prompt state"
        );

        let nested = adapter
            .parse_line(
                r#"{"type":"assistant","parent_tool_use_id":"toolu_agent","message":{"content":[{"type":"tool_use","id":"toolu_wait","name":"Bash","input":{"command":"make"}}]}}"#,
                &mut next,
            )
            .unwrap();
        assert!(matches!(
            &nested.events[0],
            TurnEvent::ToolCall { task_id, .. } if task_id.as_deref() == Some("agent-bg-1")
        ));
        let own_answer = adapter
            .parse_line(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"4"}"#,
                &mut next,
            )
            .unwrap();
        assert!(
            !own_answer.terminal,
            "inherited background work keeps the new turn open"
        );
        let finished = adapter.parse_line(BACKGROUND_FINISHED, &mut next).unwrap();
        assert!(matches!(
            &finished.events[0],
            TurnEvent::TaskActivity { activity }
                if activity.task_id == "agent-bg-1"
                    && activity.kind == AgentTaskActivityKind::Completed
        ));
    }

    #[test]
    fn a_chain_of_handoffs_never_fills_the_task_table() {
        let adapter = Claude::default();
        let mut state = retained_state(&adapter);
        for line in [BACKGROUND_STARTED, PARENT_RESULT] {
            adapter.parse_line(line, &mut state).unwrap();
        }
        // A long-running subagent keeps every turn open while each turn
        // starts and finishes another one, then hands off.
        for turn in 0..(MAX_NATIVE_TASKS * 2) {
            let mut next = retained_state(&adapter);
            adapter.inherit_retained_handoff(state, &mut next);
            state = next;
            let task_id = format!("short-{turn}");
            let started = adapter
                .parse_line(
                    &format!(
                        r#"{{"type":"system","subtype":"task_started","task_id":"{task_id}","tool_use_id":"toolu_{task_id}","description":"Short task","task_type":"local_agent","is_backgrounded":true}}"#
                    ),
                    &mut state,
                )
                .unwrap();
            assert!(
                started.events.iter().any(|event| matches!(
                    event,
                    TurnEvent::TaskActivity { activity } if activity.task_id == task_id
                )),
                "turn {turn} stopped tracking new subagents"
            );
            adapter
                .parse_line(
                    &format!(
                        r#"{{"type":"assistant","parent_tool_use_id":"toolu_{task_id}","message":{{"content":[{{"type":"tool_use","id":"toolu_call_{task_id}","name":"Bash","input":{{}}}}]}}}}"#
                    ),
                    &mut state,
                )
                .unwrap();
            adapter
                .parse_line(
                    &format!(
                        r#"{{"type":"user","parent_tool_use_id":"toolu_{task_id}","message":{{"content":[{{"type":"tool_result","tool_use_id":"toolu_call_{task_id}","content":"ok"}}]}}}}"#
                    ),
                    &mut state,
                )
                .unwrap();
            adapter
                .parse_line(
                    &format!(
                        r#"{{"type":"system","subtype":"task_notification","task_id":"{task_id}","status":"completed"}}"#
                    ),
                    &mut state,
                )
                .unwrap();
            adapter.parse_line(PARENT_RESULT, &mut state).unwrap();
        }
        let native = peek_native_state(&state).unwrap();
        assert!(native.tasks.len() <= 2, "{:?}", native.tasks.keys());
        assert!(native.tool_use_to_task.len() <= 2);
        assert!(
            native.tool_names.is_empty() || native.tool_names.len() <= 1,
            "answered tool calls are forgotten: {:?}",
            native.tool_names
        );
        assert!(native.background_task_ids.contains("agent-bg-1"));
    }

    fn own_uuids(state: &AdapterState) -> Vec<String> {
        peek_native_state(state)
            .unwrap()
            .own_commands
            .keys()
            .cloned()
            .collect()
    }

    fn lifecycle(uuid: &str, state: &str) -> String {
        format!(r#"{{"type":"command_lifecycle","command_uuid":"{uuid}","state":"{state}"}}"#)
    }

    /// A retained turn prepared like the runtime does, with its prompt UUID.
    fn retained_turn_with_prompt(adapter: &Claude) -> (AdapterState, String) {
        let mut state = AdapterState::default();
        adapter
            .prepare_turn(
                &TurnRequest::new(Provider::Claude, ".", "build it"),
                &mut state,
            )
            .unwrap();
        adapter.mark_retained_turn(&mut state);
        let prompt = own_uuids(&state).remove(0);
        (state, prompt)
    }

    const WORKFLOW_STREAM: &str = include_str!("fixtures/claude_workflow_stream.jsonl");
    const WORKFLOW_AGENT_TRANSCRIPT: &str =
        include_str!("fixtures/claude_workflow_agent_transcript.jsonl");

    /// Replay a recorded Workflow run, returning every event and the final
    /// task snapshot.
    fn replay_workflow() -> (Vec<TurnEvent>, Vec<AgentTask>) {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let mut events = Vec::new();
        let mut tasks = Vec::new();
        for line in WORKFLOW_STREAM.lines() {
            for event in adapter.parse_line(line, &mut state).unwrap().events {
                if let TurnEvent::TasksChanged { tasks: snapshot } = &event {
                    tasks.clone_from(snapshot);
                }
                events.push(event);
            }
        }
        (events, tasks)
    }

    #[test]
    fn a_workflow_task_carries_its_phases_agents_and_transcripts() {
        let (_, tasks) = replay_workflow();
        let [task] = tasks.as_slice() else {
            panic!("one workflow task: {tasks:?}");
        };
        assert_eq!(task.kind, "workflow");
        let workflow = task.workflow.as_ref().expect("workflow structure");
        assert_eq!(workflow.name.as_deref(), Some("probe-tools"));
        assert_eq!(workflow.run_id.as_deref(), Some("wf_1cd9e1a5-ccf"));
        assert_eq!(
            workflow.transcript_dir.as_deref(),
            Some("/home/user/.claude/projects/-workspace/session-1/subagents/workflows/wf_1cd9e1a5-ccf")
        );
        let phases: Vec<_> = workflow
            .phases
            .iter()
            .map(|p| (p.index, p.title.as_str()))
            .collect();
        assert_eq!(phases, [(1, "Scan"), (2, "Report")]);
        let agents: Vec<_> = workflow
            .agents
            .iter()
            .map(|a| (a.label.as_str(), a.phase_index, a.state))
            .collect();
        assert_eq!(
            agents,
            [
                ("scan:read", Some(1), AgentWorkflowAgentState::Completed),
                ("scan:list", Some(1), AgentWorkflowAgentState::Completed),
                ("report", Some(2), AgentWorkflowAgentState::Completed),
            ]
        );
        let read = &workflow.agents[0];
        assert_eq!(read.last_tool_name.as_deref(), Some("Read"));
        assert_eq!(read.tool_calls, Some(1));
        assert_eq!(read.result_preview.as_deref(), Some("alpha"));
        assert_eq!(
            workflow.agent_transcript_path(read).unwrap(),
            std::path::Path::new(
                "/home/user/.claude/projects/-workspace/session-1/subagents/workflows/wf_1cd9e1a5-ccf/agent-ab60d568c07a44b25.jsonl"
            )
        );
        assert_eq!(workflow.omitted_agents, 0);
    }

    #[test]
    fn workflow_activity_records_agent_state_changes_not_every_tick() {
        let (events, _) = replay_workflow();
        let changes: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                TurnEvent::TaskActivity { activity }
                    if activity.kind == AgentTaskActivityKind::Progress =>
                {
                    let agent = activity.workflow_agent.as_ref().expect("agent change");
                    Some((agent.label.clone(), agent.state, activity.summary.clone()))
                }
                _ => None,
            })
            .collect();
        let ticks = WORKFLOW_STREAM.matches("\"task_progress\"").count();
        assert!(
            changes.len() < ticks,
            "{} changes for {ticks} ticks",
            changes.len()
        );
        assert!(changes.contains(&(
            "scan:read".to_string(),
            AgentWorkflowAgentState::Running,
            Some("scan:read started".to_string())
        )));
        assert!(changes.contains(&(
            "report".to_string(),
            AgentWorkflowAgentState::Completed,
            Some("report completed".to_string())
        )));
        // Each agent's transitions are recorded once each.
        let mut seen = std::collections::BTreeSet::new();
        for (label, state, _) in &changes {
            assert!(seen.insert((label.clone(), *state as u8)), "{changes:?}");
        }
    }

    #[test]
    fn counter_only_workflow_ticks_are_emitted_every_few_ticks() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let tick = |tokens: u64, workflow_state: &str| {
            json!({"type": "system", "subtype": "task_progress", "task_id": "wf-1",
            "workflow_progress": [
                {"type": "workflow_agent", "index": 1, "label": "a", "state": workflow_state,
                 "startedAt": 1, "tokens": tokens}
            ]})
            .to_string()
        };
        let snapshots = |state: &mut AdapterState, line: String| {
            adapter
                .parse_line(&line, state)
                .unwrap()
                .events
                .iter()
                .filter(|event| matches!(event, TurnEvent::TasksChanged { .. }))
                .count()
        };
        assert_eq!(snapshots(&mut state, tick(1, "start")), 1);
        let emitted: usize = (2..=u64::from(WORKFLOW_COUNTER_TICKS) * 2 + 1)
            .map(|tokens| snapshots(&mut state, tick(tokens, "start")))
            .sum();
        assert_eq!(emitted, 2);
        // A state change is emitted at once, carrying the latest counters.
        assert_eq!(snapshots(&mut state, tick(99, "done")), 1);
        let task = take_native_state(&mut state).tasks.remove("wf-1").unwrap();
        assert_eq!(task.workflow.unwrap().agents[0].tokens, Some(99));
    }

    #[test]
    fn workflow_ticks_that_change_nothing_are_not_emitted() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let tick = json!({"type": "system", "subtype": "task_progress", "task_id": "wf-1",
        "workflow_progress": [
            {"type": "workflow_agent", "index": 1, "label": "a", "state": "start",
             "startedAt": 1, "tokens": 5}
        ]})
        .to_string();
        let bare =
            json!({"type": "system", "subtype": "task_progress", "task_id": "wf-1"}).to_string();
        let snapshots = |state: &mut AdapterState, line: &str| {
            adapter
                .parse_line(line, state)
                .unwrap()
                .events
                .iter()
                .filter(|event| matches!(event, TurnEvent::TasksChanged { .. }))
                .count()
        };
        assert_eq!(snapshots(&mut state, &tick), 1);
        let emitted: usize = (0..WORKFLOW_COUNTER_TICKS * 3)
            .map(|n| snapshots(&mut state, if n % 2 == 0 { &tick } else { &bare }))
            .sum();
        assert_eq!(emitted, 0);
        let task = take_native_state(&mut state).tasks.remove("wf-1").unwrap();
        assert_eq!(task.kind, "workflow");
        assert_eq!(task.workflow.unwrap().agents.len(), 1);
    }

    #[test]
    fn workflow_agent_states_are_normalized() {
        let parse = |entry: Value| parse_workflow_agent(&entry).unwrap().state;
        assert_eq!(
            parse(json!({"index": 1, "state": "start", "queuedAt": 1})),
            AgentWorkflowAgentState::Queued
        );
        assert_eq!(
            parse(json!({"index": 1, "state": "start", "startedAt": 2})),
            AgentWorkflowAgentState::Running
        );
        assert_eq!(
            parse(json!({"index": 1, "state": "error", "blocked": true, "error": "refused"})),
            AgentWorkflowAgentState::Blocked
        );
        assert_eq!(
            parse(json!({"index": 1, "state": "error", "error": {"message": "boom"}})),
            AgentWorkflowAgentState::Failed
        );
        assert!(parse_workflow_agent(&json!({"state": "done"})).is_none());
    }

    #[test]
    fn a_large_workflow_is_bounded() {
        let mut entries: Vec<Value> = (1..=MAX_WORKFLOW_AGENTS + 5)
            .map(|index| {
                json!({"type": "workflow_agent", "index": index, "label": "x".repeat(1_000),
                       "state": "start", "startedAt": 1})
            })
            .collect();
        entries.extend(
            (0..MAX_WORKFLOW_LOGS + 3)
                .map(|n| json!({"type": "workflow_log", "message": format!("log {n}")})),
        );
        let workflow = parse_workflow(&entries, &AgentWorkflow::default());
        assert_eq!(workflow.agents.len(), MAX_WORKFLOW_AGENTS);
        assert_eq!(workflow.omitted_agents, 5);
        assert!(workflow.agents[0].label.chars().count() <= MAX_WORKFLOW_TEXT_CHARS + 1);
        assert_eq!(workflow.logs.len(), MAX_WORKFLOW_LOGS);
        assert_eq!(workflow.logs.last().map(String::as_str), Some("log 12"));
    }

    #[test]
    fn transcript_paths_never_leave_the_transcript_directory() {
        let mut workflow = AgentWorkflow {
            transcript_dir: Some("/runs/wf_1".to_string()),
            ..AgentWorkflow::default()
        };
        let mut agent = parse_workflow_agent(&json!({"index": 1})).unwrap();
        assert_eq!(workflow.agent_transcript_path(&agent), None);
        for unsafe_id in ["../../etc/passwd", "a/b", "a\\b", ".", ""] {
            agent.agent_id = Some(unsafe_id.to_string());
            assert_eq!(workflow.agent_transcript_path(&agent), None, "{unsafe_id}");
        }
        agent.agent_id = Some("ab60d568c07a44b25".to_string());
        assert_eq!(
            workflow.agent_transcript_path(&agent).unwrap(),
            std::path::Path::new("/runs/wf_1/agent-ab60d568c07a44b25.jsonl")
        );
        workflow.transcript_dir = None;
        assert_eq!(workflow.agent_transcript_path(&agent), None);
    }

    #[test]
    fn a_workflow_agent_transcript_reads_as_its_activity() {
        let entries = Claude::default().transcript_activity(WORKFLOW_AGENT_TRANSCRIPT, 100);
        let summary: Vec<String> = entries
            .iter()
            .map(|entry| match &entry.event {
                TurnEvent::ToolCall {
                    name,
                    status,
                    input,
                    output,
                    ..
                } => format!(
                    "{name} {status:?} {} {}",
                    input.as_ref().map(Value::to_string).unwrap_or_default(),
                    output.clone().unwrap_or_default()
                ),
                TurnEvent::TextDelta { text } => format!("text {text}"),
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(
            summary,
            [
                r#"Read Started {"file_path":"/workspace/notes.txt"} "#.to_string(),
                "Read Succeeded  1\talpha\n2\t".to_string(),
                "text alpha".to_string(),
            ]
        );
        assert!(entries.iter().all(|entry| entry.timestamp.is_some()));
        // Only the most recent entries are kept.
        let last = Claude::default().transcript_activity(WORKFLOW_AGENT_TRANSCRIPT, 1);
        assert!(matches!(
            &last[..],
            [AgentTranscriptEntry {
                event: TurnEvent::TextDelta { .. },
                ..
            }]
        ));
        assert_eq!(
            Claude::default().transcript_activity(WORKFLOW_AGENT_TRANSCRIPT, 0),
            Vec::<AgentTranscriptEntry>::new()
        );
    }

    #[test]
    fn a_transcript_replays_recorded_reasoning() {
        let transcript = [
            json!({"type": "assistant", "timestamp": "2026-01-01T00:00:00Z", "message": {
            "role": "assistant", "content": [
                {"type": "thinking", "thinking": "Check the notes first.", "signature": "s"},
                {"type": "text", "text": "Done."}
            ]}}),
            json!({"type": "assistant", "message": {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "", "signature": "redacted"}
            ]}}),
        ]
        .map(|line| line.to_string())
        .join("\n");
        let events: Vec<_> = Claude::default()
            .transcript_activity(&transcript, 10)
            .into_iter()
            .map(|entry| entry.event)
            .collect();
        assert_eq!(
            events,
            [
                TurnEvent::ReasoningDelta {
                    text: "Check the notes first.".into()
                },
                TurnEvent::TextDelta {
                    text: "Done.".into()
                },
            ]
        );
    }

    #[test]
    fn workflow_snapshots_from_before_the_field_existed_still_load() {
        let task: AgentTask = serde_json::from_value(json!({
            "id": "t1", "kind": "subagent", "description": "d", "status": "running",
            "agent_type": null, "error": null, "summary": null
        }))
        .unwrap();
        assert_eq!(task.workflow, None);
        let activity: AgentTaskActivity = serde_json::from_value(json!({
            "task_id": "t1", "kind": "progress", "description": null, "status": null,
            "agent_type": null, "summary": null, "last_tool_name": null, "spawn_depth": null,
            "usage": null
        }))
        .unwrap();
        assert_eq!(activity.workflow_agent, None);
    }

    #[test]
    fn message_uuids_are_unique_version_four_uuids() {
        let first = new_message_uuid();
        assert_ne!(first, new_message_uuid());
        let parts: Vec<_> = first.split('-').map(str::len).collect();
        assert_eq!(parts, [8, 4, 4, 4, 12], "{first}");
        assert!(first.chars().all(|c| c == '-' || c.is_ascii_hexdigit()));
        assert_eq!(&first[14..15], "4");
        assert!(matches!(&first[19..20], "8" | "9" | "a" | "b"), "{first}");
    }

    #[test]
    fn the_prompt_carries_the_uuid_its_turn_tracks() {
        let adapter = Claude::default();
        let (state, prompt) = retained_turn_with_prompt(&adapter);
        let request = TurnRequest::new(Provider::Claude, ".", "build it");
        let spec = adapter.command_for_turn(&request, &state).unwrap();
        let frame: Value = serde_json::from_slice(spec.initial_stdin.as_deref().unwrap()).unwrap();
        assert_eq!(frame["uuid"], prompt.as_str());
        assert_eq!(frame["message"]["content"][0]["text"], "build it");
    }

    #[test]
    fn a_message_folded_into_the_running_reply_is_answered_by_its_turn() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        for line in [lifecycle(&prompt, "queued"), lifecycle(&prompt, "started")] {
            adapter.parse_line(&line, &mut state).unwrap();
        }
        let frame = adapter
            .encode_user_message("also run the tests", &mut state)
            .unwrap()
            .expect("a retained turn accepts messages");
        let frame: Value = serde_json::from_slice(&frame).unwrap();
        let message = frame["uuid"].as_str().unwrap().to_string();
        assert_eq!(frame["message"]["content"][0]["text"], "also run the tests");

        // Claude folds the queued message into the running exchange: one
        // result, then the prompt's own completion.
        for line in [
            lifecycle(&message, "queued"),
            lifecycle(&message, "started"),
            lifecycle(&message, "completed"),
            PARENT_RESULT.to_string(),
        ] {
            assert!(
                !adapter.parse_line(&line, &mut state).unwrap().terminal,
                "{line}"
            );
        }
        assert!(
            adapter
                .parse_line(&lifecycle(&prompt, "completed"), &mut state)
                .unwrap()
                .terminal
        );
    }

    #[test]
    fn a_reply_before_a_message_is_queued_does_not_end_the_turn() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        for line in [
            lifecycle(&prompt, "queued"),
            lifecycle(&prompt, "started"),
            PARENT_RESULT.to_string(),
            lifecycle(&prompt, "completed"),
        ] {
            adapter.parse_line(&line, &mut state).unwrap();
        }
        // Written, but Claude has not reported it yet when another result
        // (for example its answer to a finished task) arrives.
        adapter.encode_user_message("next", &mut state).unwrap();
        assert!(
            !adapter
                .parse_line(PARENT_RESULT, &mut state)
                .unwrap()
                .terminal
        );
        assert!(!adapter.retained_handoff_ready(&state));
    }

    #[test]
    fn claudes_own_command_after_the_answer_is_a_follow_up() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        for line in [
            lifecycle(&prompt, "started"),
            BACKGROUND_STARTED.to_string(),
            PARENT_RESULT.to_string(),
            lifecycle(&prompt, "completed"),
            BACKGROUND_FINISHED.to_string(),
        ] {
            assert!(
                !adapter.parse_line(&line, &mut state).unwrap().terminal,
                "{line}"
            );
        }
        // Claude answers the notification under a command UUID of its own.
        assert!(
            !adapter
                .parse_line(&lifecycle("notification-1", "started"), &mut state)
                .unwrap()
                .terminal
        );
        assert_eq!(adapter.retained_completion_grace(&state), None);
        assert!(
            adapter
                .parse_line(FOLLOW_UP_RESULT, &mut state)
                .unwrap()
                .terminal
        );
    }

    #[test]
    fn interruption_stops_queued_messages_and_settles_once_they_end() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        adapter
            .parse_line(&lifecycle(&prompt, "started"), &mut state)
            .unwrap();
        let queued = adapter
            .encode_user_message("queued", &mut state)
            .unwrap()
            .unwrap();
        let queued: Value = serde_json::from_slice(&queued).unwrap();
        let queued = queued["uuid"].as_str().unwrap().to_string();
        adapter
            .parse_line(&lifecycle(&queued, "queued"), &mut state)
            .unwrap();

        assert_eq!(adapter.retained_interrupt_settled(&state), Some(false));
        let interrupt: Value =
            serde_json::from_slice(&adapter.interrupt_request(&state).unwrap()).unwrap();
        assert_eq!(interrupt["request"]["subtype"], "interrupt");
        assert_eq!(interrupt["request"]["cancel_queued"], true);
        adapter
            .parse_line(
                &format!(
                    r#"{{"type":"control_response","response":{{"subtype":"success","request_id":"{}","response":{{"still_queued":["{queued}"]}}}}}}"#,
                    interrupt["request_id"].as_str().unwrap()
                ),
                &mut state,
            )
            .unwrap();
        for line in [
            r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":""}"#
                .to_string(),
            lifecycle(&prompt, "cancelled"),
        ] {
            adapter.parse_line(&line, &mut state).unwrap();
        }
        assert_eq!(adapter.retained_interrupt_settled(&state), Some(false));
        // Claude starts the queued message; the turn stops it too.
        let started = adapter
            .parse_line(&lifecycle(&queued, "started"), &mut state)
            .unwrap();
        assert_eq!(started.writes.len(), 1);
        let again: Value = serde_json::from_slice(&started.writes[0]).unwrap();
        assert_eq!(again["request"]["subtype"], "interrupt");
        adapter
            .parse_line(&lifecycle(&queued, "cancelled"), &mut state)
            .unwrap();
        assert_eq!(adapter.retained_interrupt_settled(&state), Some(true));
    }

    #[test]
    fn claude_cancelling_queued_messages_with_the_stop_settles_the_turn() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        adapter
            .parse_line(&lifecycle(&prompt, "started"), &mut state)
            .unwrap();
        let queued = adapter
            .encode_user_message("queued", &mut state)
            .unwrap()
            .unwrap();
        let queued: Value = serde_json::from_slice(&queued).unwrap();
        let queued = queued["uuid"].as_str().unwrap().to_string();
        adapter
            .parse_line(&lifecycle(&queued, "queued"), &mut state)
            .unwrap();
        adapter.interrupt_request(&state).unwrap();

        // `interrupt_cancel_queued_v1`: the queued message is cancelled with
        // the abort and never starts, so nothing has to be stopped again.
        let mut writes = Vec::new();
        for line in [
            lifecycle(&queued, "cancelled"),
            format!(
                r#"{{"type":"control_response","response":{{"subtype":"success","request_id":"{INTERRUPT_REQUEST_ID}","response":{{"still_queued":[],"cancelled":["{queued}"]}}}}}}"#
            ),
            r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":""}"#
                .to_string(),
            lifecycle(&prompt, "cancelled"),
        ] {
            writes.extend(adapter.parse_line(&line, &mut state).unwrap().writes);
        }
        assert_eq!(writes, Vec::<Vec<u8>>::new());
        assert_eq!(adapter.retained_interrupt_settled(&state), Some(true));
    }

    #[test]
    fn an_answered_turn_settles_without_an_interrupt() {
        let adapter = Claude::default();
        let (mut state, prompt) = retained_turn_with_prompt(&adapter);
        for line in [
            lifecycle(&prompt, "started"),
            BACKGROUND_STARTED.to_string(),
            PARENT_RESULT.to_string(),
            lifecycle(&prompt, "completed"),
        ] {
            adapter.parse_line(&line, &mut state).unwrap();
        }
        assert_eq!(adapter.retained_interrupt_settled(&state), Some(true));
        assert!(adapter.retained_background_work(&state));
    }

    #[test]
    fn one_shot_turns_keep_hard_cancellation_and_reject_messages() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        adapter
            .prepare_turn(
                &TurnRequest::new(Provider::Claude, ".", "build it"),
                &mut state,
            )
            .unwrap();
        assert!(adapter.interrupt_request(&state).is_none());
        assert_eq!(adapter.retained_interrupt_settled(&state), None);
        assert!(adapter
            .encode_user_message("later", &mut state)
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_turn_bounds_the_messages_it_accepts() {
        let adapter = Claude::default();
        let (mut state, _) = retained_turn_with_prompt(&adapter);
        for _ in 1..MAX_TURN_MESSAGES {
            adapter
                .encode_user_message("more", &mut state)
                .unwrap()
                .unwrap();
        }
        assert!(matches!(
            adapter.encode_user_message("one too many", &mut state),
            Err(RuntimeError::InvalidRequest {
                field: "message",
                ..
            })
        ));
    }

    #[test]
    fn retains_error_result_as_a_native_terminal_failure() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":"authentication failed for secret"}"#,
                &mut state,
            )
            .unwrap();

        assert!(output.terminal);
        assert!(output.events.is_empty(), "{:?}", output.events);
        assert!(state.result.text.is_empty(), "{:?}", state.result.text);
        let failure = state.terminal_failure.unwrap();
        assert_eq!(
            failure.kind,
            crate::ProviderProcessErrorKind::AuthenticationFailed
        );
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("claude::error_during_execution")
        );
        assert_eq!(failure.diagnostic, "authentication failed for secret");
    }

    #[test]
    fn retains_errors_array_when_claude_omits_result_text() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"result","subtype":"error_during_execution","is_error":true,"errors":["No conversation found with session ID: old-session"]}"#,
                &mut state,
            )
            .expect("Claude result parses");

        assert!(output.terminal);
        assert!(output.events.is_empty(), "{:?}", output.events);
        let failure = state.terminal_failure.expect("terminal failure");
        assert_eq!(
            failure.diagnostic,
            "No conversation found with session ID: old-session"
        );
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("claude::error_during_execution")
        );
    }

    #[test]
    fn links_permission_denials_to_their_native_subagent() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        adapter
            .parse_line(
                r#"{"type":"system","subtype":"task_started","task_id":"agent-1","tool_use_id":"toolu_agent","description":"Check policy","task_type":"local_agent"}"#,
                &mut state,
            )
            .unwrap();
        let denied = adapter
            .parse_line(
                r#"{"type":"system","subtype":"permission_denied","agent_id":"toolu_agent","tool_use_id":"toolu_bash","tool_name":"Bash","message":"Denied by policy"}"#,
                &mut state,
            )
            .unwrap();
        assert!(matches!(
            &denied.events[0],
            TurnEvent::ToolCall { status: ToolCallStatus::Failed, task_id, .. }
                if task_id.as_deref() == Some("agent-1")
        ));
    }

    #[test]
    fn normalizes_claude_session_and_weekly_usage_windows() {
        let adapter = Claude::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed_warning","rateLimitType":"five_hour","unifiedWindows":{"five_hour":{"utilization":0.63,"resetsAt":1780000000},"seven_day":{"utilization":0.41,"resetsAt":1780500000}}}}"#,
                &mut state,
            )
            .unwrap();

        let TurnEvent::AccountUsageUpdated { usage } = &output.events[0] else {
            panic!("expected account usage event");
        };
        assert_eq!(usage.windows.len(), 2);
        assert_eq!(usage.windows[0].kind, AccountUsageWindowKind::Session);
        assert!((usage.windows[0].used_percent - 63.0).abs() < f64::EPSILON * 100.0);
        assert_eq!(usage.windows[0].duration_minutes, Some(300));
        assert_eq!(usage.windows[1].kind, AccountUsageWindowKind::Weekly);
        assert_eq!(usage.windows[1].resets_at_unix_seconds, Some(1_780_500_000));
    }

    #[test]
    fn keeps_model_specific_claude_weekly_windows() {
        let usage = claude_account_usage(&json!({
            "rate_limit_info": {
                "rateLimitType": "seven_day_sonnet",
                "utilization": 1.12,
                "resetsAt": 1_780_500_000
            }
        }))
        .unwrap();

        assert_eq!(usage.windows[0].id, "seven_day_sonnet");
        assert_eq!(usage.windows[0].kind, AccountUsageWindowKind::Weekly);
        assert!((usage.windows[0].used_percent - 112.0).abs() < f64::EPSILON * 100.0);
    }

    #[test]
    fn parses_fetch_on_demand_claude_account_usage() {
        let report = Claude::default()
            .parse_account_usage_probe(&[r#"{"type":"temps_agent_runtime_account_usage","status":"available","usage":{"provider":"claude","plan":"Max 20","windows":[{"id":"five_hour","kind":"session","used_percent":63.0,"duration_minutes":300,"resets_at_unix_seconds":1780000000},{"id":"weekly_scoped:model:opus","label":"Opus","kind":"weekly","used_percent":41.0,"duration_minutes":10080,"resets_at_unix_seconds":1780500000}],"credits":null}}"#.into()])
            .unwrap();

        assert_eq!(report.status, crate::AccountUsageStatus::Available);
        let usage = report.usage.unwrap();
        assert_eq!(usage.plan.as_deref(), Some("Max 20"));
        assert_eq!(usage.windows[1].label.as_deref(), Some("Opus"));
        assert_eq!(usage.windows[1].kind, AccountUsageWindowKind::Weekly);
    }

    #[test]
    fn preserves_fetch_on_demand_claude_unavailability() {
        let report = Claude::default()
            .parse_account_usage_probe(&[r#"{"type":"temps_agent_runtime_account_usage","status":"unavailable","reason":"Claude Code is not authenticated on this execution host","retryable":false}"#.into()])
            .unwrap();

        assert_eq!(report.status, crate::AccountUsageStatus::Unavailable);
        assert!(report.usage.is_none());
        assert_eq!(
            report.reason.as_deref(),
            Some("Claude Code is not authenticated on this execution host")
        );
        assert!(!report.retryable);
    }

    #[test]
    fn parses_provider_native_authentication_status() {
        let adapter = Claude::default();
        let authenticated = adapter
            .parse_authentication_probe(
                br#"{"loggedIn":true,"authMethod":"oauth"}"#,
                "",
                TransportExitStatus {
                    success: true,
                    code: Some(0),
                },
            )
            .unwrap();
        assert_eq!(
            authenticated.status,
            crate::HarnessAuthenticationStatus::Authenticated
        );

        let required = adapter
            .parse_authentication_probe(
                br#"{"loggedIn":false,"authMethod":"none"}"#,
                "",
                TransportExitStatus {
                    success: false,
                    code: Some(1),
                },
            )
            .unwrap();
        assert_eq!(
            required.status,
            crate::HarnessAuthenticationStatus::Required
        );
    }
}
