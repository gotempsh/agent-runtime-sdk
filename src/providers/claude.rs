use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::adapter::{inspect_executable, resolve_executable, AdapterState};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    AccountCredits, AccountUsageProbeSpec, AccountUsageReport, AccountUsageSnapshot,
    AccountUsageWindow, AccountUsageWindowKind, AdapterOutput, AgentAdapter, AgentTask,
    AgentTaskActivity, AgentTaskActivityKind, AgentTaskUsage, ApprovalDecision, ApprovalRequest,
    AuthenticationProbeSpec, AutoCompactionPolicy, CatalogProbeSpec, CommandSpec,
    CompactionTrigger, ContextCompaction, ContextWindowUsage, HarnessAuthentication,
    HarnessCatalogStatus, HarnessControlGroup, HarnessControlKind, HarnessControlOption,
    HarnessModel, HarnessModelCatalog, HarnessReasoningEffort, InteractionRequest,
    LaunchContextCapabilities, McpServerConfig, PermissionMode, PermissionSupport, Provider,
    ProviderReadiness, ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result, RunStatus,
    RuntimeError, ToolCallStatus, TransportExitStatus, TurnEvent, TurnRequest, Usage,
};

const CLAUDE_STATE_KEY: &str = "claude.native_tasks";
const MAX_NATIVE_TASKS: usize = 32;
const MAX_TASK_FIELD_CHARS: usize = 4_000;

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
                    translate_compaction(&value, state, &mut output);
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
                usage.context_window = context_window_usage(&value, state.result.model.as_deref());
                if usage != Usage::default() {
                    super::merge_usage(&mut state.result.usage, &usage);
                    output.events.push(TurnEvent::Usage(usage));
                }
            }
            "user" => {
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
                        let tool_name = native
                            .tool_names
                            .get(tool_use_id)
                            .cloned()
                            .unwrap_or_else(|| "tool".to_string());
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
                            task_id: task_id.clone(),
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
                // Claude can emit its terminal result before background Task
                // subagents finish. Keep stdin available for their approvals
                // and continue reading task progress until the native task set
                // clears and the provider exits.
                output.terminal = native.background_task_ids.is_empty();
                let failed = value.get("is_error").and_then(Value::as_bool) == Some(true);
                state.result.status = if failed {
                    let diagnostic = value
                        .get("result")
                        .or_else(|| value.pointer("/error/message"))
                        .or_else(|| value.get("error"))
                        .and_then(Value::as_str)
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
                let usage = super::usage_from(&value);
                super::merge_usage(&mut state.result.usage, &usage);
                if usage != crate::Usage::default() {
                    output.events.push(TurnEvent::Usage(usage));
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
            ApprovalDecision::Allow => json!({"behavior": "allow", "updatedInput": original_input}),
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
    }
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
            native.tasks.insert(
                task_id.to_string(),
                AgentTask {
                    id: task_id.to_string(),
                    kind: task_kind(
                        value.get("task_type").and_then(Value::as_str),
                        agent_type.as_deref(),
                    ),
                    description: description.clone(),
                    status: "running".to_string(),
                    agent_type: agent_type.clone(),
                    error: None,
                    summary: None,
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
            task.kind = task_kind(Some(&task.kind), agent_type.as_deref());
            if let Some(description) = &description {
                task.description.clone_from(description);
            }
            task.agent_type.clone_from(&agent_type);
            if let Some(summary) = &summary {
                task.summary.clone_from(&Some(summary.clone()));
            }
            let status = task.status.clone();
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
            if native.result_seen && native.background_task_ids.is_empty() {
                output.terminal = true;
            }
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
            if native.result_seen && native.background_task_ids.is_empty() {
                output.terminal = true;
            }
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
        assert!(catalog.models[1].reasoning_efforts.is_empty());
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
        assert!(adapter
            .parse_line(assistant, &mut state)
            .unwrap()
            .events
            .is_empty());
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

        assert!(output.events.is_empty());
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
        assert!(output.events.is_empty());
        assert!(state.result.text.is_empty());
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
