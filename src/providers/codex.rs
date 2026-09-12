use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::adapter::{inspect_executable, resolve_executable, AdapterState};
use crate::error::classify_provider_failure;
use crate::lifecycle::DeliveryState;
use crate::{
    AccountCredits, AccountUsageProbeSpec, AccountUsageReport, AccountUsageSnapshot,
    AccountUsageWindow, AccountUsageWindowKind, AdapterOutput, AgentAdapter, ApprovalDecision,
    ApprovalRequest, AuthenticationProbeSpec, CatalogProbeSpec, CommandSpec, HarnessAuthentication,
    HarnessCatalogStatus, HarnessControlGroup, HarnessControlKind, HarnessControlOption,
    HarnessModel, HarnessModelCatalog, HarnessReasoningEffort, HarnessServiceTier,
    LaunchContextCapabilities, McpServerConfig, PermissionMode, PermissionSupport, Provider,
    ProviderReadiness, ProviderTerminalFailure, QuestionAnswer, QuestionRequest, Result,
    RuntimeError, ToolCallStatus, TransportExitStatus, TurnEvent, TurnRequest,
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRelay {
    base_url: String,
    token_env: String,
}

// Only trusted embedders select the relay destination. Credentials are referenced
// from an explicit per-turn environment, never serialized into command arguments.
fn append_model_relay(
    spec: &mut CommandSpec,
    request: &TurnRequest,
    trusted_http_origin: Option<&str>,
) -> Result<()> {
    if request.harness_options.contains_key("config_overrides") {
        return Err(RuntimeError::InvalidRequest {
            field: "harness_options.config_overrides",
            message: "arbitrary Codex configuration overrides are not supported".into(),
        });
    }
    let Some(encoded) = request.harness_options.get("model_relay") else {
        return Ok(());
    };
    let invalid = || {
        RuntimeError::InvalidRequest {
        field: "harness_options.model_relay",
        message: "expected an HTTPS or loopback HTTP base_url without credentials or URL parameters, and an explicit turn environment token_env reference".into(),
    }
    };
    if encoded.len() > 8192 {
        return Err(invalid());
    }
    let relay: ModelRelay = serde_json::from_str(encoded).map_err(|_| invalid())?;
    let url =
        crate::url_security::validate_http_endpoint(&relay.base_url).map_err(|_| invalid())?;
    if (url.scheme() == "http"
        && !crate::url_security::is_loopback_endpoint(&url)
        && trusted_http_origin != Some(url.origin().ascii_serialization().as_str()))
        || url.query().is_some()
        || relay.token_env.is_empty()
        || relay.token_env.len() > 128
        || !relay
            .token_env
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        || !request.environment.contains_key(&relay.token_env)
    {
        return Err(invalid());
    }
    for value in [
        "model_provider=\"sdk_relay\"".to_owned(),
        "model_providers.sdk_relay.name=\"Runtime model relay\"".to_owned(),
        format!(
            "model_providers.sdk_relay.base_url={}",
            codex_config_string(&relay.base_url)?
        ),
        "model_providers.sdk_relay.wire_api=\"responses\"".to_owned(),
        "model_providers.sdk_relay.requires_openai_auth=false".to_owned(),
        "model_providers.sdk_relay.supports_websockets=false".to_owned(),
        format!(
            "model_providers.sdk_relay.env_key={}",
            codex_config_string(&relay.token_env)?
        ),
    ] {
        spec.args.extend(["--config".into(), value.into()]);
    }
    Ok(())
}

/// Codex CLI adapter using `codex exec --json`.
#[derive(Debug, Clone, Default)]
pub struct Codex {
    executable: Option<PathBuf>,
    trusted_http_model_relay_origin: Option<String>,
}

impl Codex {
    /// Use an executable path meaningful inside the selected transport.
    pub fn with_executable(path: impl Into<PathBuf>) -> Self {
        Self {
            executable: Some(path.into()),
            trusted_http_model_relay_origin: None,
        }
    }

    /// Explicitly trusts one plaintext HTTP origin for a host-isolated model relay.
    ///
    /// Only the embedding host may call this; turn requests cannot add trusted origins.
    /// The host must provide network isolation and prevent untrusted workloads from
    /// impersonating the relay. This does not permit credentials, paths, or URL parameters.
    pub fn with_insecure_model_relay_origin(mut self, origin: &str) -> Result<Self> {
        let invalid = || RuntimeError::InvalidRequest {
            field: "trusted_http_model_relay_origin",
            message: "expected one bare HTTP origin with an explicit host and port".into(),
        };
        let url = crate::url_security::validate_http_endpoint(origin).map_err(|_| invalid())?;
        if url.scheme() != "http"
            || url.path() != "/"
            || url.query().is_some()
            || url.port().is_none()
            || origin != url.origin().ascii_serialization()
        {
            return Err(invalid());
        }
        self.trusted_http_model_relay_origin = Some(origin.to_owned());
        Ok(self)
    }

    fn resolved(&self) -> Option<PathBuf> {
        resolve_executable(self.executable.as_ref(), "codex")
    }

    fn configured_executable(&self) -> PathBuf {
        self.executable
            .clone()
            .unwrap_or_else(|| PathBuf::from("codex"))
    }
}

fn control_group(
    id: &str,
    label: &str,
    kind: HarnessControlKind,
    options: &[(&str, &str, &str, bool, bool)],
) -> HarnessControlGroup {
    HarnessControlGroup {
        id: id.into(),
        label: label.into(),
        kind,
        options: options
            .iter()
            .map(
                |(id, label, description, is_default, dangerous)| HarnessControlOption {
                    id: (*id).into(),
                    label: (*label).into(),
                    description: (*description).into(),
                    is_default: *is_default,
                    dangerous: *dangerous,
                },
            )
            .collect(),
    }
}

fn catalog_protocol(message: &str) -> RuntimeError {
    RuntimeError::Protocol {
        provider: Provider::Codex,
        message: message.into(),
    }
}

fn codex_config_string(value: &str) -> Result<String> {
    serde_json::to_string(value).map_err(|error| RuntimeError::Protocol {
        provider: Provider::Codex,
        message: format!("could not encode Codex configuration: {error}"),
    })
}

fn codex_tool_event(item: &Value, completed: bool) -> Option<TurnEvent> {
    let kind = item.get("type")?.as_str()?;
    let (name, input, output, error) = match kind {
        "mcp_tool_call" => {
            let server = item
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("unknown-server");
            let tool = item
                .get("tool")
                .and_then(Value::as_str)
                .unwrap_or("unknown-tool");
            let name = format!("mcp__{server}__{tool}");
            let error = item
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    item.get("error")
                        .filter(|value| !value.is_null())
                        .map(Value::to_string)
                });
            (
                name,
                item.get("arguments").cloned(),
                item.get("result")
                    .filter(|value| !value.is_null())
                    .map(Value::to_string),
                error,
            )
        }
        "command_execution" => {
            let input = item
                .get("command")
                .cloned()
                .map(|command| json!({"command": command}));
            let output = item
                .get("aggregated_output")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let mut error = item
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| item.get("error").and_then(Value::as_str).map(str::to_owned))
                .or_else(|| {
                    (item.get("status").and_then(Value::as_str) == Some("failed")).then(|| {
                        item.get("exit_code").and_then(Value::as_i64).map_or_else(
                            || "command execution failed".to_owned(),
                            |code| format!("command exited with code {code}"),
                        )
                    })
                });
            if let (Some(reason), Some(detail)) = (error.as_mut(), output.as_deref()) {
                let detail = detail.trim();
                if !detail.is_empty() {
                    reason.push_str(": ");
                    reason.extend(detail.chars().take(4096));
                }
            }
            (kind.to_owned(), input, output, error)
        }
        "file_change" => (
            kind.to_owned(),
            item.get("changes").cloned(),
            item.get("output")
                .and_then(Value::as_str)
                .map(str::to_owned),
            item.pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ),
        _ => return None,
    };
    let failed = item.get("status").and_then(Value::as_str) == Some("failed");
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

fn append_http_mcp_launch_context(spec: &mut CommandSpec, request: &TurnRequest) -> Result<()> {
    for (name, server) in &request.launch_context.mcp_servers {
        let McpServerConfig::Http { url, headers_from } = server else {
            return Err(RuntimeError::InvalidRequest {
                field: "launch_context.mcp_servers",
                message: "Codex supports only turn-scoped HTTP MCP servers".into(),
            });
        };
        spec.args.extend([
            "--config".into(),
            format!("mcp_servers.{name}.url={}", codex_config_string(url)?).into(),
        ]);
        for (header, source) in headers_from {
            spec.args.extend([
                "--config".into(),
                format!(
                    "mcp_servers.{name}.env_http_headers.{header}={}",
                    codex_config_string(source)?
                )
                .into(),
            ]);
        }
    }
    Ok(())
}

fn response_result(lines: &[String], id: u64) -> Result<Value> {
    lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| value.get("id").and_then(Value::as_u64) == Some(id))
        .and_then(|value| value.get("result").cloned())
        .ok_or_else(|| catalog_protocol(&format!("Codex app server omitted response id {id}")))
}

fn parse_model(model: &Value) -> Option<HarnessModel> {
    let id = model.get("id")?.as_str()?.to_string();
    let default_effort = model.get("defaultReasoningEffort").and_then(Value::as_str);
    let default_tier = model.get("defaultServiceTier").and_then(Value::as_str);
    Some(HarnessModel {
        label: model
            .get("displayName")
            .and_then(Value::as_str)
            .unwrap_or(&id)
            .to_string(),
        description: model
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string),
        context_window_tokens: model
            .get("contextWindow")
            .or_else(|| model.get("context_window"))
            .or_else(|| model.get("contextWindowTokens"))
            .and_then(Value::as_u64),
        is_default: model.get("isDefault").and_then(Value::as_bool) == Some(true),
        reasoning_efforts: model
            .get("supportedReasoningEfforts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|effort| {
                let effort_id = effort.get("reasoningEffort")?.as_str()?;
                Some(HarnessReasoningEffort {
                    id: effort_id.into(),
                    label: match effort_id {
                        "low" => "Low",
                        "medium" => "Medium",
                        "high" => "High",
                        "xhigh" => "Extra high",
                        "max" => "Max",
                        "ultra" => "Ultra",
                        value => value,
                    }
                    .into(),
                    description: effort
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    is_default: Some(effort_id) == default_effort,
                })
            })
            .collect(),
        service_tiers: model
            .get("serviceTiers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|tier| {
                let tier_id = tier.get("id")?.as_str()?;
                Some(HarnessServiceTier {
                    id: tier_id.into(),
                    label: tier
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or(tier_id)
                        .into(),
                    description: tier
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    is_default: Some(tier_id) == default_tier,
                })
            })
            .collect(),
        id,
    })
}

fn codex_window(id: &str, value: &Value) -> Option<AccountUsageWindow> {
    let used_percent = value
        .get("usedPercent")
        .or_else(|| value.get("used_percent"))
        .and_then(Value::as_f64)?;
    let duration_minutes = value
        .get("windowDurationMins")
        .or_else(|| value.get("window_minutes"))
        .and_then(Value::as_u64);
    let kind = match duration_minutes {
        Some(minutes) if minutes >= 7 * 24 * 60 => AccountUsageWindowKind::Weekly,
        Some(_) => AccountUsageWindowKind::Session,
        None if id == "secondary" => AccountUsageWindowKind::Weekly,
        None if id == "primary" => AccountUsageWindowKind::Session,
        None => AccountUsageWindowKind::Other,
    };
    Some(AccountUsageWindow {
        id: id.into(),
        label: None,
        kind,
        used_percent,
        duration_minutes,
        resets_at_unix_seconds: value
            .get("resetsAt")
            .or_else(|| value.get("resets_at"))
            .and_then(Value::as_u64),
    })
}

fn codex_account_usage(value: &Value) -> Option<AccountUsageSnapshot> {
    let container = value
        .get("rateLimits")
        .or_else(|| value.get("rate_limits"))
        .or_else(|| value.pointer("/result/rateLimits"))
        .or_else(|| value.pointer("/payload/rate_limits"))?;
    let mut windows = Vec::with_capacity(2);
    if let Some(window) = container
        .get("primary")
        .and_then(|item| codex_window("primary", item))
    {
        windows.push(window);
    }
    if let Some(window) = container
        .get("secondary")
        .and_then(|item| codex_window("secondary", item))
    {
        windows.push(window);
    }
    let credits = container.get("credits").and_then(|credits| {
        let unlimited = credits
            .get("unlimited")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let balance = credits.get("balance").and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| Some(value.to_string()))
        });
        (credits.is_object()).then_some(AccountCredits {
            unlimited,
            balance,
            currency: Some("USD".into()),
        })
    });
    let plan = container
        .get("planType")
        .or_else(|| container.get("plan_type"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    (!windows.is_empty() || credits.is_some() || plan.is_some()).then_some(AccountUsageSnapshot {
        provider: Provider::Codex,
        plan,
        windows,
        credits,
    })
}

#[async_trait]
impl AgentAdapter for Codex {
    fn provider(&self) -> Provider {
        Provider::Codex
    }

    fn executable(&self) -> PathBuf {
        self.configured_executable()
    }

    fn permission_support(&self) -> PermissionSupport {
        PermissionSupport {
            default: true,
            accept_edits: true,
            plan: false,
            full_access: true,
            custom: true,
            live_approvals: false,
            live_questions: false,
        }
    }

    fn launch_context_capabilities(&self) -> LaunchContextCapabilities {
        LaunchContextCapabilities {
            http_mcp: true,
            ..LaunchContextCapabilities::default()
        }
    }

    fn control_groups(&self) -> Vec<HarnessControlGroup> {
        vec![
            control_group(
                "approval_policy",
                "Approval",
                HarnessControlKind::Permission,
                &[
                    (
                        "untrusted",
                        "Review commands",
                        "Prompt for commands outside Codex's trusted set.",
                        false,
                        false,
                    ),
                    (
                        "on-request",
                        "On request",
                        "Let Codex request elevated execution when needed.",
                        true,
                        false,
                    ),
                    (
                        "never",
                        "Never ask",
                        "Never pause for approval; failures return to the agent.",
                        false,
                        true,
                    ),
                ],
            ),
            control_group(
                "sandbox_mode",
                "Sandbox",
                HarnessControlKind::Sandbox,
                &[
                    (
                        "read-only",
                        "Read only",
                        "Allow inspection without workspace writes.",
                        false,
                        false,
                    ),
                    (
                        "workspace-write",
                        "Workspace write",
                        "Allow writes inside the selected workspace.",
                        true,
                        false,
                    ),
                    (
                        "danger-full-access",
                        "Full access",
                        "Disable Codex filesystem and network isolation.",
                        false,
                        true,
                    ),
                ],
            ),
            control_group(
                "collaboration_mode",
                "Mode",
                HarnessControlKind::Collaboration,
                &[
                    (
                        "default",
                        "Work",
                        "Execute the requested task.",
                        true,
                        false,
                    ),
                    (
                        "plan",
                        "Plan",
                        "Research and return a decision-complete plan without edits.",
                        false,
                        false,
                    ),
                ],
            ),
        ]
    }

    fn catalog_probe(&self) -> Option<CatalogProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.push("app-server".into());
        command.initial_stdin = Some(
            [
                json!({"id": 1, "method": "initialize", "params": {"clientInfo": {"name": "temps-agent-runtime", "version": env!("CARGO_PKG_VERSION")}, "capabilities": {"experimentalApi": true}}}),
                json!({"method": "initialized"}),
                // Optional and intentionally sent before the required catalog
                // requests. Older app servers may reject it; model discovery
                // must still complete from response ids 2 and 3.
                json!({"id": 4, "method": "account/rateLimits/read", "params": {}}),
                json!({"id": 2, "method": "model/list", "params": {"limit": 100}}),
                json!({"id": 3, "method": "collaborationMode/list", "params": {}}),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes(),
        );
        Some(CatalogProbeSpec {
            command,
            expected_response_ids: Some(vec![2, 3]),
        })
    }

    fn parse_catalog(&self, lines: &[String]) -> Result<HarnessModelCatalog> {
        let response = response_result(lines, 2)?;
        let models = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| catalog_protocol("Codex model/list response omitted data"))?
            .iter()
            .filter(|model| model.get("hidden").and_then(Value::as_bool) != Some(true))
            .filter_map(parse_model)
            .collect();
        Ok(HarnessModelCatalog {
            status: HarnessCatalogStatus::Ready,
            source: "app_server".into(),
            models,
            error: None,
        })
    }

    fn parse_control_groups(&self, lines: &[String]) -> Result<Vec<HarnessControlGroup>> {
        let mut groups = self.control_groups();
        let response = response_result(lines, 3)?;
        let modes = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| catalog_protocol("Codex collaborationMode/list response omitted data"))?
            .iter()
            .filter_map(|mode| {
                let id = mode.get("mode")?.as_str()?;
                Some(HarnessControlOption {
                    id: id.into(),
                    label: if id == "default" {
                        "Work".into()
                    } else {
                        mode.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or(id)
                            .into()
                    },
                    description: if id == "plan" {
                        "Research and return a decision-complete plan without edits.".into()
                    } else {
                        "Execute the requested task.".into()
                    },
                    is_default: id == "default",
                    dangerous: false,
                })
            })
            .collect::<Vec<_>>();
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.id == "collaboration_mode")
        {
            if !modes.is_empty() {
                group.options = modes;
            }
        }
        Ok(groups)
    }

    fn parse_account_usage(&self, lines: &[String]) -> Result<Option<AccountUsageSnapshot>> {
        Ok(response_result(lines, 4)
            .ok()
            .and_then(|response| codex_account_usage(&response)))
    }

    fn account_usage_probe(&self) -> Option<AccountUsageProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.push("app-server".into());
        let requests = [
                json!({"id": 1, "method": "initialize", "params": {"clientInfo": {"name": "temps-agent-runtime", "version": env!("CARGO_PKG_VERSION")}, "capabilities": {"experimentalApi": true}}}),
                json!({"method": "initialized"}),
                json!({"id": 2, "method": "account/rateLimits/read", "params": {}}),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        command.initial_stdin = Some(format!("{requests}\n").into_bytes());
        Some(AccountUsageProbeSpec {
            command,
            expected_response_ids: Some(vec![2]),
        })
    }

    fn parse_account_usage_probe(&self, lines: &[String]) -> Result<AccountUsageReport> {
        Ok(response_result(lines, 2)
            .ok()
            .and_then(|response| codex_account_usage(&response))
            .map_or_else(
                || {
                    AccountUsageReport::unavailable(
                        Provider::Codex,
                        "Codex did not return account rate-limit metadata",
                        true,
                    )
                },
                AccountUsageReport::available,
            ))
    }

    fn authentication_probe(&self) -> Option<AuthenticationProbeSpec> {
        let mut command = CommandSpec::new(self.configured_executable());
        command.args.extend(["login".into(), "status".into()]);
        Some(AuthenticationProbeSpec { command })
    }

    fn parse_authentication_probe(
        &self,
        stdout: &[u8],
        stderr: &str,
        status: TransportExitStatus,
    ) -> Result<HarnessAuthentication> {
        let stdout = String::from_utf8_lossy(stdout);
        let diagnostic = format!("{}\n{}", stdout.trim(), stderr.trim());
        let normalized = diagnostic.to_ascii_lowercase();
        if normalized.contains("not logged in") || normalized.contains("login required") {
            return Ok(HarnessAuthentication::required(
                "login_status",
                "Codex is not authenticated on this execution target",
            ));
        }
        if status.success && normalized.contains("logged in") {
            return Ok(HarnessAuthentication::authenticated("login_status"));
        }
        if classify_provider_failure(&diagnostic)
            == crate::ProviderProcessErrorKind::AuthenticationFailed
        {
            return Ok(HarnessAuthentication::rejected(
                "login_status",
                "Codex credentials were rejected",
            ));
        }
        Err(RuntimeError::Protocol {
            provider: Provider::Codex,
            message: "Codex login status returned an unrecognized response".into(),
        })
    }

    async fn readiness(&self) -> ProviderReadiness {
        inspect_executable(Provider::Codex, self.resolved()).await
    }

    fn command(&self, request: &TurnRequest) -> Result<CommandSpec> {
        let mut spec = CommandSpec::new(self.configured_executable());
        spec.args.extend([
            "exec".into(),
            "--json".into(),
            "--skip-git-repo-check".into(),
        ]);
        append_http_mcp_launch_context(&mut spec, request)?;
        append_model_relay(
            &mut spec,
            request,
            self.trusted_http_model_relay_origin.as_deref(),
        )?;
        let has_native_options = ["approval_policy", "collaboration_mode", "sandbox_mode"]
            .iter()
            .any(|key| request.harness_options.contains_key(*key));
        if !has_native_options && matches!(request.permission_mode, PermissionMode::FullAccess) {
            spec.args
                .push("--dangerously-bypass-approvals-and-sandbox".into());
        } else {
            let approval = request.harness_options.get("approval_policy").map_or_else(
                || match &request.permission_mode {
                    PermissionMode::Custom(value) => value.as_str(),
                    _ => "on-request",
                },
                String::as_str,
            );
            if !["untrusted", "on-request", "never"].contains(&approval) {
                return Err(RuntimeError::InvalidRequest {
                    field: "harness_options.approval_policy",
                    message: format!("unsupported Codex approval policy `{approval}`"),
                });
            }
            let collaboration = request.harness_options.get("collaboration_mode").map_or(
                if matches!(request.permission_mode, PermissionMode::Plan) {
                    "plan"
                } else {
                    "default"
                },
                String::as_str,
            );
            if !["default", "plan"].contains(&collaboration) {
                return Err(RuntimeError::InvalidRequest {
                    field: "harness_options.collaboration_mode",
                    message: format!("unsupported Codex collaboration mode `{collaboration}`"),
                });
            }
            let sandbox = request.harness_options.get("sandbox_mode").map_or(
                if collaboration == "plan" {
                    "read-only"
                } else {
                    "workspace-write"
                },
                String::as_str,
            );
            if !["read-only", "workspace-write", "danger-full-access"].contains(&sandbox) {
                return Err(RuntimeError::InvalidRequest {
                    field: "harness_options.sandbox_mode",
                    message: format!("unsupported Codex sandbox mode `{sandbox}`"),
                });
            }
            spec.args.extend(["--sandbox".into(), sandbox.into()]);
            let approval =
                serde_json::to_string(approval).map_err(|error| RuntimeError::Protocol {
                    provider: Provider::Codex,
                    message: format!("could not encode approval policy: {error}"),
                })?;
            spec.args.extend([
                "--config".into(),
                format!("approval_policy={approval}").into(),
            ]);
            if collaboration == "plan" && request.reasoning.is_none() {
                spec.args.extend([
                    "--config".into(),
                    "model_reasoning_effort=\"medium\"".into(),
                ]);
            }
        }
        if let Some(reasoning) = request.reasoning.as_deref() {
            let reasoning =
                serde_json::to_string(reasoning).map_err(|error| RuntimeError::Protocol {
                    provider: Provider::Codex,
                    message: format!("could not encode reasoning effort: {error}"),
                })?;
            spec.args.extend([
                "--config".into(),
                format!("model_reasoning_effort={reasoning}").into(),
            ]);
        }
        if let Some(model) = request.model.as_deref() {
            spec.args.extend(["--model".into(), model.into()]);
        }
        if let Some(tier) = request.harness_options.get("service_tier") {
            let tier = serde_json::to_string(tier).map_err(|error| RuntimeError::Protocol {
                provider: Provider::Codex,
                message: format!("could not encode service tier: {error}"),
            })?;
            spec.args
                .extend(["--config".into(), format!("service_tier={tier}").into()]);
        }
        if let Some(session_id) = request.session_id.as_deref() {
            spec.args.extend(["resume".into(), session_id.into()]);
        }
        // `-` keeps prompts out of process listings and shell history.
        spec.args.push("-".into());
        spec.initial_stdin = Some(request.prompt.as_bytes().to_vec());
        Ok(spec)
    }

    fn parse_line(&self, line: &str, state: &mut AdapterState) -> Result<AdapterOutput> {
        let value: Value = serde_json::from_str(line).map_err(|error| RuntimeError::Protocol {
            provider: Provider::Codex,
            message: format!("invalid JSON event: {error}"),
        })?;
        let mut output = AdapterOutput::default();
        if let Some(usage) = codex_account_usage(&value) {
            output.events.push(TurnEvent::AccountUsageUpdated { usage });
        }
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "thread.started" => {
                state.result.session_title = value
                    .pointer("/thread/name")
                    .or_else(|| value.pointer("/thread/title"))
                    .or_else(|| value.get("name"))
                    .or_else(|| value.get("title"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|title| !title.is_empty())
                    .map(str::to_owned);
                if let Some(id) = value.get("thread_id").and_then(Value::as_str) {
                    if state.result.session_id.as_deref() != Some(id) {
                        state.result.session_id = Some(id.to_string());
                        output.events.push(TurnEvent::SessionStarted {
                            session_id: id.to_string(),
                            title: state.result.session_title.clone(),
                        });
                    }
                }
            }
            "item.started" | "item.updated" | "item.completed" => {
                let completed = value.get("type").and_then(Value::as_str) == Some("item.completed");
                let item = value.get("item").unwrap_or(&Value::Null);
                match item.get("type").and_then(Value::as_str) {
                    Some("agent_message") if completed => {
                        if let Some(text) = item.get("text").and_then(Value::as_str) {
                            state.result.text.push_str(text);
                            output.events.push(TurnEvent::TextDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("reasoning") if completed => {
                        if let Some(text) = item.get("text").and_then(Value::as_str) {
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
                    Some("command_execution" | "file_change" | "mcp_tool_call") => {
                        if let Some(event) = codex_tool_event(item, completed) {
                            output.events.push(event);
                        }
                    }
                    _ => {}
                }
            }
            "turn.completed" => {
                output.terminal = true;
                let usage = super::usage_from(&value);
                super::merge_usage(&mut state.result.usage, &usage);
                if usage != crate::Usage::default() {
                    output.events.push(TurnEvent::Usage(usage));
                }
                state.result.model = value
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or(state.result.model.take());
            }
            "turn.failed" | "error" => {
                let diagnostic = value
                    .pointer("/error/message")
                    .or_else(|| value.get("message"))
                    .or_else(|| value.get("error"))
                    .and_then(Value::as_str)
                    .unwrap_or("Codex reported an error")
                    .to_string();
                let provider_code = value
                    .pointer("/error/code")
                    .or_else(|| value.pointer("/error/type"))
                    .or_else(|| value.get("code"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let kind = classify_provider_failure(&format!(
                    "{} {diagnostic}",
                    provider_code.as_deref().unwrap_or_default()
                ));
                let mut failure =
                    ProviderTerminalFailure::new(kind, diagnostic, DeliveryState::Accepted);
                if let Some(code) = provider_code {
                    failure = failure.with_provider_code(format!("codex::{code}"));
                }
                state.terminal_failure = Some(failure);
                state.result.status = crate::RunStatus::Failed;
                output.terminal = true;
            }
            _ => {}
        }
        Ok(output)
    }

    fn approval_response(
        &self,
        _request: &ApprovalRequest,
        _original: &Value,
        _decision: ApprovalDecision,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    fn question_response(
        &self,
        _request: &QuestionRequest,
        _original: &Value,
        _answer: Option<QuestionAnswer>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_config_does_not_reenable_nested_sandboxing() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request.permission_mode = PermissionMode::FullAccess;
        request.environment.insert(
            "RELAY_TOKEN".into(),
            crate::SecretString::new("test-secret"),
        );
        request.harness_options.insert(
            "model_relay".into(),
            r#"{"base_url":"http://127.0.0.1:8000/v1","token_env":"RELAY_TOKEN"}"#.into(),
        );
        let command = Codex::default().command(&request).unwrap();
        let args: Vec<_> = command
            .args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect();
        assert!(args
            .iter()
            .any(|arg| arg == "--dangerously-bypass-approvals-and-sandbox"));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--config", "model_provider=\"sdk_relay\""]));
        assert!(!format!("{args:?}").contains("test-secret"));
    }

    #[test]
    fn model_relay_rejects_credentials_missing_environment_and_arbitrary_keys() {
        for relay in [
            r#"{"base_url":"https://user:pass@example.test","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"https://example.test/?token=secret","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"https://example.test/#secret","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"http://example.test/v1","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"http://localhost.example.test/v1","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"http://127.0.0.1.example.test/v1","token_env":"RELAY_TOKEN"}"#,
            r#"{"base_url":"https://example.test","token_env":"MISSING"}"#,
            r#"{"base_url":"https://example.test","token_env":"RELAY_TOKEN","auth_command":"bad"}"#,
        ] {
            let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
            request
                .environment
                .insert("RELAY_TOKEN".into(), crate::SecretString::new("secret"));
            request
                .harness_options
                .insert("model_relay".into(), relay.into());
            assert!(matches!(
                Codex::default().command(&request),
                Err(RuntimeError::InvalidRequest {
                    field: "harness_options.model_relay",
                    ..
                })
            ));
        }
    }

    #[test]
    fn model_relay_allows_https_and_loopback_http() {
        for base_url in [
            "https://relay.example.test/v1",
            "http://localhost:8000/v1",
            "http://runtime.localhost:8000/v1",
            "http://127.0.0.1:8000/v1",
            "http://[::1]:8000/v1",
        ] {
            let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
            request.environment.insert(
                "RELAY_TOKEN".into(),
                crate::SecretString::new("test-secret"),
            );
            request.harness_options.insert(
                "model_relay".into(),
                serde_json::json!({"base_url": base_url, "token_env": "RELAY_TOKEN"}).to_string(),
            );
            let command = Codex::default().command(&request).unwrap();
            assert!(
                command.args.iter().any(|arg| {
                    arg.to_string_lossy()
                        .contains("model_providers.sdk_relay.base_url=")
                }),
                "relay URL {base_url} was not configured"
            );
            assert!(!format!("{:?}", command.args).contains("test-secret"));
        }
    }

    #[test]
    fn trusted_plaintext_relay_is_exact_origin_and_not_request_controlled() {
        let adapter = Codex::default()
            .with_insecure_model_relay_origin("http://isolated-relay.test:3128")
            .unwrap();
        for (base_url, allowed) in [
            ("http://isolated-relay.test:3128/.internal/relay", true),
            ("http://isolated-relay.test:3129/.internal/relay", false),
            ("http://other-relay.test:3128/.internal/relay", false),
            (
                "http://isolated-relay.test.evil:3128/.internal/relay",
                false,
            ),
        ] {
            let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
            request
                .environment
                .insert("RELAY_TOKEN".into(), crate::SecretString::new("secret"));
            request.harness_options.insert(
                "model_relay".into(),
                serde_json::json!({
                    "base_url": base_url, "token_env": "RELAY_TOKEN"
                })
                .to_string(),
            );
            assert_eq!(adapter.command(&request).is_ok(), allowed, "{base_url}");
            if allowed {
                assert!(Codex::default().command(&request).is_err());
            }
            request.harness_options.insert(
                "trusted_http_model_relay_origin".into(),
                "http://other-relay.test:3128".into(),
            );
            assert_eq!(
                adapter.command(&request).is_ok(),
                allowed,
                "request option affected trust: {base_url}"
            );
        }
        for origin in [
            "https://isolated-relay.test:3128",
            "http://isolated-relay.test",
            "http://isolated-relay.test:3128/path",
            "http://isolated-relay.test:3128?x=1",
            "http://user:pass@isolated-relay.test:3128",
            "http://isolated-relay.test:3128/",
        ] {
            assert!(
                Codex::default()
                    .with_insecure_model_relay_origin(origin)
                    .is_err(),
                "{origin}"
            );
        }
    }

    #[test]
    fn rejects_invalid_or_unbounded_config_overrides() {
        for invalid in [
            "{}".to_string(),
            r#"["no-assignment"]"#.into(),
            serde_json::to_string(&vec!["key=1"; 65]).unwrap(),
        ] {
            let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
            request
                .harness_options
                .insert("config_overrides".into(), invalid);
            assert!(matches!(
                Codex::default().command(&request),
                Err(RuntimeError::InvalidRequest {
                    field: "harness_options.config_overrides",
                    ..
                })
            ));
        }
    }

    #[test]
    fn keeps_plan_separate_from_approval_and_sandbox() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request
            .harness_options
            .insert("approval_policy".into(), "on-request".into());
        request
            .harness_options
            .insert("sandbox_mode".into(), "workspace-write".into());
        request
            .harness_options
            .insert("collaboration_mode".into(), "plan".into());
        request
            .harness_options
            .insert("service_tier".into(), "priority".into());
        let command = Codex::default().command(&request).unwrap();
        let args = command
            .args
            .iter()
            .map(|argument| argument.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--sandbox", "workspace-write"]));
        assert!(args
            .iter()
            .any(|argument| argument == "approval_policy=\"on-request\""));
        assert!(args
            .iter()
            .any(|argument| argument == "service_tier=\"priority\""));
        assert!(args
            .iter()
            .any(|argument| argument == "model_reasoning_effort=\"medium\""));
    }

    #[test]
    fn applies_turn_scoped_http_mcp_without_serializing_secret_values() {
        let mut request = TurnRequest::new(Provider::Codex, ".", "inspect");
        request.environment.insert(
            "TURN_MCP_TOKEN".into(),
            crate::SecretString::new("super-secret-token"),
        );
        request.launch_context.mcp_servers.insert(
            "platform".into(),
            McpServerConfig::Http {
                url: "https://relay.example.test/mcp".into(),
                headers_from: std::collections::BTreeMap::from([(
                    "Authorization".into(),
                    "TURN_MCP_TOKEN".into(),
                )]),
            },
        );

        let command = Codex::default().command(&request).unwrap();
        let arguments = command
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert!(arguments
            .iter()
            .any(|argument| argument
                == "mcp_servers.platform.url=\"https://relay.example.test/mcp\""));
        assert!(arguments.iter().any(|argument| {
            argument == "mcp_servers.platform.env_http_headers.Authorization=\"TURN_MCP_TOKEN\""
        }));
        assert!(!arguments.join(" ").contains("super-secret-token"));
        assert!(!format!("{request:?}").contains("super-secret-token"));
    }

    #[test]
    fn parses_model_specific_reasoning_and_fast_tier() {
        let line = r#"{"id":2,"result":{"data":[{"id":"gpt-test","displayName":"GPT Test","description":"Test model","hidden":false,"supportedReasoningEfforts":[{"reasoningEffort":"low","description":"Fast"},{"reasoningEffort":"high","description":"Deep"}],"defaultReasoningEffort":"low","serviceTiers":[{"id":"priority","name":"Fast","description":"Quicker"}],"defaultServiceTier":null,"isDefault":true}]}}"#;
        let catalog = Codex::default().parse_catalog(&[line.into()]).unwrap();
        assert_eq!(catalog.status, HarnessCatalogStatus::Ready);
        assert_eq!(catalog.models[0].id, "gpt-test");
        assert!(catalog.models[0].reasoning_efforts[0].is_default);
        assert_eq!(catalog.models[0].reasoning_efforts[0].label, "Low");
        assert_eq!(catalog.models[0].service_tiers[0].id, "priority");
    }

    #[test]
    fn gives_the_native_default_collaboration_mode_a_descriptive_label() {
        let line = r#"{"id":3,"result":{"data":[{"mode":"default","name":"Default"},{"mode":"plan","name":"Plan"}]}}"#;
        let groups = Codex::default()
            .parse_control_groups(&[line.into()])
            .unwrap();
        let modes = &groups
            .iter()
            .find(|group| group.id == "collaboration_mode")
            .unwrap()
            .options;

        assert_eq!(modes[0].id, "default");
        assert_eq!(modes[0].label, "Work");
        assert!(modes.iter().all(|mode| mode.label != "Default"));
    }

    #[test]
    fn parses_agent_message_and_usage() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let text = r#"{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#;
        let usage = r#"{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":4}}"#;
        assert_eq!(
            adapter.parse_line(text, &mut state).unwrap().events,
            vec![TurnEvent::TextDelta {
                text: "done".into()
            }]
        );
        assert!(adapter.parse_line(usage, &mut state).unwrap().terminal);
        assert_eq!(state.result.usage.input_tokens, Some(10));
    }

    #[test]
    fn mcp_tool_lifecycle_preserves_server_function_arguments_and_structured_result() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let item = json!({
            "type": "mcp_tool_call", "id": "call-1", "server": "temps-chat",
            "tool": "temps__process_start", "arguments": {"project_id": 7, "command": "npm test"},
            "status": "in_progress"
        });
        for lifecycle in ["item.started", "item.updated"] {
            let event = adapter
                .parse_line(
                    &json!({"type": lifecycle, "item": item.clone()}).to_string(),
                    &mut state,
                )
                .unwrap();
            assert_eq!(event.events.len(), 1);
            assert!(matches!(&event.events[0], TurnEvent::ToolCall {
                id: Some(id), name, status: ToolCallStatus::Started,
                input: Some(input), ..
            } if id == "call-1" && name == "mcp__temps-chat__temps__process_start"
                && input == &json!({"project_id": 7, "command": "npm test"})));
        }
        let completed = json!({"type": "item.completed", "item": {
            "type": "mcp_tool_call", "id": "call-1", "server": "temps-chat",
            "tool": "temps__process_start", "arguments": {"project_id": 7, "command": "npm test"},
            "status": "completed", "result": {
                "content": [{"type": "text", "text": "started"}],
                "structured_content": {"process_id": "p-1"}
            }
        }});
        let event = adapter
            .parse_line(&completed.to_string(), &mut state)
            .unwrap();
        assert!(matches!(&event.events[0], TurnEvent::ToolCall {
            id: Some(id), name, status: ToolCallStatus::Succeeded,
            input: Some(input), output: Some(output), error: None, ..
        } if id == "call-1" && name == "mcp__temps-chat__temps__process_start"
            && input == &json!({"project_id": 7, "command": "npm test"})
            && serde_json::from_str::<Value>(output).unwrap()["structured_content"]["process_id"] == "p-1"));
    }

    #[test]
    fn mcp_tool_failure_preserves_provider_error() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let event = adapter.parse_line(&json!({"type": "item.completed", "item": {
            "type": "mcp_tool_call", "id": "call-2", "server": "workspace", "tool": "list_projects",
            "arguments": {"owner": "david"}, "status": "failed",
            "error": {"message": "permission denied by workspace"}
        }}).to_string(), &mut state).unwrap();
        assert!(matches!(&event.events[0], TurnEvent::ToolCall {
            name, status: ToolCallStatus::Failed, input: Some(input), error: Some(error), ..
        } if name == "mcp__workspace__list_projects"
            && input == &json!({"owner": "david"})
            && error == "permission denied by workspace"));
    }

    #[test]
    fn command_execution_preserves_command_output_and_exit_failure() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let started = adapter.parse_line(&json!({"type": "item.started", "item": {
            "type": "command_execution", "id": "cmd-1", "command": "pwd", "status": "in_progress",
            "aggregated_output": ""
        }}).to_string(), &mut state).unwrap();
        assert!(matches!(&started.events[0], TurnEvent::ToolCall {
            id: Some(id), status: ToolCallStatus::Started, input: Some(input), ..
        } if id == "cmd-1" && input == &json!({"command": "pwd"})));
        let completed = adapter.parse_line(&json!({"type": "item.completed", "item": {
            "type": "command_execution", "id": "cmd-1", "command": "pwd", "status": "failed",
            "aggregated_output": "bwrap: No permissions to create namespace", "exit_code": 1
        }}).to_string(), &mut state).unwrap();
        assert!(matches!(&completed.events[0], TurnEvent::ToolCall {
            id: Some(id), status: ToolCallStatus::Failed,
            input: Some(input), output: Some(output), error: Some(error), ..
        } if id == "cmd-1" && input == &json!({"command": "pwd"})
            && output.contains("bwrap: No permissions")
            && error.contains("command exited with code 1")
            && error.contains("bwrap: No permissions")));
    }

    #[test]
    fn retains_native_terminal_failure_without_emitting_unredacted_warning() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"turn.failed","error":{"message":"usage limit reached for token secret","code":"usage_limit"}}"#,
                &mut state,
            )
            .unwrap();

        assert!(output.terminal);
        assert!(output.events.is_empty());
        let failure = state.terminal_failure.unwrap();
        assert_eq!(failure.kind, crate::ProviderProcessErrorKind::RateLimited);
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(failure.provider_code.as_deref(), Some("codex::usage_limit"));
        assert_eq!(failure.diagnostic, "usage limit reached for token secret");
    }

    #[test]
    fn parses_codex_account_usage_windows_and_resets() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"token_count","rate_limits":{"plan_type":"pro","primary":{"used_percent":63.0,"window_minutes":300,"resets_at":1780000000},"secondary":{"used_percent":41.0,"window_minutes":10080,"resets_at":1780500000},"credits":{"unlimited":false,"balance":"12.50"}}}"#,
                &mut state,
            )
            .unwrap();

        let TurnEvent::AccountUsageUpdated { usage } = &output.events[0] else {
            panic!("expected account usage event");
        };
        assert_eq!(usage.plan.as_deref(), Some("pro"));
        assert_eq!(usage.windows[0].kind, AccountUsageWindowKind::Session);
        assert!((usage.windows[0].used_percent - 63.0).abs() < f64::EPSILON * 100.0);
        assert_eq!(usage.windows[1].kind, AccountUsageWindowKind::Weekly);
        assert_eq!(usage.windows[1].resets_at_unix_seconds, Some(1_780_500_000));
        assert_eq!(
            usage
                .credits
                .as_ref()
                .and_then(|credits| credits.balance.as_deref()),
            Some("12.50")
        );
    }

    #[test]
    fn parses_account_usage_from_app_server_catalog_probe() {
        let lines = vec![r#"{"id":4,"result":{"rateLimits":{"primary":{"usedPercent":12,"windowDurationMins":300,"resetsAt":1780000000},"secondary":{"usedPercent":34,"windowDurationMins":10080,"resetsAt":1780500000}}}}"#.into()];
        let usage = Codex::default()
            .parse_account_usage(&lines)
            .unwrap()
            .unwrap();
        assert_eq!(usage.windows.len(), 2);
        assert_eq!(usage.windows[1].kind, AccountUsageWindowKind::Weekly);
    }

    #[test]
    fn builds_and_parses_an_explicit_account_usage_probe() {
        let adapter = Codex::default();
        let probe = adapter.account_usage_probe().unwrap();
        assert_eq!(probe.command.program, PathBuf::from("codex"));
        let request = String::from_utf8(probe.command.initial_stdin.unwrap()).unwrap();
        assert!(request.contains("account/rateLimits/read"));

        let report = adapter
            .parse_account_usage_probe(&[r#"{"id":2,"result":{"rateLimits":{"planType":"pro","primary":{"usedPercent":84,"windowDurationMins":300,"resetsAt":1780000000},"credits":{"balance":"0.00","unlimited":false}}}}"#.into()])
            .unwrap();

        assert_eq!(report.status, crate::AccountUsageStatus::Available);
        let usage = report.usage.unwrap();
        assert_eq!(usage.plan.as_deref(), Some("pro"));
        assert!((usage.windows[0].used_percent - 84.0).abs() < f64::EPSILON * 100.0);
        assert_eq!(usage.credits.unwrap().balance.as_deref(), Some("0.00"));
    }

    #[test]
    fn parses_provider_native_login_status() {
        let adapter = Codex::default();
        let authenticated = adapter
            .parse_authentication_probe(
                b"Logged in using ChatGPT\n",
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
                b"",
                "Not logged in",
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

    #[test]
    fn parses_the_harness_session_title() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        let output = adapter
            .parse_line(
                r#"{"type":"thread.started","thread_id":"thread-123","thread":{"title":"Fix the SSH inventory"}}"#,
                &mut state,
            )
            .unwrap();

        assert_eq!(
            state.result.session_title.as_deref(),
            Some("Fix the SSH inventory")
        );
        assert_eq!(
            output.events,
            vec![TurnEvent::SessionStarted {
                session_id: "thread-123".into(),
                title: Some("Fix the SSH inventory".into()),
            }]
        );
    }

    #[test]
    fn repeated_resumed_thread_id_is_not_started_again() {
        let adapter = Codex::default();
        let mut state = AdapterState::default();
        state.result.session_id = Some("thread-123".into());

        let output = adapter
            .parse_line(
                r#"{"type":"thread.started","thread_id":"thread-123"}"#,
                &mut state,
            )
            .unwrap();

        assert!(output.events.is_empty());
        assert_eq!(state.result.session_id.as_deref(), Some("thread-123"));
    }

    #[test]
    fn resumes_after_exec_level_flags() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("codex");
        std::fs::write(&executable, "stub").unwrap();
        let mut request = TurnRequest::new(Provider::Codex, temp.path(), "continue");
        request.session_id = Some("thread-123".into());
        let command = Codex::with_executable(executable)
            .command(&request)
            .unwrap();
        let args = command
            .args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>();
        let resume = args.iter().position(|arg| arg == "resume").unwrap();
        assert!(args[..resume].contains(&std::borrow::Cow::Borrowed("--json")));
        assert_eq!(args[resume + 1], "thread-123");
        assert_eq!(args[resume + 2], "-");
        assert_eq!(
            command.initial_stdin.as_deref(),
            Some(b"continue".as_slice())
        );
    }
}
