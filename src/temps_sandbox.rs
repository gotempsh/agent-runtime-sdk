//! Temps sandbox HTTP execution transport.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use reqwest::{Client, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio::sync::watch;

use crate::{
    ExecutionTransport, ProviderReadiness, SandboxCapabilities, SecretString,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter,
    WorkingDirectoryCandidates, WorkingDirectoryQuery,
};

static NEXT_INPUT_ID: AtomicU64 = AtomicU64::new(1);
const CLEAR_ENV_PREFIX: &str = "${PATH+PATH=\"$PATH\"} ${HOME+HOME=\"$HOME\"} ${USER+USER=\"$USER\"} ${LOGNAME+LOGNAME=\"$LOGNAME\"} ${SHELL+SHELL=\"$SHELL\"} ${LANG+LANG=\"$LANG\"} ${LC_ALL+LC_ALL=\"$LC_ALL\"} ${LC_CTYPE+LC_CTYPE=\"$LC_CTYPE\"} ${TERM+TERM=\"$TERM\"} ${NO_COLOR+NO_COLOR=\"$NO_COLOR\"} ${TMPDIR+TMPDIR=\"$TMPDIR\"} ${CLAUDE_HOME+CLAUDE_HOME=\"$CLAUDE_HOME\"} ${CODEX_HOME+CODEX_HOME=\"$CODEX_HOME\"} ${SSL_CERT_FILE+SSL_CERT_FILE=\"$SSL_CERT_FILE\"} ${SSL_CERT_DIR+SSL_CERT_DIR=\"$SSL_CERT_DIR\"}";

/// Authentication attached to every Temps sandbox API request.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TempsSandboxAuth {
    /// API key sent as a bearer token.
    Bearer(SecretString),
    /// Existing browser/session cookie value, intended mainly for local development.
    SessionCookie(SecretString),
}

/// Builder for [`TempsSandboxTransport`].
#[derive(Clone, Debug)]
pub struct TempsSandboxTransportBuilder {
    base_url: String,
    sandbox_id: String,
    auth: TempsSandboxAuth,
    request_timeout: Duration,
    poll_interval: Duration,
    intrinsic_sandbox: SandboxCapabilities,
    allow_insecure_http: bool,
}

impl TempsSandboxTransportBuilder {
    /// Create a builder for an existing running Temps sandbox.
    pub fn new(
        base_url: impl Into<String>,
        sandbox_id: impl Into<String>,
        auth: TempsSandboxAuth,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            sandbox_id: sandbox_id.into(),
            auth,
            request_timeout: Duration::from_secs(30),
            poll_interval: Duration::from_millis(100),
            intrinsic_sandbox: SandboxCapabilities {
                filesystem: true,
                process_isolation: true,
                ..SandboxCapabilities::NONE
            },
            allow_insecure_http: false,
        }
    }

    /// Per-request API deadline.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Process-output polling interval.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Override isolation controls guaranteed by the selected sandbox profile.
    pub fn intrinsic_sandbox(mut self, capabilities: SandboxCapabilities) -> Self {
        self.intrinsic_sandbox = capabilities;
        self
    }

    /// Permit cleartext HTTP for a non-loopback endpoint.
    ///
    /// This should be enabled only when another authenticated encrypted network boundary, such as
    /// a private WireGuard/Tailscale network, protects the bearer token and sandbox traffic.
    pub fn allow_insecure_http(mut self, allow: bool) -> Self {
        self.allow_insecure_http = allow;
        self
    }

    /// Validate configuration and construct the transport.
    pub fn build(self) -> TransportResult<TempsSandboxTransport> {
        let mut base_url = self.base_url.trim_end_matches('/').to_string();
        let parsed = crate::url_security::validate_http_endpoint(&base_url).map_err(|message| {
            TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "temps_sandbox",
                "configure",
                format!("invalid Temps API base URL: {message}"),
                false,
            )
        })?;
        if parsed.query().is_some() {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "temps_sandbox",
                "configure",
                "Temps API base URL must not contain a query string",
                false,
            ));
        }
        if parsed.scheme() == "http"
            && !crate::url_security::is_loopback_endpoint(&parsed)
            && !self.allow_insecure_http
        {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "temps_sandbox",
                "configure",
                "cleartext HTTP is allowed only for loopback endpoints unless allow_insecure_http(true) is explicitly configured",
                false,
            ));
        }
        if self.sandbox_id.is_empty()
            || !self.sandbox_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || character == '_' || character == '-'
            })
        {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "temps_sandbox",
                "configure",
                "sandbox_id contains unsupported characters",
                false,
            ));
        }
        if self.request_timeout.is_zero() || self.poll_interval.is_zero() {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "temps_sandbox",
                "configure",
                "request_timeout and poll_interval must be greater than zero",
                false,
            ));
        }
        if !base_url.ends_with("/api") {
            base_url.push_str("/api");
        }
        let client = Client::builder()
            .timeout(self.request_timeout)
            .build()
            .map_err(|error| http_client_error("configure", error))?;
        Ok(TempsSandboxTransport {
            inner: Arc::new(TempsSandboxInner {
                client,
                base_url,
                sandbox_id: self.sandbox_id,
                auth: self.auth,
                poll_interval: self.poll_interval,
                intrinsic_sandbox: self.intrinsic_sandbox,
            }),
        })
    }
}

/// Execute commands through an existing Temps sandbox's HTTP API.
#[derive(Clone, Debug)]
pub struct TempsSandboxTransport {
    inner: Arc<TempsSandboxInner>,
}

impl TempsSandboxTransport {
    /// Create a builder for an existing running sandbox.
    pub fn builder(
        base_url: impl Into<String>,
        sandbox_id: impl Into<String>,
        auth: TempsSandboxAuth,
    ) -> TempsSandboxTransportBuilder {
        TempsSandboxTransportBuilder::new(base_url, sandbox_id, auth)
    }

    async fn exec(
        &self,
        command: Vec<String>,
        cwd: Option<String>,
    ) -> TransportResult<ExecResponse> {
        self.inner
            .json(
                Method::POST,
                &format!("/v1/sandboxes/{}/exec", self.inner.sandbox_id),
                Some(&ExecBody {
                    cmd: command,
                    env: BTreeMap::new(),
                    cwd,
                }),
                "exec",
            )
            .await
    }

    async fn start_process(
        &self,
        request: TransportSpawnRequest,
    ) -> TransportResult<TransportProcess> {
        let prepared = self.prepare_command(request).await?;
        let response: DetachedResponse = self
            .inner
            .json(
                Method::POST,
                &format!("/v1/sandboxes/{}/exec-detached", self.inner.sandbox_id),
                Some(&prepared),
                "spawn",
            )
            .await?;
        Ok(self.process_for_job(response.job_id, 0, 0))
    }

    async fn prepare_command(&self, request: TransportSpawnRequest) -> TransportResult<ExecBody> {
        let directory = path_to_utf8(&request.working_directory, "working directory")?.to_string();
        let program = path_to_utf8(&request.command.program, "program")?.to_string();
        let arguments = request
            .command
            .args
            .iter()
            .map(|value| os_to_utf8(value, "argument").map(str::to_string))
            .collect::<TransportResult<Vec<_>>>()?;
        let environment = environment_to_strings(&request.command.environment)?;

        let mut cmd = Vec::new();
        if let Some(initial) = request.command.initial_stdin {
            // Stage stdin beside the requested working directory. Temps owns files
            // under its workspace as the sandbox runtime user; `/tmp` uploads can
            // otherwise remain owned by the host-side writer and make a secure
            // `0600` prompt unreadable to the process we are about to spawn.
            let input_path = stdin_staging_path(
                &directory,
                std::process::id(),
                NEXT_INPUT_ID.fetch_add(1, Ordering::Relaxed),
            );
            let mut contents = initial;
            contents.push(b'\n');
            self.inner
                .json::<_, serde_json::Value>(
                    Method::POST,
                    &format!("/v1/sandboxes/{}/fs/write", self.inner.sandbox_id),
                    Some(&WriteFileBody {
                        path: input_path.clone(),
                        contents_b64: base64::engine::general_purpose::STANDARD.encode(contents),
                        mode: 0o600,
                    }),
                    "stage_stdin",
                )
                .await?;
            cmd.extend([
                "/bin/sh".to_string(),
                "-c".to_string(),
                if request.command.clear_environment {
                    format!(
                        "exec 3<\"$0\"; rm -f -- \"$0\"; shift; exec env -i {CLEAR_ENV_PREFIX} \"$@\" <&3"
                    )
                } else {
                    "exec 3<\"$0\"; rm -f -- \"$0\"; shift; exec \"$@\" <&3".to_string()
                },
                input_path,
                "temps-agent-runtime".to_string(),
            ]);
            if request.command.clear_environment {
                cmd.extend(
                    environment
                        .iter()
                        .map(|(name, value)| format!("{name}={value}")),
                );
            }
            cmd.push(program);
            cmd.extend(arguments);
        } else if request.command.clear_environment {
            cmd.extend([
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("shift; exec env -i {CLEAR_ENV_PREFIX} \"$@\""),
                "temps-agent-runtime".to_string(),
                "temps-agent-runtime".to_string(),
            ]);
            cmd.extend(
                environment
                    .iter()
                    .map(|(name, value)| format!("{name}={value}")),
            );
            cmd.push(program);
            cmd.extend(arguments);
        } else {
            cmd.push(program);
            cmd.extend(arguments);
        }
        Ok(ExecBody {
            cmd,
            env: if request.command.clear_environment {
                BTreeMap::new()
            } else {
                environment
            },
            cwd: Some(directory),
        })
    }

    fn process_for_job(
        &self,
        job_id: String,
        stdout_cursor: usize,
        stderr_cursor: usize,
    ) -> TransportProcess {
        let (sdk_stdout, remote_stdout) = tokio::io::duplex(64 * 1024);
        let (sdk_stderr, remote_stderr) = tokio::io::duplex(64 * 1024);
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let terminated = Arc::new(AtomicBool::new(false));
        tokio::spawn(monitor_job(
            Arc::clone(&self.inner),
            job_id.clone(),
            remote_stdout,
            remote_stderr,
            stdout_cursor,
            stderr_cursor,
            terminal_tx.clone(),
            Arc::clone(&terminated),
        ));
        TransportProcess::new(
            TransportProcessHandle {
                transport: self.name().to_string(),
                native_id: job_id.clone(),
            },
            None,
            Some(Box::new(tokio::io::sink()) as TransportWriter),
            Box::new(sdk_stdout) as TransportReader,
            Box::new(sdk_stderr) as TransportReader,
            TempsProcessControl {
                inner: Arc::clone(&self.inner),
                job_id,
                terminal_rx,
                terminal_tx,
                terminated,
                finished: false,
            },
        )
    }
}

#[async_trait]
impl ExecutionTransport for TempsSandboxTransport {
    fn name(&self) -> &'static str {
        "temps_sandbox"
    }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: true,
            interactive_stdin: false,
            reconnect: true,
            managed_processes: true,
            process_tree_termination: true,
            sandbox: self.inner.intrinsic_sandbox,
        }
    }

    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        let program = path_to_utf8(&request.program, "provider executable")?.to_string();
        let response = self
            .exec(
                vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "if command -v -- \"$1\" >/dev/null 2>&1; then \"$1\" --version; else exit 127; fi"
                        .to_string(),
                    "temps-agent-runtime".to_string(),
                    program,
                ],
                None,
            )
            .await?;
        if response.exit_code == 127 {
            return Ok(ProviderReadiness {
                provider: request.provider,
                installed: false,
                executable: None,
                version: None,
                detail: format!(
                    "Install {} inside sandbox {} and authenticate it there.",
                    request.provider, self.inner.sandbox_id
                ),
            });
        }
        if response.exit_code != 0 {
            return Err(TransportError::new(
                TransportErrorKind::RemoteUnavailable,
                self.name(),
                "readiness",
                bounded(&response.stderr),
                false,
            ));
        }
        let version = response.stdout.trim().to_string();
        Ok(ProviderReadiness {
            provider: request.provider,
            installed: true,
            executable: Some(request.program),
            version: (!version.is_empty()).then_some(version),
            detail: "The CLI executable is available inside the Temps sandbox; authentication is checked when a turn starts.".to_string(),
        })
    }

    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()> {
        let directory = path_to_utf8(working_directory, "working directory")?.to_string();
        let response = self
            .exec(
                vec!["test".to_string(), "-d".to_string(), directory.clone()],
                None,
            )
            .await?;
        if response.exit_code == 0 {
            Ok(())
        } else {
            Err(TransportError::new(
                TransportErrorKind::WorkingDirectoryNotFound,
                self.name(),
                "validate_working_directory",
                format!(
                    "{directory} is not a directory in sandbox {}",
                    self.inner.sandbox_id
                ),
                false,
            ))
        }
    }

    async fn suggest_working_directories(
        &self,
        query: WorkingDirectoryQuery,
    ) -> TransportResult<WorkingDirectoryCandidates> {
        let script = "query=$1; limit=$2; \
            home=${HOME:-/}; \
            case \"$query\" in \
              ''|'~') expanded=$home ;; \
              '~/'*) expanded=$home/${query#~/} ;; \
              /*) expanded=$query ;; \
              *) expanded=$home/$query ;; \
            esac; \
            if test -d \"$expanded\"; then \
              exact=1; parent=${expanded%/}; prefix=; \
            else \
              exact=0; \
              case \"$expanded\" in \
                */) parent=${expanded%/}; prefix= ;; \
                *) parent=${expanded%/*}; prefix=${expanded##*/} ;; \
              esac; \
            fi; \
            test -n \"$parent\" || parent=/; \
            printf '\\0TEMPS_AGENT_RUNTIME_DIRECTORIES\\0%s\\0%s\\0' \"$home\" \"$exact\"; \
            count=0; \
            if test \"$exact\" = 1 && test \"$count\" -lt \"$limit\"; then \
              printf '%s\\0' \"$expanded\"; count=$((count + 1)); \
            fi; \
            if test -d \"$parent\" && test \"$count\" -lt \"$limit\"; then \
              for candidate in \"$parent\"/\"$prefix\"*; do \
                test -d \"$candidate\" || continue; \
                printf '%s\\0' \"$candidate\"; count=$((count + 1)); \
                test \"$count\" -lt \"$limit\" || break; \
              done; \
            fi";
        let response = self
            .exec(
                vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    script.to_string(),
                    "runtime-directories".to_string(),
                    query.input,
                    query.limit.to_string(),
                ],
                None,
            )
            .await?;
        if response.exit_code != 0 {
            return Err(TransportError::new(
                TransportErrorKind::RemoteUnavailable,
                self.name(),
                "suggest_working_directories",
                bounded(&response.stderr),
                true,
            ));
        }
        parse_directory_candidates(&response.stdout, query.limit)
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        if request.command.interactive_stdin {
            return Err(TransportError::new(
                TransportErrorKind::Unsupported,
                self.name(),
                "spawn",
                "the Temps HTTP command API does not support interactive stdin; use an SSH or terminal-WebSocket transport for interactive approvals",
                false,
            ));
        }
        self.start_process(request).await
    }

    async fn attach(
        &self,
        handle: &TransportProcessHandle,
        cursor: Option<u64>,
    ) -> TransportResult<TransportProcess> {
        if handle.transport != self.name() {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                self.name(),
                "attach",
                format!("handle belongs to {}", handle.transport),
                false,
            ));
        }
        let cursor = usize::try_from(cursor.unwrap_or(0)).map_err(|_| {
            TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                self.name(),
                "attach",
                "cursor exceeds this platform's addressable range",
                false,
            )
        })?;
        Ok(self.process_for_job(handle.native_id.clone(), cursor, cursor))
    }
}

#[derive(Debug)]
struct TempsSandboxInner {
    client: Client,
    base_url: String,
    sandbox_id: String,
    auth: TempsSandboxAuth,
    poll_interval: Duration,
    intrinsic_sandbox: SandboxCapabilities,
}

impl TempsSandboxInner {
    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let request = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        match &self.auth {
            TempsSandboxAuth::Bearer(token) => request.bearer_auth(token.expose()),
            TempsSandboxAuth::SessionCookie(cookie) => request.header("cookie", cookie.expose()),
        }
    }

    async fn json<B: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        operation: &'static str,
    ) -> TransportResult<R> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| http_client_error(operation, error))?;
        decode_response(response, operation).await
    }

    async fn kill_job(&self, job_id: &str) -> TransportResult<()> {
        let _: serde_json::Value = self
            .json(
                Method::POST,
                &format!("/v1/sandboxes/{}/jobs/{job_id}/kill", self.sandbox_id),
                Some(&KillBody { force: false }),
                "terminate",
            )
            .await?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct ExecBody {
    cmd: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecResponse {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Deserialize)]
struct DetachedResponse {
    job_id: String,
}

#[derive(Debug, Deserialize)]
struct JobResponse {
    status: String,
    exit_code: Option<i32>,
    reason: Option<String>,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Serialize)]
struct WriteFileBody {
    path: String,
    contents_b64: String,
    mode: u32,
}

#[derive(Debug, Serialize)]
struct KillBody {
    force: bool,
}

struct TempsProcessControl {
    inner: Arc<TempsSandboxInner>,
    job_id: String,
    terminal_rx: watch::Receiver<Option<TransportResult<TransportExitStatus>>>,
    terminal_tx: watch::Sender<Option<TransportResult<TransportExitStatus>>>,
    terminated: Arc<AtomicBool>,
    finished: bool,
}

#[async_trait]
impl TransportProcessControl for TempsProcessControl {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        loop {
            if let Some(result) = self.terminal_rx.borrow().clone() {
                self.finished = true;
                return result;
            }
            self.terminal_rx.changed().await.map_err(|_| {
                TransportError::new(
                    TransportErrorKind::ProcessControlFailed,
                    "temps_sandbox",
                    "wait",
                    "sandbox job monitor closed before reporting terminal state",
                    true,
                )
            })?;
        }
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        self.terminated.store(true, Ordering::Release);
        self.inner.kill_job(&self.job_id).await?;
        self.terminal_tx.send_replace(Some(Ok(TransportExitStatus {
            success: false,
            code: None,
        })));
        self.finished = true;
        Ok(())
    }

    fn disarm(&mut self) {
        self.finished = true;
    }
}

impl Drop for TempsProcessControl {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.terminated.store(true, Ordering::Release);
        let inner = Arc::clone(&self.inner);
        let job_id = self.job_id.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = inner.kill_job(&job_id).await;
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn monitor_job(
    inner: Arc<TempsSandboxInner>,
    job_id: String,
    mut stdout: DuplexStream,
    mut stderr: DuplexStream,
    mut stdout_cursor: usize,
    mut stderr_cursor: usize,
    terminal: watch::Sender<Option<TransportResult<TransportExitStatus>>>,
    terminated: Arc<AtomicBool>,
) {
    loop {
        let result: TransportResult<JobResponse> = inner
            .json::<serde_json::Value, _>(
                Method::GET,
                &format!("/v1/sandboxes/{}/jobs/{job_id}", inner.sandbox_id),
                None,
                "poll",
            )
            .await;
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if terminated.load(Ordering::Acquire) {
                    terminal.send_replace(Some(Ok(TransportExitStatus {
                        success: false,
                        code: None,
                    })));
                } else {
                    terminal.send_replace(Some(Err(error)));
                }
                break;
            }
        };
        if write_delta(&mut stdout, &snapshot.stdout, &mut stdout_cursor)
            .await
            .is_err()
            || write_delta(&mut stderr, &snapshot.stderr, &mut stderr_cursor)
                .await
                .is_err()
        {
            terminal.send_replace(Some(Err(TransportError::new(
                TransportErrorKind::StreamFailed,
                "temps_sandbox",
                "stream",
                "the SDK consumer closed a sandbox process stream",
                false,
            ))));
            break;
        }
        match snapshot.status.as_str() {
            "running" => tokio::time::sleep(inner.poll_interval).await,
            "exited" => {
                let code = snapshot.exit_code;
                terminal.send_replace(Some(Ok(TransportExitStatus {
                    success: code == Some(0),
                    code,
                })));
                break;
            }
            "failed" => {
                terminal.send_replace(Some(Err(TransportError::new(
                    TransportErrorKind::RemoteUnavailable,
                    "temps_sandbox",
                    "process",
                    snapshot
                        .reason
                        .as_deref()
                        .map_or_else(|| "sandbox process failed".to_string(), bounded),
                    false,
                ))));
                break;
            }
            other => {
                terminal.send_replace(Some(Err(TransportError::new(
                    TransportErrorKind::Protocol,
                    "temps_sandbox",
                    "poll",
                    format!("unknown sandbox job status {other:?}"),
                    false,
                ))));
                break;
            }
        }
    }
}

async fn write_delta(
    writer: &mut DuplexStream,
    contents: &str,
    cursor: &mut usize,
) -> std::io::Result<()> {
    let bytes = contents.as_bytes();
    if *cursor > bytes.len() {
        *cursor = 0;
    }
    if *cursor < bytes.len() {
        writer.write_all(&bytes[*cursor..]).await?;
        writer.flush().await?;
        *cursor = bytes.len();
    }
    Ok(())
}

async fn decode_response<R: DeserializeOwned>(
    response: reqwest::Response,
    operation: &'static str,
) -> TransportResult<R> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| http_client_error(operation, error))?;
    if !status.is_success() {
        return Err(http_status_error(operation, status, &bytes));
    }
    if bytes.is_empty() {
        return serde_json::from_slice(b"null").map_err(|_| {
            TransportError::new(
                TransportErrorKind::Protocol,
                "temps_sandbox",
                operation,
                "empty response from Temps sandbox API",
                false,
            )
        });
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "temps_sandbox",
            operation,
            format!("invalid JSON response: {error}"),
            false,
        )
    })
}

fn http_client_error(operation: &'static str, error: reqwest::Error) -> TransportError {
    let (kind, retryable) = if error.is_timeout() {
        (TransportErrorKind::ConnectionTimedOut, true)
    } else if error.is_connect() {
        (TransportErrorKind::RemoteUnavailable, true)
    } else {
        (TransportErrorKind::Protocol, false)
    };
    TransportError::new(
        kind,
        "temps_sandbox",
        operation,
        error.to_string(),
        retryable,
    )
}

fn http_status_error(operation: &'static str, status: StatusCode, bytes: &[u8]) -> TransportError {
    let detail = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("detail")
                .or_else(|| value.get("message"))
                .and_then(serde_json::Value::as_str)
                .map(bounded)
        })
        .unwrap_or_else(|| format!("Temps sandbox API returned HTTP {status}"));
    let stopped_or_paused = status == StatusCode::CONFLICT
        && (detail.contains("state 'stopped'") || detail.contains("state 'paused'"));
    let (kind, retryable) = if stopped_or_paused {
        (TransportErrorKind::RemoteUnavailable, true)
    } else {
        match status {
            StatusCode::UNAUTHORIZED => (TransportErrorKind::AuthenticationFailed, false),
            StatusCode::FORBIDDEN => (TransportErrorKind::PermissionDenied, false),
            StatusCode::NOT_FOUND => (TransportErrorKind::RemoteUnavailable, false),
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
                (TransportErrorKind::ConnectionTimedOut, true)
            }
            StatusCode::TOO_MANY_REQUESTS => (TransportErrorKind::RemoteUnavailable, true),
            status if status.is_server_error() => (TransportErrorKind::RemoteUnavailable, true),
            _ => (TransportErrorKind::Protocol, false),
        }
    };
    TransportError::new(kind, "temps_sandbox", operation, detail, retryable)
}

fn environment_to_strings(
    environment: &BTreeMap<OsString, OsString>,
) -> TransportResult<BTreeMap<String, String>> {
    environment
        .iter()
        .map(|(name, value)| {
            let name = os_to_utf8(name, "environment variable name")?;
            if !valid_environment_name(name) {
                return Err(TransportError::new(
                    TransportErrorKind::InvalidConfiguration,
                    "temps_sandbox",
                    "spawn",
                    format!("invalid environment variable name {name:?}"),
                    false,
                ));
            }
            Ok((
                name.to_string(),
                os_to_utf8(value, "environment variable value")?.to_string(),
            ))
        })
        .collect()
}

fn stdin_staging_path(directory: &str, process_id: u32, input_id: u64) -> String {
    let directory = directory.trim_end_matches('/');
    if directory.is_empty() {
        format!("/.temps-agent-runtime-stdin-{process_id}-{input_id}")
    } else {
        format!("{directory}/.temps-agent-runtime-stdin-{process_id}-{input_id}")
    }
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && chars.all(|character| matches!(character, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

fn path_to_utf8<'a>(path: &'a Path, field: &'static str) -> TransportResult<&'a str> {
    path.to_str().ok_or_else(|| invalid_utf8(field))
}

fn os_to_utf8<'a>(value: &'a OsStr, field: &'static str) -> TransportResult<&'a str> {
    value.to_str().ok_or_else(|| invalid_utf8(field))
}

fn invalid_utf8(field: &'static str) -> TransportError {
    TransportError::new(
        TransportErrorKind::InvalidConfiguration,
        "temps_sandbox",
        "configure",
        format!("{field} must be valid UTF-8 for sandbox execution"),
        false,
    )
}

fn parse_directory_candidates(
    stdout: &str,
    limit: usize,
) -> TransportResult<WorkingDirectoryCandidates> {
    const MARKER: &str = "\0TEMPS_AGENT_RUNTIME_DIRECTORIES\0";
    let marker = stdout.rfind(MARKER).ok_or_else(|| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "temps_sandbox",
            "suggest_working_directories",
            "the sandbox omitted its directory response marker",
            false,
        )
    })?;
    let mut fields = stdout[marker + MARKER.len()..].split('\0');
    let home = fields
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Protocol,
                "temps_sandbox",
                "suggest_working_directories",
                "the sandbox returned an empty home directory",
                false,
            )
        })?;
    let exact_match = match fields.next() {
        Some("1") => true,
        Some("0") => false,
        _ => {
            return Err(TransportError::new(
                TransportErrorKind::Protocol,
                "temps_sandbox",
                "suggest_working_directories",
                "the sandbox returned an invalid exact-match flag",
                false,
            ));
        }
    };
    let directories = fields
        .filter(|field| !field.is_empty())
        .take(limit)
        .map(PathBuf::from)
        .collect();
    Ok(WorkingDirectoryCandidates {
        home: PathBuf::from(home),
        directories,
        exact_match,
    })
}

fn bounded(value: &str) -> String {
    let tail = if value.len() > 8 * 1024 {
        &value[value.len() - 8 * 1024..]
    } else {
        value
    };
    tail.trim().replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_sandbox_identifiers() {
        let error = TempsSandboxTransport::builder(
            "http://localhost:8080/api",
            "../escape",
            TempsSandboxAuth::Bearer(SecretString::new("redacted")),
        )
        .build()
        .expect_err("invalid id");
        assert_eq!(error.kind, TransportErrorKind::InvalidConfiguration);
    }

    #[test]
    fn rejects_credentials_fragments_and_unapproved_cleartext_endpoints() {
        for url in [
            "https://user:secret@example.test/api",
            "https://example.test/api#private",
            "http://192.0.2.10/api",
        ] {
            let error = TempsSandboxTransport::builder(
                url,
                "sandbox-1",
                TempsSandboxAuth::Bearer(SecretString::new("redacted")),
            )
            .build()
            .expect_err("unsafe URL must be rejected");
            assert_eq!(error.kind, TransportErrorKind::InvalidConfiguration);
        }

        TempsSandboxTransport::builder(
            "http://192.0.2.10/api",
            "sandbox-1",
            TempsSandboxAuth::Bearer(SecretString::new("redacted")),
        )
        .allow_insecure_http(true)
        .build()
        .expect("an explicit trusted-network override is allowed");
    }

    #[test]
    fn classifies_auth_and_retryable_server_errors() {
        let auth = http_status_error("spawn", StatusCode::UNAUTHORIZED, b"{}");
        assert_eq!(auth.kind, TransportErrorKind::AuthenticationFailed);
        assert!(!auth.retryable);

        let server = http_status_error("spawn", StatusCode::BAD_GATEWAY, b"{}");
        assert_eq!(server.kind, TransportErrorKind::RemoteUnavailable);
        assert!(server.retryable);
    }

    #[test]
    fn stages_secure_stdin_inside_the_remote_working_directory() {
        assert_eq!(
            stdin_staging_path("/home/temps/workspace", 42, 7),
            "/home/temps/workspace/.temps-agent-runtime-stdin-42-7"
        );
        assert_eq!(
            stdin_staging_path("/", 42, 7),
            "/.temps-agent-runtime-stdin-42-7"
        );
    }

    #[test]
    fn parses_current_sandbox_folder_before_its_children() {
        let candidates = parse_directory_candidates(
            "shell noise\n\0TEMPS_AGENT_RUNTIME_DIRECTORIES\0/home/agent\x001\0/home/agent\0/home/agent/projects\0",
            20,
        )
        .expect("directory candidates");

        assert_eq!(candidates.home, PathBuf::from("/home/agent"));
        assert!(candidates.exact_match);
        assert_eq!(
            candidates.directories,
            [
                PathBuf::from("/home/agent"),
                PathBuf::from("/home/agent/projects")
            ]
        );
    }
}
