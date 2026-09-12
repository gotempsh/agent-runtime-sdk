//! SSH-backed execution transport.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, Command};
use tokio::sync::OnceCell;

use crate::{
    ExecutionTransport, Provider, ProviderReadiness, SandboxCapabilities, SecretString,
    TransportCapabilities, TransportError, TransportErrorKind, TransportExitStatus,
    TransportProcess, TransportProcessControl, TransportProcessHandle, TransportReader,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, TransportWriter,
    WorkingDirectoryCandidates, WorkingDirectoryQuery,
};

const STDERR_CAPTURE_BYTES: usize = 32 * 1024;
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
static NEXT_PROCESS_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_ASKPASS_ID: AtomicU64 = AtomicU64::new(1);
const ASKPASS_PASSWORD_ENV: &str = "TEMPS_AGENT_RUNTIME_SSH_PASSWORD";
const LOGIN_ENVIRONMENT_MARKER: &[u8] = b"\0TEMPS_AGENT_RUNTIME_LOGIN_ENV\0";
const DIRECTORY_CANDIDATES_MARKER: &[u8] = b"\0TEMPS_AGENT_RUNTIME_DIRECTORIES\0";
const CONTROL_DIRECTORY_MARKER: &[u8] = b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0";
const SAFE_REMOTE_ENVIRONMENT: &[&str] = &[
    "USER",
    "LOGNAME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "NO_COLOR",
    "TMPDIR",
    "CLAUDE_HOME",
    "CODEX_HOME",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

/// Host-key behavior for an SSH connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SshHostKeyPolicy {
    /// Require the host key to exist in the configured known-hosts database.
    Strict,
    /// Trust a previously unseen host key, while still rejecting changed keys.
    AcceptNew,
}

/// Authentication method used by the local OpenSSH client.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SshAuthentication {
    /// Use the local SSH agent and OpenSSH's default identity files.
    #[default]
    Agent,
    /// Use one explicit private key file.
    IdentityFile(PathBuf),
    /// Use password authentication through a forced askpass channel.
    ///
    /// The password is never placed in command arguments. Password mode is
    /// currently available on Unix hosts running OpenSSH.
    Password(SecretString),
}

/// Builder for [`SshTransport`].
#[derive(Debug, Clone)]
pub struct SshTransportBuilder {
    host: String,
    user: Option<String>,
    port: Option<u16>,
    authentication: SshAuthentication,
    known_hosts_file: Option<PathBuf>,
    host_key_policy: SshHostKeyPolicy,
    connect_timeout: Duration,
    executable: PathBuf,
    intrinsic_sandbox: SandboxCapabilities,
}

impl SshTransportBuilder {
    /// Create a builder for a DNS name or IP address.
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            user: None,
            port: None,
            authentication: SshAuthentication::Agent,
            known_hosts_file: None,
            host_key_policy: SshHostKeyPolicy::Strict,
            connect_timeout: Duration::from_secs(10),
            executable: PathBuf::from("ssh"),
            intrinsic_sandbox: SandboxCapabilities::NONE,
        }
    }

    /// Remote SSH user.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Remote SSH port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Private key path passed to OpenSSH with `-i`.
    pub fn identity_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.authentication = SshAuthentication::IdentityFile(path.into());
        self
    }

    /// Authenticate with a password through OpenSSH's askpass protocol.
    ///
    /// Password authentication requires an explicit [`Self::user`]. The
    /// secret is redacted from debug output and is never placed in argv.
    pub fn password(mut self, password: SecretString) -> Self {
        self.authentication = SshAuthentication::Password(password);
        self
    }

    /// Use the local SSH agent and OpenSSH's default identity files.
    pub fn agent(mut self) -> Self {
        self.authentication = SshAuthentication::Agent;
        self
    }

    /// Dedicated known-hosts database used for host-key verification.
    pub fn known_hosts_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.known_hosts_file = Some(path.into());
        self
    }

    /// Configure strict or accept-new host-key behavior.
    pub fn host_key_policy(mut self, policy: SshHostKeyPolicy) -> Self {
        self.host_key_policy = policy;
        self
    }

    /// Connection establishment deadline.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the local OpenSSH executable.
    pub fn executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = path.into();
        self
    }

    /// Declare isolation controls enforced by the SSH destination itself.
    pub fn intrinsic_sandbox(mut self, capabilities: SandboxCapabilities) -> Self {
        self.intrinsic_sandbox = capabilities;
        self
    }

    /// Validate the configuration and construct a transport.
    pub fn build(self) -> TransportResult<SshTransport> {
        validate_destination_part("host", &self.host)?;
        if let Some(user) = self.user.as_deref() {
            validate_destination_part("user", user)?;
        }
        if let SshAuthentication::Password(password) = &self.authentication {
            if self.user.is_none() {
                return Err(TransportError::new(
                    TransportErrorKind::InvalidConfiguration,
                    "ssh",
                    "configure",
                    "SSH password authentication requires an explicit user",
                    false,
                ));
            }
            if password.expose().is_empty() || password.expose().contains('\0') {
                return Err(TransportError::new(
                    TransportErrorKind::InvalidConfiguration,
                    "ssh",
                    "configure",
                    "SSH password must be non-empty and cannot contain NUL bytes",
                    false,
                ));
            }
        }
        if self.connect_timeout.is_zero() {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "ssh",
                "configure",
                "connect_timeout must be greater than zero",
                false,
            ));
        }
        let askpass = match &self.authentication {
            SshAuthentication::Password(_) => Some(Arc::new(AskpassHelper::create()?)),
            _ => None,
        };
        Ok(SshTransport {
            host: self.host,
            user: self.user,
            port: self.port,
            authentication: self.authentication,
            known_hosts_file: self.known_hosts_file,
            host_key_policy: self.host_key_policy,
            connect_timeout: self.connect_timeout,
            executable: self.executable,
            intrinsic_sandbox: self.intrinsic_sandbox,
            askpass,
            login_environment: Arc::new(OnceCell::new()),
        })
    }
}

#[derive(Debug)]
struct AskpassHelper {
    directory: PathBuf,
    executable: PathBuf,
}

impl AskpassHelper {
    #[cfg(unix)]
    fn create() -> TransportResult<Self> {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

        let directory = std::env::temp_dir().join(format!(
            "temps-agent-runtime-askpass-{}-{}",
            std::process::id(),
            NEXT_ASKPASS_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut directory_builder = std::fs::DirBuilder::new();
        directory_builder.mode(0o700);
        directory_builder.create(&directory).map_err(|error| {
            TransportError::new(
                TransportErrorKind::SpawnFailed,
                "ssh",
                "configure_password",
                format!("could not create the protected SSH askpass directory: {error}"),
                false,
            )
        })?;
        let executable = directory.join("askpass");
        let result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o700)
                .open(&executable)?;
            file.write_all(
                b"#!/bin/sh\nif [ \"${TEMPS_AGENT_RUNTIME_SSH_PASSWORD+x}\" = x ]; then\n  printf '%s\\n' \"$TEMPS_AGENT_RUNTIME_SSH_PASSWORD\"\nelse\n  exit 1\nfi\n",
            )?;
            file.sync_all()
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&executable);
            let _ = std::fs::remove_dir(&directory);
            return Err(TransportError::new(
                TransportErrorKind::SpawnFailed,
                "ssh",
                "configure_password",
                format!("could not create the protected SSH askpass helper: {error}"),
                false,
            ));
        }
        Ok(Self {
            directory,
            executable,
        })
    }

    #[cfg(not(unix))]
    fn create() -> TransportResult<Self> {
        Err(TransportError::new(
            TransportErrorKind::Unsupported,
            "ssh",
            "configure_password",
            "SSH password authentication currently requires a Unix host with OpenSSH",
            false,
        ))
    }
}

impl Drop for AskpassHelper {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.executable);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

/// Execute provider CLIs and managed processes through the local OpenSSH client.
#[derive(Debug, Clone)]
pub struct SshTransport {
    host: String,
    user: Option<String>,
    port: Option<u16>,
    authentication: SshAuthentication,
    known_hosts_file: Option<PathBuf>,
    host_key_policy: SshHostKeyPolicy,
    connect_timeout: Duration,
    executable: PathBuf,
    intrinsic_sandbox: SandboxCapabilities,
    askpass: Option<Arc<AskpassHelper>>,
    login_environment: Arc<OnceCell<RemoteLoginEnvironment>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteLoginEnvironment {
    home: String,
    path: String,
    shell: String,
}

impl SshTransport {
    /// Create a builder for an SSH destination.
    pub fn builder(host: impl Into<String>) -> SshTransportBuilder {
        SshTransportBuilder::new(host)
    }

    fn destination(&self) -> String {
        self.user
            .as_ref()
            .map_or_else(|| self.host.clone(), |user| format!("{user}@{}", self.host))
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .arg("-o")
            .arg(format!(
                "ConnectTimeout={}",
                self.connect_timeout.as_secs().max(1)
            ))
            .arg("-o")
            .arg("ServerAliveInterval=15")
            .arg("-o")
            .arg("ServerAliveCountMax=2")
            .arg("-o")
            .arg(match self.host_key_policy {
                SshHostKeyPolicy::Strict => "StrictHostKeyChecking=yes",
                SshHostKeyPolicy::AcceptNew => "StrictHostKeyChecking=accept-new",
            });
        match &self.authentication {
            SshAuthentication::Agent => {
                command.arg("-o").arg("BatchMode=yes");
            }
            SshAuthentication::IdentityFile(path) => {
                command.arg("-o").arg("BatchMode=yes").arg("-i").arg(path);
            }
            SshAuthentication::Password(password) => {
                let askpass = self
                    .askpass
                    .as_ref()
                    .expect("password authentication constructs an askpass helper");
                command
                    .arg("-o")
                    .arg("BatchMode=no")
                    .arg("-o")
                    .arg("PreferredAuthentications=password")
                    .arg("-o")
                    .arg("PubkeyAuthentication=no")
                    .arg("-o")
                    .arg("KbdInteractiveAuthentication=no")
                    .arg("-o")
                    .arg("NumberOfPasswordPrompts=1")
                    .arg("-o")
                    .arg(format!("SendEnv=-{ASKPASS_PASSWORD_ENV}"))
                    .env("SSH_ASKPASS", &askpass.executable)
                    .env("SSH_ASKPASS_REQUIRE", "force")
                    .env("DISPLAY", "temps-agent-runtime:0")
                    .env(ASKPASS_PASSWORD_ENV, password.expose());
            }
        }
        if let Some(path) = &self.known_hosts_file {
            command
                .arg("-o")
                .arg(format!("UserKnownHostsFile={}", path.display()));
        }
        if let Some(port) = self.port {
            command.arg("-p").arg(port.to_string());
        }
        command.arg("--").arg(self.destination());
        command
    }

    async fn login_environment(&self) -> TransportResult<&RemoteLoginEnvironment> {
        self.login_environment
            .get_or_try_init(|| async {
                let output = self
                    .output(
                        "resolve_login_environment",
                        &login_environment_probe_command(),
                    )
                    .await?;
                if !output.status.success() {
                    let (kind, message) = if output.status.code() == Some(127) {
                        (
                            TransportErrorKind::Unsupported,
                            "the SSH user needs Bash or Zsh to resolve its login PATH".to_string(),
                        )
                    } else {
                        let diagnostic = bounded_diagnostic(&output.stderr);
                        let message = if diagnostic.is_empty() {
                            "the SSH user's login shell could not resolve HOME and PATH".to_string()
                        } else {
                            format!(
                                "the SSH user's login shell could not resolve HOME and PATH: {diagnostic}"
                            )
                        };
                        (TransportErrorKind::RemoteUnavailable, message)
                    };
                    return Err(TransportError::new(
                        kind,
                        self.name(),
                        "resolve_login_environment",
                        message,
                        false,
                    ));
                }
                parse_login_environment(&output.stdout)
            })
            .await
    }

    async fn output(
        &self,
        operation: &'static str,
        remote_command: &str,
    ) -> TransportResult<std::process::Output> {
        self.output_with_timeout(
            operation,
            remote_command,
            self.connect_timeout + Duration::from_secs(5),
        )
        .await
    }

    async fn output_with_timeout(
        &self,
        operation: &'static str,
        remote_command: &str,
        timeout: Duration,
    ) -> TransportResult<std::process::Output> {
        let mut command = self.base_command();
        command.arg(remote_command).kill_on_drop(true);
        let output = tokio::time::timeout(timeout, command.output())
            .await
            .map_err(|_| {
                TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.name(),
                    operation,
                    "SSH operation exceeded its connection deadline",
                    true,
                )
            })?
            .map_err(|error| classify_local_ssh_error(operation, error))?;
        if output.status.code() == Some(255) {
            return Err(classify_ssh_diagnostic(operation, &output.stderr));
        }
        Ok(output)
    }

    async fn stage_remote_launcher(&self, launcher: &[u8]) -> TransportResult<String> {
        let remote_command = "set -eu; umask 077; \
            control_dir=$(mktemp -d \"${TMPDIR:-/tmp}/temps-agent-runtime.XXXXXXXXXX\"); \
            trap 'rm -rf -- \"$control_dir\"' EXIT HUP INT TERM; \
            cat > \"$control_dir/launch\"; chmod 700 \"$control_dir/launch\"; \
            printf '\\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\\0%s\\0' \"$control_dir\"; \
            trap - EXIT HUP INT TERM";
        let mut command = self.base_command();
        command
            .arg(remote_command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| classify_local_ssh_error("stage_launcher", error))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| stream_setup_error("stdin"))?;
        let operation = async move {
            stdin.write_all(launcher).await?;
            stdin.shutdown().await?;
            drop(stdin);
            child.wait_with_output().await
        };
        let output = tokio::time::timeout(self.connect_timeout + Duration::from_secs(5), operation)
            .await
            .map_err(|_| {
                TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.name(),
                    "stage_launcher",
                    "SSH launcher staging exceeded its connection deadline",
                    true,
                )
            })?
            .map_err(|error| classify_local_ssh_error("stage_launcher", error))?;
        if output.status.code() == Some(255) {
            return Err(classify_ssh_diagnostic("stage_launcher", &output.stderr));
        }
        if !output.status.success() {
            return Err(TransportError::new(
                TransportErrorKind::SpawnFailed,
                self.name(),
                "stage_launcher",
                bounded_diagnostic(&output.stderr),
                true,
            ));
        }
        parse_control_directory(&output.stdout)
    }

    async fn cleanup_remote_control_directory(&self, control_directory: &str) {
        let _ = self
            .output(
                "cleanup_launcher",
                &format!("rm -rf -- {}", quote_posix(control_directory)),
            )
            .await;
    }

    async fn terminate_remote(&self, pid_file: &str) -> TransportResult<()> {
        let command = format!(
            "i=0; while [ ! -s {pid_file} ] && [ \"$i\" -lt 20 ]; do sleep 0.05; i=$((i + 1)); done; \
             if IFS= read -r pid < {pid_file}; then \
               case \"$pid\" in (*[!0-9]*|'') exit 64;; esac; \
               kill -TERM -- -\"$pid\" 2>/dev/null || true; sleep 0.2; \
               kill -KILL -- -\"$pid\" 2>/dev/null || true; \
             fi; rm -f -- {pid_file}",
            pid_file = quote_posix(pid_file),
        );
        let output = self.output("terminate", &command).await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(TransportError::new(
                TransportErrorKind::ProcessControlFailed,
                self.name(),
                "terminate",
                bounded_diagnostic(&output.stderr),
                true,
            ))
        }
    }
}

#[async_trait]
impl ExecutionTransport for SshTransport {
    fn name(&self) -> &'static str {
        "ssh"
    }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: true,
            interactive_stdin: true,
            reconnect: false,
            managed_processes: true,
            process_tree_termination: true,
            sandbox: self.intrinsic_sandbox,
        }
    }

    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        let environment = self.login_environment().await?;
        let program = os_to_utf8(&request.program, "provider executable")?;
        let resolution_command = build_executable_resolution_command(program, environment);
        let output = self
            .output("resolve_executable", &resolution_command)
            .await?;
        if output.status.code() == Some(127) {
            return Ok(ProviderReadiness {
                provider: request.provider,
                installed: false,
                executable: None,
                version: None,
                detail: format!(
                    "Install {} inside the SSH destination and authenticate it as the SSH user.",
                    request.provider
                ),
            });
        }
        if !output.status.success() {
            return Err(TransportError::new(
                TransportErrorKind::RemoteUnavailable,
                self.name(),
                "readiness",
                bounded_diagnostic(&output.stderr),
                false,
            ));
        }
        let executable = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if executable.is_empty() {
            return Err(TransportError::new(
                TransportErrorKind::Protocol,
                self.name(),
                "resolve_executable",
                "the SSH shell found the provider executable but did not return its path",
                false,
            ));
        }
        let version_command = build_version_command(&executable, environment);
        let version_output = self
            .output_with_timeout("version", &version_command, VERSION_PROBE_TIMEOUT)
            .await;
        Ok(readiness_from_version_probe(
            request.provider,
            executable,
            version_output,
        ))
    }

    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()> {
        let directory = os_to_utf8(working_directory, "working directory")?;
        let output = self
            .output(
                "validate_working_directory",
                &format!("test -d {}", quote_posix(directory)),
            )
            .await?;
        if output.status.success() {
            Ok(())
        } else {
            Err(TransportError::new(
                TransportErrorKind::WorkingDirectoryNotFound,
                self.name(),
                "validate_working_directory",
                format!("{directory} is not a directory on the SSH destination"),
                false,
            ))
        }
    }

    async fn suggest_working_directories(
        &self,
        query: WorkingDirectoryQuery,
    ) -> TransportResult<WorkingDirectoryCandidates> {
        let command = build_directory_suggestion_command(&query.input, query.limit);
        let output = self.output("suggest_working_directories", &command).await?;
        if !output.status.success() {
            return Err(TransportError::new(
                TransportErrorKind::RemoteUnavailable,
                self.name(),
                "suggest_working_directories",
                bounded_diagnostic(&output.stderr),
                true,
            ));
        }
        parse_directory_candidates(&output.stdout, query.limit)
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        let native_id = format!(
            "{}-{}-{}",
            std::process::id(),
            now_millis(),
            NEXT_PROCESS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let environment = self.login_environment().await?;
        let launcher = build_remote_launcher(&request, environment)?;
        let control_directory = self.stage_remote_launcher(launcher.as_bytes()).await?;
        let pid_file = format!("{control_directory}/pid");
        let remote_command = build_remote_command(&request, &control_directory)?;
        let mut command = self.base_command();
        command
            .arg(remote_command)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.cleanup_remote_control_directory(&control_directory)
                    .await;
                return Err(classify_local_ssh_error("spawn", error));
            }
        };
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .map(|stream| Box::new(stream) as TransportWriter);
        let stdout = child
            .stdout
            .take()
            .map(|stream| Box::new(stream) as TransportReader)
            .ok_or_else(|| stream_setup_error("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| stream_setup_error("stderr"))?;
        let stderr_capture = Arc::new(Mutex::new(Vec::new()));
        let stderr = Box::new(CaptureReader {
            inner: Box::new(stderr),
            captured: Arc::clone(&stderr_capture),
        }) as TransportReader;
        let process_tree = crate::process::ProcessTreeGuard::for_child(&child);
        Ok(TransportProcess::new(
            TransportProcessHandle {
                transport: self.name().to_string(),
                native_id,
            },
            pid,
            stdin,
            stdout,
            stderr,
            SshProcessControl {
                child,
                process_tree,
                stderr_capture,
                transport: self.clone(),
                pid_file,
                control_directory,
                remote_finished: false,
            },
        ))
    }
}

struct SshProcessControl {
    child: Child,
    process_tree: crate::process::ProcessTreeGuard,
    stderr_capture: Arc<Mutex<Vec<u8>>>,
    transport: SshTransport,
    pid_file: String,
    control_directory: String,
    remote_finished: bool,
}

#[async_trait]
impl TransportProcessControl for SshProcessControl {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        let status = self
            .child
            .wait()
            .await
            .map_err(|error| classify_local_ssh_error("wait", error))?;
        if status.code() == Some(255) {
            let stderr = self
                .stderr_capture
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            return Err(classify_ssh_diagnostic("wait", &stderr));
        }
        self.remote_finished = true;
        Ok(status.into())
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        let remote_result = self.transport.terminate_remote(&self.pid_file).await;
        self.transport
            .cleanup_remote_control_directory(&self.control_directory)
            .await;
        self.process_tree.terminate();
        self.remote_finished = true;
        remote_result
    }

    fn disarm(&mut self) {
        self.remote_finished = true;
        self.process_tree.disarm();
    }
}

impl Drop for SshProcessControl {
    fn drop(&mut self) {
        if self.remote_finished {
            return;
        }
        self.remote_finished = true;
        let transport = self.transport.clone();
        let pid_file = self.pid_file.clone();
        let control_directory = self.control_directory.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = transport.terminate_remote(&pid_file).await;
                transport
                    .cleanup_remote_control_directory(&control_directory)
                    .await;
            });
        }
    }
}

struct CaptureReader {
    inner: TransportReader,
    captured: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for CaptureReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) {
            let bytes = &buffer.filled()[before..];
            if !bytes.is_empty() {
                let mut captured = self
                    .captured
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                captured.extend_from_slice(bytes);
                if captured.len() > STDERR_CAPTURE_BYTES {
                    let remove = captured.len() - STDERR_CAPTURE_BYTES;
                    captured.drain(..remove);
                }
            }
        }
        result
    }
}

fn build_remote_command(
    request: &TransportSpawnRequest,
    control_directory: &str,
) -> TransportResult<String> {
    let directory = os_to_utf8(&request.working_directory, "working directory")?;
    let pid_file = format!("{control_directory}/pid");
    let launcher = format!("{control_directory}/launch");
    let cleanup = format!("rm -rf -- {}", quote_posix(control_directory));
    Ok(format!(
        "cd -- {directory} && umask 077 && printf '%s\\n' \"$$\" > {pid_file} && \
         trap {cleanup} EXIT HUP INT TERM; \
         {launcher}; runtime_exit_code=$?; rm -rf -- {control_directory}; \
         trap - EXIT HUP INT TERM; exit $runtime_exit_code",
        directory = quote_posix(directory),
        pid_file = quote_posix(&pid_file),
        launcher = quote_posix(&launcher),
        control_directory = quote_posix(control_directory),
        cleanup = quote_posix(&cleanup),
    ))
}

fn build_remote_launcher(
    request: &TransportSpawnRequest,
    login_environment: &RemoteLoginEnvironment,
) -> TransportResult<String> {
    let program = os_to_utf8(&request.command.program, "program")?;
    let mut invocation = "#!/bin/sh\nexec env".to_string();
    if request.command.clear_environment {
        invocation.push_str(" -i");
    }
    for (name, value) in [
        ("HOME", login_environment.home.as_str()),
        ("PATH", login_environment.path.as_str()),
        ("SHELL", login_environment.shell.as_str()),
    ] {
        invocation.push(' ');
        invocation.push_str(&quote_posix(&format!("{name}={value}")));
    }
    if request.command.clear_environment {
        for name in SAFE_REMOTE_ENVIRONMENT {
            invocation.push(' ');
            invocation.push_str("${");
            invocation.push_str(name);
            invocation.push('+');
            invocation.push_str(name);
            invocation.push_str("=\"$");
            invocation.push_str(name);
            invocation.push_str("\"}");
        }
    }
    append_environment(&mut invocation, &request.command.environment)?;
    invocation.push(' ');
    invocation.push_str(&quote_posix(program));
    for argument in &request.command.args {
        invocation.push(' ');
        invocation.push_str(&quote_posix(os_string_to_utf8(argument, "argument")?));
    }
    invocation.push('\n');
    Ok(invocation)
}

fn parse_control_directory(stdout: &[u8]) -> TransportResult<String> {
    let marker = stdout
        .windows(CONTROL_DIRECTORY_MARKER.len())
        .rposition(|window| window == CONTROL_DIRECTORY_MARKER)
        .ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Protocol,
                "ssh",
                "stage_launcher",
                "the remote shell omitted its launcher control directory",
                false,
            )
        })?;
    let payload = &stdout[marker + CONTROL_DIRECTORY_MARKER.len()..];
    let end = payload.iter().position(|byte| *byte == 0).ok_or_else(|| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "stage_launcher",
            "the remote shell returned an unterminated launcher control directory",
            false,
        )
    })?;
    let directory = std::str::from_utf8(&payload[..end]).map_err(|_| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "stage_launcher",
            "the remote shell returned a non-UTF-8 launcher control directory",
            false,
        )
    })?;
    // The directory belongs to a POSIX SSH host even when this client runs on Windows.
    let valid_name = directory.rsplit('/').next().is_some_and(|name| {
        name.strip_prefix("temps-agent-runtime.")
            .is_some_and(|suffix| {
                suffix.len() == 10 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
    });
    if !directory.starts_with('/')
        || directory
            .split('/')
            .any(|component| component == "." || component == "..")
        || directory.contains(['\\', '\r', '\n'])
        || !valid_name
    {
        return Err(TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "stage_launcher",
            "the remote shell returned an invalid launcher control directory",
            false,
        ));
    }
    Ok(directory.to_string())
}

fn build_executable_resolution_command(
    program: &str,
    environment: &RemoteLoginEnvironment,
) -> String {
    format!(
        "env -i {home} {path} {shell} /bin/sh -c 'command -v \"$1\"' runtime-probe {program}",
        home = quote_posix(&format!("HOME={}", environment.home)),
        path = quote_posix(&format!("PATH={}", environment.path)),
        shell = quote_posix(&format!("SHELL={}", environment.shell)),
        program = quote_posix(program),
    )
}

fn build_version_command(executable: &str, environment: &RemoteLoginEnvironment) -> String {
    format!(
        "env -i {home} {path} {shell} {executable} --version",
        home = quote_posix(&format!("HOME={}", environment.home)),
        path = quote_posix(&format!("PATH={}", environment.path)),
        shell = quote_posix(&format!("SHELL={}", environment.shell)),
        executable = quote_posix(executable),
    )
}

fn build_directory_suggestion_command(input: &str, limit: usize) -> String {
    let script = "query=$1; limit=$2; \
        case \"$query\" in \
          ''|'~') expanded=$HOME ;; \
          '~/'*) expanded=$HOME/${query#~/} ;; \
          /*) expanded=$query ;; \
          *) expanded=$HOME/$query ;; \
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
        printf '\\0TEMPS_AGENT_RUNTIME_DIRECTORIES\\0%s\\0%s\\0' \"$HOME\" \"$exact\"; \
        count=0; \
        if test \"$exact\" = 1 && test \"$count\" -lt \"$limit\"; then \
          printf '%s\\0' \"$expanded\"; \
          count=$((count + 1)); \
        fi; \
        if test -d \"$parent\" && test \"$count\" -lt \"$limit\"; then \
          for candidate in \"$parent\"/\"$prefix\"*; do \
            test -d \"$candidate\" || continue; \
            printf '%s\\0' \"$candidate\"; \
            count=$((count + 1)); \
            test \"$count\" -lt \"$limit\" || break; \
          done; \
        fi";
    format!(
        "/bin/sh -c {script} runtime-directories {input} {limit}",
        script = quote_posix(script),
        input = quote_posix(input),
    )
}

fn parse_directory_candidates(
    stdout: &[u8],
    limit: usize,
) -> TransportResult<WorkingDirectoryCandidates> {
    let marker = stdout
        .windows(DIRECTORY_CANDIDATES_MARKER.len())
        .rposition(|window| window == DIRECTORY_CANDIDATES_MARKER)
        .ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Protocol,
                "ssh",
                "suggest_working_directories",
                "the remote shell omitted its directory response marker",
                false,
            )
        })?;
    let payload = &stdout[marker + DIRECTORY_CANDIDATES_MARKER.len()..];
    let mut fields = payload.split(|byte| *byte == 0);
    let home = parse_directory_field(fields.next(), "home")?;
    let exact_match = match fields.next() {
        Some(b"1") => true,
        Some(b"0") => false,
        _ => {
            return Err(TransportError::new(
                TransportErrorKind::Protocol,
                "ssh",
                "suggest_working_directories",
                "the remote shell returned an invalid exact-match flag",
                false,
            ));
        }
    };
    let directories = fields
        .filter(|field| !field.is_empty())
        .take(limit)
        .map(|field| parse_directory_field(Some(field), "directory").map(PathBuf::from))
        .collect::<TransportResult<Vec<_>>>()?;
    Ok(WorkingDirectoryCandidates {
        home: PathBuf::from(home),
        directories,
        exact_match,
    })
}

fn parse_directory_field(value: Option<&[u8]>, name: &'static str) -> TransportResult<String> {
    let value = value.filter(|value| !value.is_empty()).ok_or_else(|| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "suggest_working_directories",
            format!("the remote shell omitted the {name} field"),
            false,
        )
    })?;
    if value.len() > 4_096 {
        return Err(TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "suggest_working_directories",
            format!("the remote shell returned an oversized {name} field"),
            false,
        ));
    }
    String::from_utf8(value.to_vec()).map_err(|_| {
        TransportError::new(
            TransportErrorKind::Protocol,
            "ssh",
            "suggest_working_directories",
            format!("the remote shell returned non-UTF-8 {name}"),
            false,
        )
    })
}

fn readiness_from_version_probe(
    provider: Provider,
    executable: String,
    version_output: TransportResult<std::process::Output>,
) -> ProviderReadiness {
    let (version, detail) = match version_output {
        Ok(version_output) if version_output.status.success() => {
            let version = String::from_utf8_lossy(&version_output.stdout)
                .trim()
                .to_string();
            (
                (!version.is_empty()).then_some(version),
                "The CLI executable is available inside the SSH destination; authentication is checked when a turn starts.".to_string(),
            )
        }
        Ok(_) => (
            None,
            "The CLI executable is available inside the SSH destination; its optional version probe failed. Authentication is checked when a turn starts.".to_string(),
        ),
        Err(error) if error.kind == TransportErrorKind::ConnectionTimedOut => (
            None,
            format!(
                "The CLI executable is available at {executable}; its optional version probe timed out. Authentication is checked when a turn starts."
            ),
        ),
        Err(error) => (
            None,
            format!(
                "The CLI executable is available at {executable}; its optional version probe could not complete: {}",
                error.message
            ),
        ),
    };
    ProviderReadiness {
        provider,
        installed: true,
        executable: Some(PathBuf::from(executable)),
        version,
        detail,
    }
}

fn login_environment_probe_command() -> String {
    let probe = quote_posix(
        "printf '\\0TEMPS_AGENT_RUNTIME_LOGIN_ENV\\0%s\\0%s\\0%s\\0' \"$HOME\" \"$PATH\" \"$SHELL\"",
    );
    format!(
        "runtime_shell=${{SHELL:-}}; \
         case \"$runtime_shell\" in \
           */zsh|*/bash) test -x \"$runtime_shell\" || runtime_shell= ;; \
           *) runtime_shell= ;; \
         esac; \
         if test -z \"$runtime_shell\"; then \
           if command -v zsh >/dev/null 2>&1; then runtime_shell=$(command -v zsh); \
           elif command -v bash >/dev/null 2>&1; then runtime_shell=$(command -v bash); \
           else exit 127; fi; \
         fi; \
         test -n \"${{HOME:-}}\" && cd -- \"$HOME\" || exit 72; \
         SHELL=\"$runtime_shell\" \"$runtime_shell\" -lic {probe}"
    )
}

fn parse_login_environment(stdout: &[u8]) -> TransportResult<RemoteLoginEnvironment> {
    let marker = stdout
        .windows(LOGIN_ENVIRONMENT_MARKER.len())
        .rposition(|window| window == LOGIN_ENVIRONMENT_MARKER)
        .ok_or_else(|| {
            login_environment_protocol_error("the login shell omitted its environment marker")
        })?;
    let payload = &stdout[marker + LOGIN_ENVIRONMENT_MARKER.len()..];
    let mut fields = payload.split(|byte| *byte == 0);
    let home = parse_login_environment_field(fields.next(), "HOME")?;
    let path = parse_login_environment_field(fields.next(), "PATH")?;
    let shell = parse_login_environment_field(fields.next(), "SHELL")?;
    if !matches!(
        Path::new(&shell).file_name().and_then(OsStr::to_str),
        Some("bash" | "zsh")
    ) {
        return Err(login_environment_protocol_error(
            "the login environment did not report Bash or Zsh",
        ));
    }
    Ok(RemoteLoginEnvironment { home, path, shell })
}

fn parse_login_environment_field(
    value: Option<&[u8]>,
    name: &'static str,
) -> TransportResult<String> {
    let value = value.filter(|value| !value.is_empty()).ok_or_else(|| {
        login_environment_protocol_error(&format!("the login shell omitted {name}"))
    })?;
    if value.len() > 64 * 1024 {
        return Err(login_environment_protocol_error(&format!(
            "the login shell returned an oversized {name}"
        )));
    }
    String::from_utf8(value.to_vec()).map_err(|_| {
        login_environment_protocol_error(&format!("the login shell returned non-UTF-8 {name}"))
    })
}

fn login_environment_protocol_error(message: &str) -> TransportError {
    TransportError::new(
        TransportErrorKind::Protocol,
        "ssh",
        "resolve_login_environment",
        message,
        false,
    )
}

fn now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn append_environment(
    output: &mut String,
    environment: &BTreeMap<OsString, OsString>,
) -> TransportResult<()> {
    for (name, value) in environment {
        let name = os_string_to_utf8(name, "environment variable name")?;
        if !valid_environment_name(name) {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                "ssh",
                "spawn",
                format!("invalid environment variable name {name:?}"),
                false,
            ));
        }
        let value = os_string_to_utf8(value, "environment variable value")?;
        output.push(' ');
        output.push_str(&quote_posix(&format!("{name}={value}")));
    }
    Ok(())
}

fn quote_posix(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && chars.all(|character| matches!(character, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

fn validate_destination_part(field: &'static str, value: &str) -> TransportResult<()> {
    if value.is_empty()
        || value.starts_with('-')
        || value.chars().any(|character| {
            character.is_whitespace() || character.is_control() || character == '@'
        })
    {
        return Err(TransportError::new(
            TransportErrorKind::InvalidConfiguration,
            "ssh",
            "configure",
            format!("invalid SSH {field}"),
            false,
        ));
    }
    Ok(())
}

fn os_to_utf8<'a>(value: &'a Path, field: &'static str) -> TransportResult<&'a str> {
    value.to_str().ok_or_else(|| invalid_utf8(field))
}

fn os_string_to_utf8<'a>(value: &'a OsStr, field: &'static str) -> TransportResult<&'a str> {
    value.to_str().ok_or_else(|| invalid_utf8(field))
}

fn invalid_utf8(field: &'static str) -> TransportError {
    TransportError::new(
        TransportErrorKind::InvalidConfiguration,
        "ssh",
        "configure",
        format!("{field} must be valid UTF-8 for remote execution"),
        false,
    )
}

fn stream_setup_error(stream: &'static str) -> TransportError {
    TransportError::new(
        TransportErrorKind::StreamFailed,
        "ssh",
        "spawn",
        format!("SSH {stream} was not piped"),
        false,
    )
}

fn classify_local_ssh_error(operation: &'static str, error: std::io::Error) -> TransportError {
    let kind = match error.kind() {
        std::io::ErrorKind::NotFound => TransportErrorKind::ExecutableNotFound,
        std::io::ErrorKind::PermissionDenied => TransportErrorKind::PermissionDenied,
        std::io::ErrorKind::TimedOut => TransportErrorKind::ConnectionTimedOut,
        _ => TransportErrorKind::SpawnFailed,
    };
    TransportError::new(kind, "ssh", operation, error.to_string(), false)
}

fn classify_ssh_diagnostic(operation: &'static str, stderr: &[u8]) -> TransportError {
    let diagnostic = bounded_diagnostic(stderr);
    let lower = diagnostic.to_ascii_lowercase();
    let (kind, retryable, message) = if lower.contains("permission denied") {
        (
            TransportErrorKind::AuthenticationFailed,
            false,
            "SSH authentication was rejected; verify the user and selected password, key, or agent credentials",
        )
    } else if lower.contains("host key verification failed")
        || lower.contains("remote host identification has changed")
    {
        (
            TransportErrorKind::HostKeyVerificationFailed,
            false,
            "SSH host-key verification failed; verify and update the trusted known_hosts entry",
        )
    } else if lower.contains("could not resolve hostname")
        || lower.contains("name or service not known")
    {
        (
            TransportErrorKind::NameResolutionFailed,
            true,
            "the SSH host name could not be resolved",
        )
    } else if lower.contains("connection refused") {
        (
            TransportErrorKind::ConnectionRefused,
            true,
            "the SSH endpoint refused the connection",
        )
    } else if lower.contains("connection timed out") || lower.contains("operation timed out") {
        (
            TransportErrorKind::ConnectionTimedOut,
            true,
            "the SSH connection timed out",
        )
    } else {
        (
            TransportErrorKind::RemoteUnavailable,
            true,
            "the SSH connection or remote command failed",
        )
    };
    let detail = if diagnostic.is_empty() {
        message.to_string()
    } else {
        format!("{message}: {diagnostic}")
    };
    TransportError::new(kind, "ssh", operation, detail, retryable)
}

fn bounded_diagnostic(bytes: &[u8]) -> String {
    let start = bytes.len().saturating_sub(8 * 1024);
    String::from_utf8_lossy(&bytes[start..])
        .trim()
        .replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommandSpec;

    fn login_environment() -> RemoteLoginEnvironment {
        RemoteLoginEnvironment {
            home: "/Users/agent".into(),
            path: "/opt/homebrew/bin:/Users/agent/.bun/bin:/usr/bin:/bin".into(),
            shell: "/bin/zsh".into(),
        }
    }

    #[test]
    fn quotes_untrusted_remote_values_without_shell_injection() {
        let mut command = CommandSpec::new("/usr/local/bin/codex");
        command
            .args
            .push(OsString::from("hello'; touch /tmp/pwned; '"));
        command
            .environment
            .insert(OsString::from("SAFE"), OsString::from("a'b"));
        let request = TransportSpawnRequest {
            command,
            working_directory: PathBuf::from("/workspace/a'b"),
        };
        let launcher =
            build_remote_launcher(&request, &login_environment()).expect("render remote launcher");
        let rendered = build_remote_command(&request, "/tmp/temps-agent-runtime.A1b2C3d4E5")
            .expect("render command");
        assert!(launcher.contains("'hello'\"'\"'; touch /tmp/pwned; '\"'\"''"));
        assert!(launcher.contains("'SAFE=a'\"'\"'b'"));
        assert!(launcher.contains("env -i"));
        assert!(launcher.contains("'HOME=/Users/agent'"));
        assert!(launcher.contains("'PATH=/opt/homebrew/bin:/Users/agent/.bun/bin:/usr/bin:/bin'"));
        assert!(!rendered.contains("touch /tmp/pwned"));
        assert!(!rendered.contains("SAFE"));
        assert!(!rendered.contains("a'b"));
    }

    #[test]
    fn login_environment_probe_prefers_bash_or_zsh_login_shells() {
        let command = login_environment_probe_command();
        assert!(command.contains("*/zsh|*/bash"));
        assert!(command.contains("command -v zsh"));
        assert!(command.contains("command -v bash"));
        assert!(command.contains("-lic"));
        assert!(command.contains("cd -- \"$HOME\""));
        assert!(command.contains("TEMPS_AGENT_RUNTIME_LOGIN_ENV"));
    }

    #[test]
    fn parses_login_environment_after_shell_startup_output() {
        let output = b"welcome from zshrc\n\0TEMPS_AGENT_RUNTIME_LOGIN_ENV\0/Users/agent\0/opt/homebrew/bin:/Users/agent/.bun/bin:/usr/bin:/bin\0/bin/zsh\0";
        assert_eq!(
            parse_login_environment(output).unwrap(),
            login_environment()
        );
    }

    #[test]
    fn readiness_resolves_the_bare_program_with_the_login_path() {
        let command = build_executable_resolution_command("claude", &login_environment());
        assert!(command.contains("'PATH=/opt/homebrew/bin:/Users/agent/.bun/bin:/usr/bin:/bin'"));
        assert!(command.ends_with("runtime-probe 'claude'"));
        assert!(command.contains("command -v"));
    }

    #[test]
    fn version_probe_uses_the_resolved_executable() {
        let command = build_version_command("/opt/homebrew/bin/claude", &login_environment());
        assert!(command.ends_with("'/opt/homebrew/bin/claude' --version"));
    }

    #[test]
    fn directory_autocomplete_quotes_remote_input_and_is_bounded() {
        let command =
            build_directory_suggestion_command("/Users/agent/proj'; touch /tmp/escaped; '", 20);
        assert!(command.contains(
            "runtime-directories '/Users/agent/proj'\"'\"'; touch /tmp/escaped; '\"'\"'' 20"
        ));
        assert!(command.contains("test \"$count\" -lt \"$limit\" || break"));
    }

    #[test]
    fn parses_nul_delimited_remote_directory_candidates() {
        let output = b"shell startup\n\0TEMPS_AGENT_RUNTIME_DIRECTORIES\0/Users/agent\x001\0/Users/agent\0/Users/agent/projects\0/Users/agent/private projects\0";
        let candidates = parse_directory_candidates(output, 20).unwrap();
        assert_eq!(candidates.home, Path::new("/Users/agent"));
        assert!(candidates.exact_match);
        assert_eq!(
            candidates.directories,
            [
                PathBuf::from("/Users/agent"),
                PathBuf::from("/Users/agent/projects"),
                PathBuf::from("/Users/agent/private projects")
            ]
        );
    }

    #[test]
    fn remote_command_does_not_assign_zsh_read_only_status() {
        let request = TransportSpawnRequest {
            command: CommandSpec::new("claude"),
            working_directory: PathBuf::from("/Users/agent/project"),
        };
        let rendered =
            build_remote_command(&request, "/tmp/temps-agent-runtime.A1b2C3d4E5").unwrap();
        assert!(rendered.contains("runtime_exit_code=$?"));
        assert!(!rendered.contains("; status=$?"));
    }

    #[test]
    fn parses_only_private_mktemp_control_directories() {
        let output = b"startup noise\n\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\0";
        assert_eq!(
            parse_control_directory(output).unwrap(),
            "/tmp/temps-agent-runtime.A1b2C3d4E5"
        );

        let escaped =
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/../temps-agent-runtime.A1b2C3d4E5\0";
        assert!(parse_control_directory(escaped).is_err());
        for invalid in [
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/./temps-agent-runtime.A1b2C3d4E5\0"
                .as_slice(),
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp\\other/temps-agent-runtime.A1b2C3d4E5\0",
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\r\0",
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\n\0",
        ] {
            assert!(parse_control_directory(invalid).is_err());
        }
        let predictable =
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime-test.pid\0";
        assert!(parse_control_directory(predictable).is_err());
    }

    #[test]
    fn installed_harness_remains_available_when_version_probe_times_out() {
        let readiness = readiness_from_version_probe(
            Provider::Claude,
            "/opt/homebrew/bin/claude".into(),
            Err(TransportError::new(
                TransportErrorKind::ConnectionTimedOut,
                "ssh",
                "version",
                "deadline exceeded",
                true,
            )),
        );
        assert!(readiness.installed);
        assert_eq!(
            readiness.executable.as_deref(),
            Some(Path::new("/opt/homebrew/bin/claude"))
        );
        assert_eq!(readiness.version, None);
        assert!(readiness
            .detail
            .contains("optional version probe timed out"));
    }

    #[test]
    fn classifies_actionable_ssh_failures() {
        let error = classify_ssh_diagnostic("wait", b"Permission denied (publickey).");
        assert_eq!(error.kind, TransportErrorKind::AuthenticationFailed);
        assert!(!error.retryable);

        let error = classify_ssh_diagnostic("wait", b"ssh: connect to host x: Connection refused");
        assert_eq!(error.kind, TransportErrorKind::ConnectionRefused);
        assert!(error.retryable);
    }

    #[cfg(unix)]
    #[test]
    fn password_authentication_uses_redacted_askpass_instead_of_argv() {
        let password = "correct horse battery staple";
        let transport = SshTransport::builder("worker.example")
            .user("agent")
            .password(SecretString::new(password))
            .build()
            .expect("password transport");
        let command = transport.base_command();
        let standard = command.as_std();
        let arguments = standard
            .get_args()
            .map(|argument| argument.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(!arguments.contains(password));
        assert!(arguments.contains("PreferredAuthentications=password"));
        assert!(arguments.contains("agent@worker.example"));
        let environment = standard
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            environment.get(ASKPASS_PASSWORD_ENV),
            Some(&Some(password.to_string()))
        );
        assert!(environment.contains_key("SSH_ASKPASS"));
        assert!(!format!("{transport:?}").contains(password));
    }

    #[test]
    fn password_authentication_requires_an_explicit_user() {
        let error = SshTransport::builder("worker.example")
            .password(SecretString::new("secret"))
            .build()
            .expect_err("missing user must fail");
        assert_eq!(error.kind, TransportErrorKind::InvalidConfiguration);
        assert!(error.message.contains("explicit user"));
        assert!(!error.message.contains("secret"));
    }
}
