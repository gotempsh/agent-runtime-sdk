//! Shared machinery for transports that reach a POSIX shell through a local
//! carrier command: `ssh host …` ([`crate::SshTransport`]) or
//! `docker exec -i container …` ([`crate::DockerTransport`]).
//!
//! The carrier only decides how a remote shell command is started and how its
//! failures are classified. Everything that happens inside the shell — the
//! login environment probe, the private launcher that carries secrets out of
//! argv, the pid lease used to terminate the whole process group, readiness
//! and working-directory checks — is identical for every carrier and lives
//! here.

use std::any::Any;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, Command};
use tokio::sync::OnceCell;

use crate::{
    Provider, ProviderReadiness, SandboxCapabilities, TransportCapabilities, TransportError,
    TransportErrorKind, TransportExitStatus, TransportProcess, TransportProcessControl,
    TransportProcessHandle, TransportReader, TransportReadinessRequest, TransportResult,
    TransportSpawnRequest, TransportWriter, WorkingDirectoryCandidates, WorkingDirectoryQuery,
};

const STDERR_CAPTURE_BYTES: usize = 32 * 1024;
pub(crate) const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
static NEXT_PROCESS_ID: AtomicU64 = AtomicU64::new(1);
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
    "PI_CODING_AGENT_DIR",
    "PI_CODING_AGENT_SESSION_DIR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

/// How a remote shell is reached from the SDK host.
pub(crate) trait ShellCarrier: Clone + Debug + Send + Sync + 'static {
    /// Stable transport name used in errors and process handles.
    fn transport_name(&self) -> &'static str;

    /// Human name of the place commands run in, for readiness details.
    fn location(&self) -> &'static str;

    /// A local command that runs `remote_command` in a POSIX shell at the
    /// destination. `loopback_ports` are ports the remote process serves that
    /// the SDK host must reach; carriers that cannot forward them inline
    /// ignore them here and arrange forwarding in their own `spawn`.
    fn shell_command(&self, remote_command: &str, loopback_ports: &[u16]) -> Command;

    /// Deadline for short control operations (probes, staging, kill).
    fn control_deadline(&self) -> Duration;

    /// Classify a failure to start the local carrier command.
    fn classify_local_error(
        &self,
        operation: &'static str,
        error: std::io::Error,
    ) -> TransportError;

    /// A carrier-level failure (as opposed to the remote command's own exit
    /// status), or `None` when the remote command ran.
    fn carrier_failure(
        &self,
        operation: &'static str,
        status: std::process::ExitStatus,
        stderr: &[u8],
    ) -> Option<TransportError>;
}

/// Login environment of the remote shell user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteLoginEnvironment {
    pub(crate) home: String,
    pub(crate) path: String,
    pub(crate) shell: String,
}

/// Shell operations shared by every carrier.
#[derive(Debug, Clone)]
pub(crate) struct RemoteShell<C: ShellCarrier> {
    pub(crate) carrier: C,
    intrinsic_sandbox: SandboxCapabilities,
    login_environment: Arc<OnceCell<RemoteLoginEnvironment>>,
}

impl<C: ShellCarrier> RemoteShell<C> {
    pub(crate) fn new(carrier: C, intrinsic_sandbox: SandboxCapabilities) -> Self {
        Self {
            carrier,
            intrinsic_sandbox,
            login_environment: Arc::new(OnceCell::new()),
        }
    }

    fn name(&self) -> &'static str {
        self.carrier.transport_name()
    }

    pub(crate) fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            remote: true,
            interactive_stdin: true,
            reconnect: false,
            managed_processes: true,
            process_tree_termination: true,
            sandbox: self.intrinsic_sandbox,
        }
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
                    let location = self.carrier.location();
                    let (kind, message) = if output.status.code() == Some(127) {
                        (
                            TransportErrorKind::Unsupported,
                            format!("the {location} user needs Bash or Zsh to resolve its login PATH"),
                        )
                    } else {
                        let diagnostic = bounded_diagnostic(&output.stderr);
                        let message = if diagnostic.is_empty() {
                            format!("the {location} user's login shell could not resolve HOME and PATH")
                        } else {
                            format!(
                                "the {location} user's login shell could not resolve HOME and PATH: {diagnostic}"
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
                parse_login_environment(self.name(), &output.stdout)
            })
            .await
    }

    pub(crate) async fn output(
        &self,
        operation: &'static str,
        remote_command: &str,
    ) -> TransportResult<std::process::Output> {
        self.output_with_timeout(operation, remote_command, self.carrier.control_deadline())
            .await
    }

    async fn output_with_timeout(
        &self,
        operation: &'static str,
        remote_command: &str,
        timeout: Duration,
    ) -> TransportResult<std::process::Output> {
        let mut command = self.carrier.shell_command(remote_command, &[]);
        command
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        let output = tokio::time::timeout(timeout, command.output())
            .await
            .map_err(|_| {
                TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.name(),
                    operation,
                    format!("{} operation exceeded its deadline", self.name()),
                    true,
                )
            })?
            .map_err(|error| self.carrier.classify_local_error(operation, error))?;
        if let Some(error) = self
            .carrier
            .carrier_failure(operation, output.status, &output.stderr)
        {
            return Err(error);
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
        let mut command = self.carrier.shell_command(remote_command, &[]);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| self.carrier.classify_local_error("stage_launcher", error))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| stream_setup_error(self.name(), "stdin"))?;
        let operation = async move {
            stdin.write_all(launcher).await?;
            stdin.shutdown().await?;
            drop(stdin);
            child.wait_with_output().await
        };
        let output = tokio::time::timeout(self.carrier.control_deadline(), operation)
            .await
            .map_err(|_| {
                TransportError::new(
                    TransportErrorKind::ConnectionTimedOut,
                    self.name(),
                    "stage_launcher",
                    format!("{} launcher staging exceeded its deadline", self.name()),
                    true,
                )
            })?
            .map_err(|error| self.carrier.classify_local_error("stage_launcher", error))?;
        if let Some(error) =
            self.carrier
                .carrier_failure("stage_launcher", output.status, &output.stderr)
        {
            return Err(error);
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
        parse_control_directory(self.name(), &output.stdout)
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
        // The launcher is a process-group leader over SSH, but not under
        // `docker exec`, which starts it in the exec session's group; so the
        // descendants are also collected from `/proc` (where it exists) before
        // the first signal, while they are still parented to the launcher.
        let command = format!(
            "descendants() {{ all=$1; [ -d /proc ] || {{ echo \"$all\"; return 0; }}; changed=1; \
               while [ \"$changed\" = 1 ]; do changed=0; \
                 for stat in /proc/[0-9]*/stat; do \
                   {{ IFS= read -r line < \"$stat\"; }} 2>/dev/null || continue; \
                   child=${{stat#/proc/}}; child=${{child%/stat}}; \
                   set -- ${{line##*) }}; \
                   case \" $all \" in (*\" $child \"*) continue;; esac; \
                   case \" $all \" in (*\" $2 \"*) all=\"$all $child\"; changed=1;; esac; \
                 done; \
               done; echo \"$all\"; }}; \
             i=0; while [ ! -s {pid_file} ] && [ \"$i\" -lt 20 ]; do sleep 0.05; i=$((i + 1)); done; \
             if IFS= read -r pid < {pid_file}; then \
               case \"$pid\" in (*[!0-9]*|'') exit 64;; esac; \
               tree=$(descendants \"$pid\"); \
               kill -TERM -- -\"$pid\" 2>/dev/null || true; kill -TERM $tree 2>/dev/null || true; sleep 0.2; \
               kill -KILL -- -\"$pid\" 2>/dev/null || true; kill -KILL $tree 2>/dev/null || true; \
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

    pub(crate) async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        let environment = self.login_environment().await?;
        let program = os_to_utf8(self.name(), &request.program, "provider executable")?;
        let resolution_command = build_executable_resolution_command(program, environment);
        let output = self
            .output("resolve_executable", &resolution_command)
            .await?;
        let location = self.carrier.location();
        if output.status.code() == Some(127) {
            return Ok(ProviderReadiness {
                provider: request.provider,
                installed: false,
                executable: None,
                version: None,
                detail: format!(
                    "Install {} inside the {location} and authenticate it as the {location} user.",
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
                format!(
                    "the {location} shell found the provider executable but did not return its path"
                ),
                false,
            ));
        }
        let version_command = build_version_command(&executable, environment);
        let version_output = self
            .output_with_timeout("version", &version_command, VERSION_PROBE_TIMEOUT)
            .await;
        Ok(readiness_from_version_probe(
            request.provider,
            location,
            executable,
            version_output,
        ))
    }

    pub(crate) async fn validate_working_directory(
        &self,
        working_directory: &Path,
    ) -> TransportResult<()> {
        let directory = os_to_utf8(self.name(), working_directory, "working directory")?;
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
                format!(
                    "{directory} is not a directory in the {}",
                    self.carrier.location()
                ),
                false,
            ))
        }
    }

    pub(crate) async fn suggest_working_directories(
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
        parse_directory_candidates(self.name(), &output.stdout, query.limit)
    }

    /// Start the request's process. `keepalive` is held until the process
    /// ends — a carrier keeps per-process resources (port tunnels) in it.
    pub(crate) async fn spawn(
        &self,
        request: TransportSpawnRequest,
        keepalive: Option<Box<dyn Any + Send + Sync>>,
    ) -> TransportResult<TransportProcess> {
        let native_id = format!(
            "{}-{}-{}",
            std::process::id(),
            now_millis(),
            NEXT_PROCESS_ID.fetch_add(1, Ordering::Relaxed)
        );
        let environment = self.login_environment().await?;
        let launcher = build_remote_launcher(self.name(), &request, environment)?;
        let control_directory = self.stage_remote_launcher(launcher.as_bytes()).await?;
        let pid_file = format!("{control_directory}/pid");
        let remote_command = build_remote_command(self.name(), &request, &control_directory)?;
        let mut command = self
            .carrier
            .shell_command(&remote_command, &request.command.loopback_ports);
        command
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
                return Err(self.carrier.classify_local_error("spawn", error));
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
            .ok_or_else(|| stream_setup_error(self.name(), "stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| stream_setup_error(self.name(), "stderr"))?;
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
            ShellProcessControl {
                child,
                process_tree,
                stderr_capture,
                shell: self.clone(),
                pid_file,
                control_directory,
                remote_finished: false,
                _keepalive: keepalive,
            },
        ))
    }
}

struct ShellProcessControl<C: ShellCarrier> {
    child: Child,
    process_tree: crate::process::ProcessTreeGuard,
    stderr_capture: Arc<Mutex<Vec<u8>>>,
    shell: RemoteShell<C>,
    pid_file: String,
    control_directory: String,
    remote_finished: bool,
    _keepalive: Option<Box<dyn Any + Send + Sync>>,
}

#[async_trait::async_trait]
impl<C: ShellCarrier> TransportProcessControl for ShellProcessControl<C> {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        let status = self
            .child
            .wait()
            .await
            .map_err(|error| self.shell.carrier.classify_local_error("wait", error))?;
        let stderr = self
            .stderr_capture
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(error) = self.shell.carrier.carrier_failure("wait", status, &stderr) {
            return Err(error);
        }
        self.remote_finished = true;
        Ok(status.into())
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        let remote_result = self.shell.terminate_remote(&self.pid_file).await;
        self.shell
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

impl<C: ShellCarrier> Drop for ShellProcessControl<C> {
    fn drop(&mut self) {
        if self.remote_finished {
            return;
        }
        self.remote_finished = true;
        let shell = self.shell.clone();
        let pid_file = self.pid_file.clone();
        let control_directory = self.control_directory.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = shell.terminate_remote(&pid_file).await;
                shell
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

pub(crate) fn build_remote_command(
    transport: &'static str,
    request: &TransportSpawnRequest,
    control_directory: &str,
) -> TransportResult<String> {
    let directory = os_to_utf8(transport, &request.working_directory, "working directory")?;
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

pub(crate) fn build_remote_launcher(
    transport: &'static str,
    request: &TransportSpawnRequest,
    login_environment: &RemoteLoginEnvironment,
) -> TransportResult<String> {
    let program = os_to_utf8(transport, &request.command.program, "program")?;
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
    append_environment(transport, &mut invocation, &request.command.environment)?;
    invocation.push(' ');
    invocation.push_str(&quote_posix(program));
    for argument in &request.command.args {
        invocation.push(' ');
        invocation.push_str(&quote_posix(os_string_to_utf8(
            transport, argument, "argument",
        )?));
    }
    invocation.push('\n');
    Ok(invocation)
}

pub(crate) fn parse_control_directory(
    transport: &'static str,
    stdout: &[u8],
) -> TransportResult<String> {
    let protocol = |message: &str| {
        TransportError::new(
            TransportErrorKind::Protocol,
            transport,
            "stage_launcher",
            message,
            false,
        )
    };
    let marker = stdout
        .windows(CONTROL_DIRECTORY_MARKER.len())
        .rposition(|window| window == CONTROL_DIRECTORY_MARKER)
        .ok_or_else(|| protocol("the remote shell omitted its launcher control directory"))?;
    let payload = &stdout[marker + CONTROL_DIRECTORY_MARKER.len()..];
    let end = payload.iter().position(|byte| *byte == 0).ok_or_else(|| {
        protocol("the remote shell returned an unterminated launcher control directory")
    })?;
    let directory = std::str::from_utf8(&payload[..end]).map_err(|_| {
        protocol("the remote shell returned a non-UTF-8 launcher control directory")
    })?;
    // The directory belongs to a POSIX shell even when this client runs on Windows.
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
        return Err(protocol(
            "the remote shell returned an invalid launcher control directory",
        ));
    }
    Ok(directory.to_string())
}

pub(crate) fn build_executable_resolution_command(
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

pub(crate) fn build_version_command(
    executable: &str,
    environment: &RemoteLoginEnvironment,
) -> String {
    format!(
        "env -i {home} {path} {shell} {executable} --version",
        home = quote_posix(&format!("HOME={}", environment.home)),
        path = quote_posix(&format!("PATH={}", environment.path)),
        shell = quote_posix(&format!("SHELL={}", environment.shell)),
        executable = quote_posix(executable),
    )
}

pub(crate) fn build_directory_suggestion_command(input: &str, limit: usize) -> String {
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

pub(crate) fn parse_directory_candidates(
    transport: &'static str,
    stdout: &[u8],
    limit: usize,
) -> TransportResult<WorkingDirectoryCandidates> {
    let marker = stdout
        .windows(DIRECTORY_CANDIDATES_MARKER.len())
        .rposition(|window| window == DIRECTORY_CANDIDATES_MARKER)
        .ok_or_else(|| {
            TransportError::new(
                TransportErrorKind::Protocol,
                transport,
                "suggest_working_directories",
                "the remote shell omitted its directory response marker",
                false,
            )
        })?;
    let payload = &stdout[marker + DIRECTORY_CANDIDATES_MARKER.len()..];
    let mut fields = payload.split(|byte| *byte == 0);
    let home = parse_directory_field(transport, fields.next(), "home")?;
    let exact_match = match fields.next() {
        Some(b"1") => true,
        Some(b"0") => false,
        _ => {
            return Err(TransportError::new(
                TransportErrorKind::Protocol,
                transport,
                "suggest_working_directories",
                "the remote shell returned an invalid exact-match flag",
                false,
            ));
        }
    };
    let directories = fields
        .filter(|field| !field.is_empty())
        .take(limit)
        .map(|field| parse_directory_field(transport, Some(field), "directory").map(PathBuf::from))
        .collect::<TransportResult<Vec<_>>>()?;
    Ok(WorkingDirectoryCandidates {
        home: PathBuf::from(home),
        directories,
        exact_match,
    })
}

fn parse_directory_field(
    transport: &'static str,
    value: Option<&[u8]>,
    name: &'static str,
) -> TransportResult<String> {
    let protocol = |message: String| {
        TransportError::new(
            TransportErrorKind::Protocol,
            transport,
            "suggest_working_directories",
            message,
            false,
        )
    };
    let value = value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| protocol(format!("the remote shell omitted the {name} field")))?;
    if value.len() > 4_096 {
        return Err(protocol(format!(
            "the remote shell returned an oversized {name} field"
        )));
    }
    String::from_utf8(value.to_vec())
        .map_err(|_| protocol(format!("the remote shell returned non-UTF-8 {name}")))
}

pub(crate) fn readiness_from_version_probe(
    provider: Provider,
    location: &str,
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
                format!("The CLI executable is available inside the {location}; authentication is checked when a turn starts."),
            )
        }
        Ok(_) => (
            None,
            format!("The CLI executable is available inside the {location}; its optional version probe failed. Authentication is checked when a turn starts."),
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

pub(crate) fn login_environment_probe_command() -> String {
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

pub(crate) fn parse_login_environment(
    transport: &'static str,
    stdout: &[u8],
) -> TransportResult<RemoteLoginEnvironment> {
    let marker = stdout
        .windows(LOGIN_ENVIRONMENT_MARKER.len())
        .rposition(|window| window == LOGIN_ENVIRONMENT_MARKER)
        .ok_or_else(|| {
            login_environment_protocol_error(
                transport,
                "the login shell omitted its environment marker",
            )
        })?;
    let payload = &stdout[marker + LOGIN_ENVIRONMENT_MARKER.len()..];
    let mut fields = payload.split(|byte| *byte == 0);
    let home = parse_login_environment_field(transport, fields.next(), "HOME")?;
    let path = parse_login_environment_field(transport, fields.next(), "PATH")?;
    let shell = parse_login_environment_field(transport, fields.next(), "SHELL")?;
    if !matches!(
        Path::new(&shell).file_name().and_then(OsStr::to_str),
        Some("bash" | "zsh")
    ) {
        return Err(login_environment_protocol_error(
            transport,
            "the login environment did not report Bash or Zsh",
        ));
    }
    Ok(RemoteLoginEnvironment { home, path, shell })
}

fn parse_login_environment_field(
    transport: &'static str,
    value: Option<&[u8]>,
    name: &'static str,
) -> TransportResult<String> {
    let value = value.filter(|value| !value.is_empty()).ok_or_else(|| {
        login_environment_protocol_error(transport, &format!("the login shell omitted {name}"))
    })?;
    if value.len() > 64 * 1024 {
        return Err(login_environment_protocol_error(
            transport,
            &format!("the login shell returned an oversized {name}"),
        ));
    }
    String::from_utf8(value.to_vec()).map_err(|_| {
        login_environment_protocol_error(
            transport,
            &format!("the login shell returned non-UTF-8 {name}"),
        )
    })
}

fn login_environment_protocol_error(transport: &'static str, message: &str) -> TransportError {
    TransportError::new(
        TransportErrorKind::Protocol,
        transport,
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
    transport: &'static str,
    output: &mut String,
    environment: &BTreeMap<OsString, OsString>,
) -> TransportResult<()> {
    for (name, value) in environment {
        let name = os_string_to_utf8(transport, name, "environment variable name")?;
        if !valid_environment_name(name) {
            return Err(TransportError::new(
                TransportErrorKind::InvalidConfiguration,
                transport,
                "spawn",
                format!("invalid environment variable name {name:?}"),
                false,
            ));
        }
        let value = os_string_to_utf8(transport, value, "environment variable value")?;
        output.push(' ');
        output.push_str(&quote_posix(&format!("{name}={value}")));
    }
    Ok(())
}

pub(crate) fn quote_posix(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && chars.all(|character| matches!(character, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

pub(crate) fn os_to_utf8<'a>(
    transport: &'static str,
    value: &'a Path,
    field: &'static str,
) -> TransportResult<&'a str> {
    value.to_str().ok_or_else(|| invalid_utf8(transport, field))
}

fn os_string_to_utf8<'a>(
    transport: &'static str,
    value: &'a OsStr,
    field: &'static str,
) -> TransportResult<&'a str> {
    value.to_str().ok_or_else(|| invalid_utf8(transport, field))
}

fn invalid_utf8(transport: &'static str, field: &'static str) -> TransportError {
    TransportError::new(
        TransportErrorKind::InvalidConfiguration,
        transport,
        "configure",
        format!("{field} must be valid UTF-8 for remote execution"),
        false,
    )
}

fn stream_setup_error(transport: &'static str, stream: &'static str) -> TransportError {
    TransportError::new(
        TransportErrorKind::StreamFailed,
        transport,
        "spawn",
        format!("{transport} {stream} was not piped"),
        false,
    )
}

pub(crate) fn bounded_diagnostic(bytes: &[u8]) -> String {
    let start = bytes.len().saturating_sub(8 * 1024);
    String::from_utf8_lossy(&bytes[start..])
        .trim()
        .replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommandSpec;

    pub(crate) fn login_environment() -> RemoteLoginEnvironment {
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
        let launcher = build_remote_launcher("ssh", &request, &login_environment())
            .expect("render remote launcher");
        let rendered = build_remote_command("ssh", &request, "/tmp/temps-agent-runtime.A1b2C3d4E5")
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
            parse_login_environment("ssh", output).unwrap(),
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
        let candidates = parse_directory_candidates("ssh", output, 20).unwrap();
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
            build_remote_command("ssh", &request, "/tmp/temps-agent-runtime.A1b2C3d4E5").unwrap();
        assert!(rendered.contains("runtime_exit_code=$?"));
        assert!(!rendered.contains("; status=$?"));
    }

    #[test]
    fn parses_only_private_mktemp_control_directories() {
        let output = b"startup noise\n\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\0";
        assert_eq!(
            parse_control_directory("ssh", output).unwrap(),
            "/tmp/temps-agent-runtime.A1b2C3d4E5"
        );

        let escaped =
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/../temps-agent-runtime.A1b2C3d4E5\0";
        assert!(parse_control_directory("ssh", escaped).is_err());
        for invalid in [
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/./temps-agent-runtime.A1b2C3d4E5\0"
                .as_slice(),
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp\\other/temps-agent-runtime.A1b2C3d4E5\0",
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\r\0",
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime.A1b2C3d4E5\n\0",
        ] {
            assert!(parse_control_directory("ssh", invalid).is_err());
        }
        let predictable =
            b"\0TEMPS_AGENT_RUNTIME_CONTROL_DIRECTORY\0/tmp/temps-agent-runtime-test.pid\0";
        assert!(parse_control_directory("ssh", predictable).is_err());
    }

    #[test]
    fn installed_harness_remains_available_when_version_probe_times_out() {
        let readiness = readiness_from_version_probe(
            Provider::Claude,
            "SSH destination",
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
}
