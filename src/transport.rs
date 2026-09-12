//! Pluggable local or remote process execution.
//!
//! Provider adapters describe a command and parse its protocol. An execution
//! transport decides where that command exists and runs: the local host, a
//! managed sandbox, an SSH-backed worker, or another remote environment.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::process::Child;

use crate::{CommandSpec, Provider, ProviderReadiness, SandboxCapabilities};

/// Result returned by execution transports.
pub type TransportResult<T> = std::result::Result<T, TransportError>;

/// Stable category applications can use to present a recovery action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// The transport configuration is incomplete or internally inconsistent.
    InvalidConfiguration,
    /// Authentication material was rejected or is unavailable.
    AuthenticationFailed,
    /// The remote host key is unknown or does not match the trusted key.
    HostKeyVerificationFailed,
    /// The remote host name could not be resolved.
    NameResolutionFailed,
    /// The remote endpoint actively refused the connection.
    ConnectionRefused,
    /// Establishing or using the connection exceeded its deadline.
    ConnectionTimedOut,
    /// The network or remote execution service is unavailable.
    RemoteUnavailable,
    /// The requested executable does not exist in the execution environment.
    ExecutableNotFound,
    /// The requested working directory does not exist in the execution environment.
    WorkingDirectoryNotFound,
    /// The remote identity lacks permission for the requested operation.
    PermissionDenied,
    /// A process could not be created after the transport connected.
    SpawnFailed,
    /// A process stream could not be read or written.
    StreamFailed,
    /// Waiting for, attaching to, or terminating a process failed.
    ProcessControlFailed,
    /// The remote service returned a malformed or unsupported response.
    Protocol,
    /// The requested operation is not implemented by this transport.
    Unsupported,
}

/// Typed transport failure with a bounded user-facing explanation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("{transport} {operation} failed ({kind:?}): {message}")]
pub struct TransportError {
    /// Stable category suitable for application control flow.
    pub kind: TransportErrorKind,
    /// Stable transport name.
    pub transport: String,
    /// Short operation name such as `connect`, `spawn`, or `terminate`.
    pub operation: String,
    /// Bounded diagnostic that must not contain credentials.
    pub message: String,
    /// Whether retrying without changing configuration may succeed.
    pub retryable: bool,
}

impl TransportError {
    /// Construct a typed transport failure.
    pub fn new(
        kind: TransportErrorKind,
        transport: impl Into<String>,
        operation: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            kind,
            transport: transport.into(),
            operation: operation.into(),
            message: message.into(),
            retryable,
        }
    }

    fn local(operation: &'static str, error: io::Error) -> Self {
        let kind = match error.kind() {
            io::ErrorKind::NotFound => TransportErrorKind::ExecutableNotFound,
            io::ErrorKind::PermissionDenied => TransportErrorKind::PermissionDenied,
            io::ErrorKind::TimedOut => TransportErrorKind::ConnectionTimedOut,
            _ => TransportErrorKind::SpawnFailed,
        };
        Self::new(kind, "local", operation, error.to_string(), false)
    }
}

/// Boxed readable byte stream returned by a transport.
pub type TransportReader = Box<dyn AsyncRead + Send + Unpin>;

/// Boxed writable byte stream returned by a transport.
pub type TransportWriter = Box<dyn AsyncWrite + Send + Unpin>;

/// Stable native identity for a process in an execution transport.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransportProcessHandle {
    /// Stable transport name.
    pub transport: String,
    /// Transport-native process, command, or session identifier.
    pub native_id: String,
}

/// Process features implemented by one configured transport.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportCapabilities {
    /// The executable runs outside the SDK host operating system.
    pub remote: bool,
    /// The process accepts writes after its initial stdin payload.
    pub interactive_stdin: bool,
    /// A process can be reattached using its native handle.
    pub reconnect: bool,
    /// Long-running commands can remain owned beyond one agent turn.
    pub managed_processes: bool,
    /// Termination covers provider tool descendants, not only the immediate process.
    pub process_tree_termination: bool,
    /// Isolation controls enforced intrinsically by the execution environment.
    pub sandbox: SandboxCapabilities,
}

/// Provider executable probe performed in the selected execution environment.
#[derive(Debug, Clone)]
pub struct TransportReadinessRequest {
    /// Provider being inspected.
    pub provider: Provider,
    /// Executable name or path meaningful inside the selected transport.
    pub program: PathBuf,
}

/// Bounded, transport-scoped working-directory autocomplete request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingDirectoryQuery {
    /// User-entered absolute, home-relative, or transport-relative path prefix.
    pub input: String,
    /// Maximum number of directory candidates to return.
    pub limit: usize,
}

/// Working-directory candidates resolved inside the selected transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkingDirectoryCandidates {
    /// Home directory of the identity executing commands in this transport.
    pub home: PathBuf,
    /// Existing directories matching the input prefix. When the entered path exists,
    /// that directory is the first candidate, followed by its bounded children.
    pub directories: Vec<PathBuf>,
    /// Whether the entered path itself resolves to an existing directory.
    pub exact_match: bool,
}

/// Complete request to launch a process through a transport.
#[derive(Clone)]
pub struct TransportSpawnRequest {
    /// Separated executable, arguments, environment, and stdin contract.
    pub command: CommandSpec,
    /// Working directory meaningful inside the selected transport.
    pub working_directory: PathBuf,
}

impl fmt::Debug for TransportSpawnRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportSpawnRequest")
            .field("program", &self.command.program)
            .field("args", &self.command.args)
            .field(
                "environment_keys",
                &self.command.environment.keys().collect::<Vec<_>>(),
            )
            .field("clear_environment", &self.command.clear_environment)
            .field(
                "initial_stdin_bytes",
                &self.command.initial_stdin.as_ref().map(Vec::len),
            )
            .field("interactive_stdin", &self.command.interactive_stdin)
            .field("working_directory", &self.working_directory)
            .finish()
    }
}

/// Provider-neutral exit status returned by a local or remote process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportExitStatus {
    /// True when the transport reports successful completion.
    pub success: bool,
    /// Native exit code, or `None` when unavailable or signal-terminated.
    pub code: Option<i32>,
}

impl From<ExitStatus> for TransportExitStatus {
    fn from(status: ExitStatus) -> Self {
        Self {
            success: status.success(),
            code: status.code(),
        }
    }
}

/// Lifecycle control implemented by a transport-specific process.
///
/// Dropping a live control object must initiate best-effort cleanup. Remote
/// transports should additionally use leases because Rust cannot perform an
/// asynchronous network request from `Drop`.
#[async_trait]
pub trait TransportProcessControl: Send {
    /// Wait for terminal process status.
    async fn wait(&mut self) -> TransportResult<TransportExitStatus>;

    /// Request termination of the complete owned process tree.
    async fn terminate(&mut self) -> TransportResult<()>;

    /// Stop cleanup after a natural provider exit when detached descendants
    /// are intentionally preserved.
    fn disarm(&mut self) {}
}

/// Spawned process streams plus lifecycle control.
pub struct TransportProcess {
    handle: TransportProcessHandle,
    pid: Option<u32>,
    stdin: Option<TransportWriter>,
    stdout: Option<TransportReader>,
    stderr: Option<TransportReader>,
    control: Box<dyn TransportProcessControl>,
}

impl TransportProcess {
    /// Construct a process returned by a custom transport.
    pub fn new(
        handle: TransportProcessHandle,
        pid: Option<u32>,
        stdin: Option<TransportWriter>,
        stdout: TransportReader,
        stderr: TransportReader,
        control: impl TransportProcessControl + 'static,
    ) -> Self {
        Self {
            handle,
            pid,
            stdin,
            stdout: Some(stdout),
            stderr: Some(stderr),
            control: Box::new(control),
        }
    }

    /// Stable native process identity.
    pub fn handle(&self) -> &TransportProcessHandle {
        &self.handle
    }

    /// Local PID when the transport exposes one.
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Take the writable stdin stream.
    pub fn take_stdin(&mut self) -> Option<TransportWriter> {
        self.stdin.take()
    }

    /// Take the readable stdout stream.
    pub fn take_stdout(&mut self) -> Option<TransportReader> {
        self.stdout.take()
    }

    /// Take the readable stderr stream.
    pub fn take_stderr(&mut self) -> Option<TransportReader> {
        self.stderr.take()
    }

    /// Wait for process completion.
    pub async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        self.control.wait().await
    }

    /// Terminate the complete owned process tree.
    pub async fn terminate(&mut self) -> TransportResult<()> {
        self.control.terminate().await
    }

    /// Preserve detached descendants after a natural provider exit.
    pub fn disarm(&mut self) {
        self.control.disarm();
    }
}

/// Execution boundary for local hosts, remote workers, and managed sandboxes.
#[async_trait]
pub trait ExecutionTransport: Send + Sync {
    /// Stable diagnostic and persistence name.
    fn name(&self) -> &'static str;

    /// Features and intrinsic isolation provided by this configured transport.
    fn capabilities(&self) -> TransportCapabilities;

    /// Probe an executable inside this transport's environment.
    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness>;

    /// Validate a working directory inside this transport's filesystem.
    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()>;

    /// Suggest existing working directories without scanning recursively.
    async fn suggest_working_directories(
        &self,
        _query: WorkingDirectoryQuery,
    ) -> TransportResult<WorkingDirectoryCandidates> {
        Err(TransportError::new(
            TransportErrorKind::Unsupported,
            self.name(),
            "suggest_working_directories",
            "working-directory autocomplete is not supported",
            false,
        ))
    }

    /// Spawn one process with streaming stdin, stdout, and stderr.
    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess>;

    /// Reattach to an existing native process and resume after an optional
    /// transport-native event cursor.
    async fn attach(
        &self,
        _handle: &TransportProcessHandle,
        _cursor: Option<u64>,
    ) -> TransportResult<TransportProcess> {
        Err(TransportError::new(
            TransportErrorKind::Unsupported,
            self.name(),
            "attach",
            "process reattachment is not supported",
            false,
        ))
    }
}

/// Default transport that starts child processes on the SDK host.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalTransport;

#[async_trait]
impl ExecutionTransport for LocalTransport {
    fn name(&self) -> &'static str {
        "local"
    }

    fn capabilities(&self) -> TransportCapabilities {
        TransportCapabilities {
            interactive_stdin: true,
            managed_processes: true,
            process_tree_termination: true,
            ..TransportCapabilities::default()
        }
    }

    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        Ok(inspect_local_executable(request.provider, &request.program).await)
    }

    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()> {
        if working_directory.is_dir() {
            Ok(())
        } else {
            Err(TransportError::new(
                TransportErrorKind::WorkingDirectoryNotFound,
                self.name(),
                "validate_working_directory",
                format!("{} is not a directory", working_directory.display()),
                false,
            ))
        }
    }

    async fn suggest_working_directories(
        &self,
        query: WorkingDirectoryQuery,
    ) -> TransportResult<WorkingDirectoryCandidates> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        suggest_local_directories(&home, &query).await
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        let mut child = crate::process::spawn(&request.command, &request.working_directory)
            .map_err(|error| TransportError::local("spawn", error))?;
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .map(|stream| Box::new(stream) as TransportWriter);
        let stdout = child
            .stdout
            .take()
            .map(|stream| Box::new(stream) as TransportReader)
            .ok_or_else(|| {
                TransportError::new(
                    TransportErrorKind::StreamFailed,
                    self.name(),
                    "spawn",
                    "stdout was not piped",
                    false,
                )
            })?;
        let stderr = child
            .stderr
            .take()
            .map(|stream| Box::new(stream) as TransportReader)
            .ok_or_else(|| {
                TransportError::new(
                    TransportErrorKind::StreamFailed,
                    self.name(),
                    "spawn",
                    "stderr was not piped",
                    false,
                )
            })?;
        let handle = TransportProcessHandle {
            transport: self.name().to_string(),
            native_id: pid.map_or_else(|| "unknown".to_string(), |pid| pid.to_string()),
        };
        let process_tree = crate::process::ProcessTreeGuard::for_child(&child);
        Ok(TransportProcess::new(
            handle,
            pid,
            stdin,
            stdout,
            stderr,
            LocalProcessControl {
                child,
                process_tree,
            },
        ))
    }
}

async fn suggest_local_directories(
    home: &Path,
    query: &WorkingDirectoryQuery,
) -> TransportResult<WorkingDirectoryCandidates> {
    let expanded = expand_directory_input(home, &query.input);
    let exact_match = tokio::fs::metadata(&expanded)
        .await
        .is_ok_and(|metadata| metadata.is_dir());
    let (parent, prefix) = if exact_match {
        (expanded.clone(), String::new())
    } else {
        directory_parent_and_prefix(&expanded, query.input.ends_with('/'))
    };
    let mut directories = Vec::new();
    if exact_match && query.limit > 0 {
        directories.push(expanded.clone());
    }
    if let Ok(mut entries) = tokio::fs::read_dir(&parent).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if directories.len() >= query.limit {
                break;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with(&prefix) {
                continue;
            }
            if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
                directories.push(entry.path());
            }
        }
    }
    if exact_match {
        directories[1..].sort();
    } else {
        directories.sort();
    }
    directories.truncate(query.limit);
    Ok(WorkingDirectoryCandidates {
        home: home.to_path_buf(),
        directories,
        exact_match,
    })
}

fn expand_directory_input(home: &Path, input: &str) -> PathBuf {
    if input.is_empty() || input == "~" {
        home.to_path_buf()
    } else if let Some(relative) = input.strip_prefix("~/") {
        home.join(relative)
    } else {
        let path = PathBuf::from(input);
        if path.is_absolute() {
            path
        } else {
            home.join(path)
        }
    }
}

fn directory_parent_and_prefix(path: &Path, trailing_separator: bool) -> (PathBuf, String) {
    if trailing_separator {
        return (path.to_path_buf(), String::new());
    }
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new("/"))
        .to_path_buf();
    let prefix = path
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    (parent, prefix)
}

struct LocalProcessControl {
    child: Child,
    process_tree: crate::process::ProcessTreeGuard,
}

#[async_trait]
impl TransportProcessControl for LocalProcessControl {
    async fn wait(&mut self) -> TransportResult<TransportExitStatus> {
        self.child
            .wait()
            .await
            .map(Into::into)
            .map_err(|error| TransportError::local("wait", error))
    }

    async fn terminate(&mut self) -> TransportResult<()> {
        self.process_tree.terminate();
        Ok(())
    }

    fn disarm(&mut self) {
        self.process_tree.disarm();
    }
}

async fn inspect_local_executable(provider: Provider, program: &Path) -> ProviderReadiness {
    let resolved = resolve_local_executable(program);
    let Some(path) = resolved else {
        return ProviderReadiness {
            provider,
            installed: false,
            executable: None,
            version: None,
            detail: format!(
                "Install the {provider} CLI in the local transport and authenticate it as the runtime user."
            ),
        };
    };
    let version = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::process::Command::new(&path)
            .arg("--version")
            .output(),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .filter(|output| output.status.success())
    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
    .filter(|version| !version.is_empty());
    ProviderReadiness {
        provider,
        installed: true,
        executable: Some(path),
        version,
        detail: "The CLI executable is available in the local transport; authentication is checked when a turn starts.".to_string(),
    }
}

fn resolve_local_executable(program: &Path) -> Option<PathBuf> {
    if program.components().count() > 1 {
        return program.is_file().then(|| program.to_path_buf());
    }
    let name = program.as_os_str();
    let mut candidates = std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        candidates.extend([
            home.join(".local/bin"),
            home.join(".cargo/bin"),
            home.join(".bun/bin"),
            home.join(".npm-global/bin"),
        ]);
    }
    #[cfg(windows)]
    let names = [
        PathBuf::from(format!("{}.exe", name.to_string_lossy())),
        PathBuf::from(format!("{}.cmd", name.to_string_lossy())),
        PathBuf::from(name),
    ];
    #[cfg(not(windows))]
    let names = [PathBuf::from(name)];
    candidates
        .into_iter()
        .flat_map(|directory| names.iter().map(move |name| directory.join(name)))
        .find(|path| path.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_autocomplete_lists_the_current_directory_then_children_without_recursing() {
        let root = tempfile::tempdir().expect("temporary directory");
        let projects = root.path().join("projects");
        tokio::fs::create_dir_all(projects.join("runtime").join("nested"))
            .await
            .expect("create nested directory");
        tokio::fs::create_dir(projects.join("website"))
            .await
            .expect("create sibling directory");

        let candidates = suggest_local_directories(
            root.path(),
            &WorkingDirectoryQuery {
                input: projects.display().to_string(),
                limit: 20,
            },
        )
        .await
        .expect("suggest directories");

        assert!(candidates.exact_match);
        assert_eq!(
            candidates.directories,
            [
                projects.clone(),
                projects.join("runtime"),
                projects.join("website")
            ]
        );
        assert!(!candidates
            .directories
            .contains(&projects.join("runtime/nested")));
    }
}
