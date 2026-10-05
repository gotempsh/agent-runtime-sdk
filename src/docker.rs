//! Docker-backed execution transport: provider CLIs run inside an existing,
//! already-running container through `docker exec -i`.
//!
//! Provider processes that serve a loopback port the SDK host must reach
//! (`CommandSpec::loopback_ports`, used by `OpenCode::serve()`) are not
//! supported yet and fail with [`TransportErrorKind::Unsupported`].
//!
//! The container's lifecycle (image, mounts, environment, resource limits,
//! start and removal) belongs to the application; this transport only runs
//! commands in it. Everything inside the container — the login environment,
//! the private launcher that keeps secrets out of argv, the pid lease used to
//! stop a whole process group — is the shared remote-shell machinery SSH uses,
//! because killing the local `docker exec` client does not stop the process it
//! started inside the container.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;

use crate::remote_shell::{bounded_diagnostic, RemoteShell, ShellCarrier};
use crate::{
    ExecutionTransport, ProviderReadiness, SandboxCapabilities, TransportCapabilities,
    TransportError, TransportErrorKind, TransportProcess, TransportReadinessRequest,
    TransportResult, TransportSpawnRequest, WorkingDirectoryCandidates, WorkingDirectoryQuery,
};

/// Builder for [`DockerTransport`].
#[derive(Debug, Clone)]
pub struct DockerTransportBuilder {
    container: String,
    user: Option<String>,
    executable: PathBuf,
    command_timeout: Duration,
    intrinsic_sandbox: SandboxCapabilities,
}

impl DockerTransportBuilder {
    /// Create a builder for a running container, by name or id.
    pub fn new(container: impl Into<String>) -> Self {
        Self {
            container: container.into(),
            user: None,
            executable: PathBuf::from("docker"),
            command_timeout: Duration::from_secs(15),
            intrinsic_sandbox: SandboxCapabilities::NONE,
        }
    }

    /// Run commands as this container user (`docker exec -u`).
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Override the local Docker CLI executable.
    pub fn executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = path.into();
        self
    }

    /// Deadline for short control commands (probes, launcher staging, kill).
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Declare the isolation the container itself enforces (its mounts,
    /// network mode and so on), which the runtime reports to callers.
    pub fn intrinsic_sandbox(mut self, capabilities: SandboxCapabilities) -> Self {
        self.intrinsic_sandbox = capabilities;
        self
    }

    /// Validate the configuration and construct a transport.
    pub fn build(self) -> TransportResult<DockerTransport> {
        validate_reference("container", &self.container)?;
        if let Some(user) = self.user.as_deref() {
            validate_reference("user", user)?;
        }
        if self.command_timeout.is_zero() {
            return Err(configuration_error(
                "command_timeout must be greater than zero",
            ));
        }
        Ok(DockerTransport {
            shell: RemoteShell::new(
                DockerCarrier {
                    container: self.container,
                    user: self.user,
                    executable: self.executable,
                    command_timeout: self.command_timeout,
                },
                self.intrinsic_sandbox,
            ),
        })
    }
}

/// Execute provider CLIs and managed processes inside a running container.
#[derive(Debug, Clone)]
pub struct DockerTransport {
    shell: RemoteShell<DockerCarrier>,
}

impl DockerTransport {
    /// Create a builder for a running container, by name or id.
    pub fn builder(container: impl Into<String>) -> DockerTransportBuilder {
        DockerTransportBuilder::new(container)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DockerCarrier {
    container: String,
    user: Option<String>,
    executable: PathBuf,
    command_timeout: Duration,
}

impl DockerCarrier {
    /// `docker exec -i [-u user] <container>` followed by `program`.
    fn exec(&self, program: &str) -> Command {
        let mut command = Command::new(&self.executable);
        command.arg("exec").arg("-i");
        if let Some(user) = &self.user {
            command.arg("-u").arg(user);
        }
        command.arg(&self.container).arg(program);
        command
    }
}

impl ShellCarrier for DockerCarrier {
    fn transport_name(&self) -> &'static str {
        "docker"
    }

    fn location(&self) -> &'static str {
        "container"
    }

    fn shell_command(&self, remote_command: &str, _loopback_ports: &[u16]) -> Command {
        // `DockerTransport::spawn` refuses loopback ports before this runs.
        let mut command = self.exec("/bin/sh");
        command.arg("-c").arg(remote_command);
        command
    }

    fn control_deadline(&self) -> Duration {
        self.command_timeout
    }

    fn classify_local_error(
        &self,
        operation: &'static str,
        error: std::io::Error,
    ) -> TransportError {
        let (kind, message) = match error.kind() {
            std::io::ErrorKind::NotFound => (
                TransportErrorKind::ExecutableNotFound,
                format!("the Docker CLI was not found: {error}"),
            ),
            std::io::ErrorKind::PermissionDenied => (
                TransportErrorKind::PermissionDenied,
                format!("the Docker CLI could not be started: {error}"),
            ),
            _ => (TransportErrorKind::SpawnFailed, error.to_string()),
        };
        TransportError::new(kind, "docker", operation, message, false)
    }

    fn carrier_failure(
        &self,
        operation: &'static str,
        status: std::process::ExitStatus,
        stderr: &[u8],
    ) -> Option<TransportError> {
        classify_docker_failure(operation, status.code(), stderr)
    }
}

/// `docker exec` reports its own failures as exit status 125, and an OCI
/// runtime that cannot start the command as 126/127 with an `OCI runtime`
/// diagnostic. Any other status is the command's own.
fn classify_docker_failure(
    operation: &'static str,
    code: Option<i32>,
    stderr: &[u8],
) -> Option<TransportError> {
    let diagnostic = bounded_diagnostic(stderr);
    let lower = diagnostic.to_ascii_lowercase();
    let oci_failure = matches!(code, Some(126 | 127)) && lower.contains("oci runtime");
    if code != Some(125) && !oci_failure {
        return None;
    }
    let (kind, retryable, message) = if lower.contains("no such container") {
        (
            TransportErrorKind::RemoteUnavailable,
            false,
            "the container does not exist",
        )
    } else if lower.contains("is not running") || lower.contains("is paused") {
        (
            TransportErrorKind::RemoteUnavailable,
            true,
            "the container is not running",
        )
    } else if lower.contains("cannot connect to the docker daemon") {
        (
            TransportErrorKind::ConnectionRefused,
            true,
            "the Docker daemon is not reachable",
        )
    } else if lower.contains("permission denied") {
        (
            TransportErrorKind::PermissionDenied,
            false,
            "the Docker daemon refused this user",
        )
    } else if oci_failure {
        (
            TransportErrorKind::SpawnFailed,
            false,
            "the container could not start the command",
        )
    } else {
        (
            TransportErrorKind::RemoteUnavailable,
            true,
            "docker exec failed",
        )
    };
    let detail = if diagnostic.is_empty() {
        message.to_string()
    } else {
        format!("{message}: {diagnostic}")
    };
    Some(TransportError::new(
        kind, "docker", operation, detail, retryable,
    ))
}

#[async_trait]
impl ExecutionTransport for DockerTransport {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn capabilities(&self) -> TransportCapabilities {
        self.shell.capabilities()
    }

    async fn readiness(
        &self,
        request: TransportReadinessRequest,
    ) -> TransportResult<ProviderReadiness> {
        self.shell.readiness(request).await
    }

    async fn validate_working_directory(&self, working_directory: &Path) -> TransportResult<()> {
        self.shell
            .validate_working_directory(working_directory)
            .await
    }

    async fn suggest_working_directories(
        &self,
        query: WorkingDirectoryQuery,
    ) -> TransportResult<WorkingDirectoryCandidates> {
        self.shell.suggest_working_directories(query).await
    }

    async fn spawn(&self, request: TransportSpawnRequest) -> TransportResult<TransportProcess> {
        if !request.command.loopback_ports.is_empty() {
            return Err(TransportError::new(
                TransportErrorKind::Unsupported,
                "docker",
                "spawn",
                "this provider serves a loopback port the SDK host must reach, which the Docker \
                 transport does not forward yet",
                false,
            ));
        }
        self.shell.spawn(request, None).await
    }
}

fn validate_reference(field: &'static str, value: &str) -> TransportResult<()> {
    if value.is_empty()
        || value.starts_with('-')
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.-:".contains(character))
    {
        return Err(configuration_error(&format!("invalid Docker {field}")));
    }
    Ok(())
}

fn configuration_error(message: &str) -> TransportError {
    TransportError::new(
        TransportErrorKind::InvalidConfiguration,
        "docker",
        "configure",
        message,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(command: &Command) -> Vec<String> {
        command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn runs_shell_commands_through_docker_exec_with_stdin() {
        let transport = DockerTransport::builder("fleet-agent-1")
            .user("agent")
            .build()
            .unwrap();
        let command = transport.shell.carrier.shell_command("echo hi", &[]);
        assert_eq!(command.as_std().get_program(), "docker");
        assert_eq!(
            args(&command),
            [
                "exec",
                "-i",
                "-u",
                "agent",
                "fleet-agent-1",
                "/bin/sh",
                "-c",
                "echo hi"
            ]
        );
    }

    #[test]
    fn rejects_references_that_could_become_options_or_shell() {
        for container in ["", "-it", "name with space", "a;b", "$(x)"] {
            assert!(
                DockerTransport::builder(container).build().is_err(),
                "{container:?}"
            );
        }
        assert!(DockerTransport::builder("fleet-agent_1.dev")
            .build()
            .is_ok());
        assert!(DockerTransport::builder("c")
            .user("--privileged")
            .build()
            .is_err());
        assert!(DockerTransport::builder("c")
            .user("1000:1000")
            .build()
            .is_ok());
    }

    #[test]
    fn separates_docker_failures_from_the_commands_own_exit_status() {
        let missing = classify_docker_failure(
            "spawn",
            Some(125),
            b"Error response from daemon: No such container: fleet-agent-1",
        )
        .expect("docker failure");
        assert_eq!(missing.kind, TransportErrorKind::RemoteUnavailable);
        assert!(!missing.retryable);

        let stopped = classify_docker_failure(
            "spawn",
            Some(125),
            b"Error response from daemon: container abc is not running",
        )
        .expect("docker failure");
        assert!(stopped.retryable);

        let daemon = classify_docker_failure(
            "spawn",
            Some(1),
            b"Cannot connect to the Docker daemon at unix:///var/run/docker.sock",
        );
        assert!(daemon.is_none(), "exit 1 is the command's own status");

        let oci = classify_docker_failure(
            "spawn",
            Some(127),
            b"OCI runtime exec failed: exec failed: unable to start container process: exec: \"/bin/sh\": no such file",
        )
        .expect("oci failure");
        assert_eq!(oci.kind, TransportErrorKind::SpawnFailed);

        // A login-shell probe that exits 127 because Bash/Zsh are missing is
        // the remote command's status, not a Docker failure.
        assert!(classify_docker_failure("probe", Some(127), b"").is_none());
    }
}
