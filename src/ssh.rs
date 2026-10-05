//! SSH-backed execution transport.

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::process::Command;

use crate::remote_shell::{bounded_diagnostic, RemoteShell, ShellCarrier};
use crate::{
    ExecutionTransport, ProviderReadiness, SandboxCapabilities, SecretString,
    TransportCapabilities, TransportError, TransportErrorKind, TransportProcess,
    TransportReadinessRequest, TransportResult, TransportSpawnRequest, WorkingDirectoryCandidates,
    WorkingDirectoryQuery,
};

#[cfg(unix)]
static NEXT_ASKPASS_ID: AtomicU64 = AtomicU64::new(1);
const ASKPASS_PASSWORD_ENV: &str = "TEMPS_AGENT_RUNTIME_SSH_PASSWORD";

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
            shell: RemoteShell::new(
                SshCarrier {
                    host: self.host,
                    user: self.user,
                    port: self.port,
                    authentication: self.authentication,
                    known_hosts_file: self.known_hosts_file,
                    host_key_policy: self.host_key_policy,
                    connect_timeout: self.connect_timeout,
                    executable: self.executable,
                    askpass,
                },
                self.intrinsic_sandbox,
            ),
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
    shell: RemoteShell<SshCarrier>,
}

/// The OpenSSH connection a [`SshTransport`] runs remote shell commands over.
#[derive(Debug, Clone)]
pub(crate) struct SshCarrier {
    host: String,
    user: Option<String>,
    port: Option<u16>,
    authentication: SshAuthentication,
    known_hosts_file: Option<PathBuf>,
    host_key_policy: SshHostKeyPolicy,
    connect_timeout: Duration,
    executable: PathBuf,
    askpass: Option<Arc<AskpassHelper>>,
}

impl SshTransport {
    /// Create a builder for an SSH destination.
    pub fn builder(host: impl Into<String>) -> SshTransportBuilder {
        SshTransportBuilder::new(host)
    }

    #[cfg(test)]
    fn base_command(&self) -> Command {
        self.shell.carrier.base_command_forwarding(&[])
    }

    #[cfg(test)]
    fn base_command_forwarding(&self, loopback_ports: &[u16]) -> Command {
        self.shell.carrier.base_command_forwarding(loopback_ports)
    }
}

impl SshCarrier {
    fn destination(&self) -> String {
        self.user
            .as_ref()
            .map_or_else(|| self.host.clone(), |user| format!("{user}@{}", self.host))
    }

    /// The `ssh` invocation up to its destination, plus `-L` for each
    /// loopback port the remote process serves, so the SDK host reaches it at
    /// the same local port. A failed forward fails the connection instead of
    /// leaving the client unable to reach the server.
    fn base_command_forwarding(&self, loopback_ports: &[u16]) -> Command {
        let mut command = Command::new(&self.executable);
        for port in loopback_ports {
            command
                .arg("-o")
                .arg("ExitOnForwardFailure=yes")
                .arg("-L")
                .arg(format!("127.0.0.1:{port}:127.0.0.1:{port}"));
        }
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
}

impl ShellCarrier for SshCarrier {
    fn transport_name(&self) -> &'static str {
        "ssh"
    }

    fn location(&self) -> &'static str {
        "SSH destination"
    }

    fn shell_command(&self, remote_command: &str, loopback_ports: &[u16]) -> Command {
        let mut command = self.base_command_forwarding(loopback_ports);
        command.arg(remote_command);
        command
    }

    fn control_deadline(&self) -> Duration {
        self.connect_timeout + Duration::from_secs(5)
    }

    fn classify_local_error(
        &self,
        operation: &'static str,
        error: std::io::Error,
    ) -> TransportError {
        classify_local_ssh_error(operation, error)
    }

    fn carrier_failure(
        &self,
        operation: &'static str,
        status: std::process::ExitStatus,
        stderr: &[u8],
    ) -> Option<TransportError> {
        (status.code() == Some(255)).then(|| classify_ssh_diagnostic(operation, stderr))
    }
}

#[async_trait]
impl ExecutionTransport for SshTransport {
    fn name(&self) -> &'static str {
        "ssh"
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
        self.shell.spawn(request, None).await
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn loopback_ports_are_forwarded_before_the_destination() {
        let transport = SshTransport::builder("example.test")
            .user("fleet")
            .build()
            .unwrap();
        let command = transport.base_command_forwarding(&[4242]);
        let args: Vec<String> = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        let forward = args.iter().position(|arg| arg == "-L").expect("forward");
        assert_eq!(args[forward + 1], "127.0.0.1:4242:127.0.0.1:4242");
        assert!(args.contains(&"ExitOnForwardFailure=yes".to_string()));
        let destination = args
            .iter()
            .position(|arg| arg == "fleet@example.test")
            .unwrap();
        assert!(forward < destination);
        assert!(!transport
            .base_command()
            .as_std()
            .get_args()
            .any(|arg| arg == "-L"));
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
