//! Nono sandbox discovery, profile management, and process wrapping.
//!
//! The runtime invokes the `nono` CLI as an outer process boundary. It does
//! not apply Nono's irreversible in-process sandbox to the embedding server.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::adapter::{resolve_executable, CommandSpec};
use crate::{
    SandboxBackend, SandboxCapabilities, SandboxContext, SandboxError, SandboxPathAccess,
    SandboxProfileChange, SandboxResource, SandboxViolation, ToolCallStatus, TurnEvent,
};

const MAX_NAME_BYTES: usize = 80;
const MAX_PROFILE_REFERENCE_BYTES: usize = 512;
const MAX_PATHS_PER_CLASS: usize = 100;
const MAX_TOKENS_PER_CLASS: usize = 100;
const MAX_PATH_BYTES: usize = 1024;
const MAX_VALIDATION_OUTPUT_BYTES: usize = 4096;
static TEMPORARY_PROFILE_ID: AtomicU64 = AtomicU64::new(0);

/// Nono-specific failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NonoError {
    /// Nono could not be resolved.
    #[error("nono is not installed or the configured executable does not exist")]
    Unavailable,
    /// A profile or execution option failed validation.
    #[error("invalid nono {field}: {message}")]
    Invalid {
        /// Rejected field.
        field: &'static str,
        /// Actionable reason.
        message: String,
    },
    /// A profile directory could not be created.
    #[error("could not create nono profile directory at {path}: {source}")]
    CreateDirectory {
        /// Target directory.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// A profile artifact could not be written.
    #[error("could not write nono profile at {path}: {source}")]
    WriteProfile {
        /// Artifact path.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// Nono could not be executed for inspection or validation.
    #[error("could not run nono for {operation}: {source}")]
    Execute {
        /// Operation being attempted.
        operation: &'static str,
        /// Process error.
        #[source]
        source: std::io::Error,
    },
    /// Nono inspection or validation exceeded its deadline.
    #[error("nono {operation} timed out after {seconds} seconds")]
    Timeout {
        /// Operation being attempted.
        operation: &'static str,
        /// Deadline.
        seconds: u64,
    },
    /// Nono rejected a generated profile.
    #[error("nono rejected profile {path}: {detail}")]
    Validation {
        /// Rejected artifact.
        path: PathBuf,
        /// Bounded validator output.
        detail: String,
    },
    /// Profile JSON serialization failed.
    #[error("could not serialize nono profile: {0}")]
    Serialize(#[from] serde_json::Error),
    /// A portable profile change cannot be represented safely by Nono's
    /// managed profile document.
    #[error("nono cannot apply sandbox profile change `{change}`: {message}")]
    UnsupportedChange {
        /// Stable change kind.
        change: &'static str,
        /// Actionable explanation.
        message: String,
    },
}

/// Whether Nono remains as a supervisor or replaces itself with the child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum NonoMode {
    /// Supervised execution with network policy, credentials, and audit support.
    #[default]
    Run,
    /// Exec-style filesystem boundary with no supervisor.
    ///
    /// Nono `wrap` does not enforce destination network policy. Capability
    /// reporting makes that limitation explicit.
    Wrap,
}

/// Access granted to an additional path at execution time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathAccess {
    /// Read-only access.
    Read,
    /// Read and write access.
    ReadWrite,
}

/// One explicit per-turn filesystem grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathGrant {
    /// Absolute path being granted.
    pub path: PathBuf,
    /// Granted access.
    pub access: PathAccess,
    /// Override a deny rule in the base profile.
    pub bypass_protection: bool,
}

/// Capability truth for a selected Nono mode.
pub type NonoCapabilities = SandboxCapabilities;

impl NonoMode {
    /// Report controls that the selected primitive actually enforces.
    pub fn capabilities(self) -> NonoCapabilities {
        match self {
            Self::Run => NonoCapabilities {
                filesystem: true,
                network_block: true,
                network_allowlist: true,
                credentials: true,
                audit: true,
                process_isolation: true,
            },
            Self::Wrap => NonoCapabilities {
                filesystem: true,
                network_block: true,
                network_allowlist: false,
                credentials: false,
                audit: false,
                process_isolation: true,
            },
        }
    }
}

#[async_trait::async_trait]
impl SandboxBackend for NonoExecution {
    fn name(&self) -> &'static str {
        "nono"
    }

    fn capabilities(&self) -> SandboxCapabilities {
        NonoExecution::capabilities(self)
    }

    async fn prepare(
        &self,
        context: SandboxContext,
        command: CommandSpec,
    ) -> Result<CommandSpec, SandboxError> {
        self.wrap(command, &context.working_directory)
            .map_err(|error| SandboxError::backend(self.name(), error))
    }

    fn classify_event(&self, event: &TurnEvent) -> Option<SandboxViolation> {
        let TurnEvent::ToolCall {
            id,
            name,
            status: ToolCallStatus::Failed,
            output,
            error,
            ..
        } = event
        else {
            return None;
        };
        let diagnostic = error
            .as_deref()
            .into_iter()
            .chain(output.as_deref())
            .find(|text| detect_denied_path(text).is_some())?;
        let path = detect_denied_path(diagnostic)?;
        Some(SandboxViolation {
            step_id: id.clone(),
            tool_name: Some(name.clone()),
            resource: SandboxResource::Path {
                path: PathBuf::from(path),
                access: None,
            },
            message: diagnostic.chars().take(1024).collect(),
        })
    }
}

fn detect_denied_path(text: &str) -> Option<String> {
    const DENIAL_PHRASES: [&str; 2] = ["Operation not permitted", "Permission denied"];
    text.lines()
        .filter(|line| DENIAL_PHRASES.iter().any(|phrase| line.contains(phrase)))
        .find_map(extract_path_token)
}

fn extract_path_token(line: &str) -> Option<String> {
    line.split(|character: char| {
        character.is_whitespace() || character == '\'' || character == '"' || character == ':'
    })
    .map(str::trim)
    .filter(|token| token.starts_with('/') || token.starts_with("~/"))
    .find_map(|token| {
        let trimmed = token.trim_end_matches(|character: char| {
            !character.is_alphanumeric() && character != '/' && character != '.'
        });
        (trimmed.len() > 1).then(|| trimmed.to_string())
    })
}

/// Per-turn Nono wrapper configuration.
#[derive(Debug, Clone)]
pub struct NonoExecution {
    /// Resolved Nono executable.
    pub executable: PathBuf,
    /// Profile name, registry reference, or JSON path.
    pub profile: String,
    /// Nono execution primitive.
    pub mode: NonoMode,
    /// Explicit path grants layered over the profile.
    pub grants: Vec<PathGrant>,
    /// Loopback ports a supervised process may listen on.
    pub listen_ports: Vec<u16>,
    /// Named credentials provided through Nono's credential proxy.
    ///
    /// Credentials are supported only by [`NonoMode::Run`]. Secret values are
    /// never part of this structure or the generated process arguments.
    pub credentials: Vec<String>,
    /// Trust Nono's shared proxy CA in the macOS user trust store.
    ///
    /// This is required by some native TLS clients when Nono intercepts TLS,
    /// but it is a consequential persistent trust change and is never enabled
    /// automatically. It is supported only by [`NonoMode::Run`].
    pub trust_proxy_ca: bool,
    /// Tailnet the supervised process may reach through its split proxy.
    ///
    /// The tailnet's proxy environment is applied to the wrapped command, its
    /// loopback ports, control socket, and ssh config are opened, and Nono's
    /// own destination filtering stays in place.
    #[cfg(feature = "tailnet")]
    pub tailnet: Option<crate::tailnet::TailnetAccess>,
    /// Chain Nono's destination-filtering proxy into the tailnet split proxy.
    ///
    /// Required when the profile allowlists destinations, because Nono then
    /// replaces the proxy environment with its own proxy. Leave it `false`
    /// for unrestricted profiles so the process talks to the split proxy
    /// directly. It is supported only by [`NonoMode::Run`].
    #[cfg(feature = "tailnet")]
    pub tailnet_chain_proxy: bool,
}

impl NonoExecution {
    /// Resolve Nono from an optional override and create a supervised policy.
    pub fn discover(profile: impl Into<String>) -> Result<Self, NonoError> {
        let executable = resolve_executable(None, "nono").ok_or(NonoError::Unavailable)?;
        let execution = Self {
            executable,
            profile: profile.into(),
            mode: NonoMode::Run,
            grants: Vec::new(),
            listen_ports: Vec::new(),
            credentials: Vec::new(),
            trust_proxy_ca: false,
            #[cfg(feature = "tailnet")]
            tailnet: None,
            #[cfg(feature = "tailnet")]
            tailnet_chain_proxy: false,
        };
        execution.validate()?;
        Ok(execution)
    }

    /// Use an explicit Nono executable and profile reference.
    pub fn new(
        executable: impl Into<PathBuf>,
        profile: impl Into<String>,
    ) -> Result<Self, NonoError> {
        let execution = Self {
            executable: executable.into(),
            profile: profile.into(),
            mode: NonoMode::Run,
            grants: Vec::new(),
            listen_ports: Vec::new(),
            credentials: Vec::new(),
            trust_proxy_ca: false,
            #[cfg(feature = "tailnet")]
            tailnet: None,
            #[cfg(feature = "tailnet")]
            tailnet_chain_proxy: false,
        };
        execution.validate()?;
        Ok(execution)
    }

    /// Report enforcement available for this execution mode.
    pub fn capabilities(&self) -> NonoCapabilities {
        self.mode.capabilities()
    }

    fn validate(&self) -> Result<(), NonoError> {
        validate_profile_reference(&self.profile)?;
        if !self.executable.is_file() {
            return Err(NonoError::Unavailable);
        }
        for grant in &self.grants {
            if !grant.path.is_absolute() {
                return Err(NonoError::Invalid {
                    field: "grant.path",
                    message: format!("{} is not absolute", grant.path.display()),
                });
            }
        }
        if self.grants.len() > MAX_PATHS_PER_CLASS {
            return Err(NonoError::Invalid {
                field: "grants",
                message: format!("accepts at most {MAX_PATHS_PER_CLASS} paths"),
            });
        }
        normalize_tokens("credentials", self.credentials.clone())?;
        if self.mode == NonoMode::Wrap && !self.credentials.is_empty() {
            return Err(NonoError::Invalid {
                field: "credentials",
                message: "credential proxying requires NonoMode::Run".to_string(),
            });
        }
        if self.mode == NonoMode::Wrap && self.trust_proxy_ca {
            return Err(NonoError::Invalid {
                field: "trust_proxy_ca",
                message: "proxy CA trust requires NonoMode::Run".to_string(),
            });
        }
        #[cfg(feature = "tailnet")]
        if self.tailnet_chain_proxy {
            if self.tailnet.is_none() {
                return Err(NonoError::Invalid {
                    field: "tailnet_chain_proxy",
                    message: "proxy chaining requires a tailnet".to_string(),
                });
            }
            if self.mode == NonoMode::Wrap {
                return Err(NonoError::Invalid {
                    field: "tailnet_chain_proxy",
                    message: "tailnet proxy chaining requires NonoMode::Run".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Wrap an adapter command without invoking a shell.
    pub fn wrap(
        &self,
        inner: CommandSpec,
        working_directory: &Path,
    ) -> Result<CommandSpec, NonoError> {
        self.validate()?;
        let mut args = vec![match self.mode {
            NonoMode::Run => "run".into(),
            NonoMode::Wrap => "wrap".into(),
        }];
        args.push(format!("--profile={}", self.profile).into());
        args.extend([
            "--allow-cwd".into(),
            "--workdir".into(),
            working_directory.as_os_str().to_owned(),
        ]);
        for port in &self.listen_ports {
            args.extend(["--listen-port".into(), port.to_string().into()]);
        }
        for credential in &self.credentials {
            args.extend(["--credential".into(), credential.into()]);
        }
        if self.trust_proxy_ca {
            args.push("--trust-proxy-ca".into());
        }
        for grant in &self.grants {
            if grant.bypass_protection {
                args.extend([
                    "--bypass-protection".into(),
                    grant.path.as_os_str().to_owned(),
                ]);
            }
            args.push(match grant.access {
                PathAccess::Read => "--read".into(),
                PathAccess::ReadWrite => "--allow".into(),
            });
            args.push(grant.path.as_os_str().to_owned());
        }
        #[cfg(feature = "tailnet")]
        let inner = match &self.tailnet {
            Some(tailnet) => {
                args.extend(tailnet.nono_arguments(self.tailnet_chain_proxy));
                tailnet.apply(inner)
            }
            None => inner,
        };
        Ok(inner.wrap_with(&self.executable, args, Some("--".into())))
    }
}

/// Work-directory access encoded in a managed profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WorkdirAccess {
    /// Read-only workspace.
    Read,
    /// Read-write workspace.
    #[default]
    ReadWrite,
}

/// Network intent encoded in a managed profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[non_exhaustive]
pub enum NetworkPolicy {
    /// Inherit the base profile's network policy.
    #[default]
    Inherit,
    /// Deny all network access.
    Block,
    /// Allow only listed destinations.
    AllowDomains {
        /// Hostnames or URL patterns allowed by the network proxy.
        domains: Vec<String>,
    },
    /// Allow unrestricted outbound network access.
    Unrestricted,
}

/// Serializable, provider-independent Nono profile intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedProfile {
    /// Human-readable profile name.
    pub name: String,
    /// Base profile name, registry reference, or path.
    pub extends: String,
    /// Workspace access.
    pub workdir: WorkdirAccess,
    /// Additional read-only paths.
    pub readable_paths: Vec<String>,
    /// Additional read-write paths.
    pub writable_paths: Vec<String>,
    /// Explicitly denied paths.
    pub denied_paths: Vec<String>,
    /// Environment variable patterns removed before the child starts.
    pub denied_environment: Vec<String>,
    /// Network policy.
    pub network: NetworkPolicy,
}

impl ManagedProfile {
    /// Create a restrictive profile extending an existing base.
    pub fn new(name: impl Into<String>, extends: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            extends: extends.into(),
            workdir: WorkdirAccess::ReadWrite,
            readable_paths: Vec::new(),
            writable_paths: Vec::new(),
            denied_paths: Vec::new(),
            denied_environment: Vec::new(),
            network: NetworkPolicy::Inherit,
        }
    }

    /// Validate and normalize set-like fields.
    pub fn normalize(mut self) -> Result<Self, NonoError> {
        validate_name(&self.name)?;
        validate_profile_reference(&self.extends)?;
        self.readable_paths = normalize_paths("readable_paths", self.readable_paths)?;
        self.writable_paths = normalize_paths("writable_paths", self.writable_paths)?;
        self.denied_paths = normalize_paths("denied_paths", self.denied_paths)?;
        self.denied_environment = normalize_tokens("denied_environment", self.denied_environment)?;
        if let NetworkPolicy::AllowDomains { domains } = &mut self.network {
            *domains = normalize_domains(std::mem::take(domains))?;
        }
        Ok(self)
    }

    /// Apply a portable, approved profile change before saving a new
    /// content-addressed revision with [`NonoManager::save`].
    ///
    /// Changes that Nono cannot represent without weakening unrelated policy
    /// fail explicitly. In particular, protection bypasses and credential
    /// mappings remain application-owned operations.
    pub fn apply_change(&mut self, change: &SandboxProfileChange) -> Result<(), NonoError> {
        let mut updated = self.clone();
        match change {
            SandboxProfileChange::GrantPath {
                path,
                access,
                bypass_protection,
            } => {
                if *bypass_protection {
                    return Err(NonoError::UnsupportedChange {
                        change: "grant_path",
                        message: "persistent protection bypasses require backend-specific review"
                            .to_string(),
                    });
                }
                let path = path.to_string_lossy().to_string();
                if updated.denied_paths.iter().any(|denied| denied == &path) {
                    return Err(NonoError::UnsupportedChange {
                        change: "grant_path",
                        message: format!("{path} is explicitly denied by this profile"),
                    });
                }
                match access {
                    SandboxPathAccess::Read => updated.readable_paths.push(path),
                    SandboxPathAccess::Write | SandboxPathAccess::ReadWrite => {
                        updated.writable_paths.push(path);
                    }
                }
            }
            SandboxProfileChange::AllowNetworkDestination { destination } => {
                let NetworkPolicy::AllowDomains { domains } = &mut updated.network else {
                    return Err(NonoError::UnsupportedChange {
                        change: "allow_network_destination",
                        message: "automatic additions require an existing destination allowlist"
                            .to_string(),
                    });
                };
                domains.push(destination.clone());
            }
            SandboxProfileChange::AddCredential { .. } => {
                return Err(NonoError::UnsupportedChange {
                    change: "add_credential",
                    message: "named credential mappings are execution settings, not profile JSON"
                        .to_string(),
                });
            }
            SandboxProfileChange::Custom { .. } => {
                return Err(NonoError::UnsupportedChange {
                    change: "custom",
                    message: "custom changes must be handled by the application profile manager"
                        .to_string(),
                });
            }
        }
        let normalized = updated.normalize()?;
        *self = normalized;
        Ok(())
    }

    fn document(&self) -> ProfileDocument<'_> {
        let (network_block, hosts) = match &self.network {
            NetworkPolicy::Inherit => (None, &[][..]),
            NetworkPolicy::Block => (Some(true), &[][..]),
            NetworkPolicy::AllowDomains { domains } => (Some(false), domains.as_slice()),
            NetworkPolicy::Unrestricted => (Some(false), &[][..]),
        };
        ProfileDocument {
            extends: &self.extends,
            meta: ProfileMeta {
                name: &self.name,
                version: "1.0.0",
            },
            workdir: ProfileWorkdir {
                access: match self.workdir {
                    WorkdirAccess::Read => "read",
                    WorkdirAccess::ReadWrite => "readwrite",
                },
            },
            filesystem: ProfileFilesystem {
                allow: &self.writable_paths,
                read: &self.readable_paths,
                deny: &self.denied_paths,
            },
            environment: (!self.denied_environment.is_empty()).then_some(ProfileEnvironment {
                deny_vars: &self.denied_environment,
            }),
            network: network_block.map(|block| ProfileNetwork {
                block,
                allow_domain: hosts,
            }),
        }
    }
}

#[derive(Serialize)]
struct ProfileDocument<'a> {
    extends: &'a str,
    meta: ProfileMeta<'a>,
    workdir: ProfileWorkdir<'a>,
    filesystem: ProfileFilesystem<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    environment: Option<ProfileEnvironment<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    network: Option<ProfileNetwork<'a>>,
}

#[derive(Serialize)]
struct ProfileMeta<'a> {
    name: &'a str,
    version: &'a str,
}
#[derive(Serialize)]
struct ProfileWorkdir<'a> {
    access: &'a str,
}
#[derive(Serialize)]
struct ProfileFilesystem<'a> {
    allow: &'a [String],
    read: &'a [String],
    deny: &'a [String],
}
#[derive(Serialize)]
struct ProfileEnvironment<'a> {
    deny_vars: &'a [String],
}
#[derive(Serialize)]
struct ProfileNetwork<'a> {
    block: bool,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    allow_domain: &'a [String],
}

/// Creates, validates, and atomically activates managed profiles.
#[derive(Debug, Clone)]
pub struct NonoManager {
    executable: PathBuf,
    profile_directory: PathBuf,
    validation_timeout: Duration,
}

impl NonoManager {
    /// Discover Nono and manage profiles under `profile_directory`.
    pub fn discover(profile_directory: impl Into<PathBuf>) -> Result<Self, NonoError> {
        let executable = resolve_executable(None, "nono").ok_or(NonoError::Unavailable)?;
        Ok(Self::new(executable, profile_directory))
    }

    /// Use an explicit Nono executable and managed-profile directory.
    pub fn new(executable: impl Into<PathBuf>, profile_directory: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            profile_directory: profile_directory.into(),
            validation_timeout: Duration::from_secs(5),
        }
    }

    /// Change the deadline for version inspection and validation.
    pub fn validation_timeout(mut self, timeout: Duration) -> Self {
        self.validation_timeout = timeout;
        self
    }

    /// Best-effort Nono version string.
    pub async fn version(&self) -> Result<String, NonoError> {
        let output = tokio::time::timeout(
            self.validation_timeout,
            tokio::process::Command::new(&self.executable)
                .arg("--version")
                .output(),
        )
        .await
        .map_err(|_| NonoError::Timeout {
            operation: "version inspection",
            seconds: self.validation_timeout.as_secs(),
        })?
        .map_err(|source| NonoError::Execute {
            operation: "version inspection",
            source,
        })?;
        if !output.status.success() {
            return Err(NonoError::Validation {
                path: self.executable.clone(),
                detail: bounded_output(&output),
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Write, validate, and atomically activate a managed profile.
    pub async fn save(&self, profile: ManagedProfile) -> Result<PathBuf, NonoError> {
        if !self.executable.is_file() {
            return Err(NonoError::Unavailable);
        }
        let profile = profile.normalize()?;
        tokio::fs::create_dir_all(&self.profile_directory)
            .await
            .map_err(|source| NonoError::CreateDirectory {
                path: self.profile_directory.clone(),
                source,
            })?;
        #[cfg(unix)]
        tokio::fs::set_permissions(
            &self.profile_directory,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
        )
        .await
        .map_err(|source| NonoError::CreateDirectory {
            path: self.profile_directory.clone(),
            source,
        })?;
        let mut bytes = serde_json::to_vec_pretty(&profile.document())?;
        bytes.push(b'\n');
        let slug = artifact_slug(&profile.name);
        let hash = Sha256::digest(&bytes);
        let suffix = hash[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let destination = self.profile_directory.join(format!("{slug}-{suffix}.json"));
        if destination.is_file() {
            return Ok(destination);
        }
        let temporary_id = TEMPORARY_PROFILE_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = self.profile_directory.join(format!(
            ".{slug}-{suffix}.tmp-{}-{temporary_id}",
            std::process::id()
        ));
        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file =
            options
                .open(&temporary)
                .await
                .map_err(|source| NonoError::WriteProfile {
                    path: temporary.clone(),
                    source,
                })?;
        if let Err(source) = file.write_all(&bytes).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(NonoError::WriteProfile {
                path: temporary,
                source,
            });
        }
        if let Err(source) = file.sync_all().await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(NonoError::WriteProfile {
                path: temporary,
                source,
            });
        }
        drop(file);
        if let Err(error) = self.validate_path(&temporary).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        tokio::fs::rename(&temporary, &destination)
            .await
            .map_err(|source| NonoError::WriteProfile {
                path: destination.clone(),
                source,
            })?;
        Ok(destination)
    }

    /// Ask Nono to validate an existing profile in strict JSON mode.
    pub async fn validate_path(&self, path: &Path) -> Result<(), NonoError> {
        let output = tokio::time::timeout(
            self.validation_timeout,
            tokio::process::Command::new(&self.executable)
                .args(["profile", "validate"])
                .arg(path)
                .args(["--json", "--strict"])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| NonoError::Timeout {
            operation: "profile validation",
            seconds: self.validation_timeout.as_secs(),
        })?
        .map_err(|source| NonoError::Execute {
            operation: "profile validation",
            source,
        })?;
        if output.status.success() {
            Ok(())
        } else {
            Err(NonoError::Validation {
                path: path.to_owned(),
                detail: bounded_output(&output),
            })
        }
    }
}

fn validate_name(name: &str) -> Result<(), NonoError> {
    let name = name.trim();
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(NonoError::Invalid {
            field: "name",
            message: format!("must contain 1-{MAX_NAME_BYTES} non-control bytes"),
        });
    }
    Ok(())
}

fn validate_profile_reference(reference: &str) -> Result<(), NonoError> {
    let reference = reference.trim();
    if reference.is_empty()
        || reference.len() > MAX_PROFILE_REFERENCE_BYTES
        || reference.starts_with('-')
        || reference.chars().any(char::is_control)
    {
        return Err(NonoError::Invalid {
            field: "profile",
            message:
                "must be a profile name, registry reference, or path and cannot start with `-`"
                    .to_string(),
        });
    }
    Ok(())
}

fn normalize_paths(field: &'static str, values: Vec<String>) -> Result<Vec<String>, NonoError> {
    if values.len() > MAX_PATHS_PER_CLASS {
        return Err(NonoError::Invalid {
            field,
            message: format!("accepts at most {MAX_PATHS_PER_CLASS} paths"),
        });
    }
    let mut normalized = BTreeSet::new();
    for value in values {
        let value = value.trim();
        let supported = value.starts_with('/')
            || value == "$HOME"
            || value.starts_with("$HOME/")
            || value == "$NONO_CONFIG"
            || value.starts_with("$NONO_CONFIG/")
            || value == "$NONO_PACKAGES"
            || value.starts_with("$NONO_PACKAGES/");
        if !supported || value.len() > MAX_PATH_BYTES || value.chars().any(char::is_control) {
            return Err(NonoError::Invalid { field, message: format!("`{value}` must be absolute or start with $HOME, $NONO_CONFIG, or $NONO_PACKAGES") });
        }
        normalized.insert(value.to_string());
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_tokens(field: &'static str, values: Vec<String>) -> Result<Vec<String>, NonoError> {
    if values.len() > MAX_TOKENS_PER_CLASS {
        return Err(NonoError::Invalid {
            field,
            message: format!("accepts at most {MAX_TOKENS_PER_CLASS} values"),
        });
    }
    let mut normalized = BTreeSet::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(NonoError::Invalid {
                field,
                message: "contains an empty, overlong, or control-character value".to_string(),
            });
        }
        normalized.insert(value.to_string());
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_domains(values: Vec<String>) -> Result<Vec<String>, NonoError> {
    let domains = normalize_tokens("network.domains", values)?;
    if domains
        .iter()
        .any(|domain| domain.contains(char::is_whitespace))
    {
        return Err(NonoError::Invalid {
            field: "network.domains",
            message: "entries cannot contain whitespace".to_string(),
        });
    }
    Ok(domains)
}

fn artifact_slug(name: &str) -> String {
    let slug = name
        .chars()
        .flat_map(char::to_lowercase)
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        "profile".to_string()
    } else {
        slug
    }
}

fn bounded_output(output: &std::process::Output) -> String {
    let raw = if output.stdout.is_empty() {
        &output.stderr
    } else {
        &output.stdout
    };
    let detail = String::from_utf8_lossy(raw)
        .chars()
        .take(MAX_VALIDATION_OUTPUT_BYTES)
        .collect::<String>();
    if detail.trim().is_empty() {
        format!("nono exited with {}", output.status)
    } else {
        detail.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn wrap_never_uses_a_shell_and_preserves_argument_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let nono = temp.path().join("nono");
        std::fs::write(&nono, "stub").unwrap();
        let execution = NonoExecution::new(&nono, "claude-code").unwrap();
        let mut inner = CommandSpec::new("claude");
        inner.args = vec!["--model".into(), "name with spaces".into()];
        let wrapped = execution.wrap(inner, Path::new("/work/project")).unwrap();
        assert_eq!(wrapped.program, nono);
        assert_eq!(wrapped.args[0], OsString::from("run"));
        assert_eq!(
            wrapped.args.last(),
            Some(&OsString::from("name with spaces"))
        );
    }

    #[cfg(feature = "tailnet")]
    fn tailnet_access() -> crate::tailnet::TailnetAccess {
        crate::tailnet::TailnetAccess {
            name: "gala".into(),
            tailnet_name: Some("gala.games".into()),
            magic_dns_suffix: Some("jerboa-altered.ts.net".into()),
            proxy_addr: "127.0.0.1:41001".parse().unwrap(),
            socks_addr: "127.0.0.1:41002".parse().unwrap(),
            socket_path: PathBuf::from("/data/tailnets/abcd/ts.sock"),
            tailscale: PathBuf::from("/opt/homebrew/bin/tailscale"),
            ssh_config_path: PathBuf::from("/data/tailnets/abcd/ssh_config"),
        }
    }

    #[cfg(feature = "tailnet")]
    #[test]
    fn wrap_applies_tailnet_environment_and_opens_its_ports() {
        let temp = tempfile::tempdir().unwrap();
        let nono = temp.path().join("nono");
        std::fs::write(&nono, "stub").unwrap();
        let mut execution = NonoExecution::new(&nono, "claude-code").unwrap();
        execution.tailnet = Some(tailnet_access());
        let wrapped = execution
            .wrap(CommandSpec::new("codex"), Path::new("/work/project"))
            .unwrap();
        let text = wrapped
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(!text.contains(&"--upstream-proxy".to_string()));
        assert!(text.windows(2).any(|w| w == ["--open-port", "41001"]));
        assert!(text
            .windows(2)
            .any(|w| w == ["--allow-unix-socket", "/data/tailnets/abcd/ts.sock"]));
        assert_eq!(
            wrapped.environment.get(&OsString::from("HTTPS_PROXY")),
            Some(&OsString::from("http://127.0.0.1:41001"))
        );
        assert_eq!(
            wrapped
                .environment
                .get(&OsString::from("TEMPS_TAILNET_NAME")),
            Some(&OsString::from("gala"))
        );
        let separator = text.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(text[separator + 1], "codex");

        execution.tailnet_chain_proxy = true;
        let wrapped = execution
            .wrap(CommandSpec::new("codex"), Path::new("/work/project"))
            .unwrap();
        let text = wrapped
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(text
            .windows(2)
            .any(|w| w == ["--upstream-proxy", "127.0.0.1:41001"]));
    }

    #[cfg(feature = "tailnet")]
    #[test]
    fn tailnet_proxy_chaining_requires_a_tailnet_and_run_mode() {
        let temp = tempfile::tempdir().unwrap();
        let nono = temp.path().join("nono");
        std::fs::write(&nono, "stub").unwrap();
        let mut execution = NonoExecution::new(&nono, "claude-code").unwrap();
        execution.tailnet_chain_proxy = true;
        assert!(matches!(
            execution.wrap(CommandSpec::new("codex"), Path::new("/work")),
            Err(NonoError::Invalid {
                field: "tailnet_chain_proxy",
                ..
            })
        ));
        execution.tailnet = Some(tailnet_access());
        execution.mode = NonoMode::Wrap;
        assert!(matches!(
            execution.wrap(CommandSpec::new("codex"), Path::new("/work")),
            Err(NonoError::Invalid {
                field: "tailnet_chain_proxy",
                ..
            })
        ));
    }

    #[test]
    fn wrap_reports_missing_network_enforcement() {
        let capabilities = NonoMode::Wrap.capabilities();
        assert!(capabilities.filesystem);
        assert!(capabilities.network_block);
        assert!(!capabilities.network_allowlist);
        assert!(!capabilities.credentials);
    }

    #[test]
    fn managed_profile_normalizes_and_deduplicates_paths() {
        let mut profile = ManagedProfile::new("CI agent", "default");
        profile.readable_paths = vec!["$HOME/src".into(), "$HOME/src".into()];
        let profile = profile.normalize().unwrap();
        assert_eq!(profile.readable_paths, ["$HOME/src"]);
    }

    #[test]
    fn managed_profile_uses_current_nono_network_schema() {
        let mut profile = ManagedProfile::new("Network agent", "default");
        profile.network = NetworkPolicy::AllowDomains {
            domains: vec!["api.anthropic.com".into()],
        };
        let value = serde_json::to_value(profile.normalize().unwrap().document()).unwrap();
        assert_eq!(value["network"]["block"], false);
        assert_eq!(value["network"]["allow_domain"][0], "api.anthropic.com");
        assert!(value["network"].get("allow_hosts").is_none());
        assert!(value["network"].get("credentials").is_none());
    }

    #[test]
    fn managed_profile_applies_portable_recovery_changes() {
        let mut profile = ManagedProfile::new("Recovery", "default");
        profile.network = NetworkPolicy::AllowDomains {
            domains: vec!["api.anthropic.com".into()],
        };
        profile
            .apply_change(&SandboxProfileChange::GrantPath {
                path: PathBuf::from("/work/artifacts"),
                access: SandboxPathAccess::ReadWrite,
                bypass_protection: false,
            })
            .unwrap();
        profile
            .apply_change(&SandboxProfileChange::AllowNetworkDestination {
                destination: "github.com".into(),
            })
            .unwrap();

        assert_eq!(profile.writable_paths, ["/work/artifacts"]);
        assert!(matches!(
            profile.network,
            NetworkPolicy::AllowDomains { ref domains }
                if domains == &["api.anthropic.com", "github.com"]
        ));
    }

    #[test]
    fn managed_profile_rejects_protection_bypass() {
        let mut profile = ManagedProfile::new("Recovery", "default");
        let original = profile.clone();
        let error = profile
            .apply_change(&SandboxProfileChange::GrantPath {
                path: PathBuf::from("/private/key"),
                access: SandboxPathAccess::Read,
                bypass_protection: true,
            })
            .unwrap_err();

        assert!(matches!(error, NonoError::UnsupportedChange { .. }));
        assert_eq!(profile, original);
    }

    #[test]
    fn credentials_are_run_arguments_and_wrap_rejects_them() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("nono");
        std::fs::write(&executable, "stub").unwrap();
        let mut execution = NonoExecution::new(&executable, "default").unwrap();
        execution.credentials.push("github".into());
        let wrapped = execution
            .wrap(CommandSpec::new("claude"), Path::new("/work"))
            .unwrap();
        assert!(wrapped
            .args
            .windows(2)
            .any(|args| { args == [OsString::from("--credential"), OsString::from("github")] }));
        execution.mode = NonoMode::Wrap;
        assert!(execution
            .wrap(CommandSpec::new("claude"), Path::new("/work"))
            .is_err());
    }

    #[test]
    fn proxy_ca_trust_is_explicit_and_run_only() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("nono");
        std::fs::write(&executable, "stub").unwrap();
        let mut execution = NonoExecution::new(&executable, "default").unwrap();
        let ordinary = execution
            .wrap(CommandSpec::new("claude"), Path::new("/work"))
            .unwrap();
        assert!(!ordinary.args.iter().any(|arg| arg == "--trust-proxy-ca"));

        execution.trust_proxy_ca = true;
        let trusted = execution
            .wrap(CommandSpec::new("claude"), Path::new("/work"))
            .unwrap();
        assert!(trusted.args.iter().any(|arg| arg == "--trust-proxy-ca"));

        execution.mode = NonoMode::Wrap;
        assert!(execution
            .wrap(CommandSpec::new("claude"), Path::new("/work"))
            .is_err());
    }

    #[test]
    fn classifies_a_failed_tool_path_denial() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("nono");
        std::fs::write(&executable, "stub").unwrap();
        let execution = NonoExecution::new(&executable, "default").unwrap();
        let event = TurnEvent::ToolCall {
            id: Some("tool-7".into()),
            name: "shell".into(),
            status: ToolCallStatus::Failed,
            input: None,
            output: None,
            error: Some("cat: /Users/example/private/key: Operation not permitted".into()),
            task_id: None,
        };

        let violation = execution.classify_event(&event).unwrap();

        assert_eq!(violation.step_id.as_deref(), Some("tool-7"));
        assert!(matches!(
            violation.resource,
            SandboxResource::Path { path, access: None }
                if path == Path::new("/Users/example/private/key")
        ));
    }

    #[test]
    fn ignores_pathless_nested_sandbox_failures() {
        assert_eq!(
            detect_denied_path("sandbox-exec: sandbox_apply: Operation not permitted"),
            None
        );
    }
}
