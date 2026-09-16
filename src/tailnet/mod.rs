//! Per-agent Tailscale network identities ("tailnets").
//!
//! A machine's system `tailscaled` can be logged into only one Tailscale
//! profile at a time, and `tailscale switch` flips the whole host. Hosts
//! whose users belong to several tailnets (one per client, employer, or
//! project) need an agent to reach *one* of them without disturbing the
//! others or the user's own session. This module runs one extra userspace
//! `tailscaled` per configured tailnet (no TUN device, no root), each with
//! its own state directory, control socket, and loopback SOCKS5 listener,
//! and fronts it with a split proxy so an agent's public traffic stays direct
//! while tailnet traffic goes through the daemon.
//!
//! The daemon is supervised by [`ManagedProcessSupervisor`], so it inherits
//! the crate's bounded logs, restart policy, and process-tree ownership. A
//! running daemon yields a [`TailnetAccess`], which any provider command can
//! consume through [`TailnetAccess::apply`] and which the Nono backend turns
//! into sandbox flags. Nothing is reachable from a sandboxed child except the
//! two loopback ports and the control socket that are explicitly opened.
//!
//! Applications keep ownership of persistence (which tailnets exist), UI, and
//! the browser login step; the module only reports the login URL.

mod proxy;
mod tailscale;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex, RwLock};

use crate::adapter::CommandSpec;
use crate::services::{
    ManagedProcessError, ManagedProcessId, ManagedProcessSpec, ManagedProcessStatus,
    ManagedProcessSupervisor, RestartPolicy,
};
use proxy::{RouteTable, SocksUpstream, SplitProxy};
use tailscale::{StatusJson, UpJsonLine};

pub use tailscale::{install_hint, TailscaleBinaries};

const MAX_NAME_LEN: usize = 64;
const MAX_HOSTNAME_LEN: usize = 63;
/// How long [`TailnetDaemon::login`] waits for the daemon to hand back a
/// login URL before returning "still waiting" and letting the host poll.
const LOGIN_URL_WAIT: Duration = Duration::from_secs(20);
/// The `tailscale up` helper is killed after this if nobody completes the
/// browser login; the host can start a new one.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const SOCKET_WAIT: Duration = Duration::from_secs(10);
const STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(20);
const CLI_TIMEOUT: Duration = Duration::from_secs(10);
/// Environment variable prefix for every non-proxy variable a tailnet adds.
pub const ENVIRONMENT_PREFIX: &str = "TEMPS_TAILNET_";

/// Failures of the tailnet subsystem.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TailnetError {
    /// `tailscaled` or `tailscale` could not be found.
    #[error("tailscaled is not installed: {hint}")]
    Unavailable {
        /// Platform-specific install remedy.
        hint: &'static str,
    },
    /// A specification field failed validation.
    #[error("invalid tailnet {field}: {message}")]
    Invalid {
        /// Rejected field.
        field: &'static str,
        /// Actionable reason.
        message: String,
    },
    /// The per-tailnet state directory could not be prepared.
    #[error("tailnet state directory {path}: {source}")]
    StateDirectory {
        /// Directory that failed.
        path: PathBuf,
        /// Filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// The supervised `tailscaled` process failed.
    #[error("tailscaled process: {0}")]
    Process(#[from] ManagedProcessError),
    /// A `tailscale` CLI invocation failed.
    #[error("{command}: {message}")]
    Cli {
        /// Command that ran, without secrets.
        command: String,
        /// Bounded stderr or timeout description.
        message: String,
    },
    /// The loopback split proxy could not bind.
    #[error("tailnet split proxy could not start: {0}")]
    Proxy(#[source] std::io::Error),
    /// `tailscaled` started but never created its control socket.
    #[error("tailscaled did not create its control socket at {path} within {timeout:?}")]
    SocketTimeout {
        /// Expected socket path.
        path: PathBuf,
        /// Deadline that elapsed.
        timeout: Duration,
    },
    /// The tailnet is not connected, so no access can be granted.
    #[error("tailnet \"{name}\" is not connected ({state:?}): {detail}")]
    NotConnected {
        /// Tailnet label.
        name: String,
        /// Observed state.
        state: TailnetState,
        /// Latest actionable detail.
        detail: String,
    },
}

/// Lifecycle state of one tailnet daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TailnetState {
    /// No daemon process is running for this tailnet.
    Stopped,
    /// The daemon was spawned and is not yet answering, or is reconnecting.
    Starting,
    /// The daemon runs but has no node identity yet; a browser login is needed.
    NeedsLogin,
    /// Logged in and connected to the tailnet.
    Running,
    /// The daemon exited or could not be started; `detail` says why.
    Failed,
}

/// Observable status of a tailnet daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TailnetStatus {
    /// Lifecycle state.
    pub state: TailnetState,
    /// Current step or last error, safe to show verbatim.
    pub detail: String,
    /// Login URL to open in a browser signed into the wanted Tailscale
    /// account. Present only while a login is pending.
    pub auth_url: Option<String>,
    /// Tailnet organization name reported by Tailscale, e.g. `example.com`.
    pub tailnet_name: Option<String>,
    /// MagicDNS suffix, e.g. `tail1234.ts.net`.
    pub magic_dns_suffix: Option<String>,
    /// This node's own MagicDNS name inside the tailnet.
    pub self_dns_name: Option<String>,
    /// This node's tailnet addresses.
    pub self_ips: Vec<String>,
    /// Peers currently online.
    pub online_peers: u32,
    /// Peers known to the tailnet.
    pub total_peers: u32,
    /// Daemon process id when running.
    pub pid: Option<u32>,
    /// Automatic or explicit daemon restarts.
    pub restart_count: u32,
    /// Loopback split-proxy port agents use; `None` until the proxy is up.
    pub proxy_port: Option<u16>,
    /// Loopback SOCKS5 port served by tailscaled.
    pub socks_port: u16,
    /// Last state-change time as Unix milliseconds.
    pub updated_at_ms: u64,
}

/// Everything a provider launch needs from a connected tailnet.
///
/// The structure is a snapshot; it stays valid while the daemon that produced
/// it runs. Obtain a fresh one per turn with [`TailnetDaemon::access`]. It is
/// plain data so hosts can build fixtures for their own launch tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailnetAccess {
    /// Application label of the tailnet.
    pub name: String,
    /// Tailnet organization name, when reported.
    pub tailnet_name: Option<String>,
    /// MagicDNS suffix, when reported.
    pub magic_dns_suffix: Option<String>,
    /// HTTP `CONNECT` split proxy; safe as `HTTPS_PROXY` for all traffic.
    pub proxy_addr: SocketAddr,
    /// tailscaled's own SOCKS5 listener; tailnet destinations only.
    pub socks_addr: SocketAddr,
    /// tailscaled control socket, for `tailscale --socket … nc` and `status`.
    pub socket_path: PathBuf,
    /// `tailscale` CLI matching the daemon.
    pub tailscale: PathBuf,
    /// Generated ssh config routing tailnet hosts through the daemon.
    pub ssh_config_path: PathBuf,
}

impl TailnetAccess {
    /// Environment that routes HTTP(S) through the split proxy and describes
    /// the tailnet to the agent.
    ///
    /// Proxy variables keep loopback direct so local dev servers and MCP
    /// endpoints are unaffected. Every other name starts with
    /// [`ENVIRONMENT_PREFIX`], which hosts should reserve so credential
    /// mappings cannot impersonate them.
    pub fn environment(&self) -> BTreeMap<OsString, OsString> {
        let proxy_url = format!("http://{}", self.proxy_addr);
        let mut environment = BTreeMap::new();
        for name in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"] {
            environment.insert(name.into(), proxy_url.clone().into());
        }
        for name in ["NO_PROXY", "no_proxy"] {
            environment.insert(name.into(), "localhost,127.0.0.1,::1".into());
        }
        // Node only honors proxy variables when asked to (Node >= 24).
        environment.insert("NODE_USE_ENV_PROXY".into(), "1".into());
        let prefixed = |suffix: &str| OsString::from(format!("{ENVIRONMENT_PREFIX}{suffix}"));
        environment.insert(prefixed("NAME"), self.name.clone().into());
        environment.insert(
            prefixed("SOCKS"),
            format!("socks5h://{}", self.socks_addr).into(),
        );
        environment.insert(prefixed("SOCKET"), self.socket_path.clone().into());
        environment.insert(prefixed("SSH_CONFIG"), self.ssh_config_path.clone().into());
        environment.insert(prefixed("TAILSCALE_BIN"), self.tailscale.clone().into());
        if let Some(tailnet_name) = &self.tailnet_name {
            environment.insert(prefixed("ORG"), tailnet_name.clone().into());
        }
        if let Some(suffix) = &self.magic_dns_suffix {
            environment.insert(prefixed("DNS_SUFFIX"), suffix.clone().into());
        }
        environment
    }

    /// Add the tailnet environment to a provider command.
    ///
    /// Works for every provider because it only touches the process
    /// environment; use it from a custom [`crate::SandboxBackend`] or before
    /// handing an unsandboxed command to a transport.
    pub fn apply(&self, mut spec: CommandSpec) -> CommandSpec {
        spec.environment.extend(self.environment());
        spec
    }

    /// Short instructions for the agent's system prompt so it knows the
    /// tailnet exists and how to reach hosts on it.
    pub fn guidance(&self) -> String {
        let org = self.tailnet_name.as_deref().unwrap_or(self.name.as_str());
        let suffix = self
            .magic_dns_suffix
            .as_deref()
            .map(|suffix| format!(" MagicDNS names end in .{suffix}."))
            .unwrap_or_default();
        format!(
            "This session has access to the Tailscale tailnet \"{org}\" (tailnet \"{name}\").{suffix} \
             HTTP and HTTPS requests to tailnet hosts work transparently through the configured proxy environment. \
             For SSH use `ssh -F \"${prefix}SSH_CONFIG\" <host>` (or `GIT_SSH_COMMAND=\"ssh -F ${prefix}SSH_CONFIG\"` for git). \
             Raw TCP clients can use the SOCKS5 proxy in ${prefix}SOCKS. \
             `\"${prefix}TAILSCALE_BIN\" --socket=\"${prefix}SOCKET\" status` lists reachable machines.",
            name = self.name,
            prefix = ENVIRONMENT_PREFIX,
        )
    }

    /// Nono `run`/`wrap` flags that let a sandboxed child reach the tailnet.
    ///
    /// The split proxy and SOCKS5 listener are loopback ports the child must
    /// be able to connect to; the control socket lets `tailscale --socket …
    /// nc` (the ssh `ProxyCommand`) work; the ssh config must be readable.
    /// With `chain_filtering_proxy`, Nono's own destination-filtering proxy
    /// is chained into the split proxy so HTTP(S) to tailnet hosts flows
    /// without the child knowing. Enable it only when the profile filters
    /// destinations: the flag switches Nono into proxy mode, which blocks
    /// direct loopback connections the agent may rely on.
    pub fn nono_arguments(&self, chain_filtering_proxy: bool) -> Vec<OsString> {
        let mut args: Vec<OsString> = Vec::new();
        if chain_filtering_proxy {
            args.extend([
                "--upstream-proxy".into(),
                self.proxy_addr.to_string().into(),
            ]);
        }
        for port in [self.proxy_addr.port(), self.socks_addr.port()] {
            args.extend(["--open-port".into(), port.to_string().into()]);
        }
        args.extend([
            "--allow-unix-socket".into(),
            self.socket_path.as_os_str().to_owned(),
        ]);
        args.extend([
            "--read-file".into(),
            self.ssh_config_path.as_os_str().to_owned(),
        ]);
        args
    }
}

/// Configuration for one tailnet daemon.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TailnetSpec {
    /// Application label, e.g. the client or organization.
    pub name: String,
    /// Node hostname advertised inside the tailnet.
    pub hostname: String,
    /// Private directory for `tailscaled` state, its socket, and the ssh config.
    pub state_dir: PathBuf,
    /// Daemon and CLI executables.
    pub binaries: TailscaleBinaries,
    /// Accept subnet routes advertised by the tailnet.
    pub accept_routes: bool,
    /// Upload daemon logs to Tailscale support. Off by default.
    pub upload_logs: bool,
}

impl TailnetSpec {
    /// Describe a tailnet whose node hostname is derived from `name`.
    pub fn new(
        name: impl Into<String>,
        state_dir: impl Into<PathBuf>,
        binaries: TailscaleBinaries,
    ) -> Result<Self, TailnetError> {
        let name = normalize_name(&name.into())?;
        let hostname = normalize_hostname(None, &name)?;
        let state_dir = state_dir.into();
        if !state_dir.is_absolute() {
            return Err(TailnetError::Invalid {
                field: "state_dir",
                message: format!("{} is not absolute", state_dir.display()),
            });
        }
        Ok(Self {
            name,
            hostname,
            state_dir,
            binaries,
            accept_routes: true,
            upload_logs: false,
        })
    }

    /// Use an explicit node hostname.
    pub fn with_hostname(mut self, hostname: impl AsRef<str>) -> Result<Self, TailnetError> {
        self.hostname = normalize_hostname(Some(hostname.as_ref()), &self.name)?;
        Ok(self)
    }
}

/// Validate and normalize a user-supplied tailnet label.
pub fn normalize_name(name: &str) -> Result<String, TailnetError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(TailnetError::Invalid {
            field: "name",
            message: "is required".into(),
        });
    }
    if name.chars().count() > MAX_NAME_LEN {
        return Err(TailnetError::Invalid {
            field: "name",
            message: format!("must be at most {MAX_NAME_LEN} characters"),
        });
    }
    if name.chars().any(char::is_control) {
        return Err(TailnetError::Invalid {
            field: "name",
            message: "cannot contain control characters".into(),
        });
    }
    Ok(name.to_string())
}

/// Tailscale hostnames are DNS labels: lowercase letters, digits, inner
/// hyphens. `None` derives `agent-<slug>` from the label.
pub fn normalize_hostname(hostname: Option<&str>, name: &str) -> Result<String, TailnetError> {
    let candidate = if let Some(value) = hostname.map(str::trim).filter(|value| !value.is_empty()) {
        value.to_ascii_lowercase()
    } else {
        let slug = name
            .to_ascii_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>();
        let slug = slug
            .split('-')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("-");
        if slug.is_empty() {
            "agent-tailnet".to_string()
        } else {
            format!("agent-{slug}")
        }
    };
    if candidate.len() > MAX_HOSTNAME_LEN {
        return Err(TailnetError::Invalid {
            field: "hostname",
            message: format!("must be at most {MAX_HOSTNAME_LEN} characters"),
        });
    }
    if !candidate
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        || candidate.starts_with('-')
        || candidate.ends_with('-')
    {
        return Err(TailnetError::Invalid {
            field: "hostname",
            message: "may only contain lowercase letters, digits, and inner hyphens".into(),
        });
    }
    Ok(candidate)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// The control socket lives inside the state directory unless that path
/// would exceed the 104-byte `sun_path` limit on macOS (common under
/// `~/Library/Application Support/...`), in which case it moves to the
/// temporary directory with a name derived from the state directory.
fn socket_path_for(state_dir: &Path) -> PathBuf {
    let preferred = state_dir.join("ts.sock");
    if cfg!(windows) || preferred.as_os_str().len() <= 100 {
        return preferred;
    }
    let digest = Sha256::digest(state_dir.as_os_str().as_encoded_bytes());
    let short = digest
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    std::env::temp_dir().join(format!("tailnet-{short}.sock"))
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

async fn free_loopback_port() -> std::io::Result<u16> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    Ok(listener.local_addr()?.port())
}

fn ssh_config_contents(
    name: &str,
    tailscale: &Path,
    socket_path: &Path,
    routes: &RouteTable,
) -> String {
    let mut patterns = vec!["*.ts.net".to_string(), "100.*".to_string()];
    if let Some(suffix) = &routes.magic_dns_suffix {
        patterns.push(format!("*.{suffix}"));
    }
    patterns.extend(routes.host_names.iter().cloned());
    format!(
        "# Generated by temps-agent-runtime for tailnet \"{}\". Do not edit; it is rewritten on every status refresh.\n\
         Host {}\n\
         \x20   ProxyCommand {} --socket={} nc %h %p\n\
         \x20   # tailscaled resolves MagicDNS names inside the tunnel.\n\
         \x20   CheckHostIP no\n\
         \n\
         Include ~/.ssh/config\n",
        name.replace('"', "'"),
        patterns.join(" "),
        tailscale.display(),
        socket_path.display(),
    )
}

struct DaemonState {
    tailscale: Option<StatusJson>,
    auth_url: Option<String>,
    /// Detail that overrides the derived one, e.g. a login failure.
    detail_override: Option<String>,
    updated_at_ms: u64,
}

struct Inner {
    spec: TailnetSpec,
    socket_path: PathBuf,
    ssh_config_path: PathBuf,
    socks_addr: SocketAddr,
    supervisor: ManagedProcessSupervisor,
    process: ManagedProcessId,
    proxy: Mutex<Option<Arc<SplitProxy>>>,
    state: RwLock<DaemonState>,
    login: Mutex<Option<tokio::task::JoinHandle<()>>>,
    refresh: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self
            .refresh
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
        // The supervisor is dropped with `Inner`; it owns the tailscaled
        // process tree and stops it.
    }
}

/// One supervised userspace `tailscaled` plus its split proxy.
///
/// Cloning shares the daemon; dropping the last clone stops it.
#[derive(Clone)]
pub struct TailnetDaemon {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TailnetDaemon {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TailnetDaemon")
            .field("name", &self.inner.spec.name)
            .field("hostname", &self.inner.spec.hostname)
            .field("state_dir", &self.inner.spec.state_dir)
            .field("socks_addr", &self.inner.socks_addr)
            .finish()
    }
}

impl TailnetDaemon {
    /// Start `tailscaled` for `spec` and return once it is supervised.
    ///
    /// Returns as soon as the daemon's control socket answers; the tailnet
    /// itself may still need a login, see [`Self::status`] and
    /// [`Self::login`]. Existing state in `spec.state_dir` is reused, so a
    /// previously logged-in tailnet reconnects without a new login.
    pub async fn start(spec: TailnetSpec) -> Result<Self, TailnetError> {
        prepare_state_dir(&spec.state_dir).await?;
        let socket_path = socket_path_for(&spec.state_dir);
        // A stale socket from a crashed daemon makes the new one refuse to
        // start.
        let _ = tokio::fs::remove_file(&socket_path).await;
        let socks_port = free_loopback_port().await.map_err(TailnetError::Proxy)?;
        let socks_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, socks_port));

        let mut process_spec = ManagedProcessSpec::service(
            format!("tailscaled ({})", spec.name),
            &spec.binaries.tailscaled,
            &spec.state_dir,
        )
        .restart_policy(RestartPolicy::Always)
        .args([
            OsString::from("--tun=userspace-networking"),
            format!(
                "--state={}",
                spec.state_dir.join("tailscaled.state").display()
            )
            .into(),
            format!("--statedir={}", spec.state_dir.display()).into(),
            format!("--socket={}", socket_path.display()).into(),
            "--port=0".into(),
            format!("--socks5-server=127.0.0.1:{socks_port}").into(),
        ]);
        if !spec.upload_logs {
            process_spec = process_spec.arg("--no-logs-no-support");
        }
        let supervisor = ManagedProcessSupervisor::builder()
            .max_processes(1)
            .restart_delay(Duration::from_secs(2))
            .build()?;
        let handle = supervisor.start(process_spec).await?;
        let process = handle.id().clone();
        drop(handle);

        let inner = Arc::new(Inner {
            ssh_config_path: spec.state_dir.join("ssh_config"),
            spec,
            socket_path,
            socks_addr,
            supervisor,
            process,
            proxy: Mutex::new(None),
            state: RwLock::new(DaemonState {
                tailscale: None,
                auth_url: None,
                detail_override: None,
                updated_at_ms: now_ms(),
            }),
            login: Mutex::new(None),
            refresh: StdMutex::new(None),
        });
        let daemon = Self { inner };
        daemon.await_socket().await?;
        daemon.ensure_proxy().await?;
        // A daemon that is up but not answering yet is reported as
        // `Starting`; the refresh loop keeps polling.
        let _ = daemon.refresh().await;
        daemon.spawn_refresh_loop();
        Ok(daemon)
    }

    /// Specification this daemon was started with.
    pub fn spec(&self) -> &TailnetSpec {
        &self.inner.spec
    }

    /// Control socket of the daemon.
    pub fn socket_path(&self) -> &Path {
        &self.inner.socket_path
    }

    async fn await_socket(&self) -> Result<(), TailnetError> {
        let deadline = tokio::time::Instant::now() + SOCKET_WAIT;
        while tokio::time::Instant::now() < deadline {
            if self.inner.socket_path.exists() {
                return Ok(());
            }
            if let Ok(snapshot) = self.inner.supervisor.snapshot(&self.inner.process).await {
                if snapshot.status == ManagedProcessStatus::Failed {
                    return Err(TailnetError::Cli {
                        command: "tailscaled".into(),
                        message: snapshot.detail,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(TailnetError::SocketTimeout {
            path: self.inner.socket_path.clone(),
            timeout: SOCKET_WAIT,
        })
    }

    async fn ensure_proxy(&self) -> Result<(), TailnetError> {
        let mut proxy = self.inner.proxy.lock().await;
        if proxy.is_none() {
            let started = SplitProxy::start(
                SocksUpstream {
                    addr: self.inner.socks_addr,
                },
                RouteTable::default(),
            )
            .await
            .map_err(TailnetError::Proxy)?;
            *proxy = Some(Arc::new(started));
        }
        Ok(())
    }

    fn spawn_refresh_loop(&self) {
        let weak = Arc::downgrade(&self.inner);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(STATUS_REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(inner) = weak.upgrade() else { return };
                let daemon = TailnetDaemon { inner };
                let _ = daemon.refresh().await;
            }
        });
        *self
            .inner
            .refresh
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
    }

    /// Query `tailscale status` now, update the split proxy's routes and the
    /// generated ssh config, and return the new status.
    pub async fn refresh(&self) -> Result<TailnetStatus, TailnetError> {
        let output = tailscale::run_cli(
            &self.inner.spec.binaries.tailscale,
            &self.inner.socket_path,
            &["status", "--json"],
            CLI_TIMEOUT,
        )
        .await?;
        let status = StatusJson::parse(&output).map_err(|error| TailnetError::Cli {
            command: "tailscale status --json".into(),
            message: format!("could not parse output: {error}"),
        })?;
        let routes = status.route_table();
        if let Some(proxy) = self.inner.proxy.lock().await.as_ref() {
            proxy.set_routes(routes.clone()).await;
        }
        self.write_ssh_config(&routes).await;
        {
            let mut state = self.inner.state.write().await;
            if status.is_running() {
                state.auth_url = None;
                state.detail_override = None;
            } else if status.needs_login() && !status.auth_url.is_empty() {
                state.auth_url = Some(status.auth_url.clone());
            }
            state.tailscale = Some(status);
            state.updated_at_ms = now_ms();
        }
        Ok(self.status().await)
    }

    async fn write_ssh_config(&self, routes: &RouteTable) {
        let contents = ssh_config_contents(
            &self.inner.spec.name,
            &self.inner.spec.binaries.tailscale,
            &self.inner.socket_path,
            routes,
        );
        let current = tokio::fs::read_to_string(&self.inner.ssh_config_path)
            .await
            .unwrap_or_default();
        if current != contents {
            let _ = tokio::fs::write(&self.inner.ssh_config_path, contents).await;
        }
    }

    /// Current status without querying the daemon.
    pub async fn status(&self) -> TailnetStatus {
        let snapshot = self
            .inner
            .supervisor
            .snapshot(&self.inner.process)
            .await
            .ok();
        let proxy_port = self
            .inner
            .proxy
            .lock()
            .await
            .as_ref()
            .map(|proxy| proxy.local_addr().port());
        let state = self.inner.state.read().await;
        let tailscale = state.tailscale.as_ref();
        let (lifecycle, mut detail) = match snapshot.as_ref().map(|snapshot| snapshot.status) {
            Some(ManagedProcessStatus::Running) => match tailscale {
                Some(status) if status.is_running() => (
                    TailnetState::Running,
                    match status.tailnet_name() {
                        Some(name) => format!("Connected to {name}"),
                        None => "Connected".to_string(),
                    },
                ),
                Some(status) if status.needs_login() => (
                    TailnetState::NeedsLogin,
                    "Log in to a Tailscale account to join a tailnet".to_string(),
                ),
                Some(status) => (
                    TailnetState::Starting,
                    format!("tailscaled is {}", status.backend_state),
                ),
                None => (
                    TailnetState::Starting,
                    "Waiting for tailscaled to answer".to_string(),
                ),
            },
            Some(ManagedProcessStatus::Queued) => (
                TailnetState::Starting,
                snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.detail.clone())
                    .unwrap_or_default(),
            ),
            Some(ManagedProcessStatus::Failed) => (
                TailnetState::Failed,
                snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.detail.clone())
                    .unwrap_or_default(),
            ),
            Some(ManagedProcessStatus::Cancelled | ManagedProcessStatus::Succeeded) | None => (
                TailnetState::Stopped,
                snapshot.as_ref().map_or_else(
                    || "tailscaled is not running".to_string(),
                    |snapshot| snapshot.detail.clone(),
                ),
            ),
        };
        if let Some(override_detail) = &state.detail_override {
            detail.clone_from(override_detail);
        }
        let running = lifecycle == TailnetState::Running;
        TailnetStatus {
            state: lifecycle,
            detail,
            auth_url: if running {
                None
            } else {
                state.auth_url.clone()
            },
            tailnet_name: tailscale.and_then(StatusJson::tailnet_name),
            magic_dns_suffix: tailscale.and_then(StatusJson::magic_dns_suffix),
            self_dns_name: tailscale.and_then(StatusJson::self_dns_name),
            self_ips: tailscale.map(StatusJson::self_ips).unwrap_or_default(),
            online_peers: tailscale.map_or(0, |status| saturating_u32(status.online_peer_count())),
            total_peers: tailscale.map_or(0, |status| saturating_u32(status.peer.len())),
            pid: snapshot.as_ref().and_then(|snapshot| snapshot.pid),
            restart_count: snapshot
                .as_ref()
                .map_or(0, |snapshot| snapshot.restart_count),
            proxy_port,
            socks_port: self.inner.socks_addr.port(),
            updated_at_ms: state
                .updated_at_ms
                .max(snapshot.map_or(0, |snapshot| snapshot.updated_at_ms)),
        }
    }

    /// Start or resume a browser login.
    ///
    /// Runs `tailscale up --json` in the background and returns as soon as
    /// the login URL is known (or after a short wait if the daemon is slow).
    /// The host shows `auth_url` to the user; the status switches to
    /// [`TailnetState::Running`] on its own once the node is approved.
    pub async fn login(&self) -> Result<TailnetStatus, TailnetError> {
        if self.status().await.state == TailnetState::Running {
            return Ok(self.status().await);
        }
        let mut login = self.inner.login.lock().await;
        let active = login.as_ref().is_some_and(|task| !task.is_finished());
        if !active {
            {
                let mut state = self.inner.state.write().await;
                state.auth_url = None;
                state.detail_override = Some("Requesting a login URL from Tailscale".into());
                state.updated_at_ms = now_ms();
            }
            let daemon = self.clone();
            *login = Some(tokio::spawn(async move { daemon.run_login().await }));
        }
        drop(login);
        let deadline = tokio::time::Instant::now() + LOGIN_URL_WAIT;
        while tokio::time::Instant::now() < deadline {
            let status = self.status().await;
            if status.auth_url.is_some()
                || matches!(status.state, TailnetState::Running | TailnetState::Failed)
            {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(self.status().await)
    }

    async fn run_login(&self) {
        let spec = &self.inner.spec;
        let mut command = tokio::process::Command::new(&spec.binaries.tailscale);
        command
            .arg(format!("--socket={}", self.inner.socket_path.display()))
            .arg("up")
            .arg("--json")
            .arg("--reset")
            .arg(format!("--hostname={}", spec.hostname))
            .arg("--accept-dns=false")
            .arg(format!("--accept-routes={}", spec.accept_routes))
            .arg(format!("--timeout={}s", LOGIN_TIMEOUT.as_secs()))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.set_detail(format!("Could not run tailscale up: {error}"))
                    .await;
                return;
            }
        };
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let for_stdout = self.clone();
        let stdout_task = tokio::spawn(async move {
            let Some(stdout) = stdout else { return };
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let url = UpJsonLine::parse(&line)
                    .map(|parsed| parsed.auth_url)
                    .filter(|url| !url.is_empty())
                    .or_else(|| tailscale::extract_auth_url(&line));
                if let Some(url) = url {
                    for_stdout.set_auth_url(url).await;
                }
            }
        });
        let for_stderr = self.clone();
        let stderr_task = tokio::spawn(async move {
            let Some(stderr) = stderr else {
                return String::new();
            };
            let mut lines = BufReader::new(stderr).lines();
            let mut last = String::new();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(url) = tailscale::extract_auth_url(&line) {
                    for_stderr.set_auth_url(url).await;
                } else if !line.trim().is_empty() {
                    last = line.chars().take(400).collect();
                }
            }
            last
        });
        let status = child.wait().await;
        let _ = stdout_task.await;
        let stderr_tail = stderr_task.await.unwrap_or_default();
        match status {
            Ok(status) if status.success() => {
                if let Err(error) = self.refresh().await {
                    self.set_detail(format!("Login finished but status is unavailable: {error}"))
                        .await;
                }
            }
            Ok(status) => {
                let mut state = self.inner.state.write().await;
                state.auth_url = None;
                state.detail_override = Some(if stderr_tail.is_empty() {
                    format!("Login did not complete (tailscale up exited with {status}); start it again")
                } else {
                    format!("Login did not complete: {stderr_tail}")
                });
                state.updated_at_ms = now_ms();
            }
            Err(error) => {
                self.set_detail(format!("Could not wait for tailscale up: {error}"))
                    .await;
            }
        }
    }

    async fn set_auth_url(&self, url: String) {
        let mut state = self.inner.state.write().await;
        state.auth_url = Some(url);
        state.detail_override = Some(
            "Open the login URL in a browser signed into the Tailscale account for this tailnet"
                .into(),
        );
        state.updated_at_ms = now_ms();
    }

    async fn set_detail(&self, detail: String) {
        let mut state = self.inner.state.write().await;
        state.detail_override = Some(detail);
        state.updated_at_ms = now_ms();
    }

    /// Log the node out of its tailnet so no device is left behind.
    pub async fn logout(&self) -> Result<(), TailnetError> {
        if let Some(task) = self.inner.login.lock().await.take() {
            task.abort();
        }
        tailscale::run_cli(
            &self.inner.spec.binaries.tailscale,
            &self.inner.socket_path,
            &["logout"],
            CLI_TIMEOUT,
        )
        .await?;
        let _ = self.refresh().await;
        Ok(())
    }

    /// Stop `tailscaled` and the split proxy. State on disk is kept.
    pub async fn stop(&self) -> Result<TailnetStatus, TailnetError> {
        if let Some(task) = self.inner.login.lock().await.take() {
            task.abort();
        }
        self.inner.supervisor.stop(&self.inner.process).await?;
        self.inner.proxy.lock().await.take();
        {
            let mut state = self.inner.state.write().await;
            state.auth_url = None;
            state.tailscale = None;
            state.detail_override = None;
            state.updated_at_ms = now_ms();
        }
        Ok(self.status().await)
    }

    /// Stop and relaunch `tailscaled` with the same specification.
    pub async fn restart(&self) -> Result<TailnetStatus, TailnetError> {
        let _ = self.stop().await;
        let _ = tokio::fs::remove_file(&self.inner.socket_path).await;
        self.inner.supervisor.restart(&self.inner.process).await?;
        self.await_socket().await?;
        self.ensure_proxy().await?;
        let _ = self.refresh().await;
        Ok(self.status().await)
    }

    /// Endpoints for a provider launch, or why the tailnet cannot serve one.
    pub async fn access(&self) -> Result<TailnetAccess, TailnetError> {
        let status = self.status().await;
        if status.state != TailnetState::Running {
            return Err(TailnetError::NotConnected {
                name: self.inner.spec.name.clone(),
                state: status.state,
                detail: status.detail,
            });
        }
        let Some(proxy_port) = status.proxy_port else {
            return Err(TailnetError::NotConnected {
                name: self.inner.spec.name.clone(),
                state: status.state,
                detail: "the split proxy is not listening".into(),
            });
        };
        Ok(TailnetAccess {
            name: self.inner.spec.name.clone(),
            tailnet_name: status.tailnet_name,
            magic_dns_suffix: status.magic_dns_suffix,
            proxy_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, proxy_port)),
            socks_addr: self.inner.socks_addr,
            socket_path: self.inner.socket_path.clone(),
            tailscale: self.inner.spec.binaries.tailscale.clone(),
            ssh_config_path: self.inner.ssh_config_path.clone(),
        })
    }
}

async fn prepare_state_dir(path: &Path) -> Result<(), TailnetError> {
    tokio::fs::create_dir_all(path)
        .await
        .map_err(|source| TailnetError::StateDirectory {
            path: path.to_path_buf(),
            source,
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|source| TailnetError::StateDirectory {
                path: path.to_path_buf(),
                source,
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access() -> TailnetAccess {
        TailnetAccess {
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

    #[test]
    fn environment_routes_http_through_proxy_and_keeps_loopback_direct() {
        let environment = access().environment();
        let value = |name: &str| {
            environment
                .get(&OsString::from(name))
                .map(|value| value.to_string_lossy().into_owned())
        };
        assert_eq!(
            value("HTTPS_PROXY").as_deref(),
            Some("http://127.0.0.1:41001")
        );
        assert_eq!(
            value("https_proxy").as_deref(),
            Some("http://127.0.0.1:41001")
        );
        assert_eq!(
            value("NO_PROXY").as_deref(),
            Some("localhost,127.0.0.1,::1")
        );
        assert_eq!(
            value("TEMPS_TAILNET_SOCKS").as_deref(),
            Some("socks5h://127.0.0.1:41002")
        );
        assert_eq!(value("TEMPS_TAILNET_ORG").as_deref(), Some("gala.games"));
        assert_eq!(
            value("TEMPS_TAILNET_DNS_SUFFIX").as_deref(),
            Some("jerboa-altered.ts.net")
        );
        assert!(environment.keys().all(|key| {
            let key = key.to_string_lossy();
            key.starts_with(ENVIRONMENT_PREFIX)
                || key.to_ascii_uppercase().ends_with("_PROXY")
                || key == "NODE_USE_ENV_PROXY"
        }));
    }

    #[test]
    fn apply_adds_environment_without_touching_arguments() {
        let mut spec = CommandSpec::new("claude");
        spec.args = vec!["--print".into()];
        let applied = access().apply(spec);
        assert_eq!(applied.args, vec![OsString::from("--print")]);
        assert!(applied
            .environment
            .contains_key(&OsString::from("TEMPS_TAILNET_SSH_CONFIG")));
    }

    #[test]
    fn nono_arguments_open_ports_socket_and_ssh_config() {
        let args = access().nono_arguments(false);
        let text = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(!text.contains(&"--upstream-proxy".to_string()));
        assert!(text.windows(2).any(|w| w == ["--open-port", "41001"]));
        assert!(text.windows(2).any(|w| w == ["--open-port", "41002"]));
        assert!(text
            .windows(2)
            .any(|w| w == ["--allow-unix-socket", "/data/tailnets/abcd/ts.sock"]));
        assert!(text
            .windows(2)
            .any(|w| w == ["--read-file", "/data/tailnets/abcd/ssh_config"]));
        let chained = access().nono_arguments(true);
        assert_eq!(chained[0], OsString::from("--upstream-proxy"));
        assert_eq!(chained[1], OsString::from("127.0.0.1:41001"));
    }

    #[test]
    fn guidance_names_the_tailnet_and_ssh_config() {
        let guidance = access().guidance();
        assert!(guidance.contains("gala.games"));
        assert!(guidance.contains(".jerboa-altered.ts.net"));
        assert!(guidance.contains("TEMPS_TAILNET_SSH_CONFIG"));
    }

    #[test]
    fn hostname_defaults_to_a_slug_of_the_name() {
        assert_eq!(
            normalize_hostname(None, "Gala Games").unwrap(),
            "agent-gala-games"
        );
        assert_eq!(
            normalize_hostname(None, "  ---  ").unwrap(),
            "agent-tailnet"
        );
        assert_eq!(
            normalize_hostname(Some("Custom-Node"), "x").unwrap(),
            "custom-node"
        );
        assert!(normalize_hostname(Some("bad host"), "x").is_err());
        assert!(normalize_hostname(Some("-lead"), "x").is_err());
        assert!(normalize_hostname(Some(&"a".repeat(64)), "x").is_err());
    }

    #[test]
    fn names_are_trimmed_and_bounded() {
        assert_eq!(normalize_name("  gala  ").unwrap(), "gala");
        assert!(normalize_name("").is_err());
        assert!(normalize_name("bad\u{7}name").is_err());
        assert!(normalize_name(&"n".repeat(65)).is_err());
    }

    #[test]
    fn spec_requires_absolute_state_dir_and_validates_hostname() {
        let temp = tempfile::tempdir().unwrap();
        let tailscaled = temp.path().join("tailscaled");
        let tailscale = temp.path().join("tailscale");
        std::fs::write(&tailscaled, "stub").unwrap();
        std::fs::write(&tailscale, "stub").unwrap();
        let binaries = TailscaleBinaries::new(&tailscaled, &tailscale).unwrap();
        assert!(TailnetSpec::new("gala", "relative/dir", binaries.clone()).is_err());
        let spec = TailnetSpec::new("Gala", temp.path().join("state"), binaries).unwrap();
        assert_eq!(spec.hostname, "agent-gala");
        assert!(!spec.upload_logs);
        assert!(spec.accept_routes);
        assert!(spec.with_hostname("not valid").is_err());
    }

    #[test]
    fn socket_path_falls_back_to_tmp_when_state_dir_is_deep() {
        assert_eq!(
            socket_path_for(Path::new("/tmp/agent")),
            PathBuf::from("/tmp/agent/ts.sock")
        );
        let deep = PathBuf::from(format!("/{}", "long-segment/".repeat(12)));
        let fallback = socket_path_for(&deep);
        if cfg!(windows) {
            assert!(fallback.starts_with(&deep));
        } else {
            assert!(fallback
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("tailnet-"));
            assert!(fallback.as_os_str().len() <= 100);
        }
    }

    #[test]
    fn ssh_config_routes_tailnet_patterns_through_tailscale_nc() {
        let routes = RouteTable {
            magic_dns_suffix: Some("jerboa-altered.ts.net".into()),
            dns_names: vec![],
            host_names: vec!["grafana".into()],
        };
        let config = ssh_config_contents(
            "Gala \"prod\"",
            Path::new("/opt/homebrew/bin/tailscale"),
            Path::new("/data/tailnets/id/ts.sock"),
            &routes,
        );
        assert!(config.contains("Host *.ts.net 100.* *.jerboa-altered.ts.net grafana\n"));
        assert!(config.contains(
            "ProxyCommand /opt/homebrew/bin/tailscale --socket=/data/tailnets/id/ts.sock nc %h %p"
        ));
        assert!(config.contains("Include ~/.ssh/config"));
        assert!(config.contains("for tailnet \"Gala 'prod'\""));
    }

    /// A stub `tailscaled` that creates the socket file and sleeps lets the
    /// lifecycle be exercised without Tailscale installed.
    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_starts_supervises_and_stops_a_stub_tailscaled() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let tailscaled = temp.path().join("tailscaled");
        std::fs::write(
            &tailscaled,
            "#!/bin/sh\nfor arg in \"$@\"; do case \"$arg\" in --socket=*) sock=\"${arg#--socket=}\";; esac; done\ntouch \"$sock\"\nsleep 60\n",
        )
        .unwrap();
        let tailscale = temp.path().join("tailscale");
        std::fs::write(
            &tailscale,
            "#!/bin/sh\necho '{\"BackendState\":\"NeedsLogin\",\"AuthURL\":\"https://login.tailscale.com/a/stub\",\"Peer\":null}'\n",
        )
        .unwrap();
        for path in [&tailscaled, &tailscale] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let binaries = TailscaleBinaries::new(&tailscaled, &tailscale).unwrap();
        let spec = TailnetSpec::new("stub", temp.path().join("state"), binaries).unwrap();
        let daemon = TailnetDaemon::start(spec).await.unwrap();
        let status = daemon.status().await;
        assert_eq!(status.state, TailnetState::NeedsLogin, "{status:?}");
        assert!(status.pid.is_some());
        assert!(status.proxy_port.is_some());
        assert_eq!(
            status.auth_url.as_deref(),
            Some("https://login.tailscale.com/a/stub")
        );
        assert!(daemon.access().await.is_err());
        assert!(temp.path().join("state/ssh_config").is_file());
        let stopped = daemon.stop().await.unwrap();
        assert_eq!(stopped.state, TailnetState::Stopped);
        assert!(stopped.pid.is_none());
    }
}
