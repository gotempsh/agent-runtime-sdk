//! Owned background commands and long-running managed services.
//!
//! A managed process belongs to [`ManagedProcessSupervisor`], not to an agent
//! turn. This lets a development server or worker survive turn completion while
//! preserving an explicit authority that can inspect, restart, and stop its
//! complete process tree.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};

use crate::{
    CommandSpec, ExecutionTransport, LocalTransport, SecretString, TransportError,
    TransportProcessHandle, TransportSpawnRequest,
};

const DEFAULT_MAX_PROCESSES: usize = 32;
const DEFAULT_MAX_LOG_LINES: usize = 1_000;
const DEFAULT_MAX_LOG_LINE_CHARS: usize = 4_000;
const DEFAULT_EVENT_CAPACITY: usize = 256;
const DEFAULT_RESTART_DELAY: Duration = Duration::from_secs(1);
const START_TIMEOUT: Duration = Duration::from_secs(5);

/// Result returned by managed-process operations.
pub type ManagedProcessResult<T> = std::result::Result<T, ManagedProcessError>;

/// Stable identifier assigned to one managed process.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManagedProcessId(String);

impl ManagedProcessId {
    /// Return the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ManagedProcessId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Restart behavior after a managed process exits on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum RestartPolicy {
    /// Never restart after exit.
    Never,
    /// Restart only after an unsuccessful exit.
    OnFailure,
    /// Restart after every exit.
    Always,
}

/// Lifecycle status of a managed process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ManagedProcessStatus {
    /// Waiting to be spawned or restarted.
    Queued,
    /// The child process is alive.
    Running,
    /// The child exited successfully and will not restart.
    Succeeded,
    /// The child failed to spawn or exited unsuccessfully.
    Failed,
    /// An owner explicitly stopped the child.
    Cancelled,
}

/// Direct executable specification for a background command or service.
///
/// Arguments remain separate OS strings. Environment values are redacted from
/// `Debug` output and are never included in snapshots or stream events.
#[derive(Clone)]
pub struct ManagedProcessSpec {
    /// Human-readable name used in status messages.
    pub name: String,
    /// Executable path or name.
    pub program: PathBuf,
    /// Arguments passed directly to the executable.
    pub args: Vec<OsString>,
    /// Canonical working directory.
    pub working_directory: PathBuf,
    /// Explicit environment additions.
    pub environment: BTreeMap<OsString, SecretString>,
    /// Remove the host environment before applying the SDK safe allowlist.
    pub clear_environment: bool,
    /// Restart behavior after exit.
    pub restart_policy: RestartPolicy,
}

impl fmt::Debug for ManagedProcessSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedProcessSpec")
            .field("name", &self.name)
            .field("program", &self.program)
            .field("args", &self.args)
            .field("working_directory", &self.working_directory)
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("clear_environment", &self.clear_environment)
            .field("restart_policy", &self.restart_policy)
            .finish()
    }
}

impl ManagedProcessSpec {
    /// Create a one-shot background command.
    pub fn background(
        name: impl Into<String>,
        program: impl Into<PathBuf>,
        working_directory: impl Into<PathBuf>,
    ) -> Self {
        Self::new(name, program, working_directory, RestartPolicy::Never)
    }

    /// Create a long-running service that restarts after failure.
    pub fn service(
        name: impl Into<String>,
        program: impl Into<PathBuf>,
        working_directory: impl Into<PathBuf>,
    ) -> Self {
        Self::new(name, program, working_directory, RestartPolicy::OnFailure)
    }

    fn new(
        name: impl Into<String>,
        program: impl Into<PathBuf>,
        working_directory: impl Into<PathBuf>,
        restart_policy: RestartPolicy,
    ) -> Self {
        Self {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            working_directory: working_directory.into(),
            environment: BTreeMap::new(),
            clear_environment: true,
            restart_policy,
        }
    }

    /// Append one argument without passing through a shell.
    pub fn arg(mut self, argument: impl Into<OsString>) -> Self {
        self.args.push(argument.into());
        self
    }

    /// Append multiple arguments without passing through a shell.
    pub fn args(mut self, arguments: impl IntoIterator<Item = impl Into<OsString>>) -> Self {
        self.args.extend(arguments.into_iter().map(Into::into));
        self
    }

    /// Add an environment variable whose value remains redacted from debug output.
    pub fn environment(mut self, name: impl Into<OsString>, value: SecretString) -> Self {
        self.environment.insert(name.into(), value);
        self
    }

    /// Override restart behavior.
    pub fn restart_policy(mut self, restart_policy: RestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
    }
}

/// Current observable state of a managed process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessSnapshot {
    /// Stable SDK identifier.
    pub id: ManagedProcessId,
    /// Human-readable name.
    pub name: String,
    /// Working directory used at spawn time.
    pub working_directory: PathBuf,
    /// Configured restart behavior.
    pub restart_policy: RestartPolicy,
    /// Current lifecycle state.
    pub status: ManagedProcessStatus,
    /// Latest actionable progress or failure detail.
    pub detail: String,
    /// Current OS process identifier when running.
    pub pid: Option<u32>,
    /// Latest native local or remote process identity.
    pub transport_handle: Option<TransportProcessHandle>,
    /// Number of automatic or explicit restarts.
    pub restart_count: u32,
    /// Creation time as Unix milliseconds.
    pub created_at_ms: u64,
    /// Last state-change time as Unix milliseconds.
    pub updated_at_ms: u64,
}

/// One bounded stdout or stderr line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedProcessLogLine {
    /// Monotonic sequence local to this process record.
    pub sequence: u64,
    /// Capture time as Unix milliseconds.
    pub timestamp_ms: u64,
    /// `stdout` or `stderr`.
    pub stream: String,
    /// Bounded line content.
    pub text: String,
}

/// Live event emitted by a managed process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ManagedProcessEvent {
    /// Lifecycle state changed.
    StatusChanged {
        /// Complete current snapshot.
        snapshot: ManagedProcessSnapshot,
    },
    /// A new bounded stdout or stderr line arrived.
    Log {
        /// Owning process identifier.
        process_id: ManagedProcessId,
        /// Captured line.
        line: ManagedProcessLogLine,
    },
    /// The supervisor will restart the process after a bounded delay.
    RestartScheduled {
        /// Owning process identifier.
        process_id: ManagedProcessId,
        /// One-based restart attempt.
        attempt: u32,
        /// Delay before the next spawn.
        delay_ms: u64,
    },
}

/// Failure reported by the managed-process supervisor.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ManagedProcessError {
    /// A process specification was unsafe or invalid.
    #[error("invalid managed process {field}: {message}")]
    InvalidSpec {
        /// Rejected field.
        field: &'static str,
        /// Actionable explanation.
        message: String,
    },
    /// The configured process limit was reached.
    #[error("managed process limit reached ({limit})")]
    LimitReached {
        /// Configured maximum.
        limit: usize,
    },
    /// No record exists for the requested identifier.
    #[error("managed process {id} was not found")]
    NotFound {
        /// Requested identifier.
        id: ManagedProcessId,
    },
    /// The executable could not be started.
    #[error("could not start managed process {name}: {source}")]
    Spawn {
        /// Process name.
        name: String,
        /// Typed local or remote transport failure.
        #[source]
        source: TransportError,
    },
    /// The execution transport rejected preparation or lifecycle control.
    #[error("managed process {name} transport failed: {source}")]
    Transport {
        /// Process display name.
        name: String,
        /// Typed transport failure suitable for recovery UI.
        #[source]
        source: TransportError,
    },
    /// A lifecycle control message could not be delivered.
    #[error("managed process {id} is no longer supervised")]
    SupervisorClosed {
        /// Process identifier.
        id: ManagedProcessId,
    },
    /// A bounded lifecycle operation timed out.
    #[error("managed process {id} {operation} timed out")]
    Timeout {
        /// Process identifier.
        id: ManagedProcessId,
        /// Operation that timed out.
        operation: &'static str,
    },
}

/// Error returned while receiving live managed-process events.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ManagedProcessStreamError {
    /// The receiver fell behind the bounded event buffer.
    #[error("managed process stream lagged by {skipped} events")]
    Lagged {
        /// Number of skipped events.
        skipped: u64,
    },
    /// The managed process record was deleted or the supervisor shut down.
    #[error("managed process stream closed")]
    Closed,
}

/// Receiver for one managed process's live events.
pub struct ManagedProcessEventStream {
    receiver: broadcast::Receiver<ManagedProcessEvent>,
}

impl ManagedProcessEventStream {
    /// Wait for the next event.
    pub async fn recv(
        &mut self,
    ) -> std::result::Result<ManagedProcessEvent, ManagedProcessStreamError> {
        self.receiver.recv().await.map_err(|error| match error {
            broadcast::error::RecvError::Closed => ManagedProcessStreamError::Closed,
            broadcast::error::RecvError::Lagged(skipped) => {
                ManagedProcessStreamError::Lagged { skipped }
            }
        })
    }
}

#[derive(Debug)]
struct ProcessState {
    status: ManagedProcessStatus,
    detail: String,
    pid: Option<u32>,
    transport_handle: Option<TransportProcessHandle>,
    restart_count: u32,
    updated_at_ms: u64,
}

struct ProcessEntry {
    id: ManagedProcessId,
    spec: ManagedProcessSpec,
    created_at_ms: u64,
    state: RwLock<ProcessState>,
    logs: Mutex<VecDeque<ManagedProcessLogLine>>,
    next_log_sequence: std::sync::atomic::AtomicU64,
    events: broadcast::Sender<ManagedProcessEvent>,
    control: StdMutex<Option<mpsc::Sender<ProcessControl>>>,
}

enum ProcessControl {
    Stop(Option<oneshot::Sender<()>>),
}

struct SupervisorInner {
    entries: StdMutex<HashMap<ManagedProcessId, Arc<ProcessEntry>>>,
    max_processes: usize,
    max_log_lines: usize,
    max_log_line_chars: usize,
    event_capacity: usize,
    restart_delay: Duration,
    transport: Arc<dyn ExecutionTransport>,
}

struct SupervisorLimits {
    max_log_lines: usize,
    max_log_line_chars: usize,
    restart_delay: Duration,
    transport: Arc<dyn ExecutionTransport>,
}

impl Drop for SupervisorInner {
    fn drop(&mut self) {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in entries.values() {
            if let Some(control) = entry
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                let _ = control.try_send(ProcessControl::Stop(None));
            }
        }
    }
}

/// Builder for an owned, bounded managed-process supervisor.
pub struct ManagedProcessSupervisorBuilder {
    max_processes: usize,
    max_log_lines: usize,
    max_log_line_chars: usize,
    event_capacity: usize,
    restart_delay: Duration,
    transport: Arc<dyn ExecutionTransport>,
}

impl ManagedProcessSupervisorBuilder {
    /// Create a builder with conservative defaults.
    pub fn new() -> Self {
        Self {
            max_processes: DEFAULT_MAX_PROCESSES,
            max_log_lines: DEFAULT_MAX_LOG_LINES,
            max_log_line_chars: DEFAULT_MAX_LOG_LINE_CHARS,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            restart_delay: DEFAULT_RESTART_DELAY,
            transport: Arc::new(LocalTransport),
        }
    }

    /// Set the maximum number of retained process records.
    pub fn max_processes(mut self, value: usize) -> Self {
        self.max_processes = value;
        self
    }

    /// Set the maximum retained log lines per process.
    pub fn max_log_lines(mut self, value: usize) -> Self {
        self.max_log_lines = value;
        self
    }

    /// Set the maximum characters retained from one log line.
    pub fn max_log_line_chars(mut self, value: usize) -> Self {
        self.max_log_line_chars = value;
        self
    }

    /// Set the bounded live-event channel capacity per process.
    pub fn event_capacity(mut self, value: usize) -> Self {
        self.event_capacity = value;
        self
    }

    /// Set the delay before an automatic restart.
    pub fn restart_delay(mut self, value: Duration) -> Self {
        self.restart_delay = value;
        self
    }

    /// Execute managed processes through a configured local or remote transport.
    pub fn transport(mut self, transport: impl ExecutionTransport + 'static) -> Self {
        self.transport = Arc::new(transport);
        self
    }

    /// Execute managed processes through an already shared transport.
    pub fn transport_from_arc(mut self, transport: Arc<dyn ExecutionTransport>) -> Self {
        self.transport = transport;
        self
    }

    /// Validate limits and construct a supervisor.
    pub fn build(self) -> ManagedProcessResult<ManagedProcessSupervisor> {
        if self.max_processes == 0
            || self.max_log_lines == 0
            || self.max_log_line_chars == 0
            || self.event_capacity == 0
        {
            return Err(ManagedProcessError::InvalidSpec {
                field: "supervisor limits",
                message: "all limits must be greater than zero".to_string(),
            });
        }
        let capabilities = self.transport.capabilities();
        if !capabilities.managed_processes || !capabilities.process_tree_termination {
            return Err(ManagedProcessError::InvalidSpec {
                field: "execution_transport",
                message: format!(
                    "{} must support managed processes and process-tree termination",
                    self.transport.name()
                ),
            });
        }
        Ok(ManagedProcessSupervisor {
            inner: Arc::new(SupervisorInner {
                entries: StdMutex::new(HashMap::new()),
                max_processes: self.max_processes,
                max_log_lines: self.max_log_lines,
                max_log_line_chars: self.max_log_line_chars,
                event_capacity: self.event_capacity,
                restart_delay: self.restart_delay,
                transport: self.transport,
            }),
        })
    }
}

impl Default for ManagedProcessSupervisorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Owns background commands and services independently from agent turns.
#[derive(Clone)]
pub struct ManagedProcessSupervisor {
    inner: Arc<SupervisorInner>,
}

impl ManagedProcessSupervisor {
    /// Create a supervisor with default bounds.
    ///
    /// # Panics
    ///
    /// Panics only if the crate's built-in positive limits become invalid.
    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("default managed-process limits are valid")
    }

    /// Create a configurable supervisor builder.
    pub fn builder() -> ManagedProcessSupervisorBuilder {
        ManagedProcessSupervisorBuilder::new()
    }

    /// Start a background command or service and return once its first spawn succeeds.
    pub async fn start(
        &self,
        spec: ManagedProcessSpec,
    ) -> ManagedProcessResult<ManagedProcessHandle> {
        validate_spec(&spec)?;
        self.inner
            .transport
            .validate_working_directory(&spec.working_directory)
            .await
            .map_err(|source| ManagedProcessError::Transport {
                name: spec.name.clone(),
                source,
            })?;
        let id = ManagedProcessId(format!("process-{}-{}", now_ms(), next_id()));
        let created_at_ms = now_ms();
        let (events, receiver) = broadcast::channel(self.inner.event_capacity);
        let entry = Arc::new(ProcessEntry {
            id: id.clone(),
            spec,
            created_at_ms,
            state: RwLock::new(ProcessState {
                status: ManagedProcessStatus::Queued,
                detail: "Queued for start".to_string(),
                pid: None,
                transport_handle: None,
                restart_count: 0,
                updated_at_ms: created_at_ms,
            }),
            logs: Mutex::new(VecDeque::new()),
            next_log_sequence: std::sync::atomic::AtomicU64::new(1),
            events,
            control: StdMutex::new(None),
        });
        {
            let mut entries = self.entries();
            if entries.len() >= self.inner.max_processes {
                return Err(ManagedProcessError::LimitReached {
                    limit: self.inner.max_processes,
                });
            }
            entries.insert(id.clone(), entry.clone());
        }
        if let Err(error) = self.start_entry(entry.clone()).await {
            self.entries().remove(&id);
            return Err(error);
        }
        Ok(ManagedProcessHandle {
            id,
            supervisor: self.clone(),
            events: ManagedProcessEventStream { receiver },
        })
    }

    /// Return all retained process snapshots.
    pub async fn list(&self) -> Vec<ManagedProcessSnapshot> {
        let entries = self.entries().values().cloned().collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(snapshot(&entry).await);
        }
        snapshots.sort_by_key(|value| value.created_at_ms);
        snapshots
    }

    /// Return the latest snapshot for one process.
    pub async fn snapshot(
        &self,
        id: &ManagedProcessId,
    ) -> ManagedProcessResult<ManagedProcessSnapshot> {
        let entry = self.entry(id)?;
        Ok(snapshot(&entry).await)
    }

    /// Subscribe to future events for an existing process.
    ///
    /// Read [`Self::snapshot`] and [`Self::logs`] first when reconnecting; the
    /// live stream is bounded and intentionally does not replay old events.
    pub fn subscribe(
        &self,
        id: &ManagedProcessId,
    ) -> ManagedProcessResult<ManagedProcessEventStream> {
        Ok(ManagedProcessEventStream {
            receiver: self.entry(id)?.events.subscribe(),
        })
    }

    /// Return the retained bounded log tail.
    pub async fn logs(
        &self,
        id: &ManagedProcessId,
    ) -> ManagedProcessResult<Vec<ManagedProcessLogLine>> {
        Ok(self.entry(id)?.logs.lock().await.iter().cloned().collect())
    }

    /// Stop a running process and its descendants.
    pub async fn stop(
        &self,
        id: &ManagedProcessId,
    ) -> ManagedProcessResult<ManagedProcessSnapshot> {
        let entry = self.entry(id)?;
        let control = entry
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(control) = control {
            let (done_tx, done_rx) = oneshot::channel();
            control
                .send(ProcessControl::Stop(Some(done_tx)))
                .await
                .map_err(|_| ManagedProcessError::SupervisorClosed { id: id.clone() })?;
            tokio::time::timeout(START_TIMEOUT, done_rx)
                .await
                .map_err(|_| ManagedProcessError::Timeout {
                    id: id.clone(),
                    operation: "stop",
                })?
                .map_err(|_| ManagedProcessError::SupervisorClosed { id: id.clone() })?;
        }
        Ok(snapshot(&entry).await)
    }

    /// Stop and start an existing process specification.
    pub async fn restart(
        &self,
        id: &ManagedProcessId,
    ) -> ManagedProcessResult<ManagedProcessSnapshot> {
        let entry = self.entry(id)?;
        let _ = self.stop(id).await?;
        {
            let mut state = entry.state.write().await;
            state.restart_count = state.restart_count.saturating_add(1);
        }
        self.start_entry(entry.clone()).await?;
        Ok(snapshot(&entry).await)
    }

    /// Stop a process and remove its retained record.
    pub async fn delete(&self, id: &ManagedProcessId) -> ManagedProcessResult<()> {
        let _ = self.stop(id).await?;
        self.entries().remove(id);
        Ok(())
    }

    async fn start_entry(&self, entry: Arc<ProcessEntry>) -> ManagedProcessResult<()> {
        if entry
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Err(ManagedProcessError::InvalidSpec {
                field: "process state",
                message: format!("{} is already running", entry.id),
            });
        }
        let (control_tx, control_rx) = mpsc::channel(1);
        *entry
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(control_tx);
        let (ready_tx, ready_rx) = oneshot::channel();
        let limits = SupervisorLimits {
            max_log_lines: self.inner.max_log_lines,
            max_log_line_chars: self.inner.max_log_line_chars,
            restart_delay: self.inner.restart_delay,
            transport: self.inner.transport.clone(),
        };
        tokio::spawn(supervise(entry.clone(), limits, control_rx, ready_tx));
        match tokio::time::timeout(START_TIMEOUT, ready_rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(ManagedProcessError::SupervisorClosed {
                id: entry.id.clone(),
            }),
            Err(_) => {
                if let Some(control) = entry
                    .control
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    let _ = control.try_send(ProcessControl::Stop(None));
                }
                Err(ManagedProcessError::Timeout {
                    id: entry.id.clone(),
                    operation: "start",
                })
            }
        }
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<ManagedProcessId, Arc<ProcessEntry>>> {
        self.inner
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn entry(&self, id: &ManagedProcessId) -> ManagedProcessResult<Arc<ProcessEntry>> {
        self.entries()
            .get(id)
            .cloned()
            .ok_or_else(|| ManagedProcessError::NotFound { id: id.clone() })
    }
}

impl Default for ManagedProcessSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

/// Owned handle returned when a managed process starts.
pub struct ManagedProcessHandle {
    id: ManagedProcessId,
    supervisor: ManagedProcessSupervisor,
    events: ManagedProcessEventStream,
}

impl ManagedProcessHandle {
    /// Stable process identifier.
    pub fn id(&self) -> &ManagedProcessId {
        &self.id
    }

    /// Wait for the next live process event.
    pub async fn recv(
        &mut self,
    ) -> std::result::Result<ManagedProcessEvent, ManagedProcessStreamError> {
        self.events.recv().await
    }

    /// Return the latest process snapshot.
    pub async fn snapshot(&self) -> ManagedProcessResult<ManagedProcessSnapshot> {
        self.supervisor.snapshot(&self.id).await
    }

    /// Return the retained bounded log tail.
    pub async fn logs(&self) -> ManagedProcessResult<Vec<ManagedProcessLogLine>> {
        self.supervisor.logs(&self.id).await
    }

    /// Stop this process and all descendants.
    pub async fn stop(&self) -> ManagedProcessResult<ManagedProcessSnapshot> {
        self.supervisor.stop(&self.id).await
    }

    /// Restart this process from its original specification.
    pub async fn restart(&self) -> ManagedProcessResult<ManagedProcessSnapshot> {
        self.supervisor.restart(&self.id).await
    }
}

async fn supervise(
    entry: Arc<ProcessEntry>,
    limits: SupervisorLimits,
    mut control: mpsc::Receiver<ProcessControl>,
    ready: oneshot::Sender<ManagedProcessResult<()>>,
) {
    let mut ready = Some(ready);
    loop {
        set_state(
            &entry,
            ManagedProcessStatus::Queued,
            format!("Starting {}", entry.spec.name),
            None,
            None,
        )
        .await;
        let command = command_spec(&entry.spec);
        let mut process = match limits
            .transport
            .spawn(TransportSpawnRequest {
                command,
                working_directory: entry.spec.working_directory.clone(),
            })
            .await
        {
            Ok(child) => child,
            Err(source) => {
                let error = ManagedProcessError::Spawn {
                    name: entry.spec.name.clone(),
                    source,
                };
                set_state(
                    &entry,
                    ManagedProcessStatus::Failed,
                    error.to_string(),
                    None,
                    None,
                )
                .await;
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error));
                }
                clear_control(&entry);
                return;
            }
        };
        drop(process.take_stdin());
        if let Some(stdout) = process.take_stdout() {
            tokio::spawn(capture_logs(
                entry.clone(),
                stdout,
                "stdout",
                limits.max_log_lines,
                limits.max_log_line_chars,
            ));
        }
        if let Some(stderr) = process.take_stderr() {
            tokio::spawn(capture_logs(
                entry.clone(),
                stderr,
                "stderr",
                limits.max_log_lines,
                limits.max_log_line_chars,
            ));
        }
        let pid = process.pid();
        let transport_handle = process.handle().clone();
        set_state(
            &entry,
            ManagedProcessStatus::Running,
            format!("{} is running", entry.spec.name),
            pid,
            Some(transport_handle),
        )
        .await;
        if let Some(ready) = ready.take() {
            let _ = ready.send(Ok(()));
        }

        let status = tokio::select! {
            control = control.recv() => {
                set_state(
                    &entry,
                    ManagedProcessStatus::Queued,
                    format!("Stopping {}", entry.spec.name),
                    pid,
                    Some(process.handle().clone()),
                ).await;
                let _ = process.terminate().await;
                let _ = process.wait().await;
                set_state(
                    &entry,
                    ManagedProcessStatus::Cancelled,
                    format!("{} was stopped", entry.spec.name),
                    None,
                    None,
                ).await;
                if let Some(ProcessControl::Stop(Some(done))) = control {
                    let _ = done.send(());
                }
                clear_control(&entry);
                return;
            }
            status = process.wait() => status
        };

        let (failed, detail) = match status {
            Ok(status) if status.success => {
                (false, format!("{} exited successfully", entry.spec.name))
            }
            Ok(status) => (
                true,
                format!("{} exited with code {:?}", entry.spec.name, status.code),
            ),
            Err(error) => (
                true,
                format!("Could not wait for {}: {error}", entry.spec.name),
            ),
        };
        let restart = match entry.spec.restart_policy {
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure => failed,
            RestartPolicy::Always => true,
        };
        if !restart {
            set_state(
                &entry,
                if failed {
                    ManagedProcessStatus::Failed
                } else {
                    ManagedProcessStatus::Succeeded
                },
                detail,
                None,
                None,
            )
            .await;
            clear_control(&entry);
            return;
        }
        let attempt = {
            let mut state = entry.state.write().await;
            state.restart_count = state.restart_count.saturating_add(1);
            state.restart_count
        };
        set_state(
            &entry,
            ManagedProcessStatus::Queued,
            format!("{detail}; restarting"),
            None,
            None,
        )
        .await;
        let delay_ms = u64::try_from(limits.restart_delay.as_millis()).unwrap_or(u64::MAX);
        let _ = entry.events.send(ManagedProcessEvent::RestartScheduled {
            process_id: entry.id.clone(),
            attempt,
            delay_ms,
        });
        tokio::select! {
            () = tokio::time::sleep(limits.restart_delay) => {}
            control = control.recv() => {
                set_state(
                    &entry,
                    ManagedProcessStatus::Cancelled,
                    format!("{} was stopped", entry.spec.name),
                    None,
                    None,
                ).await;
                if let Some(ProcessControl::Stop(Some(done))) = control {
                    let _ = done.send(());
                }
                clear_control(&entry);
                return;
            }
        }
    }
}

async fn capture_logs<R>(
    entry: Arc<ProcessEntry>,
    reader: R,
    stream: &'static str,
    max_lines: usize,
    max_chars: usize,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(text)) = lines.next_line().await {
        let line = ManagedProcessLogLine {
            sequence: entry
                .next_log_sequence
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            timestamp_ms: now_ms(),
            stream: stream.to_string(),
            text: bounded_text(&text, max_chars),
        };
        {
            let mut logs = entry.logs.lock().await;
            if logs.len() == max_lines {
                logs.pop_front();
            }
            logs.push_back(line.clone());
        }
        let _ = entry.events.send(ManagedProcessEvent::Log {
            process_id: entry.id.clone(),
            line,
        });
    }
}

async fn set_state(
    entry: &ProcessEntry,
    status: ManagedProcessStatus,
    detail: String,
    pid: Option<u32>,
    transport_handle: Option<TransportProcessHandle>,
) {
    {
        let mut state = entry.state.write().await;
        state.status = status;
        state.detail = detail;
        state.pid = pid;
        if transport_handle.is_some() {
            state.transport_handle = transport_handle;
        }
        state.updated_at_ms = now_ms();
    }
    let _ = entry.events.send(ManagedProcessEvent::StatusChanged {
        snapshot: snapshot(entry).await,
    });
}

async fn snapshot(entry: &ProcessEntry) -> ManagedProcessSnapshot {
    let state = entry.state.read().await;
    ManagedProcessSnapshot {
        id: entry.id.clone(),
        name: entry.spec.name.clone(),
        working_directory: entry.spec.working_directory.clone(),
        restart_policy: entry.spec.restart_policy,
        status: state.status,
        detail: state.detail.clone(),
        pid: state.pid,
        transport_handle: state.transport_handle.clone(),
        restart_count: state.restart_count,
        created_at_ms: entry.created_at_ms,
        updated_at_ms: state.updated_at_ms,
    }
}

fn clear_control(entry: &ProcessEntry) {
    entry
        .control
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
}

fn command_spec(spec: &ManagedProcessSpec) -> CommandSpec {
    let mut command = CommandSpec::new(&spec.program);
    command.args.clone_from(&spec.args);
    command.clear_environment = spec.clear_environment;
    command.environment = spec
        .environment
        .iter()
        .map(|(name, value)| (name.clone(), OsString::from(value.expose())))
        .collect();
    command
}

fn validate_spec(spec: &ManagedProcessSpec) -> ManagedProcessResult<()> {
    let name = spec.name.trim();
    if name.is_empty() || name.chars().count() > 80 || name.chars().any(char::is_control) {
        return Err(ManagedProcessError::InvalidSpec {
            field: "name",
            message: "must contain 1-80 printable characters".to_string(),
        });
    }
    if spec.program.as_os_str().is_empty() {
        return Err(ManagedProcessError::InvalidSpec {
            field: "program",
            message: "must not be empty".to_string(),
        });
    }
    if spec.args.len() > 64 {
        return Err(ManagedProcessError::InvalidSpec {
            field: "args",
            message: "must contain at most 64 arguments".to_string(),
        });
    }
    for name in spec.environment.keys() {
        let name = name.to_string_lossy();
        if name.is_empty() || name.contains(['=', '\0']) {
            return Err(ManagedProcessError::InvalidSpec {
                field: "environment",
                message: "variable names must be non-empty and cannot contain `=` or NUL"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn bounded_text(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        value.to_string()
    } else {
        let mut bounded = value.chars().take(limit).collect::<String>();
        bounded.push_str("… [truncated]");
        bounded
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn next_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn background_command_streams_logs_and_survives_start() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = ManagedProcessSupervisor::new();
        let mut handle = supervisor
            .start(
                ManagedProcessSpec::background("worker", "/bin/sh", directory.path())
                    .args(["-c", "echo ready; sleep 300"]),
            )
            .await
            .unwrap();
        assert_eq!(
            handle.snapshot().await.unwrap().status,
            ManagedProcessStatus::Running
        );

        let log = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let ManagedProcessEvent::Log { line, .. } = handle.recv().await.unwrap() {
                    break line;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(log.text, "ready");
        assert_eq!(handle.logs().await.unwrap(), vec![log]);
        assert_eq!(
            handle.stop().await.unwrap().status,
            ManagedProcessStatus::Cancelled
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn service_restarts_after_failure_and_can_be_stopped_during_delay() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = ManagedProcessSupervisor::builder()
            .restart_delay(Duration::from_secs(30))
            .build()
            .unwrap();
        let mut handle = supervisor
            .start(
                ManagedProcessSpec::service("failing", "/bin/sh", directory.path())
                    .args(["-c", "exit 7"]),
            )
            .await
            .unwrap();
        let restart = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let ManagedProcessEvent::RestartScheduled { attempt, .. } =
                    handle.recv().await.unwrap()
                {
                    break attempt;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(restart, 1);
        assert_eq!(
            handle.stop().await.unwrap().status,
            ManagedProcessStatus::Cancelled
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_the_last_owner_stops_the_process_tree() {
        use nix::errno::Errno;
        use nix::unistd::Pid;

        let directory = tempfile::tempdir().unwrap();
        let supervisor = ManagedProcessSupervisor::new();
        let handle = supervisor
            .start(
                ManagedProcessSpec::background("owned", "/bin/sh", directory.path())
                    .args(["-c", "sleep 300"]),
            )
            .await
            .unwrap();
        let pid = i32::try_from(handle.snapshot().await.unwrap().pid.unwrap()).unwrap();
        assert_eq!(nix::sys::signal::kill(Pid::from_raw(pid), None), Ok(()));

        drop(handle);
        drop(supervisor);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if nix::sys::signal::kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn debug_redacts_environment_values() {
        let spec = ManagedProcessSpec::background("worker", "worker", ".")
            .environment("TOKEN", SecretString::new("very-secret"));
        let debug = format!("{spec:?}");
        assert!(debug.contains("TOKEN"));
        assert!(!debug.contains("very-secret"));
    }
}
