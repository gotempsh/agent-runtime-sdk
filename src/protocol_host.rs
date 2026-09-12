//! Carrier-independent dispatcher for the versioned remote runtime protocol.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, watch, Mutex, OwnedMutexGuard, RwLock};
use tokio::task::JoinHandle;

use crate::journal::{EventJournal, EventJournalError, EventReplayRequest};
use crate::lifecycle::{
    DeliveryState, InvocationId, RetryAdvice, RuntimeFailure, RuntimeFailureKind, RuntimeId,
};
use crate::protocol::{
    encode_client_frame, ClientFrame, ClientRequest, HostFrame, HostResponse, ProtocolCodecError,
    ProtocolRuntimeSpec, ProtocolSandboxSelection, ProtocolTurnInput,
};
use crate::retained::{
    InteractionPolicy, RuntimeClient, RuntimeConfigurationKey, RuntimeHandle, RuntimeSpec,
    TurnInput, TurnInterruptHandle,
};
use crate::{InteractionBroker, InteractionBrokerError, SandboxRequest, SecretString};

const REQUEST_CACHE_CAPACITY: usize = 1_024;
const PENDING_REQUEST_CAPACITY: usize = 256;
const LIVE_SINK_TIMEOUT: Duration = Duration::from_secs(1);
const DIRECT_SINK_TIMEOUT: Duration = Duration::from_secs(5);

/// Backpressured carrier sink used by [`RemoteRuntimeHost`].
#[async_trait]
pub trait HostFrameSink: Send + Sync {
    /// Sends one host frame to an authenticated client session.
    async fn send(&self, frame: HostFrame) -> Result<(), HostFrameSinkError>;
}

/// Carrier-specific failure while sending one host frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("host frame sink failed: {message}")]
pub struct HostFrameSinkError {
    /// Bounded carrier diagnostic without frame contents.
    pub message: String,
}

/// Typed sandbox-selection resolution failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProtocolSandboxResolutionError {
    /// The host has no configured backend with this name.
    #[error("sandbox backend `{backend}` is not configured on this runtime host")]
    BackendNotFound {
        /// Requested backend name.
        backend: String,
    },
    /// The authenticated identity cannot use the selection.
    #[error("sandbox selection is not permitted: {message}")]
    PermissionDenied {
        /// Bounded authorization detail.
        message: String,
    },
    /// The selection is structurally or semantically invalid.
    #[error("sandbox selection is invalid: {message}")]
    InvalidSelection {
        /// Bounded validation detail.
        message: String,
    },
}

/// Application-owned resolver from protocol sandbox names to configured SDK backends.
#[async_trait]
pub trait ProtocolSandboxResolver: Send + Sync {
    /// Resolves an authorized host-local sandbox selection.
    async fn resolve(
        &self,
        selection: ProtocolSandboxSelection,
    ) -> Result<SandboxRequest, ProtocolSandboxResolutionError>;
}

struct RejectSandboxResolver;

#[async_trait]
impl ProtocolSandboxResolver for RejectSandboxResolver {
    async fn resolve(
        &self,
        selection: ProtocolSandboxSelection,
    ) -> Result<SandboxRequest, ProtocolSandboxResolutionError> {
        Err(ProtocolSandboxResolutionError::BackendNotFound {
            backend: selection.backend,
        })
    }
}

/// Failure returned by the protocol dispatcher itself.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RemoteRuntimeHostError {
    /// Client frame validation or bounded encoding failed.
    #[error("invalid client protocol frame: {source}")]
    InvalidFrame {
        /// Protocol codec failure.
        #[source]
        source: ProtocolCodecError,
    },
    /// One request ID was reused for different request bytes.
    #[error("protocol request id `{request_id}` was reused for a different request")]
    RequestIdConflict {
        /// Conflicting request identifier.
        request_id: String,
    },
    /// Too many distinct deduplicated requests are currently executing.
    #[error("remote runtime host has {maximum} pending requests; retry after one completes")]
    RequestCapacityExceeded {
        /// Maximum number of concurrently pending deduplicated requests.
        maximum: usize,
    },
    /// The authenticated client sink rejected a direct response.
    #[error("could not deliver response for request `{request_id}`: {source}")]
    ResponseDelivery {
        /// Client request identifier.
        request_id: String,
        /// Carrier sink failure.
        #[source]
        source: HostFrameSinkError,
    },
}

/// Resource limits enforced by one [`RemoteRuntimeHost`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteRuntimeHostLimits {
    /// Completed idempotent responses retained for replay.
    pub completed_request_capacity: usize,
    /// Distinct idempotent requests allowed to remain pending concurrently.
    pub pending_request_capacity: usize,
}

impl Default for RemoteRuntimeHostLimits {
    fn default() -> Self {
        Self {
            completed_request_capacity: REQUEST_CACHE_CAPACITY,
            pending_request_capacity: PENDING_REQUEST_CAPACITY,
        }
    }
}

/// Invalid resource limits supplied to [`RemoteRuntimeHost::with_limits`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid remote runtime host limits: {message}")]
pub struct RemoteRuntimeHostConfigurationError {
    message: String,
}

/// Remote-host dispatcher independent from its authenticated network carrier.
#[derive(Clone)]
pub struct RemoteRuntimeHost {
    client: Arc<dyn RuntimeClient>,
    journal: Arc<dyn EventJournal>,
    sandbox_resolver: Arc<dyn ProtocolSandboxResolver>,
    runtimes: Arc<RwLock<HashMap<RuntimeId, RuntimeHandle>>>,
    active: Arc<RwLock<HashMap<(RuntimeId, InvocationId), ActiveInvocation>>>,
    lifecycle_gates: Arc<RwLock<HashMap<RuntimeId, Weak<Mutex<()>>>>>,
    live_sinks: Arc<RwLock<HashMap<RuntimeId, Arc<dyn HostFrameSink>>>>,
    delivery_gates: Arc<RwLock<HashMap<RuntimeId, Arc<Mutex<()>>>>>,
    request_cache: Arc<StdMutex<RequestCache>>,
    recovery_gate: Arc<RwLock<()>>,
}

impl RemoteRuntimeHost {
    /// Dispose every retained harness runtime, confirming provider termination.
    /// Managed application processes are not owned by this host.
    pub async fn dispose_all_runtimes(&self) -> Result<(), crate::lifecycle::RuntimeFailure> {
        let _recovery_guard = self.recovery_gate.write().await;
        let ids = self
            .runtimes
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            self.dispose_runtime(&id).await?;
        }
        Ok(())
    }

    async fn dispose_runtime(
        &self,
        runtime_id: &RuntimeId,
    ) -> Result<crate::retained::DisposeOutcome, crate::lifecycle::RuntimeFailure> {
        let gate = self.lifecycle_gate(runtime_id).await;
        let _lifecycle_guard = gate.lock().await;
        let outcome = self.client.dispose(runtime_id).await?;
        let pumps = {
            let mut active = self.active.write().await;
            let keys: Vec<_> = active
                .keys()
                .filter(|(id, _)| id == runtime_id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| active.remove(&key))
                .map(|entry| entry.pump)
                .collect::<Vec<_>>()
        };
        for pump in pumps {
            pump.abort();
            let _ = pump.await;
        }
        self.runtimes.write().await.remove(runtime_id);
        self.live_sinks.write().await.remove(runtime_id);
        self.delivery_gates.write().await.remove(runtime_id);
        self.journal.remove_runtime(runtime_id).await;
        Ok(outcome)
    }
    /// Creates a host that rejects named sandbox selections until a resolver is supplied.
    ///
    /// # Panics
    ///
    /// Panics only if the crate's built-in default limits become internally invalid.
    pub fn new(client: Arc<dyn RuntimeClient>, journal: Arc<dyn EventJournal>) -> Self {
        Self::with_limits(client, journal, RemoteRuntimeHostLimits::default())
            .expect("default remote runtime host limits are valid")
    }

    /// Creates a host with explicit completed-response and pending-request limits.
    pub fn with_limits(
        client: Arc<dyn RuntimeClient>,
        journal: Arc<dyn EventJournal>,
        limits: RemoteRuntimeHostLimits,
    ) -> Result<Self, RemoteRuntimeHostConfigurationError> {
        if limits.completed_request_capacity == 0 || limits.pending_request_capacity == 0 {
            return Err(RemoteRuntimeHostConfigurationError {
                message: "request capacities must be greater than zero".to_owned(),
            });
        }
        Ok(Self {
            client,
            journal,
            sandbox_resolver: Arc::new(RejectSandboxResolver),
            runtimes: Arc::new(RwLock::new(HashMap::new())),
            active: Arc::new(RwLock::new(HashMap::new())),
            lifecycle_gates: Arc::new(RwLock::new(HashMap::new())),
            live_sinks: Arc::new(RwLock::new(HashMap::new())),
            delivery_gates: Arc::new(RwLock::new(HashMap::new())),
            request_cache: Arc::new(StdMutex::new(RequestCache::new(limits))),
            recovery_gate: Arc::new(RwLock::new(())),
        })
    }

    /// Uses an application-owned authorized sandbox resolver.
    pub fn with_sandbox_resolver(mut self, resolver: Arc<dyn ProtocolSandboxResolver>) -> Self {
        self.sandbox_resolver = resolver;
        self
    }

    /// Dispatches one validated frame and streams direct responses to `sink`.
    ///
    /// Runtime failures are sent as [`HostFrame::Failure`] and return `Ok(())`.
    /// Dispatcher/carrier failures are returned to the embedding server.
    pub async fn dispatch(
        &self,
        frame: ClientFrame,
        sink: Arc<dyn HostFrameSink>,
    ) -> Result<(), RemoteRuntimeHostError> {
        let _recovery_guard = self.recovery_gate.read().await;
        let encoded = encode_client_frame(&frame)
            .map_err(|source| RemoteRuntimeHostError::InvalidFrame { source })?;
        let fingerprint: [u8; 32] = Sha256::digest(&encoded).into();
        let mut request_owner = None;
        if Self::request_requires_deduplication(&frame.request) {
            loop {
                match self.claim_request(&frame.request_id, fingerprint)? {
                    RequestClaim::Owner(owner) => {
                        request_owner = Some(owner);
                        break;
                    }
                    RequestClaim::Cached(frames) => {
                        return send_direct_frames(&frame.request_id, &sink, frames).await;
                    }
                    RequestClaim::Wait(mut completed) => {
                        if !*completed.borrow() {
                            let _ = completed.changed().await;
                        }
                    }
                }
            }
        }

        let execution = match self.execute(&frame, Arc::clone(&sink)).await {
            Ok(execution) => execution,
            Err(failure) => HostExecution {
                frames: vec![HostFrame::Failure {
                    request_id: frame.request_id.clone(),
                    failure,
                }],
                delivery_guard: None,
            },
        };
        let HostExecution {
            frames,
            delivery_guard,
        } = execution;
        if let Some(owner) = request_owner {
            owner.complete(frames.clone());
        }
        let delivery = send_direct_frames(&frame.request_id, &sink, frames).await;
        self.release_turn_pump(&frame.request).await;
        drop(delivery_guard);
        delivery
    }

    fn request_requires_deduplication(request: &ClientRequest) -> bool {
        !matches!(
            request,
            ClientRequest::Attach { .. } | ClientRequest::Health { .. }
        )
    }

    fn claim_request(
        &self,
        request_id: &str,
        fingerprint: [u8; 32],
    ) -> Result<RequestClaim, RemoteRuntimeHostError> {
        let mut cache = self
            .request_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match cache.entries.get(request_id) {
            Some(CacheEntry::Pending {
                fingerprint: existing,
                completed,
            }) if existing == &fingerprint => Ok(RequestClaim::Wait(completed.subscribe())),
            Some(CacheEntry::Complete {
                fingerprint: existing,
                frames,
            }) if existing == &fingerprint => Ok(RequestClaim::Cached(frames.clone())),
            Some(_) => Err(RemoteRuntimeHostError::RequestIdConflict {
                request_id: request_id.to_owned(),
            }),
            None => {
                if cache.pending >= cache.pending_maximum {
                    return Err(RemoteRuntimeHostError::RequestCapacityExceeded {
                        maximum: cache.pending_maximum,
                    });
                }
                let (completed, _) = watch::channel(false);
                cache.entries.insert(
                    request_id.to_owned(),
                    CacheEntry::Pending {
                        fingerprint,
                        completed,
                    },
                );
                cache.pending += 1;
                Ok(RequestClaim::Owner(PendingRequestGuard {
                    cache: Arc::clone(&self.request_cache),
                    request_id: request_id.to_owned(),
                    fingerprint,
                    armed: true,
                }))
            }
        }
    }

    async fn execute(
        &self,
        frame: &ClientFrame,
        sink: Arc<dyn HostFrameSink>,
    ) -> Result<HostExecution, RuntimeFailure> {
        let mut delivery_guard = None;
        let response = match &frame.request {
            ClientRequest::Acquire { spec } => {
                let gate = self.lifecycle_gate(&spec.runtime_id).await;
                let _lifecycle_guard = gate.lock().await;
                let spec = self.runtime_spec(spec.clone()).await?;
                let handle = self.client.acquire(spec).await?;
                let response = self.runtime_ready(&handle).await;
                self.runtimes
                    .write()
                    .await
                    .insert(handle.runtime_id().clone(), handle);
                vec![HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response,
                }]
            }
            ClientRequest::Attach {
                runtime_id,
                replay_after,
                replay_limit,
            } => {
                let lifecycle_gate = self.lifecycle_gate(runtime_id).await;
                let _lifecycle_guard = lifecycle_gate.lock().await;
                let handle = self.client.attach(runtime_id).await?;
                let gate = self.delivery_gate(runtime_id).await;
                delivery_guard = Some(gate.lock_owned().await);
                self.live_sinks
                    .write()
                    .await
                    .insert(runtime_id.clone(), Arc::clone(&sink));
                let response = self.runtime_ready(&handle).await;
                self.runtimes
                    .write()
                    .await
                    .insert(runtime_id.clone(), handle);
                let replay = self
                    .journal
                    .replay(EventReplayRequest {
                        runtime_id: runtime_id.clone(),
                        after: replay_after.clone(),
                        only_invocations: Some(replay_after.keys().cloned().collect()),
                        limit: *replay_limit,
                    })
                    .await
                    .map_err(|error| journal_failure(runtime_id.clone(), error))?;
                let mut frames = Vec::with_capacity(replay.events.len() + 2);
                frames.push(HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response,
                });
                frames.extend(
                    replay
                        .events
                        .into_iter()
                        .map(|envelope| HostFrame::Event { envelope }),
                );
                frames.push(HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response: HostResponse::ReplayComplete {
                        runtime_id: runtime_id.clone(),
                        truncated: replay.truncated,
                    },
                });
                frames
            }
            ClientRequest::StartTurn { runtime_id, input } => {
                let gate = self.lifecycle_gate(runtime_id).await;
                let _lifecycle_guard = gate.lock().await;
                let handle = self.runtime_handle(runtime_id).await?;
                let input = protocol_turn_input(input.clone())?;
                let invocation_id = input.invocation_id.clone();
                let broker = (input.interaction_policy == InteractionPolicy::RequireHandler)
                    .then(InteractionBroker::default);
                let turn = match &broker {
                    Some(broker) => {
                        handle
                            .start_turn_with_interactions(input, Arc::new(broker.clone()))
                            .await?
                    }
                    None => handle.start_turn(input).await?,
                };
                let interrupt = turn.interrupt_handle();
                let (pump_start, pump_ready) = oneshot::channel();
                let pump = self.spawn_turn_pump(
                    runtime_id.clone(),
                    invocation_id.clone(),
                    turn,
                    pump_ready,
                );
                self.active.write().await.insert(
                    (runtime_id.clone(), invocation_id.clone()),
                    ActiveInvocation {
                        interrupt,
                        broker,
                        pump_start: Some(pump_start),
                        pump,
                    },
                );
                self.live_sinks
                    .write()
                    .await
                    .insert(runtime_id.clone(), Arc::clone(&sink));
                vec![HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response: HostResponse::TurnAccepted {
                        runtime_id: runtime_id.clone(),
                        invocation_id,
                    },
                }]
            }
            ClientRequest::Interrupt {
                runtime_id,
                invocation_id,
            } => {
                let interrupt = self
                    .active
                    .read()
                    .await
                    .get(&(runtime_id.clone(), invocation_id.clone()))
                    .map(|active| active.interrupt.clone())
                    .ok_or_else(|| active_invocation_failure(runtime_id, invocation_id))?;
                vec![HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response: HostResponse::Interrupted {
                        outcome: interrupt.interrupt().await,
                    },
                }]
            }
            ClientRequest::Dispose { runtime_id } => {
                let outcome = self.dispose_runtime(runtime_id).await?;
                vec![HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response: HostResponse::Disposed { outcome },
                }]
            }
            ClientRequest::Health { runtime_id } => {
                let handle = self.runtime_handle(runtime_id).await?;
                vec![HostFrame::Response {
                    request_id: frame.request_id.clone(),
                    response: HostResponse::Health {
                        health: handle.health().await,
                    },
                }]
            }
            ClientRequest::ResolveApproval {
                runtime_id,
                invocation_id,
                interaction_id,
                decision,
            } => {
                let broker = self.interaction_broker(runtime_id, invocation_id).await?;
                broker
                    .resolve_approval(interaction_id, decision.clone())
                    .map_err(|error| interaction_failure(runtime_id, invocation_id, error))?;
                interaction_response(&frame.request_id, interaction_id)
            }
            ClientRequest::ResolveQuestion {
                runtime_id,
                invocation_id,
                interaction_id,
                answer,
            } => {
                let broker = self.interaction_broker(runtime_id, invocation_id).await?;
                broker
                    .resolve_question(interaction_id, answer.clone())
                    .map_err(|error| interaction_failure(runtime_id, invocation_id, error))?;
                interaction_response(&frame.request_id, interaction_id)
            }
            ClientRequest::DeclineQuestion {
                runtime_id,
                invocation_id,
                interaction_id,
            } => {
                let broker = self.interaction_broker(runtime_id, invocation_id).await?;
                broker
                    .decline_question(interaction_id)
                    .map_err(|error| interaction_failure(runtime_id, invocation_id, error))?;
                interaction_response(&frame.request_id, interaction_id)
            }
        };
        Ok(HostExecution {
            frames: response,
            delivery_guard,
        })
    }

    async fn runtime_ready(&self, runtime: &RuntimeHandle) -> HostResponse {
        let configuration_impacts = [
            RuntimeConfigurationKey::Provider,
            RuntimeConfigurationKey::WorkingDirectory,
            RuntimeConfigurationKey::Model,
            RuntimeConfigurationKey::Reasoning,
            RuntimeConfigurationKey::PermissionMode,
            RuntimeConfigurationKey::HarnessOptions,
            RuntimeConfigurationKey::LaunchContext,
            RuntimeConfigurationKey::AutoCompaction,
            RuntimeConfigurationKey::Environment,
            RuntimeConfigurationKey::Timeouts,
            RuntimeConfigurationKey::Sandbox,
        ]
        .into_iter()
        .map(|key| (key, runtime.configuration_impact(key)))
        .collect();
        HostResponse::RuntimeReady {
            provider: runtime.provider(),
            health: runtime.health().await,
            driver: runtime.driver_capabilities(),
            configuration_impacts,
        }
    }

    async fn runtime_spec(&self, spec: ProtocolRuntimeSpec) -> Result<RuntimeSpec, RuntimeFailure> {
        if spec.turn_timeout_ms == 0 || spec.interaction_timeout_ms == 0 {
            return Err(protocol_failure(
                Some(spec.runtime_id),
                None,
                RuntimeFailureKind::InvalidRequest,
                "runtime timeouts must be greater than zero",
            ));
        }
        let required_sandbox_capabilities = spec
            .sandbox
            .as_ref()
            .map_or(crate::SandboxCapabilities::NONE, |selection| {
                selection.required_capabilities
            });
        let sandbox = match spec.sandbox {
            Some(selection) => Some(self.sandbox_resolver.resolve(selection).await.map_err(
                |error| {
                    protocol_failure(
                        Some(spec.runtime_id.clone()),
                        None,
                        match error {
                            ProtocolSandboxResolutionError::PermissionDenied { .. } => {
                                RuntimeFailureKind::Permission
                            }
                            _ => RuntimeFailureKind::CapabilityUnavailable,
                        },
                        error.to_string(),
                    )
                },
            )?),
            None => None,
        };
        let mut runtime = RuntimeSpec::new(spec.runtime_id, spec.provider, spec.working_directory);
        runtime.model = spec.model;
        runtime.reasoning = spec.reasoning;
        runtime.permission_mode = spec.permission_mode;
        runtime.harness_options = spec.harness_options;
        runtime.launch_context = spec.launch_context;
        runtime.auto_compaction = spec.auto_compaction;
        runtime.provider_session_id = spec.provider_session_id;
        runtime.turn_timeout = Duration::from_millis(spec.turn_timeout_ms);
        runtime.interaction_timeout = Duration::from_millis(spec.interaction_timeout_ms);
        runtime.tool_process_policy = spec.tool_process_policy;
        runtime.environment = protocol_environment(spec.environment);
        runtime.sandbox = sandbox;
        runtime.required_sandbox_capabilities = required_sandbox_capabilities;
        Ok(runtime)
    }

    async fn runtime_handle(
        &self,
        runtime_id: &RuntimeId,
    ) -> Result<RuntimeHandle, RuntimeFailure> {
        if let Some(handle) = self.runtimes.read().await.get(runtime_id).cloned() {
            return Ok(handle);
        }
        let handle = self.client.attach(runtime_id).await?;
        self.runtimes
            .write()
            .await
            .insert(runtime_id.clone(), handle.clone());
        Ok(handle)
    }

    async fn interaction_broker(
        &self,
        runtime_id: &RuntimeId,
        invocation_id: &InvocationId,
    ) -> Result<InteractionBroker, RuntimeFailure> {
        self.active
            .read()
            .await
            .get(&(runtime_id.clone(), invocation_id.clone()))
            .and_then(|active| active.broker.clone())
            .ok_or_else(|| active_invocation_failure(runtime_id, invocation_id))
    }

    fn spawn_turn_pump(
        &self,
        runtime_id: RuntimeId,
        invocation_id: InvocationId,
        turn: crate::retained::TurnHandle,
        pump_ready: oneshot::Receiver<()>,
    ) -> JoinHandle<()> {
        let host = self.clone();
        tokio::spawn(async move {
            let _ = pump_ready.await;
            let interrupt = turn.interrupt_handle();
            let (mut events, completion) = turn.into_parts();
            while let Some(envelope) = events.next().await {
                if let Err(error) = host.journal.append(envelope.clone()).await {
                    let failure = journal_failure(runtime_id.clone(), error);
                    host.send_live(
                        &runtime_id,
                        HostFrame::Failure {
                            request_id: format!("invocation:{invocation_id}"),
                            failure,
                        },
                    )
                    .await;
                    let _ = interrupt.interrupt().await;
                    break;
                }
                host.send_live(&runtime_id, HostFrame::Event { envelope })
                    .await;
            }
            let _ = completion.wait().await;
            host.active
                .write()
                .await
                .remove(&(runtime_id, invocation_id));
        })
    }

    async fn release_turn_pump(&self, request: &ClientRequest) {
        let ClientRequest::StartTurn { runtime_id, input } = request else {
            return;
        };
        let pump_start = self
            .active
            .write()
            .await
            .get_mut(&(runtime_id.clone(), input.invocation_id.clone()))
            .and_then(|active| active.pump_start.take());
        if let Some(pump_start) = pump_start {
            let _ = pump_start.send(());
        }
    }

    async fn send_live(&self, runtime_id: &RuntimeId, frame: HostFrame) {
        let gate = self.delivery_gate(runtime_id).await;
        let _delivery_guard = gate.lock().await;
        let sink = self.live_sinks.read().await.get(runtime_id).cloned();
        let Some(sink) = sink else {
            return;
        };
        let delivered = matches!(
            tokio::time::timeout(LIVE_SINK_TIMEOUT, sink.send(frame)).await,
            Ok(Ok(()))
        );
        if delivered {
            return;
        }
        let mut sinks = self.live_sinks.write().await;
        if sinks
            .get(runtime_id)
            .is_some_and(|current| Arc::ptr_eq(current, &sink))
        {
            sinks.remove(runtime_id);
        }
    }

    async fn delivery_gate(&self, runtime_id: &RuntimeId) -> Arc<Mutex<()>> {
        if let Some(gate) = self.delivery_gates.read().await.get(runtime_id).cloned() {
            return gate;
        }
        let mut gates = self.delivery_gates.write().await;
        Arc::clone(
            gates
                .entry(runtime_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    async fn lifecycle_gate(&self, runtime_id: &RuntimeId) -> Arc<Mutex<()>> {
        // Lookup and insertion share one write lock: two concurrent callers must
        // never create different gates for the same runtime identifier.
        let mut gates = self.lifecycle_gates.write().await;
        if let Some(gate) = gates.get(runtime_id).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(Mutex::new(()));
        gates.insert(runtime_id.clone(), Arc::downgrade(&gate));
        gate
    }
}

struct HostExecution {
    frames: Vec<HostFrame>,
    delivery_guard: Option<OwnedMutexGuard<()>>,
}

struct ActiveInvocation {
    interrupt: TurnInterruptHandle,
    broker: Option<InteractionBroker>,
    pump_start: Option<oneshot::Sender<()>>,
    pump: JoinHandle<()>,
}

enum RequestClaim {
    Owner(PendingRequestGuard),
    Cached(Vec<HostFrame>),
    Wait(watch::Receiver<bool>),
}

struct RequestCache {
    entries: HashMap<String, CacheEntry>,
    order: VecDeque<String>,
    maximum: usize,
    pending: usize,
    pending_maximum: usize,
}

impl RequestCache {
    fn new(limits: RemoteRuntimeHostLimits) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            maximum: limits.completed_request_capacity,
            pending: 0,
            pending_maximum: limits.pending_request_capacity,
        }
    }

    fn evict_completed(&mut self) {
        while self.order.len() > self.maximum {
            if let Some(expired) = self.order.pop_front() {
                if matches!(
                    self.entries.get(&expired),
                    Some(CacheEntry::Complete { .. })
                ) {
                    self.entries.remove(&expired);
                }
            }
        }
    }
}

struct PendingRequestGuard {
    cache: Arc<StdMutex<RequestCache>>,
    request_id: String,
    fingerprint: [u8; 32],
    armed: bool,
}

impl PendingRequestGuard {
    fn complete(mut self, frames: Vec<HostFrame>) {
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let completed = match cache.entries.remove(&self.request_id) {
            Some(CacheEntry::Pending {
                fingerprint,
                completed,
            }) if fingerprint == self.fingerprint => Some(completed),
            entry => {
                if let Some(entry) = entry {
                    cache.entries.insert(self.request_id.clone(), entry);
                }
                None
            }
        };
        if let Some(completed) = completed {
            cache.pending = cache.pending.saturating_sub(1);
            cache.entries.insert(
                self.request_id.clone(),
                CacheEntry::Complete {
                    fingerprint: self.fingerprint,
                    frames,
                },
            );
            cache.order.push_back(self.request_id.clone());
            cache.evict_completed();
            let _ = completed.send(true);
        }
        self.armed = false;
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let completed = match cache.entries.get(&self.request_id) {
            Some(CacheEntry::Pending { fingerprint, .. }) if fingerprint == &self.fingerprint => {
                match cache.entries.remove(&self.request_id) {
                    Some(CacheEntry::Pending { completed, .. }) => Some(completed),
                    _ => None,
                }
            }
            _ => None,
        };
        if let Some(completed) = completed {
            cache.pending = cache.pending.saturating_sub(1);
            let _ = completed.send(true);
        }
    }
}

enum CacheEntry {
    Pending {
        fingerprint: [u8; 32],
        completed: watch::Sender<bool>,
    },
    Complete {
        fingerprint: [u8; 32],
        frames: Vec<HostFrame>,
    },
}

async fn send_direct_frames(
    request_id: &str,
    sink: &Arc<dyn HostFrameSink>,
    frames: Vec<HostFrame>,
) -> Result<(), RemoteRuntimeHostError> {
    for frame in frames {
        let delivery = tokio::time::timeout(DIRECT_SINK_TIMEOUT, sink.send(frame))
            .await
            .map_err(|_| RemoteRuntimeHostError::ResponseDelivery {
                request_id: request_id.to_owned(),
                source: HostFrameSinkError {
                    message: "direct response delivery exceeded 5 seconds".to_owned(),
                },
            })?;
        delivery.map_err(|source| RemoteRuntimeHostError::ResponseDelivery {
            request_id: request_id.to_owned(),
            source,
        })?;
    }
    Ok(())
}

fn protocol_turn_input(input: ProtocolTurnInput) -> Result<TurnInput, RuntimeFailure> {
    if input.timeout_ms == Some(0) || input.interaction_timeout_ms == Some(0) {
        return Err(protocol_failure(
            None,
            Some(input.invocation_id),
            RuntimeFailureKind::InvalidRequest,
            "turn timeout overrides must be greater than zero",
        ));
    }
    let timeout = input.timeout();
    let interaction_timeout = input.interaction_timeout();
    let mut turn = TurnInput::new(input.invocation_id, input.prompt);
    turn.invocation_kind = input.invocation_kind;
    turn.provenance = input.provenance;
    turn.model = input.model;
    turn.reasoning = input.reasoning;
    turn.permission_mode = input.permission_mode;
    turn.harness_options = input.harness_options;
    turn.launch_context = input.launch_context;
    turn.auto_compaction = input.auto_compaction;
    turn.max_turns = input.max_turns;
    turn.timeout = timeout;
    turn.interaction_timeout = interaction_timeout;
    turn.tool_process_policy = input.tool_process_policy;
    turn.environment = protocol_environment(input.environment);
    turn.attachments = input.attachments;
    turn.interaction_policy = input.interaction_policy;
    Ok(turn)
}

fn protocol_environment(
    variables: Vec<crate::protocol::ProtocolEnvironmentVariable>,
) -> BTreeMap<String, SecretString> {
    variables
        .into_iter()
        .map(|variable| {
            (
                variable.name,
                SecretString::new(variable.value.expose().to_owned()),
            )
        })
        .collect()
}

fn interaction_response(request_id: &str, interaction_id: &str) -> Vec<HostFrame> {
    vec![HostFrame::Response {
        request_id: request_id.to_owned(),
        response: HostResponse::InteractionResolved {
            interaction_id: interaction_id.to_owned(),
        },
    }]
}

fn protocol_failure(
    runtime_id: Option<RuntimeId>,
    invocation_id: Option<InvocationId>,
    kind: RuntimeFailureKind,
    message: impl Into<String>,
) -> RuntimeFailure {
    RuntimeFailure {
        runtime_id,
        invocation_id,
        kind,
        retry: RetryAdvice::RequiresUserAction,
        delivery: DeliveryState::NotSent,
        message: message.into(),
        provider_code: None,
    }
}

fn active_invocation_failure(
    runtime_id: &RuntimeId,
    invocation_id: &InvocationId,
) -> RuntimeFailure {
    protocol_failure(
        Some(runtime_id.clone()),
        Some(invocation_id.clone()),
        RuntimeFailureKind::RuntimeNotFound,
        "the active invocation is not attached to this runtime host",
    )
}

fn interaction_failure(
    runtime_id: &RuntimeId,
    invocation_id: &InvocationId,
    error: InteractionBrokerError,
) -> RuntimeFailure {
    protocol_failure(
        Some(runtime_id.clone()),
        Some(invocation_id.clone()),
        RuntimeFailureKind::InvalidRequest,
        error.to_string(),
    )
}

fn journal_failure(runtime_id: RuntimeId, error: EventJournalError) -> RuntimeFailure {
    RuntimeFailure {
        runtime_id: Some(runtime_id),
        invocation_id: None,
        kind: RuntimeFailureKind::Indeterminate,
        retry: RetryAdvice::RequiresUserAction,
        delivery: DeliveryState::Accepted,
        message: error.to_string(),
        provider_code: Some("event_journal".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::journal::{EventJournalLimits, InMemoryEventJournal};
    use crate::lifecycle::ConfigurationImpact;
    use crate::protocol::{ClientRequest, ProtocolEnvironmentVariable, PROTOCOL_VERSION_V1};
    use crate::retained::{
        InProcessRuntimeClient, RuntimeConfigurationKey, RuntimeDriverCapabilities,
        RuntimeTurnExecutor,
    };
    use crate::{
        EventSink, InteractionHandler, PermissionMode, Provider, RunStatus, ToolProcessPolicy,
        TurnEvent, TurnRequest, TurnResult, Usage,
    };

    struct TestExecutor {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RuntimeTurnExecutor for TestExecutor {
        fn capabilities(&self, _provider: Provider) -> RuntimeDriverCapabilities {
            RuntimeDriverCapabilities {
                retained_process: false,
                session_resume: true,
                live_interactions: true,
                ..RuntimeDriverCapabilities::default()
            }
        }

        fn configuration_impact(&self, _key: RuntimeConfigurationKey) -> ConfigurationImpact {
            ConfigurationImpact::Live
        }

        async fn execute(
            &self,
            request: TurnRequest,
            events: &dyn EventSink,
            _interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            events
                .emit(TurnEvent::TextDelta {
                    text: "done".to_owned(),
                })
                .await?;
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: "done".to_owned(),
                reasoning: None,
                session_id: Some(request.session_id.unwrap_or_else(|| "session-1".to_owned())),
                session_title: Some("Test session".to_owned()),
                model: request.model,
                usage: Usage::default(),
            })
        }
    }

    #[derive(Default)]
    struct CollectSink {
        frames: Mutex<Vec<HostFrame>>,
    }

    #[async_trait]
    impl HostFrameSink for CollectSink {
        async fn send(&self, frame: HostFrame) -> Result<(), HostFrameSinkError> {
            self.frames.lock().await.push(frame);
            Ok(())
        }
    }

    struct RejectSink;

    #[async_trait]
    impl HostFrameSink for RejectSink {
        async fn send(&self, _frame: HostFrame) -> Result<(), HostFrameSinkError> {
            Err(HostFrameSinkError {
                message: "client disconnected".to_owned(),
            })
        }
    }

    fn runtime_id(value: &str) -> RuntimeId {
        RuntimeId::new(value).expect("valid runtime identifier")
    }

    fn invocation_id(value: &str) -> InvocationId {
        InvocationId::new(value).expect("valid invocation identifier")
    }

    fn acquire_frame(request_id: &str, runtime: &str) -> ClientFrame {
        ClientFrame {
            version: PROTOCOL_VERSION_V1,
            request_id: request_id.to_owned(),
            request: ClientRequest::Acquire {
                spec: ProtocolRuntimeSpec {
                    runtime_id: runtime_id(runtime),
                    provider: Provider::Claude,
                    working_directory: ".".into(),
                    model: Some("opus".to_owned()),
                    reasoning: Some("high".to_owned()),
                    permission_mode: PermissionMode::Default,
                    harness_options: BTreeMap::new(),
                    launch_context: crate::LaunchContext::default(),
                    auto_compaction: crate::AutoCompactionPolicy::default(),
                    provider_session_id: None,
                    turn_timeout_ms: 30_000,
                    interaction_timeout_ms: 30_000,
                    tool_process_policy: ToolProcessPolicy::default(),
                    environment: Vec::<ProtocolEnvironmentVariable>::new(),
                    sandbox: None,
                },
            },
        }
    }

    fn start_frame(request_id: &str, runtime: &str, invocation: &str) -> ClientFrame {
        ClientFrame {
            version: PROTOCOL_VERSION_V1,
            request_id: request_id.to_owned(),
            request: ClientRequest::StartTurn {
                runtime_id: runtime_id(runtime),
                input: ProtocolTurnInput {
                    invocation_id: invocation_id(invocation),
                    invocation_kind: crate::retained::RuntimeInvocationKind::Turn,
                    prompt: "run once".to_owned(),
                    provenance: crate::TurnProvenance::default(),
                    model: None,
                    reasoning: None,
                    permission_mode: None,
                    harness_options: None,
                    launch_context: None,
                    auto_compaction: None,
                    max_turns: None,
                    timeout_ms: None,
                    interaction_timeout_ms: None,
                    tool_process_policy: None,
                    environment: Vec::new(),
                    attachments: Vec::new(),
                    interaction_policy: InteractionPolicy::Deny,
                },
            },
        }
    }

    fn test_host(executor: Arc<TestExecutor>) -> (RemoteRuntimeHost, Arc<InMemoryEventJournal>) {
        let client: Arc<dyn RuntimeClient> =
            Arc::new(InProcessRuntimeClient::from_executor(executor));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits {
                max_replay_events: 32,
                ..EventJournalLimits::default()
            })
            .expect("valid journal limits"),
        );
        let host = RemoteRuntimeHost::new(client, journal.clone());
        (host, journal)
    }

    async fn wait_for_terminal_replay(
        journal: &InMemoryEventJournal,
        runtime: &str,
        invocation: &str,
    ) -> crate::journal::EventReplayBatch {
        for _ in 0..100 {
            let replay = journal
                .replay(EventReplayRequest {
                    runtime_id: runtime_id(runtime),
                    after: BTreeMap::new(),
                    only_invocations: None,
                    limit: 32,
                })
                .await
                .expect("replay events");
            if replay.events.iter().any(|event| {
                event.invocation_id == invocation_id(invocation)
                    && matches!(
                        event.event,
                        crate::retained::RuntimeEvent::InvocationCompleted { .. }
                            | crate::retained::RuntimeEvent::InvocationFailed { .. }
                    )
            }) {
                return replay;
            }
            tokio::task::yield_now().await;
        }
        panic!("terminal replay event was not journaled");
    }

    #[tokio::test]
    async fn abandoned_request_claim_releases_pending_capacity() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let client = Arc::new(InProcessRuntimeClient::from_executor(executor));
        let journal = Arc::new(InMemoryEventJournal::default());
        let host = RemoteRuntimeHost::with_limits(
            client,
            journal,
            RemoteRuntimeHostLimits {
                completed_request_capacity: 2,
                pending_request_capacity: 1,
            },
        )
        .expect("valid limits");

        let RequestClaim::Owner(abandoned) = host.claim_request("request-one", [1; 32]).unwrap()
        else {
            panic!("expected owner claim");
        };
        assert!(matches!(
            host.claim_request("request-two", [2; 32]),
            Err(RemoteRuntimeHostError::RequestCapacityExceeded { maximum: 1 })
        ));

        drop(abandoned);

        assert!(matches!(
            host.claim_request("request-two", [2; 32]),
            Ok(RequestClaim::Owner(_))
        ));
    }

    #[test]
    fn remote_host_rejects_zero_request_capacity() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let client = Arc::new(InProcessRuntimeClient::from_executor(executor));
        let journal = Arc::new(InMemoryEventJournal::default());

        let result = RemoteRuntimeHost::with_limits(
            client,
            journal,
            RemoteRuntimeHostLimits {
                completed_request_capacity: 1,
                pending_request_capacity: 0,
            },
        );
        let Err(error) = result else {
            panic!("zero pending capacity must fail");
        };

        assert!(error.to_string().contains("greater than zero"));
    }

    #[tokio::test]
    async fn dispatches_deduplicated_turns_and_replays_the_complete_lifecycle() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (host, journal) = test_host(executor.clone());
        let sink = Arc::new(CollectSink::default());
        host.dispatch(
            acquire_frame("request-acquire", "runtime-host"),
            sink.clone(),
        )
        .await
        .expect("acquire through host");
        let start = start_frame("request-start", "runtime-host", "invocation-host");
        host.dispatch(start.clone(), sink.clone())
            .await
            .expect("start through host");
        host.dispatch(start, sink.clone())
            .await
            .expect("deduplicate start");

        let replay = wait_for_terminal_replay(&journal, "runtime-host", "invocation-host").await;
        assert_eq!(executor.calls.load(Ordering::Acquire), 1);
        assert_eq!(replay.events.len(), 3);
        assert_eq!(
            replay
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let replay_sink = Arc::new(CollectSink::default());
        host.dispatch(
            ClientFrame {
                version: PROTOCOL_VERSION_V1,
                request_id: "request-attach".to_owned(),
                request: ClientRequest::Attach {
                    runtime_id: runtime_id("runtime-host"),
                    replay_after: BTreeMap::from([(invocation_id("invocation-host"), 0)]),
                    replay_limit: 32,
                },
            },
            replay_sink.clone(),
        )
        .await
        .expect("attach and replay");
        let frames = replay_sink.frames.lock().await;
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame, HostFrame::Event { .. }))
                .count(),
            3
        );
        assert!(frames.iter().any(|frame| matches!(
            frame,
            HostFrame::Response {
                response: HostResponse::ReplayComplete { .. },
                ..
            }
        )));
    }

    #[tokio::test]
    async fn disconnected_live_sink_does_not_cancel_journaling() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (host, journal) = test_host(executor.clone());
        let collect = Arc::new(CollectSink::default());
        host.dispatch(
            acquire_frame("request-acquire", "runtime-disconnect"),
            collect,
        )
        .await
        .expect("acquire through host");
        let result = host
            .dispatch(
                start_frame(
                    "request-disconnected",
                    "runtime-disconnect",
                    "invocation-disconnect",
                ),
                Arc::new(RejectSink),
            )
            .await;
        assert!(matches!(
            result,
            Err(RemoteRuntimeHostError::ResponseDelivery { .. })
        ));

        let replay =
            wait_for_terminal_replay(&journal, "runtime-disconnect", "invocation-disconnect").await;
        assert_eq!(replay.events.len(), 3);
        assert_eq!(executor.calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn dispose_stops_old_pump_before_reusing_runtime_id() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (host, journal) = test_host(executor);
        let sink = Arc::new(CollectSink::default());
        host.dispatch(acquire_frame("acquire-old", "runtime-reused"), sink.clone())
            .await
            .expect("acquire original runtime");

        // Keep the pump in live delivery after its first append while disposal runs.
        let delivery_gate = host.delivery_gate(&runtime_id("runtime-reused")).await;
        let delivery_guard = delivery_gate.lock().await;
        host.dispatch(
            start_frame("start-old", "runtime-reused", "invocation-old"),
            sink.clone(),
        )
        .await
        .expect("start original turn");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let replay = journal
                    .replay(EventReplayRequest {
                        runtime_id: runtime_id("runtime-reused"),
                        after: BTreeMap::new(),
                        only_invocations: None,
                        limit: 32,
                    })
                    .await
                    .expect("replay original events");
                if !replay.events.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pump appended before disposal");

        let dispose = ClientFrame {
            version: PROTOCOL_VERSION_V1,
            request_id: "dispose-old".to_owned(),
            request: ClientRequest::Dispose {
                runtime_id: runtime_id("runtime-reused"),
            },
        };
        tokio::time::timeout(Duration::from_secs(2), host.dispatch(dispose, sink.clone()))
            .await
            .expect("disposal must not wait for blocked live delivery")
            .expect("dispose original runtime");
        drop(delivery_guard);

        host.dispatch(acquire_frame("acquire-new", "runtime-reused"), sink.clone())
            .await
            .expect("reacquire same runtime identifier");
        host.dispatch(
            start_frame("start-new", "runtime-reused", "invocation-new"),
            sink,
        )
        .await
        .expect("start new turn");
        let replay = wait_for_terminal_replay(&journal, "runtime-reused", "invocation-new").await;
        assert_eq!(replay.events.len(), 3);
        assert!(replay
            .events
            .iter()
            .all(|event| event.invocation_id == invocation_id("invocation-new")));
    }

    #[tokio::test]
    async fn missing_runtime_requests_do_not_retain_lifecycle_gates() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (host, _) = test_host(executor);
        let sink = Arc::new(CollectSink::default());
        for index in 0..100 {
            let runtime_name = format!("missing-runtime-{index}");
            host.dispatch(
                ClientFrame {
                    version: PROTOCOL_VERSION_V1,
                    request_id: format!("dispose-missing-{index}"),
                    request: ClientRequest::Dispose {
                        runtime_id: runtime_id(&runtime_name),
                    },
                },
                sink.clone(),
            )
            .await
            .expect("disposing a missing runtime returns a response");
        }
        assert!(host.lifecycle_gates.read().await.len() <= 1);
    }

    #[tokio::test]
    async fn request_ids_cannot_be_reused_for_different_mutations() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (host, _) = test_host(executor);
        let sink = Arc::new(CollectSink::default());
        host.dispatch(acquire_frame("same-request", "runtime-one"), sink.clone())
            .await
            .expect("first request");
        let conflict = host
            .dispatch(acquire_frame("same-request", "runtime-two"), sink)
            .await;
        assert!(matches!(
            conflict,
            Err(RemoteRuntimeHostError::RequestIdConflict { .. })
        ));
    }
}
