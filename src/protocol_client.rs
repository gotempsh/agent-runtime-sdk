//! Carrier-independent remote client for the retained runtime protocol.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use crate::lifecycle::{
    ConfigurationImpact, DeliveryState, InterruptOutcome, InvocationId, RetryAdvice,
    RuntimeFailure, RuntimeFailureKind, RuntimeHealth, RuntimeId,
};
use crate::protocol::{
    ClientFrame, ClientRequest, HostFrame, HostResponse, ProtocolEnvironmentVariable,
    ProtocolRuntimeSpec, ProtocolSecret, ProtocolTurnInput, DEFAULT_PROTOCOL_REPLAY_EVENTS,
    PROTOCOL_VERSION_V2,
};
use crate::retained::{
    DisposeOutcome, RetainedRuntimeResult, RuntimeClient, RuntimeConfigurationKey,
    RuntimeDriverCapabilities, RuntimeHandle, RuntimeHandleBackend, RuntimeSpec, TurnHandle,
    TurnInput, TurnInterruptBackend, TurnInterruptHandle,
};
use crate::{InteractionHandler, Provider, SecretString, TurnEvent, TurnResult};

const REQUEST_CHANNEL_CAPACITY: usize = 64;
const EVENT_CHANNEL_CAPACITY: usize = 128;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REPLAY_PAGES: usize = 1_024;
static CLIENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Bounded automatic recovery behavior after an established carrier disconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteRecoveryPolicy {
    /// Maximum reconnect-and-attach rounds before active invocations fail visibly.
    pub max_attempts: u8,
    /// Delay before the first reconnect round.
    pub initial_backoff: Duration,
    /// Maximum delay between reconnect rounds.
    pub max_backoff: Duration,
}

impl Default for RemoteRecoveryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 12,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Invalid bounded recovery configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid remote recovery policy: {message}")]
pub struct RemoteRecoveryPolicyError {
    /// Actionable validation detail.
    pub message: &'static str,
}

impl RemoteRecoveryPolicy {
    fn validate(self) -> Result<Self, RemoteRecoveryPolicyError> {
        if self.max_attempts == 0 {
            return Err(RemoteRecoveryPolicyError {
                message: "max_attempts must be greater than zero",
            });
        }
        if self.initial_backoff.is_zero() {
            return Err(RemoteRecoveryPolicyError {
                message: "initial_backoff must be greater than zero",
            });
        }
        if self.max_backoff < self.initial_backoff {
            return Err(RemoteRecoveryPolicyError {
                message: "max_backoff must be at least initial_backoff",
            });
        }
        Ok(self)
    }
}

/// Failure produced by an application-owned authenticated carrier.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RemoteTransportError {
    /// The carrier is disconnected and may be reconnected.
    #[error("remote runtime carrier disconnected: {message}")]
    Disconnected {
        /// Bounded diagnostic without credentials or frame contents.
        message: String,
    },
    /// Authentication or authorization rejected the connection.
    #[error("remote runtime carrier is not authorized: {message}")]
    Unauthorized {
        /// Bounded authorization diagnostic.
        message: String,
    },
    /// The carrier rejected a frame before delivery.
    #[error("remote runtime carrier rejected a frame: {message}")]
    Rejected {
        /// Bounded carrier diagnostic.
        message: String,
    },
}

/// One authenticated, ordered, bidirectional carrier connection.
#[async_trait]
pub trait RemoteRuntimeConnection: Send + Sync {
    /// Sends one complete protocol frame.
    async fn send(&self, frame: ClientFrame) -> Result<(), RemoteTransportError>;

    /// Receives the next complete host frame.
    async fn receive(&self) -> Result<HostFrame, RemoteTransportError>;
}

/// Application-owned connector for HTTP streams, WebSockets, SSH tunnels, or sockets.
#[async_trait]
pub trait RemoteRuntimeConnector: Send + Sync {
    /// Establishes an authenticated carrier connection.
    async fn connect(&self) -> Result<Arc<dyn RemoteRuntimeConnection>, RemoteTransportError>;
}

/// Retained runtime client backed by the versioned remote protocol.
#[derive(Clone)]
pub struct RemoteRuntimeClient {
    inner: Arc<RemoteClientInner>,
}

impl RemoteRuntimeClient {
    /// Creates a remote client. No connection is opened until the first operation.
    pub fn new(connector: Arc<dyn RemoteRuntimeConnector>) -> Self {
        Self::with_recovery_policy_unchecked(connector, RemoteRecoveryPolicy::default())
    }

    /// Creates a remote client with explicit bounded reconnect behavior.
    pub fn with_recovery_policy(
        connector: Arc<dyn RemoteRuntimeConnector>,
        policy: RemoteRecoveryPolicy,
    ) -> Result<Self, RemoteRecoveryPolicyError> {
        Ok(Self::with_recovery_policy_unchecked(
            connector,
            policy.validate()?,
        ))
    }

    fn with_recovery_policy_unchecked(
        connector: Arc<dyn RemoteRuntimeConnector>,
        recovery_policy: RemoteRecoveryPolicy,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteClientInner {
                connector,
                recovery_policy,
                connection: RwLock::new(None),
                connect_lock: Mutex::new(()),
                pending: Mutex::new(HashMap::new()),
                runtimes: RwLock::new(HashMap::new()),
                invocations: Mutex::new(HashMap::new()),
                request_prefix: client_request_prefix(),
                request_sequence: AtomicU64::new(1),
                connection_generation: AtomicU64::new(1),
                recovery_running: AtomicBool::new(false),
            }),
        }
    }

    /// Acquires a runtime with an already serializable host-side specification.
    pub async fn acquire_protocol(
        &self,
        spec: ProtocolRuntimeSpec,
    ) -> RetainedRuntimeResult<RuntimeHandle> {
        let runtime_id = spec.runtime_id.clone();
        let page = self
            .inner
            .request_runtime(ClientRequest::Acquire { spec }, false)
            .await?;
        self.inner.runtime_handle(runtime_id, page.descriptor).await
    }

    /// Reattaches and replays missed events for active invocations known to this client.
    pub async fn recover(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<RuntimeHandle> {
        let descriptor = self.inner.attach_runtime(runtime_id).await?;
        self.inner
            .runtime_handle(runtime_id.clone(), descriptor)
            .await
    }
}

#[async_trait]
impl RuntimeClient for RemoteRuntimeClient {
    async fn acquire(&self, spec: RuntimeSpec) -> RetainedRuntimeResult<RuntimeHandle> {
        if spec.sandbox.is_some() {
            return Err(remote_failure(
                Some(spec.runtime_id),
                None,
                RuntimeFailureKind::InvalidRequest,
                RetryAdvice::RequiresUserAction,
                DeliveryState::NotSent,
                "remote sandbox acquisition requires acquire_protocol with a host-resolved sandbox selection",
            ));
        }
        self.acquire_protocol(protocol_runtime_spec(spec)).await
    }

    async fn attach(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<RuntimeHandle> {
        self.recover(runtime_id).await
    }

    async fn dispose(&self, runtime_id: &RuntimeId) -> RetainedRuntimeResult<DisposeOutcome> {
        let response = self
            .inner
            .request(ClientRequest::Dispose {
                runtime_id: runtime_id.clone(),
            })
            .await?;
        let HostResponse::Disposed { outcome } = response else {
            return Err(unexpected_response(Some(runtime_id.clone()), None));
        };
        self.inner.runtimes.write().await.remove(runtime_id);
        Ok(outcome)
    }
}

struct RemoteClientInner {
    connector: Arc<dyn RemoteRuntimeConnector>,
    recovery_policy: RemoteRecoveryPolicy,
    connection: RwLock<Option<Arc<dyn RemoteRuntimeConnection>>>,
    connect_lock: Mutex<()>,
    pending: Mutex<HashMap<String, mpsc::Sender<PendingMessage>>>,
    runtimes: RwLock<HashMap<RuntimeId, RuntimeDescriptor>>,
    invocations: Mutex<HashMap<(RuntimeId, InvocationId), Arc<RemoteInvocation>>>,
    request_prefix: String,
    request_sequence: AtomicU64,
    connection_generation: AtomicU64,
    recovery_running: AtomicBool,
}

#[derive(Clone)]
struct RuntimeDescriptor {
    provider: Provider,
    driver: RuntimeDriverCapabilities,
    configuration_impacts: BTreeMap<RuntimeConfigurationKey, ConfigurationImpact>,
}

struct RuntimeReplayPage {
    descriptor: RuntimeDescriptor,
    truncated: bool,
}

enum PendingMessage {
    Response(HostResponse),
    Failure(RuntimeFailure),
    Disconnected(RemoteTransportError),
}

impl RemoteClientInner {
    async fn runtime_handle(
        self: &Arc<Self>,
        runtime_id: RuntimeId,
        descriptor: RuntimeDescriptor,
    ) -> RetainedRuntimeResult<RuntimeHandle> {
        self.runtimes
            .write()
            .await
            .insert(runtime_id.clone(), descriptor.clone());
        let backend: Arc<dyn RuntimeHandleBackend> = Arc::new(RemoteRuntimeBackend {
            client: Arc::clone(self),
            runtime_id: runtime_id.clone(),
            configuration_impacts: descriptor.configuration_impacts,
        });
        Ok(RuntimeHandle::from_backend(
            runtime_id,
            descriptor.provider,
            descriptor.driver,
            backend,
        ))
    }

    async fn request_runtime(
        self: &Arc<Self>,
        request: ClientRequest,
        wait_for_replay: bool,
    ) -> RetainedRuntimeResult<RuntimeReplayPage> {
        let request_id = self.next_request_id();
        for attempt in 0..2 {
            let mut receiver = match self
                .begin_request(request_id.clone(), request.clone())
                .await
            {
                Ok(receiver) => receiver,
                Err(failure) if attempt == 0 && retryable_transport_failure(&failure) => {
                    self.invalidate_connection().await;
                    continue;
                }
                Err(failure) => return Err(failure),
            };
            let mut descriptor = None;
            loop {
                let message = match tokio::time::timeout(REQUEST_TIMEOUT, receiver.recv()).await {
                    Ok(Some(message)) => message,
                    Ok(None) | Err(_) if attempt == 0 => {
                        self.pending.lock().await.remove(&request_id);
                        self.invalidate_connection().await;
                        break;
                    }
                    Ok(None) => return Err(request_disconnected()),
                    Err(_) => return Err(request_timeout()),
                };
                match message {
                    PendingMessage::Response(HostResponse::RuntimeReady {
                        provider,
                        driver,
                        configuration_impacts,
                        ..
                    }) => {
                        descriptor = Some(RuntimeDescriptor {
                            provider,
                            driver,
                            configuration_impacts,
                        });
                        if !wait_for_replay {
                            self.pending.lock().await.remove(&request_id);
                            return descriptor
                                .map(|descriptor| RuntimeReplayPage {
                                    descriptor,
                                    truncated: false,
                                })
                                .ok_or_else(|| unexpected_response(None, None));
                        }
                    }
                    PendingMessage::Response(HostResponse::ReplayComplete {
                        truncated, ..
                    }) => {
                        self.pending.lock().await.remove(&request_id);
                        return descriptor
                            .map(|descriptor| RuntimeReplayPage {
                                descriptor,
                                truncated,
                            })
                            .ok_or_else(|| unexpected_response(None, None));
                    }
                    PendingMessage::Response(_) => {
                        self.pending.lock().await.remove(&request_id);
                        return Err(unexpected_response(None, None));
                    }
                    PendingMessage::Failure(failure) => {
                        self.pending.lock().await.remove(&request_id);
                        return Err(failure);
                    }
                    PendingMessage::Disconnected(_) if attempt == 0 => {
                        self.pending.lock().await.remove(&request_id);
                        self.invalidate_connection().await;
                        break;
                    }
                    PendingMessage::Disconnected(error) => {
                        self.pending.lock().await.remove(&request_id);
                        return Err(transport_failure(error, DeliveryState::PossiblySent));
                    }
                }
            }
        }
        Err(request_disconnected())
    }

    async fn request(
        self: &Arc<Self>,
        request: ClientRequest,
    ) -> RetainedRuntimeResult<HostResponse> {
        let request_id = self.next_request_id();
        self.request_with_id(request_id, request).await
    }

    async fn request_with_id(
        self: &Arc<Self>,
        request_id: String,
        request: ClientRequest,
    ) -> RetainedRuntimeResult<HostResponse> {
        for attempt in 0..2 {
            let mut receiver = match self
                .begin_request(request_id.clone(), request.clone())
                .await
            {
                Ok(receiver) => receiver,
                Err(failure) if attempt == 0 && retryable_transport_failure(&failure) => {
                    self.invalidate_connection().await;
                    continue;
                }
                Err(failure) => return Err(failure),
            };
            let response_wait = tokio::time::timeout(REQUEST_TIMEOUT, receiver.recv()).await;
            self.pending.lock().await.remove(&request_id);
            match response_wait {
                Ok(Some(PendingMessage::Response(response))) => return Ok(response),
                Ok(Some(PendingMessage::Failure(failure))) => return Err(failure),
                Ok(Some(PendingMessage::Disconnected(_)) | None) | Err(_) if attempt == 0 => {
                    self.invalidate_connection().await;
                }
                Ok(Some(PendingMessage::Disconnected(error))) => {
                    return Err(transport_failure(error, DeliveryState::PossiblySent));
                }
                Ok(None) => return Err(request_disconnected()),
                Err(_) => return Err(request_timeout()),
            }
        }
        Err(request_disconnected())
    }

    async fn begin_request(
        self: &Arc<Self>,
        request_id: String,
        request: ClientRequest,
    ) -> RetainedRuntimeResult<mpsc::Receiver<PendingMessage>> {
        let (sender, receiver) = mpsc::channel(REQUEST_CHANNEL_CAPACITY);
        self.pending.lock().await.insert(request_id.clone(), sender);
        let connection = match self.connection().await {
            Ok(connection) => connection,
            Err(error) => {
                self.pending.lock().await.remove(&request_id);
                return Err(transport_failure(error, DeliveryState::NotSent));
            }
        };
        let frame = ClientFrame {
            version: PROTOCOL_VERSION_V2,
            request_id: request_id.clone(),
            request,
        };
        if let Err(error) = connection.send(frame).await {
            self.pending.lock().await.remove(&request_id);
            self.invalidate_connection().await;
            return Err(transport_failure(error, DeliveryState::PossiblySent));
        }
        Ok(receiver)
    }

    async fn connection(
        self: &Arc<Self>,
    ) -> Result<Arc<dyn RemoteRuntimeConnection>, RemoteTransportError> {
        if let Some(connection) = self.connection.read().await.clone() {
            return Ok(connection);
        }
        let _guard = self.connect_lock.lock().await;
        if let Some(connection) = self.connection.read().await.clone() {
            return Ok(connection);
        }
        let connection = self.connector.connect().await?;
        let generation = self.connection_generation.fetch_add(1, Ordering::AcqRel);
        *self.connection.write().await = Some(Arc::clone(&connection));
        let client = Arc::clone(self);
        let receive_connection = Arc::clone(&connection);
        tokio::spawn(async move {
            client.receive_loop(receive_connection, generation).await;
        });
        Ok(connection)
    }

    async fn receive_loop(
        self: Arc<Self>,
        connection: Arc<dyn RemoteRuntimeConnection>,
        generation: u64,
    ) {
        loop {
            match connection.receive().await {
                Ok(frame) => self.route_frame(frame).await,
                Err(error) => {
                    self.disconnect_generation(generation, error).await;
                    break;
                }
            }
        }
    }

    async fn route_frame(self: &Arc<Self>, frame: HostFrame) {
        match frame {
            HostFrame::Response {
                request_id,
                response,
            } => {
                let sender = { self.pending.lock().await.get(&request_id).cloned() };
                if let Some(sender) = sender {
                    let _ = sender.send(PendingMessage::Response(response)).await;
                }
            }
            HostFrame::Failure {
                request_id,
                failure,
            } => {
                let sender = { self.pending.lock().await.get(&request_id).cloned() };
                if let Some(sender) = sender {
                    let _ = sender.send(PendingMessage::Failure(failure)).await;
                }
            }
            HostFrame::Event { envelope } => self.route_event(envelope).await,
        }
    }

    async fn route_event(self: &Arc<Self>, envelope: crate::retained::EventEnvelope) {
        let key = (envelope.runtime_id.clone(), envelope.invocation_id.clone());
        let invocation = self.invocations.lock().await.get(&key).cloned();
        let Some(invocation) = invocation else {
            return;
        };
        let previous = invocation.last_sequence.load(Ordering::Acquire);
        if envelope.sequence <= previous {
            return;
        }
        if envelope.sequence != previous.saturating_add(1) {
            self.schedule_active_recovery();
            return;
        }
        match invocation.events.try_send(envelope.clone()) {
            Ok(()) => invocation
                .last_sequence
                .store(envelope.sequence, Ordering::Release),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.invocations.lock().await.remove(&key);
                self.interrupt_failed_invocation(
                    envelope.runtime_id.clone(),
                    envelope.invocation_id.clone(),
                );
                invocation.complete(Err(remote_failure(
                    Some(envelope.runtime_id),
                    Some(envelope.invocation_id),
                    RuntimeFailureKind::Transport,
                    RetryAdvice::RequiresUserAction,
                    DeliveryState::Accepted,
                    "remote invocation event channel is full; the consumer did not drain events fast enough; remote interrupt requested, but its outcome is unconfirmed",
                ))).await;
                return;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.invocations.lock().await.remove(&key);
                self.interrupt_failed_invocation(
                    envelope.runtime_id.clone(),
                    envelope.invocation_id.clone(),
                );
                invocation
                    .complete(Err(remote_failure(
                        Some(envelope.runtime_id),
                        Some(envelope.invocation_id),
                        RuntimeFailureKind::Transport,
                        RetryAdvice::RequiresUserAction,
                        DeliveryState::Accepted,
                    "remote invocation event channel was closed by its consumer; remote interrupt requested, but its outcome is unconfirmed",
                    )))
                    .await;
                return;
            }
        }
        if let crate::retained::RuntimeEvent::ProviderEvent { event } = &envelope.event {
            self.route_interaction(&invocation, event.clone());
        }
        let terminal = match envelope.event {
            crate::retained::RuntimeEvent::InvocationCompleted { result } => Some(Ok(result)),
            crate::retained::RuntimeEvent::InvocationFailed { failure } => Some(Err(failure)),
            _ => None,
        };
        if let Some(result) = terminal {
            invocation.complete(result).await;
            self.invocations.lock().await.remove(&key);
        }
    }

    fn interrupt_failed_invocation(
        self: &Arc<Self>,
        runtime_id: RuntimeId,
        invocation_id: InvocationId,
    ) {
        // One task per removed invocation, so a stalled consumer cannot block the sole
        // receiver while the host is asked to stop producing for that turn.
        let client = Arc::clone(self);
        tokio::spawn(async move {
            let _ = client
                .request(ClientRequest::Interrupt {
                    runtime_id,
                    invocation_id,
                })
                .await;
        });
    }

    fn route_interaction(self: &Arc<Self>, invocation: &Arc<RemoteInvocation>, event: TurnEvent) {
        let Some(handler) = invocation.interactions.clone() else {
            return;
        };
        let client = Arc::clone(self);
        let runtime_id = invocation.runtime_id.clone();
        let invocation_id = invocation.invocation_id.clone();
        tokio::spawn(async move {
            let request = match event {
                TurnEvent::ApprovalRequested(request)
                | TurnEvent::PlanApprovalRequested(request) => ClientRequest::ResolveApproval {
                    runtime_id: runtime_id.clone(),
                    invocation_id: invocation_id.clone(),
                    interaction_id: request.id.clone(),
                    decision: handler.approve(request).await,
                },
                TurnEvent::QuestionRequested(request) => {
                    match handler.answer(request.clone()).await {
                        Some(answer) => ClientRequest::ResolveQuestion {
                            runtime_id: runtime_id.clone(),
                            invocation_id: invocation_id.clone(),
                            interaction_id: request.id,
                            answer,
                        },
                        None => ClientRequest::DeclineQuestion {
                            runtime_id: runtime_id.clone(),
                            invocation_id: invocation_id.clone(),
                            interaction_id: request.id,
                        },
                    }
                }
                _ => return,
            };
            client
                .deliver_interaction(runtime_id, invocation_id, request)
                .await;
        });
    }

    async fn deliver_interaction(
        self: &Arc<Self>,
        runtime_id: RuntimeId,
        invocation_id: InvocationId,
        request: ClientRequest,
    ) {
        let interaction_id = match &request {
            ClientRequest::ResolveApproval { interaction_id, .. }
            | ClientRequest::ResolveQuestion { interaction_id, .. }
            | ClientRequest::DeclineQuestion { interaction_id, .. } => interaction_id.clone(),
            _ => return,
        };
        let request_id = self.next_request_id();
        let mut backoff = self.recovery_policy.initial_backoff;
        let mut last_failure = None;
        for _attempt in 0..self.recovery_policy.max_attempts {
            if !self
                .invocations
                .lock()
                .await
                .contains_key(&(runtime_id.clone(), invocation_id.clone()))
            {
                return;
            }
            match self
                .request_with_id(request_id.clone(), request.clone())
                .await
            {
                Ok(HostResponse::InteractionResolved {
                    interaction_id: resolved,
                }) if resolved == interaction_id => return,
                Ok(_) => {
                    last_failure = Some(remote_failure(
                        Some(runtime_id.clone()),
                        Some(invocation_id.clone()),
                        RuntimeFailureKind::ProviderProtocol,
                        RetryAdvice::RequiresUserAction,
                        DeliveryState::Accepted,
                        format!(
                            "remote host returned an unexpected response while resolving interaction `{interaction_id}`"
                        ),
                    ));
                    break;
                }
                Err(failure) if retryable_transport_failure(&failure) => {
                    last_failure = Some(failure);
                    tokio::time::sleep(backoff).await;
                    backoff = backoff
                        .saturating_mul(2)
                        .min(self.recovery_policy.max_backoff);
                }
                Err(failure) => {
                    last_failure = Some(failure);
                    break;
                }
            }
        }
        let source = last_failure.unwrap_or_else(request_disconnected);
        self.fail_interaction_delivery(&runtime_id, &invocation_id, &interaction_id, source)
            .await;
    }

    async fn fail_interaction_delivery(
        &self,
        runtime_id: &RuntimeId,
        invocation_id: &InvocationId,
        interaction_id: &str,
        source: RuntimeFailure,
    ) {
        let invocation = self
            .invocations
            .lock()
            .await
            .remove(&(runtime_id.clone(), invocation_id.clone()));
        let Some(invocation) = invocation else {
            return;
        };
        invocation
            .complete(Err(remote_failure(
                Some(runtime_id.clone()),
                Some(invocation_id.clone()),
                source.kind,
                RetryAdvice::RequiresUserAction,
                DeliveryState::Accepted,
                format!(
                    "could not deliver response for provider interaction `{interaction_id}` after bounded recovery: {}",
                    source.message
                ),
            )))
            .await;
    }

    async fn disconnect_generation(self: &Arc<Self>, generation: u64, error: RemoteTransportError) {
        if self.connection_generation.load(Ordering::Acquire) != generation + 1 {
            return;
        }
        *self.connection.write().await = None;
        let pending = self
            .pending
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for sender in pending {
            let _ = sender
                .send(PendingMessage::Disconnected(error.clone()))
                .await;
        }
        if matches!(error, RemoteTransportError::Unauthorized { .. }) {
            self.fail_active_recovery(error.to_string()).await;
        } else {
            self.schedule_active_recovery();
        }
    }

    fn schedule_active_recovery(self: &Arc<Self>) {
        if self
            .recovery_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let client = Arc::clone(self);
        tokio::spawn(async move {
            client.recover_active_invocations().await;
            client.recovery_running.store(false, Ordering::Release);
        });
    }

    async fn recover_active_invocations(self: &Arc<Self>) {
        if self.invocations.lock().await.is_empty() {
            return;
        }
        let mut backoff = self.recovery_policy.initial_backoff;
        let mut last_error = "the remote runtime carrier disconnected".to_owned();
        for _attempt in 0..self.recovery_policy.max_attempts {
            tokio::time::sleep(backoff).await;
            let runtime_ids = self.active_runtime_ids().await;
            if runtime_ids.is_empty() {
                return;
            }
            let mut recovered_all = true;
            for runtime_id in runtime_ids {
                match self.attach_runtime(&runtime_id).await {
                    Ok(descriptor) => {
                        self.runtimes.write().await.insert(runtime_id, descriptor);
                    }
                    Err(failure) => {
                        recovered_all = false;
                        last_error = failure.message;
                    }
                }
            }
            if recovered_all || self.invocations.lock().await.is_empty() {
                return;
            }
            backoff = backoff
                .saturating_mul(2)
                .min(self.recovery_policy.max_backoff);
        }
        self.fail_active_recovery(format!(
            "automatic reconnect and attach was exhausted: {last_error}"
        ))
        .await;
    }

    async fn active_runtime_ids(&self) -> Vec<RuntimeId> {
        let mut runtime_ids = self
            .invocations
            .lock()
            .await
            .keys()
            .map(|(runtime_id, _)| runtime_id.clone())
            .collect::<Vec<_>>();
        runtime_ids.sort();
        runtime_ids.dedup();
        runtime_ids
    }

    async fn fail_active_recovery(&self, message: String) {
        let active = {
            let mut invocations = self.invocations.lock().await;
            invocations
                .drain()
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
        };
        for invocation in active {
            invocation
                .complete(Err(remote_failure(
                    Some(invocation.runtime_id.clone()),
                    Some(invocation.invocation_id.clone()),
                    RuntimeFailureKind::Transport,
                    RetryAdvice::RequiresUserAction,
                    DeliveryState::Accepted,
                    message.clone(),
                )))
                .await;
        }
    }

    async fn invalidate_connection(&self) {
        *self.connection.write().await = None;
    }

    async fn attach_runtime(
        self: &Arc<Self>,
        runtime_id: &RuntimeId,
    ) -> RetainedRuntimeResult<RuntimeDescriptor> {
        for _page in 0..MAX_REPLAY_PAGES {
            let replay_after = self.replay_cursors(runtime_id).await;
            let page = self
                .request_runtime(
                    ClientRequest::Attach {
                        runtime_id: runtime_id.clone(),
                        replay_after: replay_after.clone(),
                        replay_limit: DEFAULT_PROTOCOL_REPLAY_EVENTS,
                    },
                    true,
                )
                .await?;
            if !page.truncated {
                return Ok(page.descriptor);
            }
            if self.replay_cursors(runtime_id).await == replay_after {
                return Err(remote_failure(
                    Some(runtime_id.clone()),
                    None,
                    RuntimeFailureKind::ProviderProtocol,
                    RetryAdvice::RequiresUserAction,
                    DeliveryState::Accepted,
                    "remote replay was truncated without advancing an invocation cursor",
                ));
            }
        }
        Err(remote_failure(
            Some(runtime_id.clone()),
            None,
            RuntimeFailureKind::ProviderProtocol,
            RetryAdvice::RequiresUserAction,
            DeliveryState::Accepted,
            "remote replay exceeded the bounded 1024-page recovery limit",
        ))
    }

    async fn replay_cursors(&self, runtime_id: &RuntimeId) -> BTreeMap<InvocationId, u64> {
        self.invocations
            .lock()
            .await
            .iter()
            .filter(|((candidate, _), _)| candidate == runtime_id)
            .map(|((_, invocation_id), invocation)| {
                (
                    invocation_id.clone(),
                    invocation.last_sequence.load(Ordering::Acquire),
                )
            })
            .collect()
    }

    fn next_request_id(&self) -> String {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}-{sequence}", self.request_prefix)
    }
}

struct RemoteRuntimeBackend {
    client: Arc<RemoteClientInner>,
    runtime_id: RuntimeId,
    configuration_impacts: BTreeMap<RuntimeConfigurationKey, ConfigurationImpact>,
}

#[async_trait]
impl RuntimeHandleBackend for RemoteRuntimeBackend {
    fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact {
        self.configuration_impacts
            .get(&key)
            .copied()
            .unwrap_or(ConfigurationImpact::ReacquireRequired)
    }

    async fn health(&self) -> RuntimeHealth {
        match self
            .client
            .request(ClientRequest::Health {
                runtime_id: self.runtime_id.clone(),
            })
            .await
        {
            Ok(HostResponse::Health { health }) => health,
            Ok(_) => degraded_health(
                &self.runtime_id,
                "remote host returned an unexpected health response",
            ),
            Err(failure) => degraded_health(&self.runtime_id, &failure.message),
        }
    }

    async fn start_turn(
        self: Arc<Self>,
        input: TurnInput,
        interactions: Option<Arc<dyn InteractionHandler>>,
    ) -> RetainedRuntimeResult<TurnHandle> {
        let invocation_id = input.invocation_id.clone();
        let (event_sender, event_receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (completion_sender, completion_receiver) = oneshot::channel();
        let invocation = Arc::new(RemoteInvocation {
            runtime_id: self.runtime_id.clone(),
            invocation_id: invocation_id.clone(),
            events: event_sender,
            completion: Mutex::new(Some(completion_sender)),
            interactions,
            last_sequence: AtomicU64::new(0),
            completed: AtomicBool::new(false),
        });
        let key = (self.runtime_id.clone(), invocation_id.clone());
        self.client
            .invocations
            .lock()
            .await
            .insert(key.clone(), Arc::clone(&invocation));
        let response = self
            .client
            .request(ClientRequest::StartTurn {
                runtime_id: self.runtime_id.clone(),
                input: protocol_turn_input(input),
            })
            .await;
        match response {
            Ok(HostResponse::TurnAccepted { .. }) => {}
            Ok(_) => {
                self.client.invocations.lock().await.remove(&key);
                return Err(unexpected_response(
                    Some(self.runtime_id.clone()),
                    Some(invocation_id),
                ));
            }
            Err(failure) => {
                if failure.delivery == DeliveryState::PossiblySent
                    && retryable_transport_failure(&failure)
                {
                    let recovery = self.client.attach_runtime(&self.runtime_id).await;
                    if let Ok(descriptor) = recovery {
                        self.client
                            .runtimes
                            .write()
                            .await
                            .insert(self.runtime_id.clone(), descriptor);
                        if invocation.last_sequence.load(Ordering::Acquire) > 0 {
                            let interrupt: Arc<dyn TurnInterruptBackend> =
                                Arc::new(RemoteTurnInterrupt {
                                    client: Arc::clone(&self.client),
                                    runtime_id: self.runtime_id.clone(),
                                    invocation_id: invocation_id.clone(),
                                });
                            return Ok(TurnHandle::from_channels(
                                self.runtime_id.clone(),
                                invocation_id,
                                event_receiver,
                                completion_receiver,
                                TurnInterruptHandle::from_backend(interrupt),
                            ));
                        }
                    }
                }
                self.client.invocations.lock().await.remove(&key);
                return Err(failure);
            }
        }
        let interrupt: Arc<dyn TurnInterruptBackend> = Arc::new(RemoteTurnInterrupt {
            client: Arc::clone(&self.client),
            runtime_id: self.runtime_id.clone(),
            invocation_id: invocation_id.clone(),
        });
        Ok(TurnHandle::from_channels(
            self.runtime_id.clone(),
            invocation_id,
            event_receiver,
            completion_receiver,
            TurnInterruptHandle::from_backend(interrupt),
        ))
    }
}

struct RemoteTurnInterrupt {
    client: Arc<RemoteClientInner>,
    runtime_id: RuntimeId,
    invocation_id: InvocationId,
}

#[async_trait]
impl TurnInterruptBackend for RemoteTurnInterrupt {
    async fn interrupt(&self) -> InterruptOutcome {
        match self
            .client
            .request(ClientRequest::Interrupt {
                runtime_id: self.runtime_id.clone(),
                invocation_id: self.invocation_id.clone(),
            })
            .await
        {
            Ok(HostResponse::Interrupted { outcome }) => outcome,
            _ => InterruptOutcome::Unconfirmed,
        }
    }
}

struct RemoteInvocation {
    runtime_id: RuntimeId,
    invocation_id: InvocationId,
    events: mpsc::Sender<crate::retained::EventEnvelope>,
    completion: Mutex<Option<oneshot::Sender<RetainedRuntimeResult<TurnResult>>>>,
    interactions: Option<Arc<dyn InteractionHandler>>,
    last_sequence: AtomicU64,
    completed: AtomicBool,
}

impl RemoteInvocation {
    async fn complete(&self, result: RetainedRuntimeResult<TurnResult>) {
        if self.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(sender) = self.completion.lock().await.take() {
            let _ = sender.send(result);
        }
    }
}

fn protocol_runtime_spec(spec: RuntimeSpec) -> ProtocolRuntimeSpec {
    ProtocolRuntimeSpec {
        runtime_id: spec.runtime_id,
        provider: spec.provider,
        working_directory: spec.working_directory,
        model: spec.model,
        reasoning: spec.reasoning,
        permission_mode: spec.permission_mode,
        harness_options: spec.harness_options,
        launch_context: spec.launch_context,
        auto_compaction: spec.auto_compaction,
        provider_session_id: spec.provider_session_id,
        turn_timeout_ms: duration_millis(spec.turn_timeout),
        interaction_timeout_ms: duration_millis(spec.interaction_timeout),
        tool_process_policy: spec.tool_process_policy,
        environment: protocol_environment(spec.environment),
        sandbox: None,
    }
}

fn protocol_turn_input(input: TurnInput) -> ProtocolTurnInput {
    ProtocolTurnInput {
        invocation_id: input.invocation_id,
        invocation_kind: input.invocation_kind,
        prompt: input.prompt,
        provenance: input.provenance,
        model: input.model,
        reasoning: input.reasoning,
        permission_mode: input.permission_mode,
        harness_options: input.harness_options,
        launch_context: input.launch_context,
        auto_compaction: input.auto_compaction,
        max_turns: input.max_turns,
        timeout_ms: input.timeout.map(duration_millis),
        interaction_timeout_ms: input.interaction_timeout.map(duration_millis),
        tool_process_policy: input.tool_process_policy,
        environment: protocol_environment(input.environment),
        attachments: input.attachments,
        interaction_policy: input.interaction_policy,
    }
}

fn protocol_environment(
    environment: BTreeMap<String, SecretString>,
) -> Vec<ProtocolEnvironmentVariable> {
    environment
        .into_iter()
        .map(|(name, value)| ProtocolEnvironmentVariable {
            name,
            value: ProtocolSecret::new(value.expose().to_owned()),
        })
        .collect()
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn client_request_prefix() -> String {
    let process_sequence = CLIENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("sdk-remote-{timestamp:x}-{process_sequence}")
}

fn degraded_health(runtime_id: &RuntimeId, detail: &str) -> RuntimeHealth {
    RuntimeHealth {
        runtime_id: runtime_id.clone(),
        status: crate::lifecycle::RuntimeStatus::Degraded,
        observed_at_unix_ms: 0,
        detail: Some(detail.to_owned()),
    }
}

fn transport_failure(error: RemoteTransportError, delivery: DeliveryState) -> RuntimeFailure {
    remote_failure(
        None,
        None,
        match error {
            RemoteTransportError::Unauthorized { .. } => RuntimeFailureKind::Permission,
            RemoteTransportError::Disconnected { .. } | RemoteTransportError::Rejected { .. } => {
                RuntimeFailureKind::Transport
            }
        },
        if delivery == DeliveryState::NotSent {
            RetryAdvice::After { milliseconds: 100 }
        } else {
            RetryAdvice::ReconnectAndAttach
        },
        delivery,
        error.to_string(),
    )
}

fn request_timeout() -> RuntimeFailure {
    remote_failure(
        None,
        None,
        RuntimeFailureKind::Timeout,
        RetryAdvice::ReconnectAndAttach,
        DeliveryState::PossiblySent,
        "remote runtime request exceeded 30 seconds",
    )
}

fn request_disconnected() -> RuntimeFailure {
    remote_failure(
        None,
        None,
        RuntimeFailureKind::Transport,
        RetryAdvice::ReconnectAndAttach,
        DeliveryState::PossiblySent,
        "remote runtime response stream ended before a response",
    )
}

fn retryable_transport_failure(failure: &RuntimeFailure) -> bool {
    matches!(
        failure.kind,
        RuntimeFailureKind::Transport | RuntimeFailureKind::Network | RuntimeFailureKind::Timeout
    )
}

fn unexpected_response(
    runtime_id: Option<RuntimeId>,
    invocation_id: Option<InvocationId>,
) -> RuntimeFailure {
    remote_failure(
        runtime_id,
        invocation_id,
        RuntimeFailureKind::Indeterminate,
        RetryAdvice::ReconnectAndAttach,
        DeliveryState::PossiblySent,
        "remote runtime host returned an unexpected protocol response",
    )
}

fn remote_failure(
    runtime_id: Option<RuntimeId>,
    invocation_id: Option<InvocationId>,
    kind: RuntimeFailureKind,
    retry: RetryAdvice,
    delivery: DeliveryState,
    message: impl Into<String>,
) -> RuntimeFailure {
    RuntimeFailure {
        runtime_id,
        invocation_id,
        kind,
        retry,
        delivery,
        provider_code: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::journal::{EventJournalLimits, InMemoryEventJournal};
    use crate::lifecycle::RuntimeStatus;
    use crate::protocol_host::{HostFrameSink, HostFrameSinkError, RemoteRuntimeHost};
    use crate::retained::{InProcessRuntimeClient, RuntimeTurnExecutor};
    use crate::{
        ApprovalDecision, ApprovalRequest, EventSink, QuestionAnswer, QuestionRequest, RunStatus,
        ToolProcessPolicy, TurnRequest, Usage,
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

        fn configuration_impact(&self, key: RuntimeConfigurationKey) -> ConfigurationImpact {
            match key {
                RuntimeConfigurationKey::WorkingDirectory | RuntimeConfigurationKey::Sandbox => {
                    ConfigurationImpact::ReacquireRequired
                }
                _ => ConfigurationImpact::Live,
            }
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
                    text: "remote event".to_owned(),
                })
                .await?;
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: "remote result".to_owned(),
                reasoning: None,
                session_id: Some(
                    request
                        .session_id
                        .unwrap_or_else(|| "remote-session".to_owned()),
                ),
                session_title: Some("Remote session".to_owned()),
                model: request.model,
                usage: Usage::default(),
            })
        }
    }

    struct ChannelSink {
        sender: mpsc::Sender<HostFrame>,
    }

    #[async_trait]
    impl HostFrameSink for ChannelSink {
        async fn send(&self, frame: HostFrame) -> Result<(), HostFrameSinkError> {
            self.sender
                .send(frame)
                .await
                .map_err(|_| HostFrameSinkError {
                    message: "test client disconnected".to_owned(),
                })
        }
    }

    struct InMemoryConnection {
        host: RemoteRuntimeHost,
        sender: mpsc::Sender<HostFrame>,
        receiver: Mutex<mpsc::Receiver<HostFrame>>,
    }

    #[async_trait]
    impl RemoteRuntimeConnection for InMemoryConnection {
        async fn send(&self, frame: ClientFrame) -> Result<(), RemoteTransportError> {
            let host = self.host.clone();
            let sink: Arc<dyn HostFrameSink> = Arc::new(ChannelSink {
                sender: self.sender.clone(),
            });
            tokio::spawn(async move {
                let _ = host.dispatch(frame, sink).await;
            });
            Ok(())
        }

        async fn receive(&self) -> Result<HostFrame, RemoteTransportError> {
            self.receiver.lock().await.recv().await.ok_or_else(|| {
                RemoteTransportError::Disconnected {
                    message: "test connection closed".to_owned(),
                }
            })
        }
    }

    struct InMemoryConnector {
        host: RemoteRuntimeHost,
    }

    struct SwitchableConnection {
        host: RemoteRuntimeHost,
        sender: mpsc::Sender<HostFrame>,
        receiver: Mutex<mpsc::Receiver<HostFrame>>,
        disconnected: AtomicBool,
        disconnect: tokio::sync::Notify,
        attach_requests: Arc<AtomicUsize>,
        attach_completions: Arc<AtomicUsize>,
    }

    struct RecoveryChannelSink {
        sender: mpsc::Sender<HostFrame>,
        attach_completions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HostFrameSink for RecoveryChannelSink {
        async fn send(&self, frame: HostFrame) -> Result<(), HostFrameSinkError> {
            let replay_complete = matches!(
                frame,
                HostFrame::Response {
                    response: HostResponse::ReplayComplete { .. },
                    ..
                }
            );
            self.sender
                .send(frame)
                .await
                .map_err(|_| HostFrameSinkError {
                    message: "test recovery client disconnected".to_owned(),
                })?;
            if replay_complete {
                self.attach_completions.fetch_add(1, Ordering::AcqRel);
            }
            Ok(())
        }
    }

    impl SwitchableConnection {
        fn disconnect(&self) {
            self.disconnected.store(true, Ordering::Release);
            self.disconnect.notify_waiters();
        }
    }

    #[async_trait]
    impl RemoteRuntimeConnection for SwitchableConnection {
        async fn send(&self, frame: ClientFrame) -> Result<(), RemoteTransportError> {
            if self.disconnected.load(Ordering::Acquire) {
                return Err(RemoteTransportError::Disconnected {
                    message: "fault-injected disconnect".to_owned(),
                });
            }
            if matches!(frame.request, ClientRequest::Attach { .. }) {
                self.attach_requests.fetch_add(1, Ordering::AcqRel);
            }
            let host = self.host.clone();
            let sink: Arc<dyn HostFrameSink> = Arc::new(RecoveryChannelSink {
                sender: self.sender.clone(),
                attach_completions: Arc::clone(&self.attach_completions),
            });
            tokio::spawn(async move {
                let _ = host.dispatch(frame, sink).await;
            });
            Ok(())
        }

        async fn receive(&self) -> Result<HostFrame, RemoteTransportError> {
            if self.disconnected.load(Ordering::Acquire) {
                return Err(RemoteTransportError::Disconnected {
                    message: "fault-injected disconnect".to_owned(),
                });
            }
            let mut receiver = self.receiver.lock().await;
            tokio::select! {
                () = self.disconnect.notified() => Err(RemoteTransportError::Disconnected {
                    message: "fault-injected disconnect".to_owned(),
                }),
                frame = receiver.recv() => frame.ok_or_else(|| {
                    RemoteTransportError::Disconnected {
                        message: "test connection closed".to_owned(),
                    }
                }),
            }
        }
    }

    struct SwitchableConnector {
        host: RemoteRuntimeHost,
        current: StdMutex<Option<Arc<SwitchableConnection>>>,
        connections: AtomicUsize,
        attach_requests: Arc<AtomicUsize>,
        attach_completions: Arc<AtomicUsize>,
        reject_connections: AtomicBool,
    }

    impl SwitchableConnector {
        fn disconnect_current(&self) {
            if let Some(connection) = self
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            {
                connection.disconnect();
            }
        }
    }

    #[async_trait]
    impl RemoteRuntimeConnector for SwitchableConnector {
        async fn connect(&self) -> Result<Arc<dyn RemoteRuntimeConnection>, RemoteTransportError> {
            if self.reject_connections.load(Ordering::Acquire) {
                return Err(RemoteTransportError::Disconnected {
                    message: "fault-injected connector outage".to_owned(),
                });
            }
            let (sender, receiver) = mpsc::channel(256);
            let connection = Arc::new(SwitchableConnection {
                host: self.host.clone(),
                sender,
                receiver: Mutex::new(receiver),
                disconnected: AtomicBool::new(false),
                disconnect: tokio::sync::Notify::new(),
                attach_requests: Arc::clone(&self.attach_requests),
                attach_completions: Arc::clone(&self.attach_completions),
            });
            self.connections.fetch_add(1, Ordering::AcqRel);
            *self
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&connection));
            Ok(connection)
        }
    }

    struct GatedExecutor {
        release: tokio::sync::Notify,
    }

    struct BurstExecutor {
        release: tokio::sync::Notify,
        emitted: tokio::sync::Notify,
        events: u64,
    }

    struct QuestionExecutor;

    struct GatedQuestionResponder {
        calls: AtomicUsize,
        requested: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait]
    impl InteractionHandler for GatedQuestionResponder {
        async fn approve(&self, _request: ApprovalRequest) -> ApprovalDecision {
            ApprovalDecision::Deny {
                reason: Some("approval was not expected in this test".to_owned()),
            }
        }

        async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.requested.notify_one();
            self.release.notified().await;
            Some(QuestionAnswer::selected("Fruit", "Banana"))
        }
    }

    #[async_trait]
    impl RuntimeTurnExecutor for QuestionExecutor {
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
            _request: TurnRequest,
            events: &dyn EventSink,
            interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            let question = QuestionRequest::new(
                "question-reconnect",
                serde_json::json!({
                    "questions": [{
                        "header": "Fruit",
                        "question": "Banana or plantain?",
                        "options": [{"label": "Banana", "description": "Choose banana"}],
                        "multiSelect": false
                    }]
                }),
            );
            events
                .emit(TurnEvent::QuestionRequested(question.clone()))
                .await?;
            let answer = interactions
                .expect("question executor requires an interaction handler")
                .answer(question)
                .await;
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: if answer.is_some() {
                    "question answered".to_owned()
                } else {
                    "question declined".to_owned()
                },
                reasoning: None,
                session_id: Some("question-session".to_owned()),
                session_title: Some("Question session".to_owned()),
                model: None,
                usage: Usage::default(),
            })
        }
    }

    #[async_trait]
    impl RuntimeTurnExecutor for BurstExecutor {
        fn capabilities(&self, _provider: Provider) -> RuntimeDriverCapabilities {
            RuntimeDriverCapabilities {
                retained_process: false,
                session_resume: true,
                live_interactions: false,
                ..RuntimeDriverCapabilities::default()
            }
        }

        fn configuration_impact(&self, _key: RuntimeConfigurationKey) -> ConfigurationImpact {
            ConfigurationImpact::Live
        }

        async fn execute(
            &self,
            _request: TurnRequest,
            events: &dyn EventSink,
            _interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            self.release.notified().await;
            for sequence in 1..=self.events {
                events
                    .emit(TurnEvent::TextDelta {
                        text: format!("burst-{sequence}"),
                    })
                    .await?;
            }
            self.emitted.notify_one();
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: "burst complete".to_owned(),
                reasoning: None,
                session_id: Some("burst-session".to_owned()),
                session_title: Some("Burst session".to_owned()),
                model: None,
                usage: Usage::default(),
            })
        }
    }

    #[async_trait]
    impl RuntimeTurnExecutor for GatedExecutor {
        fn capabilities(&self, _provider: Provider) -> RuntimeDriverCapabilities {
            RuntimeDriverCapabilities {
                retained_process: false,
                session_resume: true,
                live_interactions: false,
                ..RuntimeDriverCapabilities::default()
            }
        }

        fn configuration_impact(&self, _key: RuntimeConfigurationKey) -> ConfigurationImpact {
            ConfigurationImpact::Live
        }

        async fn execute(
            &self,
            _request: TurnRequest,
            events: &dyn EventSink,
            _interactions: Option<&dyn InteractionHandler>,
        ) -> crate::Result<TurnResult> {
            self.release.notified().await;
            events
                .emit(TurnEvent::TextDelta {
                    text: "after reconnect".to_owned(),
                })
                .await?;
            Ok(TurnResult {
                status: RunStatus::Succeeded,
                text: "recovered".to_owned(),
                reasoning: None,
                session_id: Some("recovered-session".to_owned()),
                session_title: Some("Recovered session".to_owned()),
                model: None,
                usage: Usage::default(),
            })
        }
    }

    #[async_trait]
    impl RemoteRuntimeConnector for InMemoryConnector {
        async fn connect(&self) -> Result<Arc<dyn RemoteRuntimeConnection>, RemoteTransportError> {
            let (sender, receiver) = mpsc::channel(256);
            Ok(Arc::new(InMemoryConnection {
                host: self.host.clone(),
                sender,
                receiver: Mutex::new(receiver),
            }))
        }
    }

    fn remote_fixture(executor: Arc<TestExecutor>) -> (RemoteRuntimeClient, RemoteRuntimeHost) {
        let hosted: Arc<dyn RuntimeClient> =
            Arc::new(InProcessRuntimeClient::from_executor(executor));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits {
                max_replay_events: DEFAULT_PROTOCOL_REPLAY_EVENTS,
                max_runtimes: 16,
                max_invocations_per_runtime: 32,
                max_events_per_invocation: 256,
            })
            .expect("valid journal limits"),
        );
        let host = RemoteRuntimeHost::new(hosted, journal);
        let client = RemoteRuntimeClient::new(Arc::new(InMemoryConnector { host: host.clone() }));
        (client, host)
    }

    fn runtime_spec(id: &str) -> RuntimeSpec {
        let mut spec = RuntimeSpec::new(
            RuntimeId::new(id).expect("valid runtime identifier"),
            Provider::Claude,
            "/workspace",
        );
        spec.model = Some("test-model".to_owned());
        spec.tool_process_policy = ToolProcessPolicy::TerminateOnCompletion;
        spec
    }

    #[tokio::test]
    async fn saturated_invocation_does_not_block_other_responses() {
        let (client, _) = remote_fixture(Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        }));
        let runtime_id = RuntimeId::new("slow-runtime").expect("valid runtime id");
        let invocation_id = InvocationId::new("slow-turn").expect("valid invocation id");
        let (events, _receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (completion, completed) = oneshot::channel();
        let invocation = Arc::new(RemoteInvocation {
            runtime_id: runtime_id.clone(),
            invocation_id: invocation_id.clone(),
            events,
            completion: Mutex::new(Some(completion)),
            interactions: None,
            last_sequence: AtomicU64::new(0),
            completed: AtomicBool::new(false),
        });
        client
            .inner
            .invocations
            .lock()
            .await
            .insert((runtime_id.clone(), invocation_id.clone()), invocation);
        for sequence in 1..=EVENT_CHANNEL_CAPACITY as u64 {
            client
                .inner
                .route_event(crate::retained::EventEnvelope {
                    schema_version: 1,
                    runtime_id: runtime_id.clone(),
                    invocation_id: invocation_id.clone(),
                    sequence,
                    observed_at_unix_ms: 0,
                    event: crate::retained::RuntimeEvent::ProviderEvent {
                        event: TurnEvent::TextDelta {
                            text: "x".to_owned(),
                        },
                    },
                })
                .await;
        }
        let overflow = crate::retained::EventEnvelope {
            schema_version: 1,
            runtime_id: runtime_id.clone(),
            invocation_id: invocation_id.clone(),
            sequence: EVENT_CHANNEL_CAPACITY as u64 + 1,
            observed_at_unix_ms: 0,
            event: crate::retained::RuntimeEvent::ProviderEvent {
                event: TurnEvent::TextDelta {
                    text: "overflow".to_owned(),
                },
            },
        };
        let client_inner = Arc::clone(&client.inner);
        tokio::time::timeout(Duration::from_millis(250), async move {
            client_inner.route_event(overflow).await;
        })
        .await
        .expect("overflow must not stall the receive loop");
        let (response_sender, mut response_receiver) = mpsc::channel(1);
        client
            .inner
            .pending
            .lock()
            .await
            .insert("other-request".to_owned(), response_sender);
        tokio::time::timeout(
            Duration::from_millis(250),
            client.inner.route_frame(HostFrame::Response {
                request_id: "other-request".to_owned(),
                response: HostResponse::TurnAccepted {
                    runtime_id: RuntimeId::new("other-runtime").expect("valid runtime id"),
                    invocation_id: InvocationId::new("other-turn").expect("valid invocation id"),
                },
            }),
        )
        .await
        .expect("unrelated response must be routed");
        assert!(matches!(
            response_receiver.recv().await,
            Some(PendingMessage::Response(HostResponse::TurnAccepted { .. }))
        ));
        let failure = completed
            .await
            .expect("turn must complete with backpressure failure")
            .expect_err("turn must not silently lose an event");
        assert_eq!(failure.runtime_id, Some(runtime_id));
        assert_eq!(failure.invocation_id, Some(invocation_id));
        assert!(failure.message.contains("event channel"));
    }

    #[tokio::test]
    async fn remote_client_preserves_the_retained_runtime_contract() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (client, _) = remote_fixture(executor.clone());
        let handle = client
            .acquire(runtime_spec("remote-runtime"))
            .await
            .expect("acquire remote runtime");
        assert_eq!(handle.provider(), Provider::Claude);
        assert!(handle.driver_capabilities().session_resume);
        assert_eq!(
            handle.configuration_impact(RuntimeConfigurationKey::WorkingDirectory),
            ConfigurationImpact::ReacquireRequired
        );
        assert_eq!(handle.health().await.status, RuntimeStatus::Ready);

        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("remote-turn").expect("valid invocation identifier"),
                "run remotely",
            ))
            .await
            .expect("start remote turn");
        let result = turn.wait().await.expect("remote turn result");
        assert_eq!(result.text, "remote result");
        assert_eq!(executor.calls.load(Ordering::Acquire), 1);
        assert_eq!(
            client
                .dispose(handle.runtime_id())
                .await
                .expect("dispose remote runtime"),
            DisposeOutcome::Disposed
        );
    }

    #[tokio::test]
    async fn a_fresh_client_can_attach_without_cached_runtime_metadata() {
        let executor = Arc::new(TestExecutor {
            calls: AtomicUsize::new(0),
        });
        let (first, host) = remote_fixture(executor);
        let acquired = first
            .acquire(runtime_spec("remote-attach"))
            .await
            .expect("acquire remote runtime");
        assert_eq!(acquired.provider(), Provider::Claude);

        let fresh = RemoteRuntimeClient::new(Arc::new(InMemoryConnector { host }));
        let attached = fresh
            .attach(acquired.runtime_id())
            .await
            .expect("attach from a fresh client");
        assert_eq!(attached.provider(), Provider::Claude);
        assert!(attached.driver_capabilities().live_interactions);
        assert_eq!(
            attached.configuration_impact(RuntimeConfigurationKey::Model),
            ConfigurationImpact::Live
        );
    }

    #[tokio::test]
    async fn active_turn_rebinds_to_a_new_connection_without_user_action() {
        let executor = Arc::new(GatedExecutor {
            release: tokio::sync::Notify::new(),
        });
        let hosted: Arc<dyn RuntimeClient> =
            Arc::new(InProcessRuntimeClient::from_executor(executor.clone()));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits::default()).expect("valid journal limits"),
        );
        let host = RemoteRuntimeHost::new(hosted, journal);
        let connector = Arc::new(SwitchableConnector {
            host,
            current: StdMutex::new(None),
            connections: AtomicUsize::new(0),
            attach_requests: Arc::new(AtomicUsize::new(0)),
            attach_completions: Arc::new(AtomicUsize::new(0)),
            reject_connections: AtomicBool::new(false),
        });
        let client = RemoteRuntimeClient::with_recovery_policy(
            connector.clone(),
            RemoteRecoveryPolicy {
                max_attempts: 5,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            },
        )
        .expect("valid recovery policy");
        let handle = client
            .acquire(runtime_spec("runtime-auto-recover"))
            .await
            .expect("acquire runtime");
        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("turn-auto-recover").expect("valid invocation identifier"),
                "wait for reconnect",
            ))
            .await
            .expect("start remote turn");

        connector.disconnect_current();
        tokio::time::timeout(Duration::from_secs(2), async {
            while connector.attach_completions.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("automatic attach should occur");
        executor.release.notify_one();

        let result = tokio::time::timeout(Duration::from_secs(2), turn.wait())
            .await
            .expect("recovered turn should finish")
            .expect("recovered turn result");
        assert_eq!(result.text, "recovered");
        assert!(connector.connections.load(Ordering::Acquire) >= 2);
    }

    #[tokio::test]
    async fn exhausted_recovery_finishes_the_turn_with_an_actionable_failure() {
        let executor = Arc::new(GatedExecutor {
            release: tokio::sync::Notify::new(),
        });
        let hosted: Arc<dyn RuntimeClient> =
            Arc::new(InProcessRuntimeClient::from_executor(executor.clone()));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits::default()).expect("valid journal limits"),
        );
        let connector = Arc::new(SwitchableConnector {
            host: RemoteRuntimeHost::new(hosted, journal),
            current: StdMutex::new(None),
            connections: AtomicUsize::new(0),
            attach_requests: Arc::new(AtomicUsize::new(0)),
            attach_completions: Arc::new(AtomicUsize::new(0)),
            reject_connections: AtomicBool::new(false),
        });
        let client = RemoteRuntimeClient::with_recovery_policy(
            connector.clone(),
            RemoteRecoveryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(2),
            },
        )
        .expect("valid recovery policy");
        let handle = client
            .acquire(runtime_spec("runtime-recovery-exhausted"))
            .await
            .expect("acquire runtime");
        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("turn-recovery-exhausted").expect("valid invocation identifier"),
                "remain active during outage",
            ))
            .await
            .expect("start remote turn");

        connector.reject_connections.store(true, Ordering::Release);
        connector.disconnect_current();
        let failure = tokio::time::timeout(Duration::from_secs(2), turn.wait())
            .await
            .expect("recovery exhaustion must not leave a hanging turn")
            .expect_err("outage must surface as a failure");
        assert_eq!(failure.kind, RuntimeFailureKind::Transport);
        assert_eq!(failure.delivery, DeliveryState::Accepted);
        assert_eq!(failure.retry, RetryAdvice::RequiresUserAction);
        assert!(failure.message.contains("automatic reconnect"));
        executor.release.notify_one();
    }

    #[tokio::test]
    async fn reconnect_replays_multiple_pages_without_missing_or_duplicate_events() {
        const BURST_EVENTS: u64 = 2_200;
        let executor = Arc::new(BurstExecutor {
            release: tokio::sync::Notify::new(),
            emitted: tokio::sync::Notify::new(),
            events: BURST_EVENTS,
        });
        let hosted: Arc<dyn RuntimeClient> =
            Arc::new(InProcessRuntimeClient::from_executor(executor.clone()));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits::default()).expect("valid journal limits"),
        );
        let connector = Arc::new(SwitchableConnector {
            host: RemoteRuntimeHost::new(hosted, journal),
            current: StdMutex::new(None),
            connections: AtomicUsize::new(0),
            attach_requests: Arc::new(AtomicUsize::new(0)),
            attach_completions: Arc::new(AtomicUsize::new(0)),
            reject_connections: AtomicBool::new(false),
        });
        let client = RemoteRuntimeClient::with_recovery_policy(
            connector.clone(),
            RemoteRecoveryPolicy {
                max_attempts: 30,
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_millis(100),
            },
        )
        .expect("valid recovery policy");
        let handle = client
            .acquire(runtime_spec("runtime-paged-replay"))
            .await
            .expect("acquire runtime");
        let turn = handle
            .start_turn(TurnInput::new(
                InvocationId::new("turn-paged-replay").expect("valid invocation identifier"),
                "emit a replay burst",
            ))
            .await
            .expect("start remote turn");
        connector.reject_connections.store(true, Ordering::Release);
        connector.disconnect_current();
        executor.release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), executor.emitted.notified())
            .await
            .expect("burst should enter the bounded event pipeline");
        connector.reject_connections.store(false, Ordering::Release);

        let (mut events, completion) = turn.into_parts();
        let mut sequences = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(5), events.next()).await {
                Ok(Some(envelope)) => sequences.push(envelope.sequence),
                Ok(None) => break,
                Err(error) => panic!(
                    "paged replay stalled after {} events, {} connections, {} attach requests, and {} completed attaches: {error:?}",
                    sequences.len(),
                    connector.connections.load(Ordering::Acquire),
                    connector.attach_requests.load(Ordering::Acquire),
                    connector.attach_completions.load(Ordering::Acquire),
                    error = error,
                ),
            }
        }
        let result = completion.wait().await.expect("paged replay result");
        assert_eq!(result.text, "burst complete");
        assert_eq!(
            sequences.len(),
            usize::try_from(BURST_EVENTS).expect("bounded test event count") + 2
        );
        assert_eq!(sequences.first(), Some(&1));
        assert_eq!(sequences.last(), Some(&(BURST_EVENTS + 2)));
        assert!(sequences
            .windows(2)
            .all(|window| window[1] == window[0] + 1));
        assert!(connector.attach_completions.load(Ordering::Acquire) >= 2);
    }

    #[tokio::test]
    async fn interaction_answer_survives_a_carrier_outage_without_reasking_the_user() {
        let hosted: Arc<dyn RuntimeClient> = Arc::new(InProcessRuntimeClient::from_executor(
            Arc::new(QuestionExecutor),
        ));
        let journal = Arc::new(
            InMemoryEventJournal::new(EventJournalLimits::default()).expect("valid journal limits"),
        );
        let connector = Arc::new(SwitchableConnector {
            host: RemoteRuntimeHost::new(hosted, journal),
            current: StdMutex::new(None),
            connections: AtomicUsize::new(0),
            attach_requests: Arc::new(AtomicUsize::new(0)),
            attach_completions: Arc::new(AtomicUsize::new(0)),
            reject_connections: AtomicBool::new(false),
        });
        let client = RemoteRuntimeClient::with_recovery_policy(
            connector.clone(),
            RemoteRecoveryPolicy {
                max_attempts: 30,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(20),
            },
        )
        .expect("valid recovery policy");
        let handle = client
            .acquire(runtime_spec("runtime-interaction-recovery"))
            .await
            .expect("acquire runtime");
        let responder = Arc::new(GatedQuestionResponder {
            calls: AtomicUsize::new(0),
            requested: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut input = TurnInput::new(
            InvocationId::new("turn-interaction-recovery").expect("valid invocation identifier"),
            "ask a question",
        );
        input.interaction_policy = crate::retained::InteractionPolicy::RequireHandler;
        let interactions: Arc<dyn InteractionHandler> = responder.clone();
        let turn = handle
            .start_turn_with_interactions(input, interactions)
            .await
            .expect("start interactive turn");

        tokio::time::timeout(Duration::from_secs(2), responder.requested.notified())
            .await
            .expect("question should reach the application handler");
        connector.reject_connections.store(true, Ordering::Release);
        connector.disconnect_current();
        responder.release.notify_one();
        tokio::time::sleep(Duration::from_millis(75)).await;
        connector.reject_connections.store(false, Ordering::Release);

        let result = tokio::time::timeout(Duration::from_secs(3), turn.wait())
            .await
            .expect("interactive turn should recover")
            .expect("interactive turn result");
        assert_eq!(result.text, "question answered");
        assert_eq!(responder.calls.load(Ordering::Acquire), 1);
        assert!(connector.connections.load(Ordering::Acquire) >= 2);
    }
}
