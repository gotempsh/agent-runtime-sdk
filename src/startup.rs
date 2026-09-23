//! Opt-in, payload-free timing observations for provider startup.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{Provider, RuntimeError};

/// A measured boundary, not an inferred provider readiness signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartupStage {
    /// The runtime received a turn request, before validation.
    Started,
    /// Request, working-directory and provider controls passed validation.
    Validated,
    /// Waiting for the runtime's process concurrency permit.
    WaitingForPermit,
    /// The runtime acquired a process concurrency permit.
    PermitAcquired,
    /// The adapter built its command and explicit environment.
    CommandPrepared,
    /// The requested sandbox finished preparing the command.
    SandboxPrepared,
    /// The execution transport returned a spawned process.
    ProcessSpawned,
    /// Initial protocol bytes were flushed; this does not prove acceptance.
    InitialInputWritten,
    /// Provider streams were attached, including HTTP readiness where required.
    StreamsAttached,
    /// The runtime read its first provider output line; it may be a handshake or error.
    FirstOutput,
    /// The runtime parsed its first nonempty assistant text delta, before delivery.
    /// Earlier event-sink waits are included in `elapsed`; inspect
    /// [`StartupTiming::event_delivery_elapsed`] to distinguish them.
    FirstText,
    /// The turn returned successfully.
    Succeeded,
    /// The turn returned an error other than cancellation or timeout.
    Failed,
    /// The turn returned cooperative cancellation.
    Cancelled,
    /// The turn deadline expired.
    TimedOut,
    /// The caller dropped the execution future before it returned.
    Abandoned,
}

/// A single timing sample containing no prompts, paths, session IDs or secrets.
#[derive(Debug, Clone, Copy)]
pub struct StartupTiming {
    /// Process-local observation ID; shared by all samples for one run call.
    pub observation_id: u64,
    /// Provider whose execution is being observed.
    pub provider: Provider,
    /// Boundary just reached.
    pub stage: StartupStage,
    /// Monotonic elapsed time since the run call began.
    pub elapsed: Duration,
    /// Cumulative time awaiting the application's event sink before this sample.
    /// Subtract interval differences to identify delivery backpressure; the
    /// remainder still includes protocol, observer and scheduling overhead.
    pub event_delivery_elapsed: Duration,
}

/// Receives startup boundaries without modifying provider or wire events.
///
/// Implementations must return promptly: use a bounded `try_send` to export
/// samples, or record them locally. Do not perform network or filesystem work
/// in this callback. Under unwinding builds, observer panics are contained and
/// cannot fail a turn; `panic=abort` still terminates the host process.
/// A panicking observer is disabled for the runtime and its clones. Already
/// in-flight callbacks may finish. Callbacks are skipped during stack unwinding.
pub trait StartupObserver: Send + Sync {
    /// Record one payload-free sample.
    fn observe(&self, timing: StartupTiming);
}

pub(crate) struct StartupObserverState {
    observer: Arc<dyn StartupObserver>,
    disabled: AtomicBool,
}

impl StartupObserverState {
    pub(crate) fn new(observer: Arc<dyn StartupObserver>) -> Self {
        Self {
            observer,
            disabled: AtomicBool::new(false),
        }
    }

    fn enabled(&self) -> bool {
        !self.disabled.load(Ordering::Acquire) && !std::thread::panicking()
    }

    fn observe(&self, timing: StartupTiming) {
        if !self.enabled() {
            return;
        }
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.observer.observe(timing);
        })) {
            // Disable before cleanup, which may itself call user-defined Drop.
            self.disabled.store(true, Ordering::Release);
            if let Err(cleanup_payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drop(payload);
                }))
            {
                // The second panic payload can also have a panicking Drop.
                // Quarantine it rather than recursively unwinding the host.
                // Further callbacks are disabled, limiting this exceptional
                // leak to callbacks already in flight when the observer failed.
                std::mem::forget(cleanup_payload);
            }
        }
    }
}

static NEXT_OBSERVATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct StartupTrace {
    observer: Option<Arc<StartupObserverState>>,
    observation_id: u64,
    provider: Provider,
    started: Instant,
    finished: bool,
    event_delivery_nanos: AtomicU64,
}

impl StartupTrace {
    pub(crate) fn new(provider: Provider, observer: Option<Arc<StartupObserverState>>) -> Self {
        let trace = Self {
            observation_id: if observer.is_some() {
                NEXT_OBSERVATION.fetch_add(1, Ordering::Relaxed)
            } else {
                0
            },
            observer,
            provider,
            started: Instant::now(),
            finished: false,
            event_delivery_nanos: AtomicU64::new(0),
        };
        trace.record(StartupStage::Started);
        trace
    }

    pub(crate) fn record(&self, stage: StartupStage) {
        if let Some(observer) = &self.observer {
            let timing = StartupTiming {
                observation_id: self.observation_id,
                provider: self.provider,
                stage,
                elapsed: self.started.elapsed(),
                event_delivery_elapsed: Duration::from_nanos(
                    self.event_delivery_nanos.load(Ordering::Relaxed),
                ),
            };
            observer.observe(timing);
        }
    }

    pub(crate) fn event_delivery(&self) -> EventDeliveryTimer<'_> {
        EventDeliveryTimer {
            trace: self,
            started: self
                .observer
                .as_ref()
                .filter(|observer| observer.enabled())
                .map(|_| Instant::now()),
        }
    }

    pub(crate) fn finish<T>(&mut self, result: &crate::Result<T>) {
        let stage = match result {
            Ok(_) => StartupStage::Succeeded,
            Err(RuntimeError::Cancelled { .. }) => StartupStage::Cancelled,
            Err(RuntimeError::Timeout { .. }) => StartupStage::TimedOut,
            Err(_) => StartupStage::Failed,
        };
        self.finished = true;
        self.record(stage);
    }
}

pub(crate) struct EventDeliveryTimer<'a> {
    trace: &'a StartupTrace,
    started: Option<Instant>,
}

impl Drop for EventDeliveryTimer<'_> {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let _ = self.trace.event_delivery_nanos.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |total| Some(total.saturating_add(nanos)),
            );
        }
    }
}

impl Drop for StartupTrace {
    fn drop(&mut self) {
        if !self.finished {
            self.record(StartupStage::Abandoned);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    struct SecondaryCleanupPanic;
    impl Drop for SecondaryCleanupPanic {
        fn drop(&mut self) {
            panic!("secondary panic payload must not be dropped");
        }
    }

    struct CleanupPanic;
    impl Drop for CleanupPanic {
        fn drop(&mut self) {
            std::panic::panic_any(SecondaryCleanupPanic);
        }
    }

    struct FailingObserver {
        calls: AtomicUsize,
    }
    impl StartupObserver for FailingObserver {
        fn observe(&self, _: StartupTiming) {
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                std::panic::panic_any(CleanupPanic);
            }
        }
    }

    #[test]
    fn observer_payload_cleanup_cannot_escape_or_repeat_after_failure() {
        let observer = Arc::new(FailingObserver {
            calls: AtomicUsize::new(0),
        });
        let state = Arc::new(StartupObserverState::new(observer.clone()));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut trace = StartupTrace::new(Provider::Claude, Some(state.clone()));
            trace.record(StartupStage::Validated);
            trace.finish(&Ok::<_, RuntimeError>(()));
            let mut next_turn = StartupTrace::new(Provider::Claude, Some(state.clone()));
            next_turn.finish(&Ok::<_, RuntimeError>(()));
        }));
        assert!(
            outcome.is_ok(),
            "observer cleanup escaped the isolation boundary"
        );
        assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
    }
    #[derive(Default)]
    struct CountingObserver {
        calls: AtomicUsize,
    }
    impl StartupObserver for CountingObserver {
        fn observe(&self, _: StartupTiming) {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn observer_is_not_called_while_the_caller_unwinds() {
        let observer = Arc::new(CountingObserver::default());
        let state = Arc::new(StartupObserverState::new(observer.clone()));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _trace = StartupTrace::new(Provider::Claude, Some(state));
            panic!("caller failed");
        }));
        assert!(result.is_err());
        assert_eq!(observer.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn interrupted_event_delivery_is_included_in_elapsed_accounting() {
        let state = Arc::new(StartupObserverState::new(Arc::new(
            CountingObserver::default(),
        )));
        let mut trace = StartupTrace::new(Provider::Claude, Some(state));
        let deadline = Duration::from_millis(25);
        let result = tokio::time::timeout(deadline, async {
            let _delivery = trace.event_delivery();
            std::future::pending::<()>().await;
        })
        .await;
        assert!(result.is_err());
        assert!(
            Duration::from_nanos(trace.event_delivery_nanos.load(Ordering::Relaxed)) >= deadline
        );
        trace.finish(&Ok::<_, RuntimeError>(()));
    }
}
