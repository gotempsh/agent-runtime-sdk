//! Opt-in, payload-free timing observations for provider startup.

use std::sync::atomic::{AtomicU64, Ordering};
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
    /// The first provider output line arrived; it may be a handshake or error.
    FirstOutput,
    /// The first nonempty normalized assistant text delta arrived.
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
}

/// Receives startup boundaries without modifying provider or wire events.
///
/// Implementations must return promptly: use a bounded `try_send` to export
/// samples, or record them locally. Do not perform network or filesystem work
/// in this callback. Under unwinding builds, observer panics are contained and
/// cannot fail a turn; `panic=abort` still terminates the host process.
pub trait StartupObserver: Send + Sync {
    /// Record one payload-free sample.
    fn observe(&self, timing: StartupTiming);
}

static NEXT_OBSERVATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct StartupTrace {
    observer: Option<Arc<dyn StartupObserver>>,
    observation_id: u64,
    provider: Provider,
    started: Instant,
    finished: bool,
}

impl StartupTrace {
    pub(crate) fn new(provider: Provider, observer: Option<Arc<dyn StartupObserver>>) -> Self {
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
            };
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer.observe(timing);
            }));
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

impl Drop for StartupTrace {
    fn drop(&mut self) {
        if !self.finished {
            self.record(StartupStage::Abandoned);
        }
    }
}
