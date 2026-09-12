# Recover a denied sandbox step

Use managed sandbox recovery when an application should let an operator widen
a sandbox profile and continue a blocked conversation without asking the user
to retype their message.

Recovery is opt-in. Ordinary `AgentRuntime::run` remains fail-closed and never
updates a profile or retries a turn.

## Implement profile persistence

`SandboxProfileManager` separates runtime orchestration from profile storage.
A host can implement it with SQL, an embedded database, a file, or a remote
policy service without changing the runtime.

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{
    ResolvedSandboxProfile, SandboxCapabilities, SandboxContext, SandboxError,
    SandboxProfileManager, SandboxProfileRef, SandboxProfileUpdate,
};

struct Profiles;

#[async_trait]
impl SandboxProfileManager for Profiles {
    fn name(&self) -> &'static str { "application-profiles" }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            filesystem: true,
            process_isolation: true,
            ..SandboxCapabilities::NONE
        }
    }

    async fn resolve(
        &self,
        profile: &SandboxProfileRef,
        context: &SandboxContext,
    ) -> Result<ResolvedSandboxProfile, SandboxError> {
        // Load the requested/current revision, validate and materialize it,
        // then return ResolvedSandboxProfile::new(exact_ref, backend).
        todo!()
    }

    async fn update(
        &self,
        request: SandboxProfileUpdate,
        context: &SandboxContext,
    ) -> Result<ResolvedSandboxProfile, SandboxError> {
        // Compare-and-swap on request.profile.revision, validate and durably
        // commit the new revision, then return its exact backend.
        todo!()
    }
}
```

An update must preserve `SandboxProfileRef::id` and return a non-empty new
revision. Treat the incoming revision as an optimistic concurrency token so two
operators cannot silently overwrite each other's policy changes.

For Nono, store the portable `ManagedProfile` in the application, apply an
approved `SandboxProfileChange` with `ManagedProfile::apply_change`, and call
`NonoManager::save` to validate and atomically materialize the new
content-addressed profile. Return a `NonoExecution` for that artifact from
`resolve` and `update`.

## Ask before widening access

Implement `SandboxRecoveryHandler` using the application's durable approval
workflow. A handler should normally present the denied resource and exact
proposed access to an operator. It returns either `Deny` or one explicit,
least-privilege `SandboxProfileChange`.

Do not infer write access from a denial that only proves a read was attempted.
Nono's event classifier leaves `SandboxResource::Path.access` empty when the
provider output is ambiguous.

## Run with bounded recovery

```rust,no_run
use temps_agent_runtime::{
    AgentRuntime, SandboxProfileRef, SandboxRecoveryPolicy, SandboxRequest,
};

# async fn run(
#   runtime: AgentRuntime,
#   mut request: temps_agent_runtime::TurnRequest,
#   events: &dyn temps_agent_runtime::EventSink,
#   interactions: &dyn temps_agent_runtime::InteractionHandler,
#   recovery: &dyn temps_agent_runtime::SandboxRecoveryHandler,
# ) -> temps_agent_runtime::Result<()> {
request.sandbox = Some(SandboxRequest::managed(
    Profiles,
    SandboxProfileRef::current("coding-default"),
));

let result = runtime.run_with_sandbox_recovery(
    request,
    events,
    Some(interactions),
    recovery,
    SandboxRecoveryPolicy { max_retries: 1 },
).await?;
# let _ = result;
# Ok(())
# }
```

The event stream records the state transition:

1. the failed `ToolCall` remains visible;
2. `SandboxAccessDenied` identifies the step and resource;
3. the recovery handler approves or denies a concrete change;
4. the manager commits a new revision;
5. `SandboxProfileUpdated` exposes that exact revision;
6. `SandboxStepRetrying` announces the retry;
7. the runtime resumes the same provider session and asks it to retry only the
   previously blocked operation.

Persist these events in order if conversations must survive reconnects. The
profile update is committed before `SandboxProfileUpdated` and before the retry
starts, so replay cannot claim a policy change that was never activated.

## Safety and failure behavior

- Recovery requires `SandboxRequest::managed`; a direct backend cannot be
  mutated by the runtime.
- The handler and each profile operation use the turn's bounded interaction
  deadline.
- At most eight retries can be configured; the default is one.
- A missing provider session ID stops recovery because a whole-turn replay can
  duplicate side effects.
- A manager error stops recovery. There is no unsandboxed fallback.
- Nono classifies only failed tool events containing a path and a recognized OS
  denial phrase. Ambiguous failures remain ordinary tool errors.
- Nono protection bypasses, credential changes, and backend-specific changes
  require application-owned handling; `ManagedProfile::apply_change` rejects
  them rather than weakening policy implicitly.
