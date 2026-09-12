# Operate sandboxed turns

Use `SandboxRequest` to make isolation an explicit per-turn requirement. The
runtime checks capabilities before spawning the provider and never retries
without the requested sandbox.

## Choose direct or managed configuration

Use a direct backend when the configuration is immutable for the duration of
the request:

```rust,no_run
use temps_agent_runtime::{
    SandboxCapabilities, SandboxRequest, TurnRequest,
};

# fn backend() -> impl temps_agent_runtime::SandboxBackend { todo!() }
# fn configure(request: &mut TurnRequest) {
request.sandbox = Some(
    SandboxRequest::new(backend()).requiring(SandboxCapabilities {
        filesystem: true,
        process_isolation: true,
        ..SandboxCapabilities::NONE
    }),
);
# }
```

Use `SandboxRequest::managed` when profiles live in application storage and an
operator may approve a least-privilege update. The profile reference should
contain a stable ID and an exact revision.

## Require the controls the turn needs

Treat `SandboxCapabilities` as a preflight contract. Request only controls the
operation actually needs, but fail if the selected backend cannot provide
them. Current capability fields cover filesystem boundaries, network blocking
or allowlists, process isolation, credential proxies, audit support, and
explicit proxy CA trust.

Capability declarations come from backend implementations; they are not
runtime attestation. Test each configured backend and version in your own
deployment environment.

## Persist profile revisions outside the runtime

Implement `SandboxProfileManager` around your database and materializer:

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
        // Load the exact revision, validate it, materialize a backend, and
        // return the same profile identity with a non-empty revision.
        # let _ = (profile, context);
        todo!()
    }

    async fn update(
        &self,
        request: SandboxProfileUpdate,
        context: &SandboxContext,
    ) -> Result<ResolvedSandboxProfile, SandboxError> {
        // Compare-and-swap request.profile.revision, apply only request.change,
        // validate, commit, then return the newly activated exact revision.
        # let _ = (request, context);
        todo!()
    }
}
```

Do not mutate a shared profile in place. Optimistic revision checks prevent two
operators from silently overwriting each other's policy changes.

## Recover a denied step

Use `run_with_sandbox_recovery` only when all of these are configured:

- a managed sandbox request;
- a backend that can classify a denial reliably;
- a `SandboxRecoveryHandler` that obtains application authorization;
- a bounded `SandboxRecoveryPolicy`;
- durable event and profile storage.

The observable state sequence is:

```text
ToolCall(Failed)
  → SandboxAccessDenied
  → operator denies, or approves one exact SandboxProfileChange
  → SandboxProfileUpdated(new exact revision)
  → SandboxStepRetrying
  → same provider session retries only the blocked step
```

Persist every transition. The manager must commit and validate the new profile
before `SandboxProfileUpdated` is emitted. The runtime then resumes the same
provider session automatically; the user should not need to resend the
original message.

If the provider supplied no resumable session ID, the runtime emits a warning
and does not retry. If classification is ambiguous, no access change is
proposed. The original failed tool attempt remains immutable.

## Use Nono explicitly

The optional `nono` Cargo feature supplies `NonoExecution` for direct profiles
and `NonoManager` for generated managed profiles. `NonoMode::Run` and
`NonoMode::Wrap` enforce different controls; inspect `NonoMode::capabilities()`
and require the fields your turn needs.

Never place API keys directly in generated profile text or process arguments.
Use named credential proxies where supported. Nono remains an outer process
boundary; the embedding host application is not irreversibly sandboxed
in-process.

See [Manage Nono sandboxes](nono.md) for concrete configuration and
[Implement a sandbox backend](custom-sandbox.md) for another isolation system.

## Reconcile failure and process lifetime

- Sandbox preparation failure is terminal and starts no provider process.
- Missing required capabilities are terminal and never trigger an unsandboxed
  fallback.
- Cancellation, timeout, sink failure, or dropping the turn future terminates
  the supervised provider tree.
- A sandbox backend or supervisor may prevent tool children from surviving a
  natural turn exit even when `ToolProcessPolicy` asks the SDK to preserve
  them.
- After a host restart, mark in-flight sandbox attempts interrupted; stored
  events and profile revisions do not recreate the lost provider process.

See [Persist command execution](persist-command-execution.md) for durable tool
attempts and [Persist approvals](persist-approvals.md) for the separate human
authorization boundary.
