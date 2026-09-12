# Implement a sandbox backend

Use `SandboxBackend` when an application needs an isolation mechanism other
than Nono—for example Bubblewrap or a local container runtime. Use
`ExecutionTransport` when the provider itself runs on a remote worker, managed
sandbox service, or hosted microVM.

A backend is a trusted extension. It receives a separated provider command and
must either return an equally separated prepared command or fail. It must never
silently remove a requested restriction.

## Implement the trait

This illustrative backend invokes a hypothetical `my-sandbox` executable. The
wrapper's real security properties and flags are the implementer's
responsibility.

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{
    CommandSpec, SandboxBackend, SandboxCapabilities, SandboxContext,
    SandboxError,
};

struct ReadOnlySandbox;

#[async_trait]
impl SandboxBackend for ReadOnlySandbox {
    fn name(&self) -> &'static str {
        "my-read-only-sandbox"
    }

    fn capabilities(&self) -> SandboxCapabilities {
        SandboxCapabilities {
            filesystem: true,
            process_isolation: true,
            ..SandboxCapabilities::NONE
        }
    }

    async fn prepare(
        &self,
        context: SandboxContext,
        command: CommandSpec,
    ) -> Result<CommandSpec, SandboxError> {
        let arguments = vec![
            "--read-only".into(),
            "--workdir".into(),
            context.working_directory.into_os_string(),
        ];
        Ok(command.wrap_with("my-sandbox", arguments, Some("--".into())))
    }
}
```

`CommandSpec::wrap_with` preserves the provider's stdin mode, explicit
environment, sanitized-environment setting, executable, and argument
boundaries. It does not validate that the wrapper really enforces its claimed
capabilities.

## Require capabilities per turn

```rust,no_run
# use temps_agent_runtime::{SandboxCapabilities, SandboxRequest, TurnRequest};
# struct ReadOnlySandbox;
# fn configure(request: &mut TurnRequest) {
request.sandbox = Some(
    SandboxRequest::new(ReadOnlySandbox).requiring(SandboxCapabilities {
        filesystem: true,
        process_isolation: true,
        ..SandboxCapabilities::NONE
    }),
);
# }
```

The runtime checks requirements before acquiring provider execution resources.
If a backend does not advertise every required control, the turn returns
`SandboxError::MissingCapabilities` and no provider process starts.

## Share a configured backend

Backends are immutable, `Send + Sync`, and may be shared across turns:

```rust,no_run
# use std::sync::Arc;
# use temps_agent_runtime::{SandboxBackend, SandboxRequest};
# fn select(backend: Arc<dyn SandboxBackend>) -> SandboxRequest {
SandboxRequest::from_arc(backend)
# }
```

Keep per-turn non-secret policy inside the configured backend instance. Keep
secret material in the external credential system; avoid fields whose `Debug`
implementation could disclose values.

## Failure and cancellation rules

- Backend preparation runs inside the turn deadline.
- Cooperative cancellation interrupts async preparation.
- Any preparation error stops the original turn.
- The runtime never retries with another backend or without a sandbox.
- After spawn, the ordinary process-tree cancellation guarantee applies to the
  prepared command.

If preparation starts auxiliary processes or creates temporary resources, the
backend must arrange cleanup when its future is dropped. Prefer a wrapper that
remains the parent of the provider process or returns a command with an
external lifecycle guard.

## Test a backend

At minimum, test:

- exact argv boundaries, including paths with spaces;
- preservation of stdin and explicit environment values;
- every advertised capability;
- missing-capability failure before provider spawn;
- preparation errors and cancellation;
- provider child and grandchild termination;
- behavior on every supported operating system.
