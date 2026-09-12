# Discover provider harnesses on an execution target

Use `AgentRuntime::discover_harnesses` during onboarding or when an execution
target changes. It probes every registered provider inside the configured
transport and returns one typed inventory.

## Probe the target, not the SDK host

Build the runtime with the transport the user selected, then call discovery:

```rust,no_run
use temps_agent_runtime::{AgentRuntime, HarnessStatus, SshTransport};

# async fn inspect() -> Result<(), Box<dyn std::error::Error>> {
let ssh = SshTransport::builder("agent.example.com")
    .user("runtime")
    .identity_file("/run/secrets/agent-key")
    .known_hosts_file("/etc/agent-runtime/known_hosts")
    .build()?;
let runtime = AgentRuntime::builder().transport(ssh).build()?;

let inventory = runtime.discover_harnesses().await;
for harness in inventory.harnesses {
    println!("{}: {:?}", harness.provider, harness.status);
    if harness.status == HarnessStatus::Ready {
        println!("  models: {}", harness.models.models.len());
        for group in harness.control_groups {
            println!("  {}: {:?}", group.label, group.options);
        }
    }
}
# Ok(())
# }
```

The same code works with `LocalTransport`, `TempsSandboxTransport`, or a custom
`ExecutionTransport`. SSH and managed-sandbox checks run inside that target.
Discovery never falls back to a binary installed on the Rust server.

When the selected target receives short-lived credentials or resolves project
configuration from a specific directory, use `ProviderProbeContext` instead of
ambient process state:

```rust,no_run
use temps_agent_runtime::{ProviderProbeContext, SecretString};

# async fn load_short_lived_credential() -> Result<String, Box<dyn std::error::Error>> { Ok(String::new()) }
# async fn inspect(runtime: &temps_agent_runtime::AgentRuntime) -> Result<(), Box<dyn std::error::Error>> {
let context = ProviderProbeContext::new("/workspace/project").with_environment(
    "ANTHROPIC_API_KEY",
    SecretString::new(load_short_lived_credential().await?),
);
let inventory = runtime.discover_harnesses_with(context).await?;
# let _ = inventory;
# Ok(())
# }
```

The SDK applies that context to metadata processes inside the selected
transport. It bounds environment names and values before spawning, keeps
values out of `Debug`, and redacts them from catalog diagnostics. It does not
load or refresh credentials; the embedding application owns that policy.

For SSH targets, the transport selects the remote user's configured Zsh or
Bash (falling back to an installed Zsh, then Bash) and opens it once as an
interactive login shell. It caches only the resulting `HOME`, `PATH`, and
`SHELL`. Each harness is then checked directly with `<executable> --version`
using that captured path, and subsequent provider processes reuse the same
values. This finds user installations such as Homebrew or `~/.bun/bin` without
importing API keys or other arbitrary shell environment variables into the
provider process.

## Interpret readiness

Each `HarnessReadiness` has one status:

- `Ready`: the executable is installed and the transport supplies the process
  capabilities required by that adapter.
- `NotInstalled`: the target was reachable, but executing the bare harness name
  through the target environment returned command-not-found (exit code 127).
- `Incompatible`: the executable exists, but the transport cannot safely run
  it. Inspect `limitations` for a stable reason.
- `Unavailable`: the target could not be inspected. Inspect the typed
  `TransportError` and its `retryable` field.

Executable status and provider authentication are deliberately separate.
Inspect `HarnessReadiness::authentication.status` before enabling a provider:

- `Authenticated`: a provider-native status command confirmed a usable identity.
- `Required`: no provider identity is configured on the target.
- `Rejected`: configured credentials were expired or rejected.
- `Unavailable`: the bounded status probe failed; show `reason` and honor
  `retryable`.
- `Unknown`: the adapter cannot prove authentication without a real request.

Claude uses `claude auth status --json`; Codex uses `codex login status`.
OpenCode remains `Unknown` because one OpenCode installation can expose models
from multiple providers with different credentials. An application may combine
that state with its own provider/account readiness, but must not relabel
`Unknown` as authenticated.

`PermissionSupport` is the backwards-compatible provider-neutral summary. New
integrations should render `control_groups`, where each group includes a stable
request key, semantic kind, provider-native values, default, description, and
danger marker.

Claude returns `default`, `acceptEdits`, `plan`, `auto`, `dontAsk`, and
`bypassPermissions` in `permission_mode`. Codex keeps `approval_policy`,
`sandbox_mode`, and `collaboration_mode` separate. OpenCode returns its
`permission_mode` and `agent` groups. Do not share one hard-coded permission
dropdown across providers.

## Use the model catalog

Models are fetched by a metadata-only provider command inside the same
transport as the eventual turn. Discovery never asks a model to generate a
response:

- Codex uses app-server `model/list`. The result includes model-specific
  reasoning efforts and service tiers; `priority` is the Fast tier. `Ultra` is
  present only on models whose native catalog advertises it.
  `collaborationMode/list` supplies Plan and Work separately from approval
  and sandbox policy.
- OpenCode uses `opencode models` without `--refresh`, reflecting providers
  configured in that environment.
- Claude Code receives a prompt-free `initialize` control request over its
  stream-JSON channel. The response contains the authenticated installation's
  concrete selectable models, native descriptions, and per-model effort
  levels. The ambiguous `default` alias is omitted; its resolved model marks
  the matching concrete entry as the catalog default. The normalized thinking
  picker also includes `Off` when the model permits disabled adaptive thinking.
  `Ultra code` is exposed only when Claude advertises both the session-scoped
  `ultracode` command and dynamic workflows; it launches `xhigh` effort with
  workflow orchestration and is not the same value as Codex `Ultra`.

Render `HarnessReasoningEffort::label`, not a title-cased `id`. Provider labels
carry distinctions such as Claude `Ultra code`, Codex `Ultra`, and the friendly
`Extra high` label for the wire value `xhigh`.

Check `HarnessModelCatalog::status` before rendering. `Ready` is complete,
`Partial` is usable but incomplete, `Unsupported` means no metadata endpoint is
available, and `Failed` includes a typed, retryable-aware error. Catalog failure
does not make an otherwise runnable harness unavailable. Error kinds distinguish
provider authentication, permission, model availability, rate limiting, and
network failures from transport, protocol, timeout, and generic command
failures. This lets a host show “authentication required” without treating an
installed executable as missing.

## Apply and persist the selection

Use the same keys returned by discovery:

```rust,no_run
use temps_agent_runtime::{Provider, TurnRequest};

let mut request = TurnRequest::new(Provider::Codex, "/workspace", "Inspect the auth flow");
request.model = Some("gpt-5.6-sol".into());
request.reasoning = Some("low".into());
request.harness_options.insert("approval_policy".into(), "on-request".into());
request.harness_options.insert("sandbox_mode".into(), "workspace-write".into());
request.harness_options.insert("collaboration_mode".into(), "plan".into());
request.harness_options.insert("service_tier".into(), "priority".into());
```

Unknown control keys and values are rejected as typed invalid requests. Store
`model`, `reasoning`, and `harness_options` with the conversation so a resumed
run keeps the user's actual provider configuration.

## Keep discovery bounded and explicit

Provider probes run concurrently across a fixed built-in set. Metadata time and
output are bounded. Discovery does not scan SSH hosts, sandbox accounts, home
directories, or session histories. Run it after a user selects one target,
cache the safe result for that target, and offer an explicit refresh action.

Never put SSH keys, tokens, cookies, or provider credentials in an inventory.
`HarnessInventory` contains transport capabilities, executable metadata,
provider controls, model metadata, limitations, and bounded typed errors.

The [full-stack runtime console](../../examples/fullstack-chat/README.md) shows
this flow for local, SSH, and managed-sandbox transports and persists subsequent
conversations and selections in SQLite.
