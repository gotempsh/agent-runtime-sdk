# Adopt the SDK security hardening in Temps Fleet

This guide covers the security changes through SDK commit `6177487`. It is for
the Fleet adapter and execution-host owners; the SDK applies the low-level
protections, while Fleet remains responsible for tenant authorization,
persistence, and choosing explicit limits.

## Pin a hardened SDK revision

Use a published crate version containing `6177487` or pin that immutable Git
revision. Do not use a mutable branch dependency for production builds. Keep
`Cargo.lock` committed for the Fleet binary and rebuild every execution host
that embeds the SDK.

The hardening includes:

- SSH provider arguments and environment values are staged through an
  owner-only remote launcher instead of appearing in the local `ssh` process
  arguments;
- known environment secrets are redacted from surfaced provider and transport
  diagnostics;
- retained runtimes, pending protocol requests, and completed request replay
  entries have bounded capacities;
- skill mutation rejects symlink components and uses an owner-only temporary
  file before activation;
- MCP, Agent Relay, and Temps sandbox endpoint URLs receive strict structural
  validation;
- cleartext non-loopback Temps sandbox endpoints are denied by default.

## Configure capacities explicitly

The SDK defaults are bounded, but Fleet should choose values from its worker
and tenant limits rather than silently inheriting them.

```rust
use std::sync::Arc;
use temps_agent_runtime::{
    InProcessRuntimeClient, RemoteRuntimeHost, RemoteRuntimeHostLimits,
    RetainedRuntimeLimits,
};

let runtime_client = InProcessRuntimeClient::with_limits(
    runtime,
    RetainedRuntimeLimits { max_runtimes: 256 },
)?;

let protocol_host = RemoteRuntimeHost::with_limits(
    Arc::new(runtime_client),
    journal,
    RemoteRuntimeHostLimits {
        completed_request_capacity: 2_048,
        pending_request_capacity: 128,
    },
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use Fleet's durable queue for overload instead of increasing limits without a
memory budget. Treat runtime-capacity and pending-request-capacity failures as
backpressure: leave the invocation queued and retry with the same durable
invocation or request identity. Do not create a second invocation to work
around capacity.

Dispose inactive retained runtimes when Fleet retires a conversation or moves
its execution ownership. Disposal releases the SDK runtime slot; deleting a
Fleet UI record without disposing the runtime does not.

## Migrate endpoint configuration

Before enabling the hardened revision, inspect stored MCP, relay, and Temps
sandbox URLs. The following values are now rejected:

- schemes other than `http` or `https`;
- missing hosts, embedded `user:password` data, fragments, control characters,
  or URLs larger than 16 KiB;
- query strings in a Temps sandbox base URL;
- cleartext `http` Temps endpoints that are not loopback.

Move credentials out of URLs and into Fleet's encrypted secret storage. At
dispatch time, supply only short-lived, least-privilege values through
`SecretString` environment entries. The SDK keeps those values out of `Debug`
output and SSH arguments, but the provider harness and its descendants can
still read them. Use a credential broker when the harness must not possess the
credential.

Prefer HTTPS for every remote Temps sandbox endpoint. Fleet may use
`allow_insecure_http(true)` only when it has independently established that the
endpoint is carried over an authenticated encrypted private network such as
WireGuard or Tailscale. The flag is an explicit trust decision and should not
be derived from user-controlled URL text.

## Handle skill-management rejection

Fleet should display symlink rejection as an actionable security error. Do not
retry it with broader permissions. Ask the user to replace the provider skill
root or skill directory with a real directory on the selected execution host,
then retry the original durable operation.

Fleet still owns authorization for every extension mutation. A writable path
reported by extension discovery means only that the execution identity can
write it; it does not mean the current Fleet user is allowed to install,
replace, or remove a skill.

## Choose descendant-process behavior

`ToolProcessPolicy::PreserveOnCompletion` remains the compatibility default.
Fleet should select the policy deliberately per turn:

- use `TerminateOnCompletion` for unattended jobs, shared workers, and other
  turns that must not leave processes behind;
- use `PreserveOnCompletion` only when the product intentionally supports a
  development server or another descendant that outlives the turn, and then
  adopt it into Fleet's managed-process lifecycle.

Cancellation, timeout, a dropped turn, and event-sink failure still terminate
the supervised process tree regardless of this completion policy.

## Keep Fleet's application boundary

The SDK changes do not replace these Fleet requirements:

- authenticate the carrier before passing frames to `RemoteRuntimeHost`;
- authorize the runtime ID, working directory, sandbox, permission mode,
  launch context, environment keys, and MCP definitions for the current tenant;
- persist normalized events before publishing them to reconnecting clients;
- encrypt provider sessions, attachment references, prompts, tool payloads,
  and approval/question records at the appropriate storage boundary;
- never log complete protocol frames, `TurnEvent` payloads, or provider output;
- stage attachments on the selected execution host and verify that the tenant
  is allowed to reference each resulting path;
- keep durable invocation identity stable across delivery retries.

An authenticated client is not automatically authorized for every workspace
or provider capability on an execution host. Fleet must make that decision
before SDK dispatch.

## Temporary controls for open SDK findings

The following controls belong in Fleet only as short-term containment. They do
not replace SDK fixes:

- do not use `InMemoryEventJournal` as a multi-tenant production journal;
  provide Fleet's durable byte-budgeted journal implementation, or configure
  substantially smaller count limits and isolate it per worker while the SDK
  adds byte budgets;
- do not pass secret values through `HarnessMcpDefinition::Stdio::environment`
  when invoking persistent MCP management. Prefer per-turn `McpServerConfig`
  references backed by `TurnRequest::environment`, or require an explicit
  operator acknowledgement for non-secret configuration only;
- treat every Temps sandbox endpoint as a trusted control-plane dependency.
  Use low process concurrency, server-side response/log limits, and one
  least-privilege sandbox credential per trust boundary until the transport
  enforces response byte limits itself;
- accept a persisted `TransportProcessHandle` for a Temps sandbox only from
  Fleet-owned storage associated with the same tenant, target, and sandbox.
  Never accept a native job id supplied by a browser or other untrusted client;
- avoid running mutually untrusted processes in the same sandbox workspace
  while prompt stdin is staged there. Prefer SSH/local transports or a
  single-tenant sandbox until the Temps API supports exclusive, no-follow
  temporary-file creation;
- restrict OpenCode to prompts that may appear in execution-host process
  metadata. Claude and Codex use stdin; the current OpenCode CLI adapter passes
  its prompt as a process argument;
- allowlist per-turn environment names. Do not let browser or tenant input set
  process-control variables such as `PATH`, `HOME`, dynamic-loader variables,
  language-runtime preload options, or provider configuration roots;
- do not treat provider process-group termination as a security sandbox. A
  deliberately detached descendant may leave the provider's process group;
  use an outer sandbox or OS workload boundary for untrusted tools and have
  Fleet reconcile any adopted long-running services.

Fleet should remove these workarounds after adopting SDK releases that close
the corresponding findings. Keep the controls centralized in the execution
adapter rather than scattering provider-specific checks through UI code.

## Rollout verification

For each target type (local, SSH, and sandbox), verify:

1. A normal Claude turn starts and resumes the same provider session.
2. Process inspection does not show prompt or environment-secret values in
   local `ssh` arguments.
3. Provider diagnostics redact an injected test secret.
4. Capacity exhaustion leaves work queued and does not duplicate an
   invocation.
5. A symlinked skill target is rejected without changing the linked location.
6. Invalid or credential-bearing endpoint URLs fail before provider spawn.
7. Cleartext non-loopback Temps endpoints fail unless the target has an
   explicit private-network exception.
8. Cancellation and timeout terminate the provider process tree.
9. Conversation disposal releases its retained-runtime capacity.

Roll out by a persisted conversation cohort. A fallback must happen only
before SDK delivery is accepted; never execute the same durable invocation
through both the SDK and the legacy adapter.
