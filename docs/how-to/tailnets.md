# Give an agent its own tailnet

The `tailnet` feature runs one userspace `tailscaled` per configured Tailscale
network and hands any provider command the environment it needs to reach that
network. The machine's own Tailscale login is never touched, so a host can be
signed into one tailnet while agents work on others at the same time.

## How it works

- Each tailnet gets a private state directory, a control socket, and a
  loopback SOCKS5 listener owned by its own `tailscaled --tun=userspace-networking`
  process. No TUN device and no root are required.
- A loopback split proxy fronts the daemon. MagicDNS names, `*.ts.net`
  hosts, and Tailscale address ranges go through the daemon; every other
  destination connects directly, so public APIs keep working.
- The daemon is a [`ManagedProcessSupervisor`](../reference/api.md) service with
  `RestartPolicy::Always`, bounded logs, and process-tree ownership.
- The module reports the login URL. The application owns persistence of which
  tailnets exist, its UI, and the browser login step.

## Provider boundary

Private-network lifecycle is defined by the provider-neutral
`network::NetworkProvider`, `network::NetworkSession`, and
`network::NetworkAccess` traits. `NetworkProviderRegistry` stores different
providers behind one object-safe interface and assigns provider identity to
the returned session, so persisted selection cannot disagree with a session's
self-reported identity. `TailscaleProvider` implements these contracts, while
the existing `TailnetDaemon::start` API remains available unchanged.

`NetworkAccess` applies provider-specific environment and exposes generic
sandbox requirements. A future WireGuard implementation can return an empty
environment when its interface and routes need no per-process configuration.
Provider capabilities are trusted UI/discovery hints only: never use them as
proof of isolation or to skip authorization and privilege prompts.

```rust,no_run
use std::sync::Arc;
use temps_agent_runtime::network::{NetworkInstanceSpec, NetworkProviderRegistry};
use temps_agent_runtime::tailnet::{
    TAILSCALE_PROVIDER_ID, TailscaleBinaries, TailscaleProvider,
};

# async fn start() -> Result<(), Box<dyn std::error::Error>> {
let binaries = TailscaleBinaries::discover()?;
let mut providers = NetworkProviderRegistry::new();
providers.register(Arc::new(TailscaleProvider::new(binaries)))?;
let spec = NetworkInstanceSpec::new(
    "client-a",
    "/var/lib/agent/tailnets/client-a",
)?;
let session = providers.start(&TAILSCALE_PROVIDER_ID, spec).await?;
let status = session.session().status().await;
# let _ = status;
# Ok(())
# }
```

Provider implementations own every process, route, interface, and credential
they create. Startup must clean partial resources when cancelled; stopping a
session must revoke connectivity before reporting success; dropping the last
session must clean up ephemeral resources. Authentication URLs are ephemeral
sensitive data and must not be persisted or logged. End managed sessions with
`ManagedNetworkSession::shutdown()` before reusing their state directory. A
dropped session keeps that directory reserved until process exit because
best-effort asynchronous teardown is not sufficient proof that reuse is safe.
The reservation is process-wide; separate host processes must use distinct
private state roots.

## Requirements

The open-source daemon and CLI must be installed: `brew install tailscale` on
macOS (the App Store app does not ship `tailscaled`), the distribution package
on Linux, or the MSI on Windows. `TailscaleBinaries::discover()` searches
`PATH` and the common install locations; `install_hint()` returns platform
guidance when nothing is found.

## Start a daemon and log in

```rust,no_run
use temps_agent_runtime::tailnet::{TailnetDaemon, TailnetSpec, TailnetState, TailscaleBinaries};

# async fn start() -> Result<(), Box<dyn std::error::Error>> {
let binaries = TailscaleBinaries::discover()?;
let spec = TailnetSpec::new("client-a", "/var/lib/agent/tailnets/client-a", binaries)?;
let daemon = TailnetDaemon::start(spec).await?;

let status = daemon.login().await?;
if status.state == TailnetState::NeedsLogin {
    if let Some(url) = &status.auth_url {
        println!("open {url} in a browser signed into that tailnet");
    }
}
# Ok(())
# }
```

`TailnetSpec::new` requires an absolute state directory and derives the node
hostname `agent-<slug>` from the name; `with_hostname` overrides it. The
state directory is created `0700` on Unix. Login state persists there, so a
daemon started again later reconnects without a new browser login.

Poll `status()` until `state` is `Running`. `restart()`, `stop()`, and
`logout()` cover upgrades, shutdown, and removal; `logout()` before deleting
the state directory so the node disappears from the tailnet.

## Apply it to a turn

Once the daemon is running, `access()` returns a `TailnetAccess`:

```rust,no_run
# use temps_agent_runtime::tailnet::TailnetDaemon;
# use temps_agent_runtime::CommandSpec;
# async fn apply(daemon: &TailnetDaemon, command: CommandSpec) -> Result<(), Box<dyn std::error::Error>> {
let access = daemon.access().await?;
let command = access.apply(command);
let system_prompt_note = access.guidance();
# let _ = (command, system_prompt_note);
# Ok(())
# }
```

`apply` adds:

| Variable | Purpose |
| --- | --- |
| `HTTP_PROXY`, `HTTPS_PROXY` (and lowercase) | the split proxy |
| `NO_PROXY`, `no_proxy` | `localhost,127.0.0.1,::1` |
| `NODE_USE_ENV_PROXY` | makes Node's built-in fetch honor the proxy |
| `TEMPS_TAILNET_NAME`, `TEMPS_TAILNET_ORG`, `TEMPS_TAILNET_DNS_SUFFIX` | identity |
| `TEMPS_TAILNET_SOCKS` | raw SOCKS5 endpoint for non-HTTP clients |
| `TEMPS_TAILNET_SSH_CONFIG` | ssh config routing tailnet hosts via `tailscale nc` |
| `TEMPS_TAILNET_SOCKET`, `TEMPS_TAILNET_TAILSCALE_BIN` | run `tailscale status` against this daemon |

`guidance()` is a short system-prompt note telling the agent the tailnet
exists and how to use those variables. Provider adapters do nothing special:
the environment is the whole interface, which is why it works the same for
Claude Code, Codex, and OpenCode.

## Combine with Nono

Set `NonoExecution::tailnet` and the wrapper opens the proxy and SOCKS ports,
the control socket, and the ssh config in addition to applying the
environment:

```rust,no_run
# use temps_agent_runtime::nono::NonoExecution;
# use temps_agent_runtime::tailnet::TailnetAccess;
# fn configure(mut sandbox: NonoExecution, access: TailnetAccess) {
sandbox.tailnet = Some(access);
// Only when the profile allowlists destinations:
sandbox.tailnet_chain_proxy = true;
# }
```

Nono replaces the proxy environment with its own filtering proxy whenever a
profile allowlists destinations. `tailnet_chain_proxy` passes
`--upstream-proxy` so Nono's proxy forwards to the split proxy; the allowlist
still applies first, so the profile must allow `*.ts.net` or the exact
MagicDNS names. Leave it `false` for unrestricted profiles. It requires
`NonoMode::Run`, and a profile that blocks all network access cannot be
combined with a tailnet.

## What stays reachable

The split proxy only decides which destinations go through the daemon. A
userspace `tailscaled` dials addresses that are not its own peers through the
host's normal network stack, so anything the host can already reach, including
the host's own system Tailscale login, remains reachable exactly as it is for
an unproxied process. Use the sandbox's network policy to restrict that; the
tailnet adds reachability, it does not remove any.

## Failure modes

- `TailnetError::Unavailable` when `tailscaled` or `tailscale` is missing;
  show `hint`.
- `TailnetError::NotConnected` from `access()` when the daemon is not
  `Running`; surface the state and detail so the user can log in.
- Login URLs expire. `login()` can be called again to start a fresh one.
- The `tailscale up` helper is bounded to fifteen minutes; the daemon keeps
  running and `status()` keeps reporting `NeedsLogin`.
