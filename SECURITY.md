# Security policy

## Supported versions

Until `1.0`, only the latest published `0.x` release receives security fixes.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private security advisory flow for `gotempsh/agent-runtime-sdk`. Include the
affected version, platform, provider/Nono versions, impact, and a minimal
reproduction that contains no real credentials or user data.

Maintainers will acknowledge a complete report within five business days and
coordinate disclosure after a fix is available.

## Security boundary

This crate supervises powerful local coding agents. Applications must
authorize workspaces and permission modes, protect event data, and configure
credentials. A provider process is not considered isolated unless the selected
provider sandbox or Nono capabilities enforce the required policy.

The runtime never promises that `NonoMode::Wrap` provides destination network
filtering or named credential proxying. Sandbox preparation failures are
fail-closed.

`TurnRequest::environment` is injected into the provider harness process. Its
values are redacted from SDK debug output and known surfaced diagnostics, and
the SSH transport keeps them out of local process arguments, but the harness
and its descendants can read them. Treat these values as short-lived scoped
capabilities. Use an external credential broker when the harness itself must
not possess a credential.

## Embedding the daemon

The SDK is a library, not an authenticated public service. Authenticate the
carrier before serving frames, isolate retained hosts by security principal,
and authorize workspace paths, permission modes, and configuration updates.
Do not deserialize public API input directly into unrestricted `TurnRequest`
or runtime configuration. Harness options, environment values, executable
selection, and MCP configuration are trusted host configuration.

In particular, the Codex `model_relay` destination and `token_env` reference must
be constructed or allowlisted by the host, not chosen by an untrusted caller.
By default the SDK accepts HTTPS relay URLs and HTTP only on loopback. HTTPS protects
transport confidentiality; it does not authorize the destination to receive a
credential. Loopback is relative to the provider's execution target, including
when that target is remote. Use TLS or a loopback tunnel for a remote relay;
there is no implicit insecure fallback.

A trusted embedder can configure `Codex::with_insecure_model_relay_origin` for
one exact HTTP origin on an independently isolated network. This is adapter
configuration, not a turn option: untrusted requests cannot enable it. It does
not encrypt traffic. The host must control the origin's DNS, network membership,
relay destination, and token reference. Temps uses this only for its isolated,
per-workspace sidecar; other origins remain rejected. Agent Relay MCP HTTP
exposure has no such exception and requires HTTPS or loopback.

See [daemon lifecycle and authorization responsibilities](docs/daemon-stream.md).
