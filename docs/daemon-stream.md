# Embedding a retained runtime daemon

Keep one `InProcessRuntimeClient` and `RemoteRuntimeHost` alive in the daemon,
not one per network request or chat turn. Wrap the existing `AgentRuntime` in
that retained client and provide a bounded `EventJournal` to the host.

After authenticating a connection, pass its read/write halves to
`protocol_stream::serve_authenticated_stream`. On the application side, implement
`RemoteRuntimeConnector` to open the private socket or authenticated tunnel and
return `AuthenticatedStreamConnection`. Use `RemoteRuntimeClient` for acquire,
attach, turns, approvals, interrupts, configuration updates, and disposal.

The carrier uses a four-byte big-endian payload length followed by an SDK JSON
frame. Both directions enforce the SDK's maximum frame size before allocating
incoming bodies. Reads and writes are independent, ordered, and backpressured.
After any stream error or cancelled frame write, reconnect rather than reusing
the potentially partial stream.

## Security and lifecycle ownership

- Authenticate before handing streams to the SDK. This is not a public listener.
- Use a separate host for each security principal. Knowing a runtime ID does not
  authorize access; the carrier must enforce ownership on every reconnect.
- Bound admitted connections and apply read deadlines at the listener. The host,
  journal, and retained client have their own request/event/runtime limits.
- A socket disconnect is not session disposal. Keep the host alive and let accepted
  dispatches finish; reconnect with attach/replay rather than starting duplicate turns.
- Explicit disposal ends the retained runtime. Host process restart loses in-memory
  retained state; a durable journal alone does not restore native CLI processes.
- Keep managed application processes in a daemon-owned `ManagedProcessSupervisor`,
  independently of turn completion. This transport does not add process RPCs.
- Credential relays and authorization remain application-owned. Do not expose
  protocol payloads or credential-bearing environment fields in transport logs.
- Construct/allowlist relay destinations, token references, harness options,
  executable selection, and permission modes in trusted host code; do not pass
  unrestricted client configuration through. Codex model relays default to HTTPS
  or loopback on the provider's execution target. A trusted embedder may configure
  an exact isolated HTTP origin on the adapter (see the security policy); a turn
  cannot enable this exception.

Retained SDK sessions do not promise a permanently running native provider CLI.
Consult driver capabilities for resume/configuration behavior. The stream adapter
does not change provider execution or bypass provider sandbox policies.

## Verification

`cargo test --lib protocol_stream` checks bounded framing, truncated input, clean
disconnects, and SDK-client acquire/reattach/disposal through a daemon host over
duplex streams. It does not constitute live Claude/Codex/OpenCode or Temps UI E2E
verification; consumers must test those separately when adopting this carrier.
