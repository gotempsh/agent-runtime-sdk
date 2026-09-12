# Remote runtime host

`RemoteRuntimeHost` consumes versioned `ClientFrame` values independently from
the authenticated carrier chosen by an application. Implement `HostFrameSink`
for a WebSocket, HTTP stream, Unix socket, or another bounded transport.

The host deduplicates mutating requests by request ID and a SHA-256 fingerprint.
Repeating the same frame returns its cached direct response; reusing an ID for a
different frame is rejected. Attach and health are safe to repeat and are not
cached.

Protocol version three preserves `TurnProvenance::Agent` across the remote
boundary. A version-two client cannot submit agent provenance, so a host never
silently turns a received relay envelope into an untyped ordinary prompt.

Protocol version four carries automatic-compaction policy and the retained
manual-compaction invocation kind. Older frames cannot request these features;
the codec returns `FeatureRequiresVersion` instead of silently executing a
different operation.

Started invocations are pumped into an `EventJournal` before live delivery. If a
client sink blocks for more than one second or disconnects, the provider keeps
running and the host keeps journaling. The client later attaches with durable
per-invocation cursors to replay missed events.

Sandbox names in protocol DTOs never instantiate arbitrary code. Supply an
authorized `ProtocolSandboxResolver` that maps a configured host-local backend
and optional profile to `SandboxRequest`; the default resolver rejects all
sandbox selections.
