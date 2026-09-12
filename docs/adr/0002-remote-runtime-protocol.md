# ADR 0002: Versioned remote runtime protocol

- Status: Accepted
- Date: 2026-09-02

## Context

A runtime may execute in the application process, on an SSH target, inside a
sandbox service, or behind a separately deployed runtime host. Reusing an
application's HTTP/chat schema for remote execution would couple provider
lifecycle to one product and make delivery ambiguity impossible to represent.

## Decision

The SDK defines a versioned protocol independently from its network carrier.
Version 1 supports acquire, attach with per-invocation replay cursors, start,
interrupt, health, and dispose. Host frames return typed responses, normalized
events, or `RuntimeFailure` with delivery and retry semantics.

Every client request has an application-generated request ID for correlation and
idempotency. Runtime and invocation IDs remain the stable execution identities.
Frames are bounded to 2 MiB before decoding.

Authentication, authorization, storage, and the concrete carrier (HTTP,
WebSocket, Unix socket, or an authenticated tunnel) remain host-application
responsibilities. Protocol secret values redact `Debug`, but they may only be
serialized onto an authenticated encrypted channel.

## Consequences

- In-process and remote clients can share lifecycle semantics.
- A host can replay events after a durable cursor without assuming a chat
  database schema.
- Unknown protocol versions fail before work is accepted.
- A later network host/client implementation can be tested against stable DTOs
  and codecs rather than inventing messages inside handlers.
