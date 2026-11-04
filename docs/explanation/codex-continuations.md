# Codex completion and automatic continuation

A Codex app-server `turn/completed` notification ends a provider segment. It
need not end the application's invocation: a goal can immediately start another
provider turn on the same thread. Returning the SDK result at the first segment
made hosts show Ready, discard later activity, and lose the interrupt handle while
Codex kept running.

The app-server adapter now reconciles successful segment completion before it
returns a terminal result:

1. Read `thread/goal/get`. An active goal keeps the invocation busy, even when a
   thread status notification says idle between segments. Method-not-found falls
   back to a thread read for Codex versions without goals; other failures do not
   imply idleness.
2. With no active goal, read `thread/read` with `includeTurns: false`, and accept
   only the matching thread's idle status. This is a bounded metadata read, not
   a transcript scan. It is driven by lifecycle events, not a polling loop.
3. A new `turn/started` on that thread while completion is pending adopts its
   new ID. Its output and approvals stay in the same SDK invocation; Stop uses
   the new ID. Previous segment output, foreign threads, and replies to old
   completion probes cannot end or contaminate it. Probe IDs are unique across
   invocations sharing a connection, and segment filtering also applies to
   one-shot execution.

The host's ordinary Busy guard continues to serialize queued input and settings
that replace the process. There is no new public invocation or duplicate prompt
for a provider continuation. Manual compaction and explicit failed/interrupted
segments retain their own terminal semantics.

## Bounds and failure behavior

Completion reconciliation has a 30-second absolute bound per segment, in both
retained and one-shot execution. The normal turn inactivity/deadline can expire
sooner. Messages, status frames, and partial-frame arrival cannot extend the
reconciliation bound. Starting a real continuation leaves reconciliation and
returns to the existing active-turn watchdog. At most 1,024 completed segment
IDs are retained per invocation; exceeding that bound stops with an actionable
protocol error instead of growing an unbounded history.

A completed reply can survive a transport close or failed completion-probe write
only after the provider process tree has been retired, and only when no active
goal is known. This preserves the title-cleanup contract: a best-effort hidden
title cannot turn a completed reply into a failure. A server request from an
already completed segment is not presented as a fresh approval; the connection
is retired. An active goal in that inconsistent state fails instead of claiming
success.

An idle read is a point-in-time observation, not a promise that a provider will
never start work again. An unsolicited `turn/started` or active thread status
received **after** invocation completion retires the idle connection; it is not
silently dropped as background terminal output. Such late work is not resumed
as a new host invocation. Ordinary background terminal logs remain supported.

## Regression coverage

```sh
cargo test --lib --all-features
cargo test --no-default-features --features codex --test codex_app_server
```

The scripted app-server tests cover immediate continuation, Stop targeting the
continued turn, the Busy guard for next input and process-replacing settings,
active goals without a next turn, and the independent settlement deadline.
Parser tests cover foreign/previous turns, stale completion replies, missing and
malformed goal APIs, active goals across transient idle, and unsolicited work
on idle connections. Existing title cleanup and late-output tests retain their
assertions. The mock server now emits the resumed thread ID consistently rather
than requiring the parser to accept another thread's events.

These tests use no paid model calls. Schema compatibility was inspected with
installed Codex 0.162.1. A real goal continuation was not exercised because the
isolated test Codex home was not authenticated; no production credentials were
copied into it.
