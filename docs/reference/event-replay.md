# Event replay

Remote and durable hosts implement `EventJournal`. The SDK-provided
`InMemoryEventJournal` is a bounded process-local implementation for embedded
hosts, development, and conformance tests; it is not a database replacement.

Each invocation starts at sequence 1. Appends are idempotent only when both the
sequence and complete envelope match. Gaps and conflicting reuse are typed
errors instead of being silently reordered.

`RuntimeEvent` makes the replay self-contained: `InvocationStarted` is followed
by normalized `ProviderEvent` values and exactly one terminal
`InvocationCompleted` or `InvocationFailed` event. Applications can rebuild an
invocation projection without consulting an in-memory completion receiver.

Replay takes a durable last-seen sequence per invocation and returns a bounded,
deterministically ordered batch plus updated cursors. `only_invocations` limits
the batch to the active invocations named by an attaching client. An empty set
intentionally replays no history, so attaching a fresh client cannot leak old,
unrelated turns into its event stream.

`EventReplayBatch::truncated` reports that another page remains. Consumers must
advance from `next` and request another page before accepting live delivery.
When retention has removed required events, replay returns
`ReplayWindowExpired`; the caller must reconcile from its durable projection or
provider session rather than pretending the tail is complete.

The in-memory defaults retain at most 128 runtimes, 256 invocations per runtime,
4,096 events per invocation, and return at most 2,048 events per replay. Supply
explicit `EventJournalLimits` for a host with different memory requirements.
