# Retained capacity and safe reclamation

Retained entries are SDK handles, not visible root agents or live provider slots.
Dropping a handle does not remove its registry entry. Capacity admission remains
explicit: acquire does not silently dispose anything or replay a turn.

`InProcessRuntimeClient::try_dispose_idle` is the application's bounded cleanup
primitive. It rechecks exclusive registry ownership, inactive lifecycle and a
known zero local background-task count while fencing acquire/attach. Unknown
custom-executor background state is protected by default. Applications should
select a bounded candidate list, invoke this operation only before external
submission, and release their own capabilities only once their entries are gone.

Cleanup timeout/failure retains the disposed identity and refuses subsequent
automatic reclamation. An explicit disposal may retry cleanup; success removes
only that exact entry. Concurrent disposals share successful cleanup and cannot
remove a replacement generation. Existing explicit disposal still cancels active
work; it must not be used as an idle-capacity sweep.

`retained_snapshot` provides actual registry totals plus at most 100 selected
entries, stable IDs, provider/model, lifecycle/invocation kind, age, borrowed
ownership, known/unknown background count and advisory reclaim eligibility.
It returns no secrets or launch context. Eligibility is a snapshot, never
permission to use unchecked disposal later.

Tests reproduce overlapping disposal completion before cleanup, then prove
coalesced cleanup, borrowed/background/unknown protection, cleanup failure
retention, and 300 acquire/attach/reclaim interleavings. The retained lifecycle
suite also covers real accepted fixture turns, manual compaction, session resume,
background handoff and cancellation. No external model calls are required.

`RetainedRuntimeObserver` holds a weak registry reference. Applications can keep
replaced host generations discoverable while another operation owns the client,
without retaining the host, credentials or transport themselves. Once the last
client owner drops, snapshot returns `None`; an observer does not offer control.

## Compatibility and adoption

No existing acquire/dispose call needs to change. Acquisition still refuses a full
registry and never silently replays an invocation. Applications may choose the
new idle-only primitive before submitting external effects, then retry admission
once. Custom executors default to an unknown background-task count, which blocks
automatic reclamation; implement `retained_background_task_count` only when the
executor can report that fact authoritatively. Explicit disposal remains a
cancellation operation. Snapshots are advisory and do not authorize disposal.

Provider follow-ups are protected even after their last background task ID drains.
Pending answers, buffered continuations and queued interactions report unknown
reclamation state rather than zero. The provider regression consumes the original
follow-up after attempted reclamation and asserts one process and one prompt.

A native fixture also completes and reclaims 257 distinct provider runtimes
before idle expiry. Ordinary completed processes carry no parser state and are
reclaimable; absent parser state must not be mistaken for pending work.

Provider cleanup also retains its exact supervisor entry and transport until
termination acknowledges success. Cancellation, idle-cleanup timeout or transport
failure leaves that ownership available for explicit retry; an empty provider
registry cannot falsely confirm termination. The real-supervisor regression
checks both cancellation and timeout, blocks identity reuse during retry, then
releases it only after the transport acknowledgement.

Diagnostics release the registry and state locks before executor observations.
The page shares a one-second observation budget and reports unknown when it
expires; weak page references do not count as attached owners. Buffered events,
not only explicit follow-up flags, protect pending provider output.

Clients sharing the same built-in AgentRuntime now share identity claims:
acquiring an already-owned provider runtime ID returns RuntimeAlreadyExists.
Use the owning client for attach/resume, or dispose successfully before moving
that identity to another client. Failed/cancelled cleanup retains the claim.
Custom executors using independent provider namespaces keep their existing
behavior; shared namespaces must fence identity ownership as well.

The page deadline also bounds entry-state lock acquisition. Entries whose state
cannot be read within that budget are omitted; total remains the registry count
at page selection, so a partial page is not evidence of fewer retained slots.
Local transport termination retains the blocking process-tree cleanup task and
awaits that same acknowledgement after cancellation; failed joins remain failed
rather than becoming a successful empty-guard retry.
