# Steer a retained turn without replaying input

Use `TurnMessageHandle::send_identified(id, text)` to correlate a followup with
Claude's native command lifecycle. Keep a durable, unique identity for each
intentional send; two identical texts can be two different messages.

Successful delivery means the runtime wrote the input. It does **not** mean
the provider read it. Observe `TurnEvent::InputMessageState`: `queued` means
waiting, `started` confirms consumption, and `completed` ends that command.
`cancelled` and `discarded` are terminal without proving consumption. Stale
queued receipts cannot move started or terminal input backwards.

The SDK correlates identities; applications still own durable idempotency,
ordering, receipt persistence, and recovery across process replacement. Never
replay `PossiblySent` input automatically. Keep an unresolved historical receipt
attached to its original invocation rather than making a new turn look pending.
An ended invocation's message handle cannot control its replacement.

## Release a foreground step

For an already delivered, still-pending identified message, explicitly call
`read_now(id, true)` to ask Claude to background eligible foreground tasks.
The control contains no user text. It does not resend the message. Unsupported
providers return a capability error; Claude may refuse or report that there was
nothing eligible to background.

`read_now(id, false)` explicitly interrupts foreground work while preserving
queued input (`cancel_queued: false`). This can stop actual foreground work,
including a foreground build. Explain that effect before offering it. There is
no automatic fallback from backgrounding to interruption, and this API does not
infer that an arbitrary shell command is a safe monitoring wrapper.

Observe `InputControl` for the provider acknowledgement, correlated by input ID
and the background/stop action. An acknowledgement is not consumption: wait for
the original command's `started` or `completed` lifecycle. A lost response remains
ambiguous; refresh authoritative status instead of repeating input or silently
escalating to interruption. Ordinary turn cancellation retains its existing
cancel-queued behavior.

## Process and test boundaries

These controls do not promise process survival across turn cancellation,
runtime disposal, or application restart. Use a separately owned service for
work that must outlive its provider process. A task finishing, including an
exit-zero readiness watcher, is not proof that a service became ready.

The retained-process regression fixture uses a simulated Claude protocol with
real finite shell processes. It checks that releasing a polling wrapper allows
the original followup to start once while an independent sentinel survives.
It does not certify any installed Claude CLI or running user's session.
