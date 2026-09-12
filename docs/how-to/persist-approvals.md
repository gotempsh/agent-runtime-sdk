# Persist approvals and resume a waiting turn

Use a durable `InteractionHandler` when an approval must survive browser
disconnects, navigation, or a slow human decision. The runtime keeps the
provider process waiting while your application stores, presents, and resolves
the request.

Live approval frames are currently supported by the Claude adapter. Codex and
OpenCode expose static permission modes through their current CLI adapters;
check `AgentRuntime::permission_support` before presenting a live approval UI.

## Store an approval state machine

Keep approval state separate from the provider process:

```rust,no_run
use async_trait::async_trait;
use temps_agent_runtime::{ApprovalDecision, ApprovalRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApprovalStatus {
    Pending,
    Allowed,
    Denied,
    Expired,
    Interrupted,
}

#[async_trait]
trait ApprovalStore: Send + Sync {
    async fn open_if_absent(
        &self,
        chat_id: &str,
        invocation_id: &str,
        request: &ApprovalRequest,
        expires_at_unix_ms: u64,
    ) -> Result<(), StoreError>;

    async fn wait_for_decision(
        &self,
        chat_id: &str,
        invocation_id: &str,
        approval_id: &str,
    ) -> Result<ApprovalDecision, StoreError>;
}

# struct StoreError;
```

Use `(chat_id, invocation_id, approval_id)` as a unique key. Provider-native
approval IDs can repeat in later responses in the same chat. A decision update
must compare and swap `Pending` to one terminal status so two operators cannot
decide the same request differently.

Persist only the provider input that your product policy permits. Tool input
can contain source code, paths, commands, or secrets. Store a redacted display
projection separately from encrypted or access-controlled raw input.

## Connect storage to `InteractionHandler`

Construct one handler per internal provider invocation so it carries the chat
and invocation identifiers that are not part of `ApprovalRequest`:

```rust,no_run
use std::sync::Arc;
use async_trait::async_trait;
use temps_agent_runtime::{
    ApprovalDecision, ApprovalRequest, InteractionHandler, QuestionAnswer,
    QuestionRequest,
};

// ApprovalStore is the application trait from the previous section.
struct DurableInteractions {
    chat_id: String,
    invocation_id: String,
    expires_at_unix_ms: u64,
    approvals: Arc<dyn ApprovalStore>,
}

#[async_trait]
impl InteractionHandler for DurableInteractions {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        if self.approvals
            .open_if_absent(
                &self.chat_id,
                &self.invocation_id,
                &request,
                self.expires_at_unix_ms,
            )
            .await
            .is_err()
        {
            return ApprovalDecision::Deny {
                reason: Some("Approval storage is unavailable".into()),
            };
        }

        self.approvals
            .wait_for_decision(&self.chat_id, &self.invocation_id, &request.id)
            .await
            .unwrap_or_else(|_| ApprovalDecision::Deny {
                reason: Some("Approval could not be resolved safely".into()),
            })
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}
```

The runtime applies `TurnRequest::interaction_timeout` around the handler. A
timeout denies the operation. Give the durable record the same expiration and
reject decisions submitted after it expires.

## Use the SDK broker for live delivery

An HTTP service does not need to build its own oneshot registry. Keep durable
state in the application, then use `InteractionBroker` for the live handoff:

```rust,no_run
use temps_agent_runtime::{
    ApprovalDecision, InteractionBroker, QuestionAnswer,
};

let interactions = InteractionBroker::default();

// Pass `&interactions` to AgentRuntime::run. In the authorized HTTP handler,
// persist the decision first and then deliver it to the waiting provider.
interactions.resolve_approval("approval-id", ApprovalDecision::Allow)?;
interactions.resolve_question(
    "question-id",
    QuestionAnswer::selected("Which fruit do you prefer?", "Banana"),
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The broker is bounded, supports an answer racing slightly ahead of waiter
registration, rejects duplicate resolutions, and removes a waiter when the
runtime times out or is cancelled. A process restart still invalidates the
live provider stdin; recover durable records as described below instead of
delivering them into a new invocation.

## Order persistence and delivery

For one approval, the normal sequence is:

1. `ApprovalRequested` reaches the backpressured `EventSink`.
2. The sink appends the event and projects the chat to `approval_needed`.
3. `InteractionHandler::approve` opens the idempotent approval record and
   waits for its decision.
4. The UI authorizes the operator and commits one decision.
5. The handler returns that already-persisted decision to the provider.
6. The worker projects the chat back to `running` when execution continues.

Persist before publishing to browser subscribers. On reconnect, replay the
stored event and current approval record before following live updates.

## Recover after a host restart

A stored pending approval does not mean the original provider stdin is still
alive. When the worker lease is lost, mark the approval `Interrupted` and the
chat `failed` or `interrupted`. Do not return a late decision to a new process as though it
were the original interaction.

Resume only when the provider supplied a session ID and your application can
start a new provider invocation without duplicating side effects. Link the replacement
approval to the original record for auditability.

## Enforce authorization at decision time

- Recheck workspace, project, and operator authorization when the decision is
  written, not only when the approval page is opened.
- Make deny the default for storage errors, expired requests, unknown tools,
  and missing handlers.
- Keep an immutable audit record containing who decided, when, the redacted
  operation, and the final decision.
- Never infer approval from a sandbox profile update. Tool approval and sandbox
  recovery are separate authorization boundaries.

See [Persist conversations and streams](persist-conversations.md) for replay
ordering and [Event and status catalog](../reference/events.md) for the
normalized interaction events.
