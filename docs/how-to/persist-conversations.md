# Persist chats and streams

Persist a chat, not a provider turn. A provider turn is one internal invocation
used to answer a message; the durable user-facing record is the chat containing
all messages, events, approvals, and the provider session used for continuation.

The crate exposes `ChatStore` as the database boundary. It deliberately does
not choose SQLite, Postgres, DynamoDB, or a client transport.

## Use the durable chat types

```rust,no_run
use temps_agent_runtime::{
    Chat, ChatMessage, ChatRole, ChatStatus, Provider,
};

let chat = Chat {
    id: "chat_01".into(),
    title: "Inspect the checkout".into(),
    provider: Provider::Claude,
    status: ChatStatus::Queued,
    session_id: None,
    model: None,
    revision: 0,
    created_at_unix_ms: 1_800_000_000_000,
    updated_at_unix_ms: 1_800_000_000_000,
    metadata: Default::default(),
};

let first_message = ChatMessage {
    sequence: 1,
    role: ChatRole::User,
    content: "Inspect this checkout.".into(),
    created_at_unix_ms: chat.created_at_unix_ms,
};
# let _ = (chat, first_message);
```

Store both rows atomically with `ChatStore::create_chat` before scheduling the
provider invocation. If scheduling fails, the user message still exists and
the chat can show an actionable failed state.

## Implement `ChatStore`

`ChatStore` has five responsibilities:

1. create a chat with its first message atomically;
2. load a bounded `StoredChat` transcript;
3. paginate chat summaries;
4. apply one optimistic, atomic `ChatCommit`;
5. replay events after a chat-scoped cursor.

`ChatCommit::expected_revision` prevents two workers from writing the same chat
concurrently. A successful commit should append its messages, events, and
approval updates in the same transaction that advances `Chat::revision` and
updates the materialized status.

A practical relational schema is:

```sql
CREATE TABLE chats (
  id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  status TEXT NOT NULL,
  session_id TEXT,
  snapshot_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE chat_messages (
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  sequence INTEGER NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (chat_id, sequence)
);

CREATE TABLE chat_events (
  chat_id TEXT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
  sequence INTEGER NOT NULL,
  invocation_id TEXT NOT NULL,
  occurred_at_ms INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  PRIMARY KEY (chat_id, sequence)
);

CREATE INDEX chats_updated_idx ON chats(updated_at_ms DESC, id);
CREATE INDEX chat_events_time_idx ON chat_events(chat_id, occurred_at_ms);
```

Use explicit retention limits for messages and events. Pagination and replay
limits are required even for local applications; old chats can contain millions
of streaming deltas.

## Persist one message response

For each user message:

1. append the user `ChatMessage` and move the chat to `queued`;
2. assign an internal `invocation_id`;
3. pass the chat's `session_id` into `TurnRequest::session_id`;
4. wrap every `TurnEvent` in a chat-scoped `ChatEvent` and commit it before
   publishing it to live subscribers;
5. store `TurnResult::session_id` back on the chat;
6. append one assistant `ChatMessage` and the terminal status atomically.

The `invocation_id` is not a second public conversation record. It only
disambiguates repeated provider-native IDs, approval requests, tool attempts,
and diagnostics inside the saved chat.

If the user switches provider or execution boundary, clear an incompatible
session ID instead of sending it to another harness.

## Replay and then follow

A reconnecting client sends the last chat event sequence it observed. The
server should:

1. subscribe to live chat events;
2. call `ChatStore::events_after(chat_id, cursor, limit)`;
3. send stored events in sequence order;
4. continue with live events, discarding duplicate sequence numbers.

Subscribing before the database read closes the replay/live gap. Persist before
publishing so a reconnect can never move the UI backwards.

## Model statuses and failures

`ChatStatus` covers `queued`, `running`, `approval_needed`, `input_needed`,
`succeeded`, `failed`, `cancelled`, and `timed_out`. The status describes the
latest response; it does not replace the transcript.

Store typed failures with `ChatEventData::ResponseFailed`. A later user message
can move the same chat back to `queued`. Do not create a replacement chat just
because one provider invocation failed.

On host restart, mark in-flight chats as failed or interrupted while preserving
their messages and session IDs. Resume only through a new message or an
explicitly idempotent retry policy; a database record cannot recreate a lost
provider process.

See [Persist approvals](persist-approvals.md) for the interaction state machine,
[Persist command execution](persist-command-execution.md) for durable tool
attempts, and [Operate sandboxed turns](operate-sandboxes.md) for sandbox profile
recovery.
