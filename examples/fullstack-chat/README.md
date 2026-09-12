# Full-stack runtime console

This example combines an Axum server with a React, Tailwind CSS, and
shadcn-style frontend. It demonstrates the application boundary around
`temps-agent-runtime`:

- normalized events stream over server-sent events;
- the right-hand normalized activity rail follows new events live, exposes a
  jump-to-latest control after manual scrolling, and shows relative timestamps
  with the exact event time on hover;
- the transcript uses AI Elements conversation, message, reasoning, and tool
  primitives while retaining the runtime SDK's normalized event model;
- tool lifecycle events are folded into one expandable card that preserves
  provider arguments, streams a shimmer and elapsed timer while executing, and
  displays the bounded result or typed error when it finishes;
- Claude Code's native Task/Agent lifecycle is replayed as an expandable
  subagent disclosure per user turn. Each subagent retains its status, progress,
  usage, and `task_id`-owned tools, while the activity rail tails the same
  normalized events;
- complete chats, messages, status, and event history survive browser and server restarts in SQLite;
- approval requests pause the SDK process until the UI allows or denies them;
- cancellation uses the request's `CancellationToken`;
- turns use the installed Claude Code, Codex, or OpenCode CLI;
- the console is the only application page and renders immediately from local
  state; every new chat starts in a centered setup surface that requires an
  execution target, a verified folder, and a ready harness before the first
  message can be sent;
- Local is always available as the default target. The target selector can
  reuse a saved SSH profile, and **Add target** opens the SSH or Temps sandbox
  form inline instead of interrupting the flow with a modal. The chosen target
  is immutable after the chat's first message;
- SSH onboarding can save reusable connection profiles; password credentials
  are encrypted at rest and are referenced by ID instead of being returned to
  the browser;
- a new chat chooses Local, a saved SSH connection, or the configured sandbox
  in the composer; that target identity is persisted and enforced as immutable
  after the first message;
- every new chat explicitly chooses a working directory. Folder autocomplete
  runs through the selected Local, SSH, or sandbox transport, offers the
  current directory first and then its children, and saves the selection with
  that chat—not with the target;
- the composer uploads files through the selected local, SSH, or sandbox
  transport into the chat working directory, then persists editable attachment
  references with sent and queued messages;
- the composer renders provider-native permissions, sandbox/mode controls, and
  models returned by that target instead of shipping a hard-coded catalog;
- Claude model labels include the concrete generation reported in Claude
  Code's native description, such as `Sonnet 5`, rather than an unversioned alias;
- model, reasoning, service-tier, and provider-control selections persist with
  each SQLite-backed chat.
- each execution target owns its own indexed chat list, while the selected
  chat's harness session title and working folder remain visible in the header;
- assistant messages render sanitized GitHub-flavored Markdown while user
  messages remain literal text;
- the selected harness's bounded skill inventory is silently preloaded for its
  execution host and chat folder;
- typing `/` filters those preloaded, applicable skills with keyboard and
  pointer autocomplete without duplicating the inspector's source of truth;
- the prompt supports file uploads, editable attachment references, native
  provider controls, and <kbd>Command</kbd>/<kbd>Control</kbd>+<kbd>Enter</kbd>
  submission while retaining the durable message queue behavior for running
  turns.

The server uses a trait-based `ChatStore` backed by SQLite. Chats own ordered
messages, events, provider sessions, and invocation-scoped approvals. History
is bounded, and a server restart preserves the transcript while marking an
interrupted response as failed with recovery context. Set
`AGENT_RUNTIME_EXAMPLE_DB` to override the default
`.data/runtime.sqlite3` path.

## What is wired together

- `src/main.rs` owns the Axum API, SDK runtime, SSE replay, approval endpoints,
  and provider execution boundary.
- `src/store.rs` implements the application-owned `ChatStore` trait and its
  SQLite projection.
- `web/src/App.tsx` owns the responsive operator console and reconnects from the
  last persisted event sequence.
- `web/e2e/runtime.spec.ts` covers approval, streaming, reload recovery, typed
  failure, Claude-native subagent replay, cancellation during approval,
  multi-message reload, and the mobile chats drawer.
- `.github/workflows/ci.yml` runs the browser suite.

## Run it

Install and build the frontend once:

```bash
cd examples/fullstack-chat/web
bun install
bun run build
```

Start the Rust server from the repository root:

```bash
cargo run --manifest-path examples/fullstack-chat/Cargo.toml
```

Open <http://127.0.0.1:7788>. The persistent console appears immediately. The
centered new-chat surface starts with Local and asks you to choose a folder; use
its target selector to reuse another target or open the inline **Add target**
form. The sidebar is scoped to that target; the active chat title comes from the
harness session when the provider reports one, with its fixed folder beside it.
The server validates folder suggestions through the selected transport, so it
never substitutes the server's local repository path for an SSH or sandbox
chat. Opening a saved chat restores its folder. Starting a new chat deliberately
leaves the folder empty until the user chooses one. A chat's folder and target
are fixed when its first message is created; the API rejects later attempts to
change either boundary.

The app preloads a bounded skill inventory after the chat's harness, execution
target, and folder are known. Type `/` to filter those applicable skills. The
preload does not walk project trees or session histories and does not add a
separate inspector to the prompt surface.

Use **Upload files** in the composer to attach one or more files. Each file is
limited to 10 MiB and is written with owner-only permissions below
`.temps-agent-runtime/uploads/` in that chat's working directory. The upload is
performed through the configured execution transport, so an SSH attachment is
stored on the SSH destination rather than on the web server. Queued messages
retain their attachments and allow them to be uploaded, edited, or removed
before delivery.

### Saved SSH connections

In the SSH tab, enter a connection name and choose **Save connection**. Saved
profiles restore the host, user, port, authentication method, identity-file or
known-hosts paths, and host-key policy. They can be selected or removed from the
same screen, and saved profiles also appear in the new-chat connection picker.
Passwords are encrypted with AES-256-GCM before being written to
SQLite; list responses expose only `has_password`, and later requests send a
profile ID so decrypted credentials remain on the server.

By default, the server creates a 32-byte owner-only key next to the SQLite
database using the `.ssh-key` extension. Back up that key with the database: a
lost key makes saved passwords unrecoverable. Set
`AGENT_RUNTIME_EXAMPLE_SSH_KEY` to choose another server-side key path.

For frontend development, run `bun run dev` in `web/`; Vite proxies `/api` to
the Rust server.

## Run the browser test

```bash
cd examples/fullstack-chat/web
bun run e2e
```

Playwright builds the frontend, starts the Axum server, verifies persistence
and reconnection, and covers cancellation and responsive navigation.

## Use a real provider

Select Claude Code, Codex, or OpenCode during onboarding. The provider
executable and its credentials must be available inside the chosen execution
target. The app does not copy provider credentials. It offers only the controls
and models advertised by the selected harness; dangerous values remain explicit
and are never selected as defaults.
