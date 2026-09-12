# Display provider account usage

Use account quota snapshots for the provider-level session and weekly meters.
Do not derive these meters from turn tokens: providers weight subscription
usage independently from token billing.

Inspect the authenticated local account from the repository with:

```sh
cargo run --example account_usage -- claude
cargo run --example account_usage -- codex
```

## Fetch usage when the user opens the surface

Call `AgentRuntime::fetch_account_usage` for the selected provider. The query
runs inside the configured local, SSH, or sandbox transport, so it observes the
account authenticated on that execution host. It is bounded by the runtime's
process semaphore, a 15-second deadline, and a 256 KiB response limit.

```rust
use temps_agent_runtime::{AccountUsageStatus, Provider};

let report = runtime.fetch_account_usage(Provider::Claude).await;
match report.status {
    AccountUsageStatus::Available => {
        if let Some(usage) = report.usage {
            replace_provider_usage(usage).await?;
        }
    }
    AccountUsageStatus::Unavailable => {
        show_unavailable(report.reason.as_deref(), report.retryable);
    }
    AccountUsageStatus::Unsupported => {
        hide_or_disable_provider_usage(report.reason.as_deref());
    }
    _ => {}
}
```

If the application supplies target-local credentials or project-specific
configuration, call `fetch_account_usage_with(provider, ProviderProbeContext)`.
Use the same context used for harness discovery so the picker, usage surface,
and eventual turn describe one provider identity. The convenience method above
continues to use `.` and ambient target credentials.

Fetch when the usage tooltip or account-usage page opens. Cache an available
report briefly in the host application, typically for 30–60 seconds, and
deduplicate concurrent refreshes. The SDK intentionally does not poll, cache,
or persist account usage because those policies belong to the application.

`Unavailable` means the provider supports the query but the account is not
authenticated, the endpoint could not be reached, the helper is missing, or
the response could not be understood. Display the reason; never substitute
zero. `Unsupported` means the adapter has no account-usage query.

`HarnessReadiness.account_usage` remains an opportunistic initial snapshot
from harness discovery. It can avoid a second request when present, but it is
not guaranteed to be fresh or available. Use `fetch_account_usage` for an
explicit refresh.

## Credential ownership

The fetcher reads the provider CLI's existing OAuth credential and never
modifies or refreshes it. Claude usage reads `~/.claude/.credentials.json`, or
the `Claude Code-credentials` macOS Keychain entry, then queries Anthropic's
OAuth usage endpoint. Codex asks the installed app server for
`account/rateLimits/read`; it does not read or transmit the Codex token itself.

Claude's transport-local helper requires a recent `node` executable on the
target. A missing helper returns `Unavailable` with a concrete reason and does
not affect agent turns. OAuth values stay in the target process and are never
written to stdout or placed in process arguments.

## Live updates

Handle `TurnEvent::AccountUsageUpdated` in the same durable projection as the
rest of the turn stream:

```rust
match event {
    TurnEvent::AccountUsageUpdated { usage } => {
        store.replace_account_usage(chat_id, usage).await?;
    }
    TurnEvent::Usage(usage) => {
        store.merge_turn_and_context_usage(chat_id, usage).await?;
    }
    _ => {}
}
```

Replace the previous snapshot atomically. Window percentages are already
normalized to percent for both providers. Preserve `resets_at_unix_seconds` as
an absolute timestamp and calculate relative copy such as “resets in 2h” in
the client so it remains current without another provider request.

## Window presentation

- `session` is the provider's short rolling window. Use `duration_minutes`
  when showing its length; Claude and Codex currently commonly report 300
  minutes.
- `weekly` is a seven-day allocation. More than one weekly window can exist,
  including Claude model-specific windows, so key rows by `id` rather than by
  kind.
- `other` is a provider-specific allocation that should use its normalized ID
  as fallback copy.
- Prefer `label` over `id` when a model- or surface-scoped window supplies one.
- `used_percent` can exceed 100. Show the value as reported, but clamp a visual
  progress track to the 0–100 range.
- `credits` is optional. A missing value is not a zero balance. Use `currency`
  when present instead of assuming that every provider balance is USD.

Keep `Usage.context_window` in a separate section. Context occupancy belongs to
one provider session and controls compaction; account usage belongs to the
authenticated provider account and controls when the provider may accept more
work.
