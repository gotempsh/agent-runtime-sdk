# Resume large Codex conversations

The app-server adapter requests `excludeTurns: true` when resuming a thread or
forking one after an active-writer conflict. This returns the metadata needed to
start the next turn without replaying the historical turns in one JSON frame.
Codex still uses the saved conversation context; this does not clear or truncate
history. New threads use the normal start request.

This prevents growing history replies from hitting the default 2 MiB event-line
limit. The limit remains in place for other protocol events. Clients displaying
history should fetch it separately using the provider's paginated history APIs.
