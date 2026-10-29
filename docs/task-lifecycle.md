# Claude task lifecycle receipts

Task IDs identify one execution. A replayed start or running update must not revive a terminal task. Repeated terminal notifications do not emit another completion event or wake the parent again; a later summary for the same terminal outcome can enrich the record.

Only explicit terminal statuses release background ownership. `completed` reports provider completion, `failed`/`timed_out`/`timeout` report failure, and `cancelled`/`interrupted`/`stopped`/`killed` report stopping. `ended` releases ownership without asserting success. Missing or unrecognized statuses remain updates and do not release the task.

Provider completion is not application readiness. A watcher that exits normally after its deadline has completed as a process; consumers must inspect its bounded summary/output before asserting that a server is ready. The SDK cannot infer readiness from exit code zero.

Permission changes on retained Claude processes are applied between turns. A control acknowledgement is not evidence that a queued message has been consumed; use the input lifecycle receipt for that immutable message ID.

Retained handoffs keep the most recent 128 retired task records separately from the 32-entry live task table. Late frames within that window can enrich metadata or usage without reacquiring background ownership. Older records are evicted in retirement order; this is a bounded replay window, not an unlimited archive. Usage is retained only for live and recently retired tasks. The host remains responsible for its durable history across runtime replacement.
