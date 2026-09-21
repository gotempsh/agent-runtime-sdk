//! Built-in coding-agent CLI adapters.

#[cfg(feature = "claude")]
mod claude;
#[cfg(feature = "codex")]
mod codex;
#[cfg(feature = "codex")]
mod codex_app_server;
#[cfg(feature = "opencode")]
mod opencode;

#[cfg(feature = "claude")]
pub use claude::Claude;
#[cfg(feature = "codex")]
pub use codex::{Codex, CodexTurnMode};
#[cfg(feature = "opencode")]
pub use opencode::OpenCode;

#[cfg(any(feature = "claude", feature = "codex"))]
use serde_json::Value;

#[cfg(any(feature = "claude", feature = "codex"))]
use crate::Usage;

#[cfg(any(feature = "claude", feature = "codex"))]
fn usage_from(value: &Value) -> Usage {
    let usage = value
        .get("usage")
        .or_else(|| value.pointer("/message/usage"))
        .unwrap_or(value);
    Usage {
        input_tokens: usage
            .get("input_tokens")
            .or_else(|| usage.get("prompt_tokens"))
            .and_then(Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .or_else(|| usage.get("completion_tokens"))
            .and_then(Value::as_u64),
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64),
        cache_read_input_tokens: usage.get("cache_read_input_tokens").and_then(Value::as_u64),
        context_window: None,
        cost_usd: usage
            .get("cost_usd")
            .or_else(|| value.get("total_cost_usd"))
            .and_then(Value::as_f64),
    }
}

#[cfg(any(feature = "claude", feature = "codex"))]
fn merge_usage(target: &mut Usage, incoming: &Usage) {
    target.input_tokens = incoming.input_tokens.or(target.input_tokens);
    target.output_tokens = incoming.output_tokens.or(target.output_tokens);
    target.cache_creation_input_tokens = incoming
        .cache_creation_input_tokens
        .or(target.cache_creation_input_tokens);
    target.cache_read_input_tokens = incoming
        .cache_read_input_tokens
        .or(target.cache_read_input_tokens);
    target.context_window = incoming
        .context_window
        .clone()
        .or_else(|| target.context_window.clone());
    target.cost_usd = incoming.cost_usd.or(target.cost_usd);
}
