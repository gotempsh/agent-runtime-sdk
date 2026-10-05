//! Built-in coding-agent CLI adapters.

#[cfg(feature = "claude")]
mod claude;
#[cfg(feature = "codex")]
mod codex;
#[cfg(feature = "codex")]
mod codex_app_server;
#[cfg(feature = "opencode")]
mod opencode;
#[cfg(feature = "opencode")]
mod opencode_http;
#[cfg(feature = "opencode")]
mod opencode_serve;
#[cfg(feature = "pi")]
mod pi;

#[cfg(feature = "claude")]
pub use claude::Claude;
#[cfg(feature = "codex")]
pub use codex::{Codex, CodexTurnMode};
#[cfg(feature = "opencode")]
pub use opencode::{OpenCode, OpenCodeTurnMode};
#[cfg(feature = "pi")]
pub use pi::Pi;

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

/// Whether a turn prompt is a manual compaction request (`/compact` plus
/// optional instructions), the form `CompactionInput` produces.
pub(crate) fn is_manual_compaction_prompt(prompt: &str) -> bool {
    let prompt = prompt.trim_start();
    prompt
        .strip_prefix("/compact")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
}

/// Refuse a manual compaction request carrying summary instructions on an
/// adapter whose native compaction cannot take them, instead of dropping them.
#[cfg(any(feature = "codex", feature = "opencode"))]
pub(crate) fn refuse_compaction_instructions(
    provider: crate::Provider,
    prompt: &str,
) -> crate::Result<()> {
    let has_instructions = prompt
        .trim_start()
        .strip_prefix("/compact")
        .is_some_and(|rest| !rest.trim().is_empty());
    if is_manual_compaction_prompt(prompt) && has_instructions {
        return Err(crate::RuntimeError::InvalidRequest {
            field: "prompt",
            message: format!(
                "{provider} compacts natively without summary instructions; omit them"
            ),
        });
    }
    Ok(())
}

#[cfg(all(test, any(feature = "codex", feature = "opencode")))]
mod compaction_prompt_tests {
    use super::*;

    #[test]
    fn only_a_bare_compact_is_accepted_without_instruction_support() {
        assert!(is_manual_compaction_prompt("/compact"));
        assert!(is_manual_compaction_prompt("  /compact keep the API notes"));
        assert!(!is_manual_compaction_prompt("/compaction please"));
        assert!(refuse_compaction_instructions(crate::Provider::Codex, "/compact").is_ok());
        assert!(refuse_compaction_instructions(crate::Provider::Codex, "/compact   ").is_ok());
        assert!(refuse_compaction_instructions(crate::Provider::Codex, "hello").is_ok());
        let error =
            refuse_compaction_instructions(crate::Provider::OpenCode, "/compact keep notes")
                .unwrap_err()
                .to_string();
        assert!(error.contains("without summary instructions"), "{error}");
    }
}
