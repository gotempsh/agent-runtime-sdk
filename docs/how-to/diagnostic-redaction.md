# Diagnostic environment classification

Diagnostic collection excludes `CODEX_HOME` and `CLAUDE_CONFIG_DIR` (provider
configuration paths), `NO_PROXY` and `no_proxy` (bypass lists),
`NODE_USE_ENV_PROXY` (Node flag), and `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_0`
(Git configuration metadata). `GIT_CONFIG_VALUE_0` is excluded only for literal
`*`, the safe-directory wildcard. Unknown names remain secret, with no minimum
value length. HTTP(S)/ALL proxy URLs remain secret because they can carry userinfo.

Use `SecretString::credential(value)` for explicit credential mappings, including
mappings to otherwise exempt names. This always participates in known-value
redaction. `SecretString::new(value)` uses the name-based defaults above.

Applications with their own non-secret configuration names can use
`SecretString::non_secret_configuration(value)` in the environment map. This is an
explicit trust decision per value, not a prefix allowlist. Do not use it for
credentials. Proxy URL names and non-wildcard Git values remain protected even
with this marker. Debug remains redacted and heuristic credential patterns still
apply. Retained launch collection carries this metadata only when the launched
name and value match; changed or additional launch values use safe defaults.
Explicit credentials from the request remain protected even if launch overrides them.

The exact context `Missing bearer authentication in header` is treated as prose.
Other Bearer expressions remain redacted; known secret values inside that phrase
still win. Encoding, truncation and existing credential protections are unchanged.

Temps Code integration requires marking its own non-secret launch configuration
with `non_secret_configuration`, and explicit credential mappings with `credential`.
No Temps Code variable names are built into the SDK. This SDK change does not
update a downstream dependency pin or caller.

Remote clients carry the per-value classification in an optional
`diagnostic_secret` field on each protocol environment entry. The host restores
it for both runtime acquisition and turn input. Older frames without it keep the
name-based defaults; older hosts still redact all values, so preserving caller
configuration remotely requires upgrading both ends. Debug output remains
redacted throughout transport.
