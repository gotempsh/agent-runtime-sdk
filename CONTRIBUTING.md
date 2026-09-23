# Contributing

Thanks for helping improve `temps-agent-runtime`.

## Development setup

Install stable Rust (minimum supported version: 1.88). Provider CLIs are not required for unit tests. Nono is
needed only for the ignored live compatibility test.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo check --no-default-features
python3 scripts/check-package.py
cargo package --locked
```

Package verification builds the distributable archive; it does not publish it.
For a work-in-progress checkout, use `cargo package --locked --allow-dirty`.
The full-stack example and documentation portal are repository-only applications,
not part of the Rust crate archive.

## Changes

- Open an issue before a large protocol or public-API change.
- Add a regression fixture or test for every provider behavior change.
- Keep executable and argument values separated; do not build shell commands.
- Bound new queues, reads, subprocess output, and fan-out.
- Never log prompts, credential values, or raw sensitive tool payloads.
- Treat sandbox errors as terminal; do not add an unsandboxed fallback.
- Update the capability matrix and migration notes when behavior changes.
- Use Conventional Commits for commit messages.

Public changes should remain compatible within a minor release. Additive enums
must stay non-exhaustive. Update `CHANGELOG.md` for user-visible changes.

## Pull requests

Keep pull requests focused and describe:

- the user-visible outcome;
- protocol/CLI versions tested;
- security and compatibility impact;
- automated and manual verification;
- documentation changes.

By contributing, you agree that your contributions are licensed under the
project's MIT OR Apache-2.0 terms.

## Platform compatibility

See [Linux and Windows runtime testing](docs/how-to/test-platform-compatibility.md)
for native launch fixtures and the separate authenticated-provider acceptance checks.
