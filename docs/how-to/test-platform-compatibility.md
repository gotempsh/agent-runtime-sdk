# Test Linux and Windows runtime compatibility

Run native process tests on the execution host, not only on the desktop client.
The runtime launches the provider CLI on that host. A Windows Fleet client
connected to a Linux execution target does not exercise native Windows agents.

## Credential-free launch tests

Install stable Rust and run:

```sh
cargo test --locked --lib process::tests -- --nocapture
cargo test --locked --all-features
```

The process tests compile a small native executable in a directory containing
spaces. They verify JSON and Unicode arguments, stdin, explicit environment
values, nonzero exit status, and cancellation. Windows additionally launches
`claude.cmd`, `codex.cmd`, and `opencode.cmd` fixture wrappers. These are launcher
fixtures, not actual authenticated provider turns. No provider installation or
model credentials are needed. The existing CI matrix runs these tests on Linux,
macOS, and Windows.

The SDK preserves an explicit environment allowlist. On Windows this includes
system paths and profile/configuration directories, using case-insensitive key
matching. Unrelated API keys are not inherited. Explicit command environment
values still override inherited values. Windows child launches and taskkill
cleanup suppress console windows. Rust owns native command argument escaping;
callers must supply separate program and argument values.

## Real provider acceptance

Before claiming full platform support, run each installed provider through the
SDK on Linux and Windows with the user's own login. Verify tool start/completion,
approvals, cancellation with descendant processes, session resume, missing or
expired authentication, and long-running development servers. Exercise both
native executable and npm shim installations on Windows. Run equivalent tests
through Fleet after its adapter correctly maps the SDK's launch capabilities.
A successful cross-compilation or fixture run does not certify these journeys.

## Current limits

- SSH password authentication from a Windows execution host is explicitly
  unsupported; the askpass helper currently requires Unix.
- Process-tree cleanup uses Unix process groups or Windows `taskkill /T /F`.
  This is lifecycle management, not a security sandbox.
- Sandbox backend support must be checked independently; never bypass a requested
  sandbox to make a platform test pass.
- Windows OS-service installation is an application responsibility, outside
  this library's provider launch contract.

Real CLI checks now also cover extensionless npm command names on Windows,
resolved against the child PATH before spawning. OpenCode serve readiness uses
`/global/health` and requires a healthy JSON response; `/app` is a web UI route
in current versions and is not an API readiness check. Each probe is bounded.
