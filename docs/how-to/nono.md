# Manage Nono sandboxes

The `nono` feature lets a host generate validated Nono profiles and apply one to
an individual turn. Nono remains an external OS-level supervisor; the crate
does not apply irreversible sandboxing to the embedding server process.

## Choose `run` or `wrap`

`NonoMode::Run` is the recommended mode for coding agents. It retains a
supervisor and supports filesystem controls, total network blocking,
destination allowlists, named credential proxying, and audit data.

`NonoMode::Wrap` replaces itself with the child. It supports filesystem rules
and total network blocking, but not destination allowlists, named credential
proxying, or supervisor audit data. Check `NonoExecution::capabilities()` if the
mode is selected dynamically.

## Generate and validate a profile

```rust,no_run
use temps_agent_runtime::nono::{
    ManagedProfile, NetworkPolicy, NonoManager, WorkdirAccess,
};

# async fn build_profile() -> Result<(), Box<dyn std::error::Error>> {
let manager = NonoManager::discover(".agent-runtime/nono")?;
let mut profile = ManagedProfile::new("temps-read-only", "claude-code");
profile.workdir = WorkdirAccess::Read;
profile.readable_paths = vec!["$HOME/.gitconfig".into()];
profile.denied_environment = vec!["AWS_*".into(), "GITHUB_TOKEN".into()];
profile.network = NetworkPolicy::AllowDomains {
    domains: vec!["api.anthropic.com".into()],
};

let path = manager.save(profile).await?;
println!("validated profile: {}", path.display());
# Ok(())
# }
```

`save` performs these steps:

1. validates and normalizes bounded input;
2. serializes the current Nono profile schema;
3. writes a `0600` temporary artifact inside a `0700` profile directory on
   Unix;
4. runs `nono profile validate PATH --json --strict` with a deadline;
5. atomically renames a content-addressed artifact into place.

If validation fails, no profile is activated. Re-saving identical content is
idempotent.

## Apply the profile to a turn

```rust,no_run
use temps_agent_runtime::nono::{NonoExecution, PathAccess, PathGrant};
use temps_agent_runtime::{
    Provider, SandboxCapabilities, SandboxRequest, TurnRequest,
};

# fn request(profile_path: &std::path::Path) -> Result<TurnRequest, Box<dyn std::error::Error>> {
let mut sandbox = NonoExecution::discover(profile_path.to_string_lossy())?;
sandbox.grants.push(PathGrant {
    path: std::env::current_dir()?.join("artifacts"),
    access: PathAccess::ReadWrite,
    bypass_protection: false,
});

let mut request = TurnRequest::new(
    Provider::Claude,
    std::env::current_dir()?,
    "Run the tests and report failures.",
);
request.sandbox = Some(
    SandboxRequest::new(sandbox).requiring(SandboxCapabilities {
        filesystem: true,
        process_isolation: true,
        ..SandboxCapabilities::NONE
    }),
);
# Ok(request)
# }
```

Arguments are passed as a separated argv; profile names, paths, and prompts are
never interpolated into a shell command.

## Use named credentials

Nono `run` can proxy a configured credential without putting its secret value
in process arguments:

```rust,no_run
# use temps_agent_runtime::nono::NonoExecution;
# fn configure(mut sandbox: NonoExecution) {
sandbox.credentials.push("github".into());
# }
```

The name must already be configured in Nono. `wrap` rejects named credentials
because it cannot enforce that capability. Secret values remain outside this
crate.

## Native TLS clients and proxy CA trust

Nono may intercept TLS when a network profile filters destinations or injects
credentials. Most clients honor the session CA bundle Nono provides. Some
native clients may require Nono's shared CA in the macOS user trust store:

```rust,no_run
# use temps_agent_runtime::nono::NonoExecution;
# fn configure(mut sandbox: NonoExecution) {
sandbox.trust_proxy_ca = true;
# }
```

This maps to `nono run --trust-proxy-ca`. It is deliberately false by default
because trusting a proxy CA is a persistent, security-sensitive change. The
library never enables it in response to a TLS error, and `NonoMode::Wrap`
rejects it. Obtain explicit user or administrator approval before enabling it.

Codex releases using a Rustls WebSocket transport have historically varied in
whether they honor session CA environment variables. Test the exact Codex and
Nono versions used in production. A TLS failure remains terminal; do not work
around it by disabling certificate verification or retrying unsandboxed.

## Avoid nested sandbox surprises

Provider permission modes and Nono are independent. Keeping
`PermissionMode::Default` gives defense in depth but may be more restrictive
than the Nono profile. Use `PermissionMode::FullAccess` only when Nono is the
intended sole execution boundary and the selected Nono capabilities satisfy the
product's policy.

The runtime never changes permission mode merely because a Nono profile is
present, and never retries without Nono after a sandbox error.

Nono implements the same `SandboxBackend` extension point available to custom
backends. Capability requirements are checked before Nono profile preparation
or provider execution.

## Update a managed profile after a denial

`NonoExecution` classifies failed tool events that contain a recognizable
filesystem denial and path. Use it behind `SandboxProfileManager` to enable the
runtime's opt-in recovery flow.

After the application approves a portable change, apply it to the stored policy
and materialize a new revision:

```rust,no_run
# use temps_agent_runtime::nono::{ManagedProfile, NonoManager};
# use temps_agent_runtime::SandboxProfileChange;
# async fn update(
#   manager: &NonoManager,
#   mut profile: ManagedProfile,
#   change: &SandboxProfileChange,
# ) -> Result<(), Box<dyn std::error::Error>> {
profile.apply_change(change)?;
let exact_path = manager.save(profile).await?;
# let _ = exact_path;
# Ok(())
# }
```

The helper supports path grants and additions to an existing network allowlist.
It rejects protection bypasses, named credential changes, and custom changes;
those require application-specific authorization and materialization. See
[Recover a denied sandbox step](recover-sandbox-denials.md) for the full trait
and event sequence.

## Compatibility testing

The repository includes a live, ignored test against the installed binary:

```bash
cargo test --test nono_live --all-features -- --ignored --nocapture
```

Run it when upgrading Nono or changing generated profile fields. Regular tests
use no external sandbox process.
