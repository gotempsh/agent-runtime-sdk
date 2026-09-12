# Discover and manage harness extensions

Use the extension inventory when a user explicitly asks to inspect skills or MCP servers on a
selected execution host. Do not run this as part of page mount or ordinary harness readiness: it
inspects provider-owned filesystem locations and may invoke a provider metadata command.

```rust,no_run
use std::path::PathBuf;
use temps_agent_runtime::{
    AgentRuntime, HarnessExtensionQuery, Provider,
};

# async fn inspect(runtime: &AgentRuntime) -> Result<(), Box<dyn std::error::Error>> {
let inventory = runtime
    .discover_harness_extensions(HarnessExtensionQuery {
        provider: Provider::Claude,
        working_directory: PathBuf::from("/workspace/app"),
        skill_query: Some("review".into()),
        limit: 100,
    })
    .await?;

for skill in inventory.skills {
    println!("{} ({:?})", skill.id, skill.scope);
}
# Ok(())
# }
```

Discovery runs inside the configured `ExecutionTransport`. An SSH runtime therefore searches the
SSH account's home and project directories and invokes the provider installed on that SSH host; it
does not inspect the Rust server's filesystem. The operation is bounded to 200 returned skills, 32
project ancestors, 1 MiB of metadata, and a five-second deadline per parallel skills/MCP probe.

The inventory never returns MCP commands, arguments, environment values, headers, or tokens. It
contains only server name, enabled state, transport kind, and a sanitized status.

## Check management access

Read the four `manage_*` fields before rendering an install, update, or remove action. Each is a
`HarnessExtensionAccess` with `allowed`, a stable `denial_kind`, and a user-facing `reason`.
Applications should show the reason instead of hiding unavailable controls.
For supported operations, the probe checks write access to the existing configuration/skill path
or the nearest existing parent on the selected execution host. A positive result is still
advisory: permissions may change before a later mutation, whose transport error remains
authoritative.

- Claude skills: user and project scopes; Claude CLI MCP user and project scopes.
- Codex skills: portable `.agents/skills` user and project scopes; Codex CLI MCP user scope only.
- OpenCode skills: user and project scopes. MCP config is discoverable, but SDK mutation is denied
  because safely preserving arbitrary JSON/JSONC comments and secrets is not implemented.
- A custom transport may return `transport_unsupported` if it cannot run the bounded host probes.

## Install or remove a skill

`manage_skill` writes an atomic `SKILL.md` below the provider's canonical user or project directory.
Names are restricted to portable lowercase kebab-case and content is limited to 1 MiB. The
execution-host mutation rejects symlink components in the resolved skill path before and after
directory creation, and uses an owner-only `mktemp` file for atomic replacement. This prevents a
repository-controlled `.claude`, `.agents`, or `.opencode` link from redirecting an authorized
project-scope write into another same-user location.

```rust,no_run
use std::path::PathBuf;
use temps_agent_runtime::{
    AgentRuntime, HarnessExtensionScope, Provider, SkillManagementRequest,
};

# async fn install(runtime: &AgentRuntime) -> Result<(), Box<dyn std::error::Error>> {
runtime
    .manage_skill(SkillManagementRequest {
        provider: Provider::Claude,
        scope: HarnessExtensionScope::Project,
        name: "review-pr".into(),
        working_directory: PathBuf::from("/workspace/app"),
        content: Some("---\ndescription: Review a pull request\n---\n\nReview the diff.\n".into()),
    })
    .await?;
# Ok(())
# }
```

Set `content` to `None` to remove that exact managed skill directory. The runtime never accepts a
caller-provided destination path.

## Add or remove an MCP server

Use `manage_mcp_server` only when the matching inventory access field is allowed. The SDK delegates
to the provider CLI so provider-owned config semantics and credential handling remain authoritative.
Set `definition` to `None` to remove the named server.

Secret-bearing definitions are request-only. Do not log them or persist them without application-
level encryption.

Claude/Codex stdio MCP management rejects nonempty environment values with a
typed `Unsupported` error: their CLI configuration commands would put those
values into process arguments. Use secret-reference launch-context injection
for per-turn MCP credentials instead. The SDK does not silently drop values or
write a weaker configuration. This restriction does not disable MCP removal,
secret-free stdio definitions, or supported HTTP bearer-environment references.
