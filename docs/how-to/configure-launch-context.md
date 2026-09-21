# Configure system instructions, tools, and MCP servers

Use `LaunchContext` when the host application needs to add standing instructions,
restrict available tools, or expose application-owned capabilities through MCP.
The SDK does not define higher-level concepts such as personas. Resolve those in
the host application and pass only their provider-neutral execution policy.

Support is provider- and field-specific. Claude Code implements every current
field. Codex implements additive turn-scoped stdio and HTTP MCP servers, including
header and environment values referenced from the turn environment. Inspect
`AgentRuntime::launch_context_capabilities` or the `launch_context` field in
`HarnessReadiness` before presenting controls. An unsupported field is rejected
precisely instead of being silently ignored.

## Configure one retained turn

Set the context on `TurnInput` when values can vary between invocations. It
replaces the default context from `RuntimeSpec` for that turn.

```rust,no_run
use std::collections::BTreeMap;
use temps_agent_runtime::{LaunchContext, McpServerConfig, SecretString};
use temps_agent_runtime::lifecycle::InvocationId;
use temps_agent_runtime::retained::TurnInput;

let mut input = TurnInput::new(
    InvocationId::new("message-018f")?,
    "Review the current change.",
);
input.launch_context = Some(LaunchContext {
    system_prompt_append: Some(
        "Review security-sensitive changes before approving them.".into(),
    ),
    allowed_tools: Some(vec![
        "Read".into(),
        "Grep".into(),
        "mcp__temps_fleet__create_agent".into(),
    ]),
    mcp_servers: BTreeMap::from([(
        "temps_fleet".into(),
        McpServerConfig::Stdio {
            command: "/usr/local/bin/temps-fleet".into(),
            args: vec!["mcp-server".into()],
            environment_from: BTreeMap::from([(
                "TEMPS_FLEET_MCP_PARENT_TOKEN".into(),
                "FLEET_TURN_CAPABILITY".into(),
            )]),
        },
    )]),
    strict_mcp_config: false,
});
input.environment.insert(
    "FLEET_TURN_CAPABILITY".into(),
    SecretString::new("short-lived-secret"),
);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Set `RuntimeSpec::launch_context` instead when every invocation of that runtime
uses the same values. A turn with `launch_context: None` inherits that default.

## Choose tool semantics explicitly

- `allowed_tools: None` keeps the harness default.
- `allowed_tools: Some(vec![])` disables all tools.
- A non-empty list becomes Claude Code's exact `--tools` selection.

Tool availability is separate from permission mode. The launch context controls
which tools exist; `PermissionMode` and native harness controls determine whether
their use requires approval.

Include an MCP tool using its provider-visible name, such as
`mcp__temps_fleet__create_agent`, when an exact tool list is active.

## Keep MCP credentials out of configuration

`environment_from` and `headers_from` map an MCP child environment variable or
HTTP header to a source variable in the turn's redacted `environment` map. They
never contain the credential itself. The SDK rejects references to source
variables that were not explicitly supplied.

This is serialization safety, not isolation from the harness. Claude Code must
receive each source variable so it can expand the MCP configuration; Claude and
processes it starts can therefore read the value. Supply only narrowly scoped,
short-lived capabilities. If the harness must not possess the underlying
credential, put authorization behind an application-owned proxy or credential
broker and give the turn a revocable capability for that broker instead.

Do not put credentials in MCP command arguments or endpoint URLs. Arguments and
URLs are ordinary launch configuration and may be visible to host process
inspection.

Set `strict_mcp_config: true` to pass Claude Code `--strict-mcp-config`. Claude
then ignores MCP servers discovered from its user and project configuration and
uses only the servers supplied by the host application. Leave it false for an
additive application capability that should coexist with user-configured MCP
servers.

Codex accepts stdio and HTTP MCP definitions when `strict_mcp_config` is false.
The SDK passes commands, arguments, endpoints, and environment-variable names
through native `-c mcp_servers.<name>.*` overrides; secret values remain in the
provider environment. Codex forwards stdio MCP variables by name through
`mcp_servers.<name>.env_vars` and cannot rename them, so every
`environment_from` entry must name a source variable identical to the child
variable. Codex does not yet advertise system-prompt additions, exact tool
restrictions, or strict MCP isolation. Requests using those fields fail before
spawn.

## Remote execution

Protocol version two carries `LaunchContext` on both runtime acquisition and
individual turns. Secret environment values use the existing redacted
protocol-secret representation. A version-one frame with non-empty launch
context is rejected, and the remote client emits version two, so an older host
cannot silently ignore system or tool enforcement. Pin compatible SDK revisions
on both sides of a remote boundary.

The SSH transport stages the provider invocation through an owner-only remote
launcher. Environment values, system prompts, and provider arguments are sent
over SSH standard input and do not appear in the local `ssh` process arguments.
The launcher lives in a `mktemp` control directory and is deleted when the
provider exits or is terminated.
