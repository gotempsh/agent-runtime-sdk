//! Host-scoped skills and MCP server discovery and management contracts.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::Provider;

/// Where a harness extension is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessExtensionScope {
    /// Available to the execution identity across working directories.
    User,
    /// Available from the selected working directory or its project.
    Project,
}

/// Why an extension discovery or management operation is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HarnessExtensionDenialKind {
    /// The execution transport cannot inspect or mutate the target filesystem.
    TransportUnsupported,
    /// The provider does not expose a safe management interface for this operation.
    ProviderUnsupported,
    /// The execution identity cannot read or write the provider-owned location.
    PermissionDenied,
    /// The provider executable is not available on the selected host.
    HarnessUnavailable,
    /// The requested scope is not implemented by this provider.
    ScopeUnsupported,
}

/// Explicit capability result for one extension operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessExtensionAccess {
    /// Whether the operation may be attempted on this execution host.
    pub allowed: bool,
    /// Stable denial category when `allowed` is false.
    pub denial_kind: Option<HarnessExtensionDenialKind>,
    /// User-facing reason, without credentials or config contents.
    pub reason: Option<String>,
}

impl HarnessExtensionAccess {
    pub(crate) fn allowed() -> Self {
        Self {
            allowed: true,
            denial_kind: None,
            reason: None,
        }
    }

    pub(crate) fn denied(kind: HarnessExtensionDenialKind, reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            denial_kind: Some(kind),
            reason: Some(reason.into()),
        }
    }
}

/// One skill visible to a provider on the selected execution host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessSkill {
    /// Provider-visible skill identifier.
    pub id: String,
    /// Short description extracted from `SKILL.md` frontmatter when available.
    pub description: Option<String>,
    /// Host-native path to the skill entrypoint.
    pub path: PathBuf,
    /// Visibility scope of this definition.
    pub scope: HarnessExtensionScope,
    /// Provider-compatible source root, such as `claude` or `agents`.
    pub source: String,
}

/// One MCP server advertised by the provider CLI on the selected execution host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessMcpServer {
    /// Provider-native server name.
    pub name: String,
    /// Whether the provider reports this server as enabled.
    pub enabled: Option<bool>,
    /// Provider-reported transport such as `stdio`, `http`, or `sse`.
    pub transport: Option<String>,
    /// Bounded, non-secret status detail from the provider.
    pub detail: Option<String>,
}

/// A non-fatal extension discovery issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessExtensionWarning {
    /// Stable stage, such as `skills` or `mcp_servers`.
    pub stage: String,
    /// Actionable, non-secret explanation.
    pub message: String,
    /// Whether retrying the same request may succeed.
    pub retryable: bool,
}

/// Host-specific extensions applicable to one harness and working directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessExtensionInventory {
    /// Provider whose conventions were used.
    pub provider: Provider,
    /// Selected execution transport.
    pub transport: String,
    /// Host-native working directory used for project scope.
    pub working_directory: PathBuf,
    /// Skills matching the bounded query.
    pub skills: Vec<HarnessSkill>,
    /// MCP servers returned by the provider's metadata command.
    pub mcp_servers: Vec<HarnessMcpServer>,
    /// Whether user-scoped skills can be managed.
    pub manage_user_skills: HarnessExtensionAccess,
    /// Whether project-scoped skills can be managed.
    pub manage_project_skills: HarnessExtensionAccess,
    /// Whether user-scoped MCP servers can be managed.
    pub manage_user_mcp_servers: HarnessExtensionAccess,
    /// Whether project-scoped MCP servers can be managed.
    pub manage_project_mcp_servers: HarnessExtensionAccess,
    /// Non-fatal partial discovery failures.
    pub warnings: Vec<HarnessExtensionWarning>,
}

/// Bounded skill query inside one harness execution host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessExtensionQuery {
    /// Provider whose skill and MCP conventions should be used.
    pub provider: Provider,
    /// Working directory meaningful inside the configured transport.
    pub working_directory: PathBuf,
    /// Case-insensitive substring matched against skill ids and descriptions.
    pub skill_query: Option<String>,
    /// Maximum number of skills returned, from 1 through 200.
    pub limit: usize,
}

/// Complete `SKILL.md` mutation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillManagementRequest {
    /// Target provider.
    pub provider: Provider,
    /// User or project visibility.
    pub scope: HarnessExtensionScope,
    /// Portable lowercase kebab-case skill id.
    pub name: String,
    /// Working directory used to resolve project scope.
    pub working_directory: PathBuf,
    /// Complete `SKILL.md` content for an install/update, or `None` to remove it.
    pub content: Option<String>,
}

/// MCP connection definition accepted by provider CLI management commands.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HarnessMcpDefinition {
    /// A local stdio server process.
    Stdio {
        /// Executable and ordered arguments.
        command: Vec<String>,
        /// Environment variables passed to the server. Values may contain secrets and are never
        /// returned by discovery.
        environment: BTreeMap<String, String>,
    },
    /// A streamable HTTP server.
    Http {
        /// Server URL.
        url: String,
        /// Optional environment-variable name containing a bearer token.
        bearer_token_env_var: Option<String>,
    },
}

impl fmt::Debug for HarnessMcpDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdio { .. } => formatter.write_str("Stdio { ..redacted.. }"),
            Self::Http { .. } => formatter.write_str("Http { ..redacted.. }"),
        }
    }
}

/// MCP server mutation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerManagementRequest {
    /// Target provider.
    pub provider: Provider,
    /// User or project visibility.
    pub scope: HarnessExtensionScope,
    /// Provider-native server name.
    pub name: String,
    /// Working directory used for project configuration.
    pub working_directory: PathBuf,
    /// Server definition for an add/update, or `None` to remove it.
    pub definition: Option<HarnessMcpDefinition>,
}
