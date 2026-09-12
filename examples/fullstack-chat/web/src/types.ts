export type ChatStatus =
  | "queued"
  | "running"
  | "approval_needed"
  | "input_needed"
  | "cancelling"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface ChatSnapshot {
  id: string;
  title: string;
  mode: "demo" | "installed";
  provider: "claude" | "codex" | "opencode";
  permission: "default" | "accept_edits" | "plan" | "full_access";
  scenario: "approval" | "failure";
  target: string;
  connection_kind: "local" | "ssh" | "temps_sandbox";
  connection_id: string | null;
  connection_key: string;
  working_directory: string;
  status: ChatStatus;
  draft: string;
  session_id: string | null;
  model: string | null;
  reasoning: string | null;
  usage: Usage;
  account_usage: AccountUsageSnapshot | null;
  harness_options: Record<string, string>;
  error: string | null;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface ContextWindowUsage {
  used_tokens: number | null;
  limit_tokens: number | null;
  model: string | null;
  estimated: boolean;
}

export interface Usage {
  input_tokens: number | null;
  output_tokens: number | null;
  cache_creation_input_tokens?: number | null;
  cache_read_input_tokens?: number | null;
  context_window?: ContextWindowUsage | null;
  cost_usd: number | null;
}

export interface AccountUsageWindow {
  id: string;
  label?: string | null;
  kind: "session" | "weekly" | "other";
  used_percent: number;
  duration_minutes: number | null;
  resets_at_unix_seconds: number | null;
}

export interface AccountUsageSnapshot {
  provider: "claude" | "codex" | "open_code";
  plan: string | null;
  windows: AccountUsageWindow[];
  credits: { unlimited: boolean; balance: string | null; currency?: string | null } | null;
}

export interface QuestionOption {
  label: string;
  description: string;
}

export interface QuestionPrompt {
  header: string;
  question: string;
  options: QuestionOption[];
  multiSelect: boolean;
}

export interface QuestionRequest {
  id: string;
  prompts: QuestionPrompt[];
  questions: QuestionPrompt[] | { questions: QuestionPrompt[] };
}

export type ExecutionTarget =
  | { kind: "local" }
  | {
      kind: "ssh";
      connection_id?: string | null;
      host: string;
      user: string | null;
      port: number | null;
      authentication:
        | { kind: "agent" }
        | { kind: "identity_file"; path: string }
        | { kind: "password"; password: string };
      known_hosts_file: string | null;
      accept_new_host_key: boolean;
    }
  | {
      kind: "temps_sandbox";
      base_url: string;
      sandbox_id: string;
      auth: { kind: "bearer"; token: string } | { kind: "session_cookie"; cookie: string };
    };

export type HarnessStatus = "ready" | "not_installed" | "incompatible" | "unavailable";

export interface HarnessControlOption {
  id: string;
  label: string;
  description: string;
  is_default: boolean;
  dangerous: boolean;
}

export interface HarnessControlGroup {
  id: string;
  label: string;
  kind: "permission" | "sandbox" | "collaboration" | "agent";
  options: HarnessControlOption[];
}

export interface HarnessModel {
  id: string;
  label: string;
  description: string | null;
  is_default: boolean;
  reasoning_efforts: Array<{ id: string; label: string; description: string | null; is_default: boolean }>;
  service_tiers: Array<{ id: string; label: string; description: string | null; is_default: boolean }>;
}

export interface HarnessInventory {
  transport: string;
  transport_capabilities: {
    remote: boolean;
    interactive_stdin: boolean;
    reconnect: boolean;
    managed_processes: boolean;
    process_tree_termination: boolean;
    sandbox: Record<string, boolean>;
  };
  harnesses: Array<{
    provider: "claude" | "codex" | "open_code";
    status: HarnessStatus;
    readiness: {
      installed: boolean;
      executable: string | null;
      version: string | null;
      detail: string;
    } | null;
    permissions: {
      default: boolean;
      accept_edits: boolean;
      plan: boolean;
      full_access: boolean;
      custom: boolean;
      live_approvals: boolean;
      live_questions: boolean;
    };
    control_groups: HarnessControlGroup[];
    models: {
      status: "ready" | "partial" | "unsupported" | "failed";
      source: string;
      models: HarnessModel[];
      error: { kind: string; message: string; retryable: boolean } | null;
    };
    account_usage?: AccountUsageSnapshot | null;
    limitations: string[];
    error: {
      kind: string;
      message: string;
      retryable: boolean;
    } | null;
  }>;
}

export interface WorkingDirectoryCandidates {
  home: string;
  directories: string[];
  exact_match: boolean;
}

export interface HarnessExtensionAccess {
  allowed: boolean;
  denial_kind: "transport_unsupported" | "provider_unsupported" | "permission_denied" | "harness_unavailable" | "scope_unsupported" | null;
  reason: string | null;
}

export interface HarnessExtensionInventory {
  provider: "claude" | "codex" | "open_code";
  transport: string;
  working_directory: string;
  skills: Array<{
    id: string;
    description: string | null;
    path: string;
    scope: "user" | "project";
    source: string;
  }>;
  mcp_servers: Array<{
    name: string;
    enabled: boolean | null;
    transport: string | null;
    detail: string | null;
  }>;
  manage_user_skills: HarnessExtensionAccess;
  manage_project_skills: HarnessExtensionAccess;
  manage_user_mcp_servers: HarnessExtensionAccess;
  manage_project_mcp_servers: HarnessExtensionAccess;
  warnings: Array<{ stage: string; message: string; retryable: boolean }>;
}

export interface SshConnectionSummary {
  id: string;
  label: string;
  host: string;
  user: string | null;
  port: number | null;
  authentication: "agent" | "identity_file" | "password";
  identity_file: string | null;
  known_hosts_file: string | null;
  accept_new_host_key: boolean;
  has_password: boolean;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface ChatMessage {
  sequence: number;
  role: "user" | "assistant" | "system";
  content: string;
  attachments: ChatAttachment[];
  created_at_ms: number;
}

export interface ChatAttachment {
  id: string;
  name: string;
  uri: string;
  media_type: string | null;
  metadata: Record<string, unknown>;
}

export interface QueuedChatMessage {
  id: string;
  chat_id: string;
  content: string;
  attachments: ChatAttachment[];
  revision: number;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  metadata: Record<string, unknown>;
}

export interface ChatEvent {
  sequence: number;
  timestamp_ms: number;
  kind: string;
  payload: Record<string, unknown>;
}

export interface ChatView {
  chat: ChatSnapshot;
  messages: ChatMessage[];
  events: ChatEvent[];
}

export interface ApprovalRequest {
  type?: "approval_requested" | "plan_approval_requested";
  id: string;
  tool_name: string;
  description?: string;
  input?: unknown;
}
