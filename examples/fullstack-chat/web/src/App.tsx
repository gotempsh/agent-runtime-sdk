import * as Dialog from "@radix-ui/react-dialog";
import ClaudeIcon from "@lobehub/icons/es/Claude/components/Mono.js";
import CodexIcon from "@lobehub/icons/es/Codex/components/Mono.js";
import OpenCodeIcon from "@lobehub/icons/es/OpenCode/components/Mono.js";
import {
  Activity,
  Check,
  ChevronRight,
  CircleAlert,
  Clock3,
  Code2,
  Folder,
  Gauge,
  GitBranch,
  ListChecks,
  Menu,
  Monitor,
  Network,
  Paperclip,
  PanelRight,
  Play,
  Plus,
  RefreshCw,
  Server,
  ShieldCheck,
  SlidersHorizontal,
  Trash2,
  X,
  Zap,
} from "lucide-react";
import { Fragment, type FormEvent, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Conversation as AIConversation,
  ConversationContent,
  ConversationEmptyState,
  ConversationScrollButton,
} from "@/components/ai-elements/conversation";
import {
  Message as AIMessage,
  MessageContent,
  MessageResponse,
} from "@/components/ai-elements/message";
import {
  Reasoning,
  ReasoningContent,
  ReasoningTrigger,
} from "@/components/ai-elements/reasoning";
import { LoadingState } from "@/components/loading-state";
import { PromptBar } from "@/components/prompt-bar";
import {
  ClaudeSubagents,
  type ClaudeSubagentActivityView,
  type ClaudeSubagentView,
} from "@/components/claude-subagents";
import { ToolChips, type ToolChipStep } from "@/components/tool-chips";
import { cn } from "@/lib/utils";
import type { AccountUsageSnapshot, ApprovalRequest, ChatAttachment, ExecutionTarget, HarnessExtensionInventory, HarnessInventory, HarnessStatus, ChatEvent, ChatSnapshot, ChatStatus, ChatView, QuestionRequest, QueuedChatMessage, SshConnectionSummary, Usage, WorkingDirectoryCandidates } from "@/types";

const eventNames = ["chat_status", "message_started", "message_appended", "plan_created", "question_answered", "turn_event", "turn_result", "chat_error", "compaction_requested", "compaction_progress", "compaction_invocation_started", "compaction_invocation_completed", "compaction_invocation_failed", "compaction_result", "compaction_interrupt"];

const statusCopy: Record<ChatStatus, string> = {
  queued: "Queued",
  running: "Running",
  approval_needed: "Approval needed",
  input_needed: "Answer needed",
  cancelling: "Cancelling",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
};

const statusTone: Record<ChatStatus, "neutral" | "active" | "warning" | "success" | "danger"> = {
  queued: "neutral",
  running: "active",
  approval_needed: "warning",
  input_needed: "warning",
  cancelling: "warning",
  succeeded: "success",
  failed: "danger",
  cancelled: "neutral",
};

function isTerminal(status: ChatStatus) {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

function formatTime(timestamp: number) {
  return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(timestamp);
}

function formatTimeAgo(timestamp: number, now: number) {
  const elapsedSeconds = Math.max(0, Math.floor((now - timestamp) / 1_000));
  if (elapsedSeconds < 5) return "now";
  if (elapsedSeconds < 60) return `${elapsedSeconds}s ago`;

  const elapsedMinutes = Math.floor(elapsedSeconds / 60);
  if (elapsedMinutes < 60) return `${elapsedMinutes}m ago`;

  const elapsedHours = Math.floor(elapsedMinutes / 60);
  if (elapsedHours < 24) return `${elapsedHours}h ago`;

  return `${Math.floor(elapsedHours / 24)}d ago`;
}

function eventType(event: ChatEvent) {
  return event.kind === "turn_event" && typeof event.payload.type === "string"
    ? event.payload.type
    : event.kind;
}

type CompactionPhase = "queued" | "running" | null;

function nextCompactionPhase(phase: CompactionPhase, event: ChatEvent): CompactionPhase {
  const type = eventType(event);
  if (type === "compaction_requested") return "queued";
  if (
    phase !== null
    && type === "chat_status"
    && event.payload.status === "running"
  ) return "running";
  if (
    type === "compaction_invocation_started"
    || type === "compaction_started"
    || type === "compaction_progress"
  ) return "running";
  if (
    phase !== null
    && (
      type === "compaction_invocation_completed"
      || type === "compaction_invocation_failed"
      || type === "compaction_result"
      || (type === "chat_status" && ["succeeded", "failed", "cancelled"].includes(String(event.payload.status)))
    )
  ) return null;
  return phase;
}

function compactionPhase(events: ChatEvent[]): CompactionPhase {
  return events.reduce<CompactionPhase>(nextCompactionPhase, null);
}

function projectedActivity(events: ChatEvent[]) {
  let phase: CompactionPhase = null;
  let sessionId: string | null = null;
  return events.map((event) => {
    const type = eventType(event);
    let displayType = type;
    let summary = eventSummary(event);

    if (type === "session_started") {
      const nextSessionId = typeof event.payload.session_id === "string" ? event.payload.session_id : null;
      if (phase !== null && nextSessionId !== null && nextSessionId === sessionId) {
        displayType = "session_resumed";
        summary = `Reattached session ${nextSessionId} for compaction`;
      }
      sessionId = nextSessionId;
    }

    phase = nextCompactionPhase(phase, event);
    return { event, displayType, summary };
  });
}

function effectivePermissionOption(mode: unknown) {
  if (typeof mode === "string") {
    return {
      default: "manual",
      accept_edits: "acceptEdits",
      plan: "plan",
      full_access: "bypassPermissions",
    }[mode] ?? null;
  }
  if (mode && typeof mode === "object" && typeof (mode as { custom?: unknown }).custom === "string") {
    return (mode as { custom: string }).custom;
  }
  return null;
}

function eventSummary(event: ChatEvent) {
  const payload = event.payload;
  switch (eventType(event)) {
    case "chat_status":
      return statusCopy[payload.status as ChatStatus] ?? "Status changed";
    case "plan_created":
      return String(payload.title ?? "Plan created");
    case "session_started":
      return `Session ${String(payload.session_id ?? "started")}`;
    case "reasoning_delta":
      return String(payload.text ?? "Reasoning update");
    case "approval_requested":
      return `Approval for ${String(payload.tool_name ?? "tool")}`;
    case "plan_approval_requested":
      return "Plan proposed · approval required";
    case "permission_mode_changed":
      return `Permission mode · ${String(effectivePermissionOption(payload.mode) ?? "changed")}`;
    case "question_requested":
      return "Agent asked a question";
    case "question_answered":
      return "Question answered";
    case "tool_call":
      return `${String(payload.name ?? "Tool")} · ${String(payload.status ?? "updated")}`;
    case "tasks_changed": {
      const tasks = Array.isArray(payload.tasks) ? payload.tasks : [];
      const running = tasks.filter((task) => {
        if (!task || typeof task !== "object") return false;
        const status = String((task as Record<string, unknown>).status ?? "running").toLowerCase();
        return !["completed", "succeeded", "failed", "stopped", "cancelled", "canceled", "killed"].includes(status);
      }).length;
      return `${tasks.length} Claude ${tasks.length === 1 ? "subagent" : "subagents"}${running ? ` · ${running} running` : ""}`;
    }
    case "task_activity": {
      const activity = payload.activity && typeof payload.activity === "object"
        ? payload.activity as Record<string, unknown>
        : {};
      const description = String(activity.description ?? activity.task_id ?? "Claude subagent");
      const kind = String(activity.kind ?? "updated").replaceAll("_", " ");
      const detail = activity.summary ?? activity.last_tool_name;
      return `${description} · ${kind}${detail ? ` · ${String(detail)}` : ""}`;
    }
    case "text_delta":
      return String(payload.text ?? "Response chunk");
    case "usage":
      return `${String(payload.input_tokens ?? 0)} in · ${String(payload.output_tokens ?? 0)} out`;
    case "account_usage_updated": {
      const usage = objectValue(payload.usage);
      const windows = Array.isArray(usage?.windows) ? usage.windows : [];
      return `${windows.length} account usage ${windows.length === 1 ? "window" : "windows"} refreshed`;
    }
    case "message_started":
      return `Processing message ${String(payload.message_sequence ?? "")}`.trim();
    case "message_appended":
      return `${String(payload.role ?? "Message")} message persisted`;
    case "turn_result":
      return "Provider response persisted";
    case "chat_error":
      return String(payload.message ?? "Chat failed");
    case "warning":
      return String(payload.message ?? "Provider warning");
    case "compaction_requested":
      return "Manual context compaction queued";
    case "compaction_started":
      return "Compacting the Claude session";
    case "compaction_completed":
      return "Claude compacted the session context";
    case "compaction_progress":
      return String(payload.message ?? "Claude is compacting context");
    case "compaction_invocation_started":
      return "Compaction invocation started";
    case "compaction_invocation_completed":
      return "Compaction invocation completed";
    case "compaction_invocation_failed":
      return String(payload.message ?? "Compaction failed");
    case "compaction_result":
      return "Compacted provider session persisted";
    case "compaction_interrupt":
      return `Compaction interrupt · ${String(payload.outcome ?? "requested")}`;
    default:
      return eventType(event).replaceAll("_", " ");
  }
}

interface NormalizedToolCall {
  key: string;
  turn: number;
  id: string | null;
  name: string;
  state: ToolChipStep["state"];
  input: unknown;
  output: string | null;
  error: string | null;
  taskId: string | null;
  sequence: number;
}

function normalizedToolCalls(events: ChatEvent[]) {
  const calls: NormalizedToolCall[] = [];
  let turn = 0;

  for (const event of events) {
    if (event.kind === "message_started") {
      turn += 1;
      continue;
    }
    if (eventType(event) !== "tool_call") continue;

    const id = typeof event.payload.id === "string" ? event.payload.id : null;
    const name = String(event.payload.name ?? "tool");
    const taskId = typeof event.payload.task_id === "string" ? event.payload.task_id : null;
    const status = String(event.payload.status ?? "started");
    const state: ToolChipStep["state"] = status === "failed"
      ? "output-error"
      : status === "succeeded"
        ? "output-available"
        : "input-available";
    let existingIndex = id ? calls.findIndex((call) => call.turn === turn && call.id === id) : -1;
    if (!id) {
      for (let index = calls.length - 1; index >= 0; index -= 1) {
        if (calls[index].turn === turn && calls[index].name === name && calls[index].state === "input-available") {
          existingIndex = index;
          break;
        }
      }
    }

    if (existingIndex >= 0) {
      const current = calls[existingIndex];
      calls[existingIndex] = {
        ...current,
        state,
        input: event.payload.input ?? current.input,
        output: typeof event.payload.output === "string" ? event.payload.output : current.output,
        error: typeof event.payload.error === "string" ? event.payload.error : current.error,
        taskId: taskId ?? current.taskId,
      };
      continue;
    }

    calls.push({
      key: `${turn}-${id ?? `${name}-${event.sequence}`}`,
      turn,
      id,
      name,
      state,
      input: event.payload.input ?? null,
      output: typeof event.payload.output === "string" ? event.payload.output : null,
      error: typeof event.payload.error === "string" ? event.payload.error : null,
      taskId,
      sequence: event.sequence,
    });
  }

  return calls;
}

type ConversationTimelineItem =
  | { kind: "message"; key: string; order: number; message: ChatView["messages"][number] }
  | { kind: "tool"; key: string; order: number; tool: NormalizedToolCall };

function conversationTimeline(view: ChatView): ConversationTimelineItem[] {
  const messageOrders = new Map<number, number>();
  for (const event of view.events) {
    if (event.kind === "message_started") {
      const sequence = Number(event.payload.message_sequence);
      if (Number.isSafeInteger(sequence) && !messageOrders.has(sequence)) {
        messageOrders.set(sequence, event.sequence - 0.5);
      }
      continue;
    }
    if (event.kind !== "message_appended") continue;
    const sequence = Number(event.payload.sequence);
    if (Number.isSafeInteger(sequence)) messageOrders.set(sequence, event.sequence);
  }

  const fallbackStart = (view.events.at(-1)?.sequence ?? 0) + 1;
  const items: ConversationTimelineItem[] = view.messages.map((message, index) => ({
    kind: "message",
    key: `message-${message.sequence}`,
    order: messageOrders.get(message.sequence) ?? fallbackStart + index,
    message,
  }));
  for (const tool of normalizedToolCalls(view.events)) {
    if (tool.taskId !== null) continue;
    items.push({
      kind: "tool",
      key: `tool-${tool.key}`,
      order: tool.sequence,
      tool,
    });
  }
  return items.sort((left, right) => left.order - right.order || left.key.localeCompare(right.key));
}

function objectValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringValue(value: unknown) {
  return typeof value === "string" && value.trim() ? value : null;
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function taskStatusForActivity(kind: string, current: string) {
  if (kind === "completed") return "completed";
  if (kind === "failed") return "failed";
  if (kind === "stopped") return "stopped";
  if (kind === "started" || kind === "progress") return "running";
  return current;
}

function normalizedClaudeSubagents(events: ChatEvent[]): ClaudeSubagentView[] {
  const agents = new Map<string, ClaudeSubagentView>();

  function ensure(id: string) {
    const existing = agents.get(id);
    if (existing) return existing;
    const created: ClaudeSubagentView = {
      id,
      description: id,
      status: "running",
      agentType: null,
      error: null,
      summary: null,
      spawnDepth: 0,
      usage: null,
      activities: [],
      tools: [],
    };
    agents.set(id, created);
    return created;
  }

  for (const event of events) {
    const type = eventType(event);
    if (type === "tasks_changed") {
      const tasks = Array.isArray(event.payload.tasks) ? event.payload.tasks : [];
      for (const rawTask of tasks) {
        const task = objectValue(rawTask);
        const id = stringValue(task?.id);
        if (!task || !id) continue;
        const agent = ensure(id);
        agent.description = stringValue(task.description) ?? agent.description;
        agent.status = stringValue(task.status) ?? agent.status;
        agent.agentType = stringValue(task.agent_type) ?? agent.agentType;
        agent.error = stringValue(task.error) ?? agent.error;
        agent.summary = stringValue(task.summary) ?? agent.summary;
      }
      continue;
    }

    if (type === "task_activity") {
      const activity = objectValue(event.payload.activity);
      const id = stringValue(activity?.task_id);
      if (!activity || !id) continue;
      const agent = ensure(id);
      const kind = stringValue(activity.kind) ?? "updated";
      agent.description = stringValue(activity.description) ?? agent.description;
      agent.status = stringValue(activity.status) ?? taskStatusForActivity(kind, agent.status);
      agent.agentType = stringValue(activity.agent_type) ?? agent.agentType;
      agent.summary = stringValue(activity.summary) ?? agent.summary;
      agent.spawnDepth = numberValue(activity.spawn_depth) ?? agent.spawnDepth;
      const usage = objectValue(activity.usage);
      if (usage) {
        agent.usage = {
          totalTokens: numberValue(usage.total_tokens) ?? 0,
          toolUses: numberValue(usage.tool_uses) ?? 0,
          durationMs: numberValue(usage.duration_ms) ?? 0,
        };
      }
      const normalizedActivity: ClaudeSubagentActivityView = {
        sequence: event.sequence,
        kind,
        summary: stringValue(activity.summary),
        lastToolName: stringValue(activity.last_tool_name),
        timestampMs: event.timestamp_ms,
      };
      agent.activities.push(normalizedActivity);
    }
  }

  for (const tool of normalizedToolCalls(events)) {
    if (!tool.taskId) continue;
    ensure(tool.taskId).tools.push(tool);
  }

  return [...agents.values()];
}

function claudeSubagentsByMessage(events: ChatEvent[]) {
  const eventsByMessage = new Map<number, ChatEvent[]>();
  let messageSequence: number | null = null;

  for (const event of events) {
    if (event.kind === "message_started") {
      const sequence = Number(event.payload.message_sequence);
      messageSequence = Number.isSafeInteger(sequence) ? sequence : null;
      continue;
    }
    if (messageSequence === null) continue;
    const type = eventType(event);
    const nestedTool = type === "tool_call" && typeof event.payload.task_id === "string";
    if (type !== "tasks_changed" && type !== "task_activity" && !nestedTool) continue;
    const messageEvents = eventsByMessage.get(messageSequence) ?? [];
    messageEvents.push(event);
    eventsByMessage.set(messageSequence, messageEvents);
  }

  return new Map(
    [...eventsByMessage].map(([sequence, messageEvents]) => [sequence, normalizedClaudeSubagents(messageEvents)]),
  );
}

function sortChats(chats: ChatSnapshot[]) {
  return [...chats].sort((a, b) => b.updated_at_ms - a.updated_at_ms);
}

function targetForSavedConnection(connection: SshConnectionSummary): ExecutionTarget {
  const authentication: Extract<ExecutionTarget, { kind: "ssh" }>["authentication"] = connection.authentication === "identity_file"
    ? { kind: "identity_file", path: connection.identity_file ?? "" }
    : connection.authentication === "password"
      ? { kind: "password", password: "" }
      : { kind: "agent" };
  return {
    kind: "ssh",
    connection_id: connection.id,
    host: connection.host,
    user: connection.user,
    port: connection.port,
    authentication,
    known_hosts_file: connection.known_hosts_file,
    accept_new_host_key: connection.accept_new_host_key,
  };
}

function targetForChat(chat: ChatSnapshot, connections: SshConnectionSummary[], current: ExecutionTarget): ExecutionTarget {
  if (chat.connection_kind === "local") return { kind: "local" };
  if (chat.connection_kind === "ssh") {
    const saved = connections.find((connection) => connection.id === chat.connection_id);
    if (saved) return targetForSavedConnection(saved);
    if (current.kind === "ssh" && current.connection_id === chat.connection_id) return current;
    const separator = chat.target.lastIndexOf("@");
    return {
      kind: "ssh",
      connection_id: chat.connection_id,
      host: separator >= 0 ? chat.target.slice(separator + 1) : chat.target,
      user: separator >= 0 ? chat.target.slice(0, separator) : null,
      port: 22,
      authentication: { kind: "agent" },
      known_hosts_file: null,
      accept_new_host_key: false,
    };
  }
  return current.kind === "temps_sandbox" ? current : { kind: "local" };
}

function connectionLabel(target: ExecutionTarget, connections: SshConnectionSummary[], persistedLabel?: string) {
  if (target.kind === "local") return "Local machine";
  if (target.kind === "temps_sandbox") return persistedLabel ?? `Temps sandbox · ${target.sandbox_id}`;
  const saved = connections.find((connection) => connection.id === target.connection_id);
  if (saved) return `${saved.label} · ${saved.user ? `${saved.user}@` : ""}${saved.host}`;
  return persistedLabel ?? `${target.user ? `${target.user}@` : ""}${target.host}`;
}

function connectionKey(target: ExecutionTarget) {
  if (target.kind === "local") return "local";
  if (target.kind === "temps_sandbox") {
    return `temps_sandbox:${target.base_url}:${target.sandbox_id}:${target.auth.kind}`;
  }
  if (target.connection_id) return `ssh:saved:${target.connection_id}`;
  const authentication = target.authentication.kind === "identity_file"
    ? `identity:${target.authentication.path}`
    : target.authentication.kind;
  return [
    "ssh:raw",
    target.host,
    target.user ?? "",
    target.port ?? 22,
    authentication,
    target.known_hosts_file ?? "",
    String(target.accept_new_host_key),
  ].join(":");
}

function ExecutionTargetSelect({
  target,
  connections,
  label,
  onChange,
  onAdd,
}: {
  target: ExecutionTarget;
  connections: SshConnectionSummary[];
  label: string;
  onChange: (target: ExecutionTarget) => void;
  onAdd: () => void;
}) {
  const savedTarget = target.kind === "ssh"
    ? connections.find((connection) => connection.id === target.connection_id)
    : null;
  const value = target.kind === "local"
    ? "local"
    : target.kind === "ssh" && savedTarget
      ? `ssh:${savedTarget.id}`
      : "current";
  const TargetIcon = target.kind === "local" ? Monitor : target.kind === "ssh" ? Network : Server;

  return (
    <div className="execution-target-select">
      <TargetIcon className="execution-target-icon size-4" aria-hidden="true" />
      <select
        aria-label="Execution target"
        value={value}
        onChange={(event) => {
          const nextValue = event.target.value;
          if (nextValue === "add") {
            onAdd();
            return;
          }
          if (nextValue === "local") {
            onChange({ kind: "local" });
            return;
          }
          const connection = connections.find((item) => `ssh:${item.id}` === nextValue);
          if (connection) onChange(targetForSavedConnection(connection));
        }}
      >
        <option value="local">Local machine</option>
        {connections.map((connection) => (
          <option key={connection.id} value={`ssh:${connection.id}`}>
            {connection.label} · {connection.user ? `${connection.user}@` : ""}{connection.host}
          </option>
        ))}
        {value === "current" ? <option value="current">{label}</option> : null}
        <option value="add">Add target…</option>
      </select>
      <ChevronRight className="execution-target-chevron size-4 rotate-90" aria-hidden="true" />
    </div>
  );
}

export function App() {
  const [chats, setChats] = useState<ChatSnapshot[]>([]);
  const [view, setView] = useState<ChatView | null>(null);
  const [prompt, setPrompt] = useState("Inspect the runtime stream and verify the approval boundary.");
  const [attachments, setAttachments] = useState<ChatAttachment[]>([]);
  const [queuedMessages, setQueuedMessages] = useState<QueuedChatMessage[]>([]);
  const mode = "installed" as const;
  const [provider, setProvider] = useState<"claude" | "codex" | "opencode">("claude");
  const [permission, setPermission] = useState<"default" | "accept_edits" | "plan" | "full_access">("plan");
  const scenario = "approval" as const;
  const [model, setModel] = useState("");
  const [reasoning, setReasoning] = useState("");
  const [harnessOptions, setHarnessOptions] = useState<Record<string, string>>({});
  const [connection, setConnection] = useState<"connecting" | "live" | "closed">("closed");
  const [submitting, setSubmitting] = useState(false);
  const [compacting, setCompacting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [runsOpen, setRunsOpen] = useState(false);
  const [activityOpen, setActivityOpen] = useState(false);
  const [targetFormOpen, setTargetFormOpen] = useState(false);
  const [target, setTarget] = useState<ExecutionTarget>({ kind: "local" });
  const [savedConnections, setSavedConnections] = useState<SshConnectionSummary[]>([]);
  const savedConnectionsRef = useRef<SshConnectionSummary[]>([]);
  const [workingDirectory, setWorkingDirectory] = useState("");
  const [workingDirectoryValid, setWorkingDirectoryValid] = useState(false);
  const [inventory, setInventory] = useState<HarnessInventory | null>(null);
  const [inventoryConnectionKey, setInventoryConnectionKey] = useState<string | null>(null);
  const [inventoryAttemptedKey, setInventoryAttemptedKey] = useState<string | null>(null);
  const [inventoryLoading, setInventoryLoading] = useState(false);
  const [inventoryError, setInventoryError] = useState<string | null>(null);
  const eventSource = useRef<EventSource | null>(null);
  const inventoryRequest = useRef(0);
  const chatListRequest = useRef(0);
  const initialRequestedChat = useRef(new URL(window.location.href).searchParams.get("chat"));
  const initialSelectionPending = useRef(!initialRequestedChat.current);
  const [chatIndexReady, setChatIndexReady] = useState(false);
  const selectedChatRef = useRef<ChatSnapshot | null>(null);
  selectedChatRef.current = view?.chat ?? null;

  const currentTargetKey = connectionKey(target);
  const inventoryMatchesTarget = inventoryConnectionKey === currentTargetKey;
  const activeInventory = inventoryMatchesTarget ? inventory : null;
  const targetConnectionKey = useMemo(
    () => view?.chat.connection_key?.trim() || connectionKey(target),
    [target, view?.chat.connection_key],
  );
  const selectedHarness = useMemo(
    () => activeInventory?.harnesses.find((harness) => providerChoice(harness.provider) === provider) ?? null,
    [activeInventory, provider],
  );

  const loadSavedConnections = useCallback(async () => {
    try {
      const response = await fetch("/api/ssh-connections");
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? "Could not load saved SSH connections."));
      const next = payload as SshConnectionSummary[];
      savedConnectionsRef.current = next;
      setSavedConnections(next);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not load saved SSH connections.");
    }
  }, []);

  useEffect(() => {
    void loadSavedConnections();
  }, [loadSavedConnections]);

  const discoverInventory = useCallback(async (executionTarget: ExecutionTarget) => {
    const key = connectionKey(executionTarget);
    const request = ++inventoryRequest.current;
    setInventoryAttemptedKey(key);
    setInventoryLoading(true);
    setInventoryError(null);
    try {
      const response = await fetch("/api/discovery", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ target: executionTarget }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? "Could not inspect harnesses on this execution target."));
      if (inventoryRequest.current !== request) return;
      setInventory(payload as HarnessInventory);
      setInventoryConnectionKey(key);
    } catch (cause) {
      if (inventoryRequest.current !== request) return;
      setInventoryError(cause instanceof Error ? cause.message : "Could not inspect harnesses on this execution target.");
    } finally {
      if (inventoryRequest.current === request) setInventoryLoading(false);
    }
  }, []);

  useEffect(() => {
    if (inventoryConnectionKey === currentTargetKey || inventoryAttemptedKey === currentTargetKey || inventoryLoading) return;
    if (
      target.kind === "ssh"
      && target.connection_id
      && !savedConnections.some((connection) => connection.id === target.connection_id)
    ) return;
    void discoverInventory(target);
  }, [currentTargetKey, discoverInventory, inventoryAttemptedKey, inventoryConnectionKey, inventoryLoading, savedConnections, target]);

  useEffect(() => {
    if (!selectedHarness) return;
    setPermission("default");
    setHarnessOptions((current) => {
      const next: Record<string, string> = {};
      for (const group of selectedHarness.control_groups) {
        const existing = current[group.id];
        next[group.id] = group.options.some((option) => option.id === existing)
          ? existing
          : group.options.find((option) => option.is_default)?.id ?? group.options[0]?.id ?? "";
      }
      return next;
    });
    setModel((current) => selectedHarness.models.models.some((item) => item.id === current)
      ? current
      : selectedHarness.models.models.find((item) => item.is_default)?.id ?? selectedHarness.models.models[0]?.id ?? "");
  }, [selectedHarness]);

  useEffect(() => {
    if (!selectedHarness) return;
    const selectedModel = selectedHarness.models.models.find((item) => item.id === model);
    if (!selectedModel) {
      setReasoning("");
      setHarnessOptions((current) => {
        if (!("service_tier" in current)) return current;
        const next = { ...current };
        delete next.service_tier;
        return next;
      });
      return;
    }
    setReasoning((current) => selectedModel.reasoning_efforts.some((effort) => effort.id === current)
      ? current
      : selectedModel.reasoning_efforts.find((effort) => effort.is_default)?.id ?? "");
    setHarnessOptions((current) => {
      const tier = current.service_tier;
      if (!tier || selectedModel.service_tiers.some((item) => item.id === tier)) return current;
      const next = { ...current };
      delete next.service_tier;
      return next;
    });
  }, [model, selectedHarness]);

  const selectedId = view?.chat.id ?? null;

  useEffect(() => {
    const chat = selectedChatRef.current;
    if (!chat) return;
    setTarget((current) => targetForChat(chat, savedConnections, current));
  }, [savedConnections, view?.chat.connection_id, view?.chat.connection_kind, view?.chat.id, view?.chat.target]);

  const applyChatSettings = useCallback((chat: ChatSnapshot) => {
    setProvider(chat.provider);
    setPermission(chat.permission);
    setModel(chat.model ?? "");
    setReasoning(chat.reasoning ?? "");
    setHarnessOptions({ ...chat.harness_options });
    setWorkingDirectory(chat.working_directory);
    setWorkingDirectoryValid(true);
    setTarget((current) => targetForChat(chat, savedConnectionsRef.current, current));
  }, []);

  const patchChat = useCallback((chat: ChatSnapshot) => {
    setChats((current) => sortChats([chat, ...current.filter((item) => item.id !== chat.id)]));
  }, []);

  const refreshQueue = useCallback(async (chatId: string) => {
    const response = await fetch(`/api/chats/${encodeURIComponent(chatId)}/queue`);
    if (!response.ok) throw new Error(`Could not load queued messages (${response.status}).`);
    setQueuedMessages((await response.json()) as QueuedChatMessage[]);
  }, []);

  const connect = useCallback((chatId: string, after: number) => {
    eventSource.current?.close();
    setConnection("connecting");
    const source = new EventSource(`/api/chats/${encodeURIComponent(chatId)}/events?after=${after}`);
    eventSource.current = source;
    source.onopen = () => setConnection("live");
    source.onerror = () => setConnection("connecting");

    const receive = (message: MessageEvent<string>) => {
      const event = JSON.parse(message.data) as ChatEvent;
      if (event.kind === "chat_status") void refreshQueue(chatId).catch(() => undefined);
      const effectivePermission = event.kind === "turn_event" && event.payload.type === "permission_mode_changed"
        ? effectivePermissionOption(event.payload.mode)
        : null;
      if (effectivePermission && selectedChatRef.current?.id === chatId) {
        setHarnessOptions((current) => ({ ...current, permission_mode: effectivePermission }));
        setPermission(effectivePermission === "plan"
          ? "plan"
          : effectivePermission === "acceptEdits"
            ? "accept_edits"
            : effectivePermission === "bypassPermissions"
              ? "full_access"
              : "default");
      }
      setView((current) => {
        if (!current || current.chat.id !== chatId || current.events.some((item) => item.sequence === event.sequence)) {
          return current;
        }
        const chat = { ...current.chat, updated_at_ms: event.timestamp_ms };
        let messages = current.messages;
        if (event.kind === "chat_status") chat.status = event.payload.status as ChatStatus;
        if (event.kind === "chat_error") {
          chat.error = String(event.payload.message ?? "Chat failed");
          chat.draft = "";
        }
        if (event.kind === "turn_result") {
          chat.session_id = typeof event.payload.session_id === "string" ? event.payload.session_id : chat.session_id;
          chat.title = typeof event.payload.session_title === "string" && event.payload.session_title.trim()
            ? event.payload.session_title
            : chat.title;
          chat.model = typeof event.payload.model === "string" ? event.payload.model : chat.model;
        }
        if (event.kind === "turn_event") {
          if (event.payload.type === "text_delta") chat.draft += String(event.payload.text ?? "");
          if (event.payload.type === "session_started") {
            chat.session_id = String(event.payload.session_id ?? "");
            if (typeof event.payload.title === "string" && event.payload.title.trim()) chat.title = event.payload.title;
          }
          if (event.payload.type === "permission_mode_changed") {
            if (effectivePermission) {
              chat.harness_options = { ...chat.harness_options, permission_mode: effectivePermission };
              chat.permission = effectivePermission === "plan"
                ? "plan"
                : effectivePermission === "acceptEdits"
                  ? "accept_edits"
                  : effectivePermission === "bypassPermissions"
                    ? "full_access"
                    : "default";
            }
          }
          if (event.payload.type === "approval_requested" || event.payload.type === "plan_approval_requested") {
            chat.status = "approval_needed";
          }
          if (event.payload.type === "question_requested") chat.status = "input_needed";
        }
        if (event.kind === "message_appended") {
          const appended = event.payload as unknown as ChatView["messages"][number];
          if (!messages.some((item) => item.sequence === appended.sequence)) messages = [...messages, appended];
          if (appended.role === "assistant") chat.draft = "";
        }
        patchChat(chat);
        return { chat, messages, events: [...current.events, event] };
      });
    };
    eventNames.forEach((name) => source.addEventListener(name, receive as EventListener));
  }, [patchChat, refreshQueue]);

  const selectChat = useCallback(async (chatId: string) => {
    setActionError(null);
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(chatId)}`);
      if (!response.ok) throw new Error(`Could not load chat (${response.status}).`);
      const next = (await response.json()) as ChatView;
      setView(next);
      applyChatSettings(next.chat);
      patchChat(next.chat);
      const url = new URL(window.location.href);
      url.searchParams.set("chat", next.chat.id);
      window.history.replaceState({}, "", url);
      connect(chatId, next.events.at(-1)?.sequence ?? 0);
      await refreshQueue(chatId);
      setRunsOpen(false);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not load the chat.");
    }
  }, [applyChatSettings, connect, patchChat, refreshQueue]);

  useEffect(() => {
    let active = true;
    void (async () => {
      try {
        if (initialRequestedChat.current) await selectChat(initialRequestedChat.current);
      } catch (error) {
        if (active) setActionError(error instanceof Error ? error.message : "Could not restore the requested chat.");
      } finally {
        if (active) setChatIndexReady(true);
      }
    })();
    return () => {
      active = false;
      eventSource.current?.close();
    };
  }, [selectChat]);

  useEffect(() => {
    if (!chatIndexReady) return;
    const request = ++chatListRequest.current;
    setChats([]);
    void (async () => {
      try {
        const response = await fetch(`/api/chats?connection_key=${encodeURIComponent(targetConnectionKey)}`);
        if (!response.ok) throw new Error(`Could not load chats (${response.status}).`);
        const loaded = sortChats((await response.json()) as ChatSnapshot[]);
        if (chatListRequest.current !== request) return;
        setChats(loaded);
        if (initialSelectionPending.current) {
          initialSelectionPending.current = false;
          if (loaded[0]) await selectChat(loaded[0].id);
        }
      } catch (error) {
        if (chatListRequest.current === request) {
          setActionError(error instanceof Error ? error.message : "Could not load chats for this execution target.");
        }
      }
    })();
  }, [chatIndexReady, selectChat, targetConnectionKey]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!prompt.trim() && !attachments.length) return;
    if (!selectedId && !workingDirectoryValid) {
      setActionError("Choose a folder for this chat before sending the first message.");
      return;
    }
    setSubmitting(true);
    setActionError(null);
    try {
      if (selectedId && view && !isTerminal(view.chat.status)) {
        const response = await fetch(`/api/chats/${encodeURIComponent(selectedId)}/queue`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ content: prompt.trim(), attachments }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(String(payload.error ?? `Could not queue message (${response.status}).`));
        setQueuedMessages((current) => [...current, payload as QueuedChatMessage]);
        setPrompt("");
        setAttachments([]);
        return;
      }
      const endpoint = selectedId
        ? `/api/chats/${encodeURIComponent(selectedId)}/messages`
        : "/api/chats";
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prompt: prompt.trim(),
          attachments,
          mode,
          provider,
          permission,
          scenario,
          model: model.trim() ? model.trim() : null,
          reasoning: reasoning || null,
          harness_options: harnessOptions,
          target,
          working_directory: workingDirectory,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? `Could not send message (${response.status}).`));
      const next = payload as ChatView;
      setView(next);
      patchChat(next.chat);
      const url = new URL(window.location.href);
      url.searchParams.set("chat", next.chat.id);
      window.history.replaceState({}, "", url);
      connect(next.chat.id, next.events.at(-1)?.sequence ?? 0);
      setPrompt("");
      setAttachments([]);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not send the message.");
    } finally {
      setSubmitting(false);
    }
  }

  async function updateQueuedMessage(message: QueuedChatMessage) {
    const response = await fetch(`/api/chats/${encodeURIComponent(message.chat_id)}/queue/${encodeURIComponent(message.id)}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        content: message.content,
        attachments: message.attachments,
        expected_revision: message.revision,
      }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(String(payload.error ?? `Could not update queued message (${response.status}).`));
    setQueuedMessages((current) => current.map((item) => item.id === message.id ? payload as QueuedChatMessage : item));
  }

  async function uploadAttachment(file: File) {
    if (!workingDirectoryValid) throw new Error("Choose a folder for this chat before uploading files.");
    const form = new FormData();
    form.append("target", JSON.stringify(target));
    form.append("working_directory", workingDirectory);
    form.append("file", file, file.name);
    const response = await fetch("/api/attachments", { method: "POST", body: form });
    const payload = await response.json();
    if (!response.ok) throw new Error(String(payload.error ?? `Could not upload ${file.name} (${response.status}).`));
    return payload as ChatAttachment;
  }

  async function sendQueuedMessageNow(message: QueuedChatMessage) {
    setActionError(null);
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(message.chat_id)}/queue/${encodeURIComponent(message.id)}/send`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          prompt: message.content,
          attachments: message.attachments,
          mode,
          provider,
          permission,
          scenario,
          model: model.trim() || null,
          reasoning: reasoning || null,
          harness_options: harnessOptions,
          target,
          working_directory: workingDirectory,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? `Could not send queued message (${response.status}).`));
      setQueuedMessages((current) => current.filter((item) => item.id !== message.id));
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not send the queued message.");
    }
  }

  async function resolveApproval(approvalId: string, decision: "allow" | "deny") {
    if (!selectedId) return;
    setActionError(null);
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(selectedId)}/approvals/${encodeURIComponent(approvalId)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ decision }),
      });
      await response.text();
      if (!response.ok) setActionError(`Could not ${decision} the command (${response.status}).`);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : `Could not ${decision} the command.`);
    }
  }

  async function resolveQuestion(questionId: string, answers: Record<string, string>) {
    if (!selectedId) return;
    setActionError(null);
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(selectedId)}/questions/${encodeURIComponent(questionId)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ answers }),
      });
      const payload = await response.text();
      if (!response.ok) {
        let message = `Could not answer the question (${response.status}).`;
        try { message = String((JSON.parse(payload) as { error?: string }).error ?? message); } catch { /* keep status fallback */ }
        setActionError(message);
      }
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not answer the question.");
    }
  }

  async function compactChat() {
    if (!selectedId || !view) throw new Error("Start the chat before compacting its context.");
    setCompacting(true);
    setActionError(null);
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(selectedId)}/compact`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          mode,
          provider,
          permission,
          scenario,
          model: model.trim() || null,
          reasoning: reasoning || null,
          harness_options: harnessOptions,
          target,
          working_directory: workingDirectory,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? `Could not compact this chat (${response.status}).`));
      const next = payload as ChatView;
      setView(next);
      patchChat(next.chat);
      connect(next.chat.id, next.events.at(-1)?.sequence ?? 0);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Could not compact this chat.";
      setActionError(message);
      throw error;
    } finally {
      setCompacting(false);
    }
  }

  async function cancel() {
    if (!selectedId) return;
    try {
      const response = await fetch(`/api/chats/${encodeURIComponent(selectedId)}/cancel`, { method: "POST" });
      await response.text();
      if (!response.ok) setActionError(`Could not cancel the response (${response.status}).`);
    } catch (error) {
      setActionError(error instanceof Error ? error.message : "Could not cancel the response.");
    }
  }

  function startNewChat() {
    eventSource.current?.close();
    setConnection("closed");
    setView(null);
    setPrompt("");
    setAttachments([]);
    setQueuedMessages([]);
    setWorkingDirectory("");
    setWorkingDirectoryValid(false);
    setTargetFormOpen(false);
    setRunsOpen(false);
    const url = new URL(window.location.href);
    url.searchParams.delete("chat");
    window.history.replaceState({}, "", url);
  }

  function chooseNewChatTarget(nextTarget: ExecutionTarget) {
    setTarget(nextTarget);
    setWorkingDirectory("");
    setWorkingDirectoryValid(false);
    setInventoryError(null);
  }

  function chooseHeaderTarget(nextTarget: ExecutionTarget) {
    if (view) startNewChat();
    chooseNewChatTarget(nextTarget);
  }

  function openTargetForm() {
    if (view) startNewChat();
    setTargetFormOpen(true);
  }

  const pendingApproval = useMemo(() => {
    if (view?.chat.status !== "approval_needed") return null;
    const event = [...view.events].reverse().find((item) => {
      const type = eventType(item);
      return type === "approval_requested" || type === "plan_approval_requested";
    });
    return event?.payload as unknown as ApprovalRequest | undefined;
  }, [view]);

  const pendingQuestion = useMemo(() => {
    if (view?.chat.status !== "input_needed") return null;
    const event = [...view.events].reverse().find((item) => eventType(item) === "question_requested");
    return event?.payload as unknown as QuestionRequest | undefined;
  }, [view]);

  const displayedTarget = view ? targetForChat(view.chat, savedConnections, target) : target;
  const targetName = connectionLabel(displayedTarget, savedConnections, view?.chat.target);
  const chatTitle = view?.chat.title ?? "New chat";
  const chatFolder = (view?.chat.working_directory ?? workingDirectory) || "Choose folder";
  const compactDisabledReason = !view
    ? "Start the chat before compacting its context."
    : view.chat.mode !== "installed"
      ? "Manual compaction requires an installed Claude harness."
    : view.chat.provider !== "claude"
      ? "Manual compaction is currently available only for Claude sessions."
      : !view.chat.session_id
        ? "Claude has not created a resumable session yet."
        : !isTerminal(view.chat.status)
          ? "Wait for the current operation to finish before compacting."
          : submitting
            ? "Wait for the message to finish sending before compacting."
            : null;
  const composer = (
    <Composer
      prompt={prompt}
      setPrompt={setPrompt}
      attachments={attachments}
      setAttachments={setAttachments}
      queuedMessages={queuedMessages}
      setQueuedMessages={setQueuedMessages}
      onUpdateQueued={updateQueuedMessage}
      onSendQueuedNow={sendQueuedMessageNow}
      onUploadAttachment={uploadAttachment}
      provider={provider}
      setProvider={setProvider}
      model={model}
      setModel={setModel}
      reasoning={reasoning}
      setReasoning={setReasoning}
      harness={selectedHarness}
      inventory={activeInventory}
      inventoryLoading={inventoryLoading}
      inventoryError={inventoryError}
      onRetryInventory={() => void discoverInventory(target)}
      harnessOptions={harnessOptions}
      setHarnessOptions={setHarnessOptions}
      submitting={submitting}
      compacting={compacting}
      compactAvailable={compactDisabledReason === null}
      compactDisabledReason={compactDisabledReason}
      onCompact={compactChat}
      usage={view?.chat.usage ?? null}
      accountUsage={view?.chat.account_usage ?? selectedHarness?.account_usage ?? null}
      running={view ? !isTerminal(view.chat.status) : false}
      onSubmit={submit}
      onCancel={cancel}
      target={target}
      onAddTarget={openTargetForm}
      workingDirectory={workingDirectory}
      setWorkingDirectory={setWorkingDirectory}
      workingDirectoryValid={workingDirectoryValid}
      setWorkingDirectoryValid={setWorkingDirectoryValid}
      newChat={!view}
    />
  );

  return (
    <div className="app-shell isolate">
      <header className="topbar">
        <div className="flex min-w-0 items-center gap-3">
          <Button type="button" size="icon" variant="ghost" className="lg:hidden" aria-label="Open chats" onClick={() => setRunsOpen(true)}>
            <Menu className="size-4 shrink-0" />
          </Button>
          <ExecutionTargetSelect
            target={displayedTarget}
            connections={savedConnections}
            label={targetName}
            onChange={chooseHeaderTarget}
            onAdd={openTargetForm}
          />
          <div className="chat-header-context">
            <h1>{chatTitle}</h1>
            <span className="chat-header-folder" aria-label={`Folder: ${chatFolder}`} title={chatFolder}>
              <Folder className="size-4 shrink-0" aria-hidden="true" />
              <span>{chatFolder}</span>
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="hidden items-center gap-1.5 text-xs text-zinc-500 sm:flex dark:text-zinc-400">
            <span className={cn("size-1.5 rounded-full", connection === "live" ? "bg-emerald-500" : "bg-zinc-300 dark:bg-zinc-600")} />
            {connection === "live" ? "Stream live" : connection === "connecting" ? "Connecting" : "Idle"}
          </span>
          <Button type="button" size="icon" variant="ghost" className="xl:hidden" aria-label="Open activity" onClick={() => setActivityOpen(true)}>
            <PanelRight className="size-4 shrink-0" />
          </Button>
        </div>
      </header>

      {actionError ? (
        <div className="error-banner" role="alert">
          <CircleAlert className="size-4 shrink-0" />
          <span className="min-w-0 flex-1">{actionError}</span>
          <Button type="button" size="compact" variant="ghost" onClick={() => setActionError(null)}>Dismiss</Button>
        </div>
      ) : null}

      <main className="workspace-grid">
        <aside className="runs-panel hidden lg:flex" aria-label="Chats">
          <ChatsPanel chats={chats} selectedId={selectedId} onSelect={selectChat} onNew={startNewChat} />
        </aside>

        <section className="conversation-panel min-w-0">
          {view ? (
            <>
              <Conversation
                view={view}
                pendingApproval={pendingApproval ?? null}
                pendingQuestion={pendingQuestion ?? null}
                onApproval={resolveApproval}
                onQuestion={resolveQuestion}
              />
              {composer}
            </>
          ) : targetFormOpen ? (
            <Onboarding
              allowLocal={false}
              initialTarget={target}
              initialInventory={inventory}
              onCancel={() => setTargetFormOpen(false)}
              onContinue={({ target: nextTarget, inventory: nextInventory, provider: nextProvider }) => {
                setTarget(nextTarget);
                setInventory(nextInventory);
                setInventoryConnectionKey(nextInventory ? connectionKey(nextTarget) : null);
                setInventoryAttemptedKey(nextInventory ? connectionKey(nextTarget) : null);
                setProvider(nextProvider);
                startNewChat();
                void loadSavedConnections();
              }}
            />
          ) : (
            <div className="new-chat-start">
              <div className="new-chat-copy">
                <p className="eyebrow">New chat</p>
                <h2>Choose a folder and start chatting.</h2>
                <p>Commands will run on {targetName}. Select the exact working directory for this conversation.</p>
              </div>
              {composer}
            </div>
          )}
        </section>

        <aside className="activity-panel hidden xl:flex" aria-label="Activity">
          <ActivityPanel events={view?.events ?? []} />
        </aside>
      </main>

      <Drawer open={runsOpen} onOpenChange={setRunsOpen} title="Chats" side="left">
        <ChatsPanel chats={chats} selectedId={selectedId} onSelect={selectChat} onNew={startNewChat} />
      </Drawer>
      <Drawer open={activityOpen} onOpenChange={setActivityOpen} title="Activity" side="right">
        <ActivityPanel events={view?.events ?? []} />
      </Drawer>

    </div>
  );
}

type ProviderChoice = "claude" | "codex" | "opencode";
type Harness = HarnessInventory["harnesses"][number];

const harnessStatusCopy: Record<HarnessStatus, string> = {
  ready: "Ready",
  not_installed: "Not installed",
  incompatible: "Incompatible",
  unavailable: "Unavailable",
};

const harnessTone: Record<HarnessStatus, "success" | "neutral" | "warning" | "danger"> = {
  ready: "success",
  not_installed: "neutral",
  incompatible: "warning",
  unavailable: "danger",
};

function providerChoice(provider: "claude" | "codex" | "open_code"): ProviderChoice {
  return provider === "open_code" ? "opencode" : provider;
}

function providerName(provider: "claude" | "codex" | "open_code") {
  if (provider === "claude") return "Claude Code";
  if (provider === "codex") return "Codex";
  return "OpenCode";
}

function modelLabel(provider: "claude" | "codex" | "open_code", model: { label: string; description: string | null }) {
  if (provider !== "claude" || !model.description) return model.label;
  const leading = model.description.split(" · ")[0]?.split(" with ")[0]?.trim();
  return leading && /\d/.test(leading) ? leading : model.label;
}

function persistedModelLabel(provider: ProviderChoice, model: string) {
  if (provider === "claude") {
    const match = model.match(/^claude-(haiku|sonnet|opus)-(\d+(?:\.\d+)?)/i);
    if (match) return `${match[1][0].toUpperCase()}${match[1].slice(1)} ${match[2]}`;
  }
  return model;
}

function ProviderLogo({ provider, className }: { provider: ProviderChoice | "open_code"; className?: string }) {
  const normalized = provider === "open_code" ? "opencode" : provider;
  const Icon = normalized === "claude" ? ClaudeIcon : normalized === "codex" ? CodexIcon : OpenCodeIcon;
  return <Icon size="1em" className={cn("provider-logo", className)} aria-hidden="true" />;
}

function Onboarding({
  allowLocal = true,
  initialTarget,
  initialInventory,
  onCancel,
  onContinue,
}: {
  allowLocal?: boolean;
  initialTarget: ExecutionTarget;
  initialInventory: HarnessInventory | null;
  onCancel?: () => void;
  onContinue: (selection: {
    target: ExecutionTarget;
    inventory: HarnessInventory | null;
    provider: ProviderChoice;
  }) => void;
}) {
  const [kind, setKind] = useState<ExecutionTarget["kind"]>(
    allowLocal || initialTarget.kind !== "local" ? initialTarget.kind : "ssh",
  );
  const [host, setHost] = useState(initialTarget.kind === "ssh" ? initialTarget.host : "");
  const [user, setUser] = useState(initialTarget.kind === "ssh" ? initialTarget.user ?? "" : "");
  const [port, setPort] = useState(initialTarget.kind === "ssh" ? String(initialTarget.port ?? 22) : "22");
  const [sshAuthKind, setSshAuthKind] = useState<"agent" | "identity_file" | "password">(
    initialTarget.kind === "ssh" ? initialTarget.authentication.kind : "agent",
  );
  const [identityFile, setIdentityFile] = useState(
    initialTarget.kind === "ssh" && initialTarget.authentication.kind === "identity_file"
      ? initialTarget.authentication.path
      : "",
  );
  const [password, setPassword] = useState(
    initialTarget.kind === "ssh" && initialTarget.authentication.kind === "password"
      ? initialTarget.authentication.password
      : "",
  );
  const [knownHostsFile, setKnownHostsFile] = useState(initialTarget.kind === "ssh" ? initialTarget.known_hosts_file ?? "" : "");
  const [acceptNewHostKey, setAcceptNewHostKey] = useState(initialTarget.kind === "ssh" ? initialTarget.accept_new_host_key : false);
  const [savedConnections, setSavedConnections] = useState<SshConnectionSummary[]>([]);
  const [savedConnectionId, setSavedConnectionId] = useState(initialTarget.kind === "ssh" ? initialTarget.connection_id ?? "" : "");
  const [connectionLabel, setConnectionLabel] = useState("");
  const [savingConnection, setSavingConnection] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [baseUrl, setBaseUrl] = useState(initialTarget.kind === "temps_sandbox" ? initialTarget.base_url : "http://127.0.0.1:3000");
  const [sandboxId, setSandboxId] = useState(initialTarget.kind === "temps_sandbox" ? initialTarget.sandbox_id : "");
  const [authKind, setAuthKind] = useState<"bearer" | "session_cookie">(
    initialTarget.kind === "temps_sandbox" ? initialTarget.auth.kind : "bearer",
  );
  const [credential, setCredential] = useState("");
  const [inventory, setInventory] = useState<HarnessInventory | null>(
    allowLocal || initialTarget.kind !== "local" ? initialInventory : null,
  );
  const [probing, setProbing] = useState(false);
  const [probeError, setProbeError] = useState<string | null>(null);
  const [selectedProvider, setSelectedProvider] = useState<ProviderChoice>("claude");
  const sshHostInput = useRef<HTMLInputElement>(null);
  const sshUserInput = useRef<HTMLInputElement>(null);
  const sshPortInput = useRef<HTMLInputElement>(null);
  const sshIdentityInput = useRef<HTMLInputElement>(null);
  const sshPasswordInput = useRef<HTMLInputElement>(null);
  const sshKnownHostsInput = useRef<HTMLInputElement>(null);

  const buildTarget = useCallback((): ExecutionTarget | null => {
    if (kind === "local") return { kind: "local" };
    if (kind === "ssh") {
      const currentHost = sshHostInput.current?.value ?? host;
      const currentUser = sshUserInput.current?.value ?? user;
      const currentPort = sshPortInput.current?.value ?? port;
      const currentIdentityFile = sshIdentityInput.current?.value ?? identityFile;
      const currentPassword = sshPasswordInput.current?.value ?? password;
      const currentKnownHostsFile = sshKnownHostsInput.current?.value ?? knownHostsFile;
      if (!currentHost.trim()) return null;
      if (sshAuthKind === "password" && (!currentUser.trim() || (!currentPassword && !savedConnectionId))) return null;
      if (sshAuthKind === "identity_file" && !currentIdentityFile.trim()) return null;
      const parsedPort = Number(currentPort);
      return {
        kind: "ssh",
        connection_id: savedConnectionId || null,
        host: currentHost.trim(),
        user: currentUser.trim() || null,
        port: Number.isInteger(parsedPort) && parsedPort > 0 && parsedPort <= 65535 ? parsedPort : null,
        authentication: sshAuthKind === "password"
          ? { kind: "password", password: currentPassword }
          : sshAuthKind === "identity_file"
            ? { kind: "identity_file", path: currentIdentityFile.trim() }
            : { kind: "agent" },
        known_hosts_file: currentKnownHostsFile.trim() || null,
        accept_new_host_key: acceptNewHostKey,
      };
    }
    if (!baseUrl.trim() || !sandboxId.trim() || !credential) return null;
    return {
      kind: "temps_sandbox",
      base_url: baseUrl.trim(),
      sandbox_id: sandboxId.trim(),
      auth: authKind === "bearer" ? { kind: "bearer", token: credential } : { kind: "session_cookie", cookie: credential },
    };
  }, [acceptNewHostKey, authKind, baseUrl, credential, host, identityFile, kind, knownHostsFile, password, port, sandboxId, savedConnectionId, sshAuthKind, user]);

  const loadSavedConnections = useCallback(async () => {
    const response = await fetch("/api/ssh-connections");
    const payload = await response.json();
    if (!response.ok) throw new Error(String(payload.error ?? "Could not load saved SSH connections."));
    setSavedConnections(payload as SshConnectionSummary[]);
  }, []);

  useEffect(() => {
    void loadSavedConnections().catch((error) => setConnectionError(error instanceof Error ? error.message : "Could not load saved SSH connections."));
  }, [loadSavedConnections]);

  function chooseSavedConnection(connection: SshConnectionSummary) {
    setSavedConnectionId(connection.id);
    setConnectionLabel(connection.label);
    setHost(connection.host);
    setUser(connection.user ?? "");
    setPort(String(connection.port ?? 22));
    setSshAuthKind(connection.authentication);
    setIdentityFile(connection.identity_file ?? "");
    setPassword("");
    setKnownHostsFile(connection.known_hosts_file ?? "");
    setAcceptNewHostKey(connection.accept_new_host_key);
    setInventory(null);
    setProbeError(null);
    setConnectionError(null);
  }

  function editConnectionField(update: () => void) {
    setSavedConnectionId("");
    update();
    setInventory(null);
    setProbeError(null);
  }

  async function saveConnection() {
    const target = buildTarget();
    if (!target || target.kind !== "ssh" || !connectionLabel.trim()) {
      setConnectionError("Name and complete the SSH connection before saving it.");
      return;
    }
    setSavingConnection(true);
    setConnectionError(null);
    try {
      const response = await fetch("/api/ssh-connections", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ label: connectionLabel.trim(), target: { ...target, connection_id: null } }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? "Could not save the SSH connection."));
      const saved = payload as SshConnectionSummary;
      setSavedConnections((current) => [saved, ...current.filter((item) => item.id !== saved.id)]);
      setSavedConnectionId(saved.id);
      setPassword("");
    } catch (error) {
      setConnectionError(error instanceof Error ? error.message : "Could not save the SSH connection.");
    } finally {
      setSavingConnection(false);
    }
  }

  async function deleteConnection() {
    if (!savedConnectionId) return;
    setSavingConnection(true);
    setConnectionError(null);
    try {
      const response = await fetch(`/api/ssh-connections/${encodeURIComponent(savedConnectionId)}`, { method: "DELETE" });
      if (!response.ok) throw new Error(`Could not remove the saved connection (${response.status}).`);
      setSavedConnections((current) => current.filter((item) => item.id !== savedConnectionId));
      setSavedConnectionId("");
      setConnectionLabel("");
    } catch (error) {
      setConnectionError(error instanceof Error ? error.message : "Could not remove the saved connection.");
    } finally {
      setSavingConnection(false);
    }
  }

  const probe = useCallback(async (explicitTarget?: ExecutionTarget) => {
    const target = explicitTarget ?? buildTarget();
    if (!target) {
      setProbeError("Complete the target configuration before checking harnesses.");
      return;
    }
    setProbing(true);
    setProbeError(null);
    try {
      const response = await fetch("/api/discovery", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ target }),
      });
      const payload = await response.json();
      if (!response.ok) {
        const typedKind = typeof payload.kind === "string" ? ` (${payload.kind.replaceAll("_", " ")})` : "";
        throw new Error(`${String(payload.error ?? "Could not inspect this target.")}${typedKind}`);
      }
      const next = payload as HarnessInventory;
      setInventory(next);
      const firstReady = next.harnesses.find((harness) => harness.status === "ready");
      if (firstReady) setSelectedProvider(providerChoice(firstReady.provider));
    } catch (error) {
      setInventory(null);
      setProbeError(error instanceof Error ? error.message : "Could not inspect this target.");
    } finally {
      setProbing(false);
    }
  }, [buildTarget]);

  useEffect(() => {
    if (kind === "local" && !inventory) void probe({ kind: "local" });
  }, [inventory, kind, probe]);

  function changeKind(next: ExecutionTarget["kind"]) {
    setKind(next);
    setInventory(next === initialTarget.kind ? initialInventory : null);
    setProbeError(null);
  }

  function changeSshAuthentication(next: typeof sshAuthKind) {
    setSavedConnectionId("");
    setSshAuthKind(next);
    if (next !== "password") setPassword("");
    if (next !== "identity_file") setIdentityFile("");
    setInventory(null);
    setProbeError(null);
  }

  const selectedReady = inventory?.harnesses.some(
    (harness) => providerChoice(harness.provider) === selectedProvider && harness.status === "ready",
  ) ?? false;
  const readyHarnesses = inventory?.harnesses.filter((harness) => harness.status === "ready") ?? [];
  const configuredTarget = buildTarget();
  const targetChoices = ([
    ["local", "Local", Monitor],
    ["ssh", "SSH", Network],
    ["temps_sandbox", "Temps sandbox", Server],
  ] as const).filter(([value]) => allowLocal || value !== "local");

  return (
    <div className="onboarding-shell">
      <div className="onboarding-copy">
        <p className="eyebrow">{allowLocal ? "Runtime onboarding" : "Execution target"}</p>
        <h1>{allowLocal ? "Choose where the harness lives." : "Add an execution target."}</h1>
        <p>{allowLocal
          ? "The server probes the selected execution boundary. Local checks stay local; SSH and sandbox checks run inside their target."
          : "Configure an SSH host or Temps sandbox, verify its harnesses, and save it for future chats."}</p>
      </div>

      <div className="target-tabs" role="tablist" aria-label="Execution target">
        {targetChoices.map(([value, label, Icon]) => (
          <button key={value} type="button" role="tab" aria-selected={kind === value} className={cn("target-tab", kind === value && "target-tab-active")} onClick={() => changeKind(value)}>
            <Icon className="size-4" />
            <span>{label}</span>
          </button>
        ))}
      </div>

      {kind === "ssh" ? (
        <section className="target-form" aria-label="SSH configuration">
          <div className="col-span-full rounded-lg border border-zinc-200 bg-zinc-50/70 p-3 dark:border-zinc-800 dark:bg-zinc-900/50">
            <div className="flex flex-wrap items-end gap-2">
              <label className="min-w-52 flex-1 text-xs font-medium text-zinc-700 dark:text-zinc-300">
                Saved connection
                <select
                  className="mt-1 block h-10 w-full rounded-md border border-zinc-200 bg-white px-3 text-sm dark:border-zinc-700 dark:bg-zinc-950"
                  onChange={(event) => {
                    const connection = savedConnections.find((item) => item.id === event.target.value);
                    if (connection) chooseSavedConnection(connection);
                    else {
                      setSavedConnectionId("");
                      setConnectionLabel("");
                    }
                  }}
                  value={savedConnectionId}
                >
                  <option value="">New connection</option>
                  {savedConnections.map((connection) => <option key={connection.id} value={connection.id}>{connection.label} · {connection.user ? `${connection.user}@` : ""}{connection.host}</option>)}
                </select>
              </label>
              {savedConnectionId ? (
                <Button disabled={savingConnection} onClick={() => void deleteConnection()} size="compact" type="button" variant="danger">
                  <Trash2 className="size-3.5" /> Remove saved connection
                </Button>
              ) : null}
            </div>
            {connectionError ? <p className="mt-2 text-xs text-red-600 dark:text-red-400" role="alert">{connectionError}</p> : null}
          </div>
          <Field label="Host"><input ref={sshHostInput} name="ssh_host" value={host} onChange={(event) => editConnectionField(() => setHost(event.target.value))} placeholder="agent.example.com" /></Field>
          <Field label="User"><input ref={sshUserInput} name="ssh_user" value={user} onChange={(event) => editConnectionField(() => setUser(event.target.value))} placeholder={sshAuthKind === "password" ? "Required for password" : "Optional"} autoComplete="username" /></Field>
          <Field label="Port"><input ref={sshPortInput} name="ssh_port" inputMode="numeric" value={port} onChange={(event) => editConnectionField(() => setPort(event.target.value))} /></Field>
          <Field label="Authentication">
            <select name="ssh_authentication" value={sshAuthKind} onChange={(event) => changeSshAuthentication(event.target.value as typeof sshAuthKind)}>
              <option value="agent">SSH agent</option>
              <option value="identity_file">Identity file</option>
              <option value="password">Password</option>
            </select>
          </Field>
          {sshAuthKind === "identity_file" ? (
            <Field label="Identity file"><input ref={sshIdentityInput} name="ssh_identity_file" value={identityFile} onChange={(event) => editConnectionField(() => setIdentityFile(event.target.value))} placeholder="Server-side private key path" /></Field>
          ) : null}
          {sshAuthKind === "password" ? (
            <Field label="Password"><input ref={sshPasswordInput} name="ssh_password" type="password" value={password} onInput={(event) => editConnectionField(() => setPassword(event.currentTarget.value))} onChange={(event) => editConnectionField(() => setPassword(event.target.value))} autoComplete="current-password" placeholder={savedConnectionId ? "Stored encrypted on the server" : "Encrypted when saved"} /></Field>
          ) : null}
          <Field label="Known hosts file"><input ref={sshKnownHostsInput} name="ssh_known_hosts_file" value={knownHostsFile} onChange={(event) => editConnectionField(() => setKnownHostsFile(event.target.value))} placeholder="OpenSSH default" /></Field>
          <label className="check-field"><input name="ssh_accept_new_host_key" type="checkbox" checked={acceptNewHostKey} onChange={(event) => editConnectionField(() => setAcceptNewHostKey(event.target.checked))} /><span>Trust a previously unseen host key</span></label>
          {!savedConnectionId ? (
            <div className="col-span-full flex flex-wrap items-end gap-2 rounded-lg border border-dashed border-zinc-300 p-3 dark:border-zinc-700">
              <label className="min-w-52 flex-1 text-xs font-medium text-zinc-700 dark:text-zinc-300">
                Connection name
                <input className="mt-1 block h-10 w-full rounded-md border border-zinc-200 bg-white px-3 text-sm dark:border-zinc-700 dark:bg-zinc-950" onChange={(event) => setConnectionLabel(event.target.value)} placeholder="Studio Mac" value={connectionLabel} />
              </label>
              <Button disabled={savingConnection || !connectionLabel.trim() || !configuredTarget} onClick={() => void saveConnection()} size="compact" type="button" variant="secondary">
                {savingConnection ? <RefreshCw className="size-3.5 animate-spin motion-reduce:animate-none" /> : <Plus className="size-3.5" />}
                {savingConnection ? "Saving" : "Save connection"}
              </Button>
            </div>
          ) : null}
        </section>
      ) : null}

      {kind === "temps_sandbox" ? (
        <section className="target-form" aria-label="Temps sandbox configuration">
          <Field label="API base URL"><input name="sandbox_base_url" value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} /></Field>
          <Field label="Sandbox ID"><input name="sandbox_id" value={sandboxId} onChange={(event) => setSandboxId(event.target.value)} placeholder="sandbox_…" /></Field>
          <Field label="Authentication"><select name="sandbox_authentication" value={authKind} onChange={(event) => setAuthKind(event.target.value as typeof authKind)}><option value="bearer">Bearer token</option><option value="session_cookie">Session cookie</option></select></Field>
          <Field label={authKind === "bearer" ? "Bearer token" : "Session cookie"}><input name="sandbox_credential" type="password" value={credential} onChange={(event) => setCredential(event.target.value)} autoComplete="off" placeholder="Kept in server memory for this run" /></Field>
        </section>
      ) : null}

      <section className="inventory-card" aria-live="polite" data-testid="harness-inventory">
        <div className="inventory-heading">
          <div>
            <p className="eyebrow">Transport-aware inventory</p>
            <h2>{inventory ? `${inventory.transport} harnesses` : "Harness readiness"}</h2>
          </div>
          <Button type="button" variant="secondary" onClick={() => void probe()} disabled={probing}>
            <RefreshCw className={cn("size-4", probing && "animate-spin motion-reduce:animate-none")} />
            {probing ? "Checking target" : "Check again"}
          </Button>
        </div>

        {probeError ? <div className="probe-error" role="alert"><CircleAlert className="size-4" /><span>{probeError}</span></div> : null}
        {inventory ? (
          readyHarnesses.length ? (
            <div className="harness-list">
              {inventory.harnesses.map((harness) => {
              const choice = providerChoice(harness.provider);
              return (
                <button
                  type="button"
                  key={harness.provider}
                  className={cn("harness-row", selectedProvider === choice && "harness-row-selected")}
                  onClick={() => setSelectedProvider(choice)}
                  disabled={harness.status !== "ready"}
                >
                  <span className="harness-radio" aria-hidden="true">{selectedProvider === choice && harness.status === "ready" ? <Check className="size-3" /> : null}</span>
                  <ProviderLogo provider={harness.provider} className="text-lg" />
                  <span className="min-w-0 flex-1 text-left">
                    <span className="flex flex-wrap items-center gap-2"><strong>{providerName(harness.provider)}</strong><Badge tone={harnessTone[harness.status]}>{harnessStatusCopy[harness.status]}</Badge></span>
                    <span className="mt-1 block truncate text-xs text-zinc-600 dark:text-zinc-300">
                      {harness.readiness?.version ?? harness.error?.message ?? harness.readiness?.detail ?? "Executable was not found on this target."}
                    </span>
                    {harness.limitations.length ? <span className="mt-1 block text-xs text-amber-700 dark:text-amber-400">{harness.limitations.map((item) => item.replaceAll("_", " ")).join(" · ")}</span> : null}
                  </span>
                  <span className="hidden text-right text-[0.6875rem] leading-5 text-zinc-600 sm:block dark:text-zinc-300">
                    {harness.permissions.live_approvals ? "Live approvals" : "No live approvals"}<br />
                    {harness.models.models.length ? `${harness.models.models.length} models` : harness.models.status === "failed" ? "Catalog failed" : "Harness model"}
                  </span>
                </button>
              );
              })}
            </div>
          ) : (
            <div className="harness-empty-state" role="status">
              <Server className="size-5" aria-hidden="true" />
              <div>
                <h3>No harnesses available on this target</h3>
                <p>Install and authenticate Claude Code, Codex, or OpenCode on this target, then check again.</p>
              </div>
            </div>
          )
        ) : probing ? <div className="inventory-loading"><RefreshCw className="size-4 animate-spin motion-reduce:animate-none" /> Connecting to the selected target…</div> : null}
      </section>

      <div className="onboarding-actions">
        {onCancel ? <Button type="button" variant="ghost" onClick={onCancel}>Back to new chat</Button> : null}
        <Button type="button" variant="primary" disabled={!configuredTarget || !selectedReady} onClick={() => configuredTarget && onContinue({ target: configuredTarget, inventory, provider: selectedProvider })}>
          Use {selectedProvider === "opencode" ? "OpenCode" : selectedProvider === "claude" ? "Claude Code" : "Codex"}
          <ChevronRight className="size-4" />
        </Button>
      </div>
    </div>
  );
}

function ChatsPanel({ chats, selectedId, onSelect, onNew }: { chats: ChatSnapshot[]; selectedId: string | null; onSelect: (id: string) => void; onNew: () => void }) {
  return (
    <div className="flex h-full min-h-0 w-full flex-col">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Persistent</p>
          <h2 className="panel-title">Chats</h2>
        </div>
        <div className="flex items-center gap-2">
          <Badge>{chats.length}</Badge>
          <Button type="button" size="icon" variant="ghost" onClick={onNew} aria-label="New chat"><Plus className="size-4" /></Button>
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-2">
        {chats.length ? chats.map((chat) => (
          <button
            type="button"
            key={chat.id}
            onClick={() => onSelect(chat.id)}
            className={cn("run-row", selectedId === chat.id && "run-row-selected")}
          >
            <div className="flex min-w-0 items-start gap-2.5">
              <ProviderLogo provider={chat.provider} className="mt-0.5 text-base text-zinc-500" />
              <div className="min-w-0 flex-1">
                <div className="flex min-w-0 items-center justify-between gap-2">
                  <span className="truncate text-left text-xs font-semibold text-zinc-800 dark:text-zinc-200">{chat.title}</span>
                  <span className="shrink-0 text-[0.6875rem] tabular-nums text-zinc-600 dark:text-zinc-400">{formatTime(chat.updated_at_ms)}</span>
                </div>
                <p className="mt-1 truncate text-left font-mono text-[0.6875rem] text-zinc-400">{chat.id}</p>
                <Badge tone={statusTone[chat.status]} className="mt-2">{statusCopy[chat.status]}</Badge>
              </div>
            </div>
          </button>
        )) : (
          <div className="empty-state">
            <Clock3 className="size-4" />
            <p>No chats yet</p>
            <span>Send a message to create a persistent conversation.</span>
          </div>
        )}
      </div>
    </div>
  );
}

function Conversation({
  view,
  pendingApproval,
  pendingQuestion,
  onApproval,
  onQuestion,
}: {
  view: ChatView | null;
  pendingApproval: ApprovalRequest | null;
  pendingQuestion: QuestionRequest | null;
  onApproval: (id: string, decision: "allow" | "deny") => void;
  onQuestion: (id: string, answers: Record<string, string>) => Promise<void>;
}) {
  const latestStart = view?.events.reduce(
    (latest, event, index) => event.kind === "message_started" ? index : latest,
    -1,
  ) ?? -1;
  const currentEvents = view?.events.slice(Math.max(0, latestStart)) ?? [];
  const plan = currentEvents.find((event) => event.kind === "plan_created");
  const reasoning = currentEvents
    .filter((event) => eventType(event) === "reasoning_delta")
    .map((event) => String(event.payload.text ?? ""))
    .join("");
  const currentTools = normalizedToolCalls(currentEvents);
  const persistedSubagents = useMemo(() => claudeSubagentsByMessage(view?.events ?? []), [view?.events]);
  const timeline = useMemo(() => view ? conversationTimeline(view) : [], [view]);
  const activeCompaction = useMemo(() => compactionPhase(view?.events ?? []), [view?.events]);
  const working = view
    ? !isTerminal(view.chat.status) && view.chat.status !== "approval_needed" && view.chat.status !== "input_needed"
    : false;
  const runningTool = [...currentTools].reverse().find((tool) => tool.state === "input-available");

  if (!view) {
    return (
      <AIConversation className="conversation-scroll" aria-label="Conversation history">
        <ConversationContent className="h-full">
          <ConversationEmptyState
            description="Messages, normalized activity, approvals, and provider-session continuity are saved together."
            icon={<Code2 className="size-5 text-emerald-700 dark:text-emerald-400" />}
            title="Start a persistent agent chat"
          />
        </ConversationContent>
      </AIConversation>
    );
  }

  return (
    <AIConversation className="conversation-scroll" data-testid="conversation" aria-label="Conversation history">
      <ConversationContent className="conversation-inner">
        <div className="conversation-meta">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <Badge tone={statusTone[view.chat.status]} data-testid="chat-status">{statusCopy[view.chat.status]}</Badge>
            </div>
            <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">
              {view.chat.provider} CLI · {view.chat.target}
              {view.chat.model ? ` · ${view.chat.model}` : ""}
              {view.chat.reasoning ? ` · ${view.chat.reasoning}` : ""}
              {Object.values(view.chat.harness_options).length ? ` · ${Object.values(view.chat.harness_options).join(" · ")}` : ` · ${view.chat.permission.replaceAll("_", " ")}`}
            </p>
          </div>
          {view.chat.session_id ? <code className="truncate text-[0.6875rem] text-zinc-400">{view.chat.session_id}</code> : null}
        </div>

        {timeline.map((item) => {
          if (item.kind === "tool") return <ToolChips key={item.key} tools={[item.tool]} />;
          const message = item.message;
          return (
            <Fragment key={item.key}>
            <AIMessage className="runtime-message" from={message.role === "user" ? "user" : "assistant"}>
              <div className="runtime-message-label">
                {message.role === "assistant" ? <ProviderLogo provider={view.chat.provider} className="text-base" /> : <ChevronRight className="size-4" />}
                <span>{message.role === "user" ? "You" : message.role === "assistant" ? "Assistant" : "System"}</span>
              </div>
              <MessageContent className="runtime-message-content">
                <MessageResponse
                  data-testid={message.role === "assistant" ? "assistant-message" : "user-message"}
                  markdown={message.role === "assistant"}
                >
                  {message.content}
                </MessageResponse>
                {message.attachments?.length ? (
                  <div className="mt-3 flex flex-wrap gap-2" aria-label="Message attachments">
                    {message.attachments.map((attachment) => (
                      <span className="inline-flex max-w-full items-center gap-1.5 rounded-md border border-zinc-200 bg-zinc-50 px-2 py-1 text-xs text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-300" key={attachment.id} title={attachment.uri}>
                        <Paperclip className="size-3.5 shrink-0" />
                        <span className="truncate">{attachment.name}</span>
                      </span>
                    ))}
                  </div>
                ) : null}
              </MessageContent>
            </AIMessage>

            {message.role === "user" ? (
              view.chat.provider === "claude" ? (
                <ClaudeSubagents agents={persistedSubagents.get(message.sequence) ?? []} />
              ) : null
            ) : null}
            </Fragment>
          );
        })}

        {plan ? (
          <section className="plan-card" data-testid="plan-card">
            <div className="flex items-center gap-2 text-emerald-800 dark:text-emerald-300">
              <ShieldCheck className="size-4 shrink-0" />
              <h2 className="text-sm font-semibold">{String(plan.payload.title)}</h2>
            </div>
            <ol className="mt-3 space-y-2">
              {(plan.payload.steps as string[]).map((step, index) => (
                <li key={step} className="flex gap-3 text-xs leading-5 text-zinc-600 dark:text-zinc-300">
                  <span className="font-mono text-emerald-700 dark:text-emerald-400">0{index + 1}</span>
                  <span>{step}</span>
                </li>
              ))}
            </ol>
          </section>
        ) : null}

        {reasoning ? (
          <Reasoning className="runtime-reasoning" defaultOpen={working} isStreaming={working}>
            <ReasoningTrigger />
            <ReasoningContent>{reasoning}</ReasoningContent>
          </Reasoning>
        ) : null}

        {pendingApproval ? (
          <section className="approval-card" data-testid="approval-card">
            <div className="flex items-start gap-3">
              <CircleAlert className="mt-0.5 size-4 shrink-0 text-amber-700 dark:text-amber-400" />
              <div className="min-w-0 flex-1">
                <Badge tone="warning">
                  {pendingApproval.type === "plan_approval_requested" ? "Plan approval required" : "Approval required"}
                </Badge>
                <h2 className="mt-3 text-sm font-semibold text-zinc-950 dark:text-zinc-50">
                  {pendingApproval.type === "plan_approval_requested" ? "Claude proposed a plan" : pendingApproval.tool_name}
                </h2>
                <p className="mt-1 text-xs leading-5 text-zinc-600 dark:text-zinc-300">
                  {pendingApproval.description ?? (pendingApproval.type === "plan_approval_requested"
                    ? "Accept the plan to let Claude leave plan mode and continue, or reject it with feedback."
                    : "Review this operation before allowing it to continue.")}
                </p>
                <pre className="command-preview">{JSON.stringify(pendingApproval.input, null, 2)}</pre>
                <div className="mt-4 flex flex-wrap gap-2">
                  <Button type="button" variant="secondary" onClick={() => onApproval(pendingApproval.id, "allow")}>
                    <Check className="size-4 shrink-0" />
                    {pendingApproval.type === "plan_approval_requested" ? "Accept plan" : "Allow command"}
                  </Button>
                  <Button type="button" variant="danger" onClick={() => onApproval(pendingApproval.id, "deny")}>
                    <X className="size-4 shrink-0" />
                    {pendingApproval.type === "plan_approval_requested" ? "Reject plan" : "Deny"}
                  </Button>
                </div>
              </div>
            </div>
          </section>
        ) : null}

        {pendingQuestion ? (
          <QuestionCard key={pendingQuestion.id} request={pendingQuestion} onAnswer={onQuestion} />
        ) : null}

        {view.chat.draft ? (
          <AIMessage className="runtime-message" from="assistant">
            <div className="runtime-message-label"><ProviderLogo provider={view.chat.provider} className="text-base" /><span>Assistant · streaming</span></div>
            <MessageContent className="runtime-message-content">
              <MessageResponse data-testid="assistant-draft" isAnimating markdown>{view.chat.draft}</MessageResponse>
            </MessageContent>
          </AIMessage>
        ) : null}

        {working ? (
          <div className="working-state" data-testid="working-state">
            <LoadingState label={activeCompaction === "queued"
              ? "Waiting to compact context"
              : activeCompaction === "running"
                ? "Compacting context"
                : runningTool
                  ? `Working · ${runningTool.name}`
                  : view.chat.status === "queued"
                    ? "Waiting for provider"
                    : view.chat.status === "cancelling"
                      ? "Stopping agent"
                      : "Agent is working"} />
          </div>
        ) : null}

        {view.chat.error ? (
          <div className="failure-card" role="alert">
            <CircleAlert className="size-4 shrink-0" />
            <div>
              <p className="text-sm font-semibold">Response failed</p>
              <p className="mt-1 text-xs leading-5">{view.chat.error}</p>
            </div>
          </div>
        ) : null}
      </ConversationContent>
      <ConversationScrollButton aria-label="Jump to latest message" />
    </AIConversation>
  );
}

function QuestionCard({
  request,
  onAnswer,
}: {
  request: QuestionRequest;
  onAnswer: (id: string, answers: Record<string, string>) => Promise<void>;
}) {
  const nativePrompts = Array.isArray(request.questions)
    ? request.questions
    : request.questions?.questions ?? [];
  const prompts = request.prompts?.length ? request.prompts : nativePrompts;
  const [selections, setSelections] = useState<Record<string, string[]>>({});
  const [custom, setCustom] = useState<Record<string, string>>({});
  const [submittingAnswer, setSubmittingAnswer] = useState(false);

  function toggle(question: string, label: string, multiple: boolean) {
    setCustom((current) => ({ ...current, [question]: "" }));
    setSelections((current) => {
      const selected = current[question] ?? [];
      const next = multiple
        ? selected.includes(label) ? selected.filter((value) => value !== label) : [...selected, label]
        : [label];
      return { ...current, [question]: next };
    });
  }

  const answers = Object.fromEntries(prompts.map((prompt) => {
    const typed = custom[prompt.question]?.trim();
    return [prompt.question, typed || (selections[prompt.question] ?? []).join(", ")];
  }));
  const complete = prompts.length > 0 && prompts.every((prompt) => Boolean(answers[prompt.question]));

  return (
    <section className="question-card" data-testid="question-card">
      <div className="flex items-start gap-3">
        <CircleAlert className="mt-0.5 size-4 shrink-0 text-amber-700 dark:text-amber-400" />
        <div className="min-w-0 flex-1">
          <Badge tone="warning">Answer required</Badge>
          <div className="mt-4 space-y-5">
            {prompts.map((prompt) => (
              <fieldset key={prompt.question} className="min-w-0">
                <legend className="text-sm font-semibold text-zinc-950 dark:text-zinc-50">{prompt.question}</legend>
                {prompt.header ? <p className="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{prompt.header}</p> : null}
                <div className="mt-3 grid gap-2 sm:grid-cols-2">
                  {prompt.options.map((option) => {
                    const selected = (selections[prompt.question] ?? []).includes(option.label);
                    return (
                      <button
                        key={option.label}
                        type="button"
                        aria-pressed={selected}
                        className={cn("question-option", selected && "question-option-selected")}
                        onClick={() => toggle(prompt.question, option.label, prompt.multiSelect)}
                      >
                        <span className="font-medium">{option.label}</span>
                        {option.description ? <span>{option.description}</span> : null}
                      </button>
                    );
                  })}
                </div>
                <label className="mt-2 block">
                  <span className="sr-only">Other answer for {prompt.question}</span>
                  <input
                    className="question-custom-input"
                    type="text"
                    value={custom[prompt.question] ?? ""}
                    onChange={(event) => {
                      setCustom((current) => ({ ...current, [prompt.question]: event.target.value }));
                      if (event.target.value) setSelections((current) => ({ ...current, [prompt.question]: [] }));
                    }}
                    placeholder="Other answer…"
                  />
                </label>
              </fieldset>
            ))}
          </div>
          <div className="mt-4 flex justify-end">
            <Button
              type="button"
              disabled={!complete || submittingAnswer}
              onClick={async () => {
                setSubmittingAnswer(true);
                try { await onAnswer(request.id, answers); } finally { setSubmittingAnswer(false); }
              }}
            >
              {submittingAnswer ? "Sending answer…" : "Send answer"}
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}

interface ComposerProps {
  prompt: string;
  setPrompt: (value: string) => void;
  attachments: ChatAttachment[];
  setAttachments: (value: ChatAttachment[]) => void;
  queuedMessages: QueuedChatMessage[];
  setQueuedMessages: (value: QueuedChatMessage[] | ((current: QueuedChatMessage[]) => QueuedChatMessage[])) => void;
  onUpdateQueued: (message: QueuedChatMessage) => Promise<void>;
  onSendQueuedNow: (message: QueuedChatMessage) => Promise<void>;
  onUploadAttachment: (file: File) => Promise<ChatAttachment>;
  provider: "claude" | "codex" | "opencode";
  setProvider: (value: "claude" | "codex" | "opencode") => void;
  model: string;
  setModel: (value: string) => void;
  reasoning: string;
  setReasoning: (value: string) => void;
  harness: Harness | null;
  inventory: HarnessInventory | null;
  inventoryLoading: boolean;
  inventoryError: string | null;
  onRetryInventory: () => void;
  harnessOptions: Record<string, string>;
  setHarnessOptions: (value: Record<string, string>) => void;
  submitting: boolean;
  compacting: boolean;
  compactAvailable: boolean;
  compactDisabledReason: string | null;
  onCompact: () => Promise<void>;
  usage: Usage | null;
  accountUsage: AccountUsageSnapshot | null;
  running: boolean;
  onSubmit: (event: FormEvent) => void;
  onCancel: () => void;
  target: ExecutionTarget;
  onAddTarget: () => void;
  workingDirectory: string;
  setWorkingDirectory: (value: string) => void;
  workingDirectoryValid: boolean;
  setWorkingDirectoryValid: (valid: boolean) => void;
  newChat: boolean;
}

function createAttachment(): ChatAttachment {
  return {
    id: crypto.randomUUID(),
    name: "",
    uri: "",
    media_type: null,
    metadata: {},
  };
}

function AttachmentFields({ attachments, onChange, onUpload, showActions = true }: {
  attachments: ChatAttachment[];
  onChange: (attachments: ChatAttachment[]) => void;
  onUpload: (file: File) => Promise<ChatAttachment>;
  showActions?: boolean;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  function patchAttachment(id: string, patch: Partial<ChatAttachment>) {
    onChange(attachments.map((attachment) => attachment.id === id ? { ...attachment, ...patch } : attachment));
  }

  async function uploadFiles(files: FileList | null) {
    if (!files?.length) return;
    setUploading(true);
    setUploadError(null);
    const uploaded: ChatAttachment[] = [];
    try {
      for (const file of Array.from(files)) uploaded.push(await onUpload(file));
      onChange([...attachments, ...uploaded]);
    } catch (cause) {
      if (uploaded.length) onChange([...attachments, ...uploaded]);
      setUploadError(cause instanceof Error ? cause.message : "Could not upload the attachment.");
    } finally {
      setUploading(false);
      if (inputRef.current) inputRef.current.value = "";
    }
  }

  return (
    <div className="space-y-2">
      {attachments.map((attachment) => (
        <div className="grid grid-cols-[minmax(0,0.8fr)_minmax(0,1.5fr)_auto] gap-2" key={attachment.id}>
          <input
            aria-label="Attachment name"
            className="h-9 min-w-0 rounded-md border border-zinc-200 bg-white px-3 text-xs dark:border-zinc-700 dark:bg-zinc-900"
            onChange={(event) => patchAttachment(attachment.id, { name: event.target.value })}
            placeholder="Name"
            value={attachment.name}
          />
          <input
            aria-label="Attachment URI"
            className="h-9 min-w-0 rounded-md border border-zinc-200 bg-white px-3 font-mono text-xs dark:border-zinc-700 dark:bg-zinc-900"
            onChange={(event) => patchAttachment(attachment.id, { uri: event.target.value })}
            placeholder="file:///path or https://…"
            value={attachment.uri}
          />
          <Button aria-label={`Remove ${attachment.name || "attachment"}`} onClick={() => onChange(attachments.filter((item) => item.id !== attachment.id))} size="icon" type="button" variant="ghost">
            <Trash2 className="size-4" />
          </Button>
        </div>
      ))}
      {uploadError ? <p className="text-xs text-red-600 dark:text-red-400" role="alert">{uploadError}</p> : null}
      {showActions ? <div className="flex flex-wrap items-center gap-1">
        <input className="sr-only" multiple onChange={(event) => void uploadFiles(event.target.files)} ref={inputRef} type="file" />
        <Button disabled={uploading} onClick={() => inputRef.current?.click()} size="compact" type="button" variant="ghost">
          {uploading ? <RefreshCw className="size-3.5 animate-spin motion-reduce:animate-none" /> : <Paperclip className="size-3.5" />}
          {uploading ? "Uploading" : "Upload files"}
        </Button>
        <Button onClick={() => onChange([...attachments, createAttachment()])} size="compact" type="button" variant="ghost">
        <Paperclip className="size-3.5" /> Attach reference
        </Button>
      </div> : null}
    </div>
  );
}

function QueuedMessageEditor({ message, onChange, onSave, onSendNow, onUpload }: {
  message: QueuedChatMessage;
  onChange: (message: QueuedChatMessage) => void;
  onSave: (message: QueuedChatMessage) => Promise<void>;
  onSendNow: (message: QueuedChatMessage) => Promise<void>;
  onUpload: (file: File) => Promise<ChatAttachment>;
}) {
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function run(action: (message: QueuedChatMessage) => Promise<void>) {
    setSaving(true);
    setError(null);
    try {
      await action(message);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not update this queued message.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="rounded-lg border border-zinc-200 bg-zinc-50/70 p-3 dark:border-zinc-800 dark:bg-zinc-900/60" data-testid="queued-message">
      <div className="mb-2 flex items-center justify-between gap-3">
        <span className="font-mono text-[0.6875rem] font-semibold uppercase tracking-wider text-zinc-500">Queued</span>
        <span className="text-[0.6875rem] text-zinc-500">revision {message.revision}</span>
      </div>
      <textarea
        aria-label="Queued message"
        className="min-h-20 w-full resize-y rounded-md border border-zinc-200 bg-white p-3 text-sm outline-none focus:border-emerald-500 dark:border-zinc-700 dark:bg-zinc-950"
        onChange={(event) => onChange({ ...message, content: event.target.value })}
        value={message.content}
      />
      <div className="mt-2">
        <AttachmentFields attachments={message.attachments} onChange={(next) => onChange({ ...message, attachments: next })} onUpload={onUpload} />
      </div>
      {error ? <p className="mt-2 text-xs text-red-600 dark:text-red-400" role="alert">{error}</p> : null}
      <div className="mt-3 flex justify-end gap-2">
        <Button disabled={saving} onClick={() => void run(onSave)} size="compact" type="button" variant="ghost">
          {saving ? <RefreshCw className="size-3.5 animate-spin motion-reduce:animate-none" /> : <Check className="size-3.5" />} Save
        </Button>
        <Button disabled={saving} onClick={() => void run(onSendNow)} size="compact" type="button" variant="secondary">
          <Play className="size-3.5" /> Send now
        </Button>
      </div>
    </div>
  );
}

function WorkingDirectoryPicker({
  target,
  value,
  onChange,
  valid,
  onValidChange,
  locked,
}: {
  target: ExecutionTarget;
  value: string;
  onChange: (value: string) => void;
  valid: boolean;
  onValidChange: (valid: boolean) => void;
  locked: boolean;
}) {
  const [candidates, setCandidates] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [checked, setChecked] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);

  useEffect(() => {
    if (locked) {
      setCandidates([]);
      setError(null);
      setChecked(false);
      onValidChange(true);
      return;
    }

    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      setLoading(true);
      setChecked(false);
      setError(null);
      void fetch("/api/working-directories", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ target, input: value }),
        signal: controller.signal,
      })
        .then(async (response) => {
          const payload = await response.json();
          if (!response.ok) throw new Error(String(payload.error ?? "Could not inspect folders on this target."));
          const next = payload as WorkingDirectoryCandidates;
          setCandidates(next.directories);
          setActiveIndex(0);
          setChecked(true);
          onValidChange(Boolean(value.trim()) && next.exact_match);
        })
        .catch((cause) => {
          if (cause instanceof DOMException && cause.name === "AbortError") return;
          setCandidates([]);
          setChecked(false);
          onValidChange(false);
          setError(cause instanceof Error ? cause.message : "Could not inspect folders on this target.");
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, 350);

    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [locked, onChange, onValidChange, target, value]);

  function choose(directory: string) {
    onChange(directory);
    onValidChange(true);
    setOpen(false);
  }

  return (
    <div className="chat-directory-picker">
      <div className="chat-directory-heading">
        <label htmlFor="chat-working-directory">Folder for this chat</label>
        <span>{locked ? "Saved with the provider session" : "Commands run from this folder"}</span>
      </div>
      <div className="directory-combobox">
        <Folder className="size-4 shrink-0" aria-hidden="true" />
        <input
          id="chat-working-directory"
          name="working_directory"
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={!locked && open}
          aria-controls="chat-directory-options"
          aria-activedescendant={!locked && open && candidates.length ? `chat-directory-option-${activeIndex}` : undefined}
          aria-describedby={error ? "chat-directory-error" : undefined}
          readOnly={locked}
          value={value}
          onChange={(event) => {
            onChange(event.target.value);
            onValidChange(false);
            setOpen(true);
          }}
          onFocus={() => {
            if (!locked) setOpen(true);
          }}
          onBlur={() => window.setTimeout(() => setOpen(false), 0)}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown" && candidates.length) {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((current) => Math.min(current + 1, candidates.length - 1));
            } else if (event.key === "ArrowUp" && candidates.length) {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((current) => Math.max(current - 1, 0));
            } else if (event.key === "Enter") {
              event.preventDefault();
              if (open && candidates[activeIndex]) choose(candidates[activeIndex]);
              else setOpen(false);
            } else if (event.key === "Escape") {
              setOpen(false);
            }
          }}
          placeholder="Choose home or project folder"
          autoComplete="off"
        />
        {loading ? <RefreshCw className="size-4 shrink-0 animate-spin motion-reduce:animate-none" aria-label="Loading folders" /> : valid ? <Check className="size-4 shrink-0 text-emerald-600 dark:text-emerald-400" aria-label="Folder exists" /> : null}
      </div>
      {!locked && open && !error ? (
        <div className="directory-options chat-directory-options" id="chat-directory-options" role="listbox">
          {loading && !candidates.length ? <p>Loading folders…</p> : null}
          {candidates.map((directory, index) => (
            <button
              id={`chat-directory-option-${index}`}
              type="button"
              role="option"
              aria-selected={value === directory}
              className={cn(index === activeIndex && "directory-option-active")}
              key={directory}
              onMouseDown={(event) => event.preventDefault()}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => choose(directory)}
            >
              <Folder className="size-4 shrink-0" aria-hidden="true" />
              <span>{directory}</span>
            </button>
          ))}
          {!loading && checked && !candidates.length ? <p>{valid ? "No subfolders in this folder." : "No matching folders."}</p> : null}
        </div>
      ) : null}
      {error ? <p className="chat-directory-error" id="chat-directory-error" role="alert">{error}</p> : null}
    </div>
  );
}

function Composer(props: ComposerProps) {
  const selectedModel = props.harness?.models.models.find((model) => model.id === props.model) ?? null;
  const planGroup = props.harness?.control_groups.find((group) => group.kind === "collaboration" || group.id === "agent");
  const planOption = planGroup?.options.find((option) => option.id === "plan");
  const defaultPlanOption = planGroup?.options.find((option) => option.is_default) ?? planGroup?.options[0];
  const planActive = Boolean(planGroup && props.harnessOptions[planGroup.id] === "plan");
  const fastTier = selectedModel?.service_tiers.find((tier) => tier.id === "priority" || tier.label.toLowerCase() === "fast");
  const fastActive = Boolean(fastTier && props.harnessOptions.service_tier === fastTier.id);
  const genericGroups = props.harness?.control_groups.filter((group) => group !== planGroup) ?? [];
  const readyHarnesses = props.inventory?.harnesses.filter((harness) => harness.status === "ready") ?? [];
  const selectedProviderAvailable = readyHarnesses.some((harness) => providerChoice(harness.provider) === props.provider);
  const selectedProviderChecked = Boolean(props.inventory);
  const noReadyHarnesses = selectedProviderChecked && readyHarnesses.length === 0;
  const persistedModelMissing = Boolean(props.model && !props.harness?.models.models.some((model) => model.id === props.model));
  const reasoningOptions = selectedModel?.reasoning_efforts ?? [];
  const persistedReasoningMissing = Boolean(props.reasoning && !reasoningOptions.some((effort) => effort.id === props.reasoning));
  const attachmentsValid = props.attachments.every((attachment) => attachment.name.trim() && attachment.uri.trim());
  const [extensionInventory, setExtensionInventory] = useState<HarnessExtensionInventory | null>(null);
  const [extensionLoading, setExtensionLoading] = useState(false);
  const [extensionError, setExtensionError] = useState<string | null>(null);
  const [extensionQuery, setExtensionQuery] = useState<string | null>(null);
  const extensionRequest = useRef(0);

  useEffect(() => {
    extensionRequest.current += 1;
    setExtensionInventory(null);
    setExtensionError(null);
    setExtensionLoading(false);
    setExtensionQuery(null);
  }, [props.provider, props.target, props.workingDirectory]);

  const discoverExtensions = useCallback(async (query: string) => {
    const request = ++extensionRequest.current;
    setExtensionLoading(true);
    setExtensionError(null);
    setExtensionQuery(query.trim());
    try {
      const response = await fetch("/api/extensions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          target: props.target,
          provider: props.provider,
          working_directory: props.workingDirectory,
          skill_query: query.trim() || null,
          limit: 100,
        }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload.error ?? "Could not inspect harness extensions."));
      if (extensionRequest.current !== request) return;
      setExtensionInventory(payload as HarnessExtensionInventory);
    } catch (cause) {
      if (extensionRequest.current !== request) return;
      setExtensionError(cause instanceof Error ? cause.message : "Could not inspect harness extensions.");
    } finally {
      if (extensionRequest.current === request) setExtensionLoading(false);
    }
  }, [props.provider, props.target, props.workingDirectory]);

  useEffect(() => {
    if (props.workingDirectoryValid) void discoverExtensions("");
  }, [discoverExtensions, props.workingDirectoryValid]);

  const openSkillAutocomplete = useCallback(() => {
    if (!extensionLoading && (!extensionInventory || extensionError || extensionQuery !== "")) void discoverExtensions("");
  }, [discoverExtensions, extensionError, extensionInventory, extensionLoading, extensionQuery]);

  async function uploadPromptFiles(files: FileList) {
    const uploaded: ChatAttachment[] = [];
    try {
      for (const file of Array.from(files)) uploaded.push(await props.onUploadAttachment(file));
    } finally {
      if (uploaded.length) props.setAttachments([...props.attachments, ...uploaded]);
    }
  }

  function setHarnessOption(key: string, value: string | null) {
    const next = { ...props.harnessOptions };
    if (value) next[key] = value;
    else delete next[key];
    props.setHarnessOptions(next);
  }

  function changeHarness(value: string) {
    props.setProvider(value as ProviderChoice);
  }

  return (
    <form className="composer" onSubmit={props.onSubmit}>
      <div className="composer-well">
        {props.newChat ? (
          <div className="new-chat-context-fields">
            <WorkingDirectoryPicker
              target={props.target}
              value={props.workingDirectory}
              onChange={props.setWorkingDirectory}
              valid={props.workingDirectoryValid}
              onValidChange={props.setWorkingDirectoryValid}
              locked={false}
            />
          </div>
        ) : null}
        {props.attachments.length ? <AttachmentFields attachments={props.attachments} onChange={props.setAttachments} onUpload={props.onUploadAttachment} showActions={false} /> : null}
        {props.queuedMessages.length ? (
          <section aria-label="Queued messages" className="space-y-2 border-t border-zinc-200 pt-3 dark:border-zinc-800">
            <div className="flex items-center justify-between gap-3">
              <h2 className="text-xs font-semibold text-zinc-800 dark:text-zinc-200">Queued messages</h2>
              <span className="font-mono text-[0.6875rem] text-zinc-500">{props.queuedMessages.length}</span>
            </div>
            <div className="max-h-80 space-y-2 overflow-y-auto pr-1">
              {props.queuedMessages.map((message) => (
                <QueuedMessageEditor
                  key={message.id}
                  message={message}
                  onChange={(next) => props.setQueuedMessages((current) => current.map((item) => item.id === next.id ? next : item))}
                  onSave={props.onUpdateQueued}
                  onSendNow={props.onSendQueuedNow}
                  onUpload={props.onUploadAttachment}
                />
              ))}
            </div>
          </section>
        ) : null}
        {props.inventoryLoading ? (
          <div className="composer-inventory-status" role="status">
            <LoadingState label="Checking harness models, thinking, and permissions" />
          </div>
        ) : null}
        {props.inventoryError ? (
          <div className="composer-catalog-error" role="alert">
            <CircleAlert className="size-4 shrink-0" />
            <span className="min-w-0 flex-1">{props.inventoryError}</span>
            <Button type="button" size="compact" variant="ghost" onClick={props.onRetryInventory}>Check again</Button>
          </div>
        ) : null}
        {props.harness?.models.status === "failed" ? (
          <p className="composer-catalog-error" role="status">
            <CircleAlert className="size-4" /> {props.harness.models.error?.message ?? "Model discovery failed."}
          </p>
        ) : null}
        {props.newChat && noReadyHarnesses ? (
          <div className="composer-empty-harness" role="status">
            <Server className="size-5 shrink-0" aria-hidden="true" />
            <div className="min-w-0 flex-1">
              <h2>No harnesses available on this target</h2>
              <p>Install and authenticate Claude Code, Codex, or OpenCode here, or choose another target.</p>
            </div>
            <div className="flex shrink-0 flex-wrap gap-2">
              <Button type="button" size="compact" variant="ghost" onClick={props.onRetryInventory}>Check again</Button>
              <Button type="button" size="compact" variant="secondary" onClick={props.onAddTarget}>Add target</Button>
            </div>
          </div>
        ) : (
          <PromptBar
          attachments={props.attachments}
          canSend={(Boolean(props.prompt.trim()) || Boolean(props.attachments.length)) && attachmentsValid && props.workingDirectoryValid && (!props.newChat || selectedProviderAvailable)}
          celebrateKey={`${props.provider}:${props.model}`}
          compactAvailable={props.compactAvailable}
          compactDisabledReason={props.compactDisabledReason}
          compacting={props.compacting}
          usage={props.usage}
          accountUsage={props.accountUsage}
          controls={<>
            <label className="composer-pill composer-harness-pill" title="Harness saved with this chat">
              <ProviderLogo provider={props.provider} className="text-base" />
              <span className="sr-only">Harness</span>
              <select name="harness" aria-label="Harness" value={props.provider} onChange={(event) => changeHarness(event.target.value)}>
                {!selectedProviderAvailable ? <option value={props.provider}>{props.provider === "opencode" ? "OpenCode" : props.provider === "claude" ? "Claude Code" : "Codex"}{selectedProviderChecked ? " · unavailable" : ""}</option> : null}
                {readyHarnesses.map((harness) => <option key={harness.provider} value={providerChoice(harness.provider)}>{providerName(harness.provider)}</option>)}
              </select>
            </label>
                <label className="composer-pill composer-model-pill" title={selectedModel?.description ?? "Model catalog returned by the selected harness."}>
                  <Code2 className="size-4 shrink-0" aria-hidden="true" />
                  <span className="sr-only">Model</span>
                  <select name="model" value={props.model} disabled={!props.harness?.models.models.length} onChange={(event) => props.setModel(event.target.value)}>
                    {!props.model && !props.harness?.models.models.length ? <option value="">Automatic model</option> : null}
                    {persistedModelMissing ? <option value={props.model}>{persistedModelLabel(props.provider, props.model)}</option> : null}
                    {props.harness?.models.models.map((model) => (
                      <option key={model.id} value={model.id}>{modelLabel(props.harness!.provider, model)}</option>
                    ))}
                  </select>
                </label>

                {reasoningOptions.length || props.reasoning ? (
                  <label className="composer-pill composer-pill-secondary">
                    <Gauge className="size-4" aria-hidden="true" />
                    <span className="sr-only">Thinking</span>
                    <select name="reasoning" value={props.reasoning} onChange={(event) => props.setReasoning(event.target.value)}>
                      {persistedReasoningMissing ? <option value={props.reasoning}>{props.reasoning === "xhigh" ? "Extra high" : props.reasoning}</option> : null}
                      {reasoningOptions.map((effort) => <option key={effort.id} value={effort.id}>{effort.label}</option>)}
                    </select>
                  </label>
                ) : null}

                {genericGroups.map((group) => (
                  <label className="composer-pill composer-pill-secondary" key={group.id} title={group.options.find((option) => option.id === props.harnessOptions[group.id])?.description}>
                    {group.kind === "sandbox" ? <ShieldCheck className="size-4" aria-hidden="true" /> : <SlidersHorizontal className="size-4" aria-hidden="true" />}
                    <span className="sr-only">{group.label}</span>
                    <select name={group.id} value={props.harnessOptions[group.id] ?? ""} onChange={(event) => setHarnessOption(group.id, event.target.value)}>
                      {group.options.map((option) => <option key={option.id} value={option.id}>{option.label}</option>)}
                    </select>
                  </label>
                ))}

                {fastTier ? (
                  <button type="button" className={cn("composer-toggle-control", fastActive && "composer-icon-control-active")} aria-label={`${fastActive ? "Disable" : "Enable"} ${fastTier.label} service tier`} aria-pressed={fastActive} title={fastTier.description ?? fastTier.label} onClick={() => setHarnessOption("service_tier", fastActive ? null : fastTier.id)}>
                    <Zap className="size-4" aria-hidden="true" />
                    <span>Fast</span>
                  </button>
                ) : null}

                {planGroup && planOption && defaultPlanOption ? (
                  <button type="button" className={cn("composer-toggle-control", planActive && "composer-icon-control-active")} aria-label={`${planActive ? "Disable" : "Enable"} plan mode`} aria-pressed={planActive} title={planOption.description} onClick={() => setHarnessOption(planGroup.id, planActive ? defaultPlanOption.id : planOption.id)}>
                    <ListChecks className="size-4" aria-hidden="true" />
                    <span>Plan</span>
                  </button>
                ) : null}
          </>}
          onAddReference={() => props.setAttachments([...props.attachments, createAttachment()])}
          onCancel={props.onCancel}
          onCompact={props.onCompact}
          onChange={props.setPrompt}
          onRemoveAttachment={(id) => props.setAttachments(props.attachments.filter((attachment) => attachment.id !== id))}
          onSkillsOpen={openSkillAutocomplete}
          onUploadFiles={uploadPromptFiles}
          running={props.running}
          skills={extensionInventory?.skills ?? []}
          skillsError={extensionError}
          skillsLoading={extensionLoading}
          submitting={props.submitting}
          value={props.prompt}
          />
        )}
      </div>
    </form>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return <label className="control-field"><span>{label}</span>{children}</label>;
}

function ActivityPanel({ events }: { events: ChatEvent[] }) {
  const [now, setNow] = useState(Date.now);
  const activity = useMemo(() => projectedActivity(events), [events]);

  useEffect(() => {
    if (!events.length) return;
    setNow(Date.now());
    const interval = window.setInterval(() => setNow(Date.now()), 5_000);
    return () => window.clearInterval(interval);
  }, [events.length]);

  return (
    <div className="flex h-full min-h-0 w-full flex-col">
      <div className="panel-heading">
        <div><p className="eyebrow">Normalized · live tail</p><h2 className="panel-title">Activity</h2></div>
        <Badge>{events.length}</Badge>
      </div>
      <AIConversation className="activity-tail" data-testid="activity-list" aria-label="Runtime activity">
        <ConversationContent className="gap-0 px-3 py-2">
          {activity.length ? activity.map(({ event, displayType, summary }) => {
            const type = eventType(event);
            const subagentEvent = type === "task_activity"
              || type === "tasks_changed"
              || (type === "tool_call" && typeof event.payload.task_id === "string");
            return (
            <div className={cn("event-row", subagentEvent && "event-row-subagent")} data-subagent-activity={subagentEvent || undefined} key={event.sequence}>
              <div className="event-line" aria-hidden="true"><span /></div>
              <div className="min-w-0 flex-1 pb-4">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex min-w-0 items-center gap-1.5">
                    {subagentEvent ? <GitBranch className="size-4 shrink-0 stroke-sky-600 dark:stroke-sky-400" aria-hidden="true" /> : null}
                    <code className="truncate text-[0.6875rem] font-semibold text-zinc-700 dark:text-zinc-300">{displayType}</code>
                  </div>
                  <div className="flex shrink-0 items-center gap-1.5 text-[0.625rem] tabular-nums text-zinc-500 dark:text-zinc-400">
                    <time data-testid="event-age" dateTime={new Date(event.timestamp_ms).toISOString()} title={formatTime(event.timestamp_ms)}>
                      {formatTimeAgo(event.timestamp_ms, now)}
                    </time>
                    <span><span className="sr-only">Event </span>#{event.sequence}</span>
                  </div>
                </div>
                <p className="mt-1 line-clamp-3 text-xs leading-5 text-zinc-500 dark:text-zinc-400">{summary}</p>
              </div>
            </div>
            );
          }) : <ConversationEmptyState className="min-h-48" description="Normalized events will appear here." icon={<Activity className="size-4" />} title="No activity" />}
        </ConversationContent>
        <ConversationScrollButton aria-label="Jump to latest activity" className="bottom-3 size-8" />
      </AIConversation>
    </div>
  );
}

function Drawer({ open, onOpenChange, title, side, children }: { open: boolean; onOpenChange: (open: boolean) => void; title: string; side: "left" | "right"; children: ReactNode }) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="drawer-overlay" />
        <Dialog.Content className={cn("drawer-content", side === "left" ? "left-0" : "right-0")}>
          <Dialog.Title className="sr-only">{title}</Dialog.Title>
          <Dialog.Description className="sr-only">Browse runtime {title.toLowerCase()}.</Dialog.Description>
          <Dialog.Close asChild>
            <Button type="button" size="icon" variant="ghost" className="absolute top-2 right-2 z-10" aria-label={`Close ${title.toLowerCase()}`}><X className="size-4" /></Button>
          </Dialog.Close>
          {children}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
