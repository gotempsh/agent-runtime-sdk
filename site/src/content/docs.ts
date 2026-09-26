import architecture from "../../../docs/explanation/architecture.md?raw";
import approvals from "../../../docs/how-to/persist-approvals.md?raw";
import commandExecution from "../../../docs/how-to/persist-command-execution.md?raw";
import codexResume from "../../../docs/how-to/resume-large-codex-conversations.md?raw";
import persistence from "../../../docs/how-to/persist-conversations.md?raw";
import toolProcesses from "../../../docs/how-to/keep-tool-processes-running.md?raw";
import managedProcesses from "../../../docs/how-to/manage-background-processes.md?raw";
import claudeSubagents from "../../../docs/how-to/stream-claude-subagents.md?raw";
import sandboxOperations from "../../../docs/how-to/operate-sandboxes.md?raw";
import customSandbox from "../../../docs/how-to/custom-sandbox.md?raw";
import customTransport from "../../../docs/how-to/custom-transport.md?raw";
import harnessDiscovery from "../../../docs/how-to/discover-harnesses.md?raw";
import nono from "../../../docs/how-to/nono.md?raw";
import tailnets from "../../../docs/how-to/tailnets.md?raw";
import recovery from "../../../docs/how-to/recover-sandbox-denials.md?raw";
import migration from "../../../docs/MIGRATION.md?raw";
import api from "../../../docs/reference/api.md?raw";
import events from "../../../docs/reference/events.md?raw";
import quickstart from "../../../docs/tutorials/quickstart.md?raw";

export type DocCategory = "Tutorials" | "How-to guides" | "Reference" | "Explanation";

export interface DocPage {
  slug: string;
  sourcePath: string;
  title: string;
  description: string;
  category: DocCategory;
  body: string;
}

export const categories: DocCategory[] = [
  "Tutorials",
  "How-to guides",
  "Reference",
  "Explanation",
];

export const docs: DocPage[] = [
  {
    slug: "resume-large-codex-conversations",
    sourcePath: "docs/how-to/resume-large-codex-conversations.md",
    title: "Resume large Codex conversations",
    description: "Continue long threads without returning oversized history frames or losing provider context.",
    category: "How-to guides",
    body: codexResume,
  },
  {
    slug: "quickstart",
    sourcePath: "docs/tutorials/quickstart.md",
    title: "Run your first agent turn",
    description: "Install the crate, check provider readiness, stream events, and complete one turn.",
    category: "Tutorials",
    body: quickstart,
  },
  {
    slug: "persistent-conversations",
    sourcePath: "docs/how-to/persist-conversations.md",
    title: "Persist conversations and streams",
    description: "Journal events, restore chat state, and reconnect streaming clients without losing status.",
    category: "How-to guides",
    body: persistence,
  },
  {
    slug: "persistent-approvals",
    sourcePath: "docs/how-to/persist-approvals.md",
    title: "Persist approvals",
    description: "Store approval requests and decisions, reconnect the UI, and resume waiting turns safely.",
    category: "How-to guides",
    body: approvals,
  },
  {
    slug: "persistent-command-execution",
    sourcePath: "docs/how-to/persist-command-execution.md",
    title: "Persist command execution",
    description: "Project tool events into durable attempts, reconcile interruptions, and track supervised services.",
    category: "How-to guides",
    body: commandExecution,
  },
  {
    slug: "tool-process-lifetime",
    sourcePath: "docs/how-to/keep-tool-processes-running.md",
    title: "Keep tool processes running",
    description: "Preserve development servers after a natural turn exit while retaining hard-stop cleanup.",
    category: "How-to guides",
    body: toolProcesses,
  },
  {
    slug: "managed-background-processes",
    sourcePath: "docs/how-to/manage-background-processes.md",
    title: "Manage background commands",
    description: "Own services beyond a turn with bounded logs, restart policies, and explicit lifecycle control.",
    category: "How-to guides",
    body: managedProcesses,
  },
  {
    slug: "claude-native-subagents",
    sourcePath: "docs/how-to/stream-claude-subagents.md",
    title: "Stream Claude native subagents",
    description: "Render Task/Agent snapshots, progress activity, nested tools, and background completion.",
    category: "How-to guides",
    body: claudeSubagents,
  },
  {
    slug: "sandbox-operations",
    sourcePath: "docs/how-to/operate-sandboxes.md",
    title: "Operate sandboxed turns",
    description: "Require capabilities, persist profile revisions, and recover denied steps without weakening policy.",
    category: "How-to guides",
    body: sandboxOperations,
  },
  {
    slug: "nono",
    sourcePath: "docs/how-to/nono.md",
    title: "Manage Nono sandboxes",
    description: "Generate, validate, and apply Nono profiles around a provider turn.",
    category: "How-to guides",
    body: nono,
  },
  {
    slug: "tailnets",
    sourcePath: "docs/how-to/tailnets.md",
    title: "Give an agent its own tailnet",
    description: "Run a per-agent userspace Tailscale daemon and apply it to any provider turn, with or without Nono.",
    category: "How-to guides",
    body: tailnets,
  },
  {
    slug: "sandbox-recovery",
    sourcePath: "docs/how-to/recover-sandbox-denials.md",
    title: "Recover a denied sandbox step",
    description: "Update an approved profile revision and retry safely in the same provider session.",
    category: "How-to guides",
    body: recovery,
  },
  {
    slug: "custom-sandbox",
    sourcePath: "docs/how-to/custom-sandbox.md",
    title: "Implement a sandbox backend",
    description: "Integrate Bubblewrap, containers, remote workers, or another isolation boundary.",
    category: "How-to guides",
    body: customSandbox,
  },
  {
    slug: "harness-discovery",
    sourcePath: "docs/how-to/discover-harnesses.md",
    title: "Discover provider harnesses",
    description: "Probe provider-native controls, models, reasoning levels, and service tiers inside a selected target.",
    category: "How-to guides",
    body: harnessDiscovery,
  },
  {
    slug: "custom-execution-transport",
    sourcePath: "docs/how-to/custom-transport.md",
    title: "Run agents remotely",
    description: "Use SSH, the optional Temps sandbox integration, typed failures, or a custom hosted transport.",
    category: "How-to guides",
    body: customTransport,
  },
  {
    slug: "api-and-capabilities",
    sourcePath: "docs/reference/api.md",
    title: "API and capability reference",
    description: "Provider support, core traits, limits, features, and typed error behavior.",
    category: "Reference",
    body: api,
  },
  {
    slug: "event-catalog",
    sourcePath: "docs/reference/events.md",
    title: "Event and status catalog",
    description: "Every normalized stream event and the durable state it represents.",
    category: "Reference",
    body: events,
  },
  {
    slug: "architecture",
    sourcePath: "docs/explanation/architecture.md",
    title: "Runtime architecture",
    description: "Understand adapters, process supervision, permissions, sandboxes, and trust boundaries.",
    category: "Explanation",
    body: architecture,
  },
  {
    slug: "migration",
    sourcePath: "docs/MIGRATION.md",
    title: "Adopt the runtime",
    description: "Introduce the runtime incrementally behind an existing application boundary.",
    category: "Explanation",
    body: migration,
  },
];

export const docsBySlug = new Map(docs.map((doc) => [doc.slug, doc]));
export const docsBySourcePath = new Map(docs.map((doc) => [doc.sourcePath, doc]));

export function stripMarkdown(value: string) {
  return value
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/[`*_>#()|~-]/g, " ")
    .replaceAll("[", " ")
    .replaceAll("]", " ")
    .replace(/\s+/g, " ")
    .trim();
}
