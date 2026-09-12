"use client";

import {
  Bot,
  CheckCircle2,
  ChevronDown,
  CircleStop,
  Clock3,
  GitBranch,
  XCircle,
} from "lucide-react";
import { type CSSProperties, useEffect, useState } from "react";

import { LoadingState } from "@/components/loading-state";
import { ToolChips, type ToolChipStep } from "@/components/tool-chips";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export interface ClaudeSubagentActivityView {
  sequence: number;
  kind: "started" | "updated" | "progress" | "completed" | "failed" | "stopped" | string;
  summary: string | null;
  lastToolName: string | null;
  timestampMs: number;
}

export interface ClaudeSubagentView {
  id: string;
  description: string;
  status: string;
  agentType: string | null;
  error: string | null;
  summary: string | null;
  spawnDepth: number;
  usage: {
    totalTokens: number;
    toolUses: number;
    durationMs: number;
  } | null;
  activities: ClaudeSubagentActivityView[];
  tools: ToolChipStep[];
}

function normalizedStatus(status: string) {
  return status.trim().toLowerCase().replaceAll("-", "_");
}

function isRunning(status: string) {
  return !["completed", "succeeded", "failed", "stopped", "cancelled", "canceled", "killed"].includes(normalizedStatus(status));
}

function statusCopy(status: string) {
  const normalized = normalizedStatus(status);
  if (normalized === "succeeded" || normalized === "completed") return "Completed";
  if (normalized === "failed") return "Failed";
  if (["stopped", "cancelled", "canceled", "killed"].includes(normalized)) return "Stopped";
  if (normalized === "pending" || normalized === "queued") return "Queued";
  return "Running";
}

function statusTone(status: string): "active" | "success" | "danger" | "neutral" {
  const normalized = normalizedStatus(status);
  if (normalized === "succeeded" || normalized === "completed") return "success";
  if (normalized === "failed") return "danger";
  if (["stopped", "cancelled", "canceled", "killed"].includes(normalized)) return "neutral";
  return "active";
}

function StatusIcon({ status }: { status: string }) {
  const normalized = normalizedStatus(status);
  if (normalized === "succeeded" || normalized === "completed") {
    return <CheckCircle2 className="size-4 shrink-0 stroke-emerald-600 dark:stroke-emerald-400" aria-hidden="true" />;
  }
  if (normalized === "failed") {
    return <XCircle className="size-4 shrink-0 stroke-red-600 dark:stroke-red-400" aria-hidden="true" />;
  }
  if (["stopped", "cancelled", "canceled", "killed"].includes(normalized)) {
    return <CircleStop className="size-4 shrink-0 stroke-zinc-500" aria-hidden="true" />;
  }
  return <Bot className="size-4 shrink-0 stroke-sky-600 dark:stroke-sky-400" aria-hidden="true" />;
}

function formatDuration(durationMs: number) {
  if (durationMs < 1_000) return `${durationMs}ms`;
  if (durationMs < 60_000) return `${(durationMs / 1_000).toFixed(durationMs < 10_000 ? 1 : 0)}s`;
  const minutes = Math.floor(durationMs / 60_000);
  const seconds = Math.floor((durationMs % 60_000) / 1_000);
  return `${minutes}m ${seconds}s`;
}

function activityCopy(activity: ClaudeSubagentActivityView) {
  if (activity.summary) return activity.summary;
  if (activity.lastToolName) return `${activity.kind.replaceAll("_", " ")} · ${activity.lastToolName}`;
  return activity.kind.replaceAll("_", " ");
}

export function ClaudeSubagents({ agents }: { agents: ClaudeSubagentView[] }) {
  const runningIds = agents.filter((agent) => isRunning(agent.status)).map((agent) => agent.id);
  const runningKey = runningIds.join("\u0000");
  const runningCount = runningIds.length;
  const [open, setOpen] = useState(() => runningCount > 0);
  const [openAgents, setOpenAgents] = useState<Set<string>>(
    () => new Set(runningIds),
  );

  useEffect(() => {
    if (!runningKey) return;
    const nextRunningIds = runningKey.split("\u0000");
    setOpen(true);
    setOpenAgents((current) => new Set([...current, ...nextRunningIds]));
  }, [runningKey]);

  if (!agents.length) return null;

  function toggleAgent(id: string) {
    setOpenAgents((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <section className="claude-subagents" data-testid="claude-subagents" aria-label="Claude native subagents">
      <button
        type="button"
        aria-expanded={open}
        className="subagent-summary-trigger"
        onClick={() => setOpen((current) => !current)}
      >
        <span className="absolute top-1/2 left-1/2 size-[max(100%,3rem)] -translate-1/2 pointer-fine:hidden" aria-hidden="true" />
        <GitBranch className="size-4 shrink-0 stroke-zinc-500" aria-hidden="true" />
        <span className="min-w-0 truncate">
          {agents.length} Claude {agents.length === 1 ? "subagent" : "subagents"}
        </span>
        {runningCount > 0 ? <Badge tone="active">{runningCount} running</Badge> : null}
        <ChevronDown className={cn("size-4 shrink-0 stroke-zinc-500", !open && "-rotate-90")} aria-hidden="true" />
      </button>

      {open ? (
        <div className="subagent-list">
          {agents.map((agent) => {
            const rowOpen = openAgents.has(agent.id);
            const running = isRunning(agent.status);
            return (
              <article
                className="subagent-row"
                data-testid="claude-subagent"
                key={agent.id}
                style={{ "--subagent-depth": Math.min(agent.spawnDepth, 4) } as CSSProperties}
              >
                <button
                  type="button"
                  aria-expanded={rowOpen}
                  className="subagent-row-trigger"
                  onClick={() => toggleAgent(agent.id)}
                >
                  <span className="absolute top-1/2 left-1/2 size-[max(100%,3rem)] -translate-1/2 pointer-fine:hidden" aria-hidden="true" />
                  <StatusIcon status={agent.status} />
                  <div className="min-w-0 flex-1">
                    <div className="subagent-row-title">{agent.description || agent.id}</div>
                    <div className="subagent-row-summary">{agent.error ?? agent.summary ?? agent.id}</div>
                  </div>
                  {agent.agentType ? <Badge>{agent.agentType}</Badge> : null}
                  <Badge tone={statusTone(agent.status)}>{statusCopy(agent.status)}</Badge>
                  <ChevronDown className={cn("size-4 shrink-0 stroke-zinc-500", !rowOpen && "-rotate-90")} aria-hidden="true" />
                </button>

                {rowOpen ? (
                  <div className="subagent-detail">
                    {running ? <LoadingState label={`Claude subagent working · ${agent.description || agent.id}`} variant="Dots" /> : null}

                    {agent.usage ? (
                      <dl className="subagent-usage">
                        <div><dt>Duration</dt><dd>{formatDuration(agent.usage.durationMs)}</dd></div>
                        <div><dt>Tokens</dt><dd>{agent.usage.totalTokens.toLocaleString()}</dd></div>
                        <div><dt>Tool uses</dt><dd>{agent.usage.toolUses.toLocaleString()}</dd></div>
                      </dl>
                    ) : null}

                    {agent.activities.length ? (
                      <ol className="subagent-activity-list" role="list" aria-label={`Activity for ${agent.description || agent.id}`}>
                        {agent.activities.map((activity) => (
                          <li key={activity.sequence}>
                            <Clock3 className="size-4 shrink-0 stroke-zinc-400" aria-hidden="true" />
                            <span className="min-w-0 flex-1">{activityCopy(activity)}</span>
                            <code>#{activity.sequence}</code>
                          </li>
                        ))}
                      </ol>
                    ) : null}

                    <ToolChips tools={agent.tools} />
                  </div>
                ) : null}
              </article>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}
