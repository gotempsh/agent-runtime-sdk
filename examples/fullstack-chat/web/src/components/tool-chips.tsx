"use client";

import { ChevronDown, FileText, Pencil, Sparkles, Terminal, Wrench } from "lucide-react";
import { useState } from "react";

import { LoadingState } from "@/components/loading-state";
import { cn } from "@/lib/utils";

export interface ToolChipStep {
  key: string;
  name: string;
  state: "input-available" | "output-available" | "output-error";
  input: unknown;
  output: string | null;
  error: string | null;
}

function iconForTool(name: string) {
  const normalized = name.toLowerCase();
  if (normalized.includes("bash") || normalized.includes("command") || normalized.includes("terminal")) return Terminal;
  if (normalized.includes("write") || normalized.includes("edit") || normalized.includes("patch")) return Pencil;
  if (normalized.includes("read") || normalized.includes("file")) return FileText;
  if (normalized.includes("think") || normalized.includes("reason")) return Sparkles;
  return Wrench;
}

function inputSummary(input: unknown) {
  if (!input || typeof input !== "object") return input == null ? "Waiting for arguments" : String(input);
  const values = input as Record<string, unknown>;
  const summary = values.command ?? values.file_path ?? values.path ?? values.query ?? values.url;
  if (typeof summary === "string") return summary;
  const serialized = JSON.stringify(input);
  return serialized.length > 90 ? `${serialized.slice(0, 87)}…` : serialized;
}

function pretty(value: unknown) {
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

export function ToolChips({ tools }: { tools: ToolChipStep[] }) {
  const [open, setOpen] = useState(true);
  const [openRows, setOpenRows] = useState<Set<string>>(
    () => new Set(tools.map((tool) => tool.key)),
  );

  if (!tools.length) return null;

  function toggleRow(key: string) {
    setOpenRows((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  return (
    <section className="runtime-tool-chips" aria-label={`${tools.length} tool ${tools.length === 1 ? "call" : "calls"}`}>
      <button
        aria-expanded={open}
        className="group flex items-center gap-1.5 rounded-md px-1.5 py-1 text-xs text-zinc-500 transition-colors hover:bg-zinc-100 hover:text-zinc-800 dark:hover:bg-zinc-900 dark:hover:text-zinc-200"
        onClick={() => setOpen((current) => !current)}
        type="button"
      >
        <ChevronDown className={cn("size-3 transition-transform", !open && "-rotate-90")} />
        <span className="tabular-nums">{tools.length} tool {tools.length === 1 ? "call" : "calls"}</span>
      </button>

      {open ? (
        <div className="mt-1.5 space-y-1">
          {tools.map((tool) => {
            const Icon = iconForTool(tool.name);
            const rowOpen = openRows.has(tool.key);
            const running = tool.state === "input-available";
            const failed = tool.state === "output-error";
            return (
              <div className="overflow-hidden rounded-md" data-testid="tool-call" key={tool.key}>
                <button
                  aria-expanded={rowOpen}
                  className="group flex min-h-8 w-full min-w-0 items-center gap-2 rounded-md px-2 text-left transition-colors hover:bg-zinc-100 dark:hover:bg-zinc-900"
                  onClick={() => toggleRow(tool.key)}
                  type="button"
                >
                  <span className="relative flex size-4 shrink-0 items-center justify-center text-zinc-500">
                    <Icon className="size-3.5 transition-opacity group-hover:opacity-0" />
                    <ChevronDown className={cn("absolute size-3 opacity-0 transition-all group-hover:opacity-100", !rowOpen && "-rotate-90")} />
                  </span>
                  <span className="shrink-0 text-xs font-medium text-zinc-800 dark:text-zinc-200">{tool.name}</span>
                  <code className="min-w-0 flex-1 truncate rounded bg-zinc-100 px-1.5 py-0.5 text-[0.6875rem] text-zinc-600 dark:bg-zinc-900 dark:text-zinc-400">
                    {inputSummary(tool.input)}
                  </code>
                  <span className={cn(
                    "shrink-0 font-mono text-[0.625rem] uppercase tracking-wide",
                    running ? "text-blue-600 dark:text-blue-400" : failed ? "text-red-600 dark:text-red-400" : "text-emerald-600 dark:text-emerald-400",
                  )}>
                    {running ? "Running" : failed ? "Failed" : "Completed"}
                  </span>
                </button>

                {rowOpen ? (
                  <div className="ml-4 border-l border-zinc-200 py-2 pl-4 dark:border-zinc-800">
                    {running ? <LoadingState label={`Running ${tool.name}`} variant="Dots" /> : null}
                    {tool.input !== null ? (
                      <div className="mt-2 first:mt-0">
                        <p className="mb-1 text-[0.625rem] font-semibold uppercase tracking-wider text-zinc-500">Parameters</p>
                        <pre className="max-h-56 overflow-auto whitespace-pre-wrap rounded-md bg-zinc-100 p-2.5 font-mono text-[0.6875rem] leading-5 text-zinc-700 dark:bg-zinc-900 dark:text-zinc-300">{pretty(tool.input)}</pre>
                      </div>
                    ) : null}
                    {tool.output || tool.error ? (
                      <div className="mt-2">
                        <p className="mb-1 text-[0.625rem] font-semibold uppercase tracking-wider text-zinc-500">{tool.error ? "Error" : "Result"}</p>
                        <pre className={cn(
                          "max-h-64 overflow-auto whitespace-pre-wrap rounded-md p-2.5 font-mono text-[0.6875rem] leading-5",
                          tool.error ? "bg-red-50 text-red-700 dark:bg-red-950/40 dark:text-red-300" : "bg-zinc-100 text-zinc-700 dark:bg-zinc-900 dark:text-zinc-300",
                        )}>{tool.error ?? tool.output}</pre>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}
    </section>
  );
}
