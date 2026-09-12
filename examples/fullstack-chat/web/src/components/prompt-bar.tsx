"use client";

import { accentChain, ACCENTS, createShader, playSweep } from "glimm";
import {
  ArrowUp,
  CircleAlert,
  CircleStop,
  FilePlus2,
  Gauge,
  LoaderCircle,
  Minimize2,
  Paperclip,
  Puzzle,
  X,
} from "lucide-react";
import {
  type CSSProperties,
  type ReactNode,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { cn } from "@/lib/utils";
import type { AccountUsageSnapshot, AccountUsageWindow, ChatAttachment, Usage } from "@/types";

const RAINBOW = accentChain([
  ACCENTS.red,
  ACCENTS.orange,
  ACCENTS.yellow,
  ACCENTS.green,
  ACCENTS.cyan,
  ACCENTS.blue,
  ACCENTS.purple,
]);

export interface PromptBarSkill {
  id: string;
  description: string | null;
  path: string;
  scope: "user" | "project";
}

interface PromptBarProps {
  value: string;
  onChange: (value: string) => void;
  attachments: ChatAttachment[];
  onRemoveAttachment: (id: string) => void;
  onUploadFiles: (files: FileList) => Promise<void>;
  onAddReference: () => void;
  skills: PromptBarSkill[];
  skillsLoading: boolean;
  skillsError: string | null;
  onSkillsOpen: () => void;
  compactAvailable: boolean;
  compactDisabledReason: string | null;
  compacting: boolean;
  onCompact: () => Promise<void>;
  usage: Usage | null;
  accountUsage: AccountUsageSnapshot | null;
  controls: ReactNode;
  canSend: boolean;
  submitting: boolean;
  running: boolean;
  onCancel: () => void;
  celebrateKey: string;
}

function parseSkillToken(value: string) {
  const match = /(^|\s)\/([\w-]*)$/.exec(value);
  if (!match) return null;
  return {
    query: match[2].toLowerCase(),
    start: match.index + match[1].length,
  };
}

export function PromptBar({
  value,
  onChange,
  attachments,
  onRemoveAttachment,
  onUploadFiles,
  onAddReference,
  skills,
  skillsLoading,
  skillsError,
  onSkillsOpen,
  compactAvailable,
  compactDisabledReason,
  compacting,
  onCompact,
  usage,
  accountUsage,
  controls,
  canSend,
  submitting,
  running,
  onCancel,
  celebrateKey,
}: PromptBarProps) {
  const [actionsOpen, setActionsOpen] = useState(false);
  const [dismissed, setDismissed] = useState(false);
  const [active, setActive] = useState(0);
  const [engaged, setEngaged] = useState(false);
  const [rowBox, setRowBox] = useState<{ top: number; height: number } | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [usageOpen, setUsageOpen] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const uploadRef = useRef<HTMLInputElement>(null);
  const rowRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const shaderRef = useRef<ReturnType<typeof createShader>>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const previousCelebrateKey = useRef(celebrateKey);
  const token = dismissed ? null : parseSkillToken(value);
  const query = token?.query ?? "";
  const menuOpen = Boolean(token);
  const visibleCommands = useMemo(() => {
    const commands: Array<
      | { kind: "compact"; key: string; name: string; description: string; scope: string; disabled: boolean }
      | { kind: "skill"; key: string; name: string; description: string; scope: string; skill: PromptBarSkill; disabled: false }
    > = [];
    if (!query || "compact".includes(query)) {
      commands.push({
        kind: "compact",
        key: "action:compact",
        name: "/compact",
        description: compacting
          ? "Compacting this Claude session…"
          : compactDisabledReason ?? "Summarize this Claude session and free context space.",
        scope: "action",
        disabled: !compactAvailable || compacting,
      });
    }
    const matchingSkills = !query ? skills : skills.filter((skill) =>
      skill.id.toLowerCase().includes(query)
      || skill.description?.toLowerCase().includes(query),
    );
    commands.push(...matchingSkills.map((skill) => ({
      kind: "skill" as const,
      key: `skill:${skill.scope}:${skill.path}`,
      name: `/${skill.id}`,
      description: skill.description ?? skill.path,
      scope: skill.scope,
      skill,
      disabled: false as const,
    })));
    return commands;
  }, [compactAvailable, compactDisabledReason, compacting, query, skills]);

  useEffect(() => {
    if (menuOpen) onSkillsOpen();
  }, [menuOpen, onSkillsOpen]);

  useEffect(() => {
    setActive(0);
    setEngaged(false);
  }, [query, skills.length]);

  useLayoutEffect(() => {
    const row = rowRefs.current[active];
    if (row) setRowBox({ top: row.offsetTop, height: row.offsetHeight });
  }, [active, visibleCommands.length]);

  useLayoutEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "0px";
    const contentHeight = textarea.scrollHeight;
    textarea.style.height = `${Math.min(Math.max(contentHeight, 48), 144)}px`;
    textarea.style.overflowY = contentHeight > 144 ? "auto" : "hidden";
  }, [value]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    shaderRef.current = createShader({
      canvas,
      palette: RAINBOW,
      direction: "ltr",
      bandTight: 10,
      swellAmount: 0.85,
    });
    return () => {
      shaderRef.current?.destroy();
      shaderRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (previousCelebrateKey.current === celebrateKey) return;
    previousCelebrateKey.current = celebrateKey;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const shader = shaderRef.current;
    if (!shader) return;
    void playSweep(shader, {
      palette: RAINBOW,
      direction: "ltr",
      sweepMs: 570,
      outroMs: 80,
      peakAlpha: 1,
      bandTight: 10,
      brightness: 1.25,
      swellAmount: 1,
      waveSpeed: 1.8,
      easing: "easeOutExpo",
    }).done;
  }, [celebrateKey]);

  useEffect(() => {
    if (!actionsOpen && !usageOpen) return;
    const close = (event: PointerEvent) => {
      if (!(event.target as Element).closest("[data-prompt-bar]")) {
        setActionsOpen(false);
        setUsageOpen(false);
      }
    };
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, [actionsOpen, usageOpen]);

  const context = usage?.context_window ?? null;
  const contextPercent = context?.used_tokens != null && context.limit_tokens
    ? (context.used_tokens / context.limit_tokens) * 100
    : null;
  const hasUsage = Boolean(context || accountUsage?.windows.length || accountUsage?.credits);

  function chooseSkill(skill: PromptBarSkill) {
    if (!token) return;
    onChange(`${value.slice(0, token.start)}/${skill.id} `);
    setDismissed(true);
    textareaRef.current?.focus();
  }

  async function chooseCommand(command: (typeof visibleCommands)[number]) {
    if (command.disabled) return;
    if (command.kind === "skill") {
      chooseSkill(command.skill);
      return;
    }
    if (!token) return;
    try {
      await onCompact();
      onChange(value.slice(0, token.start).trimEnd());
      setDismissed(true);
      textareaRef.current?.focus();
    } catch {
      // The parent renders the actionable error; keep `/compact` so retry is one action away.
    }
  }

  async function upload(files: FileList | null) {
    if (!files?.length) return;
    setUploading(true);
    setUploadError(null);
    try {
      await onUploadFiles(files);
      setActionsOpen(false);
    } catch (cause) {
      setUploadError(cause instanceof Error ? cause.message : "Could not upload the selected files.");
    } finally {
      setUploading(false);
      if (uploadRef.current) uploadRef.current.value = "";
    }
  }

  return (
    <div className="prompt-bar-anchor" data-prompt-bar>
      {menuOpen ? (
        <div className="prompt-command-menu" onMouseLeave={() => setEngaged(false)} role="listbox" aria-label="Commands and applicable skills">
          <span
            aria-hidden="true"
            className="prompt-command-highlight"
            style={{
              "--prompt-row-top": `${rowBox?.top ?? 0}px`,
              "--prompt-row-height": `${rowBox?.height ?? 0}px`,
              opacity: rowBox && engaged && visibleCommands.length ? 1 : 0,
            } as CSSProperties}
          />
          {visibleCommands.map((command, index) => (
            <button
              aria-selected={index === active}
              aria-disabled={command.disabled}
              className="prompt-command-row"
              key={command.key}
              onClick={() => void chooseCommand(command)}
              onMouseDown={(event) => event.preventDefault()}
              onMouseEnter={() => {
                setActive(index);
                setEngaged(true);
              }}
              ref={(element) => { rowRefs.current[index] = element; }}
              role="option"
              type="button"
            >
              {command.kind === "compact"
                ? compacting
                  ? <LoaderCircle className="size-4 shrink-0 animate-spin motion-reduce:animate-none" aria-hidden="true" />
                  : <Minimize2 className="size-4 shrink-0" aria-hidden="true" />
                : <Puzzle className="size-4 shrink-0" aria-hidden="true" />}
              <span className="prompt-command-name">{command.name}</span>
              <span className="prompt-command-description">{command.description}</span>
              <span className="prompt-command-scope">{command.scope}</span>
            </button>
          ))}
          {skillsLoading ? (
            <div className="prompt-command-state" role="status"><LoaderCircle className="size-4 shrink-0 animate-spin motion-reduce:animate-none" /> Searching this execution host…</div>
          ) : null}
          {!skillsLoading && skillsError ? (
            <div className="prompt-command-state prompt-command-error" role="alert"><CircleAlert className="size-4 shrink-0" /> {skillsError}</div>
          ) : null}
          {!skillsLoading && !skillsError && !visibleCommands.length ? (
            <div className="prompt-command-state">No actions or applicable skills match “{query}”.</div>
          ) : null}
          <p className="prompt-command-hint">Type to filter actions and host-applicable skills · ↑↓ to move · Enter to choose.</p>
        </div>
      ) : null}

      {actionsOpen ? (
        <div className="prompt-action-menu">
          <button type="button" onClick={() => uploadRef.current?.click()}>
            {uploading ? <LoaderCircle className="size-4 shrink-0 animate-spin motion-reduce:animate-none" /> : <Paperclip className="size-4 shrink-0" />}
            <span><strong>{uploading ? "Uploading files" : "Upload files"}</strong><small>Copy files into this chat’s execution host.</small></span>
          </button>
          <button type="button" onClick={() => { onAddReference(); setActionsOpen(false); }}>
            <FilePlus2 className="size-4 shrink-0" />
            <span><strong>Attach reference</strong><small>Add a file or web URI manually.</small></span>
          </button>
        </div>
      ) : null}

      {usageOpen && hasUsage ? (
        <UsagePopover usage={usage} accountUsage={accountUsage} />
      ) : null}

      <div className="prompt-bar">
        <canvas className="prompt-bar-sweep" ref={canvasRef} aria-hidden="true" />
        {attachments.length ? (
          <div className="prompt-attachments" role="list">
            {attachments.map((attachment) => (
              <div className="prompt-attachment" key={attachment.id} role="listitem">
                <Paperclip className="size-4 shrink-0" aria-hidden="true" />
                <span>{attachment.name || "New reference"}</span>
                <button type="button" onClick={() => onRemoveAttachment(attachment.id)} aria-label={`Remove ${attachment.name || "reference"}`}>
                  <X className="size-4 shrink-0" />
                </button>
              </div>
            ))}
          </div>
        ) : null}
        <textarea
          aria-label="Prompt"
          className="prompt-bar-input"
          name="prompt"
          onChange={(event) => {
            onChange(event.target.value);
            setDismissed(false);
            setActionsOpen(false);
          }}
          onKeyDown={(event) => {
            if (menuOpen && visibleCommands.length) {
              if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                setEngaged(true);
                setActive((current) => (current + (event.key === "ArrowDown" ? 1 : visibleCommands.length - 1)) % visibleCommands.length);
                return;
              }
              if ((event.key === "Enter" && !event.shiftKey) || event.key === "Tab") {
                event.preventDefault();
                void chooseCommand(visibleCommands[active]);
                return;
              }
            }
            if (event.key === "Escape") {
              setDismissed(true);
              setActionsOpen(false);
              return;
            }
            if (event.key === "Enter" && (event.metaKey || event.ctrlKey) && !event.nativeEvent.isComposing) {
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }
          }}
          placeholder="Ask the agent to inspect, change, or verify something…"
          ref={textareaRef}
          rows={1}
          value={value}
        />
        {uploadError ? <p className="prompt-inline-error" role="alert">{uploadError}</p> : null}
        <div className="prompt-bar-footer">
          <div className="prompt-bar-tools">
            <input className="sr-only" multiple name="prompt_files" onChange={(event) => void upload(event.target.files)} ref={uploadRef} type="file" />
            <button
              aria-expanded={actionsOpen}
              aria-label="Add files or references"
              className={cn("prompt-add-button", actionsOpen && "prompt-control-active")}
              onClick={() => setActionsOpen((current) => !current)}
              type="button"
            >
              <Paperclip className="size-4 shrink-0" />
            </button>
            <div className="prompt-bar-controls">{controls}</div>
          </div>
          <div className="prompt-bar-actions">
            {hasUsage ? (
              <button
                aria-expanded={usageOpen}
                aria-label="Show context and account usage"
                className={cn("prompt-usage-button", usageOpen && "prompt-control-active")}
                onClick={() => {
                  setActionsOpen(false);
                  setUsageOpen((current) => !current);
                }}
                style={{ "--usage-percent": `${Math.min(100, Math.max(0, contextPercent ?? accountUsage?.windows[0]?.used_percent ?? 0))}%` } as CSSProperties}
                type="button"
              >
                <Gauge className="size-4" aria-hidden="true" />
              </button>
            ) : null}
            {running ? (
              <button className="prompt-cancel-button" onClick={onCancel} type="button" aria-label="Cancel running turn">
                <CircleStop className="size-4 shrink-0" />
              </button>
            ) : null}
            <button className="prompt-send-button" disabled={!canSend || submitting} type="submit" aria-label={running ? "Queue message" : "Send message"}>
              {submitting ? <LoaderCircle className="size-4 shrink-0 animate-spin motion-reduce:animate-none" /> : <ArrowUp className="size-4 shrink-0" />}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function compactNumber(value: number) {
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function resetLabel(window: AccountUsageWindow) {
  if (!window.resets_at_unix_seconds) return "reset unavailable";
  const milliseconds = window.resets_at_unix_seconds * 1000 - Date.now();
  if (milliseconds <= 0) return "reset due";
  const minutes = Math.ceil(milliseconds / 60_000);
  if (minutes < 60) return `resets ${minutes}m`;
  const hours = Math.ceil(minutes / 60);
  if (hours < 48) return `resets ${hours}h`;
  return `resets ${Math.ceil(hours / 24)}d`;
}

function windowLabel(window: AccountUsageWindow) {
  if (window.id.includes("opus")) return "Opus weekly";
  if (window.id.includes("sonnet")) return "Sonnet weekly";
  if (window.id.includes("overage")) return "Weekly overage";
  if (window.kind === "session") return "Session";
  if (window.kind === "weekly") return "Weekly";
  return window.id.replaceAll("_", " ");
}

function UsagePopover({ usage, accountUsage }: { usage: Usage | null; accountUsage: AccountUsageSnapshot | null }) {
  const context = usage?.context_window ?? null;
  const contextPercent = context?.used_tokens != null && context.limit_tokens
    ? (context.used_tokens / context.limit_tokens) * 100
    : null;
  return (
    <section className="prompt-usage-popover" aria-label="Context and account usage">
      {context ? (
        <div className="prompt-usage-context">
          <strong>Context window</strong>
          {contextPercent != null ? <span>{Math.round(contextPercent)}% used</span> : null}
          {context.used_tokens != null || context.limit_tokens != null ? (
            <small>{context.used_tokens != null ? compactNumber(context.used_tokens) : "—"} / {context.limit_tokens != null ? compactNumber(context.limit_tokens) : "—"} tokens{context.estimated ? " · estimated" : ""}</small>
          ) : null}
        </div>
      ) : null}
      {accountUsage ? (
        <div className="prompt-account-usage">
          <div className="prompt-account-heading">
            <strong>{accountUsage.provider === "claude" ? "Claude" : accountUsage.provider === "codex" ? "Codex" : "OpenCode"}</strong>
            {accountUsage.plan ? <span>{accountUsage.plan}</span> : null}
          </div>
          {accountUsage.windows.map((window) => (
            <div className="prompt-usage-window" key={window.id}>
              <div><span>{windowLabel(window)}</span><span><strong>{Math.round(window.used_percent)}%</strong> · {resetLabel(window)}</span></div>
              <span className="prompt-usage-track"><span style={{ width: `${Math.min(100, Math.max(0, window.used_percent))}%` }} /></span>
            </div>
          ))}
          {accountUsage.credits ? (
            <div className="prompt-credit-row"><span>Credits</span><strong>{accountUsage.credits.unlimited ? "Unlimited" : accountUsage.credits.balance ?? "Available"}</strong></div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
