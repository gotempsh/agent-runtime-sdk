import type { HTMLAttributes } from "react";

import { cn } from "@/lib/utils";

const tones = {
  neutral: "bg-zinc-950/5 text-zinc-700 ring-zinc-950/10",
  active: "bg-sky-50 text-sky-700 ring-sky-700/15",
  warning: "bg-amber-50 text-amber-800 ring-amber-700/20",
  success: "bg-emerald-50 text-emerald-800 ring-emerald-700/15",
  danger: "bg-red-50 text-red-700 ring-red-700/15",
} as const;

export function Badge({
  className,
  tone = "neutral",
  ...props
}: HTMLAttributes<HTMLSpanElement> & { tone?: keyof typeof tones }) {
  return (
    <span
      className={cn(
        "inline-flex w-fit items-center rounded-md px-2 py-1 font-mono text-[0.6875rem] font-medium ring-1 ring-inset",
        tones[tone],
        className,
      )}
      {...props}
    />
  );
}
