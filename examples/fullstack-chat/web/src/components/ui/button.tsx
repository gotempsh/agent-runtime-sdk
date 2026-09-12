import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import type { ButtonHTMLAttributes } from "react";

import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "relative inline-flex shrink-0 items-center justify-center gap-2 rounded-lg text-sm font-medium focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-emerald-600 disabled:pointer-events-none disabled:opacity-45",
  {
    variants: {
      variant: {
        default: "bg-zinc-950 text-white hover:bg-zinc-800 dark:bg-zinc-100 dark:text-zinc-950 dark:hover:bg-white",
        primary: "bg-emerald-700 text-white hover:bg-emerald-800 dark:bg-emerald-600 dark:hover:bg-emerald-500",
        outline: "bg-transparent text-zinc-700 ring-1 ring-zinc-950/10 hover:bg-zinc-950/5 dark:text-zinc-200 dark:ring-white/15 dark:hover:bg-white/8",
        secondary: "bg-zinc-950/5 text-zinc-900 ring-1 ring-zinc-950/10 hover:bg-zinc-950/10 dark:bg-white/8 dark:text-zinc-100 dark:ring-white/12 dark:hover:bg-white/12",
        ghost: "text-zinc-600 hover:bg-zinc-950/5 hover:text-zinc-950 dark:text-zinc-300 dark:hover:bg-white/8 dark:hover:text-white",
        destructive: "bg-red-50 text-red-700 ring-1 ring-red-700/15 hover:bg-red-100 dark:bg-red-950/50 dark:text-red-300 dark:ring-red-400/20 dark:hover:bg-red-950/70",
        danger: "bg-red-50 text-red-700 ring-1 ring-red-700/15 hover:bg-red-100 dark:bg-red-950/50 dark:text-red-300 dark:ring-red-400/20 dark:hover:bg-red-950/70",
        link: "text-emerald-700 underline-offset-4 hover:underline dark:text-emerald-400",
      },
      size: {
        default: "h-9 px-3",
        xs: "h-6 px-2 text-xs",
        sm: "h-7 px-2.5 text-xs",
        compact: "h-7 px-2.5",
        icon: "size-9 p-0",
        "icon-sm": "size-7 p-0",
      },
    },
    defaultVariants: { variant: "secondary", size: "default" },
  },
);

export interface ButtonProps
  extends ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export function Button({ asChild, className, variant, size, children, ...props }: ButtonProps) {
  const Component = asChild ? Slot : "button";
  return (
    <Component className={cn(buttonVariants({ variant, size }), className)} {...props}>
      {children}
      {size === "icon" || size === "icon-sm" ? (
        <span
          className="absolute top-1/2 left-1/2 size-[max(100%,3rem)] -translate-1/2 pointer-fine:hidden"
          aria-hidden="true"
        />
      ) : null}
    </Component>
  );
}
