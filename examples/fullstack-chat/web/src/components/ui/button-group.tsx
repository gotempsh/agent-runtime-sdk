import { Slot } from "radix-ui";

import { cn } from "@/lib/utils";
import { Separator } from "@/components/ui/separator";

export function ButtonGroup({ className, orientation = "horizontal", ...props }: React.ComponentProps<"div"> & { orientation?: "horizontal" | "vertical" }) {
  return (
    <div
      className={cn(
        "group/button-group flex w-fit items-stretch",
        orientation === "vertical" ? "flex-col" : "flex-row",
        className,
      )}
      data-orientation={orientation}
      data-slot="button-group"
      role="group"
      {...props}
    />
  );
}

export function ButtonGroupText({ className, asChild = false, ...props }: React.ComponentProps<"div"> & { asChild?: boolean }) {
  const Component = asChild ? Slot.Root : "div";
  return <Component className={cn("flex items-center gap-2 rounded-lg border bg-muted px-2.5 text-sm font-medium", className)} {...props} />;
}

export function ButtonGroupSeparator({ className, orientation = "vertical", ...props }: React.ComponentProps<typeof Separator>) {
  return <Separator className={cn("relative self-stretch bg-input", className)} data-slot="button-group-separator" orientation={orientation} {...props} />;
}
