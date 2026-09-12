import { Collapsible as CollapsiblePrimitive } from "radix-ui";

export function Collapsible(props: React.ComponentProps<typeof CollapsiblePrimitive.Root>) {
  return <CollapsiblePrimitive.Root data-slot="collapsible" {...props} />;
}

export function CollapsibleTrigger(props: React.ComponentProps<typeof CollapsiblePrimitive.Trigger>) {
  return <CollapsiblePrimitive.Trigger data-slot="collapsible-trigger" {...props} />;
}

export function CollapsibleContent(props: React.ComponentProps<typeof CollapsiblePrimitive.Content>) {
  return <CollapsiblePrimitive.Content data-slot="collapsible-content" {...props} />;
}
