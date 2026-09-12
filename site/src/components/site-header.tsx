import * as Dialog from "@radix-ui/react-dialog";
import { ExternalLink, Menu, Search, X } from "lucide-react";
import { lazy, Suspense, useEffect, useState } from "react";
import { Link, NavLink } from "react-router-dom";

import { Button } from "@/components/ui/button";

const CommandPalette = lazy(() =>
  import("@/components/command-palette").then((module) => ({ default: module.CommandPalette })),
);

function apiHref() {
  return `${import.meta.env.BASE_URL}api/temps_agent_runtime/index.html`;
}

function MobileNavigation() {
  return (
    <Dialog.Root>
      <Dialog.Trigger className="icon-button mobile-nav-trigger" aria-label="Open navigation">
        <span className="touch-target" aria-hidden="true" />
        <Menu className="size-5 shrink-0" aria-hidden="true" />
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="mobile-nav-panel">
          <div className="mobile-nav-head">
            <Dialog.Title>Navigate</Dialog.Title>
            <Dialog.Close className="icon-button" aria-label="Close navigation">
              <span className="touch-target" aria-hidden="true" />
              <X className="size-5 shrink-0" aria-hidden="true" />
            </Dialog.Close>
          </div>
          <Dialog.Description className="mobile-nav-description">
            Open the SDK documentation, generated API reference, or source repository.
          </Dialog.Description>
          <nav className="mobile-nav-links" aria-label="Mobile navigation">
            <Dialog.Close asChild><Link to="/">Overview</Link></Dialog.Close>
            <Dialog.Close asChild><Link to="/docs/quickstart">Docs</Link></Dialog.Close>
            <a href={apiHref()}>Rust API <ExternalLink className="size-4 shrink-0" aria-hidden="true" /></a>
            <a href="https://github.com/gotempsh/agent-runtime-sdk">GitHub <ExternalLink className="size-4 shrink-0" aria-hidden="true" /></a>
          </nav>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

export function SiteHeader() {
  const [searchOpen, setSearchOpen] = useState(false);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setSearchOpen(true);
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  return (
    <>
      <header className="site-header">
        <div className="header-inner">
          <div className="header-side">
            <Link to="/" className="wordmark" aria-label="Homepage">
              <span aria-hidden="true">t::</span>agent-runtime
            </Link>
          </div>
          <nav className="desktop-nav" aria-label="Primary navigation">
            <NavLink to="/" end>Overview</NavLink>
            <NavLink to="/docs/quickstart">Docs</NavLink>
            <a href={apiHref()}>Rust API</a>
          </nav>
          <div className="header-side header-actions">
            <button
              type="button"
              className="search-trigger"
              aria-label="Search documentation"
              onClick={() => setSearchOpen(true)}
            >
              <Search className="size-4 shrink-0" aria-hidden="true" />
              <span>Search</span>
              <kbd>⌘K</kbd>
            </button>
            <Button asChild variant="secondary" size="compact" className="github-button">
              <a href="https://github.com/gotempsh/agent-runtime-sdk">GitHub</a>
            </Button>
            <MobileNavigation />
          </div>
        </div>
      </header>
      {searchOpen ? (
        <Suspense fallback={null}>
          <CommandPalette open={searchOpen} onOpenChange={setSearchOpen} />
        </Suspense>
      ) : null}
    </>
  );
}
