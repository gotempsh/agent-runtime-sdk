import * as Dialog from "@radix-ui/react-dialog";
import { Search, X } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import { docs, stripMarkdown } from "@/content/docs";

export function CommandPalette({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const results = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return docs;
    return docs.filter((doc) =>
      `${doc.title} ${doc.description} ${stripMarkdown(doc.body)}`
        .toLowerCase()
        .includes(normalized),
    );
  }, [query]);

  function select(slug: string) {
    navigate(`/docs/${slug}`);
    setQuery("");
    onOpenChange(false);
  }

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content
          className="command-dialog"
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            inputRef.current?.focus();
          }}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setActiveIndex((index) => Math.min(index + 1, results.length - 1));
            }
            if (event.key === "ArrowUp") {
              event.preventDefault();
              setActiveIndex((index) => Math.max(index - 1, 0));
            }
            if (event.key === "Enter" && results[activeIndex]) {
              event.preventDefault();
              select(results[activeIndex].slug);
            }
          }}
        >
          <Dialog.Title className="sr-only">Search documentation</Dialog.Title>
          <Dialog.Description className="sr-only">
            Search tutorials, guides, reference pages, and architecture explanations.
          </Dialog.Description>
          <div className="command-input-row">
            <Search className="size-5 shrink-0 sm:size-4" aria-hidden="true" />
            <input
              ref={inputRef}
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setActiveIndex(0);
              }}
              placeholder="Search the Rust SDK"
              aria-label="Search documentation"
            />
            <Dialog.Close className="icon-button" aria-label="Close search">
              <span className="touch-target" aria-hidden="true" />
              <X className="size-5 shrink-0 sm:size-4" aria-hidden="true" />
            </Dialog.Close>
          </div>
          <div className="command-results" role="listbox" aria-label="Documentation results">
            {results.length ? (
              results.slice(0, 8).map((doc, index) => (
                <button
                  key={doc.slug}
                  type="button"
                  className="command-result"
                  data-active={index === activeIndex}
                  role="option"
                  aria-selected={index === activeIndex}
                  onMouseMove={() => setActiveIndex(index)}
                  onClick={() => select(doc.slug)}
                >
                  <span className="command-result-copy">
                    <strong>{doc.title}</strong>
                    <span>{doc.description}</span>
                  </span>
                  <span className="command-category">{doc.category}</span>
                </button>
              ))
            ) : (
              <p className="command-empty">No documentation matches “{query}”.</p>
            )}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
