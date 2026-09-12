import { ArrowLeft, ArrowRight, ExternalLink, Menu } from "lucide-react";
import { useEffect, useMemo } from "react";
import Markdown from "react-markdown";
import { Link, Navigate, useParams } from "react-router-dom";
import remarkGfm from "remark-gfm";

import { categories, docs, docsBySlug, docsBySourcePath, type DocPage } from "@/content/docs";

function headingId(children: unknown) {
  return String(children)
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-");
}

function headings(markdown: string) {
  return Array.from(markdown.matchAll(/^##\s+(.+)$/gm)).map((match) => ({
    title: match[1].replace(/[`*_]/g, ""),
    id: headingId(match[1].replace(/[`*_]/g, "")),
  }));
}

function resolveMarkdownLink(current: DocPage, href?: string) {
  if (!href || href.startsWith("http") || href.startsWith("#") || href.startsWith("mailto:")) return null;
  const [path, hash] = href.split("#");
  if (!path.endsWith(".md")) return null;
  const currentParts = current.sourcePath.split("/");
  currentParts.pop();
  for (const part of path.split("/")) {
    if (part === "..") currentParts.pop();
    else if (part !== ".") currentParts.push(part);
  }
  const target = docsBySourcePath.get(currentParts.join("/"));
  return target ? `/docs/${target.slug}${hash ? `#${hash}` : ""}` : null;
}

function DocsNavigation({ current }: { current: DocPage }) {
  return (
    <nav className="docs-nav" aria-label="Documentation navigation">
      {categories.map((category) => (
        <div className="docs-nav-group" key={category}>
          <h2>{category}</h2>
          <ul role="list">
            {docs.filter((doc) => doc.category === category).map((doc) => (
              <li key={doc.slug}>
                <Link to={`/docs/${doc.slug}`} aria-current={doc.slug === current.slug ? "page" : undefined}>{doc.title}</Link>
              </li>
            ))}
          </ul>
        </div>
      ))}
      <div className="docs-nav-group">
        <h2>Generated API</h2>
        <a className="generated-api-link" href={`${import.meta.env.BASE_URL}api/temps_agent_runtime/index.html`}>
          Rustdoc <ExternalLink className="size-4 shrink-0" aria-hidden="true" />
        </a>
      </div>
    </nav>
  );
}

export function DocsPage() {
  const { slug } = useParams();
  const current = docsBySlug.get(slug ?? "");
  const currentIndex = current ? docs.indexOf(current) : -1;
  const tableOfContents = useMemo(() => current ? headings(current.body) : [], [current]);

  useEffect(() => {
    if (!current) return;
    document.title = `${current.title} — Agent Runtime for Rust`;
    return () => { document.title = "Agent Runtime for Rust"; };
  }, [current]);

  if (!current) return <Navigate to="/docs/quickstart" replace />;

  return (
    <main className="isolate docs-main">
      <div className="docs-shell">
        <aside className="docs-sidebar"><DocsNavigation current={current} /></aside>
        <article className="docs-article">
          <div className="mobile-docs-nav">
            <details>
              <summary><Menu className="size-5 shrink-0" aria-hidden="true" /> Browse documentation</summary>
              <DocsNavigation current={current} />
            </details>
          </div>
          <div className="docs-breadcrumbs">
            <Link to="/docs/quickstart">Documentation</Link><span>/</span><span>{current.category}</span>
          </div>
          <p className="docs-description">{current.description}</p>
          <div className="prose max-w-[76ch] text-pretty">
            <Markdown
              remarkPlugins={[remarkGfm]}
              components={{
                h1: ({ children }) => <h1>{children}</h1>,
                h2: ({ children }) => <h2 id={headingId(children)}>{children}</h2>,
                h3: ({ children }) => <h3 id={headingId(children)}>{children}</h3>,
                a: ({ href, children }) => {
                  const internal = resolveMarkdownLink(current, href);
                  return internal ? <Link to={internal}>{children}</Link> : <a href={href}>{children}</a>;
                },
                pre: ({ children }) => <pre tabIndex={0}>{children}</pre>,
                table: ({ children }) => <div className="prose-table-wrap"><table>{children}</table></div>,
              }}
            >{current.body}</Markdown>
          </div>
          <nav className="docs-pagination" aria-label="Adjacent documentation pages">
            {currentIndex > 0 ? (
              <Link to={`/docs/${docs[currentIndex - 1].slug}`} rel="prev">
                <ArrowLeft className="size-4 shrink-0" aria-hidden="true" />
                <span><small>Previous</small>{docs[currentIndex - 1].title}</span>
              </Link>
            ) : <span />}
            {currentIndex < docs.length - 1 ? (
              <Link to={`/docs/${docs[currentIndex + 1].slug}`} rel="next">
                <span><small>Next</small>{docs[currentIndex + 1].title}</span>
                <ArrowRight className="size-4 shrink-0" aria-hidden="true" />
              </Link>
            ) : <span />}
          </nav>
        </article>
        <aside className="docs-toc">
          <nav aria-label="On this page">
            <h2>On this page</h2>
            <ul role="list">
              {tableOfContents.map((heading) => <li key={heading.id}><a href={`#${heading.id}`}>{heading.title}</a></li>)}
            </ul>
            <a className="edit-link" href={`https://github.com/gotempsh/agent-runtime-sdk/blob/main/${current.sourcePath}`}>
              Edit this page <ExternalLink className="size-4 shrink-0" aria-hidden="true" />
            </a>
          </nav>
        </aside>
      </div>
    </main>
  );
}
