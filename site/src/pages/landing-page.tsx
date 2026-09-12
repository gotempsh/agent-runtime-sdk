import { ArrowRight, Check, ShieldCheck, TerminalSquare } from "lucide-react";
import { Link } from "react-router-dom";

import { Button } from "@/components/ui/button";

const traceEvents = [
  ["00", "session_started", "session c_01HZA"],
  ["01", "reasoning_delta", "Inspecting workspace"],
  ["02", "tool_call", "cargo test · started"],
  ["03", "sandbox_access_denied", "/private/input · read"],
  ["04", "sandbox_profile_updated", "revision 12"],
  ["05", "sandbox_step_retrying", "step tool_7 · attempt 1"],
  ["06", "text_delta", "All tests pass."],
] as const;

const docPaths = [
  {
    label: "Tutorial",
    title: "Run your first turn",
    body: "Start one installed provider, consume typed events, and return a terminal result.",
    to: "/docs/quickstart",
    className: "doc-path doc-path-wide",
  },
  {
    label: "How-to",
    title: "Persist conversations",
    body: "Journal the event stream and restore durable chat state after reconnects.",
    to: "/docs/persistent-conversations",
    className: "doc-path",
  },
  {
    label: "Reference",
    title: "Read the event catalog",
    body: "Map every normalized event to the status your product should persist.",
    to: "/docs/event-catalog",
    className: "doc-path",
  },
  {
    label: "Explanation",
    title: "Understand the boundary",
    body: "See where provider adapters, process supervision, and application ownership meet.",
    to: "/docs/architecture",
    className: "doc-path doc-path-wide",
  },
] as const;

export function LandingPage() {
  return (
    <main className="isolate">
      <section className="hero-section">
        <div className="page-container hero-grid">
          <div className="hero-copy">
            <p className="machine-label">Rust SDK · early 0.1</p>
            <h1>Run coding agents from Rust.</h1>
            <p className="hero-lede">
              Start Claude Code, Codex, or OpenCode through one typed event stream. Keep permissions explicit, supervise the process tree, and add an OS sandbox when the turn needs one.
            </p>
            <div className="hero-actions">
              <Button asChild variant="primary">
                <Link to="/docs/quickstart">Run your first turn <ArrowRight className="size-4 shrink-0" aria-hidden="true" /></Link>
              </Button>
              <a className="text-link" href={`${import.meta.env.BASE_URL}api/temps_agent_runtime/index.html`}>Read the generated API</a>
            </div>
            <p className="install-line"><span>$</span> cargo add temps-agent-runtime</p>
          </div>
          <div className="hero-workbench" role="group" aria-label="Rust SDK example and normalized event output">
            <div className="workbench-meta">
              <span>turn.rs</span>
              <span>typed · backpressured</span>
            </div>
            <pre tabIndex={0}><code><span className="code-key">let</span> runtime = AgentRuntime::builder(){"\n"}    .concurrency_limit(<span className="code-number">2</span>){"\n"}    .build()?;{"\n\n"}<span className="code-key">let</span> request = TurnRequest::new({"\n"}    Provider::Claude,{"\n"}    workspace,{"\n"}    <span className="code-string">"Run the tests."</span>,{"\n"});{"\n\n"}<span className="code-key">let</span> result = runtime{"\n"}    .run(request, &events, None){"\n"}    .await?;</code></pre>
            <div className="workbench-status"><span className="status-dot" /> succeeded <span>·</span> 8.4s</div>
          </div>
        </div>
      </section>

      <section className="trace-section">
        <div className="page-container trace-layout">
          <div className="trace-copy">
            <p className="machine-label machine-label-dark">Normalized event stream</p>
            <h2>Follow the turn while it is happening.</h2>
            <p>
              The runtime translates provider-specific JSON into a stable Rust enum. Persist the same events you send to SSE, WebSocket, or a terminal client.
            </p>
            <Link className="dark-link" to="/docs/event-catalog">Inspect every event <ArrowRight className="size-4 shrink-0" aria-hidden="true" /></Link>
          </div>
          <div className="event-trace" role="list" aria-label="Example agent events">
            {traceEvents.map(([sequence, event, detail]) => (
              <div className="trace-row" role="listitem" key={sequence}>
                <span className="trace-sequence">{sequence}</span>
                <span className="trace-event">{event}</span>
                <span className="trace-detail">{detail}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="providers-section">
        <div className="page-container section-grid">
          <div className="sticky-section-head">
            <p className="machine-label">Provider contract</p>
            <h2>One caller. Three installed agents.</h2>
            <p>Choose the CLI at runtime without rewriting process supervision, cancellation, permissions, or event parsing.</p>
          </div>
          <div className="provider-sheet-wrap">
            <table className="provider-sheet">
              <thead><tr><th>Capability</th><th>Claude</th><th>Codex</th><th>OpenCode</th></tr></thead>
              <tbody>
                {[
                  ["Typed text + reasoning", true, true, true],
                  ["Tool lifecycle", true, true, true],
                  ["Session resume", true, true, true],
                  ["Plan mode", true, true, true],
                  ["Live approvals", true, false, false],
                  ["Outer sandbox", true, true, true],
                ].map(([name, ...values]) => (
                  <tr key={String(name)}>
                    <th>{name}</th>
                    {values.map((value, index) => (
                      <td key={index} aria-label={value ? "Supported" : "Not supported"}>
                        {value ? <Check className="size-4 shrink-0" aria-hidden="true" /> : <span className="not-supported">—</span>}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="sheet-note">Live interaction support depends on the provider protocol. Static permission modes remain available for every adapter.</p>
          </div>
        </div>
      </section>

      <section className="recovery-section">
        <div className="page-container recovery-grid">
          <div className="recovery-intro">
            <ShieldCheck className="size-6 shrink-0" aria-hidden="true" />
            <h2>A denied path is a state transition, not a dead end.</h2>
            <p>
              With a managed profile, the host can ask for approval, commit an exact revision, and resume the same provider session. The runtime never retries outside the sandbox.
            </p>
            <Link className="text-link" to="/docs/sandbox-recovery">Implement recovery</Link>
          </div>
          <dl className="recovery-steps">
            <div><dt><span>1.0</span> Detect</dt><dd>Nono classifies a failed tool event and extracts the denied resource.</dd></div>
            <div><dt><span>2.0</span> Approve</dt><dd>Your recovery handler chooses the exact least-privilege profile change.</dd></div>
            <div><dt><span>3.0</span> Commit</dt><dd>Your profile manager validates and atomically writes the next revision.</dd></div>
            <div><dt><span>4.0</span> Resume</dt><dd>The SDK reopens the same session and asks it to retry only the blocked operation.</dd></div>
          </dl>
        </div>
      </section>

      <section className="docs-index-section">
        <div className="page-container">
          <div className="section-heading">
            <TerminalSquare className="size-6 shrink-0" aria-hidden="true" />
            <h2>Read according to the job in front of you.</h2>
            <p>Start with a working turn, solve one integration problem, look up exact behavior, or understand the runtime boundary.</p>
          </div>
          <div className="docs-path-grid">
            {docPaths.map((path) => (
              <Link className={path.className} to={path.to} key={path.title}>
                <span className="machine-label">{path.label}</span>
                <strong>{path.title}</strong>
                <span>{path.body}</span>
                <ArrowRight className="size-4 shrink-0" aria-hidden="true" />
              </Link>
            ))}
          </div>
        </div>
      </section>

      <section className="closing-section">
        <div className="page-container closing-inner">
          <h2>Add one runtime boundary. Keep the product yours.</h2>
          <Link className="closing-link" to="/docs/quickstart">Open the five-minute quickstart <ArrowRight className="size-4 shrink-0" aria-hidden="true" /></Link>
        </div>
      </section>
    </main>
  );
}
