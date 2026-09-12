import { Link } from "react-router-dom";

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="footer-inner">
        <div>
          <Link to="/" className="footer-wordmark" aria-label="Homepage">t::agent-runtime</Link>
          <p>One typed process boundary for coding-agent CLIs.</p>
        </div>
        <nav aria-label="Footer navigation">
          <Link to="/docs/quickstart">Quickstart</Link>
          <a href={`${import.meta.env.BASE_URL}api/temps_agent_runtime/index.html`}>Rust API</a>
          <a href="https://github.com/gotempsh/agent-runtime-sdk">Source</a>
          <a href="https://github.com/gotempsh/agent-runtime-sdk/blob/main/LICENSE-MIT">MIT / Apache-2.0</a>
        </nav>
      </div>
    </footer>
  );
}
