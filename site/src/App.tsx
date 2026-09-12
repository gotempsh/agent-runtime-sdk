import { lazy, Suspense, useEffect } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { LandingPage } from "@/pages/landing-page";

const DocsPage = lazy(() =>
  import("@/pages/docs-page").then((module) => ({ default: module.DocsPage })),
);

function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => {
    window.scrollTo({ top: 0 });
  }, [pathname]);
  return null;
}

export default function App() {
  return (
    <div className="app-shell">
      <ScrollToTop />
      <SiteHeader />
      <Routes>
        <Route path="/" element={<LandingPage />} />
        <Route
          path="/docs/:slug"
          element={(
            <Suspense fallback={<main className="docs-loading">Loading documentation…</main>}>
              <DocsPage />
            </Suspense>
          )}
        />
        <Route path="/docs" element={<Navigate to="/docs/quickstart" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      <SiteFooter />
    </div>
  );
}
