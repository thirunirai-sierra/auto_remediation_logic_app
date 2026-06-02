/**
 * @fileoverview Application shell routing: lazy feature pages under `ShellLayout`; `/` redirects to `/dashboard`.
 */
import { lazy, Suspense } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import ShellLayout from "./components/layout/shell-layout.tsx";

const Dashboard = lazy(() => import("./pages/dashboard/dashboard.tsx"));
const Orchestrator = lazy(() => import("./pages/orchestrator/orchestrator.tsx"));
const Observability = lazy(() => import("./pages/observability/observability.tsx"));
const MigrationWizard = lazy(() => import("./pages/migration-wizard/migration-wizard.tsx"));
const PipoList = lazy(() => import("./pages/pipo-list/pipo-list.tsx"));
const Pipeline = lazy(() => import("./pages/pipeline/pipeline.tsx"));
const Settings = lazy(() => import("./pages/settings/settings.tsx"));

/**
 * Root React tree for authenticated area: routes + suspense fallback.
 * @returns {JSX.Element} `ShellLayout` wrapping `Routes`.
 */
export default function App() {
  return (
    <ShellLayout>
      <Suspense fallback={<div style={{ padding: "1rem" }}>Loading...</div>}>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/orchestrator" element={<Orchestrator />} />
          <Route path="/orchestrator/:id" element={<Orchestrator />} />
          <Route path="/observability" element={<Observability />} />
          <Route path="/migration" element={<MigrationWizard />} />
          <Route path="/pipo" element={<PipoList />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </Suspense>
    </ShellLayout>
  );
}
