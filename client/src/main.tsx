import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "@ui5/webcomponents-react";
import "@ui5/webcomponents/dist/Assets.js";
import "@ui5/webcomponents-fiori/dist/Assets.js";
// Import only the icons actually used instead of AllIcons.js (saves ~500 KB)
import "@ui5/webcomponents-icons/dist/home.js";
import "@ui5/webcomponents-icons/dist/settings.js";
import "@ui5/webcomponents-icons/dist/alert.js";
import "@ui5/webcomponents-icons/dist/information.js";
import "@ui5/webcomponents-icons/dist/error.js";
import "@ui5/webcomponents-icons/dist/status-positive.js";
import "@ui5/webcomponents-icons/dist/status-negative.js";
import "@ui5/webcomponents-icons/dist/status-in-process.js";
import "@ui5/webcomponents-icons/dist/refresh.js";
import "@ui5/webcomponents-icons/dist/search.js";
import "@ui5/webcomponents-icons/dist/play.js";
import "@ui5/webcomponents-icons/dist/stop.js";
import "@ui5/webcomponents-icons/dist/download.js";
import "@ui5/webcomponents-icons/dist/filter.js";
import "@ui5/webcomponents-icons/dist/list.js";
import "@ui5/webcomponents-icons/dist/menu2.js";
import "@ui5/webcomponents-icons/dist/action.js";
import "@ui5/webcomponents-icons/dist/bell.js";
import "@ui5/webcomponents-icons/dist/user-settings.js";
import App from "./App.tsx";
import "./styles/global.css";

// Apply saved theme on startup before first render
(function applyStartupTheme() {
  const themes: Record<string, Record<string, string>> = {
    "plain":       { "--orbit-sidebar-bg":"#f8fafc","--orbit-sidebar-text":"#475569","--orbit-topbar-bg":"#ffffff","--orbit-sidebar-active":"#eff6ff","--orbit-nav-active-text":"#2563eb","--orbit-blue":"#3b82f6" },
    "sap-horizon": { "--orbit-sidebar-bg":"#354a5e","--orbit-sidebar-text":"#d1dce6","--orbit-topbar-bg":"#354a5e","--orbit-sidebar-active":"#0070f21a","--orbit-nav-active-text":"#6eb5f5","--orbit-blue":"#0070f2" },
    "azure-blue":  { "--orbit-sidebar-bg":"#0078d4","--orbit-sidebar-text":"#e8f3fd","--orbit-topbar-bg":"#0078d4","--orbit-sidebar-active":"#ffffff1a","--orbit-nav-active-text":"#ffffff","--orbit-blue":"#50abf1" },
    "aurora":      { "--orbit-sidebar-bg":"#1e3a5f","--orbit-sidebar-text":"#cbd5e1","--orbit-topbar-bg":"#1e3a5f","--orbit-sidebar-active":"#3b82f633","--orbit-nav-active-text":"#93c5fd","--orbit-blue":"#3b82f6" },
    "fresh":       { "--orbit-sidebar-bg":"#064e3b","--orbit-sidebar-text":"#a7f3d0","--orbit-topbar-bg":"#064e3b","--orbit-sidebar-active":"#10b9811a","--orbit-nav-active-text":"#6ee7b7","--orbit-blue":"#10b981" },
    "prism":       { "--orbit-sidebar-bg":"#4c1d95","--orbit-sidebar-text":"#ddd6fe","--orbit-topbar-bg":"#4c1d95","--orbit-sidebar-active":"#8b5cf61a","--orbit-nav-active-text":"#c4b5fd","--orbit-blue":"#8b5cf6" },
    "mono":        { "--orbit-sidebar-bg":"#1f2937","--orbit-sidebar-text":"#9ca3af","--orbit-topbar-bg":"#1f2937","--orbit-sidebar-active":"#6b72801a","--orbit-nav-active-text":"#d1d5db","--orbit-blue":"#6b7280" },
    "brutal":      { "--orbit-sidebar-bg":"#18181b","--orbit-sidebar-text":"#fbbf24","--orbit-topbar-bg":"#09090b","--orbit-sidebar-active":"#fbbf241a","--orbit-nav-active-text":"#fde68a","--orbit-blue":"#fbbf24" },
    "dark":        { "--orbit-sidebar-bg":"#0f172a","--orbit-sidebar-text":"#94a3b8","--orbit-topbar-bg":"#020617","--orbit-sidebar-active":"#38bdf81a","--orbit-nav-active-text":"#7dd3fc","--orbit-blue":"#38bdf8" },
    "terminal":    { "--orbit-sidebar-bg":"#0a0a0a","--orbit-sidebar-text":"#22c55e","--orbit-topbar-bg":"#000000","--orbit-sidebar-active":"#22c55e1a","--orbit-nav-active-text":"#86efac","--orbit-blue":"#22c55e" },
    "nord":        { "--orbit-sidebar-bg":"#2e3440","--orbit-sidebar-text":"#d8dee9","--orbit-topbar-bg":"#2e3440","--orbit-sidebar-active":"#88c0d01a","--orbit-nav-active-text":"#8fbcbb","--orbit-blue":"#88c0d0" },
    "copper":      { "--orbit-sidebar-bg":"#1c0a00","--orbit-sidebar-text":"#d97706","--orbit-topbar-bg":"#0f0500","--orbit-sidebar-active":"#b453091a","--orbit-nav-active-text":"#fbbf24","--orbit-blue":"#b45309" },
  };
  const id = localStorage.getItem("orbit-theme") ?? "aurora";
  const vars = themes[id] ?? themes["aurora"];
  const root = document.documentElement;
  Object.entries(vars).forEach(([k, v]) => root.style.setProperty(k, v));
  const fontSize = localStorage.getItem("orbit-font");
  if (fontSize) root.style.fontSize = fontSize === "S" ? "13px" : fontSize === "L" ? "16px" : "14px";
})();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30_000,
    },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </ThemeProvider>
  </React.StrictMode>
);
