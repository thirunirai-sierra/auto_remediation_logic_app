import { useEffect, useRef, useState } from "react";
import type { AemStatusResponse, McpToolsStatus } from "../../../services/api";
import styles from "../EventMeshFlow.module.css";
import type { Incident } from "./shared";

export default function StatsRow({
  aemStatus,
  incidents,
  mcpTools,
}: {
  aemStatus: AemStatusResponse | null;
  incidents: Incident[];
  mcpTools: McpToolsStatus | null;
}) {
  const fixedStatuses = new Set(["FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED"]);
  const terminalStatuses = new Set([
    "FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED",
    "FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME",
    "RCA_FAILED", "PIPELINE_ERROR", "REJECTED", "TICKET_CREATED", "ARTIFACT_MISSING",
  ]);

  const fixedCount = incidents.filter((i) => fixedStatuses.has(i.status.toUpperCase())).length;
  const failedCount = incidents.filter((i) => i.status.toUpperCase().includes("FAIL") || i.status.toUpperCase() === "REJECTED").length;
  const inProgress = incidents.filter((i) => !terminalStatuses.has(i.status.toUpperCase())).length;

  const [mcpOpen, setMcpOpen] = useState(false);
  const mcpRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!mcpOpen) return;
    function onOutsideClick(e: MouseEvent) {
      if (mcpRef.current && !mcpRef.current.contains(e.target as Node)) setMcpOpen(false);
    }
    document.addEventListener("mousedown", onOutsideClick);
    return () => document.removeEventListener("mousedown", onOutsideClick);
  }, [mcpOpen]);

  const cards: Array<{ label: string; value: number | string; accent: string }> = [
    { label: "Webhooks Received", value: aemStatus?.messages_retrieved ?? 0, accent: "#3b82f6" },
    { label: "Total Incidents", value: aemStatus?.total_incidents ?? incidents.length, accent: "#6366f1" },
    { label: "Fixed & Verified", value: fixedCount, accent: "#22c55e" },
    { label: "In Progress", value: inProgress, accent: "#f59e0b" },
    { label: "Failed", value: failedCount, accent: "#ef4444" },
    { label: "Queue Depth", value: aemStatus?.queue_depth ?? 0, accent: "#8b5cf6" },
  ];
  const serverEntries = Object.entries(mcpTools?.servers ?? {});

  return (
    <div className={styles.statsRow}>
      {cards.map((card) => (
        <div key={card.label} className={styles.statCard} style={{ borderTopColor: card.accent }}>
          <span className={styles.statValue} style={{ color: card.accent }}>{card.value}</span>
          <span className={styles.statLabel}>{card.label}</span>
        </div>
      ))}

      <div ref={mcpRef} className={`${styles.statCard} ${styles.statCardMcp}`} style={{ borderTopColor: "#0ea5e9", position: "relative" }} onClick={() => setMcpOpen((o) => !o)}>
        <span className={styles.statValue} style={{ color: "#0ea5e9" }}>{mcpTools?.total ?? "—"}</span>
        <span className={styles.statLabel}>MCP Tools {mcpOpen ? "▴" : "▾"}</span>
        {mcpOpen && (
          <div className={styles.mcpPopover} onClick={(e) => e.stopPropagation()}>
            <div className={styles.mcpPopoverTitle}>Loaded MCP Tools</div>
            {serverEntries.length === 0 ? (
              <p className={styles.mcpPopoverEmpty}>No tools loaded yet</p>
            ) : (
              serverEntries.map(([server, tools]) => (
                <div key={server} className={styles.mcpPopoverServer}>
                  <div className={styles.mcpPopoverServerRow}>
                    <span className={styles.mcpPopoverServerName}>{server}</span>
                    <span className={styles.mcpPopoverCount}>{tools.length}</span>
                  </div>
                  <div className={styles.mcpPopoverTools}>{tools.map((t) => <span key={t} className={styles.mcpPopoverTool}>{t}</span>)}</div>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
}
