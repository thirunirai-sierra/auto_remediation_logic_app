import styles from "../dashboard.module.css";

export const CHART_COLORS = ["#ff6b6b", "#4dabf7", "#ffd43b", "#69db7c", "#845ef7", "#f06595", "#74c0fc"];

export function formatISODate(value: string | null | undefined): string {
  if (!value) return "-";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value;
  return d.toLocaleString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

export const INCIDENT_STATE: Record<string, string> = {
  RCA_COMPLETE: styles.stateSuccess,
  IN_PROGRESS: styles.stateWarning,
  PENDING: styles.stateNone,
  FAILED: styles.stateError,
  FIX_APPLIED: styles.stateSuccess,
};

const STATUS_BADGE_STYLES: Record<string, { bg: string; color: string; dot: string; label: string }> = {
  TICKET_CREATED: { bg: "#f3e8ff", color: "#7c3aed", dot: "#7c3aed", label: "Ticket Created" },
  ARTIFACT_MISSING: { bg: "#f1f5f9", color: "#64748b", dot: "#94a3b8", label: "Artifact Missing" },
  RCA_COMPLETED: { bg: "#dcfce7", color: "#16a34a", dot: "#16a34a", label: "RCA Completed" },
  RCA_COMPLETE: { bg: "#dcfce7", color: "#16a34a", dot: "#16a34a", label: "RCA Completed" },
  FIX_IN_PROGRESS: { bg: "#fff7ed", color: "#ea580c", dot: "#ea580c", label: "Fix In Progress" },
  IN_PROGRESS: { bg: "#fff7ed", color: "#ea580c", dot: "#ea580c", label: "Fix In Progress" },
  FIX_COMPLETED: { bg: "#dcfce7", color: "#16a34a", dot: "#16a34a", label: "Fix Completed" },
  RCA_IN_PROGRESS: { bg: "#fef3c7", color: "#d97706", dot: "#d97706", label: "RCA In Progress" },
  FAILED: { bg: "#fee2e2", color: "#dc2626", dot: "#dc2626", label: "Failed" },
  AUTO_FIXED: { bg: "#dcfce7", color: "#16a34a", dot: "#16a34a", label: "Auto Fixed" },
  FIX_FAILED: { bg: "#fee2e2", color: "#dc2626", dot: "#dc2626", label: "Fix Failed" },
  PENDING: { bg: "#f1f5f9", color: "#64748b", dot: "#94a3b8", label: "Pending" },
};

export function StatusBadge({ status }: { status: string }) {
  const key = status.toUpperCase().replace(/ /g, "_");
  const style = STATUS_BADGE_STYLES[key];
  if (!style) return <span className={styles.statusError}>{status}</span>;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: "0.3rem", padding: "0.2rem 0.6rem", borderRadius: "999px", background: style.bg, color: style.color, fontSize: "0.75rem", fontWeight: 600 }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: style.dot, flexShrink: 0 }} />
      {style.label}
    </span>
  );
}

export function KpiCard({ header, subheader, value, unit, indicator, valueColor, tooltip, icon }: {
  header: string; subheader?: string; value: unknown; unit?: string; indicator?: "Up" | "Down"; valueColor?: "Good" | "Critical"; tooltip?: string; icon?: string;
}) {
  const colorClass = valueColor === "Good" ? styles.valueGood : valueColor === "Critical" ? styles.valueCritical : "";
  const arrow = indicator === "Up" ? " ↑" : indicator === "Down" ? " ↓" : "";
  return (
    <div className={styles.kpiCard} {...(tooltip ? { "data-tip": tooltip } : {})}>
      <div className={styles.kpiCardTop}><div className={styles.kpiHeader}>{header}</div>{icon && <span className={styles.kpiIcon}>{icon}</span>}</div>
      {subheader && <div className={styles.kpiSub}>{subheader}</div>}
      <div className={`${styles.kpiValue} ${colorClass}`}>{String(value ?? "-")}{unit ? ` ${unit}` : ""}{arrow}</div>
    </div>
  );
}

export function SplitKpiCard({ fixFailed, autoFixed, tooltip }: { fixFailed: unknown; autoFixed: unknown; tooltip?: string }) {
  return (
    <div className={`${styles.kpiCard} ${styles.kpiCardSplit}`} {...(tooltip ? { "data-tip": tooltip } : {})}>
      <div className={styles.kpiSplitLeft}>
        <div className={styles.kpiHeader} style={{ color: "#dc2626" }}>FIX FAILED</div>
        <div className={`${styles.kpiValue} ${styles.valueCritical}`}>{String(fixFailed ?? "-")} <span className={styles.kpiArrowDown}>↓</span></div>
      </div>
      <div className={styles.kpiSplitDivider} />
      <div className={styles.kpiSplitRight}>
        <div className={styles.kpiHeader} style={{ color: "#16a34a" }}>AUTO FIXED</div>
        <div className={`${styles.kpiValue} ${styles.valueGood}`}>{String(autoFixed ?? "-")} <span className={styles.kpiArrowUp}>↑</span></div>
      </div>
    </div>
  );
}

export function SectionTitle({ title }: { title: string }) {
  return <h3 className={styles.sectionTitle}>{title}</h3>;
}

export function TwoColumnLegend({ payload }: { payload?: Array<{ value: string; color: string }> }) {
  if (!payload?.length) return null;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.2rem 1rem", fontSize: "0.78rem", padding: "0 0.75rem", maxHeight: 300, overflowY: "auto", alignSelf: "center" }}>
      {payload.map((entry, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: "0.35rem", minWidth: 0 }}>
          <span style={{ width: 9, height: 9, borderRadius: 2, background: entry.color, flexShrink: 0 }} />
          <span style={{ color: "#94a3b8", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{entry.value}</span>
        </div>
      ))}
    </div>
  );
}

export function SkeletonChart() {
  return <div className={`${styles.skeleton} ${styles.skeletonChart}`} />;
}

export function SkeletonRows({ count = 5, colSpan = 9 }: { count?: number; colSpan?: number }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <tr key={i}><td colSpan={colSpan}><div className={`${styles.skeleton} ${styles.skeletonRow}`} /></td></tr>
      ))}
    </>
  );
}
