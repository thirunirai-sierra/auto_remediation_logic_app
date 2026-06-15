/**
 * @fileoverview Shared dashboard presentation: chart colors, date formatting, status badges,
 * KPI tiles, Recharts legend helper, and skeleton loaders for `OverviewTab`.
 */
import { STATUS_CONFIG, normalizeStatusKey } from "../../../components/StatusPill.tsx";
import styles from "../dashboard.module.css";

/** Default slice colors for Recharts pie/bar series (cycled by index). */
export const CHART_COLORS = ["#ff6b6b", "#4dabf7", "#ffd43b", "#69db7c", "#845ef7", "#f06595", "#74c0fc"];

/**
 * Formats an ISO (or ISO-like) timestamp for dashboard tables.
 * @param {string | null | undefined} value - Instant from API; empty becomes `"-"`.
 * @returns {string} Locale string (`en-GB`) or original `value` if the date is invalid.
 */
export function formatRcaConfidence(value: unknown): { label: string; color: string } {
  if (value == null || value === "") return { label: "-", color: "#94a3b8" };
  const n = typeof value === "number" ? value : parseFloat(String(value));
  if (Number.isNaN(n)) return { label: "-", color: "#94a3b8" };
  const pct = n <= 1 ? n * 100 : n;
  const label = `${pct.toFixed(0)}%`;
  if (pct >= 90) return { label, color: "#16a34a" };
  if (pct >= 70) return { label, color: "#d97706" };
  return { label, color: "#dc2626" };
}

export function formatISODate(value: string | null | undefined): string {
  if (!value) return "-";
  const d = new Date(value);
  if (isNaN(d.getTime())) return value;
  return d.toLocaleString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

/** Maps coarse incident status keys to CSS module class names (legacy). */
export const INCIDENT_STATE: Record<string, string> = {
  RCA_COMPLETE: styles.stateSuccess,
  IN_PROGRESS: styles.stateWarning,
  PENDING: styles.stateNone,
  FAILED: styles.stateError,
  FIX_APPLIED: styles.stateSuccess,
};

/**
 * Pill badge for dashboard table rows: same color rules as Observability / Pipeline.
 */
export function StatusBadge({ status }: { status: string }) {
  const key = normalizeStatusKey(status);
  const cfg = STATUS_CONFIG[key];
  if (!cfg) return <span className={styles.statusError}>{status}</span>;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: "0.3rem", padding: "0.2rem 0.6rem", borderRadius: "999px", background: cfg.bg, color: cfg.color, fontSize: "0.75rem", fontWeight: 600, whiteSpace: "nowrap" }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: cfg.dot, flexShrink: 0 }} />
      {cfg.label}
    </span>
  );
}

/**
 * Single KPI metric card with optional subheader, unit, trend arrow, and tooltip.
 * @param {object} props - Component props.
 * @param {string} props.header - Primary label shown at top of card.
 * @param {string} [props.subheader] - Secondary line under header.
 * @param {unknown} props.value - Main numeric or textual value (coerced with `String`).
 * @param {string} [props.unit] - Suffix after value (e.g. `%`, `Min`).
 * @param {"Up" | "Down"} [props.indicator] - Optional trend arrow appended to value.
 * @param {"Good" | "Critical"} [props.valueColor] - Drives success vs danger text styling.
 * @param {string} [props.tooltip] - If set, exposed as `data-tip` for shell tooltips.
 * @param {string} [props.icon] - Optional emoji/symbol in card header.
 * @returns {JSX.Element} KPI card markup.
 */
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

/**
 * Split KPI: shows FIX FAILED vs AUTO FIXED side by side in one card.
 * @param {object} props - Component props.
 * @param {unknown} props.fixFailed - Count or label for failed fixes column.
 * @param {unknown} props.autoFixed - Count or label for auto-fixed column.
 * @param {string} [props.tooltip] - Optional `data-tip` for the whole card.
 * @returns {JSX.Element} Two-column KPI card.
 */
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

/**
 * Section heading for chart/table blocks on the overview page.
 * @param {object} props - Component props.
 * @param {string} props.title - Visible section title.
 * @returns {JSX.Element} Styled `h3`.
 */
export function SectionTitle({ title }: { title: string }) {
  return <h3 className={styles.sectionTitle}>{title}</h3>;
}

/**
 * Custom Recharts legend: two-column grid of color swatch + label (scrollable).
 * @param {object} props - Component props.
 * @param {Array<{ value: string; color: string }>} [props.payload] - Legend entries from Recharts; omit or empty yields `null`.
 * @returns {JSX.Element | null} Legend grid or nothing.
 */
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

/**
 * Placeholder block while chart data is loading.
 * @returns {JSX.Element} Skeleton div sized for chart area.
 */
export function SkeletonChart() {
  return <div className={`${styles.skeleton} ${styles.skeletonChart}`} />;
}

/**
 * Table body placeholder: repeated skeleton rows for loading state.
 * @param {object} [props] - Optional props.
 * @param {number} [props.count=5] - Number of skeleton rows.
 * @param {number} [props.colSpan=9] - `<td colSpan>` for each row.
 * @returns {JSX.Element} Fragment of `<tr>` skeleton rows.
 */
export function SkeletonRows({ count = 5, colSpan = 9 }: { count?: number; colSpan?: number }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <tr key={i}><td colSpan={colSpan}><div className={`${styles.skeleton} ${styles.skeletonRow}`} /></td></tr>
      ))}
    </>
  );
}
