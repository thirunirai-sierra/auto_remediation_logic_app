/**
 * @fileoverview Dashboard overview tab: KPI row, Recharts (status, errors, top iflows, timeline),
 * and two paginated tables (recent failed messages, active incidents). Presentation-only — parent owns queries and pagination.
 */
import {
  PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, LineChart, Line,
} from "recharts";
import Pagination from "../../../components/pagination/Pagination";
import styles from "../dashboard.module.css";
import {
  CHART_COLORS,
  formatISODate,
  INCIDENT_STATE,
  KpiCard,
  SectionTitle,
  SkeletonChart,
  SkeletonRows,
  SplitKpiCard,
  StatusBadge,
  TwoColumnLegend,
} from "./DashboardCommon";

type RecentFail = { message_guid: unknown; iflow_name: unknown; status: unknown; log_end: unknown };
type ActiveIncident = {
  incident_id: unknown; message_guid: unknown; iflow_id: unknown; error_type: unknown; status: unknown;
  created_at: unknown; last_seen: unknown; occurrence_count: unknown; rca_confidence: unknown;
};

/**
 * Renders the main dashboard overview content passed from `Dashboard`.
 * @param {object} props - KPI, chart series, table rows, loading flags, pagination, and callbacks.
 * @param {boolean} props.dashLoading - When true, KPI row and charts show skeletons.
 * @param {Record<string, unknown>} props.kpi - KPI fields consumed by `KpiCard` / `SplitKpiCard`.
 * @param {Array<{ status: string; count: number }>} props.statusData - Pie chart data for status breakdown.
 * @param {Array<{ error_type: string; count: number }>} props.errorData - Pie chart data for error distribution.
 * @param {Array<{ iflow_name: string; failure_count: number }>} props.iflowData - Horizontal bar chart rows.
 * @param {Array<{ time: string; count: number }>} props.timelineData - Line chart points over time.
 * @param {RecentFail[]} props.recentFails - Current page of recent failure rows.
 * @param {ActiveIncident[]} props.activeInc - Current page of active incident rows.
 * @param {boolean} props.incidentsError - True when log-incidents query failed.
 * @param {string} props.incidentsErrorMessage - Error text for incidents table banner.
 * @param {() => void} props.onFailuresPrev - Previous page for failures table.
 * @param {() => void} props.onFailuresNext - Next page for failures table.
 * @param {(s: number) => void} props.onFailuresPageSizeChange - Change page size for failures table.
 * @param {() => void} props.onIncidentsPrev - Previous page for incidents table.
 * @param {() => void} props.onIncidentsNext - Next page for incidents table.
 * @param {(s: number) => void} props.onIncidentsPageSizeChange - Change page size for incidents table.
 * @returns {JSX.Element} Fragment containing KPIs, charts, and tables.
 */
export default function OverviewTab(props: {  dashLoading: boolean;
  kpi: Record<string, unknown>;
  statusData: { status: string; count: number }[];
  errorData: { error_type: string; count: number }[];
  iflowData: { iflow_name: string; failure_count: number }[];
  timelineData: { time: string; count: number }[];
  failuresLoading: boolean;
  failuresFetching: boolean;
  failuresTotalCount: number;
  recentFails: RecentFail[];
  failuresPage: number;
  failuresTotalPages: number;
  failuresPageSize: number;
  failuresHasNext: boolean;
  failuresHasPrev: boolean;
  onFailuresPrev: () => void;
  onFailuresNext: () => void;
  onFailuresPageSizeChange: (s: number) => void;
  incidentsError: boolean;
  incidentsErrorMessage: string;
  incidentsFetching: boolean;
  incidentsLoading: boolean;
  incidentsTotalCount: number;
  activeInc: ActiveIncident[];
  incidentsPage: number;
  incidentsTotalPages: number;
  incidentsPageSize: number;
  incidentsHasNext: boolean;
  incidentsHasPrev: boolean;
  onIncidentsPrev: () => void;
  onIncidentsNext: () => void;
  onIncidentsPageSizeChange: (s: number) => void;
}) {
  const {
    dashLoading, kpi, statusData, errorData, iflowData, timelineData, failuresLoading, failuresFetching,
    failuresTotalCount, recentFails, failuresPage, failuresTotalPages, failuresPageSize, failuresHasNext, failuresHasPrev,
    onFailuresPrev, onFailuresNext, onFailuresPageSizeChange, incidentsError, incidentsErrorMessage, incidentsFetching, incidentsLoading,
    incidentsTotalCount, activeInc, incidentsPage, incidentsTotalPages, incidentsPageSize, incidentsHasNext, incidentsHasPrev,
    onIncidentsPrev, onIncidentsNext, onIncidentsPageSizeChange,
  } = props;

  return (
    <>
      <div className={styles.kpiRow}>
        {dashLoading ? Array.from({ length: 9 }).map((_, i) => <div key={i} className={styles.kpiCard}><div className={styles.skeleton} style={{ height: "0.75rem", width: "70%" }} /><div className={`${styles.skeleton} ${styles.skeletonKpiValue}`} /></div>) : (
          <>
            <KpiCard header="In Progress" value={kpi.in_progress} tooltip="Incidents currently being analyzed or fixed by pipeline agents" icon="⟳" />
            <KpiCard header="Total Incidents" value={kpi.total_incidents} tooltip="All incidents tracked by the auto-remediation pipeline, including resolved and active" icon="⚠" />
            <KpiCard header="Pending Approval" value={kpi.pending_approval} tooltip="Fixes generated but awaiting manual approval before deployment to production" icon="📋" />
            <SplitKpiCard fixFailed={kpi.fix_failed} autoFixed={kpi.auto_fixed} tooltip="Fix Failed vs Auto Fixed counts" />
            <KpiCard header="Failed Messages" subheader="Live" value={kpi.total_failed_messages} tooltip="SAP CPI messages currently in FAILED state, polled live from the message processing log" icon="✉" />
            <KpiCard header="Auto Fix Rate" value={kpi.auto_fix_rate} unit="%" tooltip="Percentage of incidents resolved automatically vs all closed incidents" icon="⚙" />
            <KpiCard header="Avg Resolution Time" subheader="Min" value={kpi.avg_resolution_time_minutes} unit="Min" tooltip="Mean time from incident detection to terminal state (auto-fixed or failed)" icon="⏱" />
            <KpiCard header="RCA Coverage" value={kpi.rca_coverage_percent} tooltip="Percentage of incidents that received AI-powered root cause analysis" icon="📊" />
          </>
        )}
      </div>

      <div className={styles.chartsRow}>
        <div className={styles.chartHalf}>
          <SectionTitle title="Status Breakdown" />
          {dashLoading ? <SkeletonChart /> : <ResponsiveContainer width="100%" height={300}><PieChart><Pie data={statusData} dataKey="count" nameKey="status" cx="38%" label>{statusData.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}</Pie><Tooltip /><Legend layout="vertical" align="right" verticalAlign="middle" content={(p) => <TwoColumnLegend payload={p.payload as Array<{ value: string; color: string }>} />} /></PieChart></ResponsiveContainer>}
        </div>
        <div className={styles.chartHalf}>
          <SectionTitle title="Error Distribution" />
          {dashLoading ? <SkeletonChart /> : <ResponsiveContainer width="100%" height={300}><PieChart><Pie data={errorData} dataKey="count" nameKey="error_type" innerRadius="38%" outerRadius="68%">{errorData.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}</Pie><Tooltip /><Legend layout="vertical" align="right" verticalAlign="middle" /></PieChart></ResponsiveContainer>}
        </div>
      </div>

      <div className={styles.chartBlock}>
        <SectionTitle title="Top Failing Integration Artifact" />
        {dashLoading ? <SkeletonChart /> : <ResponsiveContainer width="100%" height={320}><BarChart data={iflowData} layout="vertical" margin={{ left: 10, right: 30, top: 5, bottom: 5 }}><CartesianGrid strokeDasharray="3 3" horizontal={false} /><XAxis type="number" allowDecimals={false} tick={{ fontSize: 11 }} /><YAxis type="category" dataKey="iflow_name" width={190} tick={{ fontSize: 11 }} tickFormatter={(v: string) => v.length > 25 ? `${v.slice(0, 23)}…` : v} /><Tooltip /><Bar dataKey="failure_count" name="Failures" fill="#1e6bb8" radius={[0, 3, 3, 0]} /></BarChart></ResponsiveContainer>}
      </div>

      <div className={styles.chartBlock}>
        <SectionTitle title="Failure Over Time" />
        {dashLoading ? <SkeletonChart /> : <ResponsiveContainer width="100%" height={300}><LineChart data={timelineData} margin={{ left: 10, right: 10 }}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="time" tick={{ fontSize: 11 }} /><YAxis /><Tooltip /><Line type="monotone" dataKey="count" name="Failures" stroke="#c084fc" strokeWidth={2.5} dot={false} /></LineChart></ResponsiveContainer>}
      </div>

      <div className={styles.tableBlock}>
        <div className={styles.tableBlockHeader}><span className={styles.tableBlockTitle}>Recent Failed Messages ({failuresTotalCount})</span></div>
        <div className={styles.tableWrapper} style={failuresFetching && !failuresLoading ? { opacity: 0.6, pointerEvents: "none" } : undefined}>
          <table className={styles.table}>
            <thead><tr><th>Message GUID</th><th>Integration Scenario</th><th>Status</th><th>Time</th><th>Time Preview</th></tr></thead>
            <tbody>
              {failuresLoading ? <SkeletonRows count={5} colSpan={5} /> : recentFails.length === 0 ? <tr><td colSpan={5} className={styles.emptyCell}>No data</td></tr> : recentFails.map((row, i) => <tr key={i}><td className={styles.mono}>{String(row.message_guid ?? "-")}</td><td>{String(row.iflow_name ?? "-")}</td><td><StatusBadge status={String(row.status ?? "")} /></td><td>{formatISODate(row.log_end as string)}</td><td className={styles.mono} style={{ color: "#94a3b8" }}>-</td></tr>)}
            </tbody>
          </table>
        </div>
        {!failuresLoading && recentFails.length > 0 && <Pagination currentPage={failuresPage} totalPages={failuresTotalPages} pageSize={failuresPageSize} totalCount={failuresTotalCount} hasNextPage={failuresHasNext} hasPreviousPage={failuresHasPrev} onPreviousClick={onFailuresPrev} onNextClick={onFailuresNext} onPageSizeChange={onFailuresPageSizeChange} />}
      </div>

      <div className={styles.tableBlock}>
        <div className={styles.tableBlockHeader}><span className={styles.tableBlockTitle}>Active Incidents ({incidentsTotalCount})</span></div>
        {incidentsError && <div style={{ color: "#dc2626", fontSize: "0.85rem", marginBottom: "0.6rem" }}>Failed to load incidents API: {incidentsErrorMessage}</div>}
        <div className={styles.tableWrapper} style={incidentsFetching && !incidentsLoading ? { opacity: 0.6, pointerEvents: "none" } : undefined}>
          <table className={styles.table}>
            <thead><tr><th>Incident ID</th><th>Message GUID</th><th>iFlow</th><th>Error Type</th><th>Status</th><th>Created At</th><th>Last Seen</th><th>Occurrences</th><th>RCA Confidence</th></tr></thead>
            <tbody>
              {incidentsLoading ? <SkeletonRows count={5} /> : activeInc.length === 0 ? <tr><td colSpan={9} className={styles.emptyCell}>No data</td></tr> : activeInc.map((row, i) => {
                const stateClass = INCIDENT_STATE[String(row.status ?? "")] ?? styles.stateNone;
                return <tr key={i}><td className={styles.mono}>{String(row.incident_id ?? "-")}</td><td className={styles.mono}>{String(row.message_guid ?? "-")}</td><td>{String(row.iflow_id ?? "-")}</td><td>{String(row.error_type ?? "-")}</td><td><span className={`${styles.statusBadge} ${stateClass}`}>{String(row.status ?? "-")}</span></td><td>{formatISODate(row.created_at as string)}</td><td>{formatISODate(row.last_seen as string)}</td><td>{String(row.occurrence_count ?? "-")}</td><td>{String(row.rca_confidence ?? "-")}</td></tr>;
              })}
            </tbody>
          </table>
        </div>
        {!incidentsLoading && activeInc.length > 0 && <Pagination currentPage={incidentsPage} totalPages={incidentsTotalPages} pageSize={incidentsPageSize} totalCount={incidentsTotalCount} hasNextPage={incidentsHasNext} hasPreviousPage={incidentsHasPrev} onPreviousClick={onIncidentsPrev} onNextClick={onIncidentsNext} onPageSizeChange={onIncidentsPageSizeChange} />}
      </div>
    </>
  );
}
