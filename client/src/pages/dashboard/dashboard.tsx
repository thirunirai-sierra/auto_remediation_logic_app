/**
 * @fileoverview Azure Logic Apps dashboard page: loads logs overview + log incidents via React Query,
 * maps API DTOs into `OverviewTab` props, and paginates two tables client-side.
 */
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import {
  fetchLogsOverview,
  fetchLogIncidents,
  type LogsOverviewResponse,
  type LogIncident,
} from "../../services/api.ts";
import OverviewTab from "./components/OverviewTab";
import styles from "./dashboard.module.css";

/**
 * Dashboard shell: overview tab with KPIs, charts, and tables wired to logging APIs.
 * @returns {JSX.Element} Page layout and `OverviewTab` when overview tab is active.
 */
export default function Dashboard() {
  const chartOpts = { refetchInterval: 60_000, retry: 3, retryDelay: 3_000 } as const;
  const [activeTab, setActiveTab] = useState<"overview" | "tickets">("overview");

  const { data: logsOverview, isLoading: dashLoading } = useQuery({
    queryKey: ["logs-overview"],
    queryFn: () => fetchLogsOverview(1000),
    ...chartOpts,
  });

  const [failuresPage, setFailuresPage] = useState(1);
  const [failuresPageSize, setFailuresPageSize] = useState(20);
  const failuresLoading = dashLoading;
  const failuresFetching = false;

  const [incidentsPage, setIncidentsPage] = useState(1);
  const [incidentsPageSize, setIncidentsPageSize] = useState(20);
  const {
    data: logIncidents,
    isLoading: incidentsLoading,
    isFetching: incidentsFetching,
    isError: incidentsError,
    error: incidentsErrorObj,
  } = useQuery({
    queryKey: ["azure-log-incidents"],
    queryFn: fetchLogIncidents,
    refetchInterval: 60_000,
    retry: 2,
    retryDelay: 2_000,
  });

  const logs = (logsOverview ?? {}) as LogsOverviewResponse;
  const kpi = (logs.kpi
    ? {
        in_progress: 0,
        total_incidents: logs.kpi.total_flows,
        pending_approval: 0,
        fix_failed: logs.kpi.error_flows,
        auto_fixed: logs.kpi.fixed_flows,
        total_failed_messages: logs.kpi.total_error_messages,
        auto_fix_rate:
          logs.kpi.total_flows > 0
            ? Math.round((logs.kpi.fixed_flows / logs.kpi.total_flows) * 100)
            : 0,
        avg_resolution_time_minutes: "-",
        rca_coverage_percent: "-",
      }
    : {}) as Record<string, unknown>;
  const statusData = (logs.status_breakdown ?? []) as { status: string; count: number }[];
  const errorData = (logs.error_distribution ?? []) as { error_type: string; count: number }[];
  const iflowData = (logs.top_iflows ?? []) as { iflow_name: string; failure_count: number }[];
  const timelineData = (logs.timeline ?? []) as { time: string; count: number }[];

  const allRecentFails = (logs.error_messages ?? []) as Record<string, unknown>[];
  const failuresTotalCount = allRecentFails.length;
  const failuresTotalPages = Math.max(1, Math.ceil(failuresTotalCount / failuresPageSize));
  const failuresHasNext = failuresPage < failuresTotalPages;
  const failuresHasPrev = failuresPage > 1;
  const recentFails = useMemo(() => {
    const start = (failuresPage - 1) * failuresPageSize;
    return allRecentFails.slice(start, start + failuresPageSize).map((row) => ({
      message_guid: row.runId ?? "-",
      iflow_name: row.integrationScenario ?? "-",
      status: row.status ?? "FAILED",
      log_end: row.time ?? null,
      error_message: row.errorMessage ?? "-",
    }));
  }, [allRecentFails, failuresPage, failuresPageSize]);

  const allLogIncidents = (logIncidents ?? []) as LogIncident[];
  const incidentsTotalCount = allLogIncidents.length;
  const incidentsTotalPages = Math.max(1, Math.ceil(incidentsTotalCount / incidentsPageSize));
  const incidentsHasNext = incidentsPage < incidentsTotalPages;
  const incidentsHasPrev = incidentsPage > 1;
  const activeInc = useMemo(() => {
    const start = (incidentsPage - 1) * incidentsPageSize;
    const pageItems = allLogIncidents.slice(start, start + incidentsPageSize);
    return pageItems.map((item) => ({
      incident_id: item.incidentId ?? "-",
      message_guid: item.subscriptionId ?? "-",
      iflow_id: item.integrationScenario ?? "-",
      error_type: item.errorType ?? "-",
      status: "FAILED",
      created_at: item.time ?? null,
      last_seen: item.time ?? null,
      occurrence_count: 1,
      rca_confidence: "-",
    }));
  }, [allLogIncidents, incidentsPage, incidentsPageSize]);

  return (
    <div className={styles.page}>
      <h2 className={styles.pageTitle}><strong>Azure Logic Apps Dashboard Overview</strong></h2>
      <div className={styles.tabNav}>
        <button className={`${styles.tabNavBtn} ${activeTab === "overview" ? styles.tabNavBtnActive : ""}`} onClick={() => setActiveTab("overview")}>
          Overview
        </button>
      </div>

      <div style={{ display: activeTab === "overview" ? "contents" : "none" }}>
        <OverviewTab
          dashLoading={dashLoading}
          kpi={kpi}
          statusData={statusData}
          errorData={errorData}
          iflowData={iflowData}
          timelineData={timelineData}
          failuresLoading={failuresLoading}
          failuresFetching={failuresFetching}
          failuresTotalCount={failuresTotalCount}
          recentFails={recentFails}
          failuresPage={failuresPage}
          failuresTotalPages={failuresTotalPages}
          failuresPageSize={failuresPageSize}
          failuresHasNext={failuresHasNext}
          failuresHasPrev={failuresHasPrev}
          onFailuresPrev={() => setFailuresPage((p) => Math.max(1, p - 1))}
          onFailuresNext={() => setFailuresPage((p) => p + 1)}
          onFailuresPageSizeChange={(s) => { setFailuresPageSize(s); setFailuresPage(1); }}
          incidentsError={incidentsError}
          incidentsErrorMessage={String((incidentsErrorObj as Error)?.message ?? "Unknown error")}
          incidentsFetching={incidentsFetching}
          incidentsLoading={incidentsLoading}
          incidentsTotalCount={incidentsTotalCount}
          activeInc={activeInc}
          incidentsPage={incidentsPage}
          incidentsTotalPages={incidentsTotalPages}
          incidentsPageSize={incidentsPageSize}
          incidentsHasNext={incidentsHasNext}
          incidentsHasPrev={incidentsHasPrev}
          onIncidentsPrev={() => setIncidentsPage((p) => Math.max(1, p - 1))}
          onIncidentsNext={() => setIncidentsPage((p) => p + 1)}
          onIncidentsPageSizeChange={(s) => { setIncidentsPageSize(s); setIncidentsPage(1); }}
        />
      </div>
    </div>
  );
}
