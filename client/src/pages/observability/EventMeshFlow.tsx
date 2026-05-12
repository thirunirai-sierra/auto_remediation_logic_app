/**
 * @fileoverview Observability "Event Mesh" tab: polls AEM status/incidents and MCP tools; diffs incidents into a synthetic live log;
 * renders `PipelineDiagram`, `StatsRow`, and `EventLog` from the same data.
 */
import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchAemIncidents, fetchAemStatus, fetchMcpTools } from "../../services/api";
import EventLog from "./event-mesh/EventLog";
import PipelineDiagram from "./event-mesh/PipelineDiagram";
import StatsRow from "./event-mesh/StatsRow";
import { isIncident, STATUS_ICON, statusToStage, type LogEntry, type Incident } from "./event-mesh/shared";
import styles from "./EventMeshFlow.module.css";

/**
 * Wires React Query to diagram, stats, and log; maintains `logEntries` by diffing incident id → status map.
 * @returns {JSX.Element} Column layout for Event Mesh visualization.
 */
export default function EventMeshFlow() {
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
  const prevMapRef = useRef<Map<string, string>>(new Map());

  const { data: aemStatus } = useQuery({
    queryKey: ["aem-flow-status"],
    queryFn: fetchAemStatus,
    refetchInterval: 3_000,
    retry: false,
    staleTime: 0,
  });

  const { data: incidentsData } = useQuery({
    queryKey: ["aem-flow-incidents"],
    queryFn: () => fetchAemIncidents(100),
    refetchInterval: 3_000,
    retry: 2,
    staleTime: 0,
  });

  const { data: mcpTools } = useQuery({
    queryKey: ["mcp-tools"],
    queryFn: fetchMcpTools,
    staleTime: 5 * 60 * 1_000,
    retry: false,
  });

  const incidents: Incident[] = (incidentsData?.incidents ?? []).filter(isIncident);

  /**
   * When incident list changes: append log rows for new ids or status transitions; cap list length at 120.
   * @returns {void}
   */
  useEffect(() => {
    if (!incidentsData) return;
    const prev = prevMapRef.current;
    const newEntries: LogEntry[] = [];

    for (const inc of incidents) {
      const prevStatus = prev.get(inc.incident_id);
      const statusUp = inc.status.toUpperCase();
      if (!prevStatus) {
        newEntries.push({
          id:           `${inc.incident_id}-${Date.now()}-new`,
          ts:           new Date().toISOString(),
          icon:         STATUS_ICON[statusUp] ?? "inbox",
          iflowName:    inc.iflow_name ?? inc.iflow_id ?? "—",
          stage:        statusToStage(inc.status),
          status:       inc.status,
          isNew:        true,
          incidentId:   inc.incident_id,
          errorMessage: (inc.error_message as string) ?? "",
          createdAt:    (inc.created_at as string) ?? "",
        });
      } else if (prevStatus !== inc.status) {
        newEntries.push({
          id:           `${inc.incident_id}-${Date.now()}-chg`,
          ts:           new Date().toISOString(),
          icon:         STATUS_ICON[statusUp] ?? "refresh",
          iflowName:    inc.iflow_name ?? inc.iflow_id ?? "—",
          stage:        statusToStage(inc.status),
          status:       inc.status,
          isNew:        false,
          incidentId:   inc.incident_id,
          errorMessage: (inc.error_message as string) ?? "",
          createdAt:    (inc.created_at as string) ?? "",
        });
      }
    }

    if (newEntries.length > 0) {
      setLogEntries(prev => [...newEntries, ...prev].slice(0, 120));
    }

    const newMap = new Map<string, string>();
    incidents.forEach(inc => newMap.set(inc.incident_id, inc.status));
    prevMapRef.current = newMap;
  }, [incidents, incidentsData]);

  return (
    <div className={styles.root}>
      <PipelineDiagram
        incidents={incidents}
        aemEnabled={(aemStatus?.event_mesh_enabled || aemStatus?.webhook_active) ?? false}
        messagesRetrieved={aemStatus?.messages_retrieved ?? 0}
      />
      <StatsRow aemStatus={aemStatus ?? null} incidents={incidents} mcpTools={mcpTools ?? null} />
      <EventLog entries={logEntries} onClear={() => setLogEntries([])} />
    </div>
  );
}
