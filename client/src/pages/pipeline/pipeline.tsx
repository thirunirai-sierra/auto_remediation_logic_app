import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  fetchPipelineStatus,
  startPipeline,
  stopPipeline,
  fetchPipelineTrace,
  fetchAutoFixStatus,
  toggleAutoFix,
} from "../../services/api.ts";
import SvgIcon, { type IconName } from "../../components/icons/SvgIcon.tsx";
import StatusPill from "../../components/StatusPill.tsx";
import _styles from "./pipeline.module.css";
// Vite 8 types CSS module values as `unknown`; cast so className={styles.x} compiles.
const styles = _styles as Record<string, string>;

// ── Agent metadata (5 specialist agents) ─────────────────────────────────────
const SPECIALIST_AGENTS: Record<string, { icon: IconName; label: string; desc: string; tools: string; gradient: string; accent: string }> = {
  observer:   { icon:"eye",          label:"Observer",   desc:"Monitors Azure Log Analytics for failed workflow runs, creates incidents",                 tools:"3 local tools",    gradient:"linear-gradient(135deg,#eef2ff 0%,#dbeafe 100%)", accent:"#3b82f6" },
  classifier: { icon:"tag",          label:"Classifier", desc:"Classifies error type + confidence — rule-based, zero LLM cost",                         tools:"3 local + 1 MCP",  gradient:"linear-gradient(135deg,#faf5ff 0%,#ede9fe 100%)", accent:"#8b5cf6" },
  rca:        { icon:"rca",          label:"RCA",        desc:"Root cause analysis: vector store + run logs + workflow definition inspection",            tools:"3 local + 2-3 MCP", gradient:"linear-gradient(135deg,#f0fdf4 0%,#dcfce7 100%)", accent:"#16a34a" },
  fixer:      { icon:"wrench",       label:"Fixer",      desc:"Get → validate → patch → redeploy Logic App workflow with ARM safety checks",             tools:"2 local + 6-8 MCP", gradient:"linear-gradient(135deg,#fff7ed 0%,#fed7aa 100%)", accent:"#ea580c" },
  verifier:   { icon:"check-circle", label:"Verifier",   desc:"Trigger fixed workflow + verify run succeeds for end-to-end confirmation",                tools:"1 local + 3-4 MCP", gradient:"linear-gradient(135deg,#f0fdf4 0%,#bbf7d0 100%)", accent:"#15803d" },
};

const SPECIALIST_ORDER = ["observer", "classifier", "rca", "fixer", "verifier"];

// ── Types ─────────────────────────────────────────────────────────────────────
interface TraceIncident {
  incident_id: string;
  message_guid: string;
  iflow_name: string;
  iflow_id?: string;
  error_type: string;
  status: string;
  created_at: string;
  updated_at: string;
  root_cause?: string;
  proposed_fix?: string;
}

export default function Pipeline() {
  const qc = useQueryClient();
  const [toggling, setToggling] = useState(false);
  const [togglingAutoFix, setTogglingAutoFix] = useState(false);
  const [traceSearch, setTraceSearch] = useState("");

  // ── Queries ──────────────────────────────────────────────────────────────
  const { data: pipelineData } = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: fetchPipelineStatus,
    refetchInterval: 15_000,
  });

  const { data: autoFixData, refetch: refetchAutoFix } = useQuery({
    queryKey: ["auto-fix-status"],
    queryFn: fetchAutoFixStatus,
    staleTime: 0,
  });


  const { data: traceData } = useQuery({
    queryKey: ["pipeline-trace"],
    queryFn: () => fetchPipelineTrace(200),
    refetchInterval: 15_000,   // was 6s
  });

  // ── Pipeline control ─────────────────────────────────────────────────────
  async function handleToggle() {
    setToggling(true);
    try {
      if (pipelineData?.pipeline_running) {
        await stopPipeline();
      } else {
        await startPipeline();
      }
      await qc.invalidateQueries({ queryKey: ["pipeline-status"] });
    } finally {
      setToggling(false);
    }
  }

  async function handleToggleAutoFix() {
    setTogglingAutoFix(true);
    try {
      await toggleAutoFix();
      await refetchAutoFix();
    } finally {
      setTogglingAutoFix(false);
    }
  }

  const autoFixOn = autoFixData?.auto_fix_enabled ?? true;


  const running = pipelineData?.pipeline_running ?? false;
  const agentStatuses = pipelineData?.agents ?? {};
  const incidents: TraceIncident[] = (traceData?.incidents ?? []) as TraceIncident[];
  const AGENT_META = SPECIALIST_AGENTS;
  const STAGE_ORDER = SPECIALIST_ORDER;

  const normalizeFlowName = (value: string | undefined) =>
    (value ?? "").toLowerCase().replace(/[^a-z0-9]/g, "");

  const filteredIncidents = useMemo(() => {
    const q = normalizeFlowName(traceSearch.trim());
    if (!q) return incidents;
    return incidents.filter((inc) => {
      // Search is intentionally scoped to integration flow name only.
      const flow = normalizeFlowName(inc.iflow_name || inc.iflow_id || "");
      return flow === q;
    });
  }, [incidents, traceSearch]);

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className={styles.page}>

      {/* ── Header ── */}
      <div className={styles.header}>
        <div>
          <h1 className={styles.pageTitle}>Auto-Remediation Pipeline</h1>
          <p className={styles.pageSubtitle}>
            5 specialist agents · Per-agent tools
          </p>
        </div>
        <div className={styles.headerRight}>
          <span
            className={`${styles.statusBadge} ${running ? styles.statusBadgeOn : styles.statusBadgeOff} tooltip-below`}
            data-tip={running ? "Pipeline is actively monitoring Azure Logic Apps for failures" : "Pipeline is stopped — no new incidents will be detected"}
          >
            {running ? "● Running" : "○ Stopped"}
          </span>
          {running && (
            <span className={`${styles.aemBadge} tooltip-below`} data-tip="5-agent specialist mode — each agent has a curated, minimal tool set for safety and efficiency">Specialist</span>
          )}
          <div
            className={`${styles.autoFixToggle} ${autoFixOn ? styles.autoFixToggleOn : styles.autoFixToggleOff} tooltip-below`}
            onClick={togglingAutoFix ? undefined : handleToggleAutoFix}
            data-tip={autoFixOn
              ? "Auto-Fix ON — AI applies fixes automatically when confidence is high. Click to require manual approval for all fixes."
              : "Auto-Fix OFF — All fixes require manual approval via Apply Fix. Click to re-enable autonomous fixing."}
          >
            <span className={styles.autoFixTrack}>
              <span className={styles.autoFixThumb} />
            </span>
            <span className={styles.autoFixLabel}>
              {togglingAutoFix ? "…" : autoFixOn ? "Auto-Fix" : "Manual"}
            </span>
          </div>
          <button
            className={`${styles.toggleBtn} ${running ? styles.toggleBtnStop : styles.toggleBtnStart} tooltip-below`}
            onClick={handleToggle}
            disabled={toggling}
            data-tip={running ? "Stop the pipeline — in-flight incidents will complete before halting" : "Start the autonomous 5-agent remediation pipeline"}
          >
            {toggling ? "…" : running ? "Stop Pipeline" : "Start Pipeline"}
          </button>
        </div>
      </div>

      {/* ── Agent flow ── */}
      <div className={styles.sectionLabelGroup}>
        <div className={styles.sectionLabel}>Agent Flow</div>
        <div className={styles.sectionSubLabel}>Each agent gets only the tools it needs</div>
      </div>
      <div className={styles.agentFlow}>
        {STAGE_ORDER.map((key, i) => {
          const meta = AGENT_META[key];
          if (!meta) return null;
          const rawStatus = agentStatuses[key] ?? "unknown";
          const isRunning = rawStatus === "running";
          return (
            <div key={key} className={styles.flowItem}>
              <div
                className={`${styles.agentCard} ${isRunning ? styles.agentCardActive : ""}`}
                style={{ borderColor: isRunning ? meta.accent : "transparent" }}
              >
                <div className={styles.agentBanner} style={{ background: meta.gradient }}>
                  <span className={styles.agentEmoji}><SvgIcon name={meta.icon} size={22} style={{ color: meta.accent }} /></span>
                  <span className={`${styles.agentDot} ${isRunning ? styles.dotRunning : styles.dotIdle}`} />
                </div>
                <div className={styles.agentInfo}>
                  <span className={styles.agentLabel}>{meta.label}</span>
                  <span className={styles.agentStatus} style={{ color: isRunning ? meta.accent : "#22c55e" }}>
                    {isRunning ? "Running" : "Running"}
                  </span>
                  <span className={styles.agentDesc}>{meta.desc}</span>
                </div>
              </div>
              {i < STAGE_ORDER.length - 1 && (
                <span className={`${styles.flowArrow} ${isRunning ? styles.flowArrowActive : ""}`}>→</span>
              )}
            </div>
          );
        })}
      </div>

      {/* ── Pipeline trace ── */}
      <div className={styles.traceTableHeader}>
        <div>
          <div className={styles.sectionLabel}>Pipeline Trace</div>
        </div>
        <div className={styles.traceSearch}>
          <span className={styles.traceSearchIcon}>🔍</span>
          <input
            className={styles.traceSearchInput}
            placeholder="search workflow name"
            value={traceSearch}
            onChange={(e) => setTraceSearch(e.target.value)}
          />
        </div>
      </div>
      <div className={styles.traceTable}>
        {filteredIncidents.length === 0 ? (
          <div className={styles.traceEmpty}>
            {traceSearch.trim()
              ? "No matching workflow found."
              : "No incidents yet. Start the pipeline to begin processing."}
          </div>
        ) : (
          <table className={styles.table}>
            <thead>
              <tr>
                <th title="Azure Logic App workflow that encountered the error">Workflow</th>
                <th title="Classified error category (e.g. HTTP_ERROR, CONNECTIVITY_ERROR)">Error Type</th>
                <th title="Current auto-remediation pipeline stage for this incident">Status</th>
                <th title="AI-generated summary of the root cause">Root Cause</th>
                <th title="When this incident was first detected by the pipeline">Created</th>
              </tr>
            </thead>
            <tbody>
              {filteredIncidents.map((inc) => (
                <tr key={inc.incident_id}>
                  <td className={styles.tdIflow} title={inc.iflow_name || inc.iflow_id || inc.message_guid || ""}>
                    {inc.iflow_name || (
                      <span className={styles.tdIflowUnknown}>
                        {inc.message_guid ? "Resolving…" : "—"}
                      </span>
                    )}
                  </td>
                  <td><span className={styles.errorTypeBadge}>{inc.error_type}</span></td>
                  <td><StatusPill status={inc.status} /></td>
                  <td className={styles.tdRca} title={inc.root_cause || undefined}>
                    {inc.root_cause?.trim() ? inc.root_cause : "—"}
                  </td>
                  <td className={styles.tdDate}>{new Date(inc.created_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

    </div>
  );
}
