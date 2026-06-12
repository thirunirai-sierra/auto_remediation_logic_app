/**
 * @fileoverview Observability workspace: monitor message list/detail (HANA-backed APIs), AI analyze/explain/fix flows,
 * tickets and approvals, Error Type Guide, and Event Mesh sub-view (`EventMeshFlow`).
 */
import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import SvgIcon from "../../components/icons/SvgIcon.tsx";
import {
  fetchMonitorMessages,
  fetchMonitorMessageDetail,
  analyzeMessage,
  explainError,
  generateFixPatch,
  applyMessageFix,
  fetchFixStatus,
  fetchTickets,
  updateTicket,
  fetchPendingApprovals,
  approveIncident,
  rejectIncident,
  type PendingApproval,
} from "../../services/api.ts";
import type {
  IMonitorMessage,
  IFilterState,
  IMessageDetail,
  IFixPatchResponse,
  IFieldChange,
  IFixPlanStep,
  IHistoryTimelineEntry,
  IErrorExplanation,
} from "../../types/index.ts";
import styles from "./observability.module.css";
import EventMeshFlow from "./EventMeshFlow.tsx";

// ============================================================================
// Utility functions
// ============================================================================

/** Strip raw agent noise from fix_summary strings */
function cleanFixSummary(raw: string): string {
  if (!raw) return raw;
  let text = raw.replace(/\s*__TOOLS_INVOKED__.*$/s, "").trim();
  const braceIdx = text.indexOf("{");
  if (braceIdx !== -1) {
    const jsonCandidate = text.slice(braceIdx);
    try {
      const parsed = JSON.parse(jsonCandidate) as Record<string, unknown>;
      const summary = (parsed.summary as string | undefined)?.trim();
      if (summary) return summary;
    } catch {
      // not valid JSON — fall through
    }
    const before = text.slice(0, braceIdx).trim();
    if (before) return before;
  }
  return text;
}

// ============================================================================
// Types
// ============================================================================

type MainTabKey = "errortypeguide" | "messages" | "tickets" | "approvals" | "eventmesh";

interface Ticket {
  ticket_id: string;
  incident_id: string;
  iflow_id: string;
  error_type: string;
  title: string;
  description: string;
  priority: string;
  status: string;
  assigned_to: string | null;
  resolution_notes: string | null;
  created_at: string;
  updated_at: string;
  resolved_at: string | null;
}

interface Approval extends PendingApproval {}

type StatusCfg = { label: string; color: string; bg: string; dot: string };

// ============================================================================
// Status configuration
// ============================================================================

const RED: StatusCfg = { label: "Failed", color: "#dc2626", bg: "#fee2e2", dot: "#ef4444" };
const GREEN: StatusCfg = { label: "Success", color: "#16a34a", bg: "#dcfce7", dot: "#22c55e" };
const BLUE: StatusCfg = { label: "Processing", color: "#2563eb", bg: "#dbeafe", dot: "#3b82f6" };
const AMBER: StatusCfg = { label: "Retry", color: "#d97706", bg: "#fef3c7", dot: "#f59e0b" };
const PURPLE: StatusCfg = { label: "Pending", color: "#7c3aed", bg: "#ede9fe", dot: "#8b5cf6" };
const GREY: StatusCfg = { label: "Unknown", color: "#6b7280", bg: "#f3f4f6", dot: "#9ca3af" };

const STATUS_CONFIG: Record<string, StatusCfg> = {
  FAILED: RED,
  SUCCESS: GREEN,
  PROCESSING: BLUE,
  RETRY: AMBER,
  DETECTED: { ...RED, label: "Detected" },
  CLASSIFIED: { ...BLUE, label: "Classified" },
  RCA_IN_PROGRESS: { ...BLUE, label: "Analyzing" },
  RCA_COMPLETE: { ...BLUE, label: "RCA Done" },
  RCA_FAILED: { ...RED, label: "RCA Failed" },
  FIX_IN_PROGRESS: { ...AMBER, label: "Fixing" },
  FIX_FAILED: { ...RED, label: "Fix Failed" },
  FIX_FAILED_UPDATE: { ...RED, label: "Fix Failed (Update)" },
  FIX_FAILED_DEPLOY: { ...RED, label: "Fix Failed (Deploy)" },
  FIX_FAILED_RUNTIME: { ...RED, label: "Fix Failed (Runtime)" },
  FIX_APPLIED_PENDING_VERIFICATION: { ...AMBER, label: "Verifying" },
  AUTO_FIXED: { ...GREEN, label: "Auto-Fixed" },
  HUMAN_FIXED: { ...GREEN, label: "Fixed" },
  FIX_VERIFIED: { ...GREEN, label: "Verified" },
  PENDING_APPROVAL: { ...PURPLE, label: "Pending Approval" },
  AWAITING_APPROVAL: { ...PURPLE, label: "Awaiting Approval" },
  APPROVED: { ...GREEN, label: "Approved" },
  TICKET_CREATED: { ...PURPLE, label: "Ticket Created" },
  ARTIFACT_MISSING: { ...GREY, label: "Artifact Missing or Deleted" },
  PIPELINE_ERROR: { ...RED, label: "Pipeline Error" },
  REJECTED: { ...RED, label: "Rejected" },
  RETRIED: { ...GREEN, label: "Retried" },
};

/** DB/API often returns spaced labels ("Ticket Created"); UI keys are SNAKE_CASE ("TICKET_CREATED"). */
function normalizeStatusKey(status: string | undefined | null): string {
  return (status ?? "").trim().toUpperCase().replace(/\s+/g, "_");
}

function StatusPill({ status }: { status: string }) {
  const key = normalizeStatusKey(status);
  const cfg = STATUS_CONFIG[key] ?? { ...GREY, label: status || "Unknown" };
  return (
    <span className={styles.statusPill} style={{ color: cfg.color, background: cfg.bg }}>
      <span className={styles.statusDot} style={{ background: cfg.dot }} />
      {cfg.label}
    </span>
  );
}

const TERMINAL_STATUSES = new Set([
  "AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED", "FIX_DEPLOYED",
  "FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME",
  "PIPELINE_ERROR", "REJECTED", "TICKET_CREATED", "ARTIFACT_MISSING",
  "HUMAN_INITIATED_FIX", "AWAITING_APPROVAL",
]);

const SUCCESS_STATUSES = new Set([
  "AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED",
  "FIX_DEPLOYED", "HUMAN_INITIATED_FIX",
]);

// ============================================================================
// Components: TabBar, etc.
// ============================================================================

type TabKey = "error" | "ai" | "properties" | "artifact" | "attachments" | "history";

const TABS: { key: TabKey; label: string; tip: string }[] = [
  { key: "error", label: "Error Details", tip: "Raw error message, error type and run timestamps from Azure Monitor / Log Analytics" },
  { key: "ai", label: "AI Recommendations & Suggested Fix", tip: "AI-generated diagnosis, proposed fix and confidence score" },
  { key: "properties", label: "Properties", tip: "Run properties, trigger configuration and business context" },
  { key: "artifact", label: "Workflow", tip: "Logic App workflow metadata: version, deployment info and runtime location" },
  { key: "attachments", label: "Attachments", tip: "Related knowledge and context for this remediation incident" },
  { key: "history", label: "History", tip: "Timeline of status changes for this remediation incident" },
];

const INITIAL_FILTERS: IFilterState = {
  statuses: [], types: [], artifacts: [],
  dateFrom: "", dateTo: "", idQuery: "", searchQuery: "",
};

const CARD_TIPS: Record<string, string> = {
  FAILED: "Messages in FAILED, FIX_FAILED, RCA_FAILED or DETECTED state — need attention",
  SUCCESS: "Messages that reached AUTO_FIXED, HUMAN_FIXED or FIX_VERIFIED state",
  PROCESSING: "Messages currently in RCA, classification or fix-in-progress stages",
  RETRY: "Messages pending approval, ticket created or scheduled for retry",
};

const SUMMARY_CARD_KEYS = ["FAILED", "SUCCESS", "PROCESSING", "RETRY"] as const;

const ANALYZE_STEPS = [
  "Analyzing error pattern and stack trace...",
  "Identifying root cause from workflow configuration...",
  "Searching knowledge base for known fixes...",
  "Generating fix recommendation...",
];

// ============================================================================
// Error Type Guide data & component (unchanged from original)
// ============================================================================

type ErrorTypeMeta = { label: string; description: string; action: "AUTO_FIX" | "TICKET_CREATED" | "APPROVAL" | "RETRY"; dot: string };

const ERROR_TYPE_META: Record<string, ErrorTypeMeta> = {
  EXPRESSION_ERROR: { label: "Expression Error", description: "Workflow expression evaluation failed — syntax error or reference to a missing property in the run context.", action: "AUTO_FIX", dot: "#22c55e" },
  MAPPING_ERROR: { label: "Mapping Error", description: "Data transformation issue — source/target field mismatch or schema incompatibility in a workflow action.", action: "AUTO_FIX", dot: "#22c55e" },
  TRIGGER_ERROR: { label: "Trigger Error", description: "Logic App trigger is misconfigured — wrong endpoint, missing required parameter, or invalid schema.", action: "AUTO_FIX", dot: "#22c55e" },
  CONNECTOR_ERROR: { label: "Connector Error", description: "Managed connector configuration issue — wrong connection reference or invalid action parameters.", action: "AUTO_FIX", dot: "#22c55e" },
  WORKFLOW_DEFINITION_ERROR: { label: "Workflow Definition Error", description: "Invalid Logic App workflow JSON — missing required field, unsupported action type, or schema violation.", action: "AUTO_FIX", dot: "#22c55e" },
  AUTH_CONFIG_ERROR: { label: "Auth Config Error", description: "Wrong credential or connection reference inside the workflow — API key, OAuth app registration mismatch.", action: "AUTO_FIX", dot: "#22c55e" },
  HTTP_ERROR: { label: "HTTP Error", description: "HTTP action received a 4xx client error — bad request, wrong endpoint URL, or missing required header.", action: "AUTO_FIX", dot: "#22c55e" },
  ODATA_ERROR: { label: "OData Error", description: "OData connector action failed — entity set path or query options misconfigured.", action: "AUTO_FIX", dot: "#22c55e" },
  BACKEND_ERROR: { label: "Backend Error", description: "Target service returned HTTP 5xx — the downstream system is down or returning server errors.", action: "TICKET_CREATED", dot: "#ef4444" },
  THROTTLING_ERROR: { label: "Throttling Error", description: "Azure or connector rate limit hit — too many requests in a short window. Infrastructure or retry policy change required.", action: "TICKET_CREATED", dot: "#ef4444" },
  RESOURCE_LIMIT_ERROR: { label: "Resource Limit Error", description: "Logic App run exceeded execution limits (duration, actions, or memory) — workflow redesign required.", action: "TICKET_CREATED", dot: "#ef4444" },
  SSL_ERROR: { label: "SSL Error", description: "TLS/SSL certificate error on an HTTP action — certificate expired, untrusted CA, or wrong trust anchor.", action: "TICKET_CREATED", dot: "#ef4444" },
  DEPENDENCY_ERROR: { label: "Dependency Error", description: "A required Azure resource (Key Vault, Service Bus, Storage) is unavailable or misconfigured.", action: "TICKET_CREATED", dot: "#ef4444" },
  AUTH_ERROR: { label: "Auth Error", description: "Authentication failed — OAuth token expired, API key revoked, or managed identity permission missing.", action: "APPROVAL", dot: "#f59e0b" },
  UNKNOWN_ERROR: { label: "Unknown Error", description: "Error could not be classified by the rule engine or AI — human review required before any fix.", action: "APPROVAL", dot: "#f59e0b" },
  CONNECTIVITY_ERROR: { label: "Connectivity Error", description: "Transient network issue — connection refused or timed out. Agent retries the workflow run automatically.", action: "RETRY", dot: "#3b82f6" },
  TIMEOUT_ERROR: { label: "Timeout Error", description: "Workflow action exceeded its timeout limit — transient target slowness; agent retries with backoff.", action: "RETRY", dot: "#3b82f6" },
};

const ACTION_GROUPS: { key: ErrorTypeMeta["action"]; iconName: "lightning" | "tickets" | "user" | "loop"; label: string; desc: string; color: string; bg: string }[] = [
  { key: "AUTO_FIX", iconName: "lightning", label: "Auto-Fix", desc: "Agent resolves automatically — no manual action needed", color: "#15803d", bg: "#f0fdf4" },
  { key: "TICKET_CREATED", iconName: "tickets", label: "Ticket Created", desc: "Escalated to a human team — a ticket has been created", color: "#b91c1c", bg: "#fff5f5" },
  { key: "APPROVAL", iconName: "user", label: "Awaiting Approval", desc: "Agent needs human sign-off before applying any fix", color: "#92400e", bg: "#fffbeb" },
  { key: "RETRY", iconName: "loop", label: "Retry", desc: "Transient issue — agent retries the workflow run automatically", color: "#1e40af", bg: "#eff6ff" },
];

function ErrorTypeGuideTab() {
  const [activeGroup, setActiveGroup] = useState<ErrorTypeMeta["action"] | "ALL">("AUTO_FIX");
  const [search, setSearch] = useState("");

  const groupCounts: Record<string, number> = {};
  for (const m of Object.values(ERROR_TYPE_META)) {
    groupCounts[m.action] = (groupCounts[m.action] ?? 0) + 1;
  }

  const filtered = Object.entries(ERROR_TYPE_META).filter(([type, meta]) => {
    const matchesGroup = activeGroup === "ALL" || meta.action === activeGroup;
    const q = search.toLowerCase();
    const matchesSearch = !q || type.toLowerCase().includes(q) || meta.label.toLowerCase().includes(q) || meta.description.toLowerCase().includes(q);
    return matchesGroup && matchesSearch;
  });

  const activeGroupMeta = ACTION_GROUPS.find(g => g.key === activeGroup);

  return (
    <div className={styles.errorGuideTabContent}>
      <div className={styles.errorGuideFilters}>
        <div className={styles.errorGuidePills}>
          {ACTION_GROUPS.map((group) => (
            <button
              key={group.key}
              className={`${styles.errorGuidePill} ${activeGroup === group.key ? styles.errorGuidePillActive : ""}`}
              style={activeGroup === group.key ? { background: group.bg, color: group.color, borderColor: group.color } : {}}
              onClick={() => setActiveGroup(group.key)}
            >
              {activeGroup === group.key && <span className={styles.errorGuidePillDot} style={{ background: group.color }} />}
              {group.label} ({groupCounts[group.key] ?? 0})
            </button>
          ))}
        </div>
        <div className={styles.errorGuideSearch}>
          <span className={styles.errorGuideSearchIcon}>🔍</span>
          <input
            className={styles.errorGuideSearchInput}
            placeholder="search type guide"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>
      {activeGroupMeta && <p className={styles.errorGuideGroupDesc}>{activeGroupMeta.desc}</p>}
      <div className={styles.errorGuideCardsGrid}>
        {filtered.map(([type, meta]) => (
          <div key={type} className={styles.errorGuideCardNew} style={{ borderLeft: `3px solid ${meta.dot}` }}>
            <span className={styles.errorGuideCardNewLabel}>{meta.label}</span>
            <span className={styles.errorGuideCardNewType}>{type}</span>
            <p className={styles.errorGuideCardNewDesc}>{meta.description}</p>
          </div>
        ))}
        {filtered.length === 0 && <div className={styles.errorGuideEmpty}>No error types match your search.</div>}
      </div>
    </div>
  );
}

function FieldChangeHighlight({ changes }: { changes: IFieldChange[] }) {
  if (!changes?.length) return null;
  return (
    <div className={styles.fieldChanges}>
      {changes.map((fc, i) => (
        <div key={i} className={styles.fieldChangeRow}>
          Field <span className={styles.oldField}>{fc.old_field}</span> was renamed to{" "}
          <span className={styles.newField}>{fc.new_field}</span> but message mapping still references{" "}
          <span className={styles.oldField}>{fc.old_field}</span>
        </div>
      ))}
    </div>
  );
}

function ConfidenceBadge({ value, label }: { value: number; label: string }) {
  const pct = Math.round(value * 100);
  const color = value >= 0.9 ? "#16a34a" : value >= 0.7 ? "#d97706" : "#dc2626";
  return (
    <div className={styles.confidenceSection}>
      <span className={styles.confidenceVal} style={{ color }}>
        Confidence: {value.toFixed(2)} ({label})
      </span>
      <div className={styles.confidenceBar}>
        <div className={styles.confidenceFill} style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  );
}

function FixPlanSteps({ steps }: { steps: IFixPlanStep[] }) {
  return (
    <div className={styles.fixPlanSteps}>
      {steps.map((s) => (
        <div key={s.step_number} className={styles.fixPlanStep}>
          <div className={styles.fixStepHeader}>
            <span className={styles.fixStepNum}>{s.step_number}.</span>
            <span className={styles.fixStepTitle}>{s.title}</span>
          </div>
          <p className={styles.fixStepDesc}>{s.description}</p>
          {s.sub_steps?.length > 0 && (
            <ul className={styles.fixSubSteps}>
              {s.sub_steps.map((sub, j) => <li key={j}>{sub}</li>)}
            </ul>
          )}
          {s.note && <div className={styles.fixStepNote}>{s.note}</div>}
        </div>
      ))}
    </div>
  );
}

const FIX_STAGES = ["Submit", "Get Workflow", "Validate", "Patch", "Deploy"] as const;

function PipelineStageRail({ stepIndex, totalSteps, currentStep }: { stepIndex: number; totalSteps: number; currentStep?: string }) {
  const slots = FIX_STAGES.length;
  const allDone = stepIndex >= totalSteps || (currentStep || "").toLowerCase().includes("complete");
  const active = allDone
    ? slots
    : totalSteps >= slots
      ? Math.min(stepIndex, slots - 1)
      : Math.min(stepIndex <= 0 ? 0 : Math.round((stepIndex / Math.max(totalSteps, 1)) * (slots - 1)), slots - 1);
  return (
    <div className={styles.stageRail}>
      {FIX_STAGES.map((label, i) => {
        const done = i < active;
        const isActive = !allDone && i === active;
        return (
          <div key={label} style={{ display: "contents" }}>
            <div className={styles.stageStep}>
              <div
                className={[
                  styles.stageDot,
                  done ? styles.stageDotDone : "",
                  isActive ? styles.stageDotActive : "",
                ].filter(Boolean).join(" ")}
              >
                {done ? "✓" : i + 1}
              </div>
              <span
                className={[
                  styles.stageLabel,
                  isActive ? styles.stageLabelActive : "",
                  done ? styles.stageLabelDone : "",
                ].filter(Boolean).join(" ")}
              >
                {label}
              </span>
            </div>
            {i < slots - 1 && (
              <div className={[styles.stageConnector, done ? styles.stageConnectorDone : ""].filter(Boolean).join(" ")} />
            )}
          </div>
        );
      })}
    </div>
  );
}

function Timeline({ entries }: { entries: IHistoryTimelineEntry[] }) {
  const statusIcon: Record<string, string> = {
    completed: "✓", failed: "✕", pending: "○",
    in_progress: "↻", info: "i",
  };
  const statusColor: Record<string, string> = {
    completed: "#16a34a", failed: "#dc2626", pending: "#d97706",
    in_progress: "#2563eb", info: "#6b7280",
  };
  return (
    <div className={styles.timeline}>
      {entries.map((e, i) => (
        <div key={i} className={styles.timelineEntry}>
          <div className={styles.timelineDot} style={{ background: statusColor[e.status] || "#6b7280" }}>
            {statusIcon[e.status] ?? "·"}
          </div>
          <div className={styles.timelineContent}>
            <div className={styles.timelineStep}>{e.step}</div>
            <div className={styles.timelineDesc}>{e.description}</div>
            {e.timestamp && <div className={styles.timelineTs}>{e.timestamp}</div>}
          </div>
        </div>
      ))}
    </div>
  );
}

function parseTicketDescription(desc: string): Record<string, string> {
  if (!desc) return {};
  const markerPattern = /\b(iFlow|Error|Root cause|Proposed fix|Incident ID|Occurrence count|RCA confidence):\s*/g;
  const parts = desc.split(markerPattern);
  const result: Record<string, string> = {};
  for (let i = 1; i < parts.length; i += 2) {
    const key = parts[i].toLowerCase().replace(/\s+/g, '_');
    const val = (parts[i + 1] || '').trim();
    result[key] = val;
  }
  return result;
}

function RichText({ text }: { text: string }) {
  if (!text) return null;
  const lines = text.split(/\n/).filter((l) => l.trim());
  return (
    <div className={styles.richText}>
      {lines.map((line, i) => {
        const isBullet = /^[-•*]\s/.test(line);
        const isNum = /^\d+\.\s/.test(line);
        if (isBullet) return (
          <div key={i} className={styles.richBullet}>
            <span className={styles.richBulletDot}>•</span>
            <span>{line.replace(/^[-•*]\s/, "")}</span>
          </div>
        );
        if (isNum) return (
          <div key={i} className={styles.richBullet}>
            <span className={styles.richBulletDot}>{line.match(/^\d+/)?.[0]}.</span>
            <span>{line.replace(/^\d+\.\s/, "")}</span>
          </div>
        );
        return <p key={i} className={styles.richPara}>{line}</p>;
      })}
    </div>
  );
}

const CATEGORY_COLORS: Record<string, { color: string; bg: string }> = {
  HTTP_ERROR: { color: "#b91c1c", bg: "#fee2e2" },
  MAPPING_ERROR: { color: "#92400e", bg: "#fef3c7" },
  CONNECTIVITY_ERROR: { color: "#1e40af", bg: "#dbeafe" },
  AUTH_ERROR: { color: "#6b21a8", bg: "#f3e8ff" },
  DATA_ERROR: { color: "#92400e", bg: "#fef3c7" },
  TIMEOUT_ERROR: { color: "#9a3412", bg: "#ffedd5" },
  CONFIG_ERROR: { color: "#1e40af", bg: "#dbeafe" },
  RUNTIME_ERROR: { color: "#b91c1c", bg: "#fee2e2" },
};

function ErrorExplanationCard({ exp }: { exp: IErrorExplanation }) {
  const catStyle = CATEGORY_COLORS[exp.error_category] ?? { color: "#374151", bg: "#f3f4f6" };
  return (
    <div className={styles.explainCard}>
      <div className={styles.explainCardHeader}>
        <span className={styles.explainSparkle}><SvgIcon name="rca" size={15} /></span>
        <span className={styles.explainCardTitle}>AI Error Analysis</span>
        <span className={styles.explainCategoryBadge} style={{ color: catStyle.color, background: catStyle.bg }}>
          {exp.category_label || exp.error_category}
        </span>
      </div>
      {exp.summary && (
        <div className={styles.explainSummaryBox}>
          <p className={styles.explainSummaryText}>{exp.summary}</p>
        </div>
      )}
      {exp.what_happened && (
        <div className={styles.explainSection}>
          <div className={styles.explainSectionLabel}>What Happened</div>
          <p className={styles.explainSectionBody}>{exp.what_happened}</p>
        </div>
      )}
      {exp.likely_causes?.length > 0 && (
        <div className={styles.explainSection}>
          <div className={styles.explainSectionLabel}>Likely Causes</div>
          <ul className={styles.explainList}>
            {exp.likely_causes.map((c, i) => <li key={i}>{c}</li>)}
          </ul>
        </div>
      )}
      {exp.recommended_actions?.length > 0 && (
        <div className={styles.explainSection}>
          <div className={styles.explainSectionLabel}>Recommended Actions</div>
          <ol className={styles.explainList}>
            {exp.recommended_actions.map((a, i) => <li key={i}>{a}</li>)}
          </ol>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// Main Observability component (messages, tickets, approvals, Event Mesh)
// ============================================================================

/**
 * Root page state: tabs, filters, selected message, detail + AI/fix flows, and child `EventMeshFlow` when on Event Mesh tab.
 * @returns {JSX.Element} Full observability page layout.
 */
export default function Observability() {
  const [mainTab, setMainTab] = useState<MainTabKey>("errortypeguide");
  const [filters, setFilters] = useState<IFilterState>(INITIAL_FILTERS);
  const [selectedGuid, setSelectedGuid] = useState<string | null>(null);
  const [selectedMsg, setSelectedMsg] = useState<IMonitorMessage | null>(null);
  const [detail, setDetail] = useState<IMessageDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<TabKey>("error");
  const [errorExplain, setErrorExplain] = useState<IErrorExplanation | null>(null);
  const [errorExplainLoading, setErrorExplainLoading] = useState(false);
  const [errorExplainErr, setErrorExplainErr] = useState<string | null>(null);
  const [fixPatch, setFixPatch] = useState<IFixPatchResponse | null>(null);
  const [fixPatchLoading, setFixPatchLoading] = useState(false);
  const [fixPatchError, setFixPatchError] = useState<string | null>(null);
  const [fixState, setFixState] = useState<"idle" | "loading" | "success" | "error" | "skipped" | "deployed_unverified"| "ticket_created">("idle");
  const [fixResult, setFixResult] = useState<string>("");
  const [fixProgress, setFixProgress] = useState<{
    currentStep: string; stepIndex: number; totalSteps: number; stepsDone: string[];
  } | null>(null);
  const [analyzeLoading, setAnalyzeLoading] = useState(false);
  const [analyzeStep, setAnalyzeStep] = useState(0);
  const [approvalActionError, setApprovalActionError] = useState<string | null>(null);
  const [approvalToast, setApprovalToast] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [bulkActionLoading, setBulkActionLoading] = useState<"approving" | "rejecting" | null>(null);
  const [resolvingTicketId, setResolvingTicketId] = useState<string | null>(null);
  const [ticketActionError, setTicketActionError] = useState<string | null>(null);

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ["monitor-messages"],
    queryFn: fetchMonitorMessages,
    refetchInterval: 30_000,
    staleTime: 20_000,
  });

  const { data: ticketsData, isLoading: ticketsLoading, refetch: refetchTickets } = useQuery({
    queryKey: ["escalation-tickets"],
    queryFn: fetchTickets,
    refetchInterval: 30_000,
    enabled: mainTab === "tickets",
  });

  const { data: approvalsData, isLoading: approvalsLoading, refetch: refetchApprovals } = useQuery({
    queryKey: ["pending-approvals"],
    queryFn: fetchPendingApprovals,
    refetchInterval: 30_000,
    enabled: mainTab === "approvals",
  });

  const STATUS_GROUP: Record<string, string[]> = {
    FAILED:     ["FAILED", "FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME", "RCA_FAILED", "PIPELINE_ERROR", "DETECTED", "ARTIFACT_MISSING"],
    SUCCESS:    ["AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED", "SUCCESS", "HUMAN_INITIATED_FIX", "FIX_DEPLOYED"],
    PROCESSING: ["RCA_IN_PROGRESS", "FIX_IN_PROGRESS", "FIX_ATTEMPTED", "CLASSIFIED", "RCA_COMPLETE", "FIX_APPLIED_PENDING_VERIFICATION", "PROCESSING"],
    RETRY:      ["RETRY", "PENDING_APPROVAL", "TICKET_CREATED", "AWAITING_APPROVAL"],
  };

  // Normalized messages with status filtering (spaces -> underscores)
  const messages = useMemo(() => {
    return ((data?.messages || []) as IMonitorMessage[]).filter((m) => {
      const s = normalizeStatusKey(m.status);
      if (filters.statuses.length) {
        const allowed = filters.statuses.flatMap((g) => STATUS_GROUP[g] || [g]);
        if (!allowed.includes(s)) return false;
      }
      if (filters.searchQuery) {
        const q = filters.searchQuery.toLowerCase();
        if (!(m.iflow_display || m.title || "").toLowerCase().includes(q)) return false;
      }
      if (filters.idQuery) {
        const q = filters.idQuery.toLowerCase();
        if (!(m.message_guid || "").toLowerCase().includes(q) &&
            !(m.iflow_display || "").toLowerCase().includes(q)) return false;
      }
      return true;
    });
  }, [data, filters]);

  // Counts for summary cards (normalized statuses)
  const counts = useMemo(() => {
    const s = data?.summary as { FAILED?: number; SUCCESS?: number; PROCESSING?: number; RETRY?: number } | undefined;
    if (
      s &&
      typeof s.FAILED === "number" &&
      typeof s.SUCCESS === "number" &&
      typeof s.PROCESSING === "number" &&
      typeof s.RETRY === "number"
    ) {
      return { FAILED: s.FAILED, SUCCESS: s.SUCCESS, PROCESSING: s.PROCESSING, RETRY: s.RETRY };
    }
    const all = (data?.messages || []) as IMonitorMessage[];
    const result: Record<string, number> = { FAILED: 0, SUCCESS: 0, PROCESSING: 0, RETRY: 0 };
    all.forEach((m) => {
      const st = normalizeStatusKey(m.status);
      if (["FAILED", "FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME", "RCA_FAILED", "PIPELINE_ERROR", "DETECTED", "ARTIFACT_MISSING"].includes(st)) result.FAILED++;
      else if (["AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED", "SUCCESS", "HUMAN_INITIATED_FIX", "FIX_DEPLOYED"].includes(st)) result.SUCCESS++;
      else if (["RCA_IN_PROGRESS", "FIX_IN_PROGRESS", "FIX_ATTEMPTED", "CLASSIFIED", "RCA_COMPLETE", "FIX_APPLIED_PENDING_VERIFICATION", "PROCESSING"].includes(st)) result.PROCESSING++;
      else if (["RETRY", "PENDING_APPROVAL", "TICKET_CREATED", "AWAITING_APPROVAL"].includes(st)) result.RETRY++;
    });
    return result;
  }, [data]);

  const tickets = (ticketsData?.tickets || []) as Ticket[];
  const approvals = (approvalsData?.pending || []) as Approval[];

  const showApprovalToast = useCallback((type: "success" | "error", message: string) => {
    setApprovalToast({ type, message });
  }, []);

  useEffect(() => {
    if (!approvalToast) return;
    const timer = window.setTimeout(() => setApprovalToast(null), 4000);
    return () => window.clearTimeout(timer);
  }, [approvalToast]);

  const parseActionError = useCallback((err: unknown, fallback: string) => {
    if (err instanceof Error && err.message) {
      try {
        const parsed = JSON.parse(err.message) as { detail?: string };
        if (parsed.detail) return parsed.detail;
      } catch {
        return err.message;
      }
      return err.message;
    }
    return fallback;
  }, []);

  const handleApprove = useCallback(async (incidentId: string) => {
    setApprovalActionError(null);
    try {
      await approveIncident(incidentId, "Approved via UI");
      showApprovalToast("success", `Incident ${incidentId} approved — fix pipeline started.`);
      refetchApprovals();
    } catch (err) {
      const message = parseActionError(err, "Approval failed — network error. Please check your connection.");
      setApprovalActionError(message);
      showApprovalToast("error", message);
    }
  }, [refetchApprovals, showApprovalToast, parseActionError]);

  const handleReject = useCallback(async (incidentId: string) => {
    setApprovalActionError(null);
    try {
      await rejectIncident(incidentId, "Rejected via UI");
      showApprovalToast("success", `Incident ${incidentId} rejected.`);
      refetchApprovals();
    } catch (err) {
      const message = parseActionError(err, "Rejection failed — network error. Please check your connection.");
      setApprovalActionError(message);
      showApprovalToast("error", message);
    }
  }, [refetchApprovals, showApprovalToast, parseActionError]);

  const handleApproveAll = useCallback(async () => {
    const pending = (approvalsData?.pending || []) as Approval[];
    const targets = pending.filter((a) => a.status === "AWAITING_APPROVAL");
    if (!targets.length) return;
    setApprovalActionError(null);
    setBulkActionLoading("approving");
    const errors: string[] = [];
    await Promise.allSettled(
      targets.map((a) =>
        approveIncident(a.incident_id, "Bulk approved via UI").catch(() => {
          errors.push(a.incident_id);
        })
      )
    );
    setBulkActionLoading(null);
    if (errors.length) {
      const message = `${errors.length} approval(s) failed. Others succeeded.`;
      setApprovalActionError(message);
      showApprovalToast("error", message);
    } else {
      showApprovalToast("success", `${targets.length} incident(s) approved.`);
    }
    refetchApprovals();
  }, [approvalsData, refetchApprovals, showApprovalToast]);

  const handleRejectAll = useCallback(async () => {
    const pending = (approvalsData?.pending || []) as Approval[];
    const targets = pending.filter((a) => a.status === "AWAITING_APPROVAL");
    if (!targets.length) return;
    setApprovalActionError(null);
    setBulkActionLoading("rejecting");
    const errors: string[] = [];
    await Promise.allSettled(
      targets.map((a) =>
        rejectIncident(a.incident_id, "Bulk rejected via UI").catch(() => {
          errors.push(a.incident_id);
        })
      )
    );
    setBulkActionLoading(null);
    if (errors.length) {
      const message = `${errors.length} rejection(s) failed. Others succeeded.`;
      setApprovalActionError(message);
      showApprovalToast("error", message);
    } else {
      showApprovalToast("success", `${targets.length} incident(s) rejected.`);
    }
    refetchApprovals();
  }, [approvalsData, refetchApprovals, showApprovalToast]);

  const handleMarkResolved = useCallback(async (ticketId: string, currentStatus: string) => {
    setResolvingTicketId(ticketId);
    setTicketActionError(null);
    try {
      if (currentStatus.toUpperCase() === "OPEN") {
        await updateTicket(ticketId, { status: "IN_PROGRESS" });
      }
      await updateTicket(ticketId, { status: "RESOLVED" });
      refetchTickets();
    } catch (e) {
      setTicketActionError(e instanceof Error ? e.message : "Failed to update ticket");
    } finally {
      setResolvingTicketId(null);
    }
  }, [refetchTickets]);

  /* ── Select a message and load full detail ─────────────────────────── */
  const handleSelect = useCallback(async (msg: IMonitorMessage) => {
    const guid = msg.message_guid;
    if (!guid) return;
    // Cancel any running fix poll for the previous message
    pollAbortRef.current.cancelled = true;
    setSelectedGuid(guid);
    setSelectedMsg(msg);
    setDetail(null);
    setFixPatch(null);
    setFixPatchError(null);
    setFixState("idle");
    setFixResult("");
    setFixProgress(null);
    setActiveTab("error");
    setErrorExplain(null);
    setErrorExplainLoading(false);
    setErrorExplainErr(null);
    setDetailLoading(true);
    try {
      const d = await fetchMonitorMessageDetail(guid) as IMessageDetail;
      setDetail(d);

      // Restore previously generated fix plan from DB
      if (d.ai_recommendation?.fix_patch) {
        setFixPatch(d.ai_recommendation.fix_patch);
      }

      // Restore fix outcome state from incident status
      const incStatus = normalizeStatusKey(d.incident_status);
      if (incStatus === "HUMAN_INITIATED_FIX") {
        setFixState("skipped");
        setFixResult(d.ai_recommendation?.fix_summary || "Workflow was already running — no changes were applied.");
      } else if (incStatus === "FIX_DEPLOYED") {
        setFixState("deployed_unverified");
        setFixResult(d.ai_recommendation?.fix_summary || "Fix deployed but XML validation had warnings — verify the workflow manually.");
      } else if (["AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED"].includes(incStatus)) {
        setFixState("success");
        setFixResult(d.ai_recommendation?.fix_summary || "Fix applied and deployed successfully.");
      } else if (incStatus === "AWAITING_APPROVAL") {
        setFixState("idle");
        setFixResult("Queued for approval — go to the Approvals tab to review and approve this fix.");
      } else if (incStatus === "TICKET_CREATED") {
        setFixState("ticket_created");
        setFixResult(d.ai_recommendation?.fix_summary || "Escalated — a ticket has been created for manual review.");
      } else if (["FIX_FAILED", "FIX_FAILED_UPDATE", "FIX_FAILED_DEPLOY", "FIX_FAILED_RUNTIME"].includes(incStatus)) {
        setFixState("error");
        setFixResult(d.ai_recommendation?.fix_summary || "Fix failed — see history for details.");
      }

      if (d.ai_recommendation?.diagnosis) {
        setActiveTab("ai");
      }
    } catch {
      // Keep previous state
    } finally {
      setDetailLoading(false);
    }
  }, []);

  /* ── Run / re-run AI analysis ──────────────────────────────────────── */
  const handleAnalyze = useCallback(async () => {
    if (!selectedGuid) return;
    setAnalyzeLoading(true);
    try {
      await analyzeMessage(selectedGuid);
      const d = await fetchMonitorMessageDetail(selectedGuid) as IMessageDetail;
      setDetail(d);
      setActiveTab("ai");
    } catch {
      // handled
    } finally {
      setAnalyzeLoading(false);
    }
  }, [selectedGuid]);

  /* ── Explain error ─────────────────────────────────────────────────── */
  const handleExplainError = useCallback(async () => {
    if (!selectedGuid) return;
    setErrorExplainLoading(true);
    setErrorExplainErr(null);
    try {
      const exp = await explainError(selectedGuid) as IErrorExplanation;
      setErrorExplain(exp);
    } catch (e) {
      setErrorExplainErr(e instanceof Error ? e.message : "Failed to explain error");
    } finally {
      setErrorExplainLoading(false);
    }
  }, [selectedGuid]);

  /* ── Generate fix patch ────────────────────────────────────────────── */
  const handleGenerateFixPatch = useCallback(async () => {
    if (!selectedGuid) return;
    setFixPatchLoading(true);
    setFixPatchError(null);
    try {
      const patch = await generateFixPatch(selectedGuid) as IFixPatchResponse;
      setFixPatch(patch);
      const d = await fetchMonitorMessageDetail(selectedGuid) as IMessageDetail;
      setDetail(d);
      refetch();
      if (patch.requires_approval || normalizeStatusKey(patch.incident_status) === "AWAITING_APPROVAL") {
        setFixResult("Fix plan generated and queued for approval — go to the Approvals tab to review.");
      } else if (normalizeStatusKey(patch.incident_status) === "TICKET_CREATED") {
        setFixState("ticket_created");
        setFixResult("Fix plan generated — this incident was escalated to a ticket for manual review.");
      }
    } catch (e) {
      setFixPatchError(e instanceof Error ? e.message : "Fix generation failed — check backend logs.");
    } finally {
      setFixPatchLoading(false);
    }
  }, [selectedGuid, refetch]);

  /* ── Live fix polling (shared by handleApplyFix and auto-resume) ──── */
  const pollAbortRef = useRef<{ cancelled: boolean }>({ cancelled: false });

  const startFixPolling = useCallback(async (incidentId: string) => {
    let resolved = false;
    for (let i = 0; i < 120; i++) {
      if (pollAbortRef.current.cancelled) break;
      await new Promise((r) => setTimeout(r, 5000));
      try {
        const s = await fetchFixStatus(incidentId) as Record<string, unknown>;
        const st = normalizeStatusKey(s.status as string);

        // Update the live step progress from the backend
        setFixProgress({
          currentStep: (s.current_step as string) || st,
          stepIndex: (s.step_index as number) || 1,
          totalSteps: (s.total_steps as number) || 4,
          stepsDone: (s.steps_done as string[]) || [],
        });
        const stepLabel = ((s.current_step as string) || "").toLowerCase();
        const stepImpliesDone = stepLabel.includes("complete") || stepLabel.includes("applied and") || stepLabel.includes("deployed successfully");
        if (TERMINAL_STATUSES.has(st) || stepImpliesDone) {
          resolved = true;
          setFixProgress(null);
          if (st === "AWAITING_APPROVAL") {
            setFixState("idle");
            setFixResult((s.fix_summary as string) || "Queued for approval — go to Approvals tab to review.");
          } else if (st === "TICKET_CREATED") {
            setFixState("ticket_created");
            setFixResult((s.fix_summary as string) || "Escalated — a ticket has been created for manual review.");
          } else if (st === "RETRIED") {
            setFixState("success");
            setFixResult((s.fix_summary as string) || "Retry triggered successfully.");
          } else if (st === "HUMAN_INITIATED_FIX") {
            setFixState("skipped");
            setFixResult((s.fix_summary as string) || "Workflow was already running — no changes were applied.");
          } else if (st === "FIX_DEPLOYED") {
            setFixState("deployed_unverified");
            setFixResult((s.fix_summary as string) || "Fix deployed but XML validation had warnings — verify the workflow manually.");
          } else if (SUCCESS_STATUSES.has(st) || stepImpliesDone) {
            setFixState("success");
            setFixResult((s.fix_summary as string) || "Fix applied and deployed successfully.");
          } else {
            setFixState("error");
            setFixResult((s.fix_summary as string) || `Fix failed (${st}).`);
          }
          break;
        }
      } catch {
        // keep polling
      }
    }
    if (!resolved && !pollAbortRef.current.cancelled) {
      setFixProgress(null);
      setFixState("error");
      setFixResult("Fix is taking longer than expected. Click 'Check Status' to poll again, or reload the message.");
    }
  }, []);

  const handleApplyFix = useCallback(async () => {
    if (!selectedGuid) return;
    const currentStatus = normalizeStatusKey(detail?.incident_status||detail?.status|| "");
    if (fixState === "ticket_created" || currentStatus === "TICKET_CREATED") return;
    const isForce = true;
    setFixState("loading");
    setFixResult("");
    setFixProgress({ currentStep: "Submitting fix request…", stepIndex: 0, totalSteps: 5, stepsDone: [] });
    pollAbortRef.current.cancelled = false;
    try {
      const proposedFix = fixPatch?.summary_structured?.proposed_fix || detail?.ai_recommendation?.proposed_fix || undefined;
      const result = await applyMessageFix(selectedGuid, "user", proposedFix, isForce) as Record<string, unknown>;
      const incidentId = (result.incident_id as string) || selectedGuid || detail?.incident_id || "";

      const syncStatus = normalizeStatusKey(result.status as string);
      const syncFixApplied = result.fix_applied === true;
      const syncDeploy = result.deploy_success === true;
      if (syncStatus === "HUMAN_INITIATED_FIX") {
        setFixProgress(null);
        setFixState("skipped");
        setFixResult((result.summary as string) || "Workflow was already running — no changes were applied.");
      } else if (syncStatus === "FIX_DEPLOYED") {
        setFixProgress(null);
        setFixState("deployed_unverified");
        setFixResult((result.summary as string) || "Fix deployed but XML validation had warnings — verify the workflow manually.");
      } else if (SUCCESS_STATUSES.has(syncStatus) || (syncFixApplied && syncDeploy)) {
        setFixProgress(null);
        setFixState("success");
        setFixResult((result.summary as string) || "Fix applied and deployed successfully.");
      } else if (syncStatus === "FIX_FAILED") {
        setFixProgress(null);
        setFixState("error");
        setFixResult((result.summary as string) || "Fix failed.");
      } else if (syncStatus === "AWAITING_APPROVAL") {
        setFixProgress(null);
        setFixState("idle");
        setFixResult((result.summary as string) || "Queued for approval — no automated fix applied yet. Go to Approvals to review.");
      } else if (syncStatus === "TICKET_CREATED") {
        setFixProgress(null);
        setFixState("ticket_created");
        setFixResult((result.summary as string) || "Escalated — a ticket has been created for manual review.");
      } else if (syncStatus === "RETRIED") {
        setFixProgress(null);
        setFixState("success");
        setFixResult((result.summary as string) || "Retry triggered successfully.");
      } else if (
        syncStatus === "PIPELINE_STARTED" ||
        syncStatus.startsWith("PIPELINE_") ||
        incidentId
      ) {
        setFixProgress({
          currentStep: "Pipeline running…",
          stepIndex: 1,
          totalSteps: 5,
          stepsDone: ["Submit"],
        });
        await startFixPolling(incidentId || selectedGuid);
      } else {
        setFixProgress(null);
        setFixState("success");
        setFixResult((result.message as string) || "Fix queued. Refresh later for status.");
      }
      try {
        const d = await fetchMonitorMessageDetail(selectedGuid) as IMessageDetail;
        setDetail(d);
      } catch { /* ignore */ }
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : "Fix failed";
      if (errMsg.includes("409") || errMsg.toLowerCase().includes("already in progress")) {
        const incidentId = detail?.incident_id || "";
        if (incidentId) {
          setFixResult("");
          setFixProgress({ currentStep: "Fix already in progress — connecting…", stepIndex: 0, totalSteps: 5, stepsDone: [] });
          await startFixPolling(incidentId);
        } else {
          setFixState("error");
          setFixProgress(null);
          setFixResult("Fix already in progress by another user. Reload this message to see status.");
        }
      } else {
        setFixState("error");
        setFixProgress(null);
        setFixResult(errMsg);
      }
    }
  }, [selectedGuid, fixPatch, detail, startFixPolling, fixState]);

  const handleCheckStatus = useCallback(async () => {
    const incidentId = detail?.incident_id || "";
    if (!incidentId) return;
    setFixState("loading");
    setFixResult("");
    setFixProgress({ currentStep: "Reconnecting to fix pipeline…", stepIndex: 0, totalSteps: 5, stepsDone: [] });
    pollAbortRef.current.cancelled = false;
    await startFixPolling(incidentId);
  }, [detail, startFixPolling]);

  useEffect(() => {
    if (!detail) return;
    const st = normalizeStatusKey(detail.incident_status||detail.status);
    const incidentId = detail.incident_id || "";
    if (fixState !== "idle") return;

    if (["FIX_IN_PROGRESS", "RCA_IN_PROGRESS", "FIX_APPLIED_PENDING_VERIFICATION", "FIX_ATTEMPTED", "PROCESSING"].includes(st)) {
      const stepLabel =
        st === "RCA_IN_PROGRESS"                   ? "Analyzing root cause…" :
        st === "FIX_APPLIED_PENDING_VERIFICATION"  ? "Verifying fix…" :
        st === "FIX_ATTEMPTED"                     ? "Fix attempt in progress…" :
                                                     "Fix in progress…";
      setFixState("loading");
      setFixProgress({ currentStep: stepLabel, stepIndex: 0, totalSteps: 5, stepsDone: [] });
      if (incidentId) {
        pollAbortRef.current.cancelled = false;
        startFixPolling(incidentId);
      }
    } else if (["AUTO_FIXED", "HUMAN_FIXED", "FIX_VERIFIED", "RETRIED"].includes(st)) {
      setFixState("success");
      setFixResult(detail.ai_recommendation?.fix_summary || "Fix applied and deployed.");
    } else if (["TICKET_CREATED"].includes(st)) {
      setFixState("ticket_created");
      setFixResult(detail.ai_recommendation?.fix_summary || "Escalated — a ticket has been created for manual review.");
    }else if (["FIX_FAILED", "PIPELINE_ERROR"].includes(st)) {
      setFixState("error");
      setFixResult(detail.ai_recommendation?.fix_summary || "Fix failed.");
    }
  }, [detail, fixState, startFixPolling]);

  useEffect(() => {
    return () => { pollAbortRef.current.cancelled = true; };
  }, [selectedGuid]);

  useEffect(() => {
    if (!analyzeLoading) { setAnalyzeStep(0); return; }
    const t = setInterval(() => setAnalyzeStep(s => (s + 1) % ANALYZE_STEPS.length), 1800);
    return () => clearInterval(t);
  }, [analyzeLoading]);

  // --------------------------------------------------------------------------
  // Render
  // --------------------------------------------------------------------------
  return (
    <div className={styles.page}>
      <div className={styles.pageHeader}>
        <h2 className={styles.pageTitle}>Observability</h2>
      </div>

      <div className={styles.mainTabBar}>
        <button className={`${styles.mainTab} ${mainTab === "errortypeguide" ? styles.mainTabActive : ""}`} onClick={() => setMainTab("errortypeguide")}>Error Type Guide</button>
        <button className={`${styles.mainTab} ${mainTab === "messages" ? styles.mainTabActive : ""}`} onClick={() => setMainTab("messages")}>Messages</button>
        <button className={`${styles.mainTab} ${mainTab === "tickets" ? styles.mainTabActive : ""}`} onClick={() => setMainTab("tickets")}>Tickets</button>
        <button className={`${styles.mainTab} ${mainTab === "approvals" ? styles.mainTabActive : ""}`} onClick={() => setMainTab("approvals")}>Approvals</button>
      </div>

      {mainTab === "errortypeguide" && (
        <div className={styles.errorGuideTab}>
          <ErrorTypeGuideTab />
        </div>
      )}

      <div style={{ display: mainTab === "messages" ? "contents" : "none" }}>
        {/* Summary cards */}
        <div className={styles.summaryRow}>
          {SUMMARY_CARD_KEYS.map((k) => {
            const cfg = STATUS_CONFIG[k];
            return (
              <div
                key={k}
                className={`${styles.summaryCard} ${filters.statuses.includes(k) ? styles.summaryCardActive : ""}`}
                style={{ borderTop: `3px solid ${cfg.dot}` }}
                onClick={() => setFilters((f) => ({
                  ...f,
                  statuses: f.statuses.includes(k) ? f.statuses.filter((s) => s !== k) : [...f.statuses, k],
                }))}
                data-tip={CARD_TIPS[k] ?? `Click to filter by ${cfg.label} status`}
              >
                <span className={styles.summaryCount} style={{ color: cfg.color }}>{counts[k] ?? 0}</span>
                <span className={styles.summaryLabel} style={{ color: cfg.color }}>{cfg.label}</span>
              </div>
            );
          })}
        </div>

        {/* Active filter chips */}
        {filters.statuses.length > 0 && (
          <div className={styles.chipRow}>
            {filters.statuses.map((s) => {
              const cfg = STATUS_CONFIG[s];
              return (
                <span key={s} className={styles.filterChip} style={{ background: cfg.bg, color: cfg.color, borderColor: cfg.dot }}>
                  {cfg.label}
                  <button onClick={() => setFilters((f) => ({ ...f, statuses: f.statuses.filter((x) => x !== s) }))}>x</button>
                </span>
              );
            })}
          </div>
        )}

          {/* ── Two-column layout ── */}
          <div className={styles.columns}>
            {/* Message list */}
            <div className={`${styles.listCol} ${selectedGuid ? styles.listColNarrow : ""}`}>
              <div className={styles.listColHeader}>
                <span className={styles.listColTitle}>Logic App Runs</span>
                <div className={styles.listColControls}>
                  <div className={styles.listColSearch}>
                    <span className={styles.listColSearchIcon}>🔍</span>
                    <input
                      className={styles.listColSearchInput}
                      placeholder="search run ID / workflow name"
                      value={filters.idQuery}
                      onChange={(e) => setFilters((f) => ({ ...f, idQuery: e.target.value }))}
                    />
                  </div>
                  <select
                    className={styles.listColStatusSelect}
                    value=""
                    onChange={(e) => {
                      const v = e.target.value;
                      if (!v) return;
                      setFilters((f) => ({ ...f, statuses: f.statuses.includes(v) ? f.statuses.filter((s) => s !== v) : [...f.statuses, v] }));
                    }}
                  >
                    <option value="">Status</option>
                    {Object.entries(STATUS_CONFIG).map(([k, c]) => <option key={k} value={k}>{c.label}</option>)}
                  </select>
                  <button className={styles.listColRefreshBtn} onClick={() => refetch()} disabled={isFetching}>
                    Refresh
                  </button>
                  <button className={styles.listColResetBtn} onClick={() => setFilters(INITIAL_FILTERS)}>Reset</button>
                </div>
              </div>
              {isLoading ? (
                <div className={styles.centered}>
                  <div className={styles.spinner} />
                  <span>Loading messages...</span>
                </div>
              ) : messages.length === 0 ? (
                <div className={styles.msgEmptyState}>
                  <img src="/empty-messages.svg" alt="" className={styles.msgEmptyImg} draggable={false} />
                  <p className={styles.msgEmptyTitle}>No messages found</p>
                  <p className={styles.msgEmptyHint}>Errors detected by the autonomous monitor will appear here.</p>
                </div>
              ) : (
                <div className={styles.messageList}>
                  {messages.map((msg, i) => {
                    const cfg = STATUS_CONFIG[normalizeStatusKey(msg.status)] ?? STATUS_CONFIG.FAILED;
                    const isSelected = selectedGuid !== null && selectedGuid === msg.message_guid;
                    return (
                      <div
                        key={msg.message_guid || i}
                        className={`${styles.messageRow} ${isSelected ? styles.messageRowSelected : ""}`}
                        style={{ borderLeft: `3px solid ${isSelected ? cfg.dot : "transparent"}` }}
                      >
                        <div className={styles.messageHeader}>
                          <div className={styles.messageMain}>
                            <span className={styles.messageName}>
                              {msg.iflow_display || msg.title || "Unknown"}
                            </span>
                            <StatusPill status={msg.status} />
                          </div>
                          <button
                            type="button"
                            className={styles.messageOpenBtn}
                            onClick={() => {
                              if (isSelected) {
                                setSelectedGuid(null);
                                setSelectedMsg(null);
                                setDetail(null);
                                return;
                              }
                              handleSelect(msg);
                            }}
                            aria-label={`Open details for ${msg.iflow_display || msg.title || msg.message_guid || "message"}`}
                            title={isSelected ? "Close details" : "Open details"}
                          >
                            <SvgIcon name="chevron-right" size={16} />
                          </button>
                        </div>
                        <div className={styles.messageMeta}>
                          <span className={styles.metaItem}>{msg.log_start || msg.updatedAt || "--"}</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

          {/* Detail panel */}
          {selectedGuid && (
            <div className={styles.detailPanel}>
              <div className={styles.detailHeader}>
                <div className={styles.detailHeaderLeft}>
                  <h3 className={styles.detailTitle} title={detail?.iflow_display || selectedMsg?.iflow_display || selectedGuid}>
                    {detail?.iflow_display || selectedMsg?.iflow_display || selectedGuid}
                  </h3>
                  <StatusPill status={detail?.status || selectedMsg?.status || "UNKNOWN"} />
                  {detail?.last_updated && <span className={styles.detailUpdated}>Last Updated at: {detail.last_updated}</span>}
                </div>
                <div className={styles.detailHeaderRight}>
                  <button className={styles.recheckBtn} onClick={handleAnalyze} disabled={analyzeLoading}>{analyzeLoading ? "Analyzing..." : "Recheck"}</button>
                  <button className={styles.closeBtn} onClick={() => { setSelectedGuid(null); setSelectedMsg(null); setDetail(null); }}>x</button>
                </div>
              </div>

              <div className={styles.tabBar}>
                {TABS.map((tab) => (
                  <button
                    key={tab.key}
                    className={`${styles.tab} ${activeTab === tab.key ? styles.tabActive : ""}`}
                    onClick={() => setActiveTab(tab.key)}
                    data-tip={tab.tip}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>

              {detailLoading ? (
                <div className={styles.centered}>
                  <div className={styles.spinner} />
                  <span>Loading details...</span>
                </div>
              ) : detail ? (
                <div className={styles.detailBody}>
                  {activeTab === "error" && (
                    <div className={styles.tabContent}>
                      <div className={styles.errorBox}>
                        <code className={styles.errorCode}>{detail.error_details.error_message || detail.error_details.raw_error_text || "No error details available"}</code>
                      </div>
                      {detail.error_details.error_type && (
                        <div className={styles.detailMeta}>
                          <span className={styles.metaLabel}>Error Type:</span>
                          <span className={styles.metaValue}>{detail.error_details.error_type}</span>
                        </div>
                      )}
                      {detail.error_details.log_start && (
                        <div className={styles.detailMeta}>
                          <span className={styles.metaLabel}>Processing Start:</span>
                          <span className={styles.metaValue}>{detail.error_details.log_start}</span>
                        </div>
                      )}
                      {detail.error_details.log_end && (
                        <div className={styles.detailMeta}>
                          <span className={styles.metaLabel}>Processing End:</span>
                          <span className={styles.metaValue}>{detail.error_details.log_end}</span>
                        </div>
                      )}

                      <div className={styles.explainTrigger}>
                        {errorExplain ? (
                          <ErrorExplanationCard exp={errorExplain} />
                        ) : (
                          <button className={styles.explainBtn} onClick={handleExplainError} disabled={errorExplainLoading}>
                            {errorExplainLoading ? <><span className={styles.explainSpinner} /> Analyzing error...</> : <><span className={styles.explainSparkle}><SvgIcon name="rca" size={14} /></span> Explain with AI</>}
                          </button>
                        )}
                        {errorExplainErr && <div className={styles.explainErrText}>{errorExplainErr}</div>}
                      </div>
                    </div>
                  )}

                  {activeTab === "ai" && (
                    <div className={styles.tabContent}>
                      {analyzeLoading ? (
                        <div className={styles.aiThinkingLoader}>
                          <div className={styles.aiThinkingHeader}>
                            <div className={styles.aiThinkingAvatar}>AI</div>
                            <span className={styles.aiThinkingTitle}>Azure AI</span>
                            <div className={styles.aiThinkingDots}><span /><span /><span /></div>
                          </div>
                          <div key={analyzeStep} className={styles.aiThinkingStep}>{ANALYZE_STEPS[analyzeStep]}</div>
                          <div className={styles.aiThinkingBar}><div className={styles.aiThinkingBarFill} /></div>
                        </div>
                      ) : !detail.ai_recommendation?.diagnosis ? (
                        <div className={styles.noRcaBox}>
                          <p>No AI analysis available yet for this message.</p>
                          <button className={styles.analyzeBtn} onClick={handleAnalyze} disabled={analyzeLoading}>Run AI Analysis</button>
                        </div>
                      ) : (
                        <>
                          <div className={styles.aiHeader}>
                            <span className={styles.aiIcon}><SvgIcon name="rca" size={16} /></span>
                            <span className={styles.aiTitle}>AI Recommendations & Suggested Fix</span>
                          </div>
                          {detail.ai_recommendation.diagnosis && (
                            <div className={styles.aiSection}>
                              <div className={styles.aiSectionLabel}>Diagnosis:</div>
                              <div className={styles.aiSectionText}><RichText text={detail.ai_recommendation.diagnosis} /></div>
                            </div>
                          )}
                          <FieldChangeHighlight changes={detail.ai_recommendation.field_changes} />
                          {detail.ai_recommendation.proposed_fix && (
                            <div className={styles.aiSection}>
                              <div className={styles.aiSectionLabel}>Suggested Fix:</div>
                              <div className={styles.aiSectionText}><RichText text={detail.ai_recommendation.proposed_fix} /></div>
                            </div>
                          )}
                          {detail.ai_recommendation.confidence > 0 && (
                            <div data-tip="AI confidence in the root cause: ≥90% = High (green), 70–89% = Medium (amber), <70% = Low (red)">
                              <ConfidenceBadge value={detail.ai_recommendation.confidence} label={detail.ai_recommendation.confidence_label} />
                            </div>
                          )}
                          {fixPatch && (
                            <div className={styles.fixPatchSection}>
                              <h4 className={styles.fixPatchTitle}>Steps (Fix Plan)</h4>
                              {fixPatch.summary && <div className={styles.fixPatchSummary}><strong>Summary:</strong> {fixPatch.summary}</div>}
                              <FixPlanSteps steps={fixPatch.steps} />
                            </div>
                          )}
                        </>
                      )}
                    </div>
                  )}

                  {activeTab === "properties" && (
                    <div className={styles.tabContent}>
                      <h4 className={styles.propGroupTitle}>Message Properties</h4>
                      <div className={styles.propGrid}>
                        {Object.entries(detail.properties.message || {}).map(([k, v]) => v ? (
                          <div key={k} className={styles.propRow}>
                            <span className={styles.propLabel}>{k.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}</span>
                            <span className={styles.propValue}>{String(v)}</span>
                          </div>
                        ) : null)}
                      </div>
                      {detail.properties.adapter && Object.values(detail.properties.adapter).some(Boolean) && (
                        <>
                          <h4 className={styles.propGroupTitle}>Adapter</h4>
                          <div className={styles.propGrid}>
                            {Object.entries(detail.properties.adapter).map(([k, v]) => v ? (
                              <div key={k} className={styles.propRow}>
                                <span className={styles.propLabel}>{k.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}</span>
                                <span className={styles.propValue}>{String(v)}</span>
                              </div>
                            ) : null)}
                          </div>
                        </>
                      )}
                      {detail.properties.business_context && Object.values(detail.properties.business_context).some(Boolean) && (
                        <>
                          <h4 className={styles.propGroupTitle}>Business Context</h4>
                          <div className={styles.propGrid}>
                            {Object.entries(detail.properties.business_context).map(([k, v]) => v ? (
                              <div key={k} className={styles.propRow}>
                                <span className={styles.propLabel}>{k.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}</span>
                                <span className={styles.propValue}>{String(v)}</span>
                              </div>
                            ) : null)}
                          </div>
                        </>
                      )}
                    </div>
                  )}

                  {activeTab === "artifact" && (
                    <div className={styles.tabContent}>
                      <div className={styles.propGrid}>
                        {[
                          ["Workflow Name", detail.artifact.name],
                          ["Workflow ID", detail.artifact.artifact_id],
                          ["Version", detail.artifact.version],
                          ["Resource Group", detail.artifact.package],
                          ["Created On", detail.artifact.deployed_on],
                          ["Created By", detail.artifact.deployed_by],
                          ["Region", detail.artifact.runtime_node],
                          ["Provisioning State", detail.artifact.status],
                        ].map(([label, val]) => val ? (
                          <div key={label} className={styles.propRow}>
                            <span className={styles.propLabel}>{label}</span>
                            <span className={styles.propValue}>{String(val)}</span>
                          </div>
                        ) : null)}
                      </div>
                    </div>
                  )}

                  {activeTab === "attachments" && (
                    <div className={styles.tabContent}>
                      {detail.related_knowledge && detail.related_knowledge.length > 0 ? (
                        <div className={styles.knowledgeList}>
                          {detail.related_knowledge.map((k, idx) => (
                            <div key={idx} className={styles.knowledgeCard}>
                              <div className={styles.knowledgeTitle}>{k.title}</div>
                              <div className={styles.knowledgeContent}>{k.content}</div>
                              <div className={styles.knowledgeSimilarity}>Relevance: {k.similarity}%</div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className={styles.emptyTab}>No related knowledge found for this error.</div>
                      )}
                    </div>
                  )}

                  {activeTab === "history" && (
                    <div className={styles.tabContent}>
                      {detail.history?.length > 0 ? <Timeline entries={detail.history} /> : <div className={styles.emptyTab}>No history entries yet.</div>}
                    </div>
                  )}
                </div>
              ) : (
                <div className={styles.centered}><span>Could not load message details.</span></div>
              )}

              {/* Sticky fix footer */}
              {detail && activeTab === "ai" && detail.ai_recommendation?.diagnosis && (
                <div className={styles.fixFooter}>
                  {fixState === "loading" && fixProgress && (
                    <div className={styles.fixProgressInline}>
                      <PipelineStageRail stepIndex={fixProgress.stepIndex} totalSteps={fixProgress.totalSteps} currentStep={fixProgress.currentStep} />
                      <div className={styles.fixProgressCurrentStep}><span className={styles.fixProgressSpinner} /><span>{fixProgress.currentStep}</span></div>
                    </div>
                  )}
                  {fixResult && <div className={`${styles.fixResultBanner} ${styles[`fixResultBanner_${fixState}`] || ""}`}>{cleanFixSummary(fixResult)}</div>}
                  <div className={styles.fixFooterActions}>
                    {fixPatch ? (
                      <>
                        {fixState !== "skipped" && fixState !== "deployed_unverified" && (
                          <button className={`${styles.applyFixBtn} ${styles[`applyFixBtn_${fixState}`] || ""}`} onClick={handleApplyFix} disabled={fixState === "loading" || fixState === "success" || fixState === "ticket_created"}>
                            {(fixState === "idle" || fixState === "ticket_created") && <><SvgIcon name="lightning" size={13} style={{ marginRight: "0.35rem", verticalAlign: "middle" }} />Apply Fix</>}
                            {fixState === "loading" && <><span className={styles.btnSpinner} /> Applying...</>}
                            {fixState === "success" && "✓ Fix Applied"}
                            {fixState === "error" && "↺ Retry Fix"}
                          </button>
                        )}
                        {fixState === "skipped" && (
                          <>
                            <span className={styles.alreadyRunningBadge}>⚠ Already Running — No Changes Applied</span>
                            <button className={styles.forceApplyBtn} onClick={handleApplyFix}>Force Apply Fix</button>
                          </>
                        )}
                        {fixState === "deployed_unverified" && <span className={styles.deployedUnverifiedBadge}>⚠ Deployed — Verify Workflow Manually</span>}
                        {fixState === "error" && detail?.incident_id && (
                          <button className={styles.checkStatusBtn} onClick={handleCheckStatus}>Check Status</button>
                        )}
                      </>
                    ) : (
                      detail.ai_recommendation.can_generate_fix && (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.35rem", alignItems: "flex-start" }}>
                          <button className={styles.generateFixBtn} onClick={handleGenerateFixPatch} disabled={fixPatchLoading}>
                            {fixPatchLoading ? <><span className={styles.btnSpinner} /> Generating...</> : <><SvgIcon name="wrench" size={14} style={{ verticalAlign: "middle", marginRight: "0.3rem" }} />Generate Fix Patch</>}
                          </button>
                          {fixPatchError && (
                            <div style={{ fontSize: "0.78rem", color: "#dc2626", maxWidth: 340, lineHeight: 1.4 }}>
                              ✕ {fixPatchError}
                            </div>
                          )}
                        </div>
                      )
                    )}
                    {fixPatch && fixState === "idle" && normalizeStatusKey(detail?.incident_status) === "AWAITING_APPROVAL" && (
                      <span className={styles.fixFooterHint}>Fix plan ready — review and approve in the Approvals tab before it can be applied.</span>
                    )}
                    {fixPatch && fixState === "idle" && normalizeStatusKey(detail?.incident_status) !== "AWAITING_APPROVAL" && (
                      <span className={styles.fixFooterHint}>Fix plan ready — click Apply Fix to execute the automated patch & deploy.</span>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {mainTab === "tickets" && (
        <div className={styles.ticketsContainer}>
          <div className={styles.ticketsHeader}>
            <div><h2>Escalation Tickets</h2><p className={styles.tabDescription}>Incidents that could not be auto-remediated are escalated here as tickets for manual review and resolution.</p></div>
            <button onClick={() => refetchTickets()} disabled={ticketsLoading}>{ticketsLoading ? "Loading..." : "Refresh"}</button>
          </div>
          {!ticketsLoading && tickets.length > 0 && (
            <div className={styles.kpiRow}>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #dc2626" }}>
                <span className={styles.kpiValue} style={{ color: "#dc2626" }}>{tickets.filter(t => ["CRITICAL", "HIGH"].includes((t.priority || "").toUpperCase())).length}</span>
                <span className={styles.kpiLabel}>High Priority</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #d97706" }}>
                <span className={styles.kpiValue} style={{ color: "#d97706" }}>{tickets.filter(t => (t.status || "").toUpperCase() === "OPEN").length}</span>
                <span className={styles.kpiLabel}>Open Tickets</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #2563eb" }}>
                <span className={styles.kpiValue} style={{ color: "#2563eb" }}>{tickets.filter(t => (t.status || "").toUpperCase() === "IN_PROGRESS").length}</span>
                <span className={styles.kpiLabel}>In Progress</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #16a34a" }}>
                <span className={styles.kpiValue} style={{ color: "#16a34a" }}>{tickets.filter(t => (t.status || "").toUpperCase() === "RESOLVED").length}</span>
                <span className={styles.kpiLabel}>Resolved</span>
              </div>
            </div>
          )}
          {ticketActionError && <div className={styles.approvalErrorBanner}>{ticketActionError}<button onClick={() => setTicketActionError(null)}>✕</button></div>}
          {ticketsLoading ? (
            <div className={styles.centered}><div className={styles.spinner} /><span>Loading tickets...</span></div>
          ) : tickets.length === 0 ? (
            <div className={styles.ticketEmptyState}>
              <img src="/empty-tickets.svg" alt="" className={styles.ticketEmptyImg} draggable={false} />
              <p className={styles.ticketEmptyTitle}>No Tickets Created</p>
              <p className={styles.ticketEmptyHint}>Incidents that could not be auto-remediated will appear here.</p>
            </div>
          ) : (
            <div className={styles.ticketsList}>
              {tickets.map((ticket) => (
                <div key={ticket.ticket_id} className={styles.ticketCard}>
                  <div className={styles.ticketHeader}>
                    <div><h3>{ticket.title}</h3><span className={styles.ticketId}>#{ticket.ticket_id}</span></div>
                    <div className={styles.ticketBadges}>
                      <span className={`${styles.badge} ${styles[`priority_${ticket.priority.toLowerCase()}`]}`}>{ticket.priority}</span>
                      <span className={`${styles.badge} ${styles[`status_${ticket.status.toLowerCase().replace(/\s+/g, '_')}`]}`}>{ticket.status}</span>
                    </div>
                  </div>
                  <div className={styles.ticketBody}>
                    <div className={styles.ticketMeta}>
                      <span><strong>Workflow:</strong> {ticket.iflow_id}</span>
                      <span><strong>Error Type:</strong> {ticket.error_type}</span>
                      {ticket.assigned_to && <span><strong>Assigned To:</strong> {ticket.assigned_to}</span>}
                    </div>
                    {(() => {
                      const parsed = parseTicketDescription(ticket.description);
                      const hasStructure = parsed.error || parsed.root_cause || parsed.proposed_fix;
                      if (!hasStructure) return <p className={styles.ticketDescription}>{ticket.description}</p>;
                      return (
                        <div className={styles.ticketSections}>
                          {parsed.error && (
                            <div className={styles.approvalSection}><strong>Error Message</strong><p className={styles.errorText}>{parsed.error}</p></div>
                          )}
                          {parsed.root_cause && (
                            <div className={styles.approvalSection}><strong>Root Cause</strong><p>{parsed.root_cause}</p></div>
                          )}
                          {parsed.proposed_fix && (
                            <div className={styles.approvalSection}><strong>Proposed Fix</strong><p className={styles.fixText}>{parsed.proposed_fix}</p></div>
                          )}
                          {(parsed.occurrence_count || parsed.rca_confidence) && (
                            <div className={styles.ticketSectionMeta}>
                              {parsed.occurrence_count && <span><strong>Occurrences:</strong> {parsed.occurrence_count}</span>}
                              {parsed.rca_confidence && !isNaN(parseFloat(parsed.rca_confidence)) && <span><strong>RCA Confidence:</strong> {(parseFloat(parsed.rca_confidence) * 100).toFixed(0)}%</span>}
                            </div>
                          )}
                        </div>
                      );
                    })()}
                    {ticket.resolution_notes && (
                      <div className={styles.ticketResolution}><strong>Resolution:</strong><p>{ticket.resolution_notes}</p></div>
                    )}
                  </div>
                  <div className={styles.ticketFooter}>
                    <span>Created: {new Date(ticket.created_at).toLocaleString()}</span>
                    <span>Updated: {new Date(ticket.updated_at).toLocaleString()}</span>
                    <div className={styles.approvalActions} style={{ marginLeft: "auto" }}>
                      {(ticket.status || "").toUpperCase() !== "RESOLVED" ? (
                        <button className={`${styles.btn} ${styles.btnApprove}`} disabled={resolvingTicketId === ticket.ticket_id} onClick={() => handleMarkResolved(ticket.ticket_id, ticket.status)}>
                          {resolvingTicketId === ticket.ticket_id ? "Resolving…" : "✓ Mark Resolved"}
                        </button>
                      ) : <span style={{ color: "#16a34a", fontSize: "0.82rem", fontWeight: 600 }}>✓ Resolved</span>}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {mainTab === "eventmesh" && <EventMeshFlow />}

      {mainTab === "approvals" && (
        <div className={styles.approvalsContainer}>
          <div className={styles.approvalsHeader}>
            <div><h2>Pending Approvals</h2><p className={styles.tabDescription}>AI-proposed fixes awaiting human sign-off before being applied to Azure Logic Apps. Review and approve or reject each one below.</p></div>
            <div className={styles.approvalsHeaderActions}>
              <button className={`${styles.btn} ${styles.btnApprove}`} onClick={handleApproveAll} disabled={!!bulkActionLoading || approvalsLoading || approvals.filter(a => a.status === "AWAITING_APPROVAL").length === 0}>
                {bulkActionLoading === "approving" ? "Approving..." : "✓ Approve All"}
              </button>
              <button className={`${styles.btn} ${styles.btnReject}`} onClick={handleRejectAll} disabled={!!bulkActionLoading || approvalsLoading || approvals.filter(a => a.status === "AWAITING_APPROVAL").length === 0}>
                {bulkActionLoading === "rejecting" ? "Rejecting..." : "✗ Reject All"}
              </button>
              <button onClick={() => refetchApprovals()} disabled={approvalsLoading || !!bulkActionLoading}>{approvalsLoading ? "Loading..." : "Refresh"}</button>
            </div>
          </div>
          {approvalToast && (
            <div className={`${styles.approvalToast} ${approvalToast.type === "success" ? styles.approvalToastSuccess : styles.approvalToastError}`}>
              {approvalToast.message}
              <button onClick={() => setApprovalToast(null)} aria-label="Dismiss">✕</button>
            </div>
          )}
          {approvalActionError && <div className={styles.approvalErrorBanner}>{approvalActionError}<button onClick={() => setApprovalActionError(null)}>✕</button></div>}
          {!approvalsLoading && approvals.length > 0 && (
            <div className={styles.kpiRow}>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #7c3aed" }}>
                <span className={styles.kpiValue} style={{ color: "#7c3aed" }}>{approvals.filter(a => a.status === "AWAITING_APPROVAL").length}</span>
                <span className={styles.kpiLabel}>Awaiting Approval</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #16a34a" }}>
                <span className={styles.kpiValue} style={{ color: "#16a34a" }}>{approvals.filter(a => a.rca_confidence >= 0.9).length}</span>
                <span className={styles.kpiLabel}>High Confidence (≥90%)</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #d97706" }}>
                <span className={styles.kpiValue} style={{ color: "#d97706" }}>{approvals.filter(a => a.rca_confidence >= 0.7 && a.rca_confidence < 0.9).length}</span>
                <span className={styles.kpiLabel}>Medium Confidence (70-89%)</span>
              </div>
              <div className={styles.kpiCard} style={{ borderTop: "3px solid #dc2626" }}>
                <span className={styles.kpiValue} style={{ color: "#dc2626" }}>{approvals.filter(a => a.rca_confidence < 0.7).length}</span>
                <span className={styles.kpiLabel}>Low Confidence (&lt;70%)</span>
              </div>
            </div>
          )}
          {approvalsLoading ? (
            <div className={styles.centered}><div className={styles.spinner} /><span>Loading approvals...</span></div>
          ) : approvals.length === 0 ? (
            <div className={styles.ticketEmptyState}>
              <img src="/empty-approvals.svg" alt="" className={styles.ticketEmptyImg} draggable={false} />
              <p className={styles.ticketEmptyTitle}>No Pending Approvals</p>
              <p className={styles.ticketEmptyHint}>You're all caught up! There are no pending approvals at the moment.</p>
            </div>
          ) : (
            <div className={styles.approvalsList}>
              {approvals.map((approval) => (
                <div key={approval.incident_id} className={styles.approvalCard}>
                  <div className={styles.approvalHeader}>
                    <div><h3>{approval.iflow_id}</h3><span className={styles.approvalId}>Incident: {approval.incident_id}</span></div>
                    <StatusPill status={approval.status} />
                  </div>
                  <div className={styles.approvalBody}>
                    <div className={styles.approvalSection}><strong>Error Type:</strong><span>{approval.error_type}</span></div>
                    <div className={styles.approvalSection}><strong>Error Message:</strong><p className={styles.errorText}>{approval.error_message}</p></div>
                    <div className={styles.approvalSection}><strong>Root Cause:</strong><p>{approval.root_cause}</p></div>
                    <div className={styles.approvalSection}><strong>Proposed Fix:</strong><p className={styles.fixText}>{approval.proposed_fix}</p></div>
                    <div className={styles.approvalMeta}>
                      <span><strong>Confidence:</strong> {((approval.rca_confidence ?? 0) * 100).toFixed(0)}%</span>
                      <span><strong>Created:</strong> {approval.created_at ? new Date(approval.created_at).toLocaleString() : "—"}</span>
                      <span><strong>Pending Since:</strong> {approval.pending_since ? new Date(approval.pending_since).toLocaleString() : "—"}</span>
                    </div>
                  </div>
                  {approval.status === "AWAITING_APPROVAL" && (
                    <div className={styles.approvalActions}>
                      <button className={`${styles.btn} ${styles.btnApprove}`} onClick={() => handleApprove(approval.incident_id)}>✓ Approve</button>
                      <button className={`${styles.btn} ${styles.btnReject}`} onClick={() => handleReject(approval.incident_id)}>✗ Reject</button>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}