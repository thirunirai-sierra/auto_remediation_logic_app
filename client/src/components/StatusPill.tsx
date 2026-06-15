import styles from "./statusPill.module.css";

type StatusCfg = { label: string; color: string; bg: string; dot: string };

const RED: StatusCfg = { label: "Failed", color: "#dc2626", bg: "#fee2e2", dot: "#ef4444" };
const GREEN: StatusCfg = { label: "Success", color: "#16a34a", bg: "#dcfce7", dot: "#22c55e" };
const BLUE: StatusCfg = { label: "Processing", color: "#2563eb", bg: "#dbeafe", dot: "#3b82f6" };
const AMBER: StatusCfg = { label: "Retry", color: "#d97706", bg: "#fef3c7", dot: "#f59e0b" };
const PURPLE: StatusCfg = { label: "Pending", color: "#7c3aed", bg: "#ede9fe", dot: "#8b5cf6" };
const GREY: StatusCfg = { label: "Unknown", color: "#6b7280", bg: "#f3f4f6", dot: "#9ca3af" };

export const STATUS_CONFIG: Record<string, StatusCfg> = {
  FAILED: RED,
  SUCCESS: GREEN,
  PROCESSING: BLUE,
  RETRY: AMBER,
  DETECTED: { ...RED, label: "Detected" },
  CLASSIFIED: { ...BLUE, label: "Classified" },
  ANALYZED: { ...BLUE, label: "Analyzed" },
  ANALYZING: { ...BLUE, label: "Analyzing" },
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
  ARTIFACT_MISSING: { ...GREY, label: "Artifact Missing" },
  PIPELINE_ERROR: { ...RED, label: "Pipeline Error" },
  REJECTED: { ...RED, label: "Rejected" },
  RETRIED: { ...GREEN, label: "Retried" },
  PIPELINE_STARTED: { ...BLUE, label: "Pipeline Started" },
  PIPELINE_IN_PROGRESS: { ...BLUE, label: "Pipeline Running" },
  PIPELINE_OBSERVER: { ...BLUE, label: "Pipeline Observer" },
  PIPELINE_CLASSIFIER: { ...BLUE, label: "Pipeline Classifier" },
  PIPELINE_RCA: { ...BLUE, label: "Pipeline RCA" },
  PIPELINE_FIXER: { ...BLUE, label: "Pipeline Fixer" },
  PIPELINE_VERIFIER: { ...BLUE, label: "Pipeline Verifier" },
};

/** DB/API often returns spaced labels ("Ticket Created"); keys are SNAKE_CASE. */
export function normalizeStatusKey(status: string | undefined | null): string {
  return (status ?? "").trim().toUpperCase().replace(/\s+/g, "_");
}

export default function StatusPill({ status }: { status: string }) {
  const key = normalizeStatusKey(status);
  const cfg = STATUS_CONFIG[key] ?? { ...GREY, label: status || "Unknown" };
  return (
    <span className={styles.pill} style={{ color: cfg.color, background: cfg.bg }}>
      <span className={styles.dot} style={{ background: cfg.dot }} />
      {cfg.label}
    </span>
  );
}
