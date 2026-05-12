import type { IconName } from "../../../components/icons/SvgIcon";

export interface Incident {
  [key: string]: unknown;
  incident_id: string;
  iflow_id?: string;
  iflow_name?: string;
  status: string;
}

export function isIncident(value: Record<string, unknown>): value is Incident {
  return typeof value.incident_id === "string" && typeof value.status === "string";
}

export interface LogEntry {
  id: string;
  ts: string;
  icon: IconName;
  iflowName: string;
  stage: string;
  status: string;
  isNew: boolean;
  incidentId: string;
  errorMessage: string;
  createdAt: string;
}

export type NodeId = "cpi" | "orchestrator" | "observer" | "rca" | "fixer" | "verifier";
export type GlowState = "blue" | "green" | "red" | "idle";

export interface NodeDef {
  id: NodeId;
  label: string;
  sub: string;
  abbr: string;
  cx: number;
}

export const CY = 68;
export const R = 30;
export const SVG_H = 178;
export const SVG_W = 900;

export const NODES: NodeDef[] = [
  { id: "cpi", label: "SAP CPI", sub: "Source", abbr: "CPI", cx: 75 },
  { id: "orchestrator", label: "Orchestrator", sub: "Dispatch", abbr: "ORC", cx: 225 },
  { id: "observer", label: "Observer", sub: "Detect", abbr: "OBS", cx: 375 },
  { id: "rca", label: "RCA Agent", sub: "Analyze", abbr: "RCA", cx: 525 },
  { id: "fixer", label: "Fixer Agent", sub: "Repair", abbr: "FIX", cx: 675 },
  { id: "verifier", label: "Verifier", sub: "Verify", abbr: "VER", cx: 825 },
];

export const STATUS_TO_NODE: Partial<Record<string, NodeId>> = {
  DETECTED: "orchestrator",
  CLASSIFIED: "observer",
  RCA_IN_PROGRESS: "observer",
  RCA_COMPLETE: "rca",
  FIX_IN_PROGRESS: "fixer",
  FIX_DEPLOYED: "fixer",
  AWAITING_APPROVAL: "fixer",
  FIX_VERIFIED: "verifier",
  HUMAN_INITIATED_FIX: "verifier",
  AUTO_FIXED: "verifier",
  RETRIED: "verifier",
  FIX_FAILED: "fixer",
  FIX_FAILED_UPDATE: "fixer",
  FIX_FAILED_DEPLOY: "fixer",
  FIX_FAILED_RUNTIME: "fixer",
  RCA_FAILED: "rca",
  TICKET_CREATED: "rca",
  PIPELINE_ERROR: "orchestrator",
};

export function particleColor(status: string): "blue" | "green" | "red" {
  const s = status.toUpperCase();
  if (s.includes("FAIL") || s === "REJECTED") return "red";
  if (["FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED"].includes(s)) return "green";
  return "blue";
}

export function statusToStage(status: string): string {
  const s = status.toUpperCase();
  if (["FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED"].includes(s)) return "verified";
  if (s.startsWith("FIX") || s === "AWAITING_APPROVAL" || s === "FIX_DEPLOYED") return "fix";
  if (["RCA_IN_PROGRESS", "RCA_COMPLETE", "RCA_FAILED", "TICKET_CREATED"].includes(s)) return "rca";
  if (s === "CLASSIFIED") return "classified";
  return "observed";
}

export const STATUS_ICON: Partial<Record<string, IconName>> = {
  DETECTED: "inbox",
  CLASSIFIED: "search",
  RCA_IN_PROGRESS: "rca",
  RCA_COMPLETE: "rca",
  FIX_IN_PROGRESS: "wrench",
  FIX_DEPLOYED: "wrench",
  AWAITING_APPROVAL: "user",
  FIX_VERIFIED: "check-circle",
  HUMAN_INITIATED_FIX: "check-circle",
  AUTO_FIXED: "check-circle",
  RETRIED: "loop",
  FIX_FAILED: "warning",
  FIX_FAILED_UPDATE: "warning",
  FIX_FAILED_DEPLOY: "warning",
  FIX_FAILED_RUNTIME: "warning",
  RCA_FAILED: "warning",
  TICKET_CREATED: "tickets",
  PIPELINE_ERROR: "warning",
};

export const STAGE_CFG: Record<string, { color: string; bg: string; border: string; label: string }> = {
  observed: { color: "#93c5fd", bg: "rgba(37,99,235,0.18)", border: "rgba(59,130,246,0.35)", label: "Observed" },
  classified: { color: "#fcd34d", bg: "rgba(217,119,6,0.18)", border: "rgba(245,158,11,0.35)", label: "Classified" },
  rca: { color: "#c4b5fd", bg: "rgba(124,58,237,0.18)", border: "rgba(167,139,250,0.35)", label: "RCA" },
  fix: { color: "#fca5a5", bg: "rgba(220,38,38,0.18)", border: "rgba(248,113,113,0.35)", label: "Fix" },
  verified: { color: "#86efac", bg: "rgba(22,163,74,0.18)", border: "rgba(74,222,128,0.35)", label: "Verified" },
};

export const GLOW_STROKE: Record<GlowState, string> = {
  blue: "#3b82f6",
  green: "#22c55e",
  red: "#ef4444",
  idle: "#374151",
};

export const GLOW_FILL: Record<GlowState, string> = {
  blue: "rgba(59,130,246,0.13)",
  green: "rgba(34,197,94,0.13)",
  red: "rgba(239,68,68,0.13)",
  idle: "rgba(15,23,42,0.6)",
};

export type NodeCounts = { blue: number; green: number; red: number; total: number };
export type NodeCountMap = Map<NodeId, NodeCounts>;

export function computeNodeCounts(incidents: Incident[]): NodeCountMap {
  const map: NodeCountMap = new Map();
  NODES.forEach((n) => map.set(n.id, { blue: 0, green: 0, red: 0, total: 0 }));
  for (const inc of incidents) {
    const nodeId = STATUS_TO_NODE[inc.status.toUpperCase()];
    if (!nodeId) continue;
    const entry = map.get(nodeId);
    if (!entry) continue;
    entry[particleColor(inc.status)]++;
    entry.total++;
  }
  return map;
}

export function nodeGlowState(c: NodeCounts): GlowState {
  if (c.red > 0) return "red";
  if (c.green > 0) return "green";
  if (c.blue > 0) return "blue";
  return "idle";
}
