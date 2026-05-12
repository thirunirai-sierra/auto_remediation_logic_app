/**
 * @fileoverview Event Mesh visualization shared layer: incident typing, pipeline node layout,
 * status→node mapping, log entry shape, particle colors, and aggregation helpers for `PipelineDiagram` / `EventLog`.
 */
import type { IconName } from "../../../components/icons/SvgIcon";

/** Incident record as returned from AEM-style APIs (extra fields allowed). */
export interface Incident {
  [key: string]: unknown;
  incident_id: string;
  iflow_id?: string;
  iflow_name?: string;
  status: string;
}

/**
 * Type guard so filtered incident arrays are safe for diagram code.
 * @param {Record<string, unknown>} value - Raw object from API list.
 * @returns {value is Incident} True when `incident_id` and `status` are strings.
 */
export function isIncident(value: Record<string, unknown>): value is Incident {
  return typeof value.incident_id === "string" && typeof value.status === "string";
}

/** One row in the synthetic live event log built in `EventMeshFlow`. */
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

/** Single node in the horizontal SVG pipeline. */
export interface NodeDef {
  id: NodeId;
  label: string;
  sub: string;
  abbr: string;
  cx: number;
}

/** Vertical center of node circles in SVG coordinates. */
export const CY = 68;
/** Base radius of each pipeline node circle. */
export const R = 30;
/** Total SVG canvas height. */
export const SVG_H = 178;
/** Total SVG canvas width. */
export const SVG_W = 900;

/** Ordered pipeline stages left-to-right (CPI through Verifier). */
export const NODES: NodeDef[] = [
  { id: "cpi", label: "SAP CPI", sub: "Source", abbr: "CPI", cx: 75 },
  { id: "orchestrator", label: "Orchestrator", sub: "Dispatch", abbr: "ORC", cx: 225 },
  { id: "observer", label: "Observer", sub: "Detect", abbr: "OBS", cx: 375 },
  { id: "rca", label: "RCA Agent", sub: "Analyze", abbr: "RCA", cx: 525 },
  { id: "fixer", label: "Fixer Agent", sub: "Repair", abbr: "FIX", cx: 675 },
  { id: "verifier", label: "Verifier", sub: "Verify", abbr: "VER", cx: 825 },
];

/** Maps uppercase remediation status to which diagram node should accumulate counts. */
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

/**
 * Maps a remediation status string to particle color bucket under a node.
 * @param {string} status - Raw status from incident (any casing).
 * @returns {"blue" | "green" | "red"} Color key for count aggregation.
 */
export function particleColor(status: string): "blue" | "green" | "red" {
  const s = status.toUpperCase();
  if (s.includes("FAIL") || s === "REJECTED") return "red";
  if (["FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED"].includes(s)) return "green";
  return "blue";
}

/**
 * Collapses fine-grained statuses into log stage buckets for badges.
 * @param {string} status - Incident status string.
 * @returns {string} One of `verified` | `fix` | `rca` | `classified` | `observed`.
 */
export function statusToStage(status: string): string {
  const s = status.toUpperCase();
  if (["FIX_VERIFIED", "HUMAN_INITIATED_FIX", "AUTO_FIXED", "RETRIED"].includes(s)) return "verified";
  if (s.startsWith("FIX") || s === "AWAITING_APPROVAL" || s === "FIX_DEPLOYED") return "fix";
  if (["RCA_IN_PROGRESS", "RCA_COMPLETE", "RCA_FAILED", "TICKET_CREATED"].includes(s)) return "rca";
  if (s === "CLASSIFIED") return "classified";
  return "observed";
}

/** Icon name per status for log rows (falls back to `inbox` / `refresh` in caller if missing). */
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

/** Badge colors/labels for each `statusToStage` bucket in `EventLog`. */
export const STAGE_CFG: Record<string, { color: string; bg: string; border: string; label: string }> = {
  observed: { color: "#93c5fd", bg: "rgba(37,99,235,0.18)", border: "rgba(59,130,246,0.35)", label: "Observed" },
  classified: { color: "#fcd34d", bg: "rgba(217,119,6,0.18)", border: "rgba(245,158,11,0.35)", label: "Classified" },
  rca: { color: "#c4b5fd", bg: "rgba(124,58,237,0.18)", border: "rgba(167,139,250,0.35)", label: "RCA" },
  fix: { color: "#fca5a5", bg: "rgba(220,38,38,0.18)", border: "rgba(248,113,113,0.35)", label: "Fix" },
  verified: { color: "#86efac", bg: "rgba(22,163,74,0.18)", border: "rgba(74,222,128,0.35)", label: "Verified" },
};

/** SVG stroke color per glow state. */
export const GLOW_STROKE: Record<GlowState, string> = {
  blue: "#3b82f6",
  green: "#22c55e",
  red: "#ef4444",
  idle: "#374151",
};

/** SVG fill color per glow state. */
export const GLOW_FILL: Record<GlowState, string> = {
  blue: "rgba(59,130,246,0.13)",
  green: "rgba(34,197,94,0.13)",
  red: "rgba(239,68,68,0.13)",
  idle: "rgba(15,23,42,0.6)",
};

export type NodeCounts = { blue: number; green: number; red: number; total: number };
export type NodeCountMap = Map<NodeId, NodeCounts>;

/**
 * Counts incidents per pipeline node, split by particle color for `NodeParticles`.
 * @param {Incident[]} incidents - Current incident list from AEM query.
 * @returns {NodeCountMap} Map of node id → `{ blue, green, red, total }`.
 */
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

/**
 * Picks dominant glow color for a node from its aggregated particle counts.
 * @param {NodeCounts} c - Aggregated counts for one node.
 * @returns {GlowState} Priority: red → green → blue → idle.
 */
export function nodeGlowState(c: NodeCounts): GlowState {
  if (c.red > 0) return "red";
  if (c.green > 0) return "green";
  if (c.blue > 0) return "blue";
  return "idle";
}
