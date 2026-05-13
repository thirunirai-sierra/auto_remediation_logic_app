/**
 * @fileoverview SVG Event Mesh pipeline: CPI through verifier nodes, animated connectors, glow from incident counts.
 */
import SvgIcon from "../../../components/icons/SvgIcon";
import styles from "../EventMeshFlow.module.css";
import {
  computeNodeCounts,
  CY,
  GLOW_FILL,
  GLOW_STROKE,
  NODES,
  R,
  SVG_H,
  SVG_W,
  type Incident,
  type NodeCounts,
  type NodeId,
  type GlowState,
  nodeGlowState,
} from "./shared";

interface PipelineDiagramProps {
  incidents: Incident[];
  aemEnabled: boolean;
  messagesRetrieved: number;
}

/**
 * Renders up to five colored dots under a node representing blue/green/red incident mix at that stage.
 * @param {object} props - Component props.
 * @param {number} props.cx - SVG x-center for this node's particle row.
 * @param {NodeCounts} props.counts - Aggregated particle counts from `computeNodeCounts`.
 * @returns {JSX.Element | null} SVG `<g>` of circles or null when no particles.
 */
function NodeParticles({ cx, counts }: { cx: number; counts: NodeCounts }) {
  const slots: string[] = [
    ...Array<string>(Math.min(counts.blue, 5)).fill("#3b82f6"),
    ...Array<string>(Math.min(counts.green, Math.max(0, 5 - counts.blue))).fill("#22c55e"),
    ...Array<string>(Math.min(counts.red, Math.max(0, 5 - counts.blue - counts.green))).fill("#ef4444"),
  ].slice(0, 5);
  const extra = counts.total - slots.length;
  if (slots.length === 0) return null;

  const spacing = 10;
  const startX = cx - ((slots.length - 1) * spacing) / 2;
  const dotY = CY + R + 48;

  return (
    <g>
      {slots.map((fill, i) => (
        <circle key={i} cx={startX + i * spacing} cy={dotY} r={4} fill={fill} className={styles.particleDot} style={{ animationDelay: `${i * 0.25}s` }} />
      ))}
      {extra > 0 && <text x={startX + (slots.length - 1) * spacing + 8} y={dotY + 4} fontSize={9} fill="#6b7280">+{extra}</text>}
    </g>
  );
}

/**
 * Full pipeline card: connection status banner, SVG nodes with badges, and per-node particle rows.
 * @param {PipelineDiagramProps} props - Incident list and AEM connectivity flags from `EventMeshFlow`.
 * @param {Incident[]} props.incidents - Drives per-node counts and glow.
 * @param {boolean} props.aemEnabled - When false, shows disconnected banner and idle CPI styling.
 * @param {number} props.messagesRetrieved - Webhook/message counter for CPI badge when connected.
 * @returns {JSX.Element} Diagram card with optional disconnected warning.
 */
export default function PipelineDiagram({ incidents, aemEnabled, messagesRetrieved }: PipelineDiagramProps) {
  const counts = computeNodeCounts(incidents);

  const getGlow = (nodeId: NodeId): GlowState => {
    if (nodeId === "cpi") return aemEnabled && messagesRetrieved > 0 ? "blue" : "idle";
    return nodeGlowState(counts.get(nodeId) ?? { blue: 0, green: 0, red: 0, total: 0 });
  };
  const connActive = (fromIdx: number): boolean => getGlow(NODES[fromIdx + 1].id) !== "idle";

  return (
    <div className={styles.diagramCard}>
      <div className={styles.diagramHeader}>
        <span className={styles.diagramTitle}>Event Mesh Pipeline Flow</span>
        <span className={styles.aemStatus} data-enabled={String(aemEnabled)}>
          <span className={styles.aemDot} />
          {aemEnabled ? "Event Mesh Connected" : "Event Mesh Disconnected"}
        </span>
      </div>
      {!aemEnabled && (
        <div className={styles.disconnectedBanner}>
          <SvgIcon name="warning" size={14} style={{ verticalAlign: "middle", marginRight: "0.35rem" }} />
          Event Mesh is disconnected — the pipeline is not receiving events
        </div>
      )}
      <div className={styles.svgWrapper}>
        <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} preserveAspectRatio="xMidYMid meet" style={{ width: "100%", display: "block" }} aria-label="SAP Event Mesh pipeline diagram">
          <defs>
            <marker id="em-arrow-idle" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,1 L9,5 L0,9z" fill="#374151" /></marker>
            <marker id="em-arrow-active" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,1 L9,5 L0,9z" fill="#2563eb" /></marker>
            <radialGradient id="em-rglow-blue" cx="50%" cy="50%" r="50%"><stop offset="0%" stopColor="rgba(59,130,246,0.28)" /><stop offset="100%" stopColor="rgba(59,130,246,0)" /></radialGradient>
            <radialGradient id="em-rglow-green" cx="50%" cy="50%" r="50%"><stop offset="0%" stopColor="rgba(34,197,94,0.28)" /><stop offset="100%" stopColor="rgba(34,197,94,0)" /></radialGradient>
            <radialGradient id="em-rglow-red" cx="50%" cy="50%" r="50%"><stop offset="0%" stopColor="rgba(239,68,68,0.28)" /><stop offset="100%" stopColor="rgba(239,68,68,0)" /></radialGradient>
          </defs>

          {NODES.slice(0, -1).map((node, i) => (
            <line
              key={`c${i}`}
              x1={node.cx + R + 4}
              y1={CY}
              x2={NODES[i + 1].cx - R - 4}
              y2={CY}
              stroke={connActive(i) ? "#2563eb" : "#2d3748"}
              strokeWidth={connActive(i) ? 2.5 : 1.5}
              className={connActive(i) ? styles.connectorActive : styles.connector}
              markerEnd={connActive(i) ? "url(#em-arrow-active)" : "url(#em-arrow-idle)"}
            />
          ))}

          {NODES.map((node) => {
            const glow = getGlow(node.id);
            const nodeCounts = node.id === "cpi" ? { blue: aemEnabled ? 1 : 0, green: 0, red: 0, total: aemEnabled ? 1 : 0 } : (counts.get(node.id) ?? { blue: 0, green: 0, red: 0, total: 0 });
            const total = node.id === "cpi" ? (aemEnabled && messagesRetrieved > 0 ? messagesRetrieved : 0) : nodeCounts.total;
            return (
              <g key={node.id}>
                {glow !== "idle" && <circle cx={node.cx} cy={CY} r={R + 42} fill={`url(#em-rglow-${glow})`} />}
                {glow !== "idle" && <circle cx={node.cx} cy={CY} r={R + 9} fill="none" stroke={GLOW_STROKE[glow]} strokeWidth={1} opacity={0.25} />}
                <circle cx={node.cx} cy={CY} r={R} fill={GLOW_FILL[glow]} stroke={GLOW_STROKE[glow]} strokeWidth={glow !== "idle" ? 2 : 1.5} />
                <text x={node.cx} y={CY + 5} textAnchor="middle" fontSize={11} fontWeight={700} fill="#fff" letterSpacing={1}>{node.abbr}</text>
                {total > 0 && (
                  <g>
                    <circle cx={node.cx + R - 4} cy={CY - R + 4} r={9} fill={GLOW_STROKE[glow]} />
                    <text x={node.cx + R - 4} y={CY - R + 8} textAnchor="middle" fontSize={8} fontWeight={700} fill="#fff">{total > 99 ? "99+" : String(total)}</text>
                  </g>
                )}
                <text x={node.cx} y={CY + R + 16} textAnchor="middle" fontSize={11} fontWeight={600} fill="#1e293b">{node.label}</text>
                <text x={node.cx} y={CY + R + 30} textAnchor="middle" fontSize={9} fill="#1e293b">{node.sub}</text>
              </g>
            );
          })}

          {NODES.map((node) => {
            const nodeCounts = node.id === "cpi" ? { blue: 0, green: 0, red: 0, total: 0 } : (counts.get(node.id) ?? { blue: 0, green: 0, red: 0, total: 0 });
            return <NodeParticles key={`p-${node.id}`} cx={node.cx} counts={nodeCounts} />;
          })}
        </svg>
      </div>
    </div>
  );
}
