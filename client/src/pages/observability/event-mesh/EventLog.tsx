import { useEffect, useRef, useState } from "react";
import SvgIcon from "../../../components/icons/SvgIcon";
import styles from "../EventMeshFlow.module.css";
import { STAGE_CFG, type LogEntry } from "./shared";

export default function EventLog({ entries, onClear }: { entries: LogEntry[]; onClear: () => void }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [filterText, setFilter] = useState("");
  const [expandedId, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
  }, [entries.length]);

  const filtered = filterText ? entries.filter((e) => e.iflowName.toLowerCase().includes(filterText.toLowerCase())) : entries;

  return (
    <div className={styles.logCard}>
      <div className={styles.logHeader}>
        <div className={styles.logHeaderLeft}>
          <span className={styles.logTitle}>Live Event Log</span>
          <span className={styles.liveChip}>Live</span>
        </div>
        <button className={styles.clearBtn} onClick={onClear}>Clear</button>
      </div>
      <div className={styles.logFilterBar}>
        <input type="text" className={styles.logFilterInput} placeholder="Filter by iFlow name…" value={filterText} onChange={(e) => setFilter(e.target.value)} />
      </div>
      <div className={styles.logScroll} ref={scrollRef}>
        {filtered.length === 0 ? (
          <div className={styles.logEmpty}>
            <span className={styles.logEmptyIcon}><SvgIcon name="event-mesh" size={24} /></span>
            <span className={styles.logEmptyText}>{entries.length === 0 ? "Waiting for events…" : "No entries match filter."}</span>
          </div>
        ) : (
          filtered.map((entry) => {
            const stageCfg = STAGE_CFG[entry.stage] ?? STAGE_CFG.observed;
            const ts = new Date(entry.ts);
            const timeStr = ts.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            const rawName = entry.iflowName;
            const isEmpty = !rawName || rawName === "—" || rawName.trim() === "";
            const truncated = !isEmpty && rawName.length > 30 ? `${rawName.slice(0, 30)}…` : rawName;
            const isExpanded = expandedId === entry.id;
            const s = entry.status.toUpperCase();
            const colorCls = isExpanded ? styles.logEntryActive : s.includes("FAIL") ? styles.logEntryFail : entry.stage === "verified" ? styles.logEntryVerified : entry.isNew ? styles.logEntryNew : "";
            return (
              <div key={entry.id}>
                <div className={`${styles.logEntry} ${colorCls}`} onClick={() => setExpanded(isExpanded ? null : entry.id)}>
                  <span className={styles.logTs}>{timeStr}</span>
                  <span className={styles.logIcon}><SvgIcon name={entry.icon} size={13} /></span>
                  {isEmpty ? <span className={styles.logIflowUnknown}>unknown</span> : <span className={styles.logIflow} title={rawName}>{truncated}</span>}
                  {entry.isNew ? <span className={styles.logNewBadge}>NEW</span> : <span />}
                  <span className={styles.stageBadge} style={{ color: stageCfg.color, background: stageCfg.bg, borderColor: stageCfg.border }}>{stageCfg.label}</span>
                </div>
                {isExpanded && (
                  <div className={styles.logEntryDetail}>
                    <div><span className={styles.logDetailLabel}>iFlow</span>{isEmpty ? <em>unknown</em> : rawName}</div>
                    <div><span className={styles.logDetailLabel}>Status</span>{entry.status}</div>
                    <div className={styles.logDetailFull}><span className={styles.logDetailLabel}>Error</span>{entry.errorMessage || "—"}</div>
                    <div><span className={styles.logDetailLabel}>Logged at</span>{ts.toLocaleString()}</div>
                    {entry.createdAt && <div><span className={styles.logDetailLabel}>Created</span>{entry.createdAt}</div>}
                    {entry.incidentId && <div><span className={styles.logDetailLabel}>Incident ID</span>{entry.incidentId}</div>}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
