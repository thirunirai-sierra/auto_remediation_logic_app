/**
 * @fileoverview Drag-and-drop / browse tile used twice on the migration wizard (source code vs error screenshot).
 */
import { useState } from "react";
import SvgIcon from "../../components/icons/SvgIcon";
import type { FileSource } from "../../types";
import styles from "./migration-wizard.module.css";

interface DropZoneProps {
  label: string;
  source: FileSource;
  onDrop: (e: React.DragEvent<HTMLDivElement>) => void;
  onBrowse: () => void;
}

/**
 * Renders one drop target: highlights on drag-over, forwards drop and browse to parent.
 * @param {DropZoneProps} props - Label, file source kind, and parent handlers.
 * @param {string} props.label - Visible title above the zone.
 * @param {FileSource} props.source - `CODEFILE` selects folder icon; `ERROR` selects image icon.
 * @param {(e: React.DragEvent<HTMLDivElement>) => void} props.onDrop - Called after `preventDefault` cleanup on drop.
 * @param {() => void} props.onBrowse - Opens hidden file input in parent.
 * @returns {JSX.Element} Styled drop zone with browse button.
 */
export default function MigrationDropZone({ label, source, onDrop, onBrowse }: DropZoneProps) {
  const [hovering, setHovering] = useState(false);
  const isCode = source === "CODEFILE";

  return (
    <div
      className={`${styles.dropZone} ${hovering ? styles.dropZoneHover : ""}`}
      onDragOver={(e) => { e.preventDefault(); setHovering(true); }}
      onDragLeave={() => setHovering(false)}
      onDrop={(e) => { setHovering(false); onDrop(e); }}
    >
      <span className={styles.dropIcon}><SvgIcon name={isCode ? "folder" : "image"} size={28} /></span>
      <span className={styles.dropLabel}>{label}</span>
      <span className={`${styles.dropType} ${isCode ? styles.dropTypeCode : styles.dropTypeError}`}>
        {isCode ? "Code File" : "Error / Screenshot"}
      </span>
      <span className={styles.dropHint}>Drag & drop files here, or</span>
      <button className={styles.browseBtn} onClick={onBrowse} type="button">Browse Files</button>
    </div>
  );
}
