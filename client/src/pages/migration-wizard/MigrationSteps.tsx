import styles from "./migration-wizard.module.css";

type WizardStep = "upload" | "config" | "preview";

const STEPS: { key: WizardStep; label: string }[] = [
  { key: "upload", label: "Upload Files" },
  { key: "config", label: "Configuration" },
  { key: "preview", label: "Preview" },
];

export default function MigrationSteps({ step }: { step: WizardStep }) {
  const currentIdx = STEPS.findIndex((s) => s.key === step);
  return (
    <div className={styles.stepIndicator}>
      {STEPS.map((s, i) => {
        const isDone = i < currentIdx;
        const isActive = i === currentIdx;
        return (
          <div key={s.key} className={styles.stepItem}>
            <div className={styles.stepContent}>
              <div className={`${styles.stepNum} ${isDone ? styles.stepNumDone : isActive ? styles.stepNumActive : styles.stepNumPending}`}>
                {isDone ? "✓" : i + 1}
              </div>
              <span className={`${styles.stepLabel} ${isActive ? styles.stepLabelActive : isDone ? styles.stepLabelDone : ""}`}>
                {s.label}
              </span>
            </div>
            {i < STEPS.length - 1 && <div className={`${styles.stepLine} ${isDone ? styles.stepLineDone : ""}`} />}
          </div>
        );
      })}
    </div>
  );
}

