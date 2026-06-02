import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  fetchRuntimeSettings,
  updateRuntimeSetting,
  resetRuntimeSetting,
  fetchPolicies,
  updatePolicy,
  resetPolicy,
  fetchAemStatus,
  fetchPipelineStatus,
  type RuntimeSetting,
  type RemediationPolicy,
} from "../../services/api.ts";
import styles from "./settings.module.css";

// ── Themes ─────────────────────────────────────────────────────────────────────
const THEMES = [
  { id: "plain",       label: "Plain",            sidebar: "#f8fafc", sidebarText: "#475569", accent: "#3b82f6", nav: "#eff6ff",   navText: "#2563eb",  topbar: "#ffffff", bar: "#e2e8f0" },
  { id: "sap-horizon", label: "SAP Horizon",      sidebar: "#354a5e", sidebarText: "#d1dce6", accent: "#0070f2", nav: "#0070f21a", navText: "#6eb5f5",  topbar: "#354a5e", bar: "#1b3145" },
  { id: "azure-blue",  label: "Azure Blue",       sidebar: "#0078d4", sidebarText: "#e8f3fd", accent: "#50abf1", nav: "#ffffff1a", navText: "#ffffff",  topbar: "#0078d4", bar: "#005a9e" },
  { id: "aurora",      label: "Aurora (default)", sidebar: "#1e3a5f", sidebarText: "#cbd5e1", accent: "#3b82f6", nav: "#3b82f633", navText: "#93c5fd",  topbar: "#1e3a5f", bar: "#1a3352" },
  { id: "fresh",       label: "Fresh",            sidebar: "#064e3b", sidebarText: "#a7f3d0", accent: "#10b981", nav: "#10b9811a", navText: "#6ee7b7",  topbar: "#064e3b", bar: "#065f46" },
  { id: "prism",       label: "Prism",            sidebar: "#4c1d95", sidebarText: "#ddd6fe", accent: "#8b5cf6", nav: "#8b5cf61a", navText: "#c4b5fd",  topbar: "#4c1d95", bar: "#5b21b6" },
  { id: "mono",        label: "Mono",             sidebar: "#1f2937", sidebarText: "#9ca3af", accent: "#6b7280", nav: "#6b72801a", navText: "#d1d5db",  topbar: "#1f2937", bar: "#111827" },
  { id: "brutal",      label: "Brutal",           sidebar: "#18181b", sidebarText: "#fbbf24", accent: "#fbbf24", nav: "#fbbf241a", navText: "#fde68a",  topbar: "#09090b", bar: "#000000" },
  { id: "dark",        label: "Dark",             sidebar: "#0f172a", sidebarText: "#94a3b8", accent: "#38bdf8", nav: "#38bdf81a", navText: "#7dd3fc",  topbar: "#020617", bar: "#020617" },
  { id: "terminal",    label: "Terminal",         sidebar: "#0a0a0a", sidebarText: "#22c55e", accent: "#22c55e", nav: "#22c55e1a", navText: "#86efac",  topbar: "#000000", bar: "#000000" },
  { id: "nord",        label: "Nord",             sidebar: "#2e3440", sidebarText: "#d8dee9", accent: "#88c0d0", nav: "#88c0d01a", navText: "#8fbcbb",  topbar: "#2e3440", bar: "#3b4252" },
  { id: "copper",      label: "Copper",           sidebar: "#1c0a00", sidebarText: "#d97706", accent: "#b45309", nav: "#b453091a", navText: "#fbbf24",  topbar: "#0f0500", bar: "#1c0a00" },
];

function applyThemeCss(t: typeof THEMES[number]) {
  const r = document.documentElement;
  r.style.setProperty("--orbit-sidebar-bg",        t.sidebar);
  r.style.setProperty("--orbit-sidebar-text",       t.sidebarText);
  r.style.setProperty("--orbit-sidebar-active",     t.nav);
  r.style.setProperty("--orbit-sidebar-hover",      t.nav);
  r.style.setProperty("--orbit-blue",               t.accent);
  r.style.setProperty("--orbit-blue-light",         t.accent);
  r.style.setProperty("--orbit-topbar-bg",          t.topbar);
  r.style.setProperty("--orbit-nav-active-text",    t.navText);
}

const FONT_SIZES = ["S", "M", "L"] as const;
type FontSize = typeof FONT_SIZES[number];

const SETTING_CATEGORIES = ["All", "Fix Behaviour", "Throughput", "Timing", "Remediation Policies"] as const;

const POLICY_ACTIONS: { value: string; label: string; color: string; bg: string }[] = [
  { value: "AUTO_FIX",          label: "Auto-Fix",          color: "#15803d", bg: "#f0fdf4" },
  { value: "RETRY",             label: "Retry",             color: "#1e40af", bg: "#eff6ff" },
  { value: "TICKET_CREATED",    label: "Create Ticket",     color: "#b91c1c", bg: "#fff5f5" },
  { value: "AWAITING_APPROVAL", label: "Awaiting Approval", color: "#92400e", bg: "#fffbeb" },
];

// ── Sub-components ─────────────────────────────────────────────────────────────

function ImpactBadge({ impact }: { impact: string }) {
  const map: Record<string, string> = { HIGH: styles.impactHigh, MEDIUM: styles.impactMedium, LOW: styles.impactLow };
  return <span className={`${styles.impactBadge} ${map[impact] ?? ""}`}>{impact} IMPACT</span>;
}

function SystemHealthRow({ label, value, ok }: { label: string; value: string; ok: boolean }) {
  return (
    <div className={styles.healthRow}>
      <span className={styles.healthLabel}>{label}</span>
      <span className={`${styles.healthDot} ${ok ? styles.healthDotOk : styles.healthDotWarn}`} />
      <span className={`${styles.healthValue} ${ok ? styles.healthOk : styles.healthWarn}`}>{value}</span>
    </div>
  );
}

function EditModal({ setting, onClose, onSave }: {
  setting: RuntimeSetting; onClose: () => void; onSave: (v: unknown) => Promise<void>;
}) {
  const [raw, setRaw] = useState(String(setting.value));
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState("");

  async function handleSave() {
    setSaving(true); setErr("");
    try {
      let parsed: unknown = raw;
      if (setting.type === "float") parsed = parseFloat(raw);
      else if (setting.type === "int") parsed = parseInt(raw, 10);
      else if (setting.type === "bool") parsed = raw === "true";
      if (setting.type !== "bool" && isNaN(parsed as number)) { setErr("Must be a number"); setSaving(false); return; }
      await onSave(parsed);
      onClose();
    } catch (e) { setErr(e instanceof Error ? e.message : "Save failed"); setSaving(false); }
  }

  return (
    <div className={styles.modalOverlay} onClick={onClose}>
      <div className={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div className={styles.modalHeader}>
          <span className={styles.modalTitle}>{setting.label}</span>
          <button className={styles.modalClose} onClick={onClose}>✕</button>
        </div>
        <p className={styles.modalDesc}>{setting.description}</p>
        <div className={styles.modalField}>
          {setting.type === "bool" ? (
            <select className={styles.modalInput} value={raw} onChange={(e) => setRaw(e.target.value)}>
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          ) : (
            <input className={styles.modalInput} type="number"
              step={setting.type === "float" ? "0.05" : "1"}
              min={setting.min} max={setting.max}
              value={raw} onChange={(e) => setRaw(e.target.value)} />
          )}
          {setting.unit && <span className={styles.modalUnit}>{setting.unit}</span>}
        </div>
        <div className={styles.modalMeta}>
          <span>Default: <strong>{String(setting.default)}</strong></span>
          {setting.min != null && <span>Range: {setting.min} – {setting.max}</span>}
        </div>
        <div className={styles.modalTakesEffect}>⚡ Takes effect: {setting.takes_effect}</div>
        {err && <div className={styles.modalErr}>{err}</div>}
        <div className={styles.modalActions}>
          <button className={styles.btnSave} onClick={handleSave} disabled={saving}>{saving ? "Saving…" : "Save"}</button>
          <button className={styles.btnCancel} onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────────────

export default function Settings() {
  const qc = useQueryClient();
  const [activeTheme, setActiveTheme] = useState(() => localStorage.getItem("orbit-theme") ?? "aurora");
  const [fontSize, setFontSize] = useState<FontSize>(() => (localStorage.getItem("orbit-font") as FontSize) ?? "M");
  const [settingCategory, setSettingCategory] = useState<string>("All");
  const [healthOpen, setHealthOpen] = useState(false);
  const [editSetting, setEditSetting] = useState<RuntimeSetting | null>(null);
  const [savingPolicy, setSavingPolicy] = useState<string | null>(null);

  const { data: settingsData, isLoading: settingsLoading } = useQuery({
    queryKey: ["runtime-settings"],
    queryFn: fetchRuntimeSettings,
    refetchInterval: 60_000,
    retry: 1,
  });

  const { data: policiesData, isLoading: policiesLoading } = useQuery({
    queryKey: ["remediation-policies"],
    queryFn: fetchPolicies,
    refetchInterval: 60_000,
    retry: 1,
  });

  const { data: aem } = useQuery({ queryKey: ["aem-status"], queryFn: fetchAemStatus, refetchInterval: 30_000 });
  const { data: pipeline } = useQuery({ queryKey: ["pipeline-status-settings"], queryFn: fetchPipelineStatus, refetchInterval: 30_000 });

  const allSettings: RuntimeSetting[] = settingsData?.settings ?? [];
  // Exclude Remediation Policies from the settings list (they live in their own section)
  const filteredSettings = allSettings.filter(s => s.category !== "Remediation Policies" && (settingCategory === "All" || s.category === settingCategory));

  const policies: RemediationPolicy[] = policiesData?.policies ?? [];

  function applyTheme(id: string) {
    setActiveTheme(id);
    localStorage.setItem("orbit-theme", id);
    const t = THEMES.find(x => x.id === id);
    if (t) applyThemeCss(t);
  }

  // Apply saved theme on mount
  useState(() => {
    const saved = localStorage.getItem("orbit-theme") ?? "aurora";
    const t = THEMES.find(x => x.id === saved);
    if (t) applyThemeCss(t);
  });
  function applyFont(f: FontSize) {
    setFontSize(f); localStorage.setItem("orbit-font", f);
    document.documentElement.style.fontSize = f === "S" ? "13px" : f === "L" ? "16px" : "14px";
  }

  async function handleSaveSetting(key: string, value: unknown) {
    await updateRuntimeSetting(key, value);
    qc.invalidateQueries({ queryKey: ["runtime-settings"] });
  }

  async function handleResetSetting(key: string) {
    await resetRuntimeSetting(key);
    qc.invalidateQueries({ queryKey: ["runtime-settings"] });
  }

  async function handlePolicyChange(error_type: string, action: string) {
    setSavingPolicy(error_type);
    try {
      await updatePolicy(error_type, action);
      qc.invalidateQueries({ queryKey: ["remediation-policies"] });
    } finally {
      setSavingPolicy(null);
    }
  }

  async function handleResetPolicy(error_type: string) {
    setSavingPolicy(error_type);
    try {
      await resetPolicy(error_type);
      qc.invalidateQueries({ queryKey: ["remediation-policies"] });
    } finally {
      setSavingPolicy(null);
    }
  }

  const pipelineRunning = pipeline?.pipeline_running ?? false;
  const eventMeshActive = aem?.event_mesh_enabled ?? false;

  return (
    <div className={styles.page}>
      {editSetting && (
        <EditModal setting={editSetting} onClose={() => setEditSetting(null)} onSave={(v) => handleSaveSetting(editSetting.key, v)} />
      )}

      {/* ── Page header ── */}
      <div className={styles.pageHeaderRow}>
        <h2 className={styles.pageTitle}><span className={styles.pageTitleIcon}>⚙</span> Runtime Settings</h2>
        <p className={styles.pageSubtitle}>Changes apply immediately — no restart required. All overrides are held in memory.</p>
        <div className={styles.impactLegend}>
          <span className={`${styles.impactBadge} ${styles.impactHigh}`}>HIGH IMPACT</span>
          <span className={`${styles.impactBadge} ${styles.impactMedium}`}>MEDIUM IMPACT</span>
          <span className={`${styles.impactBadge} ${styles.impactLow}`}>LOW IMPACT</span>
        </div>
      </div>

      <div className={styles.infoBar}>
        <span className={styles.infoBarIcon}>⚠</span>
        <span>
          <strong>When do changes take effect?</strong> Every setting is saved in memory and applied to the{" "}
          <em>next</em> incident, cycle, or tool call — they do not interrupt work already in progress.
          The only exception is <code>Enable Autonomous Fixing</code>, which takes effect instantly across all agents.
          Each setting row shows its specific timing below the description.
        </span>
      </div>

      {/* ── Appearance ── */}
      <div className={styles.section}>
        <h3 className={styles.sectionTitle}>Appearance</h3>
        <p className={styles.sectionDesc}>Choose a colour theme. Applied instantly and remembered across sessions.</p>
        <div className={styles.themeGrid}>
          {THEMES.map((t) => (
            <button key={t.id} className={`${styles.themeCard} ${activeTheme === t.id ? styles.themeCardActive : ""}`} onClick={() => applyTheme(t.id)}>
              <div className={styles.themePreview}>
                <div className={styles.themePreviewSidebar} style={{ background: t.sidebar }} />
                <div className={styles.themePreviewContent}>
                  <div className={styles.themePreviewBar} style={{ background: t.accent }} />
                  <div className={styles.themePreviewBar2} style={{ background: t.bar, opacity: 0.4 }} />
                </div>
              </div>
              {activeTheme === t.id && <span className={styles.themeCheck}>✓</span>}
              <span className={styles.themeLabel}>{t.label}</span>
            </button>
          ))}
        </div>
        <div className={styles.fontRow}>
          <span className={styles.fontLabel}>Font Size</span>
          {FONT_SIZES.map((f) => (
            <button key={f} className={`${styles.fontBtn} ${fontSize === f ? styles.fontBtnActive : ""}`} onClick={() => applyFont(f)}>{f}</button>
          ))}
        </div>
      </div>

      {/* ── System Health ── */}
      <div className={styles.section}>
        <button className={styles.healthToggle} onClick={() => setHealthOpen((o) => !o)}>
          <span className={styles.healthToggleIcon}>⊞</span>
          <span className={styles.sectionTitle} style={{ margin: 0 }}>System Health</span>
          <span className={styles.healthChevron}>{healthOpen ? "▲" : "▶"}</span>
        </button>
        {healthOpen && (
          <div className={styles.healthGrid}>
            <SystemHealthRow label="Pipeline" value={pipelineRunning ? "Running" : "Stopped"} ok={pipelineRunning} />
            <SystemHealthRow label="Event Mesh" value={eventMeshActive ? "Connected" : "Inactive"} ok={eventMeshActive} />
            {(["observer", "classifier", "rca", "fixer", "verifier"] as const).map((agent) => (
              <SystemHealthRow key={agent} label={agent.charAt(0).toUpperCase() + agent.slice(1)}
                value={(pipeline?.agents?.[agent] as string) ?? "unknown"}
                ok={(pipeline?.agents?.[agent] as string) === "running"} />
            ))}
            {aem && <>
              <SystemHealthRow label="Queue Depth" value={String(aem.queue_depth ?? "—")} ok={(aem.queue_depth ?? 0) < 100} />
              <SystemHealthRow label="Messages Retrieved" value={String(aem.messages_retrieved ?? "—")} ok={true} />
            </>}
          </div>
        )}
      </div>

      {/* ── Runtime Settings ── */}
      <div className={styles.section}>
        <h3 className={styles.sectionTitle}>Pipeline Settings</h3>
        <div className={styles.categoryTabRow}>
          {SETTING_CATEGORIES.map((c) => (
            <button key={c} className={`${styles.categoryTab} ${settingCategory === c ? styles.categoryTabActive : ""}`} onClick={() => setSettingCategory(c)}>{c}</button>
          ))}
        </div>
        {settingCategory === "Remediation Policies" ? (
          policiesLoading ? <div className={styles.loadingRow}>Loading policies…</div>
          : policies.length === 0 ? <div className={styles.loadingRow}>Connect the backend to load policies.</div>
          : (
            <div className={styles.policyGrid}>
              {policies.map((p) => {
                const am = POLICY_ACTIONS.find(a => a.value === p.action) ?? POLICY_ACTIONS[0];
                return (
                  <div key={p.error_type} className={`${styles.policyCard} ${p.overridden ? styles.policyCardOverridden : ""}`}>
                    <div className={styles.policyCardHeader}>
                      <div>
                        <div className={styles.policyErrorType}>{p.error_type}</div>
                        <div className={styles.policyDesc}>{p.description}</div>
                      </div>
                      {p.overridden && <button className={styles.policyResetBtn} onClick={() => handleResetPolicy(p.error_type)} disabled={savingPolicy === p.error_type}>↺</button>}
                    </div>
                    <div className={styles.policyCardFooter}>
                      <div className={styles.policySelectWrapper} style={{ borderColor: am.color, background: am.bg }}>
                        <span className={styles.policySelectDot} style={{ background: am.color }} />
                        <select className={styles.policySelect} style={{ color: am.color }} value={p.action} disabled={savingPolicy === p.error_type} onChange={(e) => handlePolicyChange(p.error_type, e.target.value)}>
                          {POLICY_ACTIONS.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
                        </select>
                      </div>
                      {!p.overridden ? <span className={styles.policyDefault}>default</span> : <span className={styles.policyCustom}>custom</span>}
                    </div>
                  </div>
                );
              })}
            </div>
          )
        ) : settingsLoading ? (
          <div className={styles.loadingRow}>Loading settings…</div>
        ) : filteredSettings.length === 0 ? (
          <div className={styles.loadingRow}>No settings in this category.</div>
        ) : (
          <div className={styles.settingsList}>
            {filteredSettings.map((s) => (
              <div key={s.key} className={styles.settingRow}>
                <div className={styles.settingLeft}>
                  <div className={styles.settingLabelRow}>
                    <span className={styles.settingLabel}>{s.label}</span>
                    <ImpactBadge impact={s.impact} />
                  </div>
                  <div className={styles.settingKey}>{s.key}</div>
                  <p className={styles.settingDesc}>{s.description}</p>
                  {s.default != null && (
                    <div className={styles.settingDefault}>Default: <strong>{String(s.default)}</strong></div>
                  )}
                  <div className={styles.settingTakesEffect}>
                    <span className={styles.takesEffectIcon}>⚡</span>
                    <span>Takes effect: <em>{s.takes_effect}</em></span>
                  </div>
                </div>
                <div className={styles.settingRight}>
                  <span className={`${styles.settingValue} ${s.overridden ? styles.settingValueOverridden : ""}`}>
                    {String(s.value)}{s.unit ? ` ${s.unit}` : ""}
                  </span>
                  <div className={styles.settingActions}>
                    <button className={styles.btnEdit} onClick={() => setEditSetting(s)}>✏ Edit</button>
                    {s.overridden && <button className={styles.btnReset} onClick={() => handleResetSetting(s.key)}>↺ Reset</button>}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── Remediation Policies ── */}
      <div className={styles.section} style={{ display: settingCategory === "Remediation Policies" ? "none" : undefined }}>
        <h3 className={styles.sectionTitle}>Remediation Policies</h3>
        <p className={styles.sectionDesc}>
          Define how the pipeline responds to each error type. Changes take effect immediately on the next detected incident.
        </p>

        <div className={styles.policyLegend}>
          {POLICY_ACTIONS.map((a) => (
            <div key={a.value} className={styles.policyLegendItem} style={{ borderColor: a.color, background: a.bg }}>
              <span className={styles.policyLegendDot} style={{ background: a.color }} />
              <span style={{ color: a.color, fontWeight: 600, fontSize: "0.78rem" }}>{a.label}</span>
            </div>
          ))}
        </div>

        {policiesLoading ? (
          <div className={styles.loadingRow}>Loading policies…</div>
        ) : policies.length === 0 ? (
          <div className={styles.loadingRow}>Connect the backend to load policies.</div>
        ) : (
          <div className={styles.policyGrid}>
            {policies.map((p) => {
              const actionMeta = POLICY_ACTIONS.find(a => a.value === p.action) ?? POLICY_ACTIONS[0];
              return (
                <div key={p.error_type} className={`${styles.policyCard} ${p.overridden ? styles.policyCardOverridden : ""}`}>
                  <div className={styles.policyCardHeader}>
                    <div>
                      <div className={styles.policyErrorType}>{p.error_type}</div>
                      <div className={styles.policyDesc}>{p.description}</div>
                    </div>
                    {p.overridden && (
                      <button className={styles.policyResetBtn} onClick={() => handleResetPolicy(p.error_type)} disabled={savingPolicy === p.error_type} title="Reset to default">↺</button>
                    )}
                  </div>
                  <div className={styles.policyCardFooter}>
                    <div className={styles.policySelectWrapper} style={{ borderColor: actionMeta.color, background: actionMeta.bg }}>
                      <span className={styles.policySelectDot} style={{ background: actionMeta.color }} />
                      <select
                        className={styles.policySelect}
                        style={{ color: actionMeta.color }}
                        value={p.action}
                        disabled={savingPolicy === p.error_type}
                        onChange={(e) => handlePolicyChange(p.error_type, e.target.value)}
                      >
                        {POLICY_ACTIONS.map((a) => (
                          <option key={a.value} value={a.value}>{a.label}</option>
                        ))}
                      </select>
                    </div>
                    {!p.overridden && (
                      <span className={styles.policyDefault}>default</span>
                    )}
                    {p.overridden && (
                      <span className={styles.policyCustom}>custom</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}