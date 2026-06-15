/**
* @fileoverview HTTP client for Orbit UI: resolves `API_BASE`, `LOG_API_BASE`, and `USER_API_BASE` from Vite env;
* provides typed `fetch` helpers for dashboard, observability/monitor, pipeline, AEM/Event Mesh, and MCP endpoints.
*/

/** Fallback primary API URL when `VITE_API_PRIMARY` is not set (Logic Apps backend on BTP). */
export const API_PRIMARY =
  import.meta.env.VITE_API_PRIMARY ?? "https://nd-orbit-eventmesh-be-logicapps.cfapps.us10-001.hana.ondemand.com";

/** True when running in browser on localhost — uses same-origin empty base (Vite proxy). */


const API_BASE =
  (import.meta.env.VITE_API_BASE ?? "").replace(/\/+$/, "");

const LOG_API_BASE =
  (import.meta.env.VITE_LOG_API_BASE ?? API_BASE).replace(/\/+$/, "");


const USER_API_BASE = (import.meta.env.VITE_USER_API_BASE ?? "").replace(/\/+$/, "");

/**
* JSON `fetch` with default `Content-Type: application/json`; throws `Error` with body text on non-OK.
* @param {string} url - Absolute or same-origin path.
* @param {RequestInit} [options] - Optional `fetch` init (method, body, headers merged with JSON header).
* @returns {Promise<T>} Parsed JSON body.
*/
async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

/**
* Multipart POST for `/query` and file uploads (does not set JSON content-type).
* @param {string} url - Target URL.
* @param {FormData} formData - Multipart body.
* @returns {Promise<T>} Parsed JSON response.
*/
async function postForm<T>(url: string, formData: FormData): Promise<T> {
  const response = await fetch(url, { method: "POST", body: formData });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

/**
* Like `request` but swallows errors — for optional endpoints (e.g. AEM status when backend is down).
* @param {string} url - Target URL.
* @param {RequestInit} [options] - Optional `fetch` init.
* @returns {Promise<T | null>} Parsed JSON or `null` on failure.
*/
async function requestMaybe<T>(url: string, options?: RequestInit): Promise<T | null> {
  try {
    return await request<T>(url, options);
  } catch {
    return null;
  }
}

/**
* Ensures `iflow_name` is populated for Event Mesh lists when API only returns ids or alternate keys.
* @param {Record<string, unknown>} incident - Raw incident object.
* @returns {Record<string, unknown>} Shallow copy with normalized `iflow_name`.
*/
function normalizeIncidentForUi(incident: Record<string, unknown>): Record<string, unknown> {
  return {
    ...incident,
    iflow_name:
      (incident.iflow_name as string) ||
      (incident.iflow_id as string) ||
      (incident.artifact_id as string) ||
      (incident.integration_flow_name as string) ||
      "",
  };
}

// ----------------------------------------------------------------------
// User and legacy endpoints (unchanged)
// ----------------------------------------------------------------------

/**
* Loads current user from `USER_API_BASE` or returns a static anonymous profile when unset.
* @returns {Promise<unknown>} User object compatible with UI5 shell expectations.
*/
export async function fetchCurrentUser(): Promise<unknown> {
  if (!USER_API_BASE) {
    return {
      firstname: "Anonymous",
      lastname: "User",
      email: "",
      name: "",
      displayName: "Anonymous User",
    };
  }
  const data = await requestMaybe<unknown>(`${USER_API_BASE}/user-api/currentUser`);
  return data ?? {
    firstname: "Anonymous",
    lastname: "User",
    email: "",
    name: "",
    displayName: "Anonymous User",
  };
}

/**
* Fetches saved chat history for a user email.
* @param {string} email - User id / email query parameter.
* @returns {Promise<{ history: unknown[] }>} History envelope from backend.
*/
export async function fetchAllHistory(email: string): Promise<{ history: unknown[] }> {
  return request(`${API_BASE}/get_all_history?user_id=${encodeURIComponent(email)}`);
}

/**
* Posts orchestrator chat as multipart form (`/query`).
* @param {FormData} formData - Must include `query` and typically `user_id` and optional `files`.
* @returns {Promise<{ response: string; id: string }>} Assistant reply and message id.
*/
export async function sendChatMessage(formData: FormData): Promise<{ response: string; id: string }> {
  return postForm(`${API_BASE}/query`, formData);
}

/** Bucket counts returned with monitor messages for observability summary cards. */
export type MonitorMessagesSummary = {
  FAILED: number;
  SUCCESS: number;
  PROCESSING: number;
  RETRY: number;
};

/**
* Paginated monitor message list from logging API; includes optional `summary` when server supports it.
* @param {unknown} [limit=50] - Page size (defaults to 50 if React Query passes a non-number first arg).
* @param {unknown} [offset=0] - Row offset for pagination.
* @returns {Promise<{ messages: unknown[]; total?: number; summary?: MonitorMessagesSummary }>} List payload.
*/
export async function fetchMonitorMessages(
  limit: unknown = 50,
  offset: unknown = 0,
): Promise<{
  messages: unknown[];
  total?: number;
  summary?: MonitorMessagesSummary;
}> {
  const l = typeof limit === "number" && Number.isFinite(limit) ? limit : 50;
  const o = typeof offset === "number" && Number.isFinite(offset) ? offset : 0;
  const params = new URLSearchParams({ limit: String(l), offset: String(o) });
  return request(`${LOG_API_BASE}/api/monitor/messages?${params}`);
}

/**
* Single incident/message detail for observability right pane.
* @param {string} guid - `INCIDENT_ID` / message guid.
* @returns {Promise<unknown>} Detail JSON from backend.
*/
export async function fetchMonitorMessageDetail(guid: string): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/message/${guid}`);
}

/**
* Triggers AI RCA analysis for an incident.
* @param {string} guid - Incident id.
* @param {string} [userId="user"] - Audit user id posted in JSON body.
* @returns {Promise<unknown>} Analysis result payload.
*/
export async function analyzeMessage(guid: string, userId = "user"): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/analyze/${guid}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

/**
* LLM explanation of error for an incident.
* @param {string} guid - Incident id.
* @param {string} [userId="user"] - Audit user id.
* @returns {Promise<unknown>} Structured explanation JSON.
*/
export async function explainError(guid: string, userId = "user"): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/explain/${guid}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

/**
* Requests structured fix plan / patch metadata for an incident.
* @param {string} guid - Incident id.
* @param {string} [userId="user"] - Audit user id.
* @returns {Promise<unknown>} Fix patch JSON stored by backend.
*/
export async function generateFixPatch(guid: string, userId = "user"): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/generate-fix/${guid}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

/**
* Runs remediation pipeline for an incident (optional proposed fix text and force flag).
* @param {string} guid - Incident id.
* @param {string} [userId="user"] - Maps to `trigger_type` in body.
* @param {string} [proposedFix] - Optional operator-provided fix text.
* @param {boolean} [force=false] - When true, appends `?force=true` to URL.
* @returns {Promise<unknown>} Remediation outcome JSON.
*/
export async function applyMessageFix(guid: string, userId = "user", proposedFix?: string, force = false): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/apply-fix/${guid}${force ? "?force=true" : ""}`, {
    method: "POST",
    body: JSON.stringify({ trigger_type: userId, proposed_fix: proposedFix, force }),
  });
}

/**
* Polls fix pipeline status for UI progress display.
* @param {string} incidentId - Incident id.
* @returns {Promise<unknown>} Status, steps, and summary fields.
*/
export async function fetchFixStatus(incidentId: string): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/monitor/fix-status/${incidentId}`);
}

/**
* Smart monitoring conversational endpoint (session-aware).
* @param {string} query - User question text.
* @param {string} [userId="user"] - User id for audit/session.
* @param {string} [messageGuid] - Optional incident/message context.
* @param {string} [sessionId] - Optional existing session id to continue.
* @returns {Promise<{ answer: string; session_id: string }>} Assistant answer and session id.
*/
export async function smartMonitoringChat(
  query: string,
  userId = "user",
  messageGuid?: string,
  sessionId?: string,
): Promise<{ answer: string; session_id: string }> {
  return request(`${API_BASE}/smart-monitoring/chat`, {
    method: "POST",
    body: JSON.stringify({ query, user_id: userId, message_guid: messageGuid, session_id: sessionId }),
  });
}

/**
* Loads test suite execution logs for dashboard / diagnostics.
* @returns {Promise<{ ts_logs: unknown[] }>} Logs array under `ts_logs`.
*/
export async function fetchTestSuiteLogs(): Promise<{ ts_logs: unknown[] }> {
  return request(`${API_BASE}/get_testsuite_logs`);
}

/**
* Uploads migration-related files via `/query` multipart; normalizes response to old/new code shape for wizard UI.
* @param {FormData} formData - Files and optional `query` / `user_id` (defaults appended if missing).
* @returns {Promise<{ oldCode: string; newCode: string }>} Parsed migration output (`oldCode` currently empty; `newCode` from `response`).
*/
export async function uploadMigrationFiles(formData: FormData): Promise<{ oldCode: string; newCode: string }> {
  if (!formData.has("query")) formData.append("query", "Analyze uploaded migration files and provide migrated code.");
  if (!formData.has("user_id")) formData.append("user_id", "user");
  const res = await postForm<{ response: string }>(`${API_BASE}/query`, formData);
  return { oldCode: "", newCode: res.response ?? "" };
}

/**
* Fetches autonomous incidents and maps them to simple `{ name, issue }` rows for PIPO-style lists.
* @returns {Promise<unknown[]>} Array of `{ name, issue }` objects.
*/
export async function fetchPipoDetails(): Promise<unknown[]> {
  const data = await request<{ incidents?: unknown[] }>(`${API_BASE}/autonomous/incidents?limit=100`);
  const incidents = (data.incidents ?? []) as Record<string, unknown>[];
  return incidents.map((item) => ({
    name: item.iflow_id ?? item.iflow_name ?? item.message_guid ?? "-",
    issue: item.error_message ?? item.status ?? "-",
  }));
}

/**
* Generic file upload to `/query` with default prompt and user id when omitted.
* @param {FormData} formData - Multipart payload (files + optional `query` / `user_id`).
* @returns {Promise<unknown>} Raw backend JSON.
*/
export async function uploadFile(formData: FormData): Promise<unknown> {
  if (!formData.has("query")) formData.append("query", "Analyze uploaded files.");
  if (!formData.has("user_id")) formData.append("user_id", "user");
  return postForm(`${API_BASE}/query`, formData);
}

/**
* Aggregated dashboard bundle (KPIs, charts, lists) from main API.
* @returns {Promise<Record<string, unknown>>} Dashboard payload keyed by backend contract.
*/
export async function fetchDashboardAll(): Promise<Record<string, unknown>> {
  return request(`${API_BASE}/dashboard/all`);
}

// ----------------------------------------------------------------------
// Autonomous / Pipeline endpoints (updated)
// ----------------------------------------------------------------------

/** Normalized autonomous pipeline / agent row state for orchestrator UI. */
export interface AgentStatus {
  pipeline_running: boolean;
  started_at: string | null;
  agents: Record<string, string>;
  message?: string;
  autonomous_running?: boolean;
  tool_distribution?: Record<string, string[]>;
}

/**
* Reads `/api/monitor/status` and maps `is_running` into per-agent idle/running labels for the UI.
* @returns {Promise<AgentStatus>} Synthetic agent map and `pipeline_running` flag.
*/
export async function fetchPipelineStatus(): Promise<AgentStatus> {
  const data = await request<{ is_running: boolean; poll_interval_seconds?: number }>(`${API_BASE}/api/monitor/status`);
  const running = data.is_running ?? false;
  const agents = {
    observer: running ? "running" : "idle",
    classifier: running ? "running" : "idle",
    rca: running ? "running" : "idle",
    fixer: running ? "running" : "idle",
    verifier: running ? "running" : "idle",
  };
  return {
    pipeline_running: running,
    started_at: null,
    agents,
    message: running ? "Autonomous pipeline running" : "Autonomous pipeline stopped",
    autonomous_running: running,
  };
}

/**
* Starts autonomous monitor pipeline then returns refreshed status.
* @returns {Promise<{ message: string; status: AgentStatus }>} Human message plus latest `AgentStatus`.
*/
export async function startPipeline(): Promise<{ message: string; status: AgentStatus }> {
  await request(`${API_BASE}/api/monitor/start`, { method: "POST" });
  const status = await fetchPipelineStatus();
  return { message: "Pipeline started", status };
}

/**
* Stops autonomous monitor pipeline.
* @returns {Promise<{ message: string }>} Confirmation message.
*/
export async function stopPipeline(): Promise<{ message: string }> {
  await request(`${API_BASE}/api/monitor/stop`, { method: "POST" });
  return { message: "Pipeline stopped" };
}

/**
* Placeholder: reports auto-fix feature as enabled (UI stub until backend exposes flag).
* @returns {Promise<{ auto_fix_enabled: boolean }>} Fixed `{ auto_fix_enabled: true }`.
*/
export async function fetchAutoFixStatus(): Promise<{ auto_fix_enabled: boolean }> {
  return { auto_fix_enabled: true };
}

/**
* Placeholder: toggles auto-fix (returns disabled); extend when API exists.
* @returns {Promise<{ auto_fix_enabled: boolean }>} Fixed `{ auto_fix_enabled: false }`.
*/
export async function toggleAutoFix(): Promise<{ auto_fix_enabled: boolean }> {
  return { auto_fix_enabled: false };
}

/**
* Fetches recent monitor messages and reshapes them into “incident trace” rows for pipeline visualization.
* @param {number} [limit=20] - Max messages to return.
* @returns {Promise<{ incidents: unknown[]; total: number }>} Mapped incidents plus total count.
*/
export async function fetchPipelineTrace(limit = 20): Promise<{ incidents: unknown[]; total: number }> {
  const data = await request<{ messages: unknown[]; total: number }>(`${API_BASE}/api/monitor/messages?limit=${limit}`);
  const incidents = data.messages.map((msg: any) => ({
    ...msg,
    iflow_name: msg.iflow_display,
    error_type: msg.error_type,
    status: msg.status,
    created_at: msg.log_start,
  }));
  return { incidents, total: data.total };
}

// ----------------------------------------------------------------------
// Tickets and Approvals (stubs)
// ----------------------------------------------------------------------

/**
 * Lists support tickets from logging service.
 * @returns {Promise<{ tickets: unknown[] }>} Tickets array.
 */
export async function fetchTickets(sync = false): Promise<{ tickets: unknown[]; sync?: unknown }> {
  const qs = sync ? "?sync=true" : "";
  return request(`${LOG_API_BASE}/api/tickets${qs}`);
}

/**
* Updates ticket fields (status, assignee, resolution notes).
* @param {string} ticketId - Ticket identifier.
* @param {{ status?: string; assigned_to?: string | null; resolution_notes?: string | null }} updates - Partial ticket update.
* @returns {Promise<{ ticket: unknown }>} Updated ticket envelope.
*/
export async function updateTicket(
  ticketId: string,
  updates: { status?: string; assigned_to?: string | null; resolution_notes?: string | null }
): Promise<{ ticket: unknown }> {
  return request(`${LOG_API_BASE}/api/tickets/${encodeURIComponent(ticketId)}/update`, {
    method: "POST",
    body: JSON.stringify(updates),
  });
}

/**
* Fetches incidents awaiting operator approval.
* @returns {Promise<{ pending: unknown[] }>} Pending approval rows.
*/
export async function fetchPendingApprovals(): Promise<{ pending: unknown[] }> {
  return request(`${LOG_API_BASE}/api/approvals/pending`);
}

/**
* Submits approve/reject decision for an incident remediation.
* @param {string} incidentId - Incident id.
* @param {boolean} approved - True to approve, false to reject.
* @param {string} [comment=""] - Optional reviewer comment.
* @returns {Promise<unknown>} Backend acknowledgment payload.
*/
export async function approveIncident(
  incidentId: string,
  approved: boolean,
  comment = ""
): Promise<unknown> {
  return request(`${LOG_API_BASE}/api/approvals/${incidentId}/approve`, {
    method: "POST",
    body: JSON.stringify({ approved, comment }),
  });
}

// ----------------------------------------------------------------------
// MCP Tools
// ----------------------------------------------------------------------

/** MCP registry summary: total tool count and tools grouped by server name. */
export interface McpToolsStatus {
  total: number;
  servers: Record<string, string[]>;
}

/**
* Loads MCP tool catalog from main API.
* @returns {Promise<McpToolsStatus>} Total count and `servers` map.
*/
export async function fetchMcpTools(): Promise<McpToolsStatus> {
  return request(`${API_BASE}/api/mcp/tools`);
}

/**
* Derives per-pipeline-role tool name lists by fuzzy-matching server keys in `fetchMcpTools` result.
* @returns {Promise<Record<string, string[]>>} Keys: observer, classifier, rca, fixer, verifier → tool names.
*/
export async function fetchToolDistribution(): Promise<Record<string, string[]>> {
  const { servers } = await fetchMcpTools();
  const findTools = (...keywords: string[]): string[] => {
    const hit = Object.entries(servers).find(([server]) =>
      keywords.some((kw) => server.toLowerCase().includes(kw))
    );
    return hit ? hit[1] : [];
  };
  return {
    observer: findTools("observer"),
    classifier: findTools("classifier"),
    rca: findTools("rca"),
    fixer: findTools("fixer"),
    verifier: findTools("verifier"),
  };
}

// ----------------------------------------------------------------------
// AEM / Event Mesh stubs
// ----------------------------------------------------------------------

/** Event mesh / AEM health and throughput snapshot for observability header. */
export interface AemStatusResponse {
  total_incidents: number;
  messages_retrieved: number;
  webhook_events_count?: number;
  queue_depth: number;
  stage_counts?: Record<string, number>;
  event_mesh_enabled?: boolean;
  webhook_active?: boolean;
}

/**
* Optional AEM status (404-safe); returns null when endpoint is absent.
* @returns {Promise<AemStatusResponse | null>} Status object or null.
*/
export async function fetchAemStatus(): Promise<AemStatusResponse | null> {
  return requestMaybe<AemStatusResponse>(`${LOG_API_BASE}/api/aem/status`);
}

/**
* Loads AEM incidents and normalizes each row for UI consumption.
* @param {number} [limit=100] - Max incidents.
* @returns {Promise<{ incidents: Record<string, unknown>[] }>} `incidents` after `normalizeIncidentForUi`.
*/
export async function fetchAemIncidents(
  limit = 100
): Promise<{ incidents: Record<string, unknown>[] }> {
  const data = await request<{ incidents?: unknown[] }>(`${LOG_API_BASE}/api/aem/incidents?limit=${limit}`);
  return {
    incidents: ((data.incidents ?? []) as Record<string, unknown>[]).map(normalizeIncidentForUi),
  };
}

/** Paginated smart-monitoring messages envelope (fields vary by backend version). */
export interface PaginatedMessagesResponse {
  messages?: unknown[];
  total_count?: number;
  count?: number;
  page?: number;
  page_size?: number;
  [key: string]: unknown;
}

/**
* Server-side paginated failed (or filtered) messages for smart monitoring tables.
* @param {number} [page=1] - 1-based page index.
* @param {number} [pageSize=20] - Rows per page.
* @param {string} [status] - Optional status filter.
* @param {string} [type] - Optional message type filter.
* @param {string} [id] - Optional id filter.
* @param {string} [artifacts] - Optional artifacts filter flag/value.
* @returns {Promise<PaginatedMessagesResponse>} Paginated list and metadata.
*/
export async function fetchFailedMessagesPaginated(
  page = 1,
  pageSize = 20,
  status?: string,
  type?: string,
  id?: string,
  artifacts?: string
): Promise<PaginatedMessagesResponse> {
  const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) });
  if (status) params.set("status", status);
  if (type) params.set("type", type);
  if (id) params.set("id", id);
  if (artifacts) params.set("artifacts", artifacts);
  return request(`${API_BASE}/smart-monitoring/messages/paginated?${params}`);
}

/** Standard paginated incidents response for dashboard tables. */
export interface PaginatedIncidentsResponse {
  count: number;
  total_count: number;
  page: number;
  page_size: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
  incidents: unknown[];
}

/** Single row shape for log-derived incident summaries. */
export interface LogIncident {
  incidentId: string | null;
  subscriptionId: string | null;
  integrationScenario: string | null;
  errorType: string | null;
  errorMessage: string | null;
  time: string | null;
}

/**
* Fetches flat incident list from logging service `/incidents`.
* @returns {Promise<LogIncident[]>} Incident rows.
*/
export async function fetchLogIncidents(): Promise<LogIncident[]> {
  return request<LogIncident[]>(`${LOG_API_BASE}/incidents`);
}

/** KPIs, breakdowns, timeline, and recent error messages for logs overview page. */
export interface LogsOverviewResponse {
  kpi: {
    total_flows: number;
    error_flows: number;
    fixed_flows: number;
    total_logs: number;
    total_error_messages: number;
  };
  status_breakdown: Array<{ status: string; count: number }>;
  error_distribution: Array<{ error_type: string; count: number }>;
  top_iflows: Array<{ workflow_name: string; failure_count: number }>;
  timeline: Array<{ time: string; count: number }>;
  error_messages: Array<{
    integrationScenario: string | null;
    errorType: string | null;
    errorMessage: string | null;
    time: string | null;
    resourceId: string | null;
    status: string | null;
    runId: string | null;
  }>;
}

/**
* Aggregated logs analytics for observability dashboard widgets.
* @param {number} [top=1000] - Cap for underlying “top N” queries.
* @returns {Promise<LogsOverviewResponse>} KPIs, distributions, timeline, messages.
*/
export async function fetchLogsOverview(top = 1000): Promise<LogsOverviewResponse> {
  const params = new URLSearchParams({ top: String(top) });
  return request<LogsOverviewResponse>(`${LOG_API_BASE}/logs/overview?${params}`);
}

/**
* Paginated active incidents for main dashboard incident grid.
* @param {number} [page=1] - Page number.
* @param {number} [pageSize=20] - Page size.
* @param {string} [status] - Optional status filter.
* @returns {Promise<PaginatedIncidentsResponse>} Page metadata and `incidents` array.
*/
export async function fetchActiveIncidentsPaginated(page = 1, pageSize = 20, status?: string): Promise<PaginatedIncidentsResponse> {
  const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) });
  if (status) params.set("status", status);
  return request(`${API_BASE}/dashboard/incidents/paginated?${params}`);
}

/**
* Lists autonomous incidents with optional status filter (non-paginated list).
* @param {string} [status] - Optional status query.
* @param {number} [limit=50] - Max rows.
* @returns {Promise<{ incidents: unknown[]; total: number }>} Incidents and total from backend.
*/
export async function fetchIncidents(status?: string, limit = 50): Promise<{ incidents: unknown[]; total: number }> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (status) params.set("status", status);
  return request(`${API_BASE}/autonomous/incidents?${params}`);
}

// ── Runtime Settings ───────────────────────────────────────────────────────────

export interface RuntimeSetting {
  key: string;
  label: string;
  category: string;
  impact: "HIGH" | "MEDIUM" | "LOW";
  description: string;
  takes_effect: string;
  unit: string;
  type: "float" | "int" | "bool" | "str";
  min?: number;
  max?: number;
  default: unknown;
  value: unknown;
  overridden: boolean;
}

export async function fetchRuntimeSettings(): Promise<{ settings: RuntimeSetting[] }> {
  return request(`${API_BASE}/api/settings`);
}

export async function updateRuntimeSetting(key: string, value: unknown): Promise<RuntimeSetting> {
  return request(`${API_BASE}/api/settings/${encodeURIComponent(key)}`, {
    method: "PATCH",
    body: JSON.stringify({ value }),
  });
}

export async function resetRuntimeSetting(key: string): Promise<RuntimeSetting> {
  return request(`${API_BASE}/api/settings/${encodeURIComponent(key)}/reset`, {
    method: "DELETE",
  });
}

// ── Remediation Policies ───────────────────────────────────────────────────────

export interface RemediationPolicy {
  error_type: string;
  description: string;
  action: "AUTO_FIX" | "RETRY" | "TICKET_CREATED" | "AWAITING_APPROVAL";
  default_action: string;
  overridden: boolean;
}

export async function fetchPolicies(): Promise<{ policies: RemediationPolicy[] }> {
  return request(`${API_BASE}/api/settings/policies`);
}

export async function updatePolicy(error_type: string, action: string): Promise<RemediationPolicy> {
  return request(`${API_BASE}/api/settings/policies/${encodeURIComponent(error_type)}`, {
    method: "PATCH",
    body: JSON.stringify({ action }),
  });
}

export async function resetPolicy(error_type: string): Promise<RemediationPolicy> {
  return request(`${API_BASE}/api/settings/policies/${encodeURIComponent(error_type)}/reset`, {
    method: "DELETE",
  });
}