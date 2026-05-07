
// services/api.ts
export const API_PRIMARY =
  import.meta.env.VITE_API_PRIMARY ?? "https://ND-ORBIT.cfapps.us10-001.hana.ondemand.com";

const isLocalHost =
  typeof window !== "undefined" &&
  (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1");

const API_BASE = isLocalHost
  ? ""
  : (import.meta.env.VITE_API_BASE ?? API_PRIMARY).replace(/\/+$/, "");

const LOG_API_BASE = isLocalHost
  ? ""
  : (import.meta.env.VITE_LOG_API_BASE ?? API_BASE).replace(/\/+$/, "");

const USER_API_BASE = (import.meta.env.VITE_USER_API_BASE ?? "").replace(/\/+$/, "");

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

async function postForm<T>(url: string, formData: FormData): Promise<T> {
  const response = await fetch(url, { method: "POST", body: formData });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

async function requestMaybe<T>(url: string, options?: RequestInit): Promise<T | null> {
  try {
    return await request<T>(url, options);
  } catch {
    return null;
  }
}

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

export async function fetchAllHistory(email: string): Promise<{ history: unknown[] }> {
  return request(`${API_BASE}/get_all_history?user_id=${encodeURIComponent(email)}`);
}

export async function sendChatMessage(formData: FormData): Promise<{ response: string; id: string }> {
  return postForm(`${API_BASE}/query`, formData);
}

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

export async function fetchTestSuiteLogs(): Promise<{ ts_logs: unknown[] }> {
  return request(`${API_BASE}/get_testsuite_logs`);
}

export async function uploadMigrationFiles(formData: FormData): Promise<{ oldCode: string; newCode: string }> {
  if (!formData.has("query")) formData.append("query", "Analyze uploaded migration files and provide migrated code.");
  if (!formData.has("user_id")) formData.append("user_id", "user");
  const res = await postForm<{ response: string }>(`${API_BASE}/query`, formData);
  return { oldCode: "", newCode: res.response ?? "" };
}

export async function fetchPipoDetails(): Promise<unknown[]> {
  const data = await request<{ incidents?: unknown[] }>(`${API_BASE}/autonomous/incidents?limit=100`);
  const incidents = (data.incidents ?? []) as Record<string, unknown>[];
  return incidents.map((item) => ({
    name: item.iflow_id ?? item.iflow_name ?? item.message_guid ?? "-",
    issue: item.error_message ?? item.status ?? "-",
  }));
}

export async function uploadFile(formData: FormData): Promise<unknown> {
  if (!formData.has("query")) formData.append("query", "Analyze uploaded files.");
  if (!formData.has("user_id")) formData.append("user_id", "user");
  return postForm(`${API_BASE}/query`, formData);
}

export async function fetchDashboardAll(): Promise<Record<string, unknown>> {
  return request(`${API_BASE}/dashboard/all`);
}

// ----------------------------------------------------------------------
// NEW OBSERVABILITY ENDPOINTS (matches your FastAPI routes)
// ----------------------------------------------------------------------
export async function fetchMonitorMessages(limit = 50, offset = 0): Promise<{ messages: unknown[]; total: number }> {
  const l = typeof limit === 'number' ? limit : 50;
  const o = typeof offset === 'number' ? offset : 0;
  return request(`${API_BASE}/api/monitor/messages?limit=${l}&offset=${o}`);
}

export async function fetchMonitorMessageDetail(incidentId: string): Promise<unknown> {
  return request(`${API_BASE}/api/monitor/message/${incidentId}`);
}

export async function analyzeMessage(incidentId: string, userId = "user"): Promise<unknown> {
  return request(`${API_BASE}/api/monitor/analyze/${incidentId}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

export async function explainError(incidentId: string, userId = "user"): Promise<unknown> {
  return request(`${API_BASE}/api/monitor/explain/${incidentId}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

export async function generateFixPatch(incidentId: string, userId = "user"): Promise<unknown> {
  return request(`${API_BASE}/api/monitor/generate-fix/${incidentId}`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
}

export async function applyMessageFix(
  incidentId: string,
  userId = "user",
  proposedFix?: string,
  force = false
): Promise<unknown> {
  const url = `${API_BASE}/api/monitor/apply-fix/${incidentId}${force ? "?force=true" : ""}`;
  return request(url, {
    method: "POST",
    body: JSON.stringify({ trigger_type: userId, proposed_fix: proposedFix, force }),
  });
}

export async function fetchFixStatus(incidentId: string): Promise<unknown> {
  return request(`${API_BASE}/api/monitor/fix-status/${incidentId}`);
}

// ----------------------------------------------------------------------
// Autonomous / Pipeline endpoints (updated)
// ----------------------------------------------------------------------
export interface AgentStatus {
  pipeline_running: boolean;
  started_at: string | null;
  agents: Record<string, string>;
  message?: string;
  autonomous_running?: boolean;
  tool_distribution?: Record<string, string[]>;
}

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

export async function startPipeline(): Promise<{ message: string; status: AgentStatus }> {
  await request(`${API_BASE}/api/monitor/start`, { method: "POST" });
  const status = await fetchPipelineStatus();
  return { message: "Pipeline started", status };
}

export async function stopPipeline(): Promise<{ message: string }> {
  await request(`${API_BASE}/api/monitor/stop`, { method: "POST" });
  return { message: "Pipeline stopped" };
}

export async function fetchAutoFixStatus(): Promise<{ auto_fix_enabled: boolean }> {
  return { auto_fix_enabled: true };
}

export async function toggleAutoFix(): Promise<{ auto_fix_enabled: boolean }> {
  return { auto_fix_enabled: false };
}

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
export async function fetchTickets(): Promise<{ tickets: unknown[] }> {
  return request(`${API_BASE}/api/tickets`);
}

export async function updateTicket(ticketId: string, updates: { status?: string; assigned_to?: string | null; resolution_notes?: string | null }): Promise<{ ticket: unknown }> {
  return request(`${API_BASE}/api/tickets/${encodeURIComponent(ticketId)}/update`, {
    method: "POST",
    body: JSON.stringify(updates),
  });
}

export async function fetchPendingApprovals(): Promise<{ pending: unknown[] }> {
  return request(`${API_BASE}/api/approvals/pending`);
}

export async function approveIncident(incidentId: string, approved: boolean, comment = ""): Promise<unknown> {
  return request(`${API_BASE}/api/approvals/${incidentId}/approve`, {
    method: "POST",
    body: JSON.stringify({ approved, comment }),
  });
}

// ----------------------------------------------------------------------
// MCP Tools
// ----------------------------------------------------------------------
export interface McpToolsStatus {
  total: number;
  servers: Record<string, string[]>;
}

export async function fetchMcpTools(): Promise<McpToolsStatus> {
  return request(`${API_BASE}/api/mcp/tools`);
}

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
export interface AemStatusResponse {
  total_incidents: number;
  messages_retrieved: number;
  webhook_events_count?: number;
  queue_depth: number;
  stage_counts?: Record<string, number>;
  event_mesh_enabled?: boolean;
  webhook_active?: boolean;
}

export async function fetchAemStatus(): Promise<AemStatusResponse | null> {
  return requestMaybe<AemStatusResponse>(`${API_BASE}/api/aem/status`);
}

export async function fetchAemIncidents(limit = 100): Promise<{ incidents: Record<string, unknown>[] }> {
  const data = await request<{ incidents?: unknown[] }>(`${API_BASE}/api/aem/incidents?limit=${limit}`);
  return { incidents: (data.incidents ?? []) as Record<string, unknown>[] };
}

// ----------------------------------------------------------------------
// Legacy paginated endpoints (keep as is)
// ----------------------------------------------------------------------
export interface PaginatedMessagesResponse {
  count: number;
  total_count: number;
  page: number;
  page_size: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
  messages: unknown[];
}

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

export interface LogIncident {
  incidentId: string | null;
  subscriptionId: string | null;
  integrationScenario: string | null;
  errorType: string | null;
  errorMessage: string | null;
  time: string | null;
}

export async function fetchLogIncidents(): Promise<LogIncident[]> {
  return request<LogIncident[]>(`${LOG_API_BASE}/incidents`);
}

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
  top_iflows: Array<{ iflow_name: string; failure_count: number }>;
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

export async function fetchLogsOverview(hours = 24, top = 1000): Promise<LogsOverviewResponse> {
  const params = new URLSearchParams({ hours: String(hours), top: String(top) });
  return request<LogsOverviewResponse>(`${LOG_API_BASE}/logs/overview?${params}`);
}

export async function fetchActiveIncidentsPaginated(page = 1, pageSize = 20, status?: string): Promise<PaginatedIncidentsResponse> {
  const params = new URLSearchParams({ page: String(page), page_size: String(pageSize) });
  if (status) params.set("status", status);
  return request(`${API_BASE}/dashboard/incidents/paginated?${params}`);
}

export async function fetchIncidents(status?: string, limit = 50): Promise<{ incidents: unknown[]; total: number }> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (status) params.set("status", status);
  return request(`${API_BASE}/autonomous/incidents?${params}`);
}