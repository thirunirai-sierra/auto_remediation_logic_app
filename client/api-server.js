import express from "express";
import axios from "axios";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();
dotenv.config({ path: ".env.local", override: false });
dotenv.config({ path: ".env.production", override: false });
dotenv.config({ path: ".env.api", override: false });

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.API_PORT || 4000);

const REQUIRED_ENV = [
  "AZURE_TENANT_ID",
  "AZURE_CLIENT_ID",
  "AZURE_CLIENT_SECRET",
  "LOG_ANALYTICS_WORKSPACE_ID",
];

function validateEnv() {
  const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }
}

async function getAccessToken() {
  const tenantId = process.env.AZURE_TENANT_ID;
  const clientId = process.env.AZURE_CLIENT_ID;
  const clientSecret = process.env.AZURE_CLIENT_SECRET;
  const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;

  const form = new URLSearchParams();
  form.append("client_id", clientId);
  form.append("client_secret", clientSecret);
  form.append("scope", "https://api.loganalytics.io/.default");
  form.append("grant_type", "client_credentials");

  const response = await axios.post(tokenUrl, form.toString(), {
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    timeout: 30000,
  });

  return response.data.access_token;
}

async function queryFailedIncidents(accessToken) {
  const workspaceId = process.env.LOG_ANALYTICS_WORKSPACE_ID;
  const queryUrl = `https://api.loganalytics.io/v1/workspaces/${workspaceId}/query`;

  const query = `
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.LOGIC"
| where status_s == "Failed"
| project
    incidentId = correlation_actionTrackingId_s,
    subscriptionId = _SubscriptionId,
    integrationScenario = resource_workflowName_s,
    errorType = error_code_s,
    errorMessage = error_message_s,
    time = TimeGenerated
| top 100 by time desc
`;

  const response = await axios.post(
    queryUrl,
    { query },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      timeout: 45000,
    }
  );

  return response.data;
}

function transformRows(result) {
  const table = result?.tables?.[0];
  if (!table || !Array.isArray(table.rows)) {
    return [];
  }
  return table.rows.map((row) => ({
    incidentId: row[0] ?? null,
    subscriptionId: row[1] ?? null,
    integrationScenario: row[2] ?? null,
    errorType: row[3] ?? null,
    errorMessage: row[4] ?? null,
    time: row[5] ?? null,
  }));
}

app.get("/incidents", async (_req, res) => {
  try {
    validateEnv();
    const accessToken = await getAccessToken();
    const data = await queryFailedIncidents(accessToken);
    const incidents = transformRows(data);
    return res.status(200).json(incidents);
  } catch (error) {
    const status = error?.response?.status || 500;
    const details = error?.response?.data || error?.message || "Unknown error";
    return res.status(status).json({
      error: "Failed to fetch incidents from Log Analytics",
      details,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Incident API running on http://localhost:${PORT}`);
});
