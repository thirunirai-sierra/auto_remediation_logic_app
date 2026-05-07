# fast_backfill.py
import os
from dotenv import load_dotenv
from hdbcli import dbapi
from azure.identity import ClientSecretCredential
from azure.monitor.query import LogsQueryClient
from datetime import datetime, timedelta

load_dotenv()

# HANA connection
conn = dbapi.connect(
    address=os.getenv("HANA_HOST"),
    port=int(os.getenv("HANA_PORT", 443)),
    user=os.getenv("HANA_USER"),
    password=os.getenv("HANA_PASSWORD"),
    encrypt=True,
    sslValidateCertificate=False
)
cursor = conn.cursor()
cursor.execute("SET SCHEMA AI_USE_CASES_HDI_DB_1")

# Azure Log Analytics client
cred = ClientSecretCredential(
    tenant_id=os.getenv("AZURE_TENANT_ID"),
    client_id=os.getenv("AZURE_CLIENT_ID"),
    client_secret=os.getenv("AZURE_CLIENT_SECRET")
)
logs_client = LogsQueryClient(cred)
workspace_id = os.getenv("LOG_ANALYTICS_WORKSPACE_ID")
sub_id = os.getenv("AZURE_SUBSCRIPTION_ID")

def categorize(msg, code):
    m = (msg or "").lower()
    c = str(code)
    if "401" in c or "unauthorized" in m: return "AUTH_CONFIG_ERROR"
    if "404" in c or "not found" in m: return "MAPPING_ERROR"
    if "ssl" in m or "certificate" in m: return "SSL_ERROR"
    if "timeout" in m: return "TIMEOUT_ERROR"
    if "null" in m or "contains" in m: return "NULL_REFERENCE_ERROR"
    if "add" in m or "div" in m: return "DATA_VALIDATION"
    if "parse_json" in m or "schema" in m: return "SCHEMA_ERROR"
    return "UNKNOWN_ERROR"

# Date range: last 7 days (adjust as needed)
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

query = f"""
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.LOGIC"
| where Category == "WorkflowRuntime"
| where status_s == "Failed"
| where TimeGenerated between (datetime({start_date.isoformat()}) .. datetime({end_date.isoformat()}))
| project 
    TimeGenerated,
    resource_runId_s,
    resource_workflowName_s,
    error_code_s,
    error_message_s
"""

print(f"Querying Log Analytics for {start_date.date()} to {end_date.date()}...")
response = logs_client.query_workspace(workspace_id, query, timespan=(start_date, end_date))
rows = response.tables[0].rows if response.tables else []
print(f"Found {len(rows)} failure rows")

inserted = 0
for row in rows:
    run_id = row[1]
    wf_name = row[2]
    error_code = row[3] or "unknown"
    error_msg = row[4] or ""
    created_at = row[0]
    category = categorize(error_msg, error_code)
    
    try:
        cursor.execute("""
            INSERT INTO LOGIC_APPS_OBSERVABILITY (
                INCIDENT_ID, SUBSCRIPTION_ID, WORKFLOW_NAME,
                ERROR_CODE, ERROR_MESSAGE, ERROR_CATEGORY,
                STATUS, CREATED_AT, UPDATED_AT,
                AUTO_FIX_ATTEMPTED, AUTO_FIX_SUCCESS, RETRY_COUNT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id, sub_id, wf_name,
            error_code, error_msg[:2000], category,
            "Ticket Created", created_at, datetime.now(),
            False, False, 0
        ))
        inserted += 1
    except Exception as e:
        # Skip duplicate primary key
        if "23505" not in str(e) and "unique constraint" not in str(e).lower():
            print(f"Insert error for {run_id}: {e}")

conn.commit()
cursor.close()
conn.close()
print(f"Backfill complete. Inserted {inserted} new records (duplicates skipped).")