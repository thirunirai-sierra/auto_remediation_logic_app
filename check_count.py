# check_count.py
import os
from dotenv import load_dotenv
from hdbcli import dbapi

load_dotenv()

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
cursor.execute("SELECT COUNT(*) FROM LOGIC_APPS_OBSERVABILITY")
count = cursor.fetchone()[0]
print(f"Total incidents in HANA: {count}")

# Also show first 5 rows for verification
cursor.execute("SELECT INCIDENT_ID, WORKFLOW_NAME, ERROR_CATEGORY, STATUS, CREATED_AT FROM LOGIC_APPS_OBSERVABILITY LIMIT 5")
print("\nFirst 5 incidents:")
for row in cursor.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")

cursor.close()
conn.close()