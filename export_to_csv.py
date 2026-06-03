
import os
import csv
from dotenv import load_dotenv
from hdbcli import dbapi
from datetime import datetime

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

# Fetch all rows
cursor.execute("SELECT * FROM LOGIC_APPS_OBSERVABILITY ORDER BY CREATED_AT DESC")
rows = cursor.fetchall()
col_names = [desc[0] for desc in cursor.description]

# Save to CSV
filename = f"hana_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
with open(filename, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(col_names)
    writer.writerows(rows)

print(f"Exported {len(rows)} rows to {filename}")
cursor.close()
conn.close()