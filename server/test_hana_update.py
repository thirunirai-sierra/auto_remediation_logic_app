import os
from dotenv import load_dotenv
from hdbcli import dbapi

load_dotenv()

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", 443))
HANA_USER = os.getenv("HANA_USER")
HANA_PASSWORD = os.getenv("HANA_PASSWORD")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")

conn = dbapi.connect(
    address=HANA_HOST,
    port=HANA_PORT,
    user=HANA_USER,
    password=HANA_PASSWORD,
    encrypt=True,
    sslValidateCertificate=False,
)
cursor = conn.cursor()
cursor.execute(f'SET SCHEMA "{HANA_SCHEMA}"')
test_id = "test-123"
cursor.execute(f"DELETE FROM LOGIC_APPS_OBSERVABILITY WHERE INCIDENT_ID = '{test_id}'")
cursor.execute(f"INSERT INTO LOGIC_APPS_OBSERVABILITY (INCIDENT_ID, ROOT_CAUSE) VALUES ('{test_id}', 'test root cause')")
conn.commit()
cursor.execute(f"SELECT ROOT_CAUSE FROM LOGIC_APPS_OBSERVABILITY WHERE INCIDENT_ID = '{test_id}'")
row = cursor.fetchone()
print("Inserted and retrieved:", row)
cursor.close()
conn.close()