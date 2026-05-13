#!/usr/bin/env python3
"""
Query HANA observability table for recent RCA results.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from hdbcli import dbapi

# Load .env
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = int(os.getenv("HANA_PORT", "443"))
HANA_USER = os.getenv("HANA_USER")
HANA_PASSWORD = os.getenv("HANA_PASSWORD")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")
TABLE_NAME = os.getenv("HANA_OBSERVABILITY_TABLE", "LOGIC_APPS_OBSERVABILITY")

if not all([HANA_HOST, HANA_USER, HANA_PASSWORD, HANA_SCHEMA]):
    print("Missing HANA credentials")
    sys.exit(1)

def main():
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
    
    # Query incidents that have ROOT_CAUSE filled (RCA performed)
    query = f"""
    SELECT 
        INCIDENT_ID, 
        WORKFLOW_NAME,
        ROOT_CAUSE,
        PROPOSED_FIX,
        RCA_CONFIDENCE,
        AFFECTED_COMPONENT,
        CREATED_AT
    FROM "{TABLE_NAME}"
    WHERE ROOT_CAUSE IS NOT NULL
    ORDER BY CREATED_AT DESC
    LIMIT 10
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        print("No RCA results found. The RCA updates may not have been committed.")
        print("Possible reasons:")
        print("  - The `update_rca_record` method was not called.")
        print("  - HANA connection failed during update.")
        print("  - The incident IDs were not found in the table (primary key mismatch).")
    else:
        print(f"Found {len(rows)} incidents with RCA data:\n")
        for row in rows:
            print(f"Incident ID: {row[0]}")
            print(f"Workflow: {row[1]}")
            print(f"Root cause: {row[2][:100]}..." if row[2] else "(null)")
            print(f"Proposed fix: {row[3][:100]}..." if row[3] else "(null)")
            print(f"Confidence: {row[4]}")
            print(f"Affected component: {row[5]}")
            print(f"Created at: {row[6]}")
            print("-" * 80)
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()