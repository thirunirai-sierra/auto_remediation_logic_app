#!/usr/bin/env python3
"""
Verify HANA database for RCA and remediation results.
Run this after the orchestrator completes remediation.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from hdbcli import dbapi
from datetime import datetime, timedelta

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
    print("❌ Missing HANA credentials in .env")
    sys.exit(1)

def main():
    print("=" * 80)
    print("HANA Database Verification")
    print("=" * 80)
    print(f"Connecting to {HANA_HOST}:{HANA_PORT}/{HANA_SCHEMA}...")
    
    try:
        conn = dbapi.connect(
            address=HANA_HOST,
            port=HANA_PORT,
            user=HANA_USER,
            password=HANA_PASSWORD,
            encrypt=True,
            sslValidateCertificate=False,
            timeout=30,
        )
        print("✅ Connected to HANA\n")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        sys.exit(1)
    
    cursor = conn.cursor()
    cursor.execute(f'SET SCHEMA "{HANA_SCHEMA}"')
    
    # Query 1: All incidents (last 24 hours)
    print("=" * 80)
    print("1️⃣  ALL INCIDENTS (Last 24 Hours)")
    print("=" * 80)
    query1 = f"""
    SELECT 
        INCIDENT_ID,
        WORKFLOW_NAME,
        ERROR_CODE,
        STATUS,
        CREATED_AT
    FROM "{TABLE_NAME}"
    WHERE CREATED_AT > ADD_SECONDS(CURRENT_TIMESTAMP, -86400)
    ORDER BY CREATED_AT DESC
    """
    cursor.execute(query1)
    rows = cursor.fetchall()
    
    if not rows:
        print("❌ No incidents found in last 24 hours\n")
    else:
        print(f"✅ Found {len(rows)} incidents:\n")
        for i, row in enumerate(rows, 1):
            print(f"{i}. {row[0]} ({row[1]})")
            print(f"   Status: {row[3]}")
            print(f"   Error: {row[2]}")
            print(f"   Time: {row[4]}\n")
    
    # Query 2: RCA results (with root cause)
    print("=" * 80)
    print("2️⃣  RCA RESULTS (With Root Cause)")
    print("=" * 80)
    query2 = f"""
    SELECT 
        INCIDENT_ID,
        WORKFLOW_NAME,
        ROOT_CAUSE,
        PROPOSED_FIX,
        RCA_CONFIDENCE,
        CREATED_AT
    FROM "{TABLE_NAME}"
    WHERE ROOT_CAUSE IS NOT NULL
    AND CREATED_AT > ADD_SECONDS(CURRENT_TIMESTAMP, -86400)
    ORDER BY CREATED_AT DESC
    """
    cursor.execute(query2)
    rows = cursor.fetchall()
    
    if not rows:
        print("⚠️  No RCA results found")
        print("   Possible reasons:")
        print("   - No incidents have ROOT_CAUSE set")
        print("   - orchestrator.update_rca_record() not called")
        print("   - HANA connection failed during update\n")
    else:
        print(f"✅ Found {len(rows)} incidents with RCA:\n")
        for i, row in enumerate(rows, 1):
            print(f"{i}. {row[0]} ({row[1]})")
            print(f"   Root Cause: {row[2][:80]}...")
            print(f"   Proposed Fix: {row[3][:80]}..." if row[3] else "   Proposed Fix: (null)")
            print(f"   Confidence: {row[4]}")
            print(f"   Time: {row[5]}\n")
    
    # Query 3: Remediation success
    print("=" * 80)
    print("3️⃣  REMEDIATION SUCCESS (Status='Remediated')")
    print("=" * 80)
    query3 = f"""
    SELECT 
        INCIDENT_ID,
        WORKFLOW_NAME,
        STATUS,
        AUTO_FIX_ATTEMPTED,
        AUTO_FIX_SUCCESS,
        CREATED_AT
    FROM "{TABLE_NAME}"
    WHERE STATUS = 'Remediated'
    AND CREATED_AT > ADD_SECONDS(CURRENT_TIMESTAMP, -86400)
    """
    cursor.execute(query3)
    rows = cursor.fetchall()
    
    if not rows:
        print("⚠️  No successful remediations found\n")
    else:
        print(f"✅ Found {len(rows)} successful remediations:\n")
        for i, row in enumerate(rows, 1):
            print(f"{i}. {row[0]} ({row[1]})")
            print(f"   Status: {row[2]}")
            print(f"   Attempted: {row[3]}, Success: {row[4]}")
            print(f"   Time: {row[5]}\n")
    
    # Query 4: Failures
    print("=" * 80)
    print("4️⃣  FAILURES (Status NOT 'Remediated')")
    print("=" * 80)
    query4 = f"""
    SELECT 
        INCIDENT_ID,
        WORKFLOW_NAME,
        STATUS,
        ERROR_CODE,
        ERROR_MESSAGE,
        CREATED_AT
    FROM "{TABLE_NAME}"
    WHERE STATUS != 'Remediated'
    AND CREATED_AT > ADD_SECONDS(CURRENT_TIMESTAMP, -86400)
    LIMIT 10
    """
    cursor.execute(query4)
    rows = cursor.fetchall()
    
    if not rows:
        print("✅ No failures (all remediated!)\n")
    else:
        print(f"⚠️  Found {len(rows)} failures:\n")
        for i, row in enumerate(rows, 1):
            print(f"{i}. {row[0]} ({row[1]})")
            print(f"   Status: {row[2]}")
            print(f"   Error: {row[3]} - {row[4][:50]}...")
            print(f"   Time: {row[5]}\n")
    
    # Query 5: Stats
    print("=" * 80)
    print("5️⃣  STATISTICS")
    print("=" * 80)
    query5 = f"""
    SELECT 
        COUNT(*) as TOTAL,
        SUM(CASE WHEN ROOT_CAUSE IS NOT NULL THEN 1 ELSE 0 END) as WITH_RCA,
        SUM(CASE WHEN STATUS = 'Remediated' THEN 1 ELSE 0 END) as SUCCESSFUL,
        SUM(CASE WHEN STATUS != 'Remediated' THEN 1 ELSE 0 END) as FAILED,
        SUM(CASE WHEN AUTO_FIX_ATTEMPTED = TRUE THEN 1 ELSE 0 END) as ATTEMPTED
    FROM "{TABLE_NAME}"
    WHERE CREATED_AT > ADD_SECONDS(CURRENT_TIMESTAMP, -86400)
    """
    cursor.execute(query5)
    row = cursor.fetchone()
    
    if row:
        total, with_rca, successful, failed, attempted = row
        print(f"Total Incidents:     {total or 0}")
        print(f"With RCA:            {with_rca or 0}")
        print(f"Successfully Fixed:  {successful or 0}")
        print(f"Failed:              {failed or 0}")
        print(f"Fix Attempted:       {attempted or 0}")
        if total and total > 0:
            success_rate = ((successful or 0) / total) * 100
            print(f"Success Rate:        {success_rate:.1f}%")
    print()
    
    cursor.close()
    conn.close()
    print("=" * 80)
    print("✅ Verification complete")
    print("=" * 80)

if __name__ == "__main__":
    main()