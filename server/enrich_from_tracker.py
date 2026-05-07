# enrich_from_tracker.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from datetime import datetime
from hana_observability import get_hana_client
from remediation_tracker import get_tracker
from config import get_settings

settings = get_settings()
hana = get_hana_client(settings)
if not hana:
    print("❌ HANA client not available")
    exit(1)

tracker = get_tracker()
stats = tracker.get_stats()
print(f"Tracker has {stats['total_remediated_runs']} remediated runs")

# Get all incidents from HANA
cursor = hana.conn.cursor()
cursor.execute(f"SELECT INCIDENT_ID, WORKFLOW_NAME FROM {hana.full_table}")
rows = cursor.fetchall()
print(f"Found {len(rows)} incidents in HANA")

updated = 0
for incident_id, workflow in rows:
    # Look up in tracker's remediated_runs
    record = tracker.get_run_record(incident_id)
    if not record:
        continue
    
    # If we have a workflow_state for this workflow
    state = tracker.workflow_states.get(workflow)
    
    # Build AI fields
    diagnosis = ""
    proposed_fix = ""
    confidence = 0.0
    fix_patch = None
    
    if record.error_type:
        diagnosis = f"Root cause: {record.error_type}"
        proposed_fix = f"Apply fix: {record.status}"
        confidence = 0.85 if record.auto_fix_success else 0.6
    
    if state and state.error_type_fixed:
        diagnosis = state.error_type_fixed
        confidence = 0.95
    
    # Update HANA
    cursor.execute(f"""
        UPDATE {hana.full_table}
        SET AI_DIAGNOSIS = ?,
            AI_PROPOSED_FIX = ?,
            AI_CONFIDENCE = ?,
            AI_FIX_PATCH = ?,
            AUTO_FIX_ATTEMPTED = ?,
            AUTO_FIX_SUCCESS = ?,
            RETRY_COUNT = ?,
            STATUS = ?
        WHERE INCIDENT_ID = ?
    """, (
        diagnosis[:4000],
        proposed_fix[:4000],
        confidence,
        json.dumps(fix_patch) if fix_patch else None,
        record.auto_fix_attempted,
        record.auto_fix_success,
        record.retry_count,
        "Fix Succeeded" if record.auto_fix_success else ("Fix Attempted" if record.auto_fix_attempted else "Ticket Created"),
        incident_id
    ))
    updated += 1
    if updated % 50 == 0:
        print(f"Updated {updated} incidents...")

hana.conn.commit()
cursor.close()
print(f"✅ Updated {updated} incidents with AI fields from tracker")