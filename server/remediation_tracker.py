"""
Smart Remediation Tracker - Prevents duplicate fixes with intelligent retry logic.
Tracks remediation attempts per run with status (attempted/succeeded) and retry counts.
"""

import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class RemediatedRunRecord:
    """Track a single remediation attempt and its outcome."""
    run_id: str
    workflow_name: str
    attempted_at: str
    error_type: str
    status: str = "attempted"          # "attempted", "succeeded", "failed"
    current_run_status: str = "unknown"
    retry_count: int = 0
    last_attempt_at: Optional[str] = None
    auto_fix_attempted: bool = True
    auto_fix_success: bool = False
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class WorkflowState:
    """Tracks state of a workflow."""
    workflow_name: str
    last_fixed_at: str
    last_fixed_run_id: str
    error_type_fixed: str
    workflow_hash: str
    fix_count: int
    last_successful_run_at: Optional[str] = None
    is_healthy: bool = False
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class RemediationTracker:
    """
    Tracks remediated runs to avoid fixing the SAME run twice.
    Supports retry logic (max 2 retries, 1-hour delays).
    """
    
    RETENTION_DAYS = 90
    MAX_RETRY_COUNT = 2
    
    def __init__(self, state_file: str = "remediation_state.json"):
        self.state_file = Path(state_file)
        self.remediated_runs: Dict[str, RemediatedRunRecord] = {}
        self.workflow_states: Dict[str, WorkflowState] = {}
        self.workflow_hashes: Dict[str, str] = {}
        self._load()
    
    def _load(self):
        """Load state from file with backward compatibility."""
        if not self.state_file.exists():
            return
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
            
            # Convert old format if needed
            for run_id, run_data in data.get('remediated_runs', {}).items():
                if isinstance(run_data, str):
                    # Old format: just timestamp
                    self.remediated_runs[run_id] = RemediatedRunRecord(
                        run_id=run_id,
                        workflow_name="",
                        attempted_at=run_data,
                        error_type="unknown"
                    )
                else:
                    self.remediated_runs[run_id] = RemediatedRunRecord.from_dict(run_data)
            
            for name, state_data in data.get('workflow_states', {}).items():
                self.workflow_states[name] = WorkflowState.from_dict(state_data)
            self.workflow_hashes = data.get('workflow_hashes', {})
            self._cleanup_old_entries()
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
    
    def _save(self):
        try:
            data = {
                'remediated_runs': {k: v.to_dict() for k, v in self.remediated_runs.items()},
                'workflow_states': {k: v.to_dict() for k, v in self.workflow_states.items()},
                'workflow_hashes': self.workflow_hashes,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save state file: {e}")
    
    def _cleanup_old_entries(self):
        cutoff = datetime.now() - timedelta(days=self.RETENTION_DAYS)
        old_runs = [rid for rid, rec in self.remediated_runs.items()
                    if datetime.fromisoformat(rec.attempted_at) < cutoff]
        for rid in old_runs:
            del self.remediated_runs[rid]
        if old_runs:
            logger.info(f"Cleaned up {len(old_runs)} runs older than {self.RETENTION_DAYS} days")
    
    def compute_workflow_hash(self, workflow_definition: dict) -> str:
        definition_str = json.dumps(workflow_definition, sort_keys=True)
        return hashlib.md5(definition_str.encode()).hexdigest()
    
    def get_run_record(self, run_id: str) -> Optional[RemediatedRunRecord]:
        return self.remediated_runs.get(run_id)
    
    def is_run_already_remediated(self, run_id: str) -> bool:
        if run_id not in self.remediated_runs:
            return False
        record = self.remediated_runs[run_id]
        if record.status == "succeeded":
            return True
        if record.retry_count >= self.MAX_RETRY_COUNT:
            return True
        return False
    
    def should_fix_run(self, run_id: str, current_status: str) -> Tuple[bool, str]:
        """
        Returns (should_fix, reason).
        Uses current Azure status and retry logic.
        """
        if current_status == "succeeded":
            return False, "already_succeeded"
        
        record = self.remediated_runs.get(run_id)
        if not record:
            return True, "first_attempt"
        
        if record.status == "succeeded":
            return False, "already_succeeded"
        
        if record.retry_count >= self.MAX_RETRY_COUNT:
            return False, "max_retries_exceeded"
        
        # Allow retry after 1 hour
        if record.last_attempt_at:
            last_attempt = datetime.fromisoformat(record.last_attempt_at)
            if datetime.now() - last_attempt > timedelta(hours=1):
                return True, f"retry_{record.retry_count + 1}"
            else:
                return False, "retry_not_yet"
        
        return False, "unknown"
    
    def mark_run_remediated(self, run_id: str, workflow_name: str, error_type: str,
                            workflow_definition: dict = None):
        """Mark a remediation attempt (first attempt)."""
        now = datetime.now().isoformat()
        record = RemediatedRunRecord(
            run_id=run_id,
            workflow_name=workflow_name,
            attempted_at=now,
            error_type=error_type,
            last_attempt_at=now,
            auto_fix_attempted=True,
            auto_fix_success=False,
            retry_count=0
        )
        self.remediated_runs[run_id] = record
        
        # Update workflow state
        if workflow_definition:
            self.workflow_hashes[workflow_name] = self.compute_workflow_hash(workflow_definition)
        
        if workflow_name in self.workflow_states:
            state = self.workflow_states[workflow_name]
            state.last_fixed_at = now
            state.last_fixed_run_id = run_id
            state.error_type_fixed = error_type
            state.fix_count += 1
        else:
            self.workflow_states[workflow_name] = WorkflowState(
                workflow_name=workflow_name,
                last_fixed_at=now,
                last_fixed_run_id=run_id,
                error_type_fixed=error_type,
                workflow_hash=self.workflow_hashes.get(workflow_name, ""),
                fix_count=1
            )
        self._save()
    
    def update_run_status(self, run_id: str, current_status: str):
        """Sync Azure run status and update auto_fix_success if needed."""
        if run_id in self.remediated_runs:
            record = self.remediated_runs[run_id]
            record.current_run_status = current_status
            if current_status == "succeeded":
                record.status = "succeeded"
                record.auto_fix_success = True
            record.last_attempt_at = datetime.now().isoformat()
            self._save()
    
    def increment_retry(self, run_id: str):
        """Increment retry count for a run (called before a retry attempt)."""
        if run_id in self.remediated_runs:
            record = self.remediated_runs[run_id]
            record.retry_count += 1
            record.last_attempt_at = datetime.now().isoformat()
            self._save()
    
    def get_all_records(self) -> Dict[str, RemediatedRunRecord]:
        return dict(self.remediated_runs)
    
    def get_stats(self) -> dict:
        total = len(self.remediated_runs)
        attempted = sum(1 for r in self.remediated_runs.values() if r.status == "attempted")
        succeeded = sum(1 for r in self.remediated_runs.values() if r.status == "succeeded")
        return {
            'total_remediated_runs': total,
            'attempted': attempted,
            'succeeded': succeeded,
            'failed': total - attempted - succeeded,
            'tracked_workflows': len(self.workflow_states),
            'healthy_workflows': sum(1 for s in self.workflow_states.values() if s.is_healthy),
            'total_fixes_applied': sum(s.fix_count for s in self.workflow_states.values())
        }


_tracker = None

def get_tracker() -> RemediationTracker:
    global _tracker
    if _tracker is None:
        _tracker = RemediationTracker()
    return _tracker