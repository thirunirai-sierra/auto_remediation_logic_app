"""
Smart Remediation Tracker - Prevents duplicate fixes with intelligent retry logic.
"""
import json
import hashlib
import logging
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, asdict

from config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class RemediatedRunRecord:
    """
    Track a single remediation attempt and its outcome.

    Attributes:
        run_id (str): Unique identifier of the workflow run.
        workflow_name (str): Name of the workflow.
        attempted_at (str): ISO timestamp when the remediation was attempted.
        error_type (str): Classified error type.
        status (str): Current status of remediation ('attempted' or 'succeeded').
        current_run_status (str): Current status of the workflow run.
        retry_count (int): Number of remediation retries.
        last_attempt_at (Optional[str]): ISO timestamp of the last attempt.
        auto_fix_attempted (bool): Whether an automatic fix was attempted.
        auto_fix_success (bool): Whether the automatic fix succeeded.
        fix_strategy (Optional[str]): Strategy used for the fix (added for new functionality).
        root_cause (Optional[str]): Root cause of the failure (added for new functionality).
    """
    run_id: str
    workflow_name: str
    attempted_at: str
    error_type: str
    status: str = "attempted"
    current_run_status: str = "unknown"
    retry_count: int = 0
    last_attempt_at: Optional[str] = None
    auto_fix_attempted: bool = True
    auto_fix_success: bool = False
    fix_strategy: Optional[str] = None  # NEW: Track which strategy was used
    root_cause: Optional[str] = None     # NEW: Track the root cause

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        # Handle backward compatibility for old records without new fields
        if 'fix_strategy' not in data:
            data['fix_strategy'] = None
        if 'root_cause' not in data:
            data['root_cause'] = None
        return cls(**data)


@dataclass
class WorkflowState:
    """
    Tracks the state of a workflow.

    Attributes:
        workflow_name (str): Name of the workflow.
        last_fixed_at (Optional[str]): ISO timestamp of last successful fix.
        last_fixed_run_id (Optional[str]): Run ID of last fixed run.
        error_type_fixed (str): Last error type fixed.
        workflow_hash (str): Hash of the workflow definition.
        fix_count (int): Number of fixes applied.
        last_fix_strategy (Optional[str]): Last fix strategy used (NEW).
        last_root_cause (Optional[str]): Last root cause fixed (NEW).
    """
    workflow_name: str = ""
    last_fixed_at: Optional[str] = None
    last_fixed_run_id: Optional[str] = None
    error_type_fixed: str = ""
    workflow_hash: str = ""
    fix_count: int = 0
    last_fix_strategy: Optional[str] = None  # NEW
    last_root_cause: Optional[str] = None     # NEW

    @property
    def is_healthy(self) -> bool:
        return self.error_type_fixed not in ("unknown", "manual_review")


class RemediationTracker:
    """
    Tracker for all workflow remediation runs, manages deduplication, retries, and state persistence.

    Attributes:
        retention_days (int): Days to keep old run records.
        max_retry_count (int): Maximum automatic retry attempts per run.
    """

    def __init__(self, state_file: str = "remediation_state.json"):
        """
        Initialize the tracker.

        Args:
            state_file (str): Filepath to store persistent tracker state.
        """
        self.state_file = Path(state_file)
        self.remediated_runs: Dict[str, RemediatedRunRecord] = {}
        self.workflow_states: Dict[str, WorkflowState] = {}
        self.workflow_hashes: Dict[str, str] = {}

        # Read configuration from settings
        settings = get_settings()
        self.retention_days = settings.TRACKER_RETENTION_DAYS
        self.max_retry_count = settings.TRACKER_MAX_RETRY_COUNT

        self._load()

    def _load(self):
        """Load state from disk."""
        if not self.state_file.exists():
            return
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
            for run_id, run_data in data.get('remediated_runs', {}).items():
                if isinstance(run_data, str):
                    # Handle old format (just timestamp string)
                    self.remediated_runs[run_id] = RemediatedRunRecord(
                        run_id=run_id, workflow_name="", attempted_at=run_data, error_type="unknown"
                    )
                else:
                    self.remediated_runs[run_id] = RemediatedRunRecord.from_dict(run_data)
            for name, state_data in data.get('workflow_states', {}).items():
                # Handle backward compatibility
                if 'last_fix_strategy' not in state_data:
                    state_data['last_fix_strategy'] = None
                if 'last_root_cause' not in state_data:
                    state_data['last_root_cause'] = None
                self.workflow_states[name] = WorkflowState(**state_data)
            self.workflow_hashes = data.get('workflow_hashes', {})
            self._cleanup_old_entries()
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")

    def _save(self):
        """Atomic file write using temporary file + rename."""
        try:
            data = {
                'remediated_runs': {k: v.to_dict() for k, v in self.remediated_runs.items()},
                'workflow_states': {k: asdict(v) for k, v in self.workflow_states.items()},
                'workflow_hashes': self.workflow_hashes,
                'last_updated': datetime.now().isoformat()
            }
            # Write to temporary file first
            fd, tmp_path = tempfile.mkstemp(dir=self.state_file.parent, suffix='.tmp')
            try:
                with os.fdopen(fd, 'w') as tmp:
                    json.dump(data, tmp, indent=2)
                os.replace(tmp_path, self.state_file)
            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except Exception as e:
            logger.warning(f"Could not save state file: {e}")

    def _cleanup_old_entries(self):
        """Remove run records older than retention_days."""
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        old_runs = [rid for rid, rec in self.remediated_runs.items()
                    if datetime.fromisoformat(rec.attempted_at) < cutoff]
        for rid in old_runs:
            del self.remediated_runs[rid]
        if old_runs:
            logger.info(f"Cleaned up {len(old_runs)} runs older than {self.retention_days} days")

    def compute_workflow_hash(self, workflow_definition: dict) -> str:
        """
        Compute MD5 hash of a workflow definition.

        Args:
            workflow_definition (dict): Workflow JSON definition.

        Returns:
            str: MD5 hash string.
        """
        return hashlib.md5(json.dumps(workflow_definition, sort_keys=True).encode()).hexdigest()

    def get_run_record(self, run_id: str) -> Optional[RemediatedRunRecord]:
        """
        Retrieve a remediated run record.

        Args:
            run_id (str): Workflow run ID.

        Returns:
            Optional[RemediatedRunRecord]: The record, or None if not found.
        """
        return self.remediated_runs.get(run_id)

    def is_run_already_remediated(self, run_id: str) -> bool:
        """
        Check if a run is already remediated or exceeded max retries.

        Args:
            run_id (str): Workflow run ID.

        Returns:
            bool: True if remediation already applied or retries exceeded.
        """
        record = self.remediated_runs.get(run_id)
        if not record:
            return False
        if record.status == "succeeded":
            return True
        if record.retry_count >= self.max_retry_count:
            return True
        return False

    def should_fix_run(self, run_id: str, current_status: str) -> Tuple[bool, str]:
        """
        Determine if the run should be fixed.

        Args:
            run_id (str): Workflow run ID.
            current_status (str): Current status of the run.

        Returns:
            Tuple[bool, str]: (should_fix, reason string)
        """
        if current_status == "succeeded":
            return False, "already_succeeded"
        record = self.remediated_runs.get(run_id)
        if not record:
            return True, "first_attempt"
        if record.status == "succeeded":
            return False, "already_succeeded"
        if record.retry_count >= self.max_retry_count:
            return False, "max_retries_exceeded"
        if record.last_attempt_at:
            last_attempt = datetime.fromisoformat(record.last_attempt_at)
            if datetime.now() - last_attempt > timedelta(hours=1):
                return True, f"retry_{record.retry_count + 1}"
            else:
                return False, "retry_not_yet"
        return False, "unknown"

    def mark_run_remediated(
        self, 
        run_id: str, 
        workflow_name: str, 
        error_type: str, 
        workflow_definition: dict = None,
        fix_strategy: Optional[str] = None,  # NEW parameter
        root_cause: Optional[str] = None      # NEW parameter
    ):
        """
        Record that a run was remediated successfully.

        Args:
            run_id (str): Workflow run ID.
            workflow_name (str): Workflow name.
            error_type (str): Error type fixed.
            workflow_definition (dict, optional): Workflow JSON definition for hashing.
            fix_strategy (str, optional): Strategy used for the fix.
            root_cause (str, optional): Root cause of the failure.
        """
        now = datetime.now().isoformat()
        record = RemediatedRunRecord(
            run_id=run_id, 
            workflow_name=workflow_name, 
            attempted_at=now, 
            error_type=error_type,
            last_attempt_at=now, 
            auto_fix_attempted=True, 
            auto_fix_success=False, 
            retry_count=0,
            fix_strategy=fix_strategy,   # NEW
            root_cause=root_cause         # NEW
        )
        self.remediated_runs[run_id] = record
        
        if workflow_definition:
            self.workflow_hashes[workflow_name] = self.compute_workflow_hash(workflow_definition)
            
        if workflow_name in self.workflow_states:
            state = self.workflow_states[workflow_name]
            state.last_fixed_at = now
            state.last_fixed_run_id = run_id
            state.error_type_fixed = error_type
            state.fix_count += 1
            state.last_fix_strategy = fix_strategy  # NEW
            state.last_root_cause = root_cause       # NEW
        else:
            self.workflow_states[workflow_name] = WorkflowState(
                workflow_name=workflow_name, 
                last_fixed_at=now, 
                last_fixed_run_id=run_id,
                error_type_fixed=error_type, 
                workflow_hash=self.workflow_hashes.get(workflow_name, ""), 
                fix_count=1,
                last_fix_strategy=fix_strategy,  # NEW
                last_root_cause=root_cause        # NEW
            )
        self._save()
        logger.info(f"Marked run {run_id} as remediated with strategy: {fix_strategy}")

    def update_run_status(self, run_id: str, current_status: str):
        """
        Update the status of a run in the tracker.

        Args:
            run_id (str): Workflow run ID.
            current_status (str): Current workflow run status.
        """
        if run_id in self.remediated_runs:
            record = self.remediated_runs[run_id]
            record.current_run_status = current_status
            if current_status == "succeeded":
                record.status = "succeeded"
                record.auto_fix_success = True
            record.last_attempt_at = datetime.now().isoformat()
            self._save()

    def increment_retry(self, run_id: str):
        """
        Increment the retry count for a run.

        Args:
            run_id (str): Workflow run ID.
        """
        if run_id in self.remediated_runs:
            self.remediated_runs[run_id].retry_count += 1
            self.remediated_runs[run_id].last_attempt_at = datetime.now().isoformat()
            self._save()

    def get_all_records(self) -> Dict[str, RemediatedRunRecord]:
        """
        Get all remediated run records.

        Returns:
            Dict[str, RemediatedRunRecord]: Mapping from run_id to record.
        """
        return dict(self.remediated_runs)

    def get_stats(self) -> dict:
        """
        Get tracker statistics.

        Returns:
            dict: Statistics including total runs, attempted, succeeded, failed, and workflow counts.
        """
        total = len(self.remediated_runs)
        attempted = sum(1 for r in self.remediated_runs.values() if r.status == "attempted")
        succeeded = sum(1 for r in self.remediated_runs.values() if r.status == "succeeded")
        
        # NEW: Stats about fix strategies
        fix_strategies = {}
        for r in self.remediated_runs.values():
            if r.fix_strategy:
                fix_strategies[r.fix_strategy] = fix_strategies.get(r.fix_strategy, 0) + 1
        
        return {
            'total_remediated_runs': total,
            'attempted': attempted,
            'succeeded': succeeded,
            'failed': total - attempted - succeeded,
            'tracked_workflows': len(self.workflow_states),
            'healthy_workflows': sum(1 for s in self.workflow_states.values() if s.is_healthy),
            'total_fixes_applied': sum(s.fix_count for s in self.workflow_states.values()),
            'fix_strategies_used': fix_strategies,  # NEW
        }
    
    def get_workflow_history(self, workflow_name: str) -> Dict[str, any]:
        """
        Get remediation history for a specific workflow.

        Args:
            workflow_name (str): Name of the workflow.

        Returns:
            Dict containing workflow state and recent fixes.
        """
        workflow_state = self.workflow_states.get(workflow_name)
        recent_fixes = {
            run_id: record.to_dict() 
            for run_id, record in self.remediated_runs.items() 
            if record.workflow_name == workflow_name
        }
        return {
            'workflow_state': asdict(workflow_state) if workflow_state else None,
            'recent_fixes': recent_fixes,
            'total_fixes': len(recent_fixes)
        }


_tracker = None

def get_tracker() -> RemediationTracker:
    """
    Retrieve the singleton tracker instance.

    Returns:
        RemediationTracker: Tracker instance.
    """
    global _tracker
    if _tracker is None:
        _tracker = RemediationTracker()
    return _tracker