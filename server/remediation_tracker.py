"""
Smart Remediation Tracker - Prevents duplicate fixes for SAME RUN only.
Allows fixing multiple runs of the same workflow.
"""

import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass, asdict

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
    Does NOT prevent fixing different runs of the same workflow.
    """
    
    def __init__(self, state_file: str = "remediation_state.json"):
        self.state_file = Path(state_file)
        self.remediated_runs: Dict[str, datetime] = {}
        self.workflow_states: Dict[str, WorkflowState] = {}
        self.workflow_hashes: Dict[str, str] = {}
        self._load()
    
    def _load(self):
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                self.remediated_runs = {
                    k: datetime.fromisoformat(v) 
                    for k, v in data.get('remediated_runs', {}).items()
                }
                for name, state_data in data.get('workflow_states', {}).items():
                    self.workflow_states[name] = WorkflowState.from_dict(state_data)
                self.workflow_hashes = data.get('workflow_hashes', {})
                self._cleanup_old_entries()
            except Exception as e:
                print(f"Warning: Could not load state file: {e}")
    
    def _save(self):
        """Save state to file."""
        try:
            data = {
                'remediated_runs': {k: v.isoformat() for k, v in self.remediated_runs.items()},
                'workflow_states': {name: state.to_dict() for name, state in self.workflow_states.items()},
                'workflow_hashes': self.workflow_hashes,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save state file: {e}")
    
    def _cleanup_old_entries(self):
        """Remove entries older than 7 days."""
        cutoff = datetime.now() - timedelta(days=7)
        old_runs = [run_id for run_id, ts in self.remediated_runs.items() if ts < cutoff]
        for run_id in old_runs:
            del self.remediated_runs[run_id]
    
    def compute_workflow_hash(self, workflow_definition: dict) -> str:
        """Compute hash of workflow definition."""
        definition_str = json.dumps(workflow_definition, sort_keys=True)
        return hashlib.md5(definition_str.encode()).hexdigest()
    
    def is_run_already_remediated(self, run_id: str) -> bool:
        """Check if this SPECIFIC run has already been fixed."""
        if run_id in self.remediated_runs:
            if datetime.now() - self.remediated_runs[run_id] > timedelta(hours=1):
                del self.remediated_runs[run_id]
                return False
            return True
        return False
    
    def is_workflow_healthy(self, workflow_name: str) -> bool:
        """Check if workflow is already healthy."""
        if workflow_name in self.workflow_states:
            state = self.workflow_states[workflow_name]
            if state.is_healthy:
                if state.last_successful_run_at:
                    last_success = datetime.fromisoformat(state.last_successful_run_at)
                    if datetime.now() - last_success > timedelta(hours=1):
                        return False
                return True
        return False
    
    def mark_run_remediated(self, run_id: str, workflow_name: str, error_type: str, 
                            workflow_definition: dict = None):
        """Mark a specific run as successfully remediated."""
        self.remediated_runs[run_id] = datetime.now()
        
        workflow_hash = None
        if workflow_definition:
            workflow_hash = self.compute_workflow_hash(workflow_definition)
            self.workflow_hashes[workflow_name] = workflow_hash
        
        if workflow_name in self.workflow_states:
            state = self.workflow_states[workflow_name]
            state.last_fixed_at = datetime.now().isoformat()
            state.last_fixed_run_id = run_id
            state.error_type_fixed = error_type
            state.fix_count += 1
            if workflow_hash:
                state.workflow_hash = workflow_hash
        else:
            self.workflow_states[workflow_name] = WorkflowState(
                workflow_name=workflow_name,
                last_fixed_at=datetime.now().isoformat(),
                last_fixed_run_id=run_id,
                error_type_fixed=error_type,
                workflow_hash=workflow_hash or "",
                fix_count=1
            )
        
        self._save()
    
    def mark_workflow_successful(self, workflow_name: str, run_id: str):
        """Mark workflow as successfully running."""
        if workflow_name in self.workflow_states:
            self.workflow_states[workflow_name].last_successful_run_at = datetime.now().isoformat()
            self.workflow_states[workflow_name].is_healthy = True
        self._save()
    
    def get_stats(self) -> dict:
        """Get tracking statistics."""
        return {
            'total_remediated_runs': len(self.remediated_runs),
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