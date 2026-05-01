"""
CLI entry: single failed run, multi-workflow scan, or continuous monitoring.
Uses new LLM-based Orchestrator (Observer → RCA → Fixer).
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("msal").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

from config import get_settings, Settings
from multi_flow_runner import collect_failed_run_errors, process_failed_runs
from remediation_tracker import get_tracker

# Load LLM-based Orchestrator (primary)
ORCHESTRATOR_AVAILABLE = False
try:
    from Orchestrator_agent import run_remediation as orchestrator_remediation
    ORCHESTRATOR_AVAILABLE = True
    print("✓ LLM-based Orchestrator loaded")
except ImportError:
    try:
        from agent.orchestrator.Orchestrator_agent import run_remediation as orchestrator_remediation
        ORCHESTRATOR_AVAILABLE = True
        print("✓ Orchestrator loaded")
    except ImportError as e:
        print(f"⚠️ Orchestrator not available: {e}")

# Fallback to legacy if needed
try:
    from workflow_agent import run_remediation as legacy_remediation
except ImportError:
    legacy_remediation = None


class ContinuousMonitor:
    def __init__(
        self,
        settings: Settings,
        subscription_id: str,
        resource_group: str,
        workspace_id: str,
        poll_interval_seconds: int = 60,
        lookback_hours: int = 24,
        use_orchestrator: bool = True,
        backup_dir: Optional[str] = None,
        max_concurrency: int = 4,
    ):
        self.settings = settings
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_id = workspace_id
        self.poll_interval = poll_interval_seconds
        self.lookback_hours = lookback_hours
        self.use_orchestrator = use_orchestrator and ORCHESTRATOR_AVAILABLE
        self.backup_dir = backup_dir
        self.max_concurrency = max_concurrency
        self._running = False
        self.tracker = get_tracker()
    
    def start(self) -> None:
        self._running = True
        print("\n" + "=" * 70)
        print("🔍 Starting Continuous Monitor (LLM-based)")
        print(f"   Poll interval: {self.poll_interval}s")
        print(f"   Lookback hours: {self.lookback_hours}")
        print(f"   Using LLM Orchestrator: {'✅ YES' if self.use_orchestrator else '❌ NO'}")
        print("=" * 70)
        print("Press Ctrl+C to stop\n")
        
        def signal_handler(signum, frame):
            print("\n⚠️ Stopping monitor...")
            self._running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            while self._running:
                try:
                    self._poll_and_remediate()
                except Exception as e:
                    print(f"❌ Polling error: {e}")
                
                if self._running:
                    for i in range(self.poll_interval):
                        if not self._running:
                            break
                        time.sleep(1)
        finally:
            print("\n🛑 Monitor stopped")
    
    def _poll_and_remediate(self) -> None:
        print(f"\n📊 Polling at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)
        
        try:
            failed_runs = collect_failed_run_errors(
                workspace_id=self.workspace_id,
                hours=self.lookback_hours,
                top_n=50,
                settings=self.settings,
            )
        except Exception as e:
            print(f"❌ Failed to query Log Analytics: {e}")
            return
        
        results = failed_runs.get("results", [])
        if not results:
            print("✓ No failed runs found")
            return
        
        print(f"📋 Found {len(results)} failed runs")
        
        remediated_count = 0
        skipped_count = 0
        failed_count = 0
        
        for run in results:
            wf_name = run.get("resource_workflowName_s") or run.get("workflow_name")
            run_id = run.get("resource_runId_s") or run.get("run_id")
            
            if not wf_name or not run_id:
                continue
            
            if self.tracker.is_run_already_remediated(run_id):
                skipped_count += 1
                print(f"⏭️ Skipping {wf_name}/{run_id}: already fixed")
                continue
            
            print(f"🔧 Remediating {wf_name}/{run_id} with LLM...")
            result = self._remediate_run(wf_name, run_id)
            
            if result and result.get("status") in ("remediated", "success"):
                remediated_count += 1
                error_type = result.get("root_cause", "unknown")
                self.tracker.mark_run_remediated(run_id, wf_name, error_type)
                print(f"✅ SUCCESS: {wf_name}/{run_id}")
                print(f"   Root cause: {result.get('root_cause')}")
                print(f"   Fix: {result.get('fix_strategy', {}).get('strategy_description', 'N/A')[:80]}")
            else:
                failed_count += 1
                error = result.get("error", "Unknown") if result else "Unknown"
                print(f"❌ FAILED: {wf_name}/{run_id} - {error[:100]}")
            
            time.sleep(2)
        
        print(f"\n📊 Poll complete: remediated={remediated_count}, skipped={skipped_count}, failed={failed_count}")
        stats = self.tracker.get_stats()
        print(f"📈 Total fixes: {stats['total_remediated_runs']}")
    
    def _remediate_run(self, workflow_name: str, run_id: str) -> Optional[Dict[str, Any]]:
        try:
            if self.use_orchestrator:
                return orchestrator_remediation(
                    workflow_name=workflow_name,
                    run_id=run_id,
                    subscription_id=self.subscription_id,
                    resource_group=self.resource_group,
                    settings=self.settings,
                    backup_dir=self.backup_dir,
                )
            elif legacy_remediation:
                result = legacy_remediation(
                    subscription_id=self.subscription_id,
                    resource_group=self.resource_group,
                    workflow_name=workflow_name,
                    run_id=run_id,
                    settings=self.settings,
                    backup_dir=self.backup_dir,
                )
                return {"status": result.get("status")} if result else None
            else:
                return {"status": "error", "error": "No remediation engine available"}
        except Exception as e:
            print(f"❌ Error: {e}")
            return {"status": "error", "error": str(e)}


def show_tracker_stats():
    tracker = get_tracker()
    stats = tracker.get_stats()
    print("\n" + "=" * 50)
    print("📊 REMEDIATION TRACKER STATISTICS")
    print("=" * 50)
    print(f"Total remediated runs:     {stats['total_remediated_runs']}")
    print(f"Tracked workflows:         {stats['tracked_workflows']}")
    print(f"Total fixes applied:       {stats['total_fixes_applied']}")
    print("=" * 50)


def reset_tracker():
    confirm = input("⚠️ Reset all tracking data? (y/N): ")
    if confirm.lower() == 'y':
        import os
        if os.path.exists("remediation_state.json"):
            os.remove("remediation_state.json")
        print("✅ Tracker reset!")
    else:
        print("❌ Cancelled")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Logic Apps Auto-Remediation (LLM-based)")
    
    p.add_argument("-s", "--subscription-id", required=False)
    p.add_argument("-g", "--resource-group", required=False)
    p.add_argument("-w", "--workflow", default=None)
    p.add_argument("-r", "--run-id", default=None)
    p.add_argument("-b", "--backup-dir", default="./backups")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("--stats", action="store_true")
    p.add_argument("--reset-tracker", action="store_true")
    
    mf = p.add_argument_group("Multi-workflow")
    mf.add_argument("--all-flows", action="store_true")
    mf.add_argument("--monitor", action="store_true")
    mf.add_argument("--workspace-id", default=None)
    mf.add_argument("--hours", type=int, default=24)
    mf.add_argument("--poll-interval", type=int, default=60)
    
    args = p.parse_args(argv)
    
    if args.stats:
        show_tracker_stats()
        return 0
    
    if args.reset_tracker:
        reset_tracker()
        return 0
    
    if args.quiet:
        logging.basicConfig(level=logging.ERROR)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    settings = get_settings()
    subscription_id = args.subscription_id or settings.subscription_id
    resource_group = args.resource_group or settings.resource_group
    workspace_id = args.workspace_id or settings.log_analytics_workspace_id
    
    # Monitor mode
    if args.monitor:
        if not workspace_id:
            print("❌ Error: --workspace-id required")
            return 1
        
        monitor = ContinuousMonitor(
            settings=settings,
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_id=workspace_id,
            poll_interval_seconds=args.poll_interval,
            lookback_hours=args.hours,
            use_orchestrator=ORCHESTRATOR_AVAILABLE,
            backup_dir=args.backup_dir,
        )
        monitor.start()
        return 0
    
    # Single run mode
    if not args.all_flows:
        if not args.workflow or not args.run_id:
            p.error("--workflow and --run-id required")
        
        if not subscription_id or not resource_group:
            p.error("subscription-id and resource-group required")
        
        tracker = get_tracker()
        if tracker.is_run_already_remediated(args.run_id):
            print(f"⚠️ Run {args.run_id} already remediated")
            return 0
        
        print(f"🔧 Starting LLM-based remediation for {args.workflow}/{args.run_id}...")
        
        if ORCHESTRATOR_AVAILABLE:
            result = orchestrator_remediation(
                workflow_name=args.workflow,
                run_id=args.run_id,
                subscription_id=subscription_id,
                resource_group=resource_group,
                settings=settings,
                backup_dir=args.backup_dir,
            )
        else:
            print("❌ LLM Orchestrator not available")
            return 1
        
        if result.get("status") == "remediated":
            tracker.mark_run_remediated(args.run_id, args.workflow, result.get("root_cause", "unknown"))
            print("✅ REMEDIATED")
        
        print(json.dumps(result, indent=2))
        return 0
    
    # Multi-flow scan
    if not workspace_id:
        p.error("--workspace-id required")
    
    report = collect_failed_run_errors(
        workspace_id=workspace_id,
        hours=args.hours,
        top_n=50,
        settings=settings,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))