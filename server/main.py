"""
Main entry point for Azure Logic Apps Auto-Remediation System.

Features:
1. Continuous monitoring of Azure Log Analytics for failed runs
2. Automatic remediation using Orchestrator Agent with LLM
3. FastAPI server for health checks, stats, and manual triggers
4. Configurable polling intervals and concurrency
5. Persistent state tracking to avoid duplicate fixes

Usage:
    python main.py                    # Start monitoring with default settings
    python main.py --once             # Run once and exit
    python main.py --daemon           # Run as background daemon
    python main.py --server-only      # Start only API server (no monitoring)
"""

from __future__ import annotations

import asyncio
import json
import logging
import requests
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any, Dict, List, Optional, Set
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from hdbcli import dbapi

# Import our modules
from config import get_settings, Settings
from workflow_agent import run_remediation
from multi_flow_runner import query_failed_runs_from_workspace

# Import Orchestrator
import importlib.util
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Setup logging before optional imports that may log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Suppress verbose logging
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Try to import orchestrator
ORCHESTRATOR_AVAILABLE = False
try:
    from agent.orchestrator.Orchestrator_agent import run_remediation as orchestrator_remediation
    ORCHESTRATOR_AVAILABLE = True
    logger.info("Orchestrator agent available")
except ImportError as e:
    logger.warning("Orchestrator agent not available: %s", e)


@dataclass
class RemediationTracker:
    """Tracks remediated runs to avoid duplicate fixes."""
    
    # Store remediated run IDs with timestamp
    remediated_runs: Dict[str, datetime] = field(default_factory=dict)
    # Store workflow versions to detect changes
    workflow_versions: Dict[str, str] = field(default_factory=dict)
    # File to persist state
    state_file: Path = Path("remediation_state.json")
    
    def __post_init__(self):
        self.load()
    
    def is_remediated(self, workflow_name: str, run_id: str) -> bool:
        """Check if this run has already been remediated."""
        key = f"{workflow_name}:{run_id}"
        if key not in self.remediated_runs:
            return False
        
        # Expire after 7 days
        if datetime.now() - self.remediated_runs[key] > timedelta(days=7):
            del self.remediated_runs[key]
            return False
        
        return True
    
    def mark_remediated(self, workflow_name: str, run_id: str) -> None:
        """Mark a run as remediated."""
        key = f"{workflow_name}:{run_id}"
        self.remediated_runs[key] = datetime.now()
        self.save()
    
    def get_workflow_version(self, workflow_name: str) -> Optional[str]:
        """Get last known workflow version."""
        return self.workflow_versions.get(workflow_name)
    
    def update_workflow_version(self, workflow_name: str, version: str) -> None:
        """Update workflow version after remediation."""
        self.workflow_versions[workflow_name] = version
        self.save()
    
    def save(self) -> None:
        """Persist state to file."""
        try:
            data = {
                "remediated_runs": {
                    k: v.isoformat() for k, v in self.remediated_runs.items()
                },
                "workflow_versions": self.workflow_versions,
            }
            with open(self.state_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
    
    def load(self) -> None:
        """Load state from file."""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                
                self.remediated_runs = {
                    k: datetime.fromisoformat(v) 
                    for k, v in data.get("remediated_runs", {}).items()
                }
                self.workflow_versions = data.get("workflow_versions", {})
                logger.info(f"Loaded state: {len(self.remediated_runs)} remediated runs")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")


class ContinuousMonitor:
    """
    Continuous monitor for Logic Apps failed runs.
    
    Polls Azure Log Analytics at regular intervals and triggers
    remediation for newly detected failed runs.
    """
    
    def __init__(
        self,
        settings: Settings,
        poll_interval_seconds: int = 60,
        lookback_hours: int = 1,
        use_orchestrator: bool = True,
        backup_dir: Optional[str] = None,
    ):
        self.settings = settings
        self.poll_interval = poll_interval_seconds
        self.lookback_hours = lookback_hours
        self.use_orchestrator = use_orchestrator and ORCHESTRATOR_AVAILABLE
        self.backup_dir = backup_dir
        self.tracker = RemediationTracker()
        self._stop_event = Event()
        self._is_running = False
        
        # Create backup directory if specified
        if backup_dir:
            Path(backup_dir).mkdir(parents=True, exist_ok=True)
    
    def start(self) -> None:
        """Start the continuous monitoring loop."""
        self._is_running = True
        self._stop_event.clear()
        
        logger.info("=" * 60)
        logger.info("🔍 Starting Continuous Monitor")
        logger.info(f"   Poll interval: {self.poll_interval}s")
        logger.info(f"   Lookback hours: {self.lookback_hours}")
        logger.info(f"   Using orchestrator: {self.use_orchestrator}")
        logger.info(f"   Backup directory: {self.backup_dir or 'disabled'}")
        logger.info("=" * 60)
        
        try:
            while not self._stop_event.is_set():
                try:
                    self._poll_and_remediate()
                except Exception as e:
                    logger.error(f"Polling error: {e}")
                
                # Wait for next poll
                if not self._stop_event.is_set():
                    self._stop_event.wait(self.poll_interval)
        finally:
            self._is_running = False
            logger.info("Monitor stopped")
    
    def stop(self) -> None:
        """Stop the monitoring loop."""
        logger.info("Stopping monitor...")
        self._stop_event.set()
        
        # Wait for current iteration to finish
        timeout = 30
        start = time.time()
        while self._is_running and (time.time() - start) < timeout:
            time.sleep(0.5)
        
        logger.info("Monitor stopped")
    
    def _poll_and_remediate(self) -> None:
        """Single poll cycle: query failed runs and remediate."""
        logger.info("-" * 40)
        logger.info(f"📊 Polling at {datetime.now().isoformat()}")
        
        # Check if Log Analytics is configured
        if not self.settings.log_analytics_workspace_id:
            logger.warning("Log Analytics workspace ID not configured. Set LOG_ANALYTICS_WORKSPACE_ID")
            return
        
        # Query failed runs
        try:
            failed_runs = query_failed_runs_from_workspace(
                workspace_id=self.settings.log_analytics_workspace_id,
                hours=self.lookback_hours,
                top_n=50,
                settings=self.settings,
            )
        except Exception as e:
            logger.error(f"Failed to query Log Analytics: {e}")
            return
        
        if not failed_runs:
            logger.info("No failed runs found")
            return
        
        logger.info(f"Found {len(failed_runs)} failed runs in logs")
        
        # Process each failed run
        remediated_count = 0
        skipped_count = 0
        failed_count = 0
        
        for run in failed_runs:
            # Extract run details
            wf_name = run.get("resource_workflowName_s") or run.get("workflowName")
            run_id = run.get("resource_runId_s") or run.get("runId")
            
            if not wf_name or not run_id:
                continue
            
            # Check if already remediated
            if self.tracker.is_remediated(wf_name, run_id):
                logger.debug(f"Skipping already remediated: {wf_name}/{run_id}")
                skipped_count += 1
                continue
            
            logger.info(f"🛠️ Remediating {wf_name}/{run_id}")
            
            # Perform remediation
            result = self._remediate_run(wf_name, run_id)
            
            if result and result.get("status") in ("remediated", "success", "no_error"):
                self.tracker.mark_remediated(wf_name, run_id)
                remediated_count += 1
                logger.info(f"✅ Remediated: {wf_name}/{run_id}")
            else:
                failed_count += 1
                logger.warning(f"❌ Failed to remediate: {wf_name}/{run_id} - {result.get('status') if result else 'unknown'}")
            
            # Small delay between remediations
            time.sleep(2)
        
        logger.info(f"Poll complete: remediated={remediated_count}, skipped={skipped_count}, failed={failed_count}")
    
    def _remediate_run(self, workflow_name: str, run_id: str) -> Optional[Dict[str, Any]]:
        """Remediate a single failed run."""
        try:
            if self.use_orchestrator:
                # Use orchestrator with LLM
                return orchestrator_remediation(
                    workflow_name=workflow_name,
                    run_id=run_id,
                    subscription_id=self.settings.subscription_id,
                    resource_group=self.settings.resource_group,
                    settings=self.settings,
                    backup_dir=self.backup_dir,
                )
            else:
                # Use legacy remediation
                return run_remediation(
                    subscription_id=self.settings.subscription_id,
                    resource_group=self.settings.resource_group,
                    workflow_name=workflow_name,
                    run_id=run_id,
                    settings=self.settings,
                    backup_dir=self.backup_dir,
                )
        except Exception as e:
            logger.error(f"Remediation error for {workflow_name}/{run_id}: {e}")
            return {"status": "error", "detail": str(e)}


class RemediationAPI:
    """FastAPI server for remediation management."""
    
    def __init__(self, settings: Settings, monitor: Optional[ContinuousMonitor] = None):
        self.settings = settings
        self.monitor = monitor
    
    def create_app(self) -> FastAPI:
        """Create the FastAPI application."""
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            logger.info("=" * 60)
            logger.info("🚀 Logic Apps Auto-Remediation API Server")
            logger.info("=" * 60)
            logger.info("Endpoints:")
            logger.info("  GET  /               - Service info")
            logger.info("  GET  /health         - Health check")
            logger.info("  GET  /stats          - Remediation statistics")
            logger.info("  POST /remediate      - Manually trigger remediation")
            logger.info("  POST /remediate/run  - Remediate specific run")
            logger.info("  GET  /monitor/status - Monitor status")
            logger.info("  POST /monitor/start  - Start monitor")
            logger.info("  POST /monitor/stop   - Stop monitor")
            logger.info("=" * 60)
            yield
            logger.info("API Server shutting down")
        
        app = FastAPI(
            title="Logic Apps Auto-Remediation API",
            description="Automatically detect and fix failed Logic App workflows",
            version="2.0.0",
            lifespan=lifespan,
        )
        
        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Register routes
        self._register_routes(app)
        
        return app
    
    def _register_routes(self, app: FastAPI):
        """Register API routes."""
        def _hana_table_name() -> str:
            return (os.getenv("HANA_OBSERVABILITY_TABLE") or "LOGIC_APPS_OBSERVABILITY").strip().strip('"')

        def _hana_connection():
            base = {
                "address": str(self.settings.hana_host).strip().strip('"'),
                "port": int(self.settings.hana_port),
                "user": str(self.settings.hana_user).strip().strip('"'),
                "password": str(self.settings.hana_password).strip().strip('"'),
                "encrypt": True,
            }
            try:
                return dbapi.connect(**base, sslValidateCertificate=False)
            except Exception:
                return dbapi.connect(**base)

        def _read_incidents_from_hana(top: int) -> List[Dict[str, Any]]:
            table = _hana_table_name()
            schema = str(self.settings.hana_schema).strip().strip('"')
            conn = _hana_connection()
            cur = conn.cursor()
            try:
                cur.execute(f'SET SCHEMA "{schema}"')
                cur.execute(
                    f"""
                    SELECT TOP {int(top)}
                        INCIDENT_ID,
                        SUBSCRIPTION_ID,
                        WORKFLOW_NAME,
                        COALESCE(ERROR_CODE, ERROR_CATEGORY),
                        ERROR_MESSAGE,
                        COALESCE(UPDATED_AT, CREATED_AT),
                        COALESCE(RCA_ROOT_CAUSE, ''),
                        COALESCE(STATUS, 'FAILED'),
                        COALESCE(CREATED_AT, UPDATED_AT)
                    FROM "{table}"
                    WHERE UPPER(COALESCE(STATUS, '')) IN ('FAILED', 'ERROR', 'IN_PROGRESS', 'PENDING', 'AUTO_FIXED', 'FIX_VERIFIED', 'HUMAN_FIXED')
                    ORDER BY COALESCE(UPDATED_AT, CREATED_AT) DESC
                    """
                )
                rows = cur.fetchall()
                return [
                    {
                        "incidentId": row[0],
                        "subscriptionId": row[1],
                        "integrationScenario": row[2],
                        "errorType": row[3],
                        "errorMessage": row[4],
                        "time": row[5].isoformat() if row[5] is not None else None,
                        "rootCause": row[6] or None,
                        "status": row[7] or "FAILED",
                        "created": row[8].isoformat() if row[8] is not None else None,
                    }
                    for row in rows
                ]
            finally:
                cur.close()
                conn.close()

        def _read_overview_from_hana(top: int) -> Dict[str, Any]:
            table = _hana_table_name()
            schema = str(self.settings.hana_schema).strip().strip('"')
            conn = _hana_connection()
            cur = conn.cursor()
            try:
                cur.execute(f'SET SCHEMA "{schema}"')

                cur.execute(f'SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM "{table}"')
                total_flows = int(cur.fetchone()[0] or 0)
                cur.execute(f'SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM "{table}" WHERE UPPER(COALESCE(STATUS, \'\')) IN (\'FAILED\', \'ERROR\')')
                error_flows = int(cur.fetchone()[0] or 0)
                cur.execute(f'SELECT COUNT(DISTINCT WORKFLOW_NAME) FROM "{table}" WHERE AUTO_FIX_SUCCESS = TRUE')
                fixed_flows = int(cur.fetchone()[0] or 0)

                cur.execute(
                    f"""
                    SELECT COALESCE(STATUS, 'UNKNOWN') AS S, COUNT(*)
                    FROM "{table}"
                    GROUP BY COALESCE(STATUS, 'UNKNOWN')
                    ORDER BY COUNT(*) DESC
                    """
                )
                status_breakdown = [{"status": r[0], "count": int(r[1] or 0)} for r in cur.fetchall()]

                cur.execute(
                    f"""
                    SELECT COALESCE(ERROR_CATEGORY, ERROR_CODE, 'UNKNOWN') AS E, COUNT(*)
                    FROM "{table}"
                    GROUP BY COALESCE(ERROR_CATEGORY, ERROR_CODE, 'UNKNOWN')
                    ORDER BY COUNT(*) DESC
                    """
                )
                error_distribution = [{"error_type": r[0], "count": int(r[1] or 0)} for r in cur.fetchall()]

                cur.execute(
                    f"""
                    SELECT TOP 10 WORKFLOW_NAME, COUNT(*)
                    FROM "{table}"
                    WHERE UPPER(COALESCE(STATUS, '')) IN ('FAILED', 'ERROR')
                    GROUP BY WORKFLOW_NAME
                    ORDER BY COUNT(*) DESC
                    """
                )
                top_iflows = [{"iflow_name": r[0] or "-", "failure_count": int(r[1] or 0)} for r in cur.fetchall()]

                cur.execute(
                    f"""
                    SELECT TOP {int(top)}
                        WORKFLOW_NAME,
                        COALESCE(ERROR_CODE, ERROR_CATEGORY),
                        ERROR_MESSAGE,
                        COALESCE(UPDATED_AT, CREATED_AT),
                        NULL,
                        COALESCE(STATUS, 'UNKNOWN'),
                        INCIDENT_ID
                    FROM "{table}"
                    WHERE ERROR_MESSAGE IS NOT NULL
                    ORDER BY COALESCE(UPDATED_AT, CREATED_AT) DESC
                    """
                )
                error_messages = [
                    {
                        "integrationScenario": r[0],
                        "errorType": r[1],
                        "errorMessage": r[2],
                        "time": r[3].isoformat() if r[3] is not None else None,
                        "resourceId": r[4],
                        "status": r[5],
                        "runId": r[6],
                    }
                    for r in cur.fetchall()
                ]

                return {
                    "kpi": {
                        "total_flows": total_flows,
                        "error_flows": error_flows,
                        "fixed_flows": fixed_flows,
                        "total_logs": sum(s["count"] for s in status_breakdown),
                        "total_error_messages": len(error_messages),
                    },
                    "status_breakdown": status_breakdown,
                    "error_distribution": error_distribution,
                    "top_iflows": top_iflows,
                    "timeline": [],
                    "error_messages": error_messages,
                }
            finally:
                cur.close()
                conn.close()

        def _query_log_analytics(query: str, *, hours: int) -> Dict[str, Any]:
            workspace_id = (self.settings.log_analytics_workspace_id or "").strip()
            if not workspace_id:
                raise HTTPException(
                    status_code=400,
                    detail="LOG_ANALYTICS_WORKSPACE_ID is not configured",
                )
            tenant_id = (self.settings.tenant_id or "").strip()
            client_id = (self.settings.client_id or "").strip()
            client_secret = (self.settings.client_secret or "").strip()
            if not (tenant_id and client_id and client_secret):
                raise HTTPException(
                    status_code=400,
                    detail="AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are required",
                )

            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            query_url = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"
            token_form = {
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://api.loganalytics.io/.default",
                "grant_type": "client_credentials",
            }
            try:
                token_resp = requests.post(
                    token_url,
                    data=token_form,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30,
                )
                token_resp.raise_for_status()
                access_token = token_resp.json().get("access_token", "")
                if not access_token:
                    raise RuntimeError("Azure AD token response missing access_token")

                logs_resp = requests.post(
                    query_url,
                    json={"query": query},
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    },
                    timeout=45,
                )
                if not logs_resp.ok:
                    body_preview = (logs_resp.text or "")[:1500]
                    raise RuntimeError(
                        f"Log Analytics query failed: status={logs_resp.status_code} body={body_preview}"
                    )
                return logs_resp.json()
            except HTTPException:
                raise
            except Exception as ex:
                logger.error("Failed to query Log Analytics: %s", ex)
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to fetch logs from Azure Monitor: {ex}",
                ) from ex
        
        @app.get("/")
        async def root():
            return {
                "service": "Logic Apps Auto-Remediation",
                "version": "2.0.0",
                "orchestrator_available": ORCHESTRATOR_AVAILABLE,
                "endpoints": [
                    "/health",
                    "/stats",
                    "/remediate",
                    "/remediate/run",
                    "/monitor/status",
                    "/monitor/start",
                    "/monitor/stop",
                ]
            }
        
        @app.get("/health")
        async def health():
            """Health check endpoint."""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "orchestrator_available": ORCHESTRATOR_AVAILABLE,
                "log_analytics_configured": bool(self.settings.log_analytics_workspace_id),
            }
        
        @app.get("/stats")
        async def get_stats():
            """Get remediation statistics."""
            if self.monitor:
                return {
                    "remediated_runs": len(self.monitor.tracker.remediated_runs),
                    "tracked_workflows": len(self.monitor.tracker.workflow_versions),
                    "monitor_running": self.monitor._is_running if self.monitor else False,
                }
            return {"message": "Monitor not initialized"}

        @app.get("/incidents")
        async def get_incidents(
            top: int = Query(100, ge=1, le=100),
        ):
            """
            Return dashboard incidents from HANA DB only.
            """
            try:
                return _read_incidents_from_hana(top)
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f"Failed to read incidents from HANA: {ex}") from ex

        @app.get("/logs/overview")
        async def get_logs_overview(
            top: int = Query(1000, ge=1, le=5000),
        ):
            """
            Return dashboard overview from HANA DB only.
            """
            try:
                return _read_overview_from_hana(top)
            except Exception as ex:
                raise HTTPException(status_code=500, detail=f"Failed to read overview from HANA: {ex}") from ex
        
        @app.post("/remediate")
        async def remediate_all(background_tasks: BackgroundTasks):
            """Manually trigger remediation for all pending failed runs."""
            if not self.monitor:
                raise HTTPException(status_code=400, detail="Monitor not initialized")
            
            background_tasks.add_task(self.monitor._poll_and_remediate)
            return {"status": "started", "message": "Remediation triggered in background"}
        
        @app.post("/remediate/run")
        async def remediate_run(
            workflow_name: str,
            run_id: str,
            subscription_id: Optional[str] = None,
            resource_group: Optional[str] = None,
        ):
            """Remediate a specific failed run."""
            sub_id = subscription_id or self.settings.subscription_id
            rg = resource_group or self.settings.resource_group
            
            if not sub_id or not rg:
                raise HTTPException(status_code=400, detail="Missing subscription_id or resource_group")
            
            if self.monitor and self.monitor.use_orchestrator:
                result = orchestrator_remediation(
                    workflow_name=workflow_name,
                    run_id=run_id,
                    subscription_id=sub_id,
                    resource_group=rg,
                    settings=self.settings,
                    backup_dir=self.monitor.backup_dir,
                )
            else:
                result = run_remediation(
                    subscription_id=sub_id,
                    resource_group=rg,
                    workflow_name=workflow_name,
                    run_id=run_id,
                    settings=self.settings,
                    backup_dir=self.monitor.backup_dir if self.monitor else None,
                )
            
            return result
        
        @app.get("/monitor/status")
        async def monitor_status():
            """Get monitor status."""
            if not self.monitor:
                raise HTTPException(status_code=400, detail="Monitor not initialized")
            
            return {
                "is_running": self.monitor._is_running,
                "poll_interval_seconds": self.monitor.poll_interval,
                "lookback_hours": self.monitor.lookback_hours,
                "use_orchestrator": self.monitor.use_orchestrator,
                "remediated_runs_count": len(self.monitor.tracker.remediated_runs),
            }
        
        @app.post("/monitor/start")
        async def start_monitor(background_tasks: BackgroundTasks):
            """Start the continuous monitor."""
            if not self.monitor:
                raise HTTPException(status_code=400, detail="Monitor not initialized")
            
            if self.monitor._is_running:
                return {"status": "already_running"}
            
            background_tasks.add_task(self.monitor.start)
            return {"status": "started", "message": "Monitor starting in background"}
        
        @app.post("/monitor/stop")
        async def stop_monitor():
            """Stop the continuous monitor."""
            if not self.monitor or not self.monitor._is_running:
                return {"status": "not_running"}
            
            self.monitor.stop()
            return {"status": "stopped"}


def run_once(settings: Settings) -> int:
    """Run remediation once and exit."""
    logger.info("Running single remediation pass...")
    
    monitor = ContinuousMonitor(
        settings=settings,
        poll_interval_seconds=60,
        lookback_hours=settings.lookback_hours,
        use_orchestrator=True,
        backup_dir=None,
    )
    
    monitor._poll_and_remediate()
    return 0


def run_daemon(settings: Settings) -> None:
    """Run as a daemon with continuous monitoring."""
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        if monitor:
            monitor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create monitor
    monitor = ContinuousMonitor(
        settings=settings,
        poll_interval_seconds=60,
        lookback_hours=settings.lookback_hours,
        use_orchestrator=True,
        backup_dir=None,
    )
    
    # Start monitor (blocks until stopped)
    monitor.start()


def run_server(settings: Settings) -> None:
    """Run only the API server (no background monitoring)."""
    monitor = ContinuousMonitor(
        settings=settings,
        poll_interval_seconds=60,
        lookback_hours=settings.lookback_hours,
        use_orchestrator=True,
        backup_dir=None,
    )
    
    api = RemediationAPI(settings, monitor)
    app = api.create_app()
    
    logger.info("Starting API server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


def run_full(settings: Settings) -> None:
    """
    Run full system: API server + background monitor.
    
    The monitor runs in a separate thread while the API server handles requests.
    """
    import threading
    
    # Create monitor
    monitor = ContinuousMonitor(
        settings=settings,
        poll_interval_seconds=60,
        lookback_hours=settings.lookback_hours,
        use_orchestrator=True,
        backup_dir=None,
    )
    
    # Start monitor in background thread
    monitor_thread = threading.Thread(target=monitor.start, daemon=True)
    monitor_thread.start()
    
    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        monitor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start API server
    api = RemediationAPI(settings, monitor)
    app = api.create_app()
    
    logger.info("=" * 60)
    logger.info("🚀 Logic Apps Auto-Remediation System")
    logger.info("   Monitor running in background")
    logger.info("   API server: http://0.0.0.0:8000")
    logger.info("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Azure Logic Apps Auto-Remediation System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py                          # Run full system (monitor + API)
    python main.py --once                   # Run once and exit
    python main.py --daemon                 # Run as daemon (monitor only)
    python main.py --server-only            # Run API server only
    python main.py --legacy <workflow> <run_id>  # Legacy single remediation
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--once", action="store_true", help="Run once and exit")
    mode_group.add_argument("--daemon", action="store_true", help="Run as daemon (monitor only)")
    mode_group.add_argument("--server-only", action="store_true", help="Run API server only")
    
    # Legacy mode
    parser.add_argument("--legacy", action="store_true", help="Legacy mode: remediate single run")
    parser.add_argument("-w", "--workflow", help="Workflow name (legacy mode)")
    parser.add_argument("-r", "--run-id", help="Run ID (legacy mode)")
    parser.add_argument("-s", "--subscription-id", help="Subscription ID (legacy mode)")
    parser.add_argument("-g", "--resource-group", help="Resource group (legacy mode)")
    
    # General options
    parser.add_argument(
        "--backup-dir",
        default=None,
        help="Optional backup directory (disabled by default)",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load settings
    settings = get_settings()
    
    # Legacy mode
    if args.legacy:
        if not all([args.workflow, args.run_id, args.subscription_id, args.resource_group]):
            print("Error: --legacy requires -w, -r, -s, -g")
            return 1
        
        result = run_remediation(
            subscription_id=args.subscription_id,
            resource_group=args.resource_group,
            workflow_name=args.workflow,
            run_id=args.run_id,
            settings=settings,
            backup_dir=args.backup_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("status") in ("remediated", "no_error") else 1
    
    # Normal modes
    if args.once:
        return run_once(settings)
    elif args.daemon:
        run_daemon(settings)
        return 0
    elif args.server_only:
        run_server(settings)
        return 0
    else:
        # Full mode: monitor + API server
        run_full(settings)
        return 0


if __name__ == "__main__":
    sys.exit(main())