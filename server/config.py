# server/config.py
"""
Application configuration loaded from environment variables.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from typing import Optional

# Try to load .env from current directory or parent directory
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Load from manifest.yaml if env vars are missing
import yaml

manifest_path = Path(__file__).parent.parent / "manifest.yaml"

if manifest_path.exists():
    with open(manifest_path, "r") as f:
        manifest = yaml.safe_load(f)

    app = manifest["applications"][0]
    env_vars = app.get("env", {})

    for key, value in env_vars.items():
        if not os.getenv(key):
            os.environ[key] = str(value)

class Settings(BaseSettings):
    # Azure AD (service principal)
    AZURE_TENANT_ID: Optional[str] = None
    AZURE_CLIENT_ID: Optional[str] = None
    AZURE_CLIENT_SECRET: Optional[str] = None

    # Azure subscription / Logic Apps
    AZURE_SUBSCRIPTION_ID: Optional[str] = None
    AZURE_RESOURCE_GROUP: Optional[str] = None
    LOG_ANALYTICS_WORKSPACE_ID: Optional[str] = None

    # Azure API versions
    AZURE_API_RUNS_VERSION: str = "2019-05-01"
    AZURE_API_WORKFLOW_VERSION: str = "2019-05-01"
    AZURE_API_TRIGGER_RUN_VERSION: str = "2019-05-01"

    # Remediation settings
    MAX_REMEDIATION_ATTEMPTS: int = 2
    FALLBACK_HTTP_URL: str = "https://httpbin.org/status/200"
    HTTP_TIMEOUT_ISO: str = "PT2M"

    # SAP AI Core
    AICORE_AUTH_URL: Optional[str] = None
    AICORE_CLIENT_ID: Optional[str] = None
    AICORE_CLIENT_SECRET: Optional[str] = None
    AICORE_BASE_URL: Optional[str] = None
    AICORE_RESOURCE_GROUP: Optional[str] = None
    AICORE_CHAT_DEPLOYMENT_ID: Optional[str] = None

    # HANA Database
    HANA_HOST: str = ""
    HANA_PORT: int = 443
    HANA_USER: str = ""
    HANA_PASSWORD: str = ""
    HANA_SCHEMA: str = ""
    HANA_TABLE: str = "LOGIC_APPS_KNOWLEDGE"
    HANA_OBSERVABILITY_TABLE: str = "LOGIC_APPS_OBSERVABILITY"

    # Embeddings (for HANA vector search)
    EMBEDDING_DEPLOYMENT_ID: str = ""
    VECTOR_DIMENSION: int = 3072

    # Multi‑flow settings
    LOOKBACK_HOURS: float = 0.3
    ENABLE_AUTO_MONITOR: bool = True 
    # Tracker settings
    DRY_RUN: bool = False
    TRACKER_RETENTION_DAYS: int = 90
    TRACKER_MAX_RETRY_COUNT: int = 2

    # Knowledge scraper settings
    KNOWLEDGE_CHUNK_SIZE: int = 1200
    KNOWLEDGE_CHUNK_OVERLAP: int = 50
    KNOWLEDGE_SCRAPE_BATCH_SIZE: int = 3
    KNOWLEDGE_SCRAPE_TIMEOUT: float = 45.0
    KNOWLEDGE_SKIP_SLOW_URLS: bool = True
    KNOWLEDGE_MICROSOFT_LEARN_URLS: str = ""

    # Event Mesh agent pipeline (5 queues)
    EVENT_MESH_PIPELINE_ENABLED: bool = True
    PIPELINE_API_BASE: str = "http://127.0.0.1:8000"
    EVENT_MESH_PUBLISH_ENABLED: bool = False
    EVENT_MESH_PUBLISH_URL: Optional[str] = None
    EVENT_MESH_TOKEN: Optional[str] = None
    EVENT_MESH_QUEUE_OBSERVER: Optional[str] = None
    EVENT_MESH_QUEUE_CLASSIFIER: Optional[str] = None
    EVENT_MESH_QUEUE_RCA: Optional[str] = None
    EVENT_MESH_QUEUE_FIXER: Optional[str] = None
    EVENT_MESH_QUEUE_VERIFIER: Optional[str] = None

    class Config:
        # Don't load from .env again (we already loaded manually)
        env_file = None
        extra = "ignore"


settings = Settings()

def get_settings() -> Settings:
    """Return singleton settings instance."""
    return settings