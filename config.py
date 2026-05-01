"""Load settings from environment and optional .env file."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


@dataclass
class Settings:
    # Azure credentials (for remediation)
    tenant_id: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    fallback_http_url: str
    auth_header_name: str
    auth_header_value: str
    http_timeout_iso: str
    
    # Azure OpenAI (optional)
    azure_openai_endpoint: Optional[str]
    azure_openai_api_key: Optional[str]
    azure_openai_deployment: str
    azure_openai_api_version: str
    azure_api_runs_version: str
    azure_api_workflow_version: str
    azure_api_trigger_run_version: str
    max_remediation_attempts: int
    rag_enabled: bool
    rag_top_k: int
    rag_embedding_deployment: Optional[str]
    rag_knowledge_path: Optional[str]
    
    # Multi-flow settings
    subscription_id: Optional[str]
    resource_group: Optional[str]
    log_analytics_workspace_id: Optional[str]
    multi_flow_enabled: bool
    lookback_hours: int
    top_n_runs: int
    max_concurrency: int
    schedule_minutes: int
    log_only: bool
    log_level: str
    
    # ========== ADD THESE MISSING FIELDS ==========
    
    # Database (PostgreSQL - legacy)
    db_connection_string: str
    
    # SAP AI Core
    sap_auth_url: Optional[str]
    sap_client_id: Optional[str]
    sap_client_secret: Optional[str]
    sap_base_url: Optional[str]
    sap_resource_group: Optional[str]
    sap_embedding_model: str
    sap_chat_deployment_id: Optional[str]
    
    # HANA Database (Knowledge Base)
    hana_host: str
    hana_port: int
    hana_user: str
    hana_password: str
    hana_schema: str
    hana_table: str
    
    # Embedding Model (SAP AI Core)
    embedding_deployment_id: str
    vector_dimension: int


def get_settings() -> Settings:
    return Settings(
        # Azure credentials
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        fallback_http_url=os.getenv(
            "REMEDIATION_FALLBACK_HTTP_URL", "https://httpbin.org/status/200"
        ),
        auth_header_name=os.getenv("REMEDIATION_AUTH_HEADER_NAME", "Authorization"),
        auth_header_value=os.getenv("REMEDIATION_AUTH_HEADER_VALUE", ""),
        http_timeout_iso=os.getenv("REMEDIATION_HTTP_TIMEOUT", "PT2M"),
        
        # Azure OpenAI
        azure_openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        azure_openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_openai_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
        azure_openai_api_version=os.getenv(
            "AZURE_OPENAI_API_VERSION", "2024-02-15-preview"
        ),
        azure_api_runs_version=os.getenv("AZURE_API_RUNS_VERSION", "2019-05-01"),
        azure_api_workflow_version=os.getenv("AZURE_API_WORKFLOW_VERSION", "2019-05-01"),
        azure_api_trigger_run_version=os.getenv("AZURE_API_TRIGGER_RUN_VERSION", "2016-06-01"),
        max_remediation_attempts=max(1, int(os.getenv("MAX_REMEDIATION_ATTEMPTS", "2"))),
        rag_enabled=_env_bool("RAG_ERROR_ANALYSIS", False),
        rag_top_k=max(1, int(os.getenv("RAG_TOP_K", "5"))),
        rag_embedding_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
        rag_knowledge_path=os.getenv("RAG_KNOWLEDGE_PATH"),
        
        # Multi-flow
        subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        resource_group=os.getenv("AZURE_RESOURCE_GROUP"),
        log_analytics_workspace_id=os.getenv("LOG_ANALYTICS_WORKSPACE_ID"),
        multi_flow_enabled=_env_bool("MULTI_FLOW_ENABLED", False),
        # Faster defaults for day-to-day RCA runs; still fully overridable via env/CLI.
        lookback_hours=_env_int("LOOKBACK_HOURS", 24),
        top_n_runs=_env_int("TOP_N_RUNS", 5),
        max_concurrency=_env_int("MAX_CONCURRENCY", 6),
        schedule_minutes=_env_int("SCHEDULE_MINUTES", 0),
        log_only=_env_bool("LOG_ONLY", False),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        
        # ========== ADD THESE VALUES ==========
        
        # PostgreSQL (legacy)
        db_connection_string=os.getenv("DB_CONNECTION_STRING", ""),
        
        # SAP AI Core
        sap_auth_url=os.getenv("AICORE_AUTH_URL"),
        sap_client_id=os.getenv("AICORE_CLIENT_ID"),
        sap_client_secret=os.getenv("AICORE_CLIENT_SECRET"),
        sap_base_url=os.getenv("AICORE_BASE_URL"),
        sap_resource_group=os.getenv("AICORE_RESOURCE_GROUP"),
        sap_embedding_model=os.getenv("AICORE_EMBEDDING_MODEL", "text-embedding-ada-002"),
        sap_chat_deployment_id=os.getenv("AICORE_CHAT_DEPLOYMENT_ID"),
        
        # HANA Database (Knowledge Base)
        hana_host=os.getenv("HANA_HOST", ""),
        hana_port=int(os.getenv("HANA_PORT", "443")),
        hana_user=os.getenv("HANA_USER", ""),
        hana_password=os.getenv("HANA_PASSWORD", ""),
        hana_schema=os.getenv("HANA_SCHEMA", ""),
        hana_table=os.getenv("HANA_TABLE", "LOGIC_APPS_KNOWLEDGE"),
        
        # Embedding Model
        embedding_deployment_id=os.getenv("EMBEDDING_DEPLOYMENT_ID", ""),
        vector_dimension=int(os.getenv("VECTOR_DIMENSION", "3072")),
    )