import os
import json
import time
import logging
from functools import lru_cache
from threading import Lock

import httpx
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

CATALOG         = os.environ.get("DATABRICKS_CATALOG", "")
SCHEMA          = os.environ.get("DATABRICKS_SCHEMA", "bom_parser")
VOLUME          = "electrical_diagrams"
TABLE_NAME      = f"{CATALOG}.{SCHEMA}.bom_extractions"
VOLUME_PATH     = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
OVERLAY_PATH    = f"{VOLUME_PATH}/overlays"
EXTRACTION_JOB_ID = int(os.environ.get("EXTRACTION_JOB_ID", "0"))
MATCHING_JOB_ID   = int(os.environ.get("MATCHING_JOB_ID",   "0"))
MATCHES_TABLE     = f"{CATALOG}.{SCHEMA}.reference_matches"
EXPORTS_TABLE     = f"{CATALOG}.{SCHEMA}.exports"
EXPORTS_PATH      = f"{VOLUME_PATH}/exports"


class AppConfig:
    def __init__(self):
        self.agent_endpoint_name = os.environ.get("AGENT_ENDPOINT_NAME", "sld-bom-agent")
        self.warehouse_id        = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        self.catalog             = CATALOG
        self.schema              = SCHEMA
        self.table_name          = TABLE_NAME
        self.volume_path         = VOLUME_PATH
        self.overlay_path        = OVERLAY_PATH
        self.extraction_job_id   = EXTRACTION_JOB_ID


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return AppConfig()


@lru_cache(maxsize=1)
def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


# In-memory store: file_name → run_id (for active extractions)
_run_id_store: dict[str, int] = {}
_run_id_lock = Lock()


def set_run_id(file_name: str, run_id: int) -> None:
    with _run_id_lock:
        _run_id_store[file_name] = run_id


def get_run_id(file_name: str) -> int | None:
    with _run_id_lock:
        return _run_id_store.get(file_name)


def clear_run_id(file_name: str) -> None:
    with _run_id_lock:
        _run_id_store.pop(file_name, None)


def exec_sql(sql: str, max_rows: int = 500) -> list[dict]:
    """Execute SQL via Statement Execution API. Returns list of row dicts."""
    config = get_config()
    w      = get_workspace_client()

    host   = w.config.host.rstrip("/")
    hdrs   = {**w.config.authenticate(), "Content-Type": "application/json"}
    url    = f"{host}/api/2.0/sql/statements"

    payload = {
        "warehouse_id": config.warehouse_id,
        "statement":    sql,
        "wait_timeout": "30s",
        "on_wait_timeout": "CONTINUE",
        "format":       "JSON_ARRAY",
        "row_limit":    max_rows,
    }

    with httpx.Client(timeout=60) as client:
        resp = client.post(url, headers=hdrs, json=payload)
        resp.raise_for_status()
        data = resp.json()

        # Poll if PENDING/RUNNING
        statement_id = data.get("statement_id")
        for _ in range(60):
            state = data.get("status", {}).get("state", "")
            if state in ("SUCCEEDED", "FAILED", "CLOSED", "CANCELED"):
                break
            if state in ("PENDING", "RUNNING") and statement_id:
                time.sleep(1)
                poll = client.get(f"{url}/{statement_id}", headers=hdrs)
                poll.raise_for_status()
                data = poll.json()
            else:
                break

        status = data.get("status", {})
        if status.get("state") != "SUCCEEDED":
            err = status.get("error", {}).get("message", "SQL failed")
            raise RuntimeError(f"SQL error: {err}")

        result  = data.get("result", {})
        columns = [c["name"] for c in (data.get("manifest", {}).get("schema", {}).get("columns") or [])]
        rows    = result.get("data_array") or []
        return [dict(zip(columns, row)) for row in rows]
