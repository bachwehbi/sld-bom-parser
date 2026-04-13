# Databricks notebook source
"""
SLD-to-BOM Agent — MLflow code-based model file (MLflow 3).

This file is the entry point for mlflow.pyfunc.log_model(python_model=<path>).
MLflow loads this file, finds mlflow.models.set_model(), and uses that object.

The agent is intentionally READ-ONLY + JOB-TRIGGER:
  - list_unprocessed_files  → SQL LIST query (fast, read-only)
  - trigger_extraction      → calls jobs.run_now() and returns run_id immediately
  - get_job_status          → polls jobs.runs.get() for current state
  - query_results           → SQL SELECT against bom_extractions (fast, read-only)
  - get_overlay_path        → pure logic (no I/O)

Extraction runs in a Databricks Job (sld-bom-extraction).
Job IDs and the SQL warehouse ID are read from environment variables at serving time
(set via the serving endpoint config or by DAB/setup.py at deploy time).
The web application polls the Jobs REST API and/or bom_extractions.progress_msg
for live status — the agent never waits for jobs to complete.
"""

import os
import json

import mlflow
from mlflow.pyfunc import ResponsesAgent

# ── Config ────────────────────────────────────────────────────────────────────
# This file is loaded by MLflow in isolation at serving time and cannot use config.py.
# All workspace-specific values are read from environment variables, which are set:
#   - automatically by DAB (databricks bundle deploy --target full)
#   - or manually in the serving endpoint environment config
# The fallback strings below are only used if the env vars are missing.
CATALOG      = os.environ.get("DATABRICKS_CATALOG", "")
SCHEMA       = "bom_parser"
VOLUME       = "electrical_diagrams"
TABLE_NAME   = f"{CATALOG}.{SCHEMA}.bom_extractions"
VOLUME_PATH  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
OVERLAY_PATH = f"{VOLUME_PATH}/overlays"

DEFAULT_MODEL_ENDPOINT  = "databricks-claude-sonnet-4-6"
DEFAULT_ENABLE_RETRY    = True
DEFAULT_MAX_RETRIES     = 2
DEFAULT_THRESHOLD       = 0.75
SQL_WAREHOUSE_ID  = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
EXTRACTION_JOB_ID = int(os.environ.get("EXTRACTION_JOB_ID", "0") or "0")
MATCHING_JOB_ID   = int(os.environ.get("MATCHING_JOB_ID",   "0") or "0")
MATCHES_TABLE     = f"{CATALOG}.{SCHEMA}.reference_matches"

# ── Agent system prompt ───────────────────────────────────────────────────────
AGENT_SYSTEM_PROMPT = """You are the SLD-to-BOM extraction agent for Schneider Electric electrical diagrams.
You help field engineers extract Bills of Materials from PDF Single Line Diagrams.

You have access to 9 tools:
- list_unprocessed_files: discover which PDFs haven't been processed yet
- trigger_extraction: submit an extraction job for a specific PDF (returns immediately with a run_id)
- get_job_status: check the current status of a previously submitted extraction job
- query_results: query the Delta table for extraction results or component details
- get_overlay_path: get the path to a diagram's QA overlay image
- trigger_reference_matching: submit the reference matching job for a diagram after extraction is successful
- check_stock: look up stock availability and pricing for a specific product reference
- find_alternatives: find alternative product references for a component type and specifications
- semantic_search_catalog: search the product catalog using natural language / semantic similarity (Vector Search)

IMPORTANT RULES:
1. Extraction is ASYNCHRONOUS. When a user asks to process a PDF:
   a. Confirm the parameters you understood (model, retries, threshold).
   b. Call trigger_extraction — it returns a run_id immediately.
   c. Tell the user: "Extraction submitted (run_id: <id>). The web app will show live progress.
      Ask me for a status update or results once it completes (~2 minutes)."
   d. If the user asks for status, call get_job_status(run_id).
   e. Once the job is TERMINATED/SUCCESS, call query_results to fetch and report the outcome.

2. After extraction completes, always report:
   total components extracted, match rate, attempts made, threshold met.

3. If threshold_met is False, explicitly flag it and suggest reviewing unmatched components.

4. When the user asks about specific components, use query_results with appropriate SQL.

5. Default extraction parameters when not specified:
   - model: databricks-claude-sonnet-4-6
   - enable_retry: true
   - max_retries: 2
   - threshold: 0.75

6. NEVER run extract_bom or any heavy computation inline — always use trigger_extraction.

7. Reference matching workflow:
   a. Matching is user-controlled — only trigger when explicitly requested.
   b. Use trigger_reference_matching after extraction is confirmed successful.
   c. Use check_stock to answer questions about specific product availability.
   d. Use find_alternatives when the user asks for options for a component type.
   e. Use semantic_search_catalog for free-text product searches (e.g. "40A 4-pole RCD type A").
   f. Reference results are stored in reference_matches — use query_results to fetch them.

8. Component types (canonical English, used in find_alternatives and query_results):
   circuit_breaker, rcd, contactor, timer, surge_protector, fuse_holder,
   energy_meter, ats, load_break_switch
"""


class SLDBomAgent(ResponsesAgent):
    """SLD-to-BOM agent using MLflow 3 ResponsesAgent.

    Tools: list_unprocessed_files, trigger_extraction, get_job_status,
           query_results, get_overlay_path.
    No heavy compute — extraction runs in a Databricks Job.
    """

    # ── Auth helpers ──────────────────────────────────────────────────────────

    def _workspace_auth(self):
        """Return (host, auth_headers) using MLflow's credential resolver."""
        host  = ""
        token = ""

        try:
            from mlflow.utils.databricks_utils import get_databricks_host_creds
            creds = get_databricks_host_creds()
            host  = (creds.host  or "").rstrip("/")
            token = (creds.token or "")
        except Exception:
            pass

        if not host or not token:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient()
                if not host:
                    host = (w.config.host or "").rstrip("/")
                if not token:
                    auth_h = {}
                    w.config.authenticate(auth_h)
                    bearer = auth_h.get("Authorization", "")
                    if bearer.startswith("Bearer "):
                        token = bearer[7:]
                if not token:
                    token = w.config.token or ""
            except Exception:
                pass

        if not host:
            host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        if not token:
            token = os.environ.get("DATABRICKS_TOKEN", "")

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return host, headers

    @staticmethod
    def _get_deploy_client():
        import mlflow.deployments
        return mlflow.deployments.get_deploy_client("databricks")

    # ── SQL execution ─────────────────────────────────────────────────────────

    def _exec_sql(self, sql: str, max_rows: int = 100) -> list:
        """Execute SQL via the Databricks SQL Statement Execution REST API."""
        import requests, time

        host, auth_headers = self._workspace_auth()
        if not host or not auth_headers.get("Authorization"):
            return [{"error": "Workspace credentials unavailable"}]

        try:
            stmt_resp = requests.post(
                f"{host}/api/2.0/sql/statements",
                headers=auth_headers,
                json={
                    "warehouse_id": SQL_WAREHOUSE_ID,
                    "statement":    sql,
                    "wait_timeout": "50s",
                    "disposition":  "INLINE",
                },
                timeout=60,
            )
            result = stmt_resp.json()
        except Exception as e:
            return [{"error": f"SQL request failed: {e}"}]

        stmt_id = result.get("statement_id")
        state   = result.get("status", {}).get("state", "FAILED")
        if state in ("PENDING", "RUNNING") and stmt_id:
            for _ in range(30):
                time.sleep(2)
                try:
                    poll   = requests.get(
                        f"{host}/api/2.0/sql/statements/{stmt_id}",
                        headers=auth_headers, timeout=15,
                    )
                    result = poll.json()
                    state  = result.get("status", {}).get("state", "FAILED")
                except Exception:
                    break
                if state not in ("PENDING", "RUNNING"):
                    break

        if state != "SUCCEEDED":
            err = result.get("status", {}).get("error", {})
            return [{"error": err.get("message", f"SQL state={state}")}]

        columns    = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        data_array = result.get("result", {}).get("data_array") or []
        return [dict(zip(columns, row)) for row in data_array[:max_rows]]

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_list_unprocessed_files(self):
        """List PDFs in the volume not yet successfully processed."""
        try:
            rows     = self._exec_sql(f"LIST '{VOLUME_PATH}'", max_rows=1000)
            all_pdfs = sorted([
                r["name"]
                for r in rows
                if isinstance(r, dict)
                and "error" not in r
                and str(r.get("name", "")).lower().endswith(".pdf")
            ])
        except Exception as e:
            return {"error": f"Failed to list volume: {e}"}

        done_rows = self._exec_sql(
            f"SELECT file_name FROM {TABLE_NAME} WHERE status IN ('SUCCESS', 'IN_PROGRESS')"
        )
        processed_in_db = {
            r.get("file_name", "")
            for r in done_rows
            if isinstance(r, dict) and "error" not in r
        }

        unprocessed = [f for f in all_pdfs if f not in processed_in_db]
        processed_in_volume = [f for f in all_pdfs if f in processed_in_db]
        return {
            "total_pdfs_in_volume": len(all_pdfs),
            "processed_count":      len(processed_in_volume),
            "unprocessed_count":    len(unprocessed),
            "unprocessed_files":    unprocessed,
        }

    def _tool_trigger_extraction(self, file_name, model=None, enable_retry=None,
                                  max_retries=None, threshold=None):
        """Submit an extraction job and return the run_id immediately."""
        import requests

        model        = model or DEFAULT_MODEL_ENDPOINT
        enable_retry = enable_retry if enable_retry is not None else DEFAULT_ENABLE_RETRY
        max_retries  = max_retries  if max_retries  is not None else DEFAULT_MAX_RETRIES
        threshold    = threshold    if threshold     is not None else DEFAULT_THRESHOLD

        host, auth_headers = self._workspace_auth()
        if not host or not auth_headers.get("Authorization"):
            return {"error": "Workspace credentials unavailable — cannot trigger job"}

        payload = {
            "job_id": EXTRACTION_JOB_ID,
            "notebook_params": {
                "file_name":     file_name,
                "model":         model,
                "enable_retry":  str(enable_retry).lower(),
                "max_retries":   str(max_retries),
                "threshold":     str(threshold),
            },
        }
        try:
            resp = requests.post(
                f"{host}/api/2.1/jobs/run-now",
                headers={**auth_headers, "Content-Type": "application/json"},
                json=payload,
                timeout=15,
            )
            data = resp.json()
        except Exception as e:
            return {"error": f"Failed to trigger job: {e}"}

        if "run_id" not in data:
            return {"error": f"Job trigger failed: {data}"}

        return {
            "status":       "submitted",
            "run_id":       data["run_id"],
            "file_name":    file_name,
            "model":        model,
            "enable_retry": enable_retry,
            "max_retries":  max_retries,
            "threshold":    threshold,
            "message": (
                f"Extraction job submitted for {file_name} (run_id={data['run_id']}). "
                "The web app will show live progress via bom_extractions.progress_msg. "
                "Ask me for a status update or results once it completes (~2 minutes)."
            ),
        }

    def _tool_get_job_status(self, run_id):
        """Check the current status of an extraction job by run_id."""
        import requests

        host, auth_headers = self._workspace_auth()
        if not host or not auth_headers.get("Authorization"):
            return {"error": "Workspace credentials unavailable"}

        try:
            resp = requests.get(
                f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=auth_headers,
                timeout=15,
            )
            data = resp.json()
        except Exception as e:
            return {"error": f"Failed to get job status: {e}"}

        state      = data.get("state", {})
        lifecycle  = state.get("life_cycle_state", "UNKNOWN")
        result     = state.get("result_state", "")
        msg        = state.get("state_message", "")
        start_time = data.get("start_time", 0)
        end_time   = data.get("end_time", 0)
        duration_s = round((end_time - start_time) / 1000) if end_time and start_time else None

        progress_rows = self._exec_sql(
            f"SELECT status, progress_msg FROM {TABLE_NAME} "
            f"ORDER BY processed_at DESC LIMIT 1",
            max_rows=1,
        )
        progress_msg = ""
        if progress_rows and "error" not in progress_rows[0]:
            progress_msg = progress_rows[0].get("progress_msg", "") or ""

        return {
            "run_id":       run_id,
            "lifecycle":    lifecycle,
            "result":       result,
            "message":      msg,
            "duration_s":   duration_s,
            "progress_msg": progress_msg,
            "done":         lifecycle == "TERMINATED",
            "success":      lifecycle == "TERMINATED" and result == "SUCCESS",
        }

    def _tool_query_results(self, sql_query):
        """Execute a SQL query against the Delta table."""
        return self._exec_sql(sql_query, max_rows=100)

    def _tool_get_overlay_path(self, file_name):
        """Return the volume path to a diagram's QA overlay JPEG."""
        stem = os.path.splitext(file_name)[0]
        return {"overlay_path": f"{OVERLAY_PATH}/{stem}_overlay.jpg"}

    def _tool_trigger_reference_matching(self, file_name, top_n=3, preferred_tier=""):
        """Submit the reference matching job for a diagram."""
        import requests

        host, auth_headers = self._workspace_auth()
        if not host or not auth_headers.get("Authorization"):
            return {"error": "Workspace credentials unavailable — cannot trigger job"}

        payload = {
            "job_id": MATCHING_JOB_ID,
            "notebook_params": {
                "file_name":      file_name,
                "top_n":          str(top_n),
                "preferred_tier": preferred_tier,
            },
        }
        try:
            resp = requests.post(
                f"{host}/api/2.1/jobs/run-now",
                headers={**auth_headers, "Content-Type": "application/json"},
                json=payload,
                timeout=15,
            )
            data = resp.json()
        except Exception as e:
            return {"error": f"Failed to trigger matching job: {e}"}

        if "run_id" not in data:
            return {"error": f"Job trigger failed: {data}"}

        return {
            "status":    "submitted",
            "run_id":    data["run_id"],
            "file_name": file_name,
            "top_n":     top_n,
            "message": (
                f"Reference matching job submitted for {file_name} (run_id={data['run_id']}). "
                "Results will appear in the References tab of the web app (~1 minute)."
            ),
        }

    def _tool_check_stock(self, reference):
        """Check stock availability and pricing for a specific product reference."""
        rows = self._exec_sql(f"""
            SELECT m.reference, m.product_description, m.range, m.tier, m.list_price_eur,
                   s.distribution_center, s.qty_available
            FROM {CATALOG}.{SCHEMA}.material m
            LEFT JOIN {CATALOG}.{SCHEMA}.stock s ON m.reference = s.reference
            WHERE m.reference = '{reference.replace("'", "")}'
            ORDER BY s.qty_available DESC NULLS LAST
        """, max_rows=10)

        if not rows or (len(rows) == 1 and "error" in rows[0]):
            return {"error": f"Reference {reference} not found in catalog"}

        # Aggregate stock across DCs
        first = rows[0]
        dc_stock = [
            {"dc": r.get("distribution_center"), "qty": r.get("qty_available")}
            for r in rows
            if r.get("distribution_center")
        ]
        total_qty = sum(int(r.get("qty_available") or 0) for r in rows if r.get("qty_available"))

        return {
            "reference":           first.get("reference"),
            "product_description": first.get("product_description"),
            "range":               first.get("range"),
            "tier":                first.get("tier"),
            "list_price_eur":      first.get("list_price_eur"),
            "total_qty_available": total_qty,
            "stock_by_dc":         dc_stock,
            "in_stock":            total_qty >= 10,
        }

    def _tool_find_alternatives(self, component_type, calibre_a=None, poles=None, tier=None):
        """Find alternative product references for a component type and specs."""
        safe_type = component_type.replace("'", "")
        # Use component_type_en (canonical English) column added in Phase 2
        conditions = [f"component_type_en = '{safe_type}'"]
        if tier:
            conditions.append(f"tier = '{tier.replace(chr(39), '')}'")

        where = " AND ".join(conditions)
        rows = self._exec_sql(f"""
            SELECT m.reference, m.product_description, m.range, m.tier,
                   m.list_price_eur, m.properties,
                   COALESCE(SUM(s.qty_available), 0) AS total_stock
            FROM {CATALOG}.{SCHEMA}.material m
            LEFT JOIN {CATALOG}.{SCHEMA}.stock s ON m.reference = s.reference
            WHERE {where} AND m.status != 'DISCONTINUED'
            GROUP BY m.reference, m.product_description, m.range, m.tier,
                     m.list_price_eur, m.properties
            ORDER BY total_stock DESC, m.tier
            LIMIT 10
        """, max_rows=10)

        if not rows:
            return {"error": f"No references found for component_type='{component_type}'"}

        # Apply calibre/poles filter post-query (properties is JSON)
        results = []
        for r in rows:
            props = {}
            try:
                props = json.loads(r.get("properties") or "{}")
            except Exception:
                pass
            if calibre_a is not None:
                cand_cal = props.get("calibre_A")
                if cand_cal is not None and float(str(cand_cal)) != float(calibre_a):
                    continue
            if poles is not None:
                cand_pol = props.get("poles")
                if cand_pol is not None and int(str(cand_pol)) != int(poles):
                    continue
            results.append({
                "reference":           r.get("reference"),
                "product_description": r.get("product_description"),
                "range":               r.get("range"),
                "tier":                r.get("tier"),
                "list_price_eur":      r.get("list_price_eur"),
                "total_stock":         r.get("total_stock"),
                "calibre_A":           props.get("calibre_A"),
                "poles":               props.get("poles"),
            })

        return {"component_type": component_type, "alternatives": results[:5]}

    def _tool_semantic_search_catalog(self, query, top_k=5, component_type_en=None):
        """Semantic search in the product catalog using Databricks Vector Search."""
        VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.material_vs_index"
        VS_COLUMNS = [
            "reference", "component_type_en", "product_description",
            "product_long_description", "range", "tier", "status",
            "list_price_eur", "properties",
        ]

        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            kwargs = dict(
                index_name=VS_INDEX_NAME,
                query_text=query,
                num_results=int(top_k),
                columns=VS_COLUMNS,
            )
            if component_type_en:
                kwargs["filters_json"] = json.dumps({"component_type_en": [component_type_en]})
            resp = w.vector_search.query_index(**kwargs)
            col_names = [c.name for c in (resp.manifest.columns or [])]
            rows = resp.result.data_array or []
            candidates = [dict(zip(col_names, row)) for row in rows]
        except Exception as e:
            return {"error": f"VS search failed: {e}"}

        if not candidates:
            return {"candidates": [], "message": "No results found in catalog"}

        # Enrich with stock info
        refs_sql = ", ".join(
            f"'{c.get('reference', '').replace(chr(39), '')}'"
            for c in candidates if c.get("reference")
        )
        stock_map: dict[str, int] = {}
        if refs_sql:
            stock_rows = self._exec_sql(
                f"SELECT reference, SUM(qty_available) AS total_qty "
                f"FROM {CATALOG}.{SCHEMA}.stock "
                f"WHERE reference IN ({refs_sql}) GROUP BY reference",
                max_rows=50,
            )
            for s in stock_rows:
                if "error" not in s:
                    stock_map[s.get("reference", "")] = int(s.get("total_qty") or 0)

        results = []
        for c in candidates:
            ref = c.get("reference", "")
            try:
                props = json.loads(c.get("properties") or "{}")
            except Exception:
                props = {}
            results.append({
                "reference":               ref,
                "product_description":     c.get("product_description"),
                "product_long_description": c.get("product_long_description"),
                "range":                   c.get("range"),
                "tier":                    c.get("tier"),
                "status":                  c.get("status"),
                "list_price_eur":          c.get("list_price_eur"),
                "total_stock":             stock_map.get(ref, 0),
                "in_stock":                stock_map.get(ref, 0) >= 10,
                "properties":              props,
            })

        return {"query": query, "candidates": results}

    # ── Tool dispatch ─────────────────────────────────────────────────────────

    def _dispatch_tool(self, tool_name, tool_args):
        if tool_name == "list_unprocessed_files":
            return self._tool_list_unprocessed_files()
        elif tool_name == "trigger_extraction":
            return self._tool_trigger_extraction(**tool_args)
        elif tool_name == "get_job_status":
            return self._tool_get_job_status(**tool_args)
        elif tool_name == "query_results":
            return self._tool_query_results(**tool_args)
        elif tool_name == "get_overlay_path":
            return self._tool_get_overlay_path(**tool_args)
        elif tool_name == "trigger_reference_matching":
            return self._tool_trigger_reference_matching(**tool_args)
        elif tool_name == "check_stock":
            return self._tool_check_stock(**tool_args)
        elif tool_name == "find_alternatives":
            return self._tool_find_alternatives(**tool_args)
        elif tool_name == "semantic_search_catalog":
            return self._tool_semantic_search_catalog(**tool_args)
        else:
            return {"error": f"Unknown tool: {tool_name}"}

    # ── Tool definitions ──────────────────────────────────────────────────────

    TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "list_unprocessed_files",
                "description": (
                    "List PDF files in the volume that have not been successfully extracted yet. "
                    "Returns: total_pdfs_in_volume (int), processed_count (int), "
                    "unprocessed_count (int), unprocessed_files (list of filenames). "
                    "Use unprocessed_count and unprocessed_files to report pending work."
                ),
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "trigger_extraction",
                "description": (
                    "Submit an extraction job for one PDF. Returns immediately with a run_id. "
                    "The job runs asynchronously — use get_job_status to check progress."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file_name":    {"type": "string",  "description": "PDF filename (e.g. 'AVILA.pdf')"},
                        "model":        {"type": "string",  "description": f"Model endpoint. Default: {DEFAULT_MODEL_ENDPOINT}"},
                        "enable_retry": {"type": "boolean", "description": "Retry if match rate below threshold. Default: true"},
                        "max_retries":  {"type": "integer", "description": "Max additional attempts. Default: 2"},
                        "threshold":    {"type": "number",  "description": "Min acceptable match rate (0–1). Default: 0.75"},
                    },
                    "required": ["file_name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_job_status",
                "description": (
                    "Check the status of an extraction job by run_id. "
                    "Returns lifecycle state, result, duration, and latest progress_msg from the Delta table."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "integer", "description": "Job run ID returned by trigger_extraction."},
                    },
                    "required": ["run_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_results",
                "description": (
                    f"Run a SQL query against {TABLE_NAME}. "
                    "Columns: file_name STRING, file_path STRING, processed_at TIMESTAMP, "
                    "status STRING ('IN_PROGRESS'/'SUCCESS'/'ERROR'), error_message STRING, "
                    "bom_json STRING (JSON array), attempts_made INT, threshold_met BOOLEAN, "
                    "progress_msg STRING (latest progress message). "
                    "bom_json element keys: 'Que és', 'Calibre (A)', 'Polos', 'Circuito', "
                    "'precise_cx' (null=unmatched), 'match_type' (null or 'circuit_shared'). "
                    "Always use 'processed_at' not 'extraction_date', 'file_name' not 'pdf_name'."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql_query": {"type": "string", "description": "Valid Spark SQL SELECT query."},
                    },
                    "required": ["sql_query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_overlay_path",
                "description": "Return the volume path to the QA overlay JPEG for a given diagram.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file_name": {"type": "string", "description": "PDF filename."},
                    },
                    "required": ["file_name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "trigger_reference_matching",
                "description": (
                    "Submit the reference matching job for a diagram that has been successfully extracted. "
                    "Returns immediately with a run_id. Results appear in the References tab (~1 minute)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file_name":      {"type": "string",  "description": "PDF filename to match."},
                        "top_n":          {"type": "integer", "description": "Number of reference candidates per component. Default: 3"},
                        "preferred_tier": {"type": "string",  "description": "Filter by tier: economy/standard/premium or blank for all."},
                    },
                    "required": ["file_name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "check_stock",
                "description": (
                    "Check stock availability and list price for a specific Schneider Electric product reference. "
                    "Returns stock by distribution center, total quantity, and pricing."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reference": {"type": "string", "description": "Product reference code (e.g. 'A9F74216')."},
                    },
                    "required": ["reference"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "find_alternatives",
                "description": (
                    "Find alternative product references for a component type and specifications. "
                    "Useful when the user asks for options or when the top suggestion is out of stock."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "component_type": {"type": "string", "description": "Canonical English component type: circuit_breaker, rcd, contactor, timer, surge_protector, fuse_holder, energy_meter, ats, load_break_switch."},
                        "calibre_a":      {"type": "number", "description": "Ampere rating filter (optional)."},
                        "poles":          {"type": "integer","description": "Number of poles filter (optional)."},
                        "tier":           {"type": "string", "description": "Tier filter: economy/standard/premium (optional)."},
                    },
                    "required": ["component_type"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "semantic_search_catalog",
                "description": (
                    "Search the Schneider Electric product catalog using natural language / semantic similarity. "
                    "Use this to find products matching a free-text description like '40A 4-pole RCD type A selective'. "
                    "Returns ranked candidates with stock info. "
                    "Prefer this over find_alternatives for open-ended or multilingual queries."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language description of the product to find (e.g. '25A 2-pole circuit breaker C-curve 6kA economy tier').",
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Number of results to return (default: 5, max: 20).",
                        },
                        "component_type_en": {
                            "type": "string",
                            "description": "Optional filter by canonical type: circuit_breaker, rcd, contactor, timer, surge_protector, fuse_holder, energy_meter, ats, load_break_switch.",
                        },
                    },
                    "required": ["query"],
                },
            },
        },
    ]

    # ── Main predict loop ─────────────────────────────────────────────────────

    def predict(self, model_input, params=None):
        """Run the agent reasoning loop with MLflow tracing.

        model_input can be a ResponsesAgentRequest (MLflow 3) or a plain dict.
        Traces every LLM call and tool dispatch as child spans.
        """
        with mlflow.start_span(name="sld_bom_agent") as root_span:
            # ── Normalise input ────────────────────────────────────────────────
            if hasattr(model_input, "input"):
                input_data = model_input.input
            elif hasattr(model_input, "messages"):
                input_data = model_input.messages
            elif isinstance(model_input, dict):
                input_data = model_input.get("input", model_input.get("messages", []))
            else:
                input_data = []

            if isinstance(input_data, str):
                raw_messages = [{"role": "user", "content": input_data}]
            else:
                raw_messages = []
                for m in (input_data or []):
                    if isinstance(m, dict):
                        raw_messages.append(m)
                    elif hasattr(m, "role"):
                        content = m.content if isinstance(m.content, str) else str(m.content)
                        raw_messages.append({"role": m.role, "content": content})

            root_span.set_inputs({"message_count": len(raw_messages)})

            if not raw_messages:
                return {"output": [{"id": "msg_0", "type": "message", "role": "assistant",
                                    "content": [{"type": "output_text", "text": ""}]}]}

            deploy_client = self._get_deploy_client()
            conversation  = [{"role": "system", "content": AGENT_SYSTEM_PROMPT}]
            conversation.extend(raw_messages)

            final_content = "Agent loop limit reached. Please try a more specific request."

            for iteration in range(10):
                # ── LLM call ──────────────────────────────────────────────────
                with mlflow.start_span(name=f"llm_call_{iteration}") as llm_span:
                    llm_span.set_inputs({
                        "model":       DEFAULT_MODEL_ENDPOINT,
                        "turn":        iteration,
                        "msg_count":   len(conversation),
                    })
                    try:
                        response = deploy_client.predict(
                            endpoint=DEFAULT_MODEL_ENDPOINT,
                            inputs={
                                "messages":    conversation,
                                "tools":       self.TOOLS,
                                "tool_choice": "auto",
                                "temperature": 0.1,
                                "max_tokens":  4096,
                            },
                        )
                    except Exception as e:
                        llm_span.set_outputs({"error": str(e)})
                        root_span.set_outputs({"error": str(e)})
                        return {
                            "output": [{"id": "msg_0", "type": "message", "role": "assistant",
                                        "content": [{"type": "output_text", "text": f"Error calling model: {e}"}]}]
                        }

                    if not isinstance(response, dict) or "choices" not in response:
                        err_msg = f"Unexpected response: {str(response)[:300]}"
                        llm_span.set_outputs({"error": err_msg})
                        root_span.set_outputs({"error": err_msg})
                        return {"output": [{"id": "msg_0", "type": "message", "role": "assistant",
                                            "content": [{"type": "output_text", "text": err_msg}]}]}

                    choice         = response["choices"][0]
                    message        = choice["message"]
                    finish_reason  = choice.get("finish_reason")
                    tool_calls_raw = message.get("tool_calls") or []

                    llm_span.set_outputs({
                        "finish_reason": finish_reason,
                        "tool_calls":    len(tool_calls_raw),
                    })

                # ── Final answer ───────────────────────────────────────────────
                if finish_reason == "stop" or not tool_calls_raw:
                    final_content = message.get("content") or ""
                    break

                # ── Append assistant turn ──────────────────────────────────────
                conversation.append({
                    "role":       "assistant",
                    "content":    message.get("content") or "",
                    "tool_calls": [
                        {"id": tc["id"], "type": "function",
                         "function": {"name": tc["function"]["name"],
                                      "arguments": tc["function"]["arguments"]}}
                        for tc in tool_calls_raw
                    ],
                })

                # ── Tool calls ─────────────────────────────────────────────────
                for tc in tool_calls_raw:
                    tool_name = tc["function"]["name"]
                    tool_args = json.loads(tc["function"]["arguments"])

                    with mlflow.start_span(name=f"tool_{tool_name}") as tool_span:
                        tool_span.set_inputs({"tool": tool_name, "args": tool_args})
                        tool_result = self._dispatch_tool(tool_name, tool_args)
                        tool_span.set_outputs(tool_result)

                    conversation.append({
                        "role":         "tool",
                        "tool_call_id": tc["id"],
                        "content":      json.dumps(tool_result, ensure_ascii=False, default=str),
                    })

            root_span.set_outputs({"response_length": len(final_content)})

        return {
            "output": [{"id": "msg_0", "type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": final_content}]}]
        }


# Register the model — required for code-based logging.
mlflow.models.set_model(SLDBomAgent())
