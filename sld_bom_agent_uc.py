# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM Agent — MLflow Registration & Unity Catalog
# MAGIC
# MAGIC This notebook defines, tests, logs, and registers the SLD-to-BOM conversational agent
# MAGIC as an MLflow model in Unity Catalog.
# MAGIC
# MAGIC ## What the agent does
# MAGIC
# MAGIC The agent wraps the extraction pipeline in a conversational interface. A field engineer
# MAGIC can interact with it in natural language instead of editing notebook config cells:
# MAGIC
# MAGIC > *"Process all new diagrams with up to 3 retries, threshold 80%"*
# MAGIC > *"Run AVILA using Opus, don't retry"*
# MAGIC > *"Show me unmatched components for ESQUEMAS"*
# MAGIC > *"Why was component D17 unmatched?"*
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC User message (natural language)
# MAGIC         │
# MAGIC         ▼
# MAGIC  SLDBomAgent (MLflow ChatModel)
# MAGIC         │  reasons about the request
# MAGIC         │  extracts parameters (retry, threshold, model)
# MAGIC         │  confirms before running
# MAGIC         ▼
# MAGIC  Tool calls → sld_bom_extractor.run_extraction()
# MAGIC                            │
# MAGIC                            ▼
# MAGIC                    Delta table / Volume
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites (Tier 3+)
# MAGIC
# MAGIC Before running this notebook on a new workspace:
# MAGIC
# MAGIC 1. Complete all `setup.py` steps and Tier 2 prerequisites (VS index must be ONLINE)
# MAGIC 2. **No manual edits needed for `sld_bom_agent_model.py`** — workspace-specific values
# MAGIC    (`CATALOG`, `SQL_WAREHOUSE_ID`, `EXTRACTION_JOB_ID`, `MATCHING_JOB_ID`) are injected
# MAGIC    automatically as environment variables when the serving endpoint is created/updated
# MAGIC    by cell 8 of this notebook. The jobs are looked up by name (`sld-bom-extraction`,
# MAGIC    `sld-bom-matching`) so no hardcoded IDs are needed.
# MAGIC
# MAGIC ## Notebook flow
# MAGIC
# MAGIC 1. Install dependencies
# MAGIC 2. Configuration
# MAGIC 3. Define the `SLDBomAgent` ChatModel class with 4 tools
# MAGIC 4. Test locally before logging
# MAGIC 5. Log to MLflow experiment
# MAGIC 6. Register to Unity Catalog
# MAGIC 7. Deploy to Model Serving endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies
# MAGIC
# MAGIC MLflow's `ChatModel` interface requires `mlflow >= 2.12` for full tool-calling support.
# MAGIC `databricks-sdk` is used in the deployment cell to create the serving endpoint programmatically.

# COMMAND ----------

# DBTITLE 1,Install dependencies
%pip install PyMuPDF Pillow openai "mlflow>=3.0" databricks-sdk --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration
# MAGIC
# MAGIC All paths and defaults come from `config.py`. To deploy to a new environment, edit `CATALOG` there.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import os, sys, json, importlib
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load pipeline module
# MAGIC
# MAGIC The agent tools call directly into `sld_bom_extractor.py`. The module path is derived
# MAGIC from the notebook's location at runtime — no hardcoded user paths.

# COMMAND ----------

# DBTITLE 1,Load pipeline module
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_module_dir = f"/Workspace{os.path.dirname(_notebook_path)}"
sys.path.insert(0, _module_dir)

import sld_bom_extractor as pipeline; importlib.reload(pipeline)
from sld_bom_extractor import rasterize_pdf, run_extraction, generate_precision_overlay, DPI

with open(PROMPT_FILE, "r", encoding="utf-8") as f:
    SYSTEM_PROMPT = f.read().strip()

print(f"Pipeline loaded | DPI={DPI}")
print(f"Prompt loaded ({len(SYSTEM_PROMPT)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define the Agent — `SLDBomAgent`
# MAGIC
# MAGIC The agent is an `mlflow.pyfunc.ChatModel` subclass. MLflow's `ChatModel` interface:
# MAGIC - Accepts a standard OpenAI-format messages list
# MAGIC - Supports tool definitions that Claude can call during its reasoning loop
# MAGIC - Is serializable and deployable to any MLflow-compatible serving infrastructure
# MAGIC
# MAGIC ### Tools exposed to Claude
# MAGIC
# MAGIC | Tool | Purpose |
# MAGIC |------|---------|
# MAGIC | `list_unprocessed_files` | Lists PDFs in the volume not yet in the Delta table |
# MAGIC | `run_extraction` | Runs the full extraction pipeline for one PDF with configurable retry |
# MAGIC | `query_results` | Runs a SQL query against the Delta table (e.g. show unmatched components) |
# MAGIC | `get_overlay_path` | Returns the path to a diagram's QA overlay JPEG |
# MAGIC
# MAGIC ### Agent system prompt
# MAGIC
# MAGIC The agent's system prompt instructs Claude to:
# MAGIC 1. Confirm interpreted parameters (threshold, retries, model) before running extraction
# MAGIC 2. Report match rates and retry outcomes in plain language after each run
# MAGIC 3. Proactively flag results where `threshold_met = False`
# MAGIC 4. Answer follow-up questions about specific components using `query_results`

# COMMAND ----------

# DBTITLE 1,Define SLDBomAgent
import mlflow
from mlflow.pyfunc import ResponsesAgent

AGENT_SYSTEM_PROMPT = """You are the SLD-to-BOM extraction agent for Schneider Electric electrical diagrams.
You help field engineers extract Bills of Materials from PDF Single Line Diagrams.

You have access to 4 tools:
- list_unprocessed_files: discover which PDFs haven't been processed yet
- run_extraction: run the extraction pipeline on a specific PDF
- query_results: query the Delta table for extraction results or component details
- get_overlay_path: get the path to a diagram's QA overlay image

IMPORTANT RULES:
1. Before calling run_extraction, always confirm the parameters you understood:
   model, enable_retry, max_retries, threshold. Example:
   "I'll process AVILA.pdf using Sonnet with up to 2 retries, accepting results above 75%. Starting now."
2. After extraction, always report: total components extracted, match rate, attempts made, threshold met.
3. If threshold_met is False, explicitly flag it and suggest the user review unmatched components.
4. When the user asks about specific components, use query_results with appropriate SQL.
5. Default parameters when not specified by user:
   - model: databricks-claude-sonnet-4-6
   - enable_retry: true
   - max_retries: 2
   - threshold: 0.75
"""


class SLDBomAgent(ResponsesAgent):
    """MLflow 3 ResponsesAgent wrapping the SLD-to-BOM extraction pipeline.

    Implements a tool-calling agent loop with MLflow tracing: Claude reasons
    about the user's request, calls pipeline tools as needed, and returns a
    natural language response.
    The agent uses the same `run_extraction` function as the production notebook,
    ensuring consistent behavior across both interfaces.
    """

    def _get_deploy_client(self):
        """Return mlflow.deployments client — handles auth automatically in serving."""
        import mlflow.deployments
        return mlflow.deployments.get_deploy_client("databricks")

    def _get_client(self):
        """Create an authenticated OpenAI-compatible client for the workspace (notebook context)."""
        from openai import OpenAI
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        return OpenAI(
            base_url=f"https://{workspace_url}/serving-endpoints",
            api_key=token,
        )

    # ── Tool implementations ──────────────────────────────────────────────────

    def _tool_list_unprocessed_files(self):
        """Return a list of PDFs in the volume that haven't been processed yet."""
        all_pdfs = [
            f.name for f in dbutils.fs.ls(f"dbfs:{VOLUME_PATH}")
            if f.name.lower().endswith(".pdf")
        ]
        processed = {
            row.file_name
            for row in spark.sql(f"SELECT file_name FROM {TABLE_NAME}").collect()
        }
        new_files = [f for f in all_pdfs if f not in processed]
        return {
            "total_pdfs": len(all_pdfs),
            "processed": len(processed),
            "unprocessed": new_files,
        }

    def _tool_run_extraction(self, file_name, model=None, enable_retry=None,
                              max_retries=None, threshold=None):
        """Run the full extraction pipeline for one PDF.

        Parameters are optional — defaults come from the notebook config.
        This mirrors the production pipeline exactly: rasterize → run_extraction
        (retry-aware) → overlay → Delta write.
        """
        model         = model or DEFAULT_MODEL_ENDPOINT
        enable_retry  = enable_retry if enable_retry is not None else DEFAULT_ENABLE_RETRY
        max_retries   = max_retries  if max_retries  is not None else DEFAULT_MAX_RETRIES
        threshold     = threshold    if threshold     is not None else DEFAULT_THRESHOLD

        file_path = f"{VOLUME_PATH}/{file_name}"
        client    = self._get_client()

        images = rasterize_pdf(file_path)
        result = run_extraction(
            client, SYSTEM_PROMPT, images, file_path, model,
            enable_retry=enable_retry,
            max_retries=max_retries,
            threshold=threshold,
            verbose=False,   # suppress print output inside agent tool calls
        )

        matched   = result["matched"]
        unmatched = result["unmatched"]
        bom_json  = json.dumps(matched + unmatched, ensure_ascii=False)

        # Save overlay
        overlay_file = f"{os.path.splitext(file_name)[0]}_overlay.jpg"
        generate_precision_overlay(file_path, matched, unmatched, f"{OVERLAY_PATH}/{overlay_file}")

        # Write to Delta table
        from pyspark.sql import Row
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
        schema = StructType([
            StructField("file_name",     StringType(),    True),
            StructField("file_path",     StringType(),    True),
            StructField("processed_at",  TimestampType(), True),
            StructField("status",        StringType(),    True),
            StructField("error_message", StringType(),    True),
            StructField("bom_json",      StringType(),    True),
            StructField("attempts_made", IntegerType(),   True),
            StructField("threshold_met", BooleanType(),   True),
        ])
        spark.createDataFrame([Row(
            file_name=file_name,
            file_path=file_path,
            processed_at=datetime.now(),
            status="SUCCESS",
            error_message=None,
            bom_json=bom_json,
            attempts_made=result["attempts_made"],
            threshold_met=result["threshold_met"],
        )], schema=schema).write.mode("append").saveAsTable(TABLE_NAME)

        return {
            "file_name":      file_name,
            "total":          len(matched) + len(unmatched),
            "matched":        len(matched),
            "unmatched":      len(unmatched),
            "match_rate":     round(result["final_match_rate"], 3),
            "attempts_made":  result["attempts_made"],
            "threshold_met":  result["threshold_met"],
            "model_used":     model,
            "overlay_path":   f"{OVERLAY_PATH}/{overlay_file}",
        }

    def _tool_query_results(self, sql_query):
        """Execute a SQL query against the Delta table and return results as a list of dicts."""
        rows = spark.sql(sql_query).limit(100).collect()
        return [row.asDict() for row in rows]

    def _tool_get_overlay_path(self, file_name):
        """Return the volume path to a diagram's QA overlay JPEG."""
        stem = os.path.splitext(file_name)[0]
        path = f"{OVERLAY_PATH}/{stem}_overlay.jpg"
        return {"overlay_path": path}

    # ── Tool dispatch ─────────────────────────────────────────────────────────

    def _dispatch_tool(self, tool_name, tool_args):
        """Route a tool call from Claude to the correct implementation."""
        if tool_name == "list_unprocessed_files":
            return self._tool_list_unprocessed_files()
        elif tool_name == "run_extraction":
            return self._tool_run_extraction(**tool_args)
        elif tool_name == "query_results":
            return self._tool_query_results(**tool_args)
        elif tool_name == "get_overlay_path":
            return self._tool_get_overlay_path(**tool_args)
        else:
            return {"error": f"Unknown tool: {tool_name}"}

    # ── Tool definitions (OpenAI function-calling schema) ─────────────────────

    TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "list_unprocessed_files",
                "description": "List all PDF files in the volume that haven't been processed yet.",
                "parameters": {"type": "object", "properties": {}, "required": []},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "run_extraction",
                "description": (
                    "Run the full SLD-to-BOM extraction pipeline for one PDF diagram. "
                    "Saves results to the Delta table and generates a QA overlay image."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file_name": {
                            "type": "string",
                            "description": "PDF filename (e.g. 'P-2025-101-01-AVILA_10_page_1.pdf')",
                        },
                        "model": {
                            "type": "string",
                            "description": "Model endpoint name. Default: databricks-claude-sonnet-4-6",
                        },
                        "enable_retry": {
                            "type": "boolean",
                            "description": "Whether to retry if match rate is below threshold. Default: true",
                        },
                        "max_retries": {
                            "type": "integer",
                            "description": "Max additional attempts after the first. Default: 2",
                        },
                        "threshold": {
                            "type": "number",
                            "description": "Minimum acceptable match rate (0.0–1.0). Default: 0.75",
                        },
                    },
                    "required": ["file_name"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "query_results",
                "description": (
                    "Run a SQL query against the BOM extractions Delta table. "
                    f"Table: {TABLE_NAME}. "
                    "Columns: file_name STRING, file_path STRING, processed_at TIMESTAMP, "
                    "status STRING ('SUCCESS' or 'ERROR'), error_message STRING, "
                    "bom_json STRING (JSON array of component objects), "
                    "attempts_made INT, threshold_met BOOLEAN. "
                    "Each element in bom_json has keys: 'Que és' (component type), "
                    "'Calibre (A)' (rated current), 'Polos' (poles), 'Circuito' (circuit name), "
                    "'precise_cx' (x position — null if unmatched), 'match_type' (null or 'circuit_shared'). "
                    "To get match_status per component: "
                    "CASE WHEN elem['precise_cx'] IS NULL THEN 'unmatched' "
                    "WHEN elem['match_type']='circuit_shared' THEN 'circuit_shared' "
                    "ELSE 'matched' END. "
                    "Always use 'processed_at' (not extraction_date), 'file_name' (not pdf_name or filename)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql_query": {
                            "type": "string",
                            "description": "Valid Spark SQL SELECT query.",
                        },
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
                        "file_name": {
                            "type": "string",
                            "description": "PDF filename.",
                        },
                    },
                    "required": ["file_name"],
                },
            },
        },
    ]

    # ── Main predict loop ─────────────────────────────────────────────────────

    def predict(self, model_input, params=None):
        """Run the agent reasoning loop with MLflow tracing (MLflow 3).

        model_input can be a ResponsesAgentRequest or a plain dict.
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
                with mlflow.start_span(name=f"llm_call_{iteration}") as llm_span:
                    llm_span.set_inputs({"model": DEFAULT_MODEL_ENDPOINT, "turn": iteration})
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
                        return {"output": [{"id": "msg_0", "type": "message", "role": "assistant",
                                            "content": [{"type": "output_text", "text": f"Error calling model: {e}"}]}]}

                    choice         = response["choices"][0]
                    message        = choice["message"]
                    finish_reason  = choice.get("finish_reason")
                    tool_calls_raw = message.get("tool_calls") or []
                    llm_span.set_outputs({"finish_reason": finish_reason, "tool_calls": len(tool_calls_raw)})

                if finish_reason == "stop" or not tool_calls_raw:
                    final_content = message.get("content") or ""
                    break

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

        return {"output": [{"id": "msg_0", "type": "message", "role": "assistant",
                            "content": [{"type": "output_text", "text": final_content}]}]}


print("SLDBomAgent class defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test locally
# MAGIC
# MAGIC Before logging to MLflow, test the agent directly in the notebook.
# MAGIC This is the most efficient debugging loop — no deploy/serve cycle needed.
# MAGIC
# MAGIC Uncomment and run one test at a time. The agent will call tools and return a natural language response.

# COMMAND ----------

# DBTITLE 1,Local test
# Instantiate the agent directly (no MLflow involved)
agent = SLDBomAgent()

# ── Test 1: list unprocessed files ────────────────────────────────────────────
# request = {"input": [{"role": "user", "content": "What files haven't been processed yet?"}]}

# ── Test 2: run extraction with custom parameters ─────────────────────────────
# request = {"input": [{"role": "user", "content": "Process CARRIAZO_CGBT_page_3.pdf with 1 retry and threshold 80%"}]}

# ── Test 3: query unmatched components ────────────────────────────────────────
# request = {"input": [{"role": "user", "content": "Show me unmatched components for the AVILA diagram"}]}

# response = agent.predict(model_input=request)
# print(response["output"][0]["content"][0]["text"])

print("Uncomment a test block above and run to test the agent locally.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Log to MLflow
# MAGIC
# MAGIC Logs the agent as an MLflow model with:
# MAGIC - **Signature**: inferred from a sample input/output so the serving endpoint knows the expected schema
# MAGIC - **pip requirements**: all packages needed at serving time
# MAGIC - **input_example**: a sample request shown in the MLflow UI
# MAGIC
# MAGIC The model is logged to the current MLflow experiment. The experiment is set to a dedicated
# MAGIC path under the workspace user's folder so it's easy to find.

# COMMAND ----------

# DBTITLE 1,Log to MLflow
import mlflow
from mlflow.models import infer_signature

# Derive the workspace user from the notebook path (already set in Cell 3)
# e.g. /Users/jane.doe@databricks.com/bom_parser/... → jane.doe@databricks.com
_ws_user = _notebook_path.split('/')[2]
mlflow.set_experiment(f"/Users/{_ws_user}/sld_bom_agent")

from mlflow.models.resources import DatabricksServingEndpoint, DatabricksSQLWarehouse, DatabricksTable
import inspect as _inspect

# DatabricksTable constructor signature varies by mlflow version
_dt_kwargs = "table_full_name" in _inspect.signature(DatabricksTable.__init__).parameters
_table_resource = (
    DatabricksTable(table_full_name="serverless_stable_bach_catalog.bom_parser.bom_extractions")
    if _dt_kwargs else
    DatabricksTable("serverless_stable_bach_catalog.bom_parser.bom_extractions")
)

# DatabricksJob resource (available in mlflow >= 2.16) — lets the scoped token call jobs.run_now
# DatabricksVolume — grants the scoped token READ VOLUME so the directory listing API works
_resources = [
    DatabricksServingEndpoint(endpoint_name="databricks-claude-sonnet-4-6"),
    DatabricksSQLWarehouse(warehouse_id="61acc98b38c08e84"),
    _table_resource,
]
try:
    from mlflow.models.resources import DatabricksJob
    _resources.append(DatabricksJob(job_id=int(_ext_job_id)))
    _resources.append(DatabricksJob(job_id=int(_mat_job_id)))
except ImportError:
    pass  # DatabricksJob not available in this mlflow version — job triggers use the SDK token
try:
    from mlflow.models.resources import DatabricksUCVolume
    _resources.append(DatabricksUCVolume(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        volume_name=VOLUME,
    ))
    print("✓ DatabricksUCVolume resource added — scoped token will get READ VOLUME")
except ImportError:
    # Fallback: grant READ VOLUME directly to the endpoint service principal via SQL
    print("⚠ DatabricksUCVolume not available in this mlflow version")
    print("  Granting READ VOLUME to the serving endpoint principal via SQL...")
    try:
        _ep_info = w.serving_endpoints.get("sld-bom-agent")
        _ep_creator = _ep_info.creator
        spark.sql(f"GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `{_ep_creator}`")
        print(f"  ✓ READ VOLUME granted to {_ep_creator}")
    except Exception as _grant_err:
        print(f"  Could not auto-grant: {_grant_err}")
        print(f"  Run manually: GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `<endpoint-principal>`")

with mlflow.start_run(run_name="sld_bom_agent_v1"):
    model_info = mlflow.pyfunc.log_model(
        name="agent",
        # Code-based logging — MLflow reads the file and uses mlflow.models.set_model().
        python_model=f"{_module_dir}/sld_bom_agent_model.py",
        # Agent is now read-only + job-trigger. No PyMuPDF/Pillow needed at serve time.
        pip_requirements=[
            "mlflow>=3.0",
            "databricks-sdk",
        ],
        # Declare resource dependencies so the serving container's scoped token
        # has permission to call the foundation model, execute SQL, and trigger the Job.
        resources=_resources,
    )

print(f"Model logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Register to Unity Catalog
# MAGIC
# MAGIC Registers the logged model under the three-part Unity Catalog name
# MAGIC `serverless_stable_bach_catalog.bom_parser.sld_bom_agent`.
# MAGIC
# MAGIC Registration creates a versioned, governed artifact that can be:
# MAGIC - Deployed to a Model Serving endpoint
# MAGIC - Granted permissions to specific users or groups
# MAGIC - Rolled back to a previous version if needed
# MAGIC
# MAGIC `mlflow.set_registry_uri("databricks-uc")` is required to target Unity Catalog
# MAGIC instead of the legacy workspace model registry.

# COMMAND ----------

# DBTITLE 1,Register to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

registered = mlflow.register_model(
    model_uri=model_info.model_uri,
    name=AGENT_MODEL_NAME,
)

print(f"Registered: {AGENT_MODEL_NAME} — version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Deploy to Model Serving
# MAGIC
# MAGIC Creates a Databricks Model Serving endpoint for the registered agent.
# MAGIC The endpoint runs on serverless compute — no cluster management needed.
# MAGIC
# MAGIC Once deployed, the agent can be called via:
# MAGIC - The Databricks Playground UI (interactive testing)
# MAGIC - The REST API (programmatic access from any application)
# MAGIC - Another Claude agent as a tool (agent chaining)
# MAGIC
# MAGIC **Note:** endpoint creation takes 5–10 minutes. The cell below submits the request
# MAGIC and returns immediately — check the Serving UI for status.

# COMMAND ----------

# DBTITLE 1,Deploy to Model Serving
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

w = WorkspaceClient()

endpoint_name = "sld-bom-agent"
model_version  = registered.version

# Resolve warehouse and job IDs from the workspace (set by setup.py)
# Warehouse: pick any non-Starter warehouse via SDK
_warehouses = list(w.warehouses.list())
_wh_id = next(
    (wh.id for wh in _warehouses if "Starter" not in (wh.name or "")),
    "61acc98b38c08e84"   # fallback to the warehouse declared in _resources above
)

# Job IDs — look up by name (created by setup.py Step 9)
_ext_jobs = [j for j in w.jobs.list(name="sld-bom-extraction") if j.settings.name == "sld-bom-extraction"]
_mat_jobs = [j for j in w.jobs.list(name="sld-bom-matching")   if j.settings.name == "sld-bom-matching"]
_ext_job_id = str(_ext_jobs[0].job_id) if _ext_jobs else "811920885410866"
_mat_job_id = str(_mat_jobs[0].job_id) if _mat_jobs else "330529910000908"
print(f"Endpoint env: CATALOG={CATALOG} | WAREHOUSE={_wh_id} | EXTRACTION={_ext_job_id} | MATCHING={_mat_job_id}")

served_entities = [
    ServedEntityInput(
        entity_name=AGENT_MODEL_NAME,
        entity_version=str(model_version),
        scale_to_zero_enabled=True,   # cost-efficient for demo/POC use
        workload_size="Small",         # 1 concurrent request — suitable for field use
        environment_vars={
            "DATABRICKS_CATALOG":      CATALOG,
            "DATABRICKS_SCHEMA":       SCHEMA,
            "DATABRICKS_WAREHOUSE_ID": _wh_id,
            "EXTRACTION_JOB_ID":       _ext_job_id,
            "MATCHING_JOB_ID":         _mat_job_id,
        },
    )
]
config = EndpointCoreConfigInput(served_entities=served_entities)

# Create if new, update if it already exists (idempotent re-runs).
try:
    w.serving_endpoints.create(name=endpoint_name, config=config)
    print(f"Endpoint '{endpoint_name}' creation submitted (version {model_version}).")
except Exception as e:
    if "already exists" in str(e).lower() or "ResourceAlreadyExists" in str(type(e).__name__):
        w.serving_endpoints.update_config(name=endpoint_name, served_entities=served_entities)
        print(f"Endpoint '{endpoint_name}' updated to model version {model_version}.")
    else:
        raise

ws_url = spark.conf.get("spark.databricks.workspaceUrl")
print(f"Monitor at: https://{ws_url}/ml/endpoints/{endpoint_name}")
