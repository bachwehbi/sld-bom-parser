# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM Parser — Environment Setup
# MAGIC
# MAGIC Run this notebook **once** on a new Databricks workspace to provision all infrastructure
# MAGIC required before deploying the app or running the pipeline.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Create Unity Catalog schema (catalog must already exist) |
# MAGIC | 2 | Create UC volume `electrical_diagrams` |
# MAGIC | 3 | Create `overlays/` and `exports/` subdirectories in the volume |
# MAGIC | 4 | Copy the extraction system prompt to the volume |
# MAGIC | 5 | Create the `bom_extractions` Delta table |
# MAGIC | 6 | Create the `reference_matches` Delta table |
# MAGIC | 7 | Create the `exports` Delta table |
# MAGIC | 8 | Verify everything is in place |
# MAGIC
# MAGIC ## Before you start
# MAGIC
# MAGIC - The **catalog** must already exist and you must have `CREATE SCHEMA` privilege on it
# MAGIC - You need `CREATE VOLUME` privilege on the schema
# MAGIC - `sld2bom_system_prompt.txt` must be in the **same workspace folder** as this notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Edit `CATALOG` (and `SCHEMA` if needed) in `config.py` before running this notebook.
# MAGIC Everything else is derived automatically.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

PROMPT_DST = PROMPT_FILE   # alias used in this notebook

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {CATALOG}.{SCHEMA}")
print(f"Volume   : {VOLUME_PATH}")
print(f"Table    : {TABLE_NAME}")
print(f"Prompt   : {PROMPT_DST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create schema

# COMMAND ----------

# DBTITLE 1,Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✓ Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create volume

# COMMAND ----------

# DBTITLE 1,Create volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"✓ Volume {VOLUME_PATH} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create overlays and exports subdirectories
# MAGIC
# MAGIC UC volumes don't have an explicit "create directory" API — writing a placeholder file
# MAGIC creates the folder implicitly, then we remove it.

# COMMAND ----------

# DBTITLE 1,Create overlays/ and exports/ subdirectories
import os

EXPORTS_PATH = f"{VOLUME_PATH}/exports"

for subdir_path in [OVERLAY_PATH, EXPORTS_PATH]:
    placeholder = f"{subdir_path}/.keep"
    dbutils.fs.put(placeholder, "", overwrite=True)
    dbutils.fs.rm(placeholder)
    print(f"✓ {subdir_path}/ ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Copy system prompt to volume
# MAGIC
# MAGIC The prompt file `sld2bom_system_prompt.txt` must live in the same workspace folder as this notebook.
# MAGIC It is read from the workspace filesystem and written to the UC volume.

# COMMAND ----------

# DBTITLE 1,Copy system prompt
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_notebook_dir  = f"/Workspace{os.path.dirname(_notebook_path)}"
PROMPT_SRC     = f"{_notebook_dir}/sld2bom_system_prompt.txt"

if not os.path.exists(PROMPT_SRC):
    raise FileNotFoundError(
        f"System prompt not found at {PROMPT_SRC}\n"
        f"Make sure sld2bom_system_prompt.txt is in the same folder as this notebook."
    )

with open(PROMPT_SRC, "r", encoding="utf-8") as f:
    prompt_content = f.read()

dbutils.fs.put(PROMPT_DST, prompt_content, overwrite=True)
print(f"✓ System prompt copied ({len(prompt_content)} chars)")
print(f"  Source : {PROMPT_SRC}")
print(f"  Dest   : {PROMPT_DST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create BOM extractions table
# MAGIC
# MAGIC The pipeline notebook creates this automatically on first run, but creating it here
# MAGIC lets you verify permissions and schema before running any extraction.

# COMMAND ----------

# DBTITLE 1,Create bom_extractions Delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
  file_name      STRING    COMMENT 'PDF filename (deduplication key)',
  file_path      STRING    COMMENT 'Full volume path at processing time',
  status         STRING    COMMENT 'IN_PROGRESS, SUCCESS or ERROR',
  processed_at   TIMESTAMP COMMENT 'When extraction last ran',
  error_message  STRING    COMMENT 'Truncated stack trace on failure',
  bom_json       STRING    COMMENT 'JSON array of extracted components (matched + unmatched)',
  attempts_made  INT       COMMENT 'Number of extraction attempts (retry logic)',
  threshold_met  BOOLEAN   COMMENT 'Whether match rate threshold was reached',
  progress_msg   STRING    COMMENT 'Latest progress message for live status polling'
)
USING DELTA
COMMENT 'BOM extraction results from Schneider Electric SLD diagrams'
""")
print(f"✓ Table {TABLE_NAME} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Create reference_matches table

# COMMAND ----------

# DBTITLE 1,Create reference_matches Delta table
MATCHES_TABLE = f"{CATALOG}.{SCHEMA}.reference_matches"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MATCHES_TABLE} (
  file_name             STRING    COMMENT 'Source diagram (FK to bom_extractions)',
  component_idx         INT       COMMENT 'Index of the component in bom_json array',
  component_summary     STRING    COMMENT 'Human-readable component description',
  suggested_references  STRING    COMMENT 'JSON array of top N reference candidates with stock info',
  selected_reference    STRING    COMMENT 'Currently selected reference (null until confirmed)',
  user_overridden       BOOLEAN   COMMENT 'True if user changed the auto-suggestion',
  status                STRING    COMMENT 'PENDING / ACCEPTED / OVERRIDDEN / SKIPPED',
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP
)
USING DELTA
COMMENT 'Reference matching results — one row per component per diagram'
""")
print(f"✓ Table {MATCHES_TABLE} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Create exports table

# COMMAND ----------

# DBTITLE 1,Create exports Delta table
EXPORTS_TABLE = f"{CATALOG}.{SCHEMA}.exports"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EXPORTS_TABLE} (
  export_id         STRING    COMMENT 'UUID for this export',
  file_name         STRING    COMMENT 'Source diagram',
  exported_by       STRING    COMMENT 'Databricks user who triggered export',
  exported_at       TIMESTAMP COMMENT 'Export timestamp',
  volume_path       STRING    COMMENT 'Full UC volume path to the Excel file',
  component_count   INT       COMMENT 'Total components in the BOM',
  referenced_count  INT       COMMENT 'Components with a selected reference',
  overridden_count  INT       COMMENT 'Components where user changed the suggestion',
  total_value_eur   DOUBLE    COMMENT 'Sum of list_price_eur for selected references'
)
USING DELTA
COMMENT 'Audit trail of all Excel exports generated by the app'
""")
print(f"✓ Table {EXPORTS_TABLE} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Verify

# COMMAND ----------

# DBTITLE 1,Verification
import json

checks = {}

# Schema exists
try:
    spark.sql(f"DESCRIBE SCHEMA {CATALOG}.{SCHEMA}")
    checks["Schema"] = "OK"
except Exception as e:
    checks["Schema"] = f"FAIL — {e}"

# Volume exists
try:
    files = dbutils.fs.ls(VOLUME_PATH)
    checks["Volume"] = "OK"
except Exception as e:
    checks["Volume"] = f"FAIL — {e}"

# Overlays dir exists
try:
    dbutils.fs.ls(OVERLAY_PATH)
    checks["Overlays dir"] = "OK"
except Exception as e:
    checks["Overlays dir"] = f"FAIL — {e}"

# System prompt in volume
try:
    prompt_check = dbutils.fs.head(PROMPT_DST, 100)
    checks["System prompt"] = f"OK ({len(prompt_check)} chars read)"
except Exception as e:
    checks["System prompt"] = f"FAIL — {e}"

# bom_extractions table
try:
    spark.sql(f"DESCRIBE TABLE {TABLE_NAME}")
    checks["bom_extractions table"] = "OK"
except Exception as e:
    checks["bom_extractions table"] = f"FAIL — {e}"

# reference_matches table
try:
    spark.sql(f"DESCRIBE TABLE {MATCHES_TABLE}")
    checks["reference_matches table"] = "OK"
except Exception as e:
    checks["reference_matches table"] = f"FAIL — {e}"

# exports table
try:
    spark.sql(f"DESCRIBE TABLE {EXPORTS_TABLE}")
    checks["exports table"] = "OK"
except Exception as e:
    checks["exports table"] = f"FAIL — {e}"

# exports volume dir
try:
    dbutils.fs.ls(EXPORTS_PATH)
    checks["exports/ dir"] = "OK"
except Exception as e:
    checks["exports/ dir"] = f"FAIL — {e}"

print("=" * 50)
print("Setup verification")
print("=" * 50)
all_ok = True
for name, result in checks.items():
    status = "✓" if result.startswith("OK") else "✗"
    print(f"  {status}  {name:<20} {result}")
    if not result.startswith("OK"):
        all_ok = False
print("=" * 50)
if all_ok:
    print("All checks passed — ready to deploy the app and run the pipeline.")
else:
    print("Some checks failed — review the errors above before proceeding.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Create Databricks Jobs
# MAGIC
# MAGIC Creates the two pipeline jobs using the Databricks SDK.
# MAGIC **Idempotent** — if a job with the same name already exists it is reused, not duplicated.
# MAGIC
# MAGIC | Job | Notebook | Purpose |
# MAGIC |-----|----------|---------|
# MAGIC | `sld-bom-extraction` | `sld_to_bom_pipeline` | Vision extraction + vector matching |
# MAGIC | `sld-bom-matching`   | `sld_bom_matching_nb` | Reference matching via Vector Search |

# COMMAND ----------

# DBTITLE 1,Create extraction and matching jobs
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask

w = WorkspaceClient()

# Derive notebook directory from this notebook's workspace path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_dir  = os.path.dirname(_nb_path)   # e.g. /Users/you@example.com/bom_parser

EXTRACTION_NOTEBOOK = f"{_nb_dir}/sld_to_bom_pipeline"
MATCHING_NOTEBOOK   = f"{_nb_dir}/sld_bom_matching_nb"


def _find_or_create_job(w, name, notebook_path, base_params, timeout=3600):
    """Return existing job_id by name, or create a new job and return its id."""
    existing = [j for j in w.jobs.list(name=name) if j.settings.name == name]
    if existing:
        job_id = existing[0].job_id
        print(f"  already exists → reusing job_id={job_id}")
        return job_id
    job = w.jobs.create(
        name=name,
        tasks=[Task(
            task_key="run",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=base_params,
            ),
        )],
        timeout_seconds=timeout,
        max_concurrent_runs=1,
    )
    print(f"  created → job_id={job.job_id}")
    return job.job_id


print("Creating extraction job…")
EXTRACTION_JOB_ID = _find_or_create_job(
    w, "sld-bom-extraction", EXTRACTION_NOTEBOOK,
    {
        "file_name":    "",
        "model":        DEFAULT_MODEL_ENDPOINT,
        "enable_retry": str(DEFAULT_ENABLE_RETRY).lower(),
        "max_retries":  str(DEFAULT_MAX_RETRIES),
        "threshold":    str(DEFAULT_THRESHOLD),
    },
)
print(f"✓ sld-bom-extraction  job_id={EXTRACTION_JOB_ID}")

print("Creating matching job…")
MATCHING_JOB_ID = _find_or_create_job(
    w, "sld-bom-matching", MATCHING_NOTEBOOK,
    {
        "file_name":       "",
        "top_n":           "3",
        "preferred_tier":  "",
        "agent_threshold": "3",
        "vs_index_name":   f"{CATALOG}.{SCHEMA}.material_vs_index",
    },
)
print(f"✓ sld-bom-matching    job_id={MATCHING_JOB_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Patch app/app.yaml with job IDs and catalog
# MAGIC
# MAGIC Updates `app/app.yaml` in-place so the Databricks App picks up the correct
# MAGIC job IDs and catalog for this environment without any manual editing.

# COMMAND ----------

# DBTITLE 1,Update app/app.yaml
_app_yaml_path = f"/Workspace{_nb_dir}/app/app.yaml"

if not os.path.exists(_app_yaml_path):
    print(f"⚠ app/app.yaml not found — skipping (Tier 1 deployment or app not yet imported)")
    print(f"  If deploying the web app later, update app/app.yaml manually with:")
    print(f"    DATABRICKS_CATALOG  = {CATALOG}")
    print(f"    EXTRACTION_JOB_ID   = {EXTRACTION_JOB_ID}")
    print(f"    MATCHING_JOB_ID     = {MATCHING_JOB_ID}")
else:
    with open(_app_yaml_path, "r") as f:
        yaml_content = f.read()

    # Replace CATALOG value (unquoted in app.yaml)
    yaml_content = re.sub(
        r'(- name: DATABRICKS_CATALOG\n\s+value: )\S+',
        f'\\1{CATALOG}',
        yaml_content,
    )
    # Replace EXTRACTION_JOB_ID value
    yaml_content = re.sub(
        r'(- name: EXTRACTION_JOB_ID\n\s+value: )"[^"]*"',
        f'\\1"{EXTRACTION_JOB_ID}"',
        yaml_content,
    )
    # Replace MATCHING_JOB_ID value
    yaml_content = re.sub(
        r'(- name: MATCHING_JOB_ID\n\s+value: )"[^"]*"',
        f'\\1"{MATCHING_JOB_ID}"',
        yaml_content,
    )

    with open(_app_yaml_path, "w") as f:
        f.write(yaml_content)

    print(f"✓ app/app.yaml updated")
    print(f"  DATABRICKS_CATALOG  = {CATALOG}")
    print(f"  EXTRACTION_JOB_ID   = {EXTRACTION_JOB_ID}")
    print(f"  MATCHING_JOB_ID     = {MATCHING_JOB_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Create Vector Search endpoint _(Tier 2+)_
# MAGIC
# MAGIC Creates the Vector Search endpoint used by the reference matching pipeline.
# MAGIC This step is safe to re-run — it skips creation if the endpoint already exists.
# MAGIC
# MAGIC > **Prerequisite:** Run `generate_material_data` first so the `material` table exists.

# COMMAND ----------

# DBTITLE 1,Create Vector Search endpoint
from databricks.sdk.service.vectorsearch import EndpointType

VS_ENDPOINT_NAME = "sld-bom-vs"

existing_endpoints = {e.name for e in w.vector_search_endpoints.list_endpoints()}
if VS_ENDPOINT_NAME not in existing_endpoints:
    print(f"Creating VS endpoint '{VS_ENDPOINT_NAME}'… (takes 2–5 min)")
    w.vector_search_endpoints.create_endpoint_and_wait(
        name=VS_ENDPOINT_NAME,
        endpoint_type=EndpointType.STANDARD,
    )
    print(f"✓ VS endpoint '{VS_ENDPOINT_NAME}' ready")
else:
    print(f"✓ VS endpoint '{VS_ENDPOINT_NAME}' already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — Create Vector Search index _(Tier 2+)_
# MAGIC
# MAGIC Creates a Delta Sync index on `material.product_description` using
# MAGIC `databricks-gte-large-en` embeddings. Initial sync starts automatically.
# MAGIC
# MAGIC > The matching notebook (`sld_bom_matching_nb`) will wait for the index to be ready on first use.

# COMMAND ----------

# DBTITLE 1,Create Vector Search index on material table
from databricks.sdk.service.vectorsearch import (
    VectorIndexType, DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn, PipelineType,
)

VS_INDEX_NAME  = f"{CATALOG}.{SCHEMA}.material_vs_index"
MATERIAL_TABLE = f"{CATALOG}.{SCHEMA}.material"

# Guard: ensure material table exists
_mat_exists = spark.sql(
    f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'material'"
).count() > 0
if not _mat_exists:
    print("⚠ material table not found — run generate_material_data first, then re-run this cell")
else:
    existing_indexes = {
        i.name
        for i in w.vector_search_indexes.list_indexes(endpoint_name=VS_ENDPOINT_NAME)
    }
    if VS_INDEX_NAME not in existing_indexes:
        print(f"Creating VS index '{VS_INDEX_NAME}'…")
        w.vector_search_indexes.create_index(
            name=VS_INDEX_NAME,
            endpoint_name=VS_ENDPOINT_NAME,
            primary_key="reference",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=MATERIAL_TABLE,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name="product_description",
                        embedding_model_endpoint_name="databricks-gte-large-en",
                    )
                ],
            ),
        )
        print(f"✓ VS index '{VS_INDEX_NAME}' created — initial sync starting")
        print(f"  Monitor progress: Catalog → Vector Search → {VS_ENDPOINT_NAME}")
    else:
        print(f"✓ VS index '{VS_INDEX_NAME}' already exists")
    print(f"  Index: {VS_INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 — Grant READ VOLUME to the agent serving principal _(Tier 3+)_
# MAGIC
# MAGIC The model serving scoped token runs as a system service principal (not the model owner).
# MAGIC This step grants it READ VOLUME so the `list_unprocessed_files` tool can run `LIST` via SQL.
# MAGIC
# MAGIC > Run this step **after** deploying the serving endpoint (cell 8 of `sld_bom_agent_uc`).
# MAGIC > The service principal UUID changes per endpoint deployment — re-run this step if you re-create the endpoint.

# COMMAND ----------

# DBTITLE 1,Grant READ VOLUME to the agent service principal
_sld_endpoint_name = "sld-bom-agent"

try:
    _ep_sp = spark.sql(
        f"SELECT current_user() AS u"
    )  # placeholder — we query via the warehouse which runs as the human user
    # The actual SP is identified by running SELECT current_user() through the endpoint itself.
    # Auto-detection: query the endpoint's token via the SDK and call the SQL API as the SP.
    from databricks.sdk.service.sql import StatementState
    _w_client = WorkspaceClient()
    _warehouse_id_for_grant = "61acc98b38c08e84"

    # Execute SELECT current_user() through the warehouse impersonating the endpoint SP
    # We can't impersonate directly, but we can look at system.serving.served_entities
    _sp_rows = spark.sql("""
        SELECT served_entity_spec:serviceAccountEmail AS sp
        FROM system.serving.served_entities
        WHERE endpoint_name = 'sld-bom-agent'
        ORDER BY update_timestamp DESC
        LIMIT 1
    """).collect()

    if _sp_rows and _sp_rows[0]["sp"]:
        _sp_id = _sp_rows[0]["sp"]
        spark.sql(f"GRANT USE CATALOG  ON CATALOG {CATALOG} TO `{_sp_id}`")
        spark.sql(f"GRANT USE SCHEMA   ON SCHEMA {CATALOG}.{SCHEMA} TO `{_sp_id}`")
        spark.sql(f"GRANT READ VOLUME  ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `{_sp_id}`")
        spark.sql(f"GRANT SELECT       ON TABLE  {TABLE_NAME} TO `{_sp_id}`")
        print(f"✓ Grants applied to serving SP: {_sp_id}")
    else:
        print("⚠ Could not auto-detect serving SP from system.serving.served_entities")
        print("  After deploying the endpoint, run SELECT current_user() via the agent chat")
        print("  then manually run:")
        print(f"    GRANT READ VOLUME ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `<sp-uuid>`")
        print(f"    GRANT USE SCHEMA  ON SCHEMA {CATALOG}.{SCHEMA} TO `<sp-uuid>`")
except Exception as _grant_ex:
    print(f"⚠ Auto-grant failed: {_grant_ex}")
    print("  Manually grant after endpoint deployment — see note above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Once all steps pass:
# MAGIC
# MAGIC | Tier | Action |
# MAGIC |------|--------|
# MAGIC | 2 | Run `generate_material_data` → then Steps 11–12 to create the VS endpoint and index |
# MAGIC | 3 | Run the `sld-bom-agent-registration` job (created by DAB `full` target), then create a Model Serving endpoint from the registered UC model |
# MAGIC | 4 | Deploy the app via DAB: `databricks bundle deploy --target full` |
# MAGIC
# MAGIC **Alternative (no DAB):**
# MAGIC 1. **Populate catalog data** — run `generate_material_data`
# MAGIC 2. **Register the agent** _(Tier 3+)_ — run `sld_bom_agent_uc` notebook (all cells)
# MAGIC 3. **Deploy the app** _(Tier 4)_ — `app/app.yaml` was patched by Step 10; run:
# MAGIC    ```bash
# MAGIC    databricks apps create sld-bom-parser
# MAGIC    databricks apps deploy sld-bom-parser \
# MAGIC      --source-code-path /Workspace/Users/<you>/bom_parser/app
# MAGIC    ```
# MAGIC 4. **Upload PDFs** to the volume via the app UI or directly to `{VOLUME_PATH}/`
