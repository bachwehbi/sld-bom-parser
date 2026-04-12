# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM Reference Matching Pipeline (VS Edition)
# MAGIC
# MAGIC Three-phase semantic matching pipeline:
# MAGIC
# MAGIC | Phase | What it does |
# MAGIC |-------|-------------|
# MAGIC | 1 | Canonical field normalisation — works for any language (Spanish, French, German, ...) |
# MAGIC | 2 | Databricks Vector Search retrieval → property scoring re-ranker |
# MAGIC | 3 | LLM agentic fallback for low-confidence components (score < threshold) |
# MAGIC
# MAGIC ## Prerequisites (Tier 2+)
# MAGIC
# MAGIC Before running this notebook for the first time on a new workspace:
# MAGIC
# MAGIC 1. Complete all `setup.py` steps (Tier 1 prerequisites must be met)
# MAGIC 2. Run `generate_material_data.py` — creates the `material`, `stock`, and `work_orders` tables
# MAGIC 3. Create a **Vector Search endpoint** (e.g. `sld-bom-vs`) in the Databricks UI (Catalog → Vector Search)
# MAGIC 4. Create a **Vector Search index** named `material_vs_index` on `{CATALOG}.bom_parser.material`
# MAGIC 5. Extraction must have completed successfully for at least one diagram (`bom_extractions` status = SUCCESS)
# MAGIC
# MAGIC > **Normal operation:** This notebook is triggered automatically by the `sld-bom-matching` Databricks Job
# MAGIC > (created by `setup.py`). Run it manually only for debugging or one-off matching runs.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `file_name` | — | PDF filename to match (required) |
# MAGIC | `top_n` | `3` | Reference candidates per component |
# MAGIC | `preferred_tier` | `""` | Filter candidates by tier after VS: economy / standard / premium |
# MAGIC | `agent_threshold` | `3` | Property score below which LLM fallback is triggered (0 = always, 99 = never) |
# MAGIC | `vs_index_name` | — | Fully-qualified VS index (set in config, overridable) |

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DBTITLE 1,Install dependencies
%pip install openpyxl --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# DBTITLE 1,Parameters
import sys, os, json, importlib

dbutils.widgets.text("file_name",        "",                        "PDF file name (required)")
dbutils.widgets.text("top_n",            "3",                       "Candidates per component")
dbutils.widgets.text("preferred_tier",   "",                        "Tier filter (economy/standard/premium or blank)")
dbutils.widgets.text("agent_threshold",  "3",                       "LLM fallback score threshold (0=always, 99=never)")
dbutils.widgets.text("vs_index_name",    f"{CATALOG}.{SCHEMA}.material_vs_index", "VS index name")

FILE_NAME       = dbutils.widgets.get("file_name").strip()
TOP_N           = int(dbutils.widgets.get("top_n").strip() or "3")
PREFERRED_TIER  = dbutils.widgets.get("preferred_tier").strip().lower()
AGENT_THRESHOLD = int(dbutils.widgets.get("agent_threshold").strip() or "3")
VS_INDEX_NAME   = dbutils.widgets.get("vs_index_name").strip()

# Batch mode: file_name="" → process all SUCCESS files with at least one component
if not FILE_NAME:
    _rows = spark.sql(f"""
        SELECT file_name FROM {TABLE_NAME}
        WHERE status = 'SUCCESS'
          AND bom_json IS NOT NULL
          AND bom_json != '[]'
          AND length(bom_json) > 2
        ORDER BY processed_at DESC
    """).collect()
    FILES_TO_PROCESS = [r["file_name"] for r in _rows]
    print(f"Batch mode: {len(FILES_TO_PROCESS)} files to process")
else:
    FILES_TO_PROCESS = [FILE_NAME]
    print(f"Single-file mode: {FILE_NAME}")

print(f"Top N         : {TOP_N}")
print(f"Tier filter   : {PREFERRED_TIER or 'none (all tiers)'}")
print(f"Agent threshold: {AGENT_THRESHOLD}  (0=always use LLM, 99=never)")
print(f"VS index      : {VS_INDEX_NAME}")

# COMMAND ----------

# DBTITLE 1,Load modules
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_module_dir    = f"/Workspace{os.path.dirname(_notebook_path)}"
if _module_dir not in sys.path:
    sys.path.insert(0, _module_dir)

import sld_bom_catalog as catalog_module
import sld_bom_vs_matcher as vs_module
importlib.reload(catalog_module)
importlib.reload(vs_module)

from sld_bom_catalog  import normalize_component_fields, build_vs_query
from sld_bom_vs_matcher import match_all_components

print("✓ sld_bom_catalog + sld_bom_vs_matcher loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load BOM from Delta

# COMMAND ----------

# DBTITLE 1,Phase 1 preview — canonical normalisation sample
# Quick sanity check: show normalisation for first 3 components of first file
_preview_rows = spark.sql(f"""
    SELECT bom_json FROM {TABLE_NAME}
    WHERE file_name = '{FILES_TO_PROCESS[0].replace("'", "")}' AND status = 'SUCCESS'
""").collect()
if _preview_rows and _preview_rows[0]["bom_json"]:
    _preview = json.loads(_preview_rows[0]["bom_json"])[:3]
    print(f"Phase 1 — canonical field normalisation preview ({FILES_TO_PROCESS[0]}):")
    for i, comp in enumerate(_preview):
        c = normalize_component_fields(comp)
        vsq = build_vs_query(c)
        print(f"  [{i}] {c.get('component_type','?')} | {c.get('amperage_a','?')}A {c.get('poles','?')}p → VS query: '{vsq}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Setup shared resources (once)

# COMMAND ----------

# DBTITLE 1,Load stock, work orders, workspace credentials, and create table
from databricks.sdk import WorkspaceClient
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

# Stock and work-order tables (loaded once — small enough)
stock_list = [r.asDict() for r in spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.stock").collect()]
wo_list    = [r.asDict() for r in spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.work_orders").collect()]
print(f"✓ Stock: {len(stock_list)} rows | Work orders: {len(wo_list)} rows")

# Workspace client and token (created once)
w = WorkspaceClient()
_ctx            = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_host  = spark.conf.get("spark.databricks.workspaceUrl")
workspace_token = _ctx.apiToken().get()
print(f"✓ Workspace: {workspace_host} | VS index: {VS_INDEX_NAME}")
print(f"✓ LLM fallback: {'ENABLED' if AGENT_THRESHOLD < 99 else 'DISABLED'}")

# reference_matches table
MATCHES_TABLE   = f"{CATALOG}.{SCHEMA}.reference_matches"
_matches_schema = StructType([
    StructField("file_name",            StringType(),    True),
    StructField("component_idx",        IntegerType(),   True),
    StructField("component_summary",    StringType(),    True),
    StructField("suggested_references", StringType(),    True),
    StructField("selected_reference",   StringType(),    True),
    StructField("user_overridden",      BooleanType(),   True),
    StructField("status",               StringType(),    True),
    StructField("created_at",           TimestampType(), True),
    StructField("updated_at",           TimestampType(), True),
])
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MATCHES_TABLE} (
        file_name             STRING  COMMENT 'Source diagram',
        component_idx         INT     COMMENT 'Index in bom_json array',
        component_summary     STRING  COMMENT 'Human-readable component description',
        suggested_references  STRING  COMMENT 'JSON array of top N candidates with stock info',
        selected_reference    STRING  COMMENT 'User-selected reference (null until confirmed)',
        user_overridden       BOOLEAN COMMENT 'True if user changed the suggestion',
        status                STRING  COMMENT 'PENDING / ACCEPTED / OVERRIDDEN / SKIPPED',
        created_at            TIMESTAMP,
        updated_at            TIMESTAMP
    ) USING DELTA
    COMMENT 'Reference matching results — one row per component per diagram'
""")
print(f"✓ {MATCHES_TABLE} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Match all files (batch loop)

# COMMAND ----------

# DBTITLE 1,Match all files and write results
for _file_idx, FILE_NAME in enumerate(FILES_TO_PROCESS):
    print(f"\n{'='*60}")
    print(f"[{_file_idx+1}/{len(FILES_TO_PROCESS)}] {FILE_NAME}")
    print(f"{'='*60}")

    # Load BOM from Delta
    _rows = spark.sql(f"""
        SELECT bom_json, status FROM {TABLE_NAME}
        WHERE file_name = '{FILE_NAME.replace("'", "")}'
    """).collect()
    if not _rows or _rows[0]["status"] != "SUCCESS":
        print(f"  ⚠ Not found or not SUCCESS — skipping")
        continue
    bom_json = _rows[0]["bom_json"]
    if not bom_json or bom_json in ("[]", ""):
        print(f"  ⚠ bom_json empty — skipping")
        continue
    components = json.loads(bom_json)
    if not components:
        print(f"  ⚠ 0 components — skipping")
        continue
    print(f"  ✓ Loaded {len(components)} components")

    # Phase 2+3: VS matching with optional LLM fallback
    enriched = match_all_components(
        components      = components,
        vs_index_name   = VS_INDEX_NAME,
        workspace_client= w,
        stock_rows      = stock_list,
        wo_rows         = wo_list,
        top_n           = TOP_N,
        agent_threshold = AGENT_THRESHOLD,
        workspace_host  = f"https://{workspace_host}",
        workspace_token = workspace_token,
        model_endpoint  = DEFAULT_MODEL_ENDPOINT,
    )

    # Apply tier filter (optional)
    if PREFERRED_TIER:
        for comp in enriched:
            orig = comp.get("references", [])
            filtered = [r for r in orig if r.get("tier") == PREFERRED_TIER]
            comp["references"] = filtered if filtered else orig[:1]

    # Write to reference_matches (replace existing rows for this file)
    spark.sql(f"DELETE FROM {MATCHES_TABLE} WHERE file_name = '{FILE_NAME.replace(chr(39), '')}'")
    now = datetime.utcnow()
    match_rows = []
    for idx, comp in enumerate(enriched):
        c       = comp.get("_canonical") or normalize_component_fields(comp)
        ctype   = c.get("component_type", "?").replace("_", " ")
        amp     = c.get("amperage_a")
        circuit = c.get("circuit", "")
        panel   = c.get("panel", "")
        summary = (ctype
                   + (f" {int(amp)}A" if amp else "")
                   + (f" — {circuit}" if circuit else "")
                   + (f" [{panel}]" if panel else ""))
        refs    = comp.get("references", [])
        match_rows.append({
            "file_name":            FILE_NAME,
            "component_idx":        idx,
            "component_summary":    summary,
            "suggested_references": json.dumps(refs, ensure_ascii=False, default=str),
            "selected_reference":   refs[0]["reference"] if refs else None,
            "user_overridden":      False,
            "status":               "PENDING",
            "created_at":           now,
            "updated_at":           now,
        })
    spark.createDataFrame(match_rows, schema=_matches_schema).write.mode("append").saveAsTable(MATCHES_TABLE)

    # Per-file stats
    high    = sum(1 for c in enriched if c.get("_confidence") == "high")
    medium  = sum(1 for c in enriched if c.get("_confidence") == "medium")
    low     = sum(1 for c in enriched if c.get("_confidence") == "low")
    none_n  = sum(1 for c in enriched if c.get("_confidence") == "none")
    matched = sum(1 for c in enriched if c.get("references"))
    agnt    = sum(1 for c in enriched if any(r.get("agent_resolved") for r in c.get("references", [])))
    print(f"  ✓ {matched}/{len(enriched)} with refs | high={high} med={medium} low={low} none={none_n} | agent={agnt} | {len(match_rows)} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Final summary

# COMMAND ----------

# DBTITLE 1,Summary display
_fn_list = ", ".join("'" + f.replace("'", "") + "'" for f in FILES_TO_PROCESS)
summary_df = spark.sql(f"""
    SELECT file_name,
           COUNT(*)                                                              AS components,
           SUM(CASE WHEN selected_reference IS NOT NULL THEN 1 ELSE 0 END)      AS with_ref
    FROM {MATCHES_TABLE}
    WHERE file_name IN ({_fn_list})
    GROUP BY file_name ORDER BY file_name
""")
display(summary_df)
total_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {MATCHES_TABLE}").collect()[0]["n"]
print(f"\n✓ Total rows in {MATCHES_TABLE}: {total_rows}")
print(f"Status: READY FOR REVIEW in the web app")
