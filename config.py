# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Configuration
# MAGIC
# MAGIC This notebook is **not run directly** — it is included by other notebooks via `%run ./config`.
# MAGIC
# MAGIC To deploy to a new environment, change `CATALOG` (and `SCHEMA` if needed) here.
# MAGIC All paths, table names, and model references are derived automatically.

# COMMAND ----------

# DBTITLE 1,Environment — edit for new deployments
CATALOG = "serverless_stable_bach_catalog"   # ← only value to change per environment
SCHEMA  = "bom_parser"

# COMMAND ----------

# DBTITLE 1,Derived paths — do not edit
VOLUME           = "electrical_diagrams"
TABLE_NAME       = f"{CATALOG}.{SCHEMA}.bom_extractions"
VOLUME_PATH      = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
OVERLAY_PATH     = f"{VOLUME_PATH}/overlays"
PROMPT_FILE      = f"{VOLUME_PATH}/sld2bom_system_prompt.txt"
AGENT_MODEL_NAME = f"{CATALOG}.{SCHEMA}.sld_bom_agent"

# COMMAND ----------

# DBTITLE 1,Default extraction parameters
DEFAULT_MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"
DEFAULT_ENABLE_RETRY   = True
DEFAULT_MAX_RETRIES    = 1    # 1 retry in prod; set to 0 for dev/testing
DEFAULT_THRESHOLD      = 0.75
