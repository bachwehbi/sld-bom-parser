# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM Extraction Pipeline
# MAGIC
# MAGIC Extracts electrical components (BOM) from Schneider Electric vectorized PDF diagrams using:
# MAGIC 1. **PyMuPDF** — rasterizes vector PDFs at high DPI to preserve fine details
# MAGIC 2. **Claude on Databricks** — Foundation Model API for vision-based component extraction
# MAGIC 3. **PyMuPDF vector text** — precise bounding boxes by matching BOM entries to PDF text positions
# MAGIC 4. **Delta table** — tracks processed files and stores structured BOM results
# MAGIC
# MAGIC ## Workflow overview
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog Volume (PDFs)
# MAGIC        │
# MAGIC        ▼
# MAGIC   1. Rasterize PDF at 400 DPI
# MAGIC        │  (large A0/A1 pages are split into overlapping tiles)
# MAGIC        ▼
# MAGIC   2. Claude vision model → JSON BOM
# MAGIC        │  (list of components with type, calibre, poles, circuit name …)
# MAGIC        ▼
# MAGIC   3. Match BOM → PDF vector text clusters
# MAGIC        │  (assigns precise pixel coordinates to each component)
# MAGIC        ▼
# MAGIC   4. Circuit-sharing pass
# MAGIC        │  (Relojes / Contactors inherit position from their Interruptor)
# MAGIC        ▼
# MAGIC   5. Write results to Delta table  +  save JPEG overlay
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites (Tier 1+)
# MAGIC
# MAGIC Before running this notebook for the first time on a new workspace:
# MAGIC
# MAGIC 1. Set `CATALOG` in `config.py` to your Unity Catalog catalog name
# MAGIC 2. Run `setup.py` — creates the schema, volume, `bom_extractions` table, system prompt, and Databricks Jobs
# MAGIC 3. Upload PDF diagrams to the UC volume (`{VOLUME_PATH}/`)
# MAGIC
# MAGIC > **Normal operation:** This notebook is triggered automatically by the `sld-bom-extraction` Databricks Job
# MAGIC > (created by `setup.py`). Run it manually only for debugging or one-off extractions.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC The pipeline tracks which files have already been processed in the Delta table.
# MAGIC Re-running the notebook is safe: it only processes **new** files found in the volume
# MAGIC that are not yet present in the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC ### Install dependencies
# MAGIC
# MAGIC Three packages are required and are not pre-installed on Databricks serverless compute:
# MAGIC
# MAGIC | Package | Purpose |
# MAGIC |---------|---------|
# MAGIC | **PyMuPDF** (`fitz`) | Rasterize PDF pages to images; extract vector text with bounding boxes |
# MAGIC | **Pillow** | Image tiling, resizing, drawing overlay boxes, JPEG/PNG encoding |
# MAGIC | **openai** | OpenAI-compatible client for the Databricks Foundation Model API |
# MAGIC
# MAGIC `dbutils.library.restartPython()` is required after `%pip` to make the newly installed packages available in the current Python session.

# COMMAND ----------

# DBTITLE 1,Install dependencies
%pip install PyMuPDF Pillow openai --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC
# MAGIC Shared paths and defaults come from `config` (see `config.py`). To deploy to a new environment, edit `CATALOG` there.
# MAGIC
# MAGIC **Retry behaviour:**
# MAGIC - `ENABLE_RETRY = True` — retries the Claude extraction + matching if the match rate falls below `MATCH_RATE_THRESHOLD`.
# MAGIC - `MAX_RETRIES` — number of additional attempts after the first (total calls = 1 + MAX_RETRIES).
# MAGIC - `MATCH_RATE_THRESHOLD` — minimum acceptable fraction of components matched to PDF vector text.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# ── Job parameters (overridable via dbutils.widgets when run as a Job) ──────
# When run interactively, these defaults are used.
# When triggered by the agent via jobs.run_now(), the agent passes widget values.
dbutils.widgets.text("file_name",  "",                              "PDF file name (e.g. AVILA.pdf)")
dbutils.widgets.text("model",      "databricks-claude-sonnet-4-6", "Model endpoint")
dbutils.widgets.text("enable_retry", "true",                       "Enable retry (true/false)")
dbutils.widgets.text("max_retries",  "2",                          "Max retries")
dbutils.widgets.text("threshold",    "0.75",                       "Match rate threshold (0-1)")

FILE_NAME            = dbutils.widgets.get("file_name").strip()
MODEL_ENDPOINT       = dbutils.widgets.get("model").strip() or "databricks-claude-sonnet-4-6"
ENABLE_RETRY         = dbutils.widgets.get("enable_retry").strip().lower() != "false"
MAX_RETRIES          = int(dbutils.widgets.get("max_retries").strip() or "2")
MATCH_RATE_THRESHOLD = float(dbutils.widgets.get("threshold").strip() or "0.75")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load pipeline module
# MAGIC
# MAGIC All extraction logic lives in `sld_bom_extractor.py` — a plain Python file stored in the Databricks workspace alongside this notebook.
# MAGIC The notebook only orchestrates; all algorithm details (clustering, scoring, circuit-sharing) are in the module.
# MAGIC
# MAGIC `importlib.reload()` ensures we always pick up the latest saved version of the module within the same Python session.

# COMMAND ----------

# DBTITLE 1,Import pipeline module
import sys
import os
import importlib

# Derive the workspace path of the current notebook's directory at runtime.
# This makes the import work for any user or folder, as long as
# sld_bom_extractor.py lives in the same directory as this notebook.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_module_dir = f"/Workspace{os.path.dirname(_notebook_path)}"
sys.path.insert(0, _module_dir)

import sld_bom_extractor as pipeline
importlib.reload(pipeline)

from sld_bom_extractor import (
    rasterize_pdf,              # PDF → list of base64 tile images
    extract_bom,                # images + Claude → JSON BOM string (direct, no retry)
    parse_json_from_response,   # strips prose preamble, returns parsed Python list
    match_bom_to_pdf_text,      # BOM list + PDF → (matched, unmatched) (direct, no retry)
    run_extraction,             # retry-aware wrapper: extract + match + quality gate
    generate_precision_overlay, # draws color-coded boxes on rasterized page (JPEG)
    generate_annotated_pdf,    # writes color-coded rect annotations onto PDF copy
    detect_pdf_type,            # classifies PDF as "vector", "scanned", or "unrecognized"
    DPI,                        # 400 — shared constant used across all stages
)

print(f"Pipeline loaded | DPI={DPI} | Model={MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks client and system prompt
# MAGIC
# MAGIC The Databricks Foundation Model API is OpenAI-API compatible, so we use the standard `openai.OpenAI` client
# MAGIC with the workspace URL and a short-lived notebook token — no hardcoded credentials needed.
# MAGIC
# MAGIC The system prompt is loaded from the volume at runtime. Keeping it in a separate `.txt` file means
# MAGIC prompt engineers can iterate on extraction instructions without touching notebook code,
# MAGIC and both this notebook and the comparison notebook always use the same prompt.

# COMMAND ----------

# DBTITLE 1,Databricks client + system prompt
import os
import json
from datetime import datetime
from openai import OpenAI

def get_client():
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    return OpenAI(base_url=f"https://{workspace_url}/serving-endpoints", api_key=token)

with open(PROMPT_FILE, "r", encoding="utf-8") as f:
    SYSTEM_PROMPT = f.read().strip()

print(f"System prompt loaded ({len(SYSTEM_PROMPT)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Tracking Table
# MAGIC
# MAGIC Creates the Delta table if it doesn't already exist. The table stores one row per processed PDF.
# MAGIC
# MAGIC | Column | Type | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `file_name` | STRING | Deduplication key — just the filename, so the table stays valid if the volume path changes |
# MAGIC | `file_path` | STRING | Full path at processing time, for audit purposes |
# MAGIC | `processed_at` | TIMESTAMP | When the extraction ran — useful to track which prompt version was active |
# MAGIC | `status` | STRING | `SUCCESS` or `ERROR` — lets downstream queries skip failures without inspecting `bom_json` |
# MAGIC | `error_message` | STRING | Truncated stack trace (max 2000 chars) for diagnosing failures |
# MAGIC | `bom_json` | STRING | Full extraction result as a JSON string. Stored as STRING (not a nested struct) so the schema can evolve without `ALTER TABLE` |
# MAGIC
# MAGIC **ACID writes:** `USING DELTA` guarantees that if the cluster crashes mid-append, the partial write is rolled back.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    file_name      STRING    COMMENT 'PDF filename',
    file_path      STRING    COMMENT 'Full volume path',
    processed_at   TIMESTAMP COMMENT 'When extraction was run',
    status         STRING    COMMENT 'IN_PROGRESS, SUCCESS or ERROR',
    error_message  STRING    COMMENT 'Error details if failed',
    bom_json       STRING    COMMENT 'JSON array of extracted components',
    attempts_made  INT       COMMENT 'Number of extraction attempts made (retry logic)',
    threshold_met  BOOLEAN   COMMENT 'Whether match rate threshold was reached',
    progress_msg   STRING    COMMENT 'Latest progress message for live status polling',
    pdf_type       STRING    COMMENT 'vector, scanned, or unrecognized — drives overlay availability'
)
USING DELTA
COMMENT 'BOM extraction results from electrical diagram PDFs'
""")

# Ensure pdf_type column exists in tables created before this column was added
try:
    spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMN pdf_type STRING COMMENT 'vector, scanned, or unrecognized'")
    print("Added pdf_type column to existing table.")
except Exception:
    pass  # column already exists

os.makedirs(OVERLAY_PATH, exist_ok=True)
print(f"Table {TABLE_NAME} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Discover Unprocessed Files
# MAGIC
# MAGIC Lists all PDFs in the volume and subtracts those already recorded in the tracking table.
# MAGIC
# MAGIC **Why `dbutils.fs.ls` instead of `os.listdir`:**
# MAGIC Unity Catalog volumes are backed by object storage. `os.listdir` uses a FUSE mount that may not
# MAGIC reflect newly uploaded files immediately. `dbutils.fs.ls` queries the object store directly and is always up to date.
# MAGIC
# MAGIC **Retrying failed files:** `processed_set` includes files with `status = 'ERROR'` — they are NOT retried automatically.
# MAGIC To retry a failed file, delete its row from the table and re-run this notebook.

# COMMAND ----------

import fitz as _fitz   # PyMuPDF — already installed via %pip above

def _page_file_name(stem, page_idx, total_pages):
    """Return the tracking key for a single extracted page.

    Single-page PDFs keep their original name; multi-page PDFs get a
    zero-padded page suffix: 'CARRIAZO CGBT_p01.pdf', '_p02.pdf', …
    """
    if total_pages == 1:
        return f"{stem}.pdf"
    return f"{stem}_p{page_idx + 1:02d}.pdf"


def _split_to_temp_pages(volume_path):
    """Split a PDF into single-page temp files.

    Returns a list of (page_idx, temp_path) tuples. For single-page PDFs
    the original path is returned directly (no copy needed).
    """
    doc = _fitz.open(volume_path)
    n   = len(doc)
    doc.close()
    if n == 1:
        return [(0, volume_path)]
    # Multi-page: write each page as a temp PDF
    results = []
    doc = _fitz.open(volume_path)
    for i in range(n):
        tmp = f"/tmp/_page_{i:02d}_{os.path.basename(volume_path)}"
        page_doc = _fitz.open()
        page_doc.insert_pdf(doc, from_page=i, to_page=i)
        page_doc.save(tmp)
        page_doc.close()
        results.append((i, tmp))
    doc.close()
    return results


all_pdfs      = [f.name for f in dbutils.fs.ls(f"dbfs:{VOLUME_PATH}") if f.name.lower().endswith(".pdf")]
processed_set = {row.file_name for row in spark.sql(f"SELECT file_name FROM {TABLE_NAME} WHERE status != 'IN_PROGRESS'").collect()}

# Expand each source PDF into its per-page tracking keys so we know which
# pages still need processing (works for both single- and multi-page PDFs).
def _source_pdfs_to_process():
    """Return list of (source_pdf_name, page_idx, page_file_name) to process."""
    to_do = []
    source_pdfs = [FILE_NAME] if FILE_NAME else all_pdfs
    for pdf_name in source_pdfs:
        vol_path = f"{VOLUME_PATH}/{pdf_name}"
        try:
            doc = _fitz.open(vol_path)
            n   = len(doc)
            doc.close()
        except Exception:
            n = 1   # fallback — will fail properly during extraction
        stem = os.path.splitext(pdf_name)[0]
        for i in range(n):
            pg_name = _page_file_name(stem, i, n)
            if pg_name not in processed_set:
                to_do.append((pdf_name, i, n, pg_name))
    return to_do

if FILE_NAME and FILE_NAME not in all_pdfs:
    raise ValueError(f"File '{FILE_NAME}' not found in volume {VOLUME_PATH}")

work_items = _source_pdfs_to_process()
print(f"{'Single-file' if FILE_NAME else 'Batch'} mode | Source PDFs: {len(set(w[0] for w in work_items))} | Pages to process: {len(work_items)}")
for src, pi, n, pg in work_items:
    suffix = f" (page {pi+1}/{n})" if n > 1 else ""
    print(f"  - {pg}{suffix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Process New Files
# MAGIC
# MAGIC For each new PDF, the pipeline runs 4 sequential stages:
# MAGIC
# MAGIC 1. **Rasterize** — convert PDF to high-resolution image(s); large A0/A1 pages are automatically tiled
# MAGIC 2. **Extract + Match** *(retry-aware)* — call `run_extraction()` which runs Claude extraction followed by vector text matching, retrying if the match rate falls below `MATCH_RATE_THRESHOLD`. The best result across all attempts is kept.
# MAGIC 3. **Overlay** — draw color-coded bounding boxes on the rasterized page and save as JPEG for visual QA
# MAGIC 4. **Persist** — append one row to the Delta table with status, BOM JSON, retry metadata, and timestamp
# MAGIC
# MAGIC Both matched and unmatched components are stored in `bom_json`. Use the `match_status` column in the results view to distinguish them.
# MAGIC
# MAGIC `attempts_made` and `threshold_met` are recorded in the Delta table so you can identify which diagrams required retries or never reached the quality threshold.
# MAGIC On any failure, an `ERROR` row is written so the file is skipped on subsequent runs.

# COMMAND ----------

def write_progress(file_name, file_path, status, progress_msg, error_message=None,
                   bom_json=None, attempts_made=None, threshold_met=None, pdf_type=None):
    """MERGE progress state into the Delta table.

    Uses MERGE so we can update the same row from IN_PROGRESS → SUCCESS/ERROR
    without creating duplicates. The web app polls `status` + `progress_msg`
    to show a live progress indicator while the job runs.
    """
    from pyspark.sql import Row
    fn_escaped  = file_name.replace("'", "''")
    msg_escaped = (progress_msg or "").replace("'", "''")
    err_escaped = (str(error_message)[:2000] if error_message else "NULL")
    err_value   = f"'{err_escaped}'" if error_message else "NULL"
    bom_escaped = (bom_json or "").replace("'", "''") if bom_json else None
    bom_value   = f"'{bom_escaped}'" if bom_json else "NULL"
    pdf_value   = f"'{pdf_type}'" if pdf_type else "NULL"

    spark.sql(f"""
        MERGE INTO {TABLE_NAME} AS t
        USING (SELECT '{fn_escaped}' AS file_name) AS s
        ON t.file_name = s.file_name
        WHEN MATCHED THEN UPDATE SET
            status        = '{status}',
            progress_msg  = '{msg_escaped}',
            processed_at  = CURRENT_TIMESTAMP(),
            error_message = {err_value},
            bom_json      = {bom_value},
            attempts_made = {attempts_made if attempts_made is not None else 'NULL'},
            threshold_met = {str(threshold_met).upper() if threshold_met is not None else 'NULL'},
            pdf_type      = {pdf_value}
        WHEN NOT MATCHED THEN INSERT (
            file_name, file_path, processed_at, status, progress_msg,
            error_message, bom_json, attempts_made, threshold_met, pdf_type
        ) VALUES (
            '{fn_escaped}', '{file_path.replace("'", "''")}', CURRENT_TIMESTAMP(),
            '{status}', '{msg_escaped}',
            {err_value}, {bom_value},
            {attempts_made if attempts_made is not None else 'NULL'},
            {str(threshold_met).upper() if threshold_met is not None else 'NULL'},
            {pdf_value}
        )
    """)


if not work_items:
    print("Nothing to process — all pages already extracted.")
else:
    client = get_client()

    # Group work_items by source PDF so we split once per source file
    from itertools import groupby
    import tempfile

    # Process all pages, split source PDFs once per group
    prev_src = None
    temp_pages = {}   # page_idx → temp_path for the current source PDF

    for src_name, page_idx, total_pages, page_file_name in work_items:
        # Re-split when the source PDF changes
        if src_name != prev_src:
            # Clean up temp files from previous source PDF
            for tp in temp_pages.values():
                if tp != f"{VOLUME_PATH}/{prev_src}":   # don't delete originals
                    try: os.remove(tp)
                    except: pass
            src_vol_path = f"{VOLUME_PATH}/{src_name}"
            splits = _split_to_temp_pages(src_vol_path)
            temp_pages = {pi: tp for pi, tp in splits}
            prev_src = src_name
            print(f"\n{'='*60}\nSource: {src_name} ({total_pages} page(s))")

        file_path = temp_pages[page_idx]
        print(f"\n  --- Page {page_idx+1}/{total_pages}: {page_file_name}")

        write_progress(page_file_name, file_path, "IN_PROGRESS", "Starting extraction...")

        try:
            # Stage 0 — Detect PDF type (vector / scanned / unrecognized)
            pdf_type = detect_pdf_type(file_path)
            print(f"    PDF type: {pdf_type}")

            # Stage 1 — Rasterize
            write_progress(page_file_name, file_path, "IN_PROGRESS", f"Rasterizing at {DPI} DPI")
            images = rasterize_pdf(file_path)
            print(f"    Got {len(images)} image tile(s)")

            # Stage 2 — Extract + Match (retry-aware)
            write_progress(page_file_name, file_path, "IN_PROGRESS",
                           f"Calling {MODEL_ENDPOINT} (up to {1 + MAX_RETRIES} attempts)")
            result = run_extraction(
                client, SYSTEM_PROMPT, images, file_path, MODEL_ENDPOINT,
                enable_retry=ENABLE_RETRY,
                max_retries=MAX_RETRIES,
                threshold=MATCH_RATE_THRESHOLD,
                verbose=True,
                progress_callback=lambda msg: write_progress(page_file_name, file_path, "IN_PROGRESS", msg),
            )
            matched       = result["matched"]
            unmatched     = result["unmatched"]
            attempts_made = result["attempts_made"]
            threshold_met = result["threshold_met"]
            final_rate    = result["final_match_rate"]
            print(f"    {len(matched)}/{len(matched)+len(unmatched)} matched ({final_rate:.0%}) in {attempts_made} attempt(s)")

            bom_json = json.dumps(matched + unmatched, ensure_ascii=False)

            # Stage 3 — Overlay artifacts (JPEG + annotated PDF)
            # Skip for scanned/unrecognized: no vector text means no precise coordinates
            stem = os.path.splitext(page_file_name)[0]
            if pdf_type == "vector":
                write_progress(page_file_name, file_path, "IN_PROGRESS", "Generating overlay")
                overlay_file = f"{stem}_overlay.jpg"
                annot_file   = f"{stem}_annotated.pdf"
                n  = generate_precision_overlay(file_path, matched, unmatched, f"{OVERLAY_PATH}/{overlay_file}")
                n2 = generate_annotated_pdf(file_path, matched, unmatched, f"{OVERLAY_PATH}/{annot_file}")
                print(f"    Overlay: {n} boxes | Annotated PDF: {n2} annotations")
            else:
                print(f"    Overlay skipped ({pdf_type} PDF — no vector text)")

            # Stage 4 — Final write (SUCCESS)
            write_progress(page_file_name, file_path, "SUCCESS",
                           f"{len(matched)}/{len(matched)+len(unmatched)} matched ({final_rate:.0%}) in {attempts_made} attempt(s)",
                           bom_json=bom_json,
                           attempts_made=attempts_made,
                           threshold_met=threshold_met,
                           pdf_type=pdf_type)

        except Exception as e:
            print(f"    ERROR: {e}")
            write_progress(page_file_name, file_path, "ERROR", f"Failed: {str(e)[:200]}",
                           error_message=str(e)[:2000])

    # Clean up last batch of temp files
    for tp in temp_pages.values():
        if prev_src and tp != f"{VOLUME_PATH}/{prev_src}":
            try: os.remove(tp)
            except: pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. View Results
# MAGIC ### Extraction status

# COMMAND ----------

# DBTITLE 1,All extractions
display(spark.sql(f"SELECT file_name, status, processed_at FROM {TABLE_NAME} ORDER BY processed_at DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploded BOM — one row per component
# MAGIC
# MAGIC Parses `bom_json` into individual rows using `from_json`. Each row is one electrical component.
# MAGIC
# MAGIC The `match_status` column indicates how reliably each component was positioned:
# MAGIC
# MAGIC | `match_status` | Meaning | Position data? |
# MAGIC |---|---|---|
# MAGIC | `matched` | Position confirmed directly by PDF vector text cluster | Yes — most reliable |
# MAGIC | `circuit_shared` | No direct spec match; position inherited from a sibling component on the same circuit (e.g. a Reloj next to its Interruptor) | Yes — reliable position, weaker electrical evidence |
# MAGIC | `unmatched` | Claude extracted the component but no PDF cluster could be assigned | No — review manually |
# MAGIC
# MAGIC Results are sorted by `match_status` so unmatched rows appear first. Filter on `match_status = 'unmatched'` to isolate them.

# COMMAND ----------

# DBTITLE 1,Exploded BOM — one row per component
display(spark.sql(f"""
SELECT
    file_name,
    elem['Que es']              AS tipo,
    elem['Calibre (A)']         AS calibre_a,
    elem['Curva']               AS curva,
    elem['Poder de Corte (kA)'] AS poder_corte_ka,
    elem['Polos']               AS polos,
    elem['Sensibilidad (mA)']   AS sensibilidad_ma,
    elem['Cuadro']              AS cuadro,
    elem['Circuito']            AS circuito,
    elem['precise_cx']          AS pos_x,
    elem['precise_cy']          AS pos_y,
    elem['match_score']         AS match_score,
    CASE
        WHEN elem['precise_cx']  IS NULL             THEN 'unmatched'
        WHEN elem['match_type'] = 'circuit_shared'   THEN 'circuit_shared'
        ELSE 'matched'
    END                         AS match_status
FROM (
    SELECT file_name, explode(from_json(bom_json, 'ARRAY<MAP<STRING,STRING>>')) AS elem
    FROM {TABLE_NAME}
    WHERE status = 'SUCCESS'
)
ORDER BY file_name, match_status, cuadro, circuito
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display Overlay Images
# MAGIC
# MAGIC Renders the JPEG overlays saved during processing. Each image shows the diagram with color-coded bounding boxes:
# MAGIC
# MAGIC | Color | Component type |
# MAGIC |-------|---------------|
# MAGIC | 🔴 Red | Interruptor automático (MCB / MCCB) |
# MAGIC | 🔵 Blue | Interruptor diferencial (RCD / RCCB) |
# MAGIC | 🟢 Green | Contactor |
# MAGIC | 🟠 Orange | Reloj (time switch) |
# MAGIC | 🟣 Purple | Limitador de sobretensión (SPD) |
# MAGIC | 🩵 Cyan | Contador de energía (energy meter) |
# MAGIC | ⚫ Grey | Any type not in the color map |
# MAGIC
# MAGIC **Unmatched components are not drawn.** Their absence on the overlay highlights gaps where the pipeline could not locate a component.

# COMMAND ----------

import matplotlib
matplotlib.use("Agg")  # non-interactive backend required in Databricks notebook environments
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

overlay_files = [f.name for f in dbutils.fs.ls(f"dbfs:{OVERLAY_PATH}") if f.name.endswith(".jpg")]

for overlay_name in sorted(overlay_files):
    fig, ax = plt.subplots(1, 1, figsize=(24, 16))
    ax.imshow(mpimg.imread(f"{OVERLAY_PATH}/{overlay_name}"))
    ax.set_title(overlay_name.replace("_overlay.jpg", ""), fontsize=14)
    ax.axis("off")
    plt.tight_layout()
    plt.show()
