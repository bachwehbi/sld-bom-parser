# Databricks notebook source
# MAGIC %md
# MAGIC # SLD-to-BOM Parser — Project README
# MAGIC
# MAGIC > **Automatically extracts a Bill of Materials (BOM) from Schneider Electric electrical diagram PDFs (Single Line Diagrams), assigns precise component positions, and surfaces results through a conversational web app.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table of Contents
# MAGIC
# MAGIC 1. [Architecture overview](#architecture-overview)
# MAGIC 2. [Repository structure](#repository-structure)
# MAGIC 3. [Notebook guide](#notebook-guide)
# MAGIC    - [setup.py](#setuppy--environment-setup)
# MAGIC    - [sld_bom_extractor.py](#sld_bom_extractorpy--core-module)
# MAGIC    - [sld2bom_system_prompt.txt](#sld2bom_system_prompttxt--extraction-prompt)
# MAGIC    - [sld_to_bom_pipeline.py](#sld_to_bom_pipelinepy--production-pipeline)
# MAGIC    - [sld_bom_catalog.py](#sld_bom_catalogpy--matching-module)
# MAGIC    - [sld_bom_vs_matcher.py](#sld_bom_vs_matcherpy--vector-search-matching)
# MAGIC    - [sld_bom_matching_nb.py](#sld_bom_matching_nbpy--matching-pipeline)
# MAGIC    - [generate_material_data.py](#generate_material_datapy--catalog-data)
# MAGIC    - [sld_bom_agent_model.py](#sld_bom_agent_modelpy--agent-class)
# MAGIC    - [sld_bom_agent_uc.py](#sld_bom_agent_ucpy--agent-registration)
# MAGIC    - [app/](#app--web-application)
# MAGIC 4. [Deployment tiers](#deployment-tiers)
# MAGIC 5. [Unity Catalog layout](#unity-catalog-layout)
# MAGIC 6. [User guide](#user-guide)
# MAGIC    - [Tier 1 — Extraction only](#tier-1--extraction-only)
# MAGIC    - [Tier 2 — + Reference matching](#tier-2----reference-matching)
# MAGIC    - [Tier 3 — + Agent](#tier-3----agent)
# MAGIC    - [Tier 4 — Full with web app](#tier-4--full-with-web-app)
# MAGIC    - [Upload a new diagram and run extraction](#upload-a-new-diagram-and-run-extraction)
# MAGIC    - [Run extraction from a notebook directly](#run-extraction-from-a-notebook-directly)
# MAGIC    - [Talk to the agent](#talk-to-the-agent)
# MAGIC    - [Download an annotated PDF](#download-an-annotated-pdf)
# MAGIC    - [Re-register the agent after code changes](#re-register-the-agent-after-code-changes)
# MAGIC    - [Deploy / redeploy the web app](#deploy--redeploy-the-web-app)
# MAGIC 7. [Configuration reference](#configuration-reference)
# MAGIC 8. [Troubleshooting](#troubleshooting)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Architecture overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │  Unity Catalog Volume  (/electrical_diagrams/)                      │
# MAGIC │   • *.pdf          — input diagrams                                 │
# MAGIC │   • overlays/      — JPEG overlays + annotated PDFs                 │
# MAGIC │   • sld2bom_system_prompt.txt                                       │
# MAGIC └───────────────────┬─────────────────────────────────────────────────┘
# MAGIC                     │
# MAGIC              ┌──────▼──────┐
# MAGIC              │  Pipeline   │  sld_to_bom_pipeline.py
# MAGIC              │  Notebook   │  (run manually or via Databricks Job)
# MAGIC              └──────┬──────┘
# MAGIC                     │  calls
# MAGIC              ┌──────▼──────┐
# MAGIC              │  Extractor  │  sld_bom_extractor.py
# MAGIC              │  Module     │  Vision → Vector matching → Overlay
# MAGIC              └──────┬──────┘
# MAGIC                     │  writes
# MAGIC          ┌───────────▼───────────┐
# MAGIC          │  Delta Table           │  bom_parser.bom_extractions
# MAGIC          │  (BOM + coordinates)   │
# MAGIC          └───────────┬───────────┘
# MAGIC                      │
# MAGIC       ┌──────────────┴──────────────┐
# MAGIC       │                             │
# MAGIC ┌─────▼──────┐              ┌───────▼──────┐
# MAGIC │  Web App   │              │  Agent        │
# MAGIC │ (FastAPI + │              │  (MLflow       │
# MAGIC │  React)    │              │  ResponsesAgent│
# MAGIC │            │              │  on Serving)   │
# MAGIC │  Upload    │              │                │
# MAGIC │  Review    │              │  Natural-lang  │
# MAGIC │  Download  │              │  interface to  │
# MAGIC └────────────┘              │  the pipeline  │
# MAGIC                             └────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Extraction pipeline (3 stages)
# MAGIC
# MAGIC | Stage | What happens |
# MAGIC |-------|-------------|
# MAGIC | **1 — Vision** | PDF rasterized to 400 DPI image → sent to Claude vision → returns JSON BOM (type, calibre, poles, circuit…) |
# MAGIC | **2 — Vector match** | PyMuPDF extracts every text span from the PDF vector layer → clusters nearby annotations → each BOM entry is matched to the closest cluster → assigned sub-pixel bounding box |
# MAGIC | **3 — Circuit sharing** | Components without a vector match (e.g. Relojes) inherit the position of a matched sibling in the same circuit |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Repository structure
# MAGIC
# MAGIC ```
# MAGIC bom_parser/
# MAGIC ├── README.py                    ← this file
# MAGIC ├── config.py                    ← shared config (CATALOG, paths, defaults) — only file to edit per environment
# MAGIC ├── setup.py                     ← run once on a new workspace to provision infra
# MAGIC ├── sld2bom_system_prompt.txt    ← extraction prompt (source of truth, copied to volume by setup.py)
# MAGIC │
# MAGIC ├── sld_bom_extractor.py         ← core extraction module (pure Python, no Databricks deps)
# MAGIC ├── sld_to_bom_pipeline.py       ← production extraction pipeline notebook
# MAGIC ├── sld_bom_catalog.py           ← reference matching module (pure Python, no Databricks deps)
# MAGIC ├── sld_bom_vs_matcher.py        ← Vector Search retrieval + property re-ranking + LLM fallback (pure Python)
# MAGIC ├── sld_bom_matching_nb.py       ← production reference matching pipeline notebook
# MAGIC ├── generate_material_data.py    ← one-time: creates material, stock, work_orders tables
# MAGIC ├── sld_bom_agent_model.py       ← MLflow code-based agent model file (⚠ update hardcoded catalog/job IDs)
# MAGIC ├── sld_bom_agent_uc.py          ← agent registration & serving endpoint notebook
# MAGIC │
# MAGIC └── app/                         ← Databricks App source (deployed from bom_parser/app/)
# MAGIC     ├── app.py                   ← FastAPI entry point
# MAGIC     ├── app.yaml                 ← Databricks App manifest (edit catalog + job ID for new env)
# MAGIC     ├── start.sh                 ← startup script (builds frontend, starts uvicorn)
# MAGIC     ├── server/
# MAGIC     │   ├── config.py            ← shared config (catalog, volume, table, endpoint)
# MAGIC     │   └── routes/
# MAGIC     │       ├── upload.py        ← POST /api/upload, POST /api/extract
# MAGIC     │       ├── diagrams.py      ← GET /api/diagrams, GET /api/annotated/{file}
# MAGIC     │       ├── chat.py          ← POST /api/chat (proxies agent serving endpoint)
# MAGIC     │       └── matching.py      ← POST /api/match, GET /api/matches/{file}, PATCH /api/matches,
# MAGIC     │                               POST /api/export/{file}, GET /api/exports/{file}
# MAGIC     └── frontend/                ← React + Tailwind UI (built to static/)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook guide
# MAGIC
# MAGIC ### `config.py` — Shared configuration
# MAGIC
# MAGIC **What it is:** Not run directly — included by every other notebook via `%run ./config`.
# MAGIC
# MAGIC **What it defines:**
# MAGIC - `CATALOG` / `SCHEMA` — the only values to change when deploying to a new environment
# MAGIC - All derived paths: `VOLUME_PATH`, `OVERLAY_PATH`, `PROMPT_FILE`, `TABLE_NAME`, `AGENT_MODEL_NAME`
# MAGIC - Default extraction parameters: `DEFAULT_MODEL_ENDPOINT`, `DEFAULT_ENABLE_RETRY`, `DEFAULT_MAX_RETRIES`, `DEFAULT_THRESHOLD`
# MAGIC
# MAGIC > To deploy to a new environment: open `config.py`, change `CATALOG` — all notebooks pick it up automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `setup.py` — Environment setup
# MAGIC
# MAGIC **When to use:** Run this notebook **once** on a new workspace before deploying the app or running the pipeline.
# MAGIC
# MAGIC **What it does:**
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Creates the UC schema |
# MAGIC | 2 | Creates the `electrical_diagrams` volume |
# MAGIC | 3 | Creates the `overlays/` and `exports/` subdirectories in the volume |
# MAGIC | 4 | Copies `sld2bom_system_prompt.txt` from the workspace folder to the volume |
# MAGIC | 5 | Creates the `bom_extractions` Delta table |
# MAGIC | 6 | Creates the `reference_matches` Delta table |
# MAGIC | 7 | Creates the `exports` Delta table |
# MAGIC | 8 | Verifies all components with a pass/fail summary |
# MAGIC
# MAGIC **Before running:** set `CATALOG = "your_catalog"` at the top. The catalog must already exist and you need `CREATE SCHEMA` privilege on it.
# MAGIC
# MAGIC > `sld2bom_system_prompt.txt` must be in the **same workspace folder** as `setup.py` — it is the source of truth for the prompt and is uploaded to the volume by this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_extractor.py` — Core module
# MAGIC
# MAGIC **What it is:** A plain Python module (not a notebook). Contains all the extraction algorithm — no Databricks dependencies so it can also be run locally or in unit tests.
# MAGIC
# MAGIC **What it does:**
# MAGIC - `rasterize_pdf(path, dpi)` — converts a PDF page to a base64 PNG image for the vision model. Large A0/A1 pages are split into overlapping tiles.
# MAGIC - `run_extraction(image_b64, client, model, system_prompt)` → JSON list of components
# MAGIC - `match_components(bom, pdf_path, dpi)` → adds `precise_cx/cy/x0/y0/x1/y1` to each entry
# MAGIC - `generate_precision_overlay(pdf_path, matched, unmatched, output_path)` → saves JPEG overlay
# MAGIC - `generate_annotated_pdf(pdf_path, matched, unmatched, output_path)` → saves colour-coded annotated PDF with component metadata embedded as annotations
# MAGIC
# MAGIC **Do not run directly** — it is imported by the pipeline notebook and the agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld2bom_system_prompt.txt` — Extraction prompt
# MAGIC
# MAGIC **What it is:** The Claude system prompt that drives Stage 1 (vision extraction). Stored in the repo root and copied to the UC volume by `setup.py`. The pipeline reads it from the volume at runtime — **you can update the prompt without touching any code or redeploying anything**.
# MAGIC
# MAGIC #### Design decisions
# MAGIC
# MAGIC **Canonical English schema (multilingual input)**
# MAGIC
# MAGIC Earlier versions of the prompt returned Spanish field names (`"Que és"`, `"Calibre (A)"`, `"Polos"`, `"Circuito"`) reflecting the language of the first test diagrams (Schneider España SLDs). This caused two problems:
# MAGIC - The matching code had to branch on language when reading field values.
# MAGIC - Any non-Spanish diagram produced inconsistent field names.
# MAGIC
# MAGIC The prompt was rewritten to use a **canonical English schema** regardless of the diagram language:
# MAGIC
# MAGIC | Field | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `component_type` | enum | One of 9 fixed values (`circuit_breaker`, `rcd`, `contactor`, …) |
# MAGIC | `amperage_a` | number | Rated current in A |
# MAGIC | `poles` | number | Pole count (Roman numerals or diagonal lines counted correctly) |
# MAGIC | `curve` | string | Trip curve: B / C / D |
# MAGIC | `breaking_ka` | number | Breaking capacity in kA |
# MAGIC | `sensitivity_ma` | number | Differential sensitivity in mA (rcd only) |
# MAGIC | `rcd_type` | string | AC / A / A-SI |
# MAGIC | `rcd_block_type` | string | standalone / vigi_block |
# MAGIC | `panel` | string | Panel / board name |
# MAGIC | `circuit` | string | Reference designator + load name (see below) |
# MAGIC
# MAGIC The `component_type` enum doubles as a normalisation layer — the table maps all Spanish, French, German, and English aliases to a single value, so downstream code never needs to handle aliases.
# MAGIC
# MAGIC **Reference designators in the `circuit` field**
# MAGIC
# MAGIC Schneider SLDs print a short reference designator directly beside each component symbol (e.g. `D35`, `I17`, `DX1`). These designators are the primary key that the Stage 2 vector-text matching algorithm uses to link a Claude-extracted BOM entry to its exact position in the PDF.
# MAGIC
# MAGIC Early prompt versions extracted only the human-readable load name (`"ALUMBRADO 22"`). This worked when a page had components with distinct specs (calibre / poles distinguish them). It failed on pages where many components share identical specs — e.g. nine 2P 40A 30mA RCDs on a single page. Without the designator, the matching algorithm has no anchor and falls back to the circuit-sharing heuristic, stacking all RCDs on top of the first matched MCB.
# MAGIC
# MAGIC The prompt now instructs Claude to **always prefix the reference designator** when it is legible:
# MAGIC
# MAGIC ```
# MAGIC "circuit": "D35 - ALUMBRADO 22"    ← MCB with designator D35
# MAGIC "circuit": "I17 - ALUMBRADO 22"    ← RCD above it with designator I17
# MAGIC "circuit": "DX1 - OFFICE BARRA 1"  ← DX-prefixed RCD variant
# MAGIC ```
# MAGIC
# MAGIC The Stage 2 `ID_PATTERNS` regex (`^[DIQC]\d+`, `^DX`, `^TX`) extracts the designator from the PDF text cluster. The matching scorer then checks `designator in circuit_string` for a +10 score bonus, which uniquely pins each component to its column even when all other specs are identical.
# MAGIC
# MAGIC > **Updating the prompt:** Edit `sld2bom_system_prompt.txt` locally, then upload it to the volume (`setup.py` cell 4, or the Databricks volume browser). The pipeline reads it fresh on every run — no notebook restart needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_to_bom_pipeline.py` — Production pipeline
# MAGIC
# MAGIC **When to use:** Run this notebook when you want to process one or more PDFs directly — for example, to re-extract a specific diagram, bulk-process new uploads, or test a change to the extractor.
# MAGIC
# MAGIC **Cells summary:**
# MAGIC
# MAGIC | Cell | What it does |
# MAGIC |------|-------------|
# MAGIC | 1 — Install deps | `%pip install PyMuPDF Pillow openai` |
# MAGIC | 2 — Config + widgets | Sets catalog/schema/volume paths; exposes widgets for `file_name`, `model`, `enable_retry`, `max_retries`, `threshold` |
# MAGIC | 3 — Load module | Imports `sld_bom_extractor` from the same workspace folder |
# MAGIC | 4 — OpenAI client | Creates the Databricks Foundation Model API client; loads system prompt from volume |
# MAGIC | 5 — Create table | Creates `bom_extractions` Delta table if it doesn't exist |
# MAGIC | 6 — Discover files | Lists PDFs in the volume that are not yet in the Delta table (or the single file from the widget) |
# MAGIC | 7 — Process loop | For each file: rasterize → extract → match → retry if below threshold → save JPEG overlay + annotated PDF → MERGE into Delta table |
# MAGIC | 8 — Status table | Displays a summary table of all processed files |
# MAGIC | 9 — BOM SQL view | Shows the full BOM as a SQL `SELECT` with exploded components |
# MAGIC | 10 — Overlay preview | Displays JPEG overlays inline via matplotlib |
# MAGIC
# MAGIC **Key behaviours:**
# MAGIC - **Idempotent in batch mode** — skips files already present in the Delta table (any status). Files with `ERROR` status are also skipped in batch mode; use single-file mode to retry them.
# MAGIC - **Retry logic** — if the match rate is below the threshold, the full vision+matching cycle is repeated up to `max_retries` additional times. The best result across all attempts is kept.
# MAGIC - **Annotated PDF** — each run produces both a JPEG overlay (`*_overlay.jpg`) and an annotated PDF (`*_annotated.pdf`) in the `overlays/` folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_catalog.py` — Matching module
# MAGIC
# MAGIC **What it is:** A pure Python module (no Databricks dependencies) that normalises extracted BOM fields and scores catalog candidates.
# MAGIC
# MAGIC **Public API:**
# MAGIC - `normalize_component_fields(comp)` — maps any language variant to the canonical English schema
# MAGIC - `build_vs_query(comp)` — builds the natural-language search string sent to Vector Search
# MAGIC - `score_candidate(comp, ref)` — scores a catalog reference against a component on calibre, poles, curve, sensitivity, etc.
# MAGIC - `resolve_stock(ref, stock_rows, work_orders)` — enriches a reference candidate with availability data
# MAGIC
# MAGIC **Do not run directly** — imported by `sld_bom_vs_matcher.py` and `sld_bom_matching_nb.py`.
# MAGIC
# MAGIC **Required by:** Tier 2+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_vs_matcher.py` — Vector Search matching
# MAGIC
# MAGIC **What it is:** A pure Python module that implements the three-phase reference matching pipeline.
# MAGIC
# MAGIC | Phase | What it does |
# MAGIC |-------|-------------|
# MAGIC | 1 | Field normalisation via `sld_bom_catalog` |
# MAGIC | 2 | Vector Search retrieval (semantic similarity) → property re-ranking |
# MAGIC | 3 | LLM agentic fallback for low-confidence components (score < threshold) |
# MAGIC
# MAGIC **Entry point:** `match_all_components(components, vs_index_name, warehouse_id, top_n, agent_threshold)`
# MAGIC
# MAGIC **Do not run directly** — imported by `sld_bom_matching_nb.py`.
# MAGIC
# MAGIC **Requires:** A Databricks Vector Search endpoint and index (`material_vs_index`) created on the `material` catalog table.
# MAGIC
# MAGIC **Required by:** Tier 2+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_matching_nb.py` — Matching pipeline
# MAGIC
# MAGIC **When to use:** Run this notebook (or schedule it as a Databricks Job) after extraction succeeds to match BOM components against the Schneider Electric product catalog.
# MAGIC
# MAGIC **Parameters (widgets):**
# MAGIC
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `file_name` | _(blank = batch)_ | PDF filename to match; leave blank to process all SUCCESS files |
# MAGIC | `top_n` | `3` | Reference candidates to keep per component |
# MAGIC | `preferred_tier` | _(blank)_ | Filter by product tier after VS: `economy` / `standard` / `premium` |
# MAGIC | `agent_threshold` | `3` | Property score below which LLM fallback triggers (0 = always, 99 = never) |
# MAGIC | `vs_index_name` | _(from config)_ | Fully-qualified VS index name |
# MAGIC
# MAGIC Results are written to `reference_matches` Delta table (one row per component per diagram).
# MAGIC
# MAGIC **Required by:** Tier 2+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `generate_material_data.py` — Catalog data
# MAGIC
# MAGIC **When to use:** Run once after `setup.py` to seed the Schneider Electric product catalog.
# MAGIC
# MAGIC **What it creates:**
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |-------|----------|
# MAGIC | `material` | ~550 product references (active + discontinued) with technical properties |
# MAGIC | `stock` | Per-reference availability across 5 distribution centers |
# MAGIC | `work_orders` | Incoming stock orders with expected delivery dates |
# MAGIC
# MAGIC Re-running is safe — tables are replaced with fresh data (`overwrite` mode).
# MAGIC
# MAGIC > **Customer catalog:** If the customer has their own catalog data, they can skip this notebook and load their own references into the same three-table schema. `sld_bom_catalog.py` and the VS matcher are schema-driven and will work with any product data.
# MAGIC
# MAGIC **Required by:** Tier 2+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_agent_model.py` — Agent class
# MAGIC
# MAGIC **What it is:** The MLflow code-based model file for the conversational agent. MLflow loads this file at serving time via `mlflow.pyfunc.log_model(python_model="sld_bom_agent_model.py")`.
# MAGIC
# MAGIC > ⚠️ **Hardcoded values — update before deploying to a new environment:**
# MAGIC > `CATALOG`, `SCHEMA`, `SQL_WAREHOUSE_ID`, `EXTRACTION_JOB_ID`, `MATCHING_JOB_ID` at the top of the file must be updated to match your workspace. These are not read from `config.py` because MLflow loads this file in isolation at serving time.
# MAGIC
# MAGIC **What it defines:** `SLDBomAgent(ResponsesAgent)` — an MLflow 3 `ResponsesAgent` subclass with 9 tools:
# MAGIC
# MAGIC | Tool | Purpose |
# MAGIC |------|---------|
# MAGIC | `list_unprocessed_files` | Lists PDFs in the volume not yet in the Delta table |
# MAGIC | `trigger_extraction` | Triggers the extraction pipeline for one PDF via Databricks Jobs API |
# MAGIC | `get_job_status` | Checks a running extraction or matching job by run_id |
# MAGIC | `query_results` | Runs a SQL query against `bom_extractions` (e.g. show unmatched components) |
# MAGIC | `get_overlay_path` | Returns the UC volume path to a diagram's JPEG overlay |
# MAGIC | `trigger_reference_matching` | Submits the reference matching job for a successfully extracted diagram |
# MAGIC | `check_stock` | Looks up stock availability and pricing for a specific product reference |
# MAGIC | `find_alternatives` | Finds alternative product references for a component type and specs |
# MAGIC | `semantic_search_catalog` | Searches the product catalog using natural language via Vector Search |
# MAGIC
# MAGIC **MLflow tracing:** Every agent invocation emits spans: `sld_bom_agent` (root) → `llm_call_N` → `tool_<name>`.
# MAGIC
# MAGIC **Required by:** Tier 3+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sld_bom_agent_uc.py` — Agent registration
# MAGIC
# MAGIC **When to use:** Run this notebook when you want to register or update the agent — after changing `sld_bom_agent_model.py`, updating tool logic, or upgrading MLflow.
# MAGIC
# MAGIC **Cells summary:**
# MAGIC
# MAGIC | Cell | What it does |
# MAGIC |------|-------------|
# MAGIC | 1 — Install deps | `%pip install PyMuPDF Pillow openai mlflow>=3.0 databricks-sdk` |
# MAGIC | 2 — Config | Reads catalog/schema/volume/model paths from `config.py` |
# MAGIC | 3 — Load module | Imports `sld_bom_extractor` and the system prompt |
# MAGIC | 4 — Define agent | `%run ./sld_bom_agent_model` to load `SLDBomAgent` class |
# MAGIC | 5 — Local test | Instantiates the agent and calls `agent.predict(...)` locally to verify before logging |
# MAGIC | 6 — Log to MLflow | `mlflow.pyfunc.log_model(python_model="sld_bom_agent_model.py", ...)` — logs the code-based model |
# MAGIC | 7 — Register to UC | Registers the logged model to `{CATALOG}.bom_parser.sld_bom_agent` |
# MAGIC | 8 — Deploy endpoint | Creates or updates the `sld-bom-agent` Model Serving endpoint |
# MAGIC
# MAGIC > **Note:** You only need to run cells 6–8 when pushing a new version. Cells 1–5 can be iterated during development without re-registering.
# MAGIC
# MAGIC **Required by:** Tier 3+

# COMMAND ----------

# MAGIC %md
# MAGIC ### `app/` — Web application
# MAGIC
# MAGIC A **Databricks App** (FastAPI backend + React frontend) that provides a visual interface to the pipeline.
# MAGIC
# MAGIC **Backend routes:**
# MAGIC
# MAGIC | Route | What it does |
# MAGIC |-------|-------------|
# MAGIC | `POST /api/upload` | Uploads a PDF to the UC volume |
# MAGIC | `POST /api/extract` | Triggers extraction via the Databricks Jobs API |
# MAGIC | `GET /api/diagrams` | Returns all processed diagrams from the Delta table |
# MAGIC | `GET /api/unprocessed` | Lists PDFs in the volume with no Delta table entry |
# MAGIC | `GET /api/annotated/{file}` | Streams the annotated PDF for download |
# MAGIC | `POST /api/chat` | Proxies a message to the agent serving endpoint |
# MAGIC | `POST /api/match` | Triggers the reference matching job for a diagram |
# MAGIC | `GET /api/match/run-status/{run_id}` | Polls matching job run state |
# MAGIC | `GET /api/matches/{file}` | Returns reference_matches rows for a diagram |
# MAGIC | `PATCH /api/matches` | Saves user-selected reference overrides |
# MAGIC | `POST /api/export/{file}` | Generates Excel (2 sheets), uploads to volume, streams back |
# MAGIC | `GET /api/exports/{file}` | Lists past exports for a diagram |
# MAGIC | `GET /api/exports/download/{id}` | Re-serves a past export from the UC volume |
# MAGIC
# MAGIC **Frontend features:**
# MAGIC - Sidebar list of all diagrams with status badges (in progress / success / error)
# MAGIC - BOM table with filters (type, circuit, rating) and matched/unmatched split
# MAGIC - Overlay tab with the spatial JPEG overlay
# MAGIC - **References tab** — match components to Schneider Electric catalog with stock badges, user overrides via dropdown, and Excel export
# MAGIC - Download annotated PDF button (with graceful 404 fallback)
# MAGIC - Chat panel for natural-language interaction with the agent
# MAGIC - Auto-polls every 2 s while extractions are in progress, 30 s otherwise
# MAGIC
# MAGIC **Required by:** Tier 4

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Deployment tiers
# MAGIC
# MAGIC The project is designed for incremental deployment. Each tier is independently useful —
# MAGIC deploy only what you need.
# MAGIC
# MAGIC | Tier | What you get | Files required |
# MAGIC |------|-------------|----------------|
# MAGIC | **1 — Extraction** | Extract BOM from PDFs; results in Delta table + annotated PDF | `config.py`, `setup.py`, `sld2bom_system_prompt.txt`, `sld_bom_extractor.py`, `sld_to_bom_pipeline.py` |
# MAGIC | **2 — + Reference matching** | Match BOM components to product catalog via Vector Search | Tier 1 + `sld_bom_catalog.py`, `sld_bom_vs_matcher.py`, `sld_bom_matching_nb.py`, `generate_material_data.py` |
# MAGIC | **3 — + Agent** | Natural-language interface to trigger and query the pipeline | Tier 2 + `sld_bom_agent_model.py`, `sld_bom_agent_uc.py` |
# MAGIC | **4 — Full (web app)** | Visual UI with upload, BOM review, references tab, Excel export, chat | Tier 3 + `app/` |
# MAGIC
# MAGIC ### Tier 1 — Extraction only
# MAGIC
# MAGIC Minimal infrastructure: one UC schema, one volume, one Delta table, one Databricks Job.
# MAGIC No Vector Search, no agent, no app required.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Import `bom_parser/` to your workspace
# MAGIC 2. Open `config.py` → set `CATALOG` (must already exist; you need `CREATE SCHEMA` on it)
# MAGIC 3. Run `setup.py` — creates schema, volume, `bom_extractions` table, and copies the system prompt
# MAGIC 4. Create a **Databricks Job** targeting `sld_to_bom_pipeline` — note the Job ID
# MAGIC 5. Upload PDFs to the volume and run the job (single file or batch mode)
# MAGIC
# MAGIC **Delta table written:** `{CATALOG}.bom_parser.bom_extractions`
# MAGIC **Outputs per page:** `overlays/<name>_overlay.jpg`, `overlays/<name>_annotated.pdf`
# MAGIC
# MAGIC ### Tier 2 — + Reference matching
# MAGIC
# MAGIC Adds catalog matching against Schneider Electric product references via Databricks Vector Search.
# MAGIC Requires an active Vector Search endpoint.
# MAGIC
# MAGIC **Additional steps (after Tier 1):**
# MAGIC 1. Run `setup.py` if not already done (creates `reference_matches` and `exports` tables)
# MAGIC 2. Run `generate_material_data.py` — creates `material`, `stock`, and `work_orders` tables
# MAGIC    - Skip this step if you're loading your own catalog data; populate those three tables with the same schema
# MAGIC 3. Create a **Vector Search endpoint** (e.g. `sld-bom-vs`) from the Databricks UI (Catalog → Vector Search)
# MAGIC 4. Create a **Vector Search index** on `{CATALOG}.bom_parser.material` named `material_vs_index`
# MAGIC    using `reference` as the primary key and the `properties` + `product_description` columns as the embedding source
# MAGIC 5. Create a **Databricks Job** targeting `sld_bom_matching_nb` — note the Job ID
# MAGIC 6. Run the matching job for each successfully extracted diagram (pass `file_name` as a job parameter, or leave blank for batch)
# MAGIC
# MAGIC **Delta table written:** `{CATALOG}.bom_parser.reference_matches`
# MAGIC
# MAGIC ### Tier 3 — + Agent
# MAGIC
# MAGIC Adds a conversational AI agent (MLflow `ResponsesAgent`) deployed as a Model Serving endpoint.
# MAGIC
# MAGIC **Additional steps (after Tier 2):**
# MAGIC 1. Open `sld_bom_agent_model.py` and update the hardcoded values at the top of the file:
# MAGIC    ```python
# MAGIC    CATALOG           = "<your_catalog>"
# MAGIC    SCHEMA            = "bom_parser"
# MAGIC    SQL_WAREHOUSE_ID  = "<your_warehouse_id>"
# MAGIC    EXTRACTION_JOB_ID = <extraction_job_id>
# MAGIC    MATCHING_JOB_ID   = <matching_job_id>
# MAGIC    ```
# MAGIC 2. Open `sld_bom_agent_uc.py`, run cells 1 → 5 to test locally
# MAGIC 3. Run cells 6 → 8 to log, register, and deploy the `sld-bom-agent` serving endpoint
# MAGIC
# MAGIC ### Tier 4 — Full (web app)
# MAGIC
# MAGIC Adds the Databricks App with the full UI. Requires all Tier 3 components.
# MAGIC
# MAGIC **Additional steps (after Tier 3):**
# MAGIC 1. Open `app/app.yaml` and set the environment variables for your workspace:
# MAGIC    - `DATABRICKS_CATALOG` → your catalog name
# MAGIC    - `EXTRACTION_JOB_ID` → extraction job ID (from Tier 1)
# MAGIC    - `MATCHING_JOB_ID` → matching job ID (from Tier 2)
# MAGIC    - `DATABRICKS_WAREHOUSE_ID` → SQL warehouse ID
# MAGIC 2. Create and deploy the app:
# MAGIC    ```bash
# MAGIC    databricks apps create sld-bom-parser
# MAGIC    databricks apps deploy sld-bom-parser \
# MAGIC      --source-code-path /Workspace/Users/<you>/bom_parser/app
# MAGIC    ```
# MAGIC 3. When prompted, bind the `sld-bom-agent` serving endpoint and a SQL warehouse to the app resources

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Unity Catalog layout
# MAGIC
# MAGIC ```
# MAGIC Catalog:  serverless_stable_bach_catalog
# MAGIC Schema:   bom_parser
# MAGIC │
# MAGIC ├── Table: bom_extractions
# MAGIC │     file_name, file_path, status, processed_at,
# MAGIC │     threshold_met, attempts_made, error_message,
# MAGIC │     bom_json (JSON array), progress_msg
# MAGIC │
# MAGIC ├── Table: material           ← Schneider Electric product catalog (~500 refs)
# MAGIC │     reference, product_description, component_type, range, tier,
# MAGIC │     status, superseded_by, list_price_eur, properties (JSON)
# MAGIC │
# MAGIC ├── Table: stock              ← stock per reference per distribution center
# MAGIC │     reference, distribution_center, qty_available
# MAGIC │
# MAGIC ├── Table: work_orders        ← incoming orders (expected_date, qty_incoming)
# MAGIC │     reference, distribution_center, qty_incoming, expected_date
# MAGIC │
# MAGIC ├── Table: reference_matches  ← matching results, one row per component per diagram
# MAGIC │     file_name, component_idx, component_summary,
# MAGIC │     suggested_references (JSON), selected_reference,
# MAGIC │     user_overridden, status, created_at, updated_at
# MAGIC │
# MAGIC ├── Table: exports            ← audit trail of Excel exports
# MAGIC │     export_id, file_name, exported_by, exported_at,
# MAGIC │     volume_path, component_count, referenced_count, total_value_eur
# MAGIC │
# MAGIC ├── Model: sld_bom_agent   (MLflow registered model)
# MAGIC │
# MAGIC └── Volume: electrical_diagrams
# MAGIC       ├── *.pdf                        ← input diagrams
# MAGIC       ├── overlays/
# MAGIC       │     ├── <name>_overlay.jpg     ← JPEG spatial overlay
# MAGIC       │     └── <name>_annotated.pdf   ← colour-coded annotated PDF
# MAGIC       ├── exports/
# MAGIC       │     └── <name>_<timestamp>.xlsx ← generated Excel exports
# MAGIC       └── sld2bom_system_prompt.txt    ← extraction prompt (edit without redeploying)
# MAGIC
# MAGIC Serving endpoint:  sld-bom-agent
# MAGIC Databricks App:    sld-bom-parser
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## User guide
# MAGIC
# MAGIC > **Choose your deployment scope first** — see the [Deployment tiers](#deployment-tiers) section above.
# MAGIC > The steps below cover the full (Tier 4) workflow; skip sections that don't apply to your tier.
# MAGIC
# MAGIC ### Upload a new diagram and run extraction
# MAGIC
# MAGIC _Requires: Tier 4 (web app deployed)_
# MAGIC
# MAGIC 1. Open the **SLD BOM Parser** app (Databricks Apps → `sld-bom-parser`)
# MAGIC 2. Click **Upload** and select your PDF — the file is uploaded to the UC volume
# MAGIC 3. The diagram appears in the sidebar with status **unprocessed**
# MAGIC 4. Click the diagram → click **Re-extract** (or the extract button for new files)
# MAGIC 5. Status changes to **extracting** with a live progress indicator
# MAGIC 6. Once done, the BOM table loads automatically with matched and unmatched components
# MAGIC 7. Use the **type / circuit / rating** dropdowns to filter the BOM
# MAGIC 8. Click **PDF** to download the annotated PDF with colour-coded bounding boxes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run extraction from a notebook directly
# MAGIC
# MAGIC _Requires: Tier 1+_
# MAGIC
# MAGIC Use this when you need more control — e.g. change the model, adjust the retry threshold, or bulk-process a batch without the app.
# MAGIC
# MAGIC 1. Open `sld_to_bom_pipeline.py`
# MAGIC 2. Set the widgets at the top of the notebook:
# MAGIC    - `file_name` — PDF filename (e.g. `AVILA.pdf`). Leave blank for **batch mode** (all unprocessed files)
# MAGIC    - `model` — model endpoint (default: `databricks-claude-sonnet-4-6`)
# MAGIC    - `enable_retry` — `true` / `false`
# MAGIC    - `max_retries` — number of additional attempts after the first (default: `2`)
# MAGIC    - `threshold` — minimum match rate to accept (default: `0.75`)
# MAGIC 3. Run **All** (or cell by cell from Cell 1)
# MAGIC 4. Cell 8 shows extraction status; Cell 9 shows the full BOM SQL view
# MAGIC
# MAGIC > **Re-running a failed file:** In batch mode, files with `ERROR` status are skipped. To retry a failed file, either:
# MAGIC > - Set `file_name` widget to the specific filename and run in single-file mode, **or**
# MAGIC > - Delete the row from the Delta table: `DELETE FROM bom_parser.bom_extractions WHERE file_name = 'YOURFILE.pdf'`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Talk to the agent
# MAGIC
# MAGIC _Requires: Tier 3+ (agent serving endpoint deployed)_
# MAGIC
# MAGIC The chat panel in the web app connects to the `sld-bom-agent` serving endpoint.
# MAGIC You can also call it directly via the serving endpoint REST API.
# MAGIC
# MAGIC **Example prompts:**
# MAGIC
# MAGIC ```
# MAGIC "Process all new diagrams with up to 3 retries and threshold 80%"
# MAGIC "Run AVILA.pdf using Opus, no retry"
# MAGIC "What is the match rate for CARRIAZO?"
# MAGIC "Show me unmatched components for ESQUEMAS"
# MAGIC "How many interruptores are in panel Q1?"
# MAGIC "Match references for AVILA.pdf"
# MAGIC "Check stock for A9F74216"
# MAGIC "Find alternatives for 40A 2P circuit breaker"
# MAGIC ```
# MAGIC
# MAGIC The agent confirms its interpretation of parameters before running anything.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get product reference matches
# MAGIC
# MAGIC _Requires: Tier 2+ (Vector Search index created, matching job deployed)_
# MAGIC
# MAGIC **From the app (Tier 4):**
# MAGIC 1. Open a diagram → click the **References** tab
# MAGIC 2. Click **Get References** — triggers the `sld-bom-matching` Job asynchronously
# MAGIC 3. A spinner shows while the job runs (~1 minute). Results appear automatically when done.
# MAGIC 4. The table shows each component with its top-ranked reference, stock status badge, distribution center, and list price.
# MAGIC 5. **Override:** use the dropdown to pick an alternative from the top-3 candidates. Changed rows are highlighted.
# MAGIC 6. Click **Save** to persist overrides to the `reference_matches` Delta table.
# MAGIC 7. Click **Excel** to generate and download a two-sheet Excel file:
# MAGIC    - Sheet 1: BOM + selected references with stock colour coding
# MAGIC    - Sheet 2: All top-N candidates per component (alternatives)
# MAGIC 8. Click **Re-run** to re-match after updating catalog data or changing the tier preference.
# MAGIC
# MAGIC **From a notebook (Tier 2):**
# MAGIC Open `sld_bom_matching_nb.py`, set the `file_name` widget (or leave blank for batch), run all cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download an annotated PDF
# MAGIC
# MAGIC _Requires: Tier 1+ (extraction must have completed)_
# MAGIC
# MAGIC The annotated PDF overlays colour-coded bounding boxes on the original diagram, with component metadata embedded as PDF annotations (hover over a box in a PDF viewer to see type, circuit, calibre, etc.).
# MAGIC
# MAGIC - **From the app (Tier 4):** Open a diagram → click **PDF** button in the header
# MAGIC - **From the volume (any tier):** `dbutils.fs.cp("dbfs:/Volumes/.../overlays/AVILA_annotated.pdf", "/tmp/AVILA_annotated.pdf")`
# MAGIC
# MAGIC > **404 / button shows error?** The annotated PDF is generated during extraction. If the diagram was processed before this feature was added, click **Re-extract** to regenerate it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-register the agent after code changes
# MAGIC
# MAGIC _Requires: Tier 3+_
# MAGIC
# MAGIC After editing `sld_bom_agent_model.py`:
# MAGIC
# MAGIC 1. Open `sld_bom_agent_uc.py`
# MAGIC 2. Run cells **1 → 5** to install deps, load config, and test locally
# MAGIC 3. Run cell **6** to log a new MLflow model version
# MAGIC 4. Run cell **7** to register to Unity Catalog
# MAGIC 5. Run cell **8** to update the serving endpoint (rolling update — no downtime)
# MAGIC
# MAGIC > If you only changed the **system prompt** (`sld2bom_system_prompt.txt` in the volume), no re-registration needed — the prompt is read from the volume at inference time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy / redeploy the web app
# MAGIC
# MAGIC _Requires: Tier 4_
# MAGIC
# MAGIC The app source lives in the workspace at `Workspace/Users/<you>/bom_parser/app/`.
# MAGIC
# MAGIC To deploy changes:
# MAGIC 1. Upload changed files to the workspace (use `databricks workspace import-dir`)
# MAGIC 2. Redeploy:
# MAGIC    ```bash
# MAGIC    databricks apps deploy sld-bom-parser \
# MAGIC      --source-code-path /Workspace/Users/<you>/bom_parser/app
# MAGIC    ```
# MAGIC 3. The app rebuilds the React frontend and restarts uvicorn automatically (`start.sh`)
# MAGIC
# MAGIC > **Verifying the new bundle loaded:** Open the app → View Source → check that `bundle.<hash>.js` matches the hash in `static/assets/`. If the hash is the same as before, files were not uploaded to the correct path.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Configuration reference
# MAGIC
# MAGIC All notebooks and the app share the same configuration values:
# MAGIC
# MAGIC | Variable | Value | Where set |
# MAGIC |----------|-------|-----------|
# MAGIC | `CATALOG` | `serverless_stable_bach_catalog` | **`config.py`** (single source of truth) |
# MAGIC | `SCHEMA` | `bom_parser` | `config.py` |
# MAGIC | `VOLUME` | `electrical_diagrams` | `config.py` |
# MAGIC | `TABLE_NAME` | `bom_parser.bom_extractions` | `config.py` (derived) |
# MAGIC | `VOLUME_PATH` / `OVERLAY_PATH` | `/Volumes/...` | `config.py` (derived) |
# MAGIC | `AGENT_MODEL_NAME` | `...bom_parser.sld_bom_agent` | `config.py` (derived) |
# MAGIC | `DEFAULT_MODEL_ENDPOINT` | `databricks-claude-sonnet-4-6` | `config.py` |
# MAGIC | `DEFAULT_THRESHOLD` | `0.75` | `config.py` |
# MAGIC | `DEFAULT_MAX_RETRIES` | `2` | `config.py` |
# MAGIC | `MATCHING_JOB_ID` | `330529910000908` | `app/server/config.py` / `app.yaml` |
# MAGIC | `MATCHES_TABLE` | `bom_parser.reference_matches` | `app/server/config.py` (derived) |
# MAGIC | `EXPORTS_TABLE` | `bom_parser.exports` | `app/server/config.py` (derived) |
# MAGIC | App catalog / job IDs | env-specific | `app/app.yaml` (updated per deployment) |
# MAGIC | `DPI` | `400` | `sld_bom_extractor.py` |
# MAGIC | System prompt | `sld2bom_system_prompt.txt` | Repo root → copied to UC volume by `setup.py` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Symptom | Likely cause | Fix |
# MAGIC |---------|-------------|-----|
# MAGIC | Extraction stuck at "IN_PROGRESS" | Databricks Job failed or timed out | Check Jobs → `sld-bom-extraction` run log |
# MAGIC | Low match rate / `threshold_met = false` | Diagram uses non-standard fonts or layout | Increase `max_retries`; inspect unmatched components in the BOM table |
# MAGIC | PDF button shows "Annotated PDF not yet generated" | Diagram processed before annotated PDF feature was added | Click **Re-extract** |
# MAGIC | Agent responds "I don't have a tool for that" | Tool not defined or agent version is stale | Re-register agent via `sld_bom_agent_uc.py` cells 6–8 |
# MAGIC | App shows old UI after redeploy | Files uploaded to wrong workspace path | Upload to `sld-bom-parser/` root, not `sld-bom-parser/app/` |
# MAGIC | `sld2bom_system_prompt.txt` not found | File missing from volume | Re-run `setup.py` cell 4, or manually upload from the repo root to the volume |
# MAGIC | Pipeline notebook `ImportError` on `sld_bom_extractor` | Module not in the same workspace folder as the notebook | Ensure `sld_bom_extractor.py` is in the same workspace directory as `sld_to_bom_pipeline.py` |
# MAGIC | Setup fails on schema/volume creation | Missing UC privileges | Ensure you have `CREATE SCHEMA` on the catalog and `CREATE VOLUME` on the schema |
# MAGIC | References tab shows "No reference matches yet" after clicking Get References | Matching job failed | Check Jobs → `sld-bom-matching` run log; ensure extraction status is SUCCESS |
# MAGIC | References all show OUT_OF_STOCK | Stock table not populated | Run `generate_material_data.py` to seed the stock table |
# MAGIC | Excel export fails with "openpyxl not installed" | Missing dependency | Add `openpyxl` to `app/requirements.txt` and redeploy the app |
