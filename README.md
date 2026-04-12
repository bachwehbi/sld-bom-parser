# SLD-to-BOM Parser — Project README

> **Automatically extracts a Bill of Materials (BOM) from Schneider Electric electrical diagram PDFs (Single Line Diagrams), assigns precise component positions, and surfaces results through a conversational web app.**

---

## Table of Contents

1. [Architecture overview](#architecture-overview)
2. [Repository structure](#repository-structure)
3. [Notebook guide](#notebook-guide)
   - [setup.py](#setuppy--environment-setup)
   - [sld_bom_extractor.py](#sld_bom_extractorpy--core-module)
   - [sld2bom_system_prompt.txt](#sld2bom_system_prompttxt--extraction-prompt)
   - [sld_to_bom_pipeline.py](#sld_to_bom_pipelinepy--production-pipeline)
   - [sld_bom_catalog.py](#sld_bom_catalogpy--matching-module)
   - [sld_bom_vs_matcher.py](#sld_bom_vs_matcherpy--vector-search-matching)
   - [sld_bom_matching_nb.py](#sld_bom_matching_nbpy--matching-pipeline)
   - [generate_material_data.py](#generate_material_datapy--catalog-data)
   - [sld_bom_agent_model.py](#sld_bom_agent_modelpy--agent-class)
   - [sld_bom_agent_uc.py](#sld_bom_agent_ucpy--agent-registration)
   - [app/](#app--web-application)
4. [Deployment tiers](#deployment-tiers)
5. [Unity Catalog layout](#unity-catalog-layout)
6. [User guide](#user-guide)
   - [Tier 1 — Extraction only](#tier-1--extraction-only)
   - [Tier 2 — + Reference matching](#tier-2----reference-matching)
   - [Tier 3 — + Agent](#tier-3----agent)
   - [Tier 4 — Full with web app](#tier-4--full-with-web-app)
   - [Upload a new diagram and run extraction](#upload-a-new-diagram-and-run-extraction)
   - [Run extraction from a notebook directly](#run-extraction-from-a-notebook-directly)
   - [Talk to the agent](#talk-to-the-agent)
   - [Download an annotated PDF](#download-an-annotated-pdf)
   - [Re-register the agent after code changes](#re-register-the-agent-after-code-changes)
   - [Deploy / redeploy the web app](#deploy--redeploy-the-web-app)
7. [Configuration reference](#configuration-reference)
8. [Troubleshooting](#troubleshooting)
---
## Architecture overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  Unity Catalog Volume  (/electrical_diagrams/)                      │
│   • *.pdf          — input diagrams                                 │
│   • overlays/      — JPEG overlays + annotated PDFs                 │
│   • sld2bom_system_prompt.txt                                       │
└───────────────────┬─────────────────────────────────────────────────┘
                    │
             ┌──────▼──────┐
             │  Pipeline   │  sld_to_bom_pipeline.py
             │  Notebook   │  (run manually or via Databricks Job)
             └──────┬──────┘
                    │  calls
             ┌──────▼──────┐
             │  Extractor  │  sld_bom_extractor.py
             │  Module     │  Vision → Vector matching → Overlay
             └──────┬──────┘
                    │  writes
         ┌───────────▼───────────┐
         │  Delta Table           │  bom_parser.bom_extractions
         │  (BOM + coordinates)   │
         └───────────┬───────────┘
                     │
      ┌──────────────┴──────────────┐
      │                             │
┌─────▼──────┐              ┌───────▼──────┐
│  Web App   │              │  Agent        │
│ (FastAPI + │              │  (MLflow       │
│  React)    │              │  ResponsesAgent│
│            │              │  on Serving)   │
│  Upload    │              │                │
│  Review    │              │  Natural-lang  │
│  Download  │              │  interface to  │
└────────────┘              │  the pipeline  │
                            └────────────────┘
```

### Extraction pipeline (3 stages)

| Stage | What happens |
|-------|-------------|
| **1 — Vision** | PDF rasterized to 400 DPI image → sent to Claude vision → returns JSON BOM (type, calibre, poles, circuit…) |
| **2 — Vector match** | PyMuPDF extracts every text span from the PDF vector layer → clusters nearby annotations → each BOM entry is matched to the closest cluster → assigned sub-pixel bounding box |
| **3 — Circuit sharing** | Components without a vector match (e.g. Relojes) inherit the position of a matched sibling in the same circuit |
---
## Repository structure

```
bom_parser/
├── README.py                    ← this file
├── config.py                    ← shared config (CATALOG, paths, defaults) — only file to edit per environment
├── setup.py                     ← run once on a new workspace to provision infra
├── sld2bom_system_prompt.txt    ← extraction prompt (source of truth, copied to volume by setup.py)
│
├── sld_bom_extractor.py         ← core extraction module (pure Python, no Databricks deps)
├── sld_to_bom_pipeline.py       ← production extraction pipeline notebook
├── sld_bom_catalog.py           ← reference matching module (pure Python, no Databricks deps)
├── sld_bom_vs_matcher.py        ← Vector Search retrieval + property re-ranking + LLM fallback (pure Python)
├── sld_bom_matching_nb.py       ← production reference matching pipeline notebook
├── generate_material_data.py    ← one-time: creates material, stock, work_orders tables
├── sld_bom_agent_model.py       ← MLflow code-based agent model file (⚠ update hardcoded catalog/job IDs)
├── sld_bom_agent_uc.py          ← agent registration & serving endpoint notebook
│
└── app/                         ← Databricks App source (deployed from bom_parser/app/)
    ├── app.py                   ← FastAPI entry point
    ├── app.yaml                 ← Databricks App manifest (edit catalog + job ID for new env)
    ├── start.sh                 ← startup script (builds frontend, starts uvicorn)
    ├── server/
    │   ├── config.py            ← shared config (catalog, volume, table, endpoint)
    │   └── routes/
    │       ├── upload.py        ← POST /api/upload, POST /api/extract
    │       ├── diagrams.py      ← GET /api/diagrams, GET /api/annotated/{file}
    │       ├── chat.py          ← POST /api/chat (proxies agent serving endpoint)
    │       └── matching.py      ← POST /api/match, GET /api/matches/{file}, PATCH /api/matches,
    │                               POST /api/export/{file}, GET /api/exports/{file}
    └── frontend/                ← React + Tailwind UI (built to static/)
```
---
## Notebook guide

### `config.py` — Shared configuration

**What it is:** Not run directly — included by every other notebook via `%run ./config`.

**What it defines:**
- `CATALOG` / `SCHEMA` — the only values to change when deploying to a new environment
- All derived paths: `VOLUME_PATH`, `OVERLAY_PATH`, `PROMPT_FILE`, `TABLE_NAME`, `AGENT_MODEL_NAME`
- Default extraction parameters: `DEFAULT_MODEL_ENDPOINT`, `DEFAULT_ENABLE_RETRY`, `DEFAULT_MAX_RETRIES`, `DEFAULT_THRESHOLD`

> To deploy to a new environment: open `config.py`, change `CATALOG` — all notebooks pick it up automatically.
### `setup.py` — Environment setup

**When to use:** Run this notebook **once** on a new workspace before deploying the app or running the pipeline.

**What it does:**

| Step | Action |
|------|--------|
| 1 | Creates the UC schema |
| 2 | Creates the `electrical_diagrams` volume |
| 3 | Creates the `overlays/` and `exports/` subdirectories in the volume |
| 4 | Copies `sld2bom_system_prompt.txt` from the workspace folder to the volume |
| 5 | Creates the `bom_extractions` Delta table |
| 6 | Creates the `reference_matches` Delta table |
| 7 | Creates the `exports` Delta table |
| 8 | Verifies all components with a pass/fail summary |

**Before running:** set `CATALOG = "your_catalog"` at the top. The catalog must already exist and you need `CREATE SCHEMA` privilege on it.

> `sld2bom_system_prompt.txt` must be in the **same workspace folder** as `setup.py` — it is the source of truth for the prompt and is uploaded to the volume by this notebook.
### `sld_bom_extractor.py` — Core module

**What it is:** A plain Python module (not a notebook). Contains all the extraction algorithm — no Databricks dependencies so it can also be run locally or in unit tests.

**What it does:**
- `rasterize_pdf(path, dpi)` — converts a PDF page to a base64 PNG image for the vision model. Large A0/A1 pages are split into overlapping tiles.
- `run_extraction(image_b64, client, model, system_prompt)` → JSON list of components
- `match_components(bom, pdf_path, dpi)` → adds `precise_cx/cy/x0/y0/x1/y1` to each entry
- `generate_precision_overlay(pdf_path, matched, unmatched, output_path)` → saves JPEG overlay
- `generate_annotated_pdf(pdf_path, matched, unmatched, output_path)` → saves colour-coded annotated PDF with component metadata embedded as annotations

**Do not run directly** — it is imported by the pipeline notebook and the agent.
### `sld2bom_system_prompt.txt` — Extraction prompt

**What it is:** The Claude system prompt that drives Stage 1 (vision extraction). Stored in the repo root and copied to the UC volume by `setup.py`. The pipeline reads it from the volume at runtime — **you can update the prompt without touching any code or redeploying anything**.

#### Design decisions

**Canonical English schema (multilingual input)**

Earlier versions of the prompt returned Spanish field names (`"Que és"`, `"Calibre (A)"`, `"Polos"`, `"Circuito"`) reflecting the language of the first test diagrams (Schneider España SLDs). This caused two problems:
- The matching code had to branch on language when reading field values.
- Any non-Spanish diagram produced inconsistent field names.

The prompt was rewritten to use a **canonical English schema** regardless of the diagram language:

| Field | Type | Description |
|-------|------|-------------|
| `component_type` | enum | One of 9 fixed values (`circuit_breaker`, `rcd`, `contactor`, …) |
| `amperage_a` | number | Rated current in A |
| `poles` | number | Pole count (Roman numerals or diagonal lines counted correctly) |
| `curve` | string | Trip curve: B / C / D |
| `breaking_ka` | number | Breaking capacity in kA |
| `sensitivity_ma` | number | Differential sensitivity in mA (rcd only) |
| `rcd_type` | string | AC / A / A-SI |
| `rcd_block_type` | string | standalone / vigi_block |
| `panel` | string | Panel / board name |
| `circuit` | string | Reference designator + load name (see below) |

The `component_type` enum doubles as a normalisation layer — the table maps all Spanish, French, German, and English aliases to a single value, so downstream code never needs to handle aliases.

**Reference designators in the `circuit` field**

Schneider SLDs print a short reference designator directly beside each component symbol (e.g. `D35`, `I17`, `DX1`). These designators are the primary key that the Stage 2 vector-text matching algorithm uses to link a Claude-extracted BOM entry to its exact position in the PDF.

Early prompt versions extracted only the human-readable load name (`"ALUMBRADO 22"`). This worked when a page had components with distinct specs (calibre / poles distinguish them). It failed on pages where many components share identical specs — e.g. nine 2P 40A 30mA RCDs on a single page. Without the designator, the matching algorithm has no anchor and falls back to the circuit-sharing heuristic, stacking all RCDs on top of the first matched MCB.

The prompt now instructs Claude to **always prefix the reference designator** when it is legible:

```
"circuit": "D35 - ALUMBRADO 22"    ← MCB with designator D35
"circuit": "I17 - ALUMBRADO 22"    ← RCD above it with designator I17
"circuit": "DX1 - OFFICE BARRA 1"  ← DX-prefixed RCD variant
```

The Stage 2 `ID_PATTERNS` regex (`^[DIQC]\d+`, `^DX`, `^TX`) extracts the designator from the PDF text cluster. The matching scorer then checks `designator in circuit_string` for a +10 score bonus, which uniquely pins each component to its column even when all other specs are identical.

> **Updating the prompt:** Edit `sld2bom_system_prompt.txt` locally, then upload it to the volume (`setup.py` cell 4, or the Databricks volume browser). The pipeline reads it fresh on every run — no notebook restart needed.
### `sld_to_bom_pipeline.py` — Production pipeline

**When to use:** Run this notebook when you want to process one or more PDFs directly — for example, to re-extract a specific diagram, bulk-process new uploads, or test a change to the extractor.

**Cells summary:**

| Cell | What it does |
|------|-------------|
| 1 — Install deps | `%pip install PyMuPDF Pillow openai` |
| 2 — Config + widgets | Sets catalog/schema/volume paths; exposes widgets for `file_name`, `model`, `enable_retry`, `max_retries`, `threshold` |
| 3 — Load module | Imports `sld_bom_extractor` from the same workspace folder |
| 4 — OpenAI client | Creates the Databricks Foundation Model API client; loads system prompt from volume |
| 5 — Create table | Creates `bom_extractions` Delta table if it doesn't exist |
| 6 — Discover files | Lists PDFs in the volume that are not yet in the Delta table (or the single file from the widget) |
| 7 — Process loop | For each file: rasterize → extract → match → retry if below threshold → save JPEG overlay + annotated PDF → MERGE into Delta table |
| 8 — Status table | Displays a summary table of all processed files |
| 9 — BOM SQL view | Shows the full BOM as a SQL `SELECT` with exploded components |
| 10 — Overlay preview | Displays JPEG overlays inline via matplotlib |

**Key behaviours:**
- **Idempotent in batch mode** — skips files already present in the Delta table (any status). Files with `ERROR` status are also skipped in batch mode; use single-file mode to retry them.
- **Retry logic** — if the match rate is below the threshold, the full vision+matching cycle is repeated up to `max_retries` additional times. The best result across all attempts is kept.
- **Annotated PDF** — each run produces both a JPEG overlay (`*_overlay.jpg`) and an annotated PDF (`*_annotated.pdf`) in the `overlays/` folder.
### `sld_bom_catalog.py` — Matching module

**What it is:** A pure Python module (no Databricks dependencies) that normalises extracted BOM fields and scores catalog candidates.

**Public API:**
- `normalize_component_fields(comp)` — maps any language variant to the canonical English schema
- `build_vs_query(comp)` — builds the natural-language search string sent to Vector Search
- `score_candidate(comp, ref)` — scores a catalog reference against a component on calibre, poles, curve, sensitivity, etc.
- `resolve_stock(ref, stock_rows, work_orders)` — enriches a reference candidate with availability data

**Do not run directly** — imported by `sld_bom_vs_matcher.py` and `sld_bom_matching_nb.py`.

**Required by:** Tier 2+
### `sld_bom_vs_matcher.py` — Vector Search matching

**What it is:** A pure Python module that implements the three-phase reference matching pipeline.

| Phase | What it does |
|-------|-------------|
| 1 | Field normalisation via `sld_bom_catalog` |
| 2 | Vector Search retrieval (semantic similarity) → property re-ranking |
| 3 | LLM agentic fallback for low-confidence components (score < threshold) |

**Entry point:** `match_all_components(components, vs_index_name, warehouse_id, top_n, agent_threshold)`

**Do not run directly** — imported by `sld_bom_matching_nb.py`.

**Requires:** A Databricks Vector Search endpoint and index (`material_vs_index`) created on the `material` catalog table.

**Required by:** Tier 2+
### `sld_bom_matching_nb.py` — Matching pipeline

**When to use:** Run this notebook (or schedule it as a Databricks Job) after extraction succeeds to match BOM components against the Schneider Electric product catalog.

**Parameters (widgets):**

| Widget | Default | Description |
|--------|---------|-------------|
| `file_name` | _(blank = batch)_ | PDF filename to match; leave blank to process all SUCCESS files |
| `top_n` | `3` | Reference candidates to keep per component |
| `preferred_tier` | _(blank)_ | Filter by product tier after VS: `economy` / `standard` / `premium` |
| `agent_threshold` | `3` | Property score below which LLM fallback triggers (0 = always, 99 = never) |
| `vs_index_name` | _(from config)_ | Fully-qualified VS index name |

Results are written to `reference_matches` Delta table (one row per component per diagram).

**Required by:** Tier 2+
### `generate_material_data.py` — Catalog data

**When to use:** Run once after `setup.py` to seed the Schneider Electric product catalog.

**What it creates:**

| Table | Contents |
|-------|----------|
| `material` | ~550 product references (active + discontinued) with technical properties |
| `stock` | Per-reference availability across 5 distribution centers |
| `work_orders` | Incoming stock orders with expected delivery dates |

Re-running is safe — tables are replaced with fresh data (`overwrite` mode).

> **Customer catalog:** If the customer has their own catalog data, they can skip this notebook and load their own references into the same three-table schema. `sld_bom_catalog.py` and the VS matcher are schema-driven and will work with any product data.

**Required by:** Tier 2+
### `sld_bom_agent_model.py` — Agent class

**What it is:** The MLflow code-based model file for the conversational agent. MLflow loads this file at serving time via `mlflow.pyfunc.log_model(python_model="sld_bom_agent_model.py")`.

> ⚠️ **Hardcoded values — update before deploying to a new environment:**
> `CATALOG`, `SCHEMA`, `SQL_WAREHOUSE_ID`, `EXTRACTION_JOB_ID`, `MATCHING_JOB_ID` at the top of the file must be updated to match your workspace. These are not read from `config.py` because MLflow loads this file in isolation at serving time.

**What it defines:** `SLDBomAgent(ResponsesAgent)` — an MLflow 3 `ResponsesAgent` subclass with 9 tools:

| Tool | Purpose |
|------|---------|
| `list_unprocessed_files` | Lists PDFs in the volume not yet in the Delta table |
| `trigger_extraction` | Triggers the extraction pipeline for one PDF via Databricks Jobs API |
| `get_job_status` | Checks a running extraction or matching job by run_id |
| `query_results` | Runs a SQL query against `bom_extractions` (e.g. show unmatched components) |
| `get_overlay_path` | Returns the UC volume path to a diagram's JPEG overlay |
| `trigger_reference_matching` | Submits the reference matching job for a successfully extracted diagram |
| `check_stock` | Looks up stock availability and pricing for a specific product reference |
| `find_alternatives` | Finds alternative product references for a component type and specs |
| `semantic_search_catalog` | Searches the product catalog using natural language via Vector Search |

**MLflow tracing:** Every agent invocation emits spans: `sld_bom_agent` (root) → `llm_call_N` → `tool_<name>`.

**Required by:** Tier 3+
### `sld_bom_agent_uc.py` — Agent registration

**When to use:** Run this notebook when you want to register or update the agent — after changing `sld_bom_agent_model.py`, updating tool logic, or upgrading MLflow.

**Cells summary:**

| Cell | What it does |
|------|-------------|
| 1 — Install deps | `%pip install PyMuPDF Pillow openai mlflow>=3.0 databricks-sdk` |
| 2 — Config | Reads catalog/schema/volume/model paths from `config.py` |
| 3 — Load module | Imports `sld_bom_extractor` and the system prompt |
| 4 — Define agent | `%run ./sld_bom_agent_model` to load `SLDBomAgent` class |
| 5 — Local test | Instantiates the agent and calls `agent.predict(...)` locally to verify before logging |
| 6 — Log to MLflow | `mlflow.pyfunc.log_model(python_model="sld_bom_agent_model.py", ...)` — logs the code-based model |
| 7 — Register to UC | Registers the logged model to `{CATALOG}.bom_parser.sld_bom_agent` |
| 8 — Deploy endpoint | Creates or updates the `sld-bom-agent` Model Serving endpoint |

> **Note:** You only need to run cells 6–8 when pushing a new version. Cells 1–5 can be iterated during development without re-registering.

**Required by:** Tier 3+
### `app/` — Web application

A **Databricks App** (FastAPI backend + React frontend) that provides a visual interface to the pipeline.

**Backend routes:**

| Route | What it does |
|-------|-------------|
| `POST /api/upload` | Uploads a PDF to the UC volume |
| `POST /api/extract` | Triggers extraction via the Databricks Jobs API |
| `GET /api/diagrams` | Returns all processed diagrams from the Delta table |
| `GET /api/unprocessed` | Lists PDFs in the volume with no Delta table entry |
| `GET /api/annotated/{file}` | Streams the annotated PDF for download |
| `POST /api/chat` | Proxies a message to the agent serving endpoint |
| `POST /api/match` | Triggers the reference matching job for a diagram |
| `GET /api/match/run-status/{run_id}` | Polls matching job run state |
| `GET /api/matches/{file}` | Returns reference_matches rows for a diagram |
| `PATCH /api/matches` | Saves user-selected reference overrides |
| `POST /api/export/{file}` | Generates Excel (2 sheets), uploads to volume, streams back |
| `GET /api/exports/{file}` | Lists past exports for a diagram |
| `GET /api/exports/download/{id}` | Re-serves a past export from the UC volume |

**Frontend features:**
- Sidebar list of all diagrams with status badges (in progress / success / error)
- BOM table with filters (type, circuit, rating) and matched/unmatched split
- Overlay tab with the spatial JPEG overlay
- **References tab** — match components to Schneider Electric catalog with stock badges, user overrides via dropdown, and Excel export
- Download annotated PDF button (with graceful 404 fallback)
- Chat panel for natural-language interaction with the agent
- Auto-polls every 2 s while extractions are in progress, 30 s otherwise

**Required by:** Tier 4
---
## Deployment tiers

The project is designed for incremental deployment. Each tier is independently useful —
deploy only what you need.

| Tier | What you get | Files required |
|------|-------------|----------------|
| **1 — Extraction** | Extract BOM from PDFs; results in Delta table + annotated PDF | `config.py`, `setup.py`, `sld2bom_system_prompt.txt`, `sld_bom_extractor.py`, `sld_to_bom_pipeline.py` |
| **2 — + Reference matching** | Match BOM components to product catalog via Vector Search | Tier 1 + `sld_bom_catalog.py`, `sld_bom_vs_matcher.py`, `sld_bom_matching_nb.py`, `generate_material_data.py` |
| **3 — + Agent** | Natural-language interface to trigger and query the pipeline | Tier 2 + `sld_bom_agent_model.py`, `sld_bom_agent_uc.py` |
| **4 — Full (web app)** | Visual UI with upload, BOM review, references tab, Excel export, chat | Tier 3 + `app/` |

### Tier 1 — Extraction only

Minimal infrastructure: one UC schema, one volume, one Delta table, one Databricks Job.
No Vector Search, no agent, no app required.

**Steps:**
1. Import `bom_parser/` to your workspace
2. Open `config.py` → set `CATALOG` (must already exist; you need `CREATE SCHEMA` on it)
3. Run `setup.py` — creates schema, volume, `bom_extractions` table, and copies the system prompt
4. Create a **Databricks Job** targeting `sld_to_bom_pipeline` — note the Job ID
5. Upload PDFs to the volume and run the job (single file or batch mode)

**Delta table written:** `{CATALOG}.bom_parser.bom_extractions`
**Outputs per page:** `overlays/<name>_overlay.jpg`, `overlays/<name>_annotated.pdf`

### Tier 2 — + Reference matching

Adds catalog matching against Schneider Electric product references via Databricks Vector Search.
Requires an active Vector Search endpoint.

**Additional steps (after Tier 1):**
1. Run `setup.py` if not already done (creates `reference_matches` and `exports` tables)
2. Run `generate_material_data.py` — creates `material`, `stock`, and `work_orders` tables
   - Skip this step if you're loading your own catalog data; populate those three tables with the same schema
3. Create a **Vector Search endpoint** (e.g. `sld-bom-vs`) from the Databricks UI (Catalog → Vector Search)
4. Create a **Vector Search index** on `{CATALOG}.bom_parser.material` named `material_vs_index`
   using `reference` as the primary key and the `properties` + `product_description` columns as the embedding source
5. Create a **Databricks Job** targeting `sld_bom_matching_nb` — note the Job ID
6. Run the matching job for each successfully extracted diagram (pass `file_name` as a job parameter, or leave blank for batch)

**Delta table written:** `{CATALOG}.bom_parser.reference_matches`

### Tier 3 — + Agent

Adds a conversational AI agent (MLflow `ResponsesAgent`) deployed as a Model Serving endpoint.

**Additional steps (after Tier 2):**
1. Open `sld_bom_agent_model.py` and update the hardcoded values at the top of the file:
   ```python
   CATALOG           = "<your_catalog>"
   SCHEMA            = "bom_parser"
   SQL_WAREHOUSE_ID  = "<your_warehouse_id>"
   EXTRACTION_JOB_ID = <extraction_job_id>
   MATCHING_JOB_ID   = <matching_job_id>
   ```
2. Open `sld_bom_agent_uc.py`, run cells 1 → 5 to test locally
3. Run cells 6 → 8 to log, register, and deploy the `sld-bom-agent` serving endpoint

### Tier 4 — Full (web app)

Adds the Databricks App with the full UI. Requires all Tier 3 components.

**Additional steps (after Tier 3):**
1. Open `app/app.yaml` and set the environment variables for your workspace:
   - `DATABRICKS_CATALOG` → your catalog name
   - `EXTRACTION_JOB_ID` → extraction job ID (from Tier 1)
   - `MATCHING_JOB_ID` → matching job ID (from Tier 2)
   - `DATABRICKS_WAREHOUSE_ID` → SQL warehouse ID
2. Create and deploy the app:
   ```bash
   databricks apps create sld-bom-parser
   databricks apps deploy sld-bom-parser \
     --source-code-path /Workspace/Users/<you>/bom_parser/app
   ```
3. When prompted, bind the `sld-bom-agent` serving endpoint and a SQL warehouse to the app resources
---
## Unity Catalog layout

```
Catalog:  <your_catalog>
Schema:   bom_parser
│
├── Table: bom_extractions
│     file_name, file_path, status, processed_at,
│     threshold_met, attempts_made, error_message,
│     bom_json (JSON array), progress_msg
│
├── Table: material           ← Schneider Electric product catalog (~500 refs)
│     reference, product_description, component_type, range, tier,
│     status, superseded_by, list_price_eur, properties (JSON)
│
├── Table: stock              ← stock per reference per distribution center
│     reference, distribution_center, qty_available
│
├── Table: work_orders        ← incoming orders (expected_date, qty_incoming)
│     reference, distribution_center, qty_incoming, expected_date
│
├── Table: reference_matches  ← matching results, one row per component per diagram
│     file_name, component_idx, component_summary,
│     suggested_references (JSON), selected_reference,
│     user_overridden, status, created_at, updated_at
│
├── Table: exports            ← audit trail of Excel exports
│     export_id, file_name, exported_by, exported_at,
│     volume_path, component_count, referenced_count, total_value_eur
│
├── Model: sld_bom_agent   (MLflow registered model)
│
└── Volume: electrical_diagrams
      ├── *.pdf                        ← input diagrams
      ├── overlays/
      │     ├── <name>_overlay.jpg     ← JPEG spatial overlay
      │     └── <name>_annotated.pdf   ← colour-coded annotated PDF
      ├── exports/
      │     └── <name>_<timestamp>.xlsx ← generated Excel exports
      └── sld2bom_system_prompt.txt    ← extraction prompt (edit without redeploying)

Serving endpoint:  sld-bom-agent
Databricks App:    sld-bom-parser
```
---
## User guide

> **Choose your deployment scope first** — see the [Deployment tiers](#deployment-tiers) section above.
> The steps below cover the full (Tier 4) workflow; skip sections that don't apply to your tier.

### Upload a new diagram and run extraction

_Requires: Tier 4 (web app deployed)_

1. Open the **SLD BOM Parser** app (Databricks Apps → `sld-bom-parser`)
2. Click **Upload** and select your PDF — the file is uploaded to the UC volume
3. The diagram appears in the sidebar with status **unprocessed**
4. Click the diagram → click **Re-extract** (or the extract button for new files)
5. Status changes to **extracting** with a live progress indicator
6. Once done, the BOM table loads automatically with matched and unmatched components
7. Use the **type / circuit / rating** dropdowns to filter the BOM
8. Click **PDF** to download the annotated PDF with colour-coded bounding boxes
### Run extraction from a notebook directly

_Requires: Tier 1+_

Use this when you need more control — e.g. change the model, adjust the retry threshold, or bulk-process a batch without the app.

1. Open `sld_to_bom_pipeline.py`
2. Set the widgets at the top of the notebook:
   - `file_name` — PDF filename (e.g. `AVILA.pdf`). Leave blank for **batch mode** (all unprocessed files)
   - `model` — model endpoint (default: `databricks-claude-sonnet-4-6`)
   - `enable_retry` — `true` / `false`
   - `max_retries` — number of additional attempts after the first (default: `2`)
   - `threshold` — minimum match rate to accept (default: `0.75`)
3. Run **All** (or cell by cell from Cell 1)
4. Cell 8 shows extraction status; Cell 9 shows the full BOM SQL view

> **Re-running a failed file:** In batch mode, files with `ERROR` status are skipped. To retry a failed file, either:
> - Set `file_name` widget to the specific filename and run in single-file mode, **or**
> - Delete the row from the Delta table: `DELETE FROM bom_parser.bom_extractions WHERE file_name = 'YOURFILE.pdf'`
### Talk to the agent

_Requires: Tier 3+ (agent serving endpoint deployed)_

The chat panel in the web app connects to the `sld-bom-agent` serving endpoint.
You can also call it directly via the serving endpoint REST API.

**Example prompts:**

```
"Process all new diagrams with up to 3 retries and threshold 80%"
"Run AVILA.pdf using Opus, no retry"
"What is the match rate for CARRIAZO?"
"Show me unmatched components for ESQUEMAS"
"How many interruptores are in panel Q1?"
"Match references for AVILA.pdf"
"Check stock for A9F74216"
"Find alternatives for 40A 2P circuit breaker"
```

The agent confirms its interpretation of parameters before running anything.
### Get product reference matches

_Requires: Tier 2+ (Vector Search index created, matching job deployed)_

**From the app (Tier 4):**
1. Open a diagram → click the **References** tab
2. Click **Get References** — triggers the `sld-bom-matching` Job asynchronously
3. A spinner shows while the job runs (~1 minute). Results appear automatically when done.
4. The table shows each component with its top-ranked reference, stock status badge, distribution center, and list price.
5. **Override:** use the dropdown to pick an alternative from the top-3 candidates. Changed rows are highlighted.
6. Click **Save** to persist overrides to the `reference_matches` Delta table.
7. Click **Excel** to generate and download a two-sheet Excel file:
   - Sheet 1: BOM + selected references with stock colour coding
   - Sheet 2: All top-N candidates per component (alternatives)
8. Click **Re-run** to re-match after updating catalog data or changing the tier preference.

**From a notebook (Tier 2):**
Open `sld_bom_matching_nb.py`, set the `file_name` widget (or leave blank for batch), run all cells.
### Download an annotated PDF

_Requires: Tier 1+ (extraction must have completed)_

The annotated PDF overlays colour-coded bounding boxes on the original diagram, with component metadata embedded as PDF annotations (hover over a box in a PDF viewer to see type, circuit, calibre, etc.).

- **From the app (Tier 4):** Open a diagram → click **PDF** button in the header
- **From the volume (any tier):** `dbutils.fs.cp("dbfs:/Volumes/.../overlays/AVILA_annotated.pdf", "/tmp/AVILA_annotated.pdf")`

> **404 / button shows error?** The annotated PDF is generated during extraction. If the diagram was processed before this feature was added, click **Re-extract** to regenerate it.
### Re-register the agent after code changes

_Requires: Tier 3+_

After editing `sld_bom_agent_model.py`:

1. Open `sld_bom_agent_uc.py`
2. Run cells **1 → 5** to install deps, load config, and test locally
3. Run cell **6** to log a new MLflow model version
4. Run cell **7** to register to Unity Catalog
5. Run cell **8** to update the serving endpoint (rolling update — no downtime)

> If you only changed the **system prompt** (`sld2bom_system_prompt.txt` in the volume), no re-registration needed — the prompt is read from the volume at inference time.
### Deploy / redeploy the web app

_Requires: Tier 4_

The app source lives in the workspace at `Workspace/Users/<you>/bom_parser/app/`.

To deploy changes:
1. Upload changed files to the workspace (use `databricks workspace import-dir`)
2. Redeploy:
   ```bash
   databricks apps deploy sld-bom-parser \
     --source-code-path /Workspace/Users/<you>/bom_parser/app
   ```
3. The app rebuilds the React frontend and restarts uvicorn automatically (`start.sh`)

> **Verifying the new bundle loaded:** Open the app → View Source → check that `bundle.<hash>.js` matches the hash in `static/assets/`. If the hash is the same as before, files were not uploaded to the correct path.
---
## Configuration reference

All notebooks and the app share the same configuration values:

| Variable | Value | Where set |
|----------|-------|-----------|
| `CATALOG` | `<your_catalog>` | **`config.py`** (single source of truth) |
| `SCHEMA` | `bom_parser` | `config.py` |
| `VOLUME` | `electrical_diagrams` | `config.py` |
| `TABLE_NAME` | `bom_parser.bom_extractions` | `config.py` (derived) |
| `VOLUME_PATH` / `OVERLAY_PATH` | `/Volumes/...` | `config.py` (derived) |
| `AGENT_MODEL_NAME` | `...bom_parser.sld_bom_agent` | `config.py` (derived) |
| `DEFAULT_MODEL_ENDPOINT` | `databricks-claude-sonnet-4-6` | `config.py` |
| `DEFAULT_THRESHOLD` | `0.75` | `config.py` |
| `DEFAULT_MAX_RETRIES` | `2` | `config.py` |
| `MATCHING_JOB_ID` | `<matching_job_id>` | `app/server/config.py` / `app.yaml` |
| `MATCHES_TABLE` | `bom_parser.reference_matches` | `app/server/config.py` (derived) |
| `EXPORTS_TABLE` | `bom_parser.exports` | `app/server/config.py` (derived) |
| App catalog / job IDs | env-specific | `app/app.yaml` (updated per deployment) |
| `DPI` | `400` | `sld_bom_extractor.py` |
| System prompt | `sld2bom_system_prompt.txt` | Repo root → copied to UC volume by `setup.py` |
---
## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Extraction stuck at "IN_PROGRESS" | Databricks Job failed or timed out | Check Jobs → `sld-bom-extraction` run log |
| Low match rate / `threshold_met = false` | Diagram uses non-standard fonts or layout | Increase `max_retries`; inspect unmatched components in the BOM table |
| PDF button shows "Annotated PDF not yet generated" | Diagram processed before annotated PDF feature was added | Click **Re-extract** |
| Agent responds "I don't have a tool for that" | Tool not defined or agent version is stale | Re-register agent via `sld_bom_agent_uc.py` cells 6–8 |
| App shows old UI after redeploy | Files uploaded to wrong workspace path | Upload to `sld-bom-parser/` root, not `sld-bom-parser/app/` |
| `sld2bom_system_prompt.txt` not found | File missing from volume | Re-run `setup.py` cell 4, or manually upload from the repo root to the volume |
| Pipeline notebook `ImportError` on `sld_bom_extractor` | Module not in the same workspace folder as the notebook | Ensure `sld_bom_extractor.py` is in the same workspace directory as `sld_to_bom_pipeline.py` |
| Setup fails on schema/volume creation | Missing UC privileges | Ensure you have `CREATE SCHEMA` on the catalog and `CREATE VOLUME` on the schema |
| References tab shows "No reference matches yet" after clicking Get References | Matching job failed | Check Jobs → `sld-bom-matching` run log; ensure extraction status is SUCCESS |
| References all show OUT_OF_STOCK | Stock table not populated | Run `generate_material_data.py` to seed the stock table |
| Excel export fails with "openpyxl not installed" | Missing dependency | Add `openpyxl` to `app/requirements.txt` and redeploy the app |
