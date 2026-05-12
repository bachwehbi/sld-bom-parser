# SLD-to-BOM Parser — Claude Context

## Project overview

Extracts a Bill of Materials (BOM) from electrical Single Line Diagram (SLD) PDFs using Claude vision + PyMuPDF vector-text matching. The output is a structured list of electrical protection components (circuit breakers, RCDs, contactors, etc.) with precise pixel coordinates for each component in the diagram. Designed to be redeployable to any customer environment by changing a single value in `config.py`.

## Deployment configuration

All environment-specific values (catalog, schema, volume path, model endpoint, job IDs) are centralised in `config.py`. That is the only file to edit when deploying to a new workspace. Do not hardcode workspace URLs, paths, or IDs anywhere else.

## Pipeline architecture (4 stages)

```
Volume PDF
    │
    ▼ 1. Classify + rasterize
    │   detect_pdf_type() → "vector" / "scanned" / "unrecognized"
    │   PyMuPDF @ 400 DPI → base64 PNG tiles
    │
    ▼ 2. Claude vision extraction
    │   System prompt: sld2bom_system_prompt.txt (stored in UC Volume)
    │   Returns JSON array of components (type, amperage, poles, circuit, panel …)
    │
    ▼ 3. Vector-text matching (vector PDFs only)
    │   PyMuPDF extracts text spans → spatial clustering
    │   Bipartite scoring: circuit_id (+10), amperage (+5), poles (+3), word overlap (+6)
    │   Retry loop if match_rate < threshold (default 0.75), up to 2 retries
    │
    ▼ 4. Delta table write + JPEG overlay
        bom_extractions: one row per PDF page
        overlays/: annotated JPEG + annotated PDF
```

## Key files

| File | Type | Purpose |
|---|---|---|
| `sld_bom_extractor.py` | Python FILE (not notebook) | Core extraction + matching module. Imported by the pipeline notebook at runtime via `sys.path`. |
| `sld_to_bom_pipeline.py` | Notebook | Production pipeline. Run by the extraction job or manually. |
| `sld2bom_system_prompt.txt` | Text | Claude system prompt. Lives both locally and in the UC Volume. |
| `config.py` | Python FILE | All deployment config: catalog, schema, paths, model, job IDs. Single file to change per environment. |
| `setup.py` | Notebook | One-shot environment bootstrap (schema, volume, table, jobs). Run once per deployment. |
| `sld_bom_matching_nb.py` | Notebook | Vector Search matching pipeline — links extracted BOM to the customer product catalog. |
| `sld_bom_agent_uc.py` | Notebook | MLflow ResponsesAgent registration on Databricks Model Serving. |
| `app/` | Directory | FastAPI + React web app (upload, review, BOM download, agent chat). |

## Delta table: `bom_extractions`

One row per processed PDF page. Key columns:

| Column | Description |
|---|---|
| `file_name` | PDF filename (page-split files are named `original_pNN.pdf`) |
| `file_path` | Full `/Volumes/...` path — NOT all files are at volume root; split pages go to `/tmp/` on the driver |
| `pdf_type` | `vector` / `scanned` / `unrecognized` |
| `status` | `SUCCESS` / `ERROR` / `IN_PROGRESS` |
| `bom_json` | JSON array of components; matched ones have `precise_cx`, `precise_cy`, `bbox` |
| `threshold_met` | `true` if match_rate >= 0.75 |
| `attempts_made` | Number of extraction attempts (retry logic) |
| `progress_msg` | Human-readable summary, e.g. `"15/15 matched (100%) in 1 attempt(s)"` |

## Code deployment

Local files are the source of truth. After editing, upload to the Databricks workspace before the next job run. Read the workspace path from `config.py`:

```bash
# Python files (importable modules)
databricks workspace import <WORKSPACE_NOTEBOOK_DIR>/sld_bom_extractor \
  --file sld_bom_extractor.py --format SOURCE --language PYTHON --overwrite

# Notebooks
databricks workspace import <WORKSPACE_NOTEBOOK_DIR>/sld_to_bom_pipeline \
  --file sld_to_bom_pipeline.py --format SOURCE --language PYTHON --overwrite
```

After uploading, verify local and workspace files are identical with `diff <(databricks workspace export ...) <local_file>`.

The **app** (`app/`) is deployed separately via Databricks Apps. The app does NOT import `sld_bom_extractor.py` — it only reads the Delta table and triggers jobs via the Jobs API. App redeploy is only needed when files under `app/` change.

## Important conventions

- **Never commit test files** — `test_*.py` files stay local only.
- **Update `CHANGELOG.md` before committing** — every meaningful commit (bug fix, feature, behaviour change) should have a corresponding entry under the current version section. Typo and doc-only commits can be skipped. Use the format already established in `CHANGELOG.md` (version header, ### Fixed / ### Added, short description with root cause for bug fixes).
- **`sld_bom_extractor.py` is a plain Python file**, not a notebook. It cannot use `%run` or `dbutils` directly. It is imported by the pipeline notebook via `sys.path.insert(0, f"/Workspace{_dir}")`.
- **`config.py` is also a plain Python file** — same import pattern. When running a notebook as a one-time job submission (not via `%run`), inline the config values instead of importing — the workspace directory resolution differs.
- **Split per-page PDFs live in `/tmp/`** on the cluster driver during pipeline execution — they are NOT persisted to the volume. Only original multi-page source PDFs and single-page PDFs uploaded directly by users are in the volume. Do not construct file paths by appending `file_name` to `VOLUME_PATH` — always use `file_path` from `bom_extractions`.
- **`ai_parse_document`** was evaluated (see `AI_FUNCTIONS_EVAL.md`) and is not suitable for SLD extraction — SLDs are vector-graphics schematics, not document-type PDFs. Keep the current Claude + PyMuPDF approach.

## Match rate logic

```python
total = len(matched) + len(unmatched)
if total == 0:
    rate = 1.0        # empty page (title page, blank panel) — not a failure
elif pdf_type == "scanned":
    rate = 1.0        # no vector text layer — vision result accepted as-is
else:
    rate = len(matched) / total
```

## Known limitations

| Issue | Detail |
|---|---|
| PDF Form XObjects | Some PDFs embed diagram sections as Form XObjects. PyMuPDF does not expand them, so components in those sections have no vector text and remain unmatched. threshold_met can still be TRUE if the rest of the page exceeds 75%. |
| Scanned PDFs | No coordinates assigned (vision-only extraction). threshold_met=TRUE by design — vector verification is not possible. |
| Large tiled diagrams | Pages wider than `LARGE_PAGE_THRESHOLD` are split into overlapping tiles before sending to Claude. The `deduplicate_bom()` function handles cross-tile duplicates using canonical panel name normalisation. |
