# Changelog

All notable changes to the SLD-to-BOM Parser are documented here.

---

## [0.2.1] — 2026-05-12

### Added

- `CLAUDE.md` — project-wide context for Claude Code sessions: pipeline architecture, key files, deployment conventions, known limitations. Environment-agnostic.
- `AI_FUNCTIONS_EVAL.md` — benchmark results for `ai_parse_document` + `ai_extract` as SLD extraction alternatives, with conclusions.

---

## [0.2.0] — 2026-05-12

### Fixed

**Empty-page match rate**
- Pages where Claude returns zero components (e.g. title-page-only panels) previously scored `rate = 0.0`, causing the quality gate to fail and triggering unnecessary retries.
- Fix: `rate = 1.0` when `total == 0` (nothing to miss → perfect score).

**Scanned PDF match rate**
- Scanned PDFs carry no vector text layer, so the vector-matching step always produced 0 matched / N unmatched, yielding `rate = 0.0`.
- Fix: when `pdf_type == "scanned"` the vision result is accepted as-is and `rate = 1.0`.

**Lone-ID cluster filtering too aggressive**
- Single-span clusters whose only text was a component ID (e.g. `"D1"`) with no adjacent spec text were removed globally. This filtered out valid anchor labels for large breakers (IGAs, MCCBs) whose spec block lands outside the cluster radius.
- Fix: the filter now applies only to clusters in the bottom 15% of the page, where title-block artefacts appear. Lone-ID clusters in the main diagram area are kept, allowing circuit-ID matching to succeed for isolated labels.

**Circuit-ID substring false match**
- The previous substring test caused short IDs like `"D1"` to match circuits labelled `"D10"`, `"D11"`, `"D17"`, etc., producing incorrect high-confidence matches and displacing the correct component.
- Fix: replaced with a word-boundary startswith check to require an exact ID token match.

### Results

| Metric | Before | After |
|--------|--------|-------|
| Files with threshold met | ~28 / 38 | **38 / 38** |
| Per-file average match rate | ~68 % | **~98.7 %** |
| Weighted vector match rate | ~68 % | **~89.6 %** |

---

## [0.1.0] — 2026-03-31

### Added

- Initial pipeline: PDF rasterization → Claude vision extraction → PyMuPDF vector-text matching → Delta table persistence.
- Per-page PDF splitting for multi-panel diagrams.
- Annotated PDF overlay generation (bounding boxes + component labels).
- One-shot environment bootstrap (schema, volume, table, jobs, system prompt).
- MLflow ResponsesAgent registered on Databricks Model Serving.
- React + FastAPI web app for upload, review, BOM download, and agent chat.
- Reference matching pipeline linking extracted components to the Schneider Electric product catalog via Vector Search.
