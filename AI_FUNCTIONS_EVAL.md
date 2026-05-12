# Databricks AI Functions — Evaluation for SLD Extraction

## What we tested

We evaluated two Databricks native AI functions — `ai_parse_document` and `ai_extract` — as potential simplifications to the current SLD-to-BOM extraction pipeline.

**Current pipeline (baseline):**
PDF → PyMuPDF rasterization at 400 DPI → Claude vision extraction → PyMuPDF vector-text cluster matching → Delta table

**Approaches evaluated:**

| Approach | Description |
|---|---|
| `ai_parse_doc` | Run `ai_parse_document` on the PDF, inspect element breakdown and bounding box coverage |
| `ai_extract_full` | Full replacement: `ai_parse_document` → concatenate text content → `ai_extract` with BOM schema |
| `hybrid` | Keep Claude vision extraction, replace PyMuPDF vector clustering with `ai_parse_document` bounding boxes |

**Test files:**

| File | Type | GT components | GT match rate |
|---|---|---|---|
| CARRIAZO_CGBT_page_3 | vector | 13 | 100% |
| UNIFILAR VIVIENDA A CAEIRA | vector | 90 | 91% |
| SP7-Lugones CS-I5.4 BT | vector (large, multi-panel) | 154 | 79% |

All results are stored in `bom_parser.bom_ai_functions_eval` for full reproducibility.

---

## Results

### ai_parse_document — Document structure

| File | GT components | Elements returned | Text | Figures | Runtime |
|---|---|---|---|---|---|
| CARRIAZO (simple) | 13 | 11 | 6 | 2 | 21s |
| UNIFILAR (medium) | 90 | 32 | 19 | 2 | 36s |
| SP7 (complex, tiled) | 154 | 10 | 4 | 3 | 143s |

### ai_extract_full — Full replacement

| File | GT components | Extracted | Type accuracy | Amperage accuracy |
|---|---|---|---|---|
| CARRIAZO | 13 | 0 | — | — |
| UNIFILAR | 90 | 0 | — | — |
| SP7 | 154 | 0 | — | — |

### hybrid — ai_parse_document bounding boxes as coordinate source

| File | GT matched | BBox coverage | Avg distance (normalised) |
|---|---|---|---|
| CARRIAZO | 13/13 | 7.7% | 18.1% |
| UNIFILAR | 82/90 | 4.9% | 20.1% |
| SP7 | 122/154 | 6.6% | 12.9% |

---

## Why it is not the right fit — for this specific use case

`ai_parse_document` is purpose-built for **document-type PDFs**: invoices, contracts, reports, forms — content where the primary value is in structured text, tables, and headings. It excels at those.

Schneider Electric SLDs are a fundamentally different document type:

**1. The content is almost entirely graphical symbols**
An SLD page contains 50–150 electrical component symbols drawn as vector graphics. The relevant information — breaker type, rated current, number of poles, trip curve — is encoded in the *spatial relationship* between a symbol and its annotation labels. `ai_parse_document` sees the page as a collection of a few large graphical regions (2–3 figures per page) rather than individual component annotations. A page with 90 components returns 32 elements total, of which 2 are figures covering most of the diagram area.

**2. Figure elements contain no component data**
For figure-type elements, `ai_parse_document` generates a general image description or leaves the content empty. It does not identify electrical symbols, read annotation values, or produce structured component data. This is expected behaviour — it was not designed for schematic interpretation.

**3. Bounding box granularity is too coarse for coordinate assignment**
The hybrid approach tested whether `ai_parse_document` bounding boxes could replace the current PyMuPDF vector-text clustering as a coordinate source. Only 5–8% of GT-matched components fell within 5% of page size from any `ai_parse_document` element. Average distance was 13–20% of page size. The current PyMuPDF approach produces 40–60 precise text-cluster coordinates per page at sub-pixel accuracy.

**4. Runtime is high relative to output**
SP7 took 143 seconds to return 10 elements from a 154-component diagram. The current pipeline processes the same file in under 60 seconds with full component extraction and coordinate assignment.

---

## Conclusion

`ai_parse_document` and `ai_extract` are the right tools for the majority of enterprise document processing workloads. For electrical single-line diagrams, the content model is fundamentally different: the diagram is a dense vector-graphics schematic, not a text document. The extraction value comes from interpreting electrical symbols in spatial context — which is where a specialised vision model with a domain-specific prompt remains the right approach.

The current pipeline (Claude vision + PyMuPDF vector-text matching) stays in place. `ai_parse_document` could be revisited if Schneider Electric also needs to process **accompanying documentation** alongside the SLDs — specification sheets, installation manuals, or inspection reports — where it would be the natural fit.
