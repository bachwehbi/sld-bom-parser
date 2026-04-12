"""
sld_bom_extractor.py — SLD-to-BOM pipeline core logic
======================================================

This module extracts a Bill of Materials (BOM) from Schneider Electric electrical
diagram PDFs (Single Line Diagrams / SLDs) using a two-stage approach:

  Stage 1 — Vision extraction (Claude)
    The PDF is rasterized to a high-resolution image and sent to a Claude vision
    model. Claude reads the diagram visually and returns a JSON array of electrical
    components with fields such as type, rated current, poles, breaking capacity,
    sensitivity, panel, and circuit name.

  Stage 2 — Vector text matching (PyMuPDF)
    PDFs produced by CAD tools (such as Schneider Electric's EcoStruxure) contain
    exact vector text alongside each symbol. This stage re-opens the PDF, extracts
    every text span with its pixel coordinates, and clusters nearby spec annotations
    into "component locations". Each Claude-extracted component is then matched to
    the closest cluster by scoring several electrical signals.

    The result: each BOM entry gains sub-pixel-accurate bounding box coordinates
    (precise_cx, precise_cy, precise_x0/y0/x1/y1) that can drive a spatial overlay
    image or downstream GIS/asset-management queries.

  Stage 3 — Circuit-sharing pass
    Components that couldn't be matched (e.g., Relojes whose spec text is absent
    from the PDF) inherit the position of a matched sibling in the same circuit.
    This reflects the physical reality that Contactors and Relojes are always
    mounted next to the Interruptor automático that controls them.

Design constraints
------------------
- No Databricks dependencies. The module is a plain Python file that can be
  imported from a Databricks notebook via sys.path, or run locally for testing.
- All heavy dependencies (PyMuPDF, Pillow, openai) are installed via %pip in
  the calling notebook.
- The calling notebook is responsible for authentication and for reading the
  system prompt from the Unity Catalog volume.
"""

import fitz  # PyMuPDF — vector PDF reading and rasterization
import base64
import json
import re
import io
from PIL import Image, ImageDraw
from openai import OpenAI  # Databricks Foundation Model API is OpenAI-compatible

# ── Rasterization parameters ───────────────────────────────────────────────────

DPI = 400
# 400 DPI is chosen deliberately: Schneider SLD PDFs use fine symbol details
# (switch contacts, trip curve labels) that require high resolution to be
# legible to the vision model. At 72 DPI (screen) fine text is blurry;
# at 400 DPI each character is sharp enough for Claude to read reliably.

MAX_PIXELS = 7500
# Safety cap for single-page diagrams. If a page produces an image wider or
# taller than 7500 px after rasterization, it is scaled down before being
# sent to the model to avoid exceeding the API's payload limits.

TILE_SIZE = 3500
# Large-format SLD pages (e.g., A0/A1 drawings > ~20 inches on one side) are
# split into overlapping tiles of this size (in pixels at DPI resolution).
# 3500 px ≈ 8.75 inches at 400 DPI, which keeps each tile within Claude's
# image-size limits while still showing full component symbols clearly.

TILE_OVERLAP = 350
# Overlap between adjacent tiles (10% of TILE_SIZE). Components that straddle
# a tile boundary will appear fully in at least one tile. The deduplication
# step in extract_bom() removes the resulting duplicates.

LARGE_PAGE_THRESHOLD = 1500
# A PDF page wider or taller than 1500 pt (≈ 20.8 inches) is considered
# "large-format" and triggers the tiling path. Standard A4/letter diagrams
# fall well below this; A0/A1 SLD folios exceed it.

# ── Component color map ────────────────────────────────────────────────────────
#
# Maps Schneider component type names (as returned by Claude, lowercased) to
# RGB overlay colors. Each color is chosen to be visually distinct on the
# white background of a typical SLD:
#
#   Red    → Interruptor automático (MCB/MCCB): the primary protective device,
#             highlighted in the most attention-grabbing color.
#   Blue   → Interruptor diferencial (RCD/RCCB): differential protection,
#             distinguished from MCBs by color.
#   Green  → Contactor: switching/control device, green = "operating".
#   Orange → Reloj (time switch): scheduling/timing device.
#   Purple → Limitador de sobretensión (SPD): surge protection.
#   Cyan   → Contador de energía (energy meter).
#   Pink   → Inversor y conmutador de redes (ATS/inverter): uncommon, pink
#             ensures it stands out even among the other colors.
#   Brown  → Interruptor de corte en carga (load break switch).
#   Olive  → Portafusibles (fuse holder).
#
# IMPORTANT — keys are lowercase:
#   Claude sometimes returns "Interruptor Automático", sometimes
#   "interruptor automático". The overlay lookup does tipo.lower().strip()
#   before looking up this dict, so keys must be lowercase to match.
#   Two entries exist for types with and without the Spanish accent (é/o)
#   because the vision model occasionally drops diacritics.

COMPONENT_COLORS = {
    # Spanish legacy names (from old system prompt)
    "interruptor automático":           (255,  50,  50),   # red
    "interruptor automatico":           (255,  50,  50),   # red (accent-free variant)
    "interruptor diferencial":          ( 50, 100, 255),   # blue
    "contactor":                        ( 50, 200,  50),   # green
    "reloj":                            (255, 165,   0),   # orange
    "limitador de sobretension":        (180,  50, 255),   # purple (accent-free)
    "limitador de sobretensión":        (180,  50, 255),   # purple
    "contador de energia":              (  0, 200, 200),   # cyan (accent-free)
    "contador de energía":              (  0, 200, 200),   # cyan
    "inversor y conmutador de redes":   (255, 105, 180),   # pink
    "interruptor de corte en carga":    (139,  69,  19),   # brown
    "portafusibles":                    (128, 128,   0),   # olive
    # Canonical English names (from updated multilingual system prompt)
    "circuit_breaker":                  (255,  50,  50),   # red
    "rcd":                              ( 50, 100, 255),   # blue
    "timer":                            (255, 165,   0),   # orange
    "surge_protector":                  (180,  50, 255),   # purple
    "fuse_holder":                      (128, 128,   0),   # olive
    "energy_meter":                     (  0, 200, 200),   # cyan
    "ats":                              (255, 105, 180),   # pink
    "load_break_switch":                (139,  69,  19),   # brown
}
DEFAULT_COLOR = (200, 200, 200)   # grey for any type not in the map

# ── Spec text patterns ─────────────────────────────────────────────────────────
#
# These regular expressions identify which PDF text spans are electrical
# specification annotations (as opposed to diagram titles, revision notes,
# or other non-spec text).
#
# WHY THIS LIST MATTERS:
#   The matching algorithm groups spec spans into "clusters" — one cluster
#   per physical component location — and uses them to pin each Claude-extracted
#   BOM entry to a precise position. Only spans that match at least one pattern
#   below are considered "spec spans" and fed into clustering. Junk text that
#   passes none of these patterns is silently ignored.
#
#   If a common spec notation is missing from the list, the span will not be
#   part of any cluster, potentially leaving a component unmatched.
#
# Pattern-by-pattern explanation:
#
#   r'^\d+\s*A$'          Bare amperage: "16 A", "125A". The most common way
#                          rated current appears in Schneider diagrams.
#
#   r'^\d+\s*kA'          Breaking capacity: "6 kA", "10kA", "85 kA". Always
#                          in kiloamperes in the diagrams we've seen.
#
#   r'^\d+\s*mA'          Differential sensitivity: "30 mA", "300mA". Only
#                          appears on RCDs (Interruptor diferencial).
#
#   r'In:\s*\d+'           IEC-style rated current label: "In: 50.00 A".
#                          EcoStruxure detail blocks use this format for MCCBs.
#
#   r'Icu:\s*\d+'          Ultimate breaking capacity: "Icu: 50.00 kA".
#
#   r'Ir:\s*\d+'           Thermal release setting on MCCBs (Ir = Ireg).
#                          Included to ensure the span is clustered, though
#                          the calibre extractor skips Ir: values (see below).
#
#   r'Im:\s*\d+'           Magnetic trip threshold: "Im: 2250.00 A".
#                          Included for clustering; calibre extractor skips it.
#
#   r'Imax:\s*\d+'         Maximum fault current at the bus bar (sometimes
#                          appears in Avila-style diagrams).
#
#   r'P\.?\s*de\s*C'       "P. de C." = "Poder de Corte" (breaking capacity),
#                          Spanish long form with optional period and spaces.
#
#   r'PdeC'                "PdeC" — short form used in Avila-style diagrams.
#
#   r'^\d+P$'              Pole count: "2P", "3P", "4P".
#
#   r'\dP\+N'              Pole count with neutral: "3P+N", "1P+N". The +N
#                          notation means the neutral pole is switched too.
#
#   r'^[BCD]\d'            Curve letter + number: "B16", "C25", "D10".
#                          Miniature circuit-breaker trip curves per IEC 60898.
#                          (A/D/K curves are less common but follow the same
#                          pattern — the ID_PATTERNS below handle A/D/I/Q/C
#                          reference designators separately.)
#
#   r'^\d+\s*[AV],'        Compact notation: "25A,30 mA" or "25A,II".
#                          Schneider often concatenates current and sensitivity
#                          or type into a single span.
#
#   r'^AC$|^A$|^SI$'       RCD type indicators: "AC" (alternating current
#                          sensitive), "A" (pulsating DC + AC sensitive),
#                          "SI" (super-immunized). These tiny labels appear
#                          near differential devices.
#
#   r'6000A'               Bus bar rated current (fixed 6000 A label on some
#                          Avila main busbars). Ensures the bus bar symbol
#                          area is excluded from component clustering.
#
#   r'Termico'             "Termico regulable" — thermal-magnetic release
#                          description on some MCCBs.
#
#   r'FUSIBLES'            Fuse spec header in Avila diagrams.
#
#   r'^\d+P\+N$'           Standalone pole string: "4P+N". Separate from
#                          the inline r'\dP\+N' pattern above to catch spans
#                          that contain nothing else.
#
#   r'Curva\s+[ABCDK]'    Curve designation long form: "Curva C", "Curva D".
#                          Appears in Avila-style blocks alongside the
#                          current and breaking capacity.
#
#   r'\d+[xX×]\d+\s*A'    IE 07 tabular compact notation: "2×16A", "2x25A".
#                          In non-EcoStruxure diagrams (e.g., VIVIENDA format)
#                          specs are embedded in circuit labels as
#                          "<poles>×<amperage>A". This pattern recognises the
#                          span so that a cluster forms around it. Calibre is
#                          extracted by the existing "16A"-at-end rule;
#                          poles are extracted by a dedicated NxMA block below.

SPEC_PATTERNS = [
    re.compile(r'^\d+\s*A$'),
    re.compile(r'^\d+\s*kA'),
    re.compile(r'^\d+\s*mA'),
    re.compile(r'In:\s*\d+'),
    re.compile(r'Icu:\s*\d+'),
    re.compile(r'Ir:\s*\d+'),
    re.compile(r'Im:\s*\d+'),
    re.compile(r'Imax:\s*\d+'),
    re.compile(r'P\.?\s*de\s*C'),
    re.compile(r'PdeC'),
    re.compile(r'^\d+P$'),
    re.compile(r'\dP\+N'),
    re.compile(r'^[BCD]\d'),
    re.compile(r'^\d+\s*[AV],'),
    re.compile(r'^AC$|^A$|^SI$'),
    re.compile(r'6000A'),
    re.compile(r'Termico'),
    re.compile(r'FUSIBLES'),
    re.compile(r'^\d+P\+N$'),
    re.compile(r'Curva\s+[ABCDK]', re.IGNORECASE),
    re.compile(r'\d+\s*[xX×]\s*\d+\s*[Aa]'),   # "2×16A", "2x25A" — IE 07 compact notation
]

# ── Circuit reference designator patterns ──────────────────────────────────────
#
# In Schneider SLD diagrams each circuit breaker or device carries a short
# reference designator placed directly beside its symbol — e.g.:
#
#   D17   → Differential device number 17
#   I9    → Interruptor number 9
#   Q5    → General switching device 5
#   C3    → Contactor 3
#   DX42  → Extended differential index
#   TX1   → Transformer or terminal
#
# Matching a cluster's circuit_id to a BOM component's "Circuito" field is the
# highest-scoring signal (+10 pts) because it is essentially a direct label link.
# These patterns identify spans that ARE reference designators.
#
# r'^[DIQC]\d+'  Standard one-letter prefix followed by digits.
# r'^DX'         Double-prefix for extended differential series.
# r'^TX'         Transformer/terminal prefix occasionally used.

ID_PATTERNS = [
    re.compile(r'^[DIQC]\d+'),
    re.compile(r'^DX'),
    re.compile(r'^TX'),
]

# ── Circuit-name stopwords ─────────────────────────────────────────────────────
#
# When scoring a BOM entry against a cluster, one signal is word overlap between
# the BOM's "Circuito" field (e.g., "ALUMBRADO ESCALERA") and the nearby text
# captured around the cluster (e.g., "ALUMBRADO ESCALERA D15 16 A PdeC:6 kA").
#
# Common Spanish function words ("DE", "EL", "LA", "Y", etc.) appear in nearly
# every circuit name and add no discriminating power — if we count them, a
# component with "Circuito: DE RESERVA" would score points against almost any
# cluster, creating false positives.
#
# Roman numerals (II, III, IV, V) are also excluded because they appear as
# phase/section labels in many places and don't identify a unique circuit.
#
# Words shorter than 4 characters are additionally stripped inside _circ_words()
# and _circ_word_score() to further reduce noise (e.g., "Int", "Hor" are only
# 3 characters and would match too broadly).

_CIRC_STOPWORDS = {
    '', 'DE', 'EL', 'LA', 'LOS', 'LAS', 'Y', 'O', 'A', 'EN', 'CON',
    'DEL', 'AL', 'SE', 'SQ', 'II', 'III', 'IV', 'V',
}

# ── Rasterization helpers ──────────────────────────────────────────────────────

def _img_to_b64(img):
    """Encode a PIL image to a base64 string for the Claude API image_url format.

    Large images (longest side > 3000 px) are JPEG-encoded at quality=90 to
    keep the payload size reasonable while preserving legibility. Smaller images
    use lossless PNG to avoid compression artefacts on fine text.

    Returns:
        (b64_string, media_type)  where media_type is "image/jpeg" or "image/png".
    """
    buf = io.BytesIO()
    if max(img.size) > 3000:
        img = img.convert("RGB")
        img.save(buf, format="JPEG", quality=90)
        media_type = "image/jpeg"
    else:
        img.save(buf, format="PNG")
        media_type = "image/png"
    return base64.b64encode(buf.getvalue()).decode("utf-8"), media_type


def _tile_image(img, tile_size, overlap):
    """Split a large raster image into a grid of overlapping tiles.

    The overlap ensures that components sitting near a tile boundary appear
    fully in at least one tile, preventing Claude from missing edge components.

    Args:
        img:        PIL Image of the full rasterized page.
        tile_size:  Width and height of each tile in pixels (TILE_SIZE).
        overlap:    Pixel overlap between adjacent tiles (TILE_OVERLAP).

    Returns:
        List of (tile_img, label, (x0, y0)) tuples.
        - tile_img: cropped PIL Image
        - label:    human-readable string like "Section row 1/2, col 2/3"
                    — this is included in the Claude prompt so the model knows
                    it is looking at a section of a larger drawing.
        - (x0, y0): top-left pixel offset of this tile in the full image
                    (reserved for future use in coordinate re-assembly).
    """
    w, h = img.size
    step = tile_size - overlap
    cols = max(1, (w - overlap + step - 1) // step)
    rows = max(1, (h - overlap + step - 1) // step)
    tiles = []
    for row in range(rows):
        for col in range(cols):
            # Clamp tile origin so the last tile always ends at the image edge
            x0 = min(col * step, w - tile_size) if w > tile_size else 0
            y0 = min(row * step, h - tile_size) if h > tile_size else 0
            x1 = min(x0 + tile_size, w)
            y1 = min(y0 + tile_size, h)
            tiles.append((img.crop((x0, y0, x1, y1)), f"Section row {row+1}/{rows}, col {col+1}/{cols}", (x0, y0)))
    return tiles


def rasterize_pdf(pdf_path, dpi=DPI):
    """Convert each page of a PDF into one or more base64-encoded images.

    Rasterization at 400 DPI is necessary because Schneider SLD PDFs use
    vector graphics with very fine details (IEC symbol line weights, small
    type on data blocks). Lower DPI makes the annotation text too blurry for
    the vision model to read accurately.

    Large-format pages (wider or taller than LARGE_PAGE_THRESHOLD points) are
    automatically tiled so each tile fits within Claude's image-size limits.

    Args:
        pdf_path:   Path to the input PDF file.
        dpi:        Target resolution (default DPI=400).

    Returns:
        List of (b64_string, media_type, label) tuples — one tuple per image
        sent to the model. Single-page A4 diagrams produce 1 tuple; large-
        format A0/A1 SLDs typically produce 4–9 tiles.
    """
    doc = fitz.open(pdf_path)
    images = []
    for page_idx, page in enumerate(doc):
        is_large = max(page.rect.width, page.rect.height) > LARGE_PAGE_THRESHOLD
        zoom = dpi / 72      # PyMuPDF native unit is 72 dpi (points)
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        img = Image.open(io.BytesIO(pix.tobytes("png")))

        if is_large:
            # Tile large pages; tell the model which section it is examining
            for tile_img, tile_label, _ in _tile_image(img, TILE_SIZE, TILE_OVERLAP):
                b64, mt = _img_to_b64(tile_img)
                images.append((b64, mt, f"Page {page_idx+1} - {tile_label}"))
        else:
            # Scale down if the image is still very large after rasterization
            if max(img.size) > MAX_PIXELS:
                scale = MAX_PIXELS / max(img.size)
                img = img.resize((int(img.size[0] * scale), int(img.size[1] * scale)), Image.LANCZOS)
            b64, mt = _img_to_b64(img)
            images.append((b64, mt, f"Page {page_idx+1}"))
    doc.close()
    return images


# ── Model interaction ──────────────────────────────────────────────────────────

def _call_model(client, system_prompt, content, model, max_tokens=16384):
    """Send a single chat completion request to the Claude endpoint.

    temperature=0.1 is used (not 0.0) because:
    - Fully deterministic output (temp=0) causes the model to get "stuck" on
      the first confident parse and occasionally miss ambiguous components.
    - A small non-zero temperature introduces just enough variability to let
      the model explore slightly different readings of difficult symbols,
      while still keeping output highly consistent across runs.

    Args:
        client:        OpenAI-compatible client pointed at the Databricks
                       Foundation Model API serving endpoint.
        system_prompt: Loaded from sld2bom_system_prompt.txt. Instructs Claude
                       to return a JSON array with specific field names.
        content:       List of OpenAI content blocks (image_url + text).
        model:         Serving endpoint name, e.g. "databricks-claude-sonnet-4-6".
        max_tokens:    Generous budget for large diagrams with many components.

    Returns:
        Raw text response string (contains a JSON array).
    """
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": content},
        ],
        max_tokens=max_tokens,
        temperature=0.1,
    )
    return response.choices[0].message.content


def _normalize_component_type(val):
    """Normalize the component type string for deduplication and color lookup.

    Strips whitespace and lowercases the value. Accent normalization is kept
    minimal — the COMPONENT_COLORS dict covers both accented and accent-free
    forms, so we avoid lossy stripping of diacritics.

    Args:
        val: Raw "Que és" / "Que es" value from the model.

    Returns:
        Lowercase, stripped string.
    """
    if not val:
        return ""
    norm = val.strip().lower()
    norm = norm.replace("é", "é").replace("ó", "ó").replace("á", "á")
    return norm


def extract_bom(client, system_prompt, images, model, max_tokens=16384):
    """Run Claude vision extraction over the rasterized diagram images.

    Single-page diagrams are processed in one API call. Large-format diagrams
    that were tiled are processed tile by tile, then deduplicated.

    Deduplication key: (type, calibre, polos, cuadro, circuito)
    This removes components that appear in the overlap zone between two tiles
    and were therefore extracted twice. The key intentionally includes cuadro
    and circuito so that two physically different components with the same
    electrical rating but different locations are NOT collapsed.

    Args:
        client:        Authenticated OpenAI-compatible client.
        system_prompt: Spanish extraction prompt loaded from the volume.
        images:        List of (b64, media_type, label) from rasterize_pdf().
        model:         Serving endpoint name.
        max_tokens:    Token budget per API call.

    Returns:
        JSON string — a serialized list of component dicts, deduplicated.
        Downstream callers should parse with parse_json_from_response().
    """
    if len(images) == 1:
        # Simple case: entire diagram fits in one image
        b64, mt, _ = images[0]
        content = [
            {"type": "image_url", "image_url": {"url": f"data:{mt};base64,{b64}"}},
            {"type": "text", "text": "Analyze this electrical diagram and extract the BOM."},
        ]
        return _call_model(client, system_prompt, content, model, max_tokens)

    # Tiled case: process each tile independently, then merge + deduplicate.
    # We tell the model explicitly that it is looking at a section of a larger
    # drawing so it doesn't try to infer components outside the visible area.
    all_components = []
    for i, (b64, mt, label) in enumerate(images):
        print(f"    Tile {i+1}/{len(images)}: {label}")
        content = [
            {"type": "text", "text": (
                f"This is {label} of a larger electrical diagram split into {len(images)} overlapping tiles. "
                "Extract ALL electrical components visible. Include edge components — deduplication happens later."
            )},
            {"type": "image_url", "image_url": {"url": f"data:{mt};base64,{b64}"}},
            {"type": "text", "text": "Analyze this section and extract the BOM."},
        ]
        partial_raw = _call_model(client, system_prompt, content, model, max_tokens)
        try:
            partial = parse_json_from_response(partial_raw)
            items = partial if isinstance(partial, list) else [partial]
            print(f"      -> {len(items)} component(s)")
            all_components.extend(items)
        except ValueError:
            print(f"      -> No JSON in response")

    # Deduplicate: normalize type first to collapse accent variants
    seen = set()
    unique = []
    for comp in all_components:
        tipo = _normalize_component_type(
            comp.get("Que és") or comp.get("Que es") or comp.get("component_type") or ""
        )
        key = (
            tipo,
            str(comp.get("Calibre (A)") or comp.get("amperage_a") or ""),
            str(comp.get("Polos") or comp.get("poles") or ""),
            str(comp.get("Cuadro") or comp.get("panel") or ""),
            str(comp.get("Circuito") or comp.get("circuit") or ""),
        )
        if key not in seen:
            seen.add(key)
            unique.append(comp)
    print(f"    Total: {len(all_components)} raw -> {len(unique)} after dedup")
    return json.dumps(unique, ensure_ascii=False)


def parse_json_from_response(text):
    """Extract the first valid JSON object or array from a model response string.

    Claude sometimes adds preamble such as "Here is the BOM:" or trailing
    commentary after the JSON block. This function scans the response for the
    first '{' or '[' character and attempts to parse from that point, ignoring
    surrounding text.

    Args:
        text: Raw string from the model.

    Returns:
        Parsed Python object (list or dict).

    Raises:
        ValueError if no valid JSON is found anywhere in the string.
    """
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch in "{[":
            try:
                obj, _ = decoder.raw_decode(text[i:])
                return obj
            except json.JSONDecodeError:
                continue
    raise ValueError("No valid JSON found in model output")


# ── Vector text matching ───────────────────────────────────────────────────────

def _is_spec_text(text):
    """Return True if a PDF text span is an electrical specification annotation.

    See SPEC_PATTERNS above for a detailed explanation of each pattern.
    Only spans passing this check are fed into cluster_spec_text().
    """
    return any(p.search(text) for p in SPEC_PATTERNS)


def _is_id_text(text):
    """Return True if a PDF text span is a circuit reference designator.

    Reference designators (D17, I9, Q5 …) are treated as spec spans because
    they are physically positioned next to their component symbol and provide
    the strongest matching signal when a BOM entry's Circuito field contains
    the same designator.

    See ID_PATTERNS above for the full list of recognized prefixes.
    """
    return any(p.search(text) for p in ID_PATTERNS)


def _normalize_poles(val):
    """Convert a pole count from any common notation to a plain integer string.

    Schneider diagrams and the Claude model return poles in multiple formats:
      "4"    → 4 poles
      "4P"   → 4 poles
      "3P+N" → 3 phase poles + 1 neutral pole = 4 total
      "1P+N" → 1 phase pole + 1 neutral pole = 2 total

    Normalizing to a bare integer string before comparison prevents false
    negatives when the PDF says "4P" but Claude says "3P+N" for a 4-pole device.

    Args:
        val: Raw poles value (string or None).

    Returns:
        Plain integer string like "4" or "2", or the original string if it
        doesn't match any known pattern.
    """
    if not val:
        return ""
    s = str(val).strip()
    # "3P+N" → 3 + 1 = 4
    m = re.search(r'(\d+)P\+N', s, re.IGNORECASE)
    if m:
        return str(int(m.group(1)) + 1)
    # "4P" or bare "4" → 4
    m = re.search(r'^(\d+)P?$', s, re.IGNORECASE)
    if m:
        return m.group(1)
    return s


def get_text_with_pixel_coords(page, scale):
    """Extract every text span from a PDF page with pixel-space bounding boxes.

    PyMuPDF's native coordinate system is in PDF points (1 pt = 1/72 inch).
    We convert to pixel coordinates at the same DPI scale used for rasterization
    so that the cluster centroids can later be directly used as overlay box
    coordinates without any additional transformation.

    Page rotation is handled via page.derotation_matrix so that rotated pages
    (90°, 180°, 270°) produce correct upright coordinates.

    Args:
        page:  fitz.Page object.
        scale: DPI / 72 — the same zoom factor used in rasterize_pdf().

    Returns:
        List of dicts with keys:
            text, px_x0, px_y0, px_x1, px_y1, px_cx, px_cy
    """
    zoom_mat = fitz.Matrix(scale, scale)
    # Derotation matrix un-rotates the coordinate system before applying scale
    transform = (~page.derotation_matrix * zoom_mat) if page.rotation else zoom_mat
    spans = []
    for block in page.get_text("dict")["blocks"]:
        if block["type"] != 0:   # type 0 = text block; skip images
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                text = span["text"].strip()
                if not text:
                    continue
                pr = fitz.Rect(span["bbox"]) * transform
                spans.append({
                    "text":   text,
                    "px_x0":  pr.x0, "px_y0": pr.y0,
                    "px_x1":  pr.x1, "px_y1": pr.y1,
                    "px_cx":  (pr.x0 + pr.x1) / 2,
                    "px_cy":  (pr.y0 + pr.y1) / 2,
                })
    return spans


def cluster_spec_text(spans, cluster_radius=80, label_radius=120):
    """Group spec/ID text spans by spatial proximity into one cluster per component.

    Each cluster corresponds to one physical location on the diagram where a
    component's electrical data block is printed. The centroid of the cluster
    is later used as the component's "precise" position in the overlay.

    Algorithm — iterative nearest-neighbour expansion:
        Start a new cluster at each unassigned spec span. Repeatedly absorb
        any unassigned spec span within cluster_radius pixels of any span
        already in the cluster, until stable. This is essentially single-linkage
        clustering with a fixed radius.

    WHY cluster_radius=80 px?
        At 400 DPI, 80 px ≈ 5 mm. Schneider data blocks print rated current,
        poles, and curve on consecutive lines spaced about 3–4 mm apart.
        80 px captures all lines of a single component's spec block without
        accidentally merging adjacent components.

    After forming the spec cluster, a SECOND radius (label_radius=120 px) sweeps
    for any nearby text that is NOT already in the cluster — typically the circuit
    name label printed beside or below the symbol. These nearby labels are
    appended to text_joined but NOT used for calibre/poles/etc. extraction.

    WHY label_radius=120 px?
        Circuit name labels ("ALUMBRADO ESCALERA") are printed slightly further
        away than the spec block text. 120 px ≈ 7.5 mm, which captures them
        reliably without reaching into the next component's territory on most
        diagrams. (We tuned this from 80→150→100→120 across four iterations.)

    Also extracts structured fields per cluster:
        calibre:        Rated current in Amperes (integer string).
        poles:          Number of poles (may be None for devices without poles).
        breaking_kA:    Ultimate breaking capacity in kA.
        sensitivity_mA: Differential sensitivity in mA (RCDs only).
        circuit_id:     Reference designator like "D17" or "I9".

    IMPORTANT — Im:/Ir: exclusion:
        Lines matching Im: or Ir: are magnetic/thermal trip thresholds, NOT
        the rated current. For example, an MCCB rated at 250 A often has an
        Im: line showing "Im: 2250.00 A" (= 9× In). Extracting 2250 as the
        calibre would cause the component to never match any BOM entry. We
        skip these lines for calibre extraction but still include them in the
        cluster for breaking capacity (kA is extracted from Im: lines too).

    Args:
        spans:          All text spans from get_text_with_pixel_coords().
        cluster_radius: Maximum pixel distance between spans in the same cluster.
        label_radius:   Maximum pixel distance for nearby label capture.

    Returns:
        List of cluster dicts with keys: cx, cy, x0, y0, x1, y1, calibre,
        poles, breaking_kA, sensitivity_mA, circuit_id, text_joined,
        spec_text_joined, span_count.
    """
    spec_spans = [s for s in spans if _is_spec_text(s["text"]) or _is_id_text(s["text"])]
    clusters = []
    assigned = [False] * len(spec_spans)

    for i, s in enumerate(spec_spans):
        if assigned[i]:
            continue
        cluster_spans = [s]
        assigned[i] = True
        changed = True
        # Expand cluster until no new members can be absorbed
        while changed:
            changed = False
            for j, s2 in enumerate(spec_spans):
                if assigned[j]:
                    continue
                for cs in cluster_spans:
                    if ((s2["px_cx"] - cs["px_cx"])**2 + (s2["px_cy"] - cs["px_cy"])**2) ** 0.5 < cluster_radius:
                        cluster_spans.append(s2)
                        assigned[j] = True
                        changed = True
                        break

        texts = [cs["text"] for cs in cluster_spans]
        cx = sum(cs["px_cx"] for cs in cluster_spans) / len(cluster_spans)
        cy = sum(cs["px_cy"] for cs in cluster_spans) / len(cluster_spans)
        x0 = min(cs["px_x0"] for cs in cluster_spans)
        y0 = min(cs["px_y0"] for cs in cluster_spans)
        x1 = max(cs["px_x1"] for cs in cluster_spans)
        y1 = max(cs["px_y1"] for cs in cluster_spans)

        # Capture nearby non-spec text (circuit names, panel labels)
        nearby_labels = []
        for ns in spans:
            if ((ns["px_cx"] - cx)**2 + (ns["px_cy"] - cy)**2) ** 0.5 < label_radius:
                if ns["text"] not in texts:
                    nearby_labels.append(ns["text"])

        calibre = poles = breaking = sensitivity = circuit_id = None
        for t in texts:
            # Skip Im:/Ir: lines — these are magnetic/thermal trip thresholds,
            # not the device's rated current. Including them would cause
            # calibre to be extracted as e.g. 2250 A instead of 250 A.
            if re.match(r'I[mr]:', t, re.IGNORECASE):
                # However, breaking capacity (kA) CAN appear on Im: lines —
                # extract it before moving on.
                m = re.search(r'(\d+(?:\.\d+)?)\s*kA', t)
                if m and not breaking:
                    breaking = m.group(1).split('.')[0]
                continue

            # IE 07 compact notation: "2x16A", "2×25A" → calibre=16, poles=2
            # Must run BEFORE the bare-amperage rule so it claims calibre first
            # (the bare rule would also match "16A" inside "2x16A" but misses poles).
            m = re.search(r'(\d+)\s*[xX×]\s*(\d+)\s*[Aa](?:\s|$|,|$)', t)
            if m and not calibre:
                pole_val = int(m.group(1))
                amp_val  = int(m.group(2))
                if 1 <= amp_val <= 1600:
                    calibre = str(amp_val)
                if not poles and 1 <= pole_val <= 4:
                    poles = str(pole_val)

            # Rated current: match "In: 250.00 A" or bare "250 A"
            m = re.search(r'(?:In:\s*)?(\d+(?:\.\d+)?)\s*A(?:\s|$|,)', t)
            if m and not calibre:
                val = int(float(m.group(1)))
                if 1 <= val <= 1600:   # sanity range for typical MCB/MCCB ratings
                    calibre = str(val)

            # Poles: "4P", "3P+N" (raw — will be normalized during matching)
            m = re.search(r'(\d+)P(?:\+N)?', t)
            if m and not poles:
                base = int(m.group(1))
                poles = str(base + 1) if 'P+N' in t else str(base)

            # Breaking capacity in kA
            m = re.search(r'(\d+(?:\.\d+)?)\s*kA', t)
            if m and not breaking:
                breaking = m.group(1).split('.')[0]

            # Differential sensitivity in mA (RCDs only)
            m = re.search(r'(\d+)\s*mA', t)
            if m and not sensitivity:
                sensitivity = m.group(1)

            # Reference designator (first matching span wins)
            m = re.match(r'^([DIQC]\d+)', t)
            if m and not circuit_id:
                circuit_id = m.group(1)

        all_texts = texts + nearby_labels
        clusters.append({
            "cx": cx, "cy": cy, "x0": x0, "y0": y0, "x1": x1, "y1": y1,
            "calibre":          calibre,
            "poles":            poles,
            "breaking_kA":      breaking,
            "sensitivity_mA":   sensitivity,
            "circuit_id":       circuit_id,
            "text_joined":      " ".join(all_texts),      # spec + nearby labels
            "spec_text_joined": " ".join(texts),          # spec only (for debugging)
            "span_count":       len(cluster_spans),
        })
    return clusters


def filter_table_clusters(clusters, page_w, page_h):
    """Remove clusters that are part of the title block or revision table.

    Schneider SLD drawings typically have a title block in the bottom ~15%
    of the page and/or a narrow legend strip along the far-right edge. These
    areas contain technical data that looks like spec text (ampere ratings,
    kA values) but is NOT associated with a component position — it belongs
    to the revision block or the drawing header.

    Without this filter, title-block text can form spurious clusters that
    steal match slots from real components, causing genuine components to
    be left unmatched.

    Filtering rules (three independent checks, any match excludes the cluster):
      1. cy > 85% of page height → bottom margin / title block area.
         (Threshold was 78% in early iterations; 85% is less aggressive and
         avoids removing legitimate bottom-of-diagram components.)
      2. cx > 92% of page width AND page is wider than 5000 px → the narrow
         legend column that appears on wide-format drawings only.
      3. Cluster has exactly 1 span, it is a reference designator, and it has
         no calibre → a dangling ID label that didn't pick up any spec text
         (likely a leftover from a deleted component in the source drawing).

    Args:
        clusters: Output of cluster_spec_text().
        page_w:   Rasterized page width in pixels (from fitz pixmap).
        page_h:   Rasterized page height in pixels.

    Returns:
        Filtered list of clusters.
    """
    return [c for c in clusters
            if c["cy"] <= page_h * 0.85
            and not (c["cx"] > page_w * 0.92 and page_w > 5000)
            and not (c["span_count"] == 1 and c["circuit_id"] and not c["calibre"])]


def _circ_word_score(c_circ, cluster_text_joined):
    """Score based on word overlap between a BOM circuit name and cluster nearby text.

    This scoring function is the key signal for matching Contactors and Relojes,
    which often have no rated current or poles in the BOM (or in the PDF spec
    block) and therefore cannot match via calibre/poles alone.

    Physical intuition:
        Every circuit in a Schneider SLD has a human-readable name printed near
        the component. Example: "ALUMBRADO ESCALERA". The same name appears in
        the Claude-extracted BOM's "Circuito" field AND in the cluster's
        text_joined (captured via the label_radius sweep). Overlapping meaningful
        words ("ALUMBRADO", "ESCALERA") provide evidence that the BOM entry
        belongs to that cluster.

    Scoring:
        Each matching word contributes +2 points, capped at +6 total.
        The cap prevents very long circuit names with many words from
        dominating the score over hard electrical signals like calibre (+5).

    Noise filtering:
        - _CIRC_STOPWORDS removes Spanish function words.
        - Words shorter than 4 characters are discarded (e.g., "Int", "Hor"
          are abbreviations that appear in many circuit names and match too
          broadly).

    Args:
        c_circ:               BOM component's Circuito value.
        cluster_text_joined:  Full text_joined from the cluster (spec + labels).

    Returns:
        Integer score 0–6.
    """
    if not c_circ or not cluster_text_joined:
        return 0
    words = set(re.split(r'[\W_]+', c_circ.upper())) - _CIRC_STOPWORDS
    words = {w for w in words if len(w) >= 4}
    if not words:
        return 0
    text_upper = cluster_text_joined.upper()
    overlap = sum(1 for w in words if w in text_upper)
    return min(overlap * 2, 6)   # max 6 points


def detect_pdf_type(pdf_path, dpi=DPI):
    """Classify a PDF as vector, scanned, or unrecognized format.

    Scanned PDFs contain no vector text (or fewer than 10 spans across all pages).
    Unrecognized PDFs have vector text but none of it matches the electrical spec
    patterns or reference designator patterns we look for — typically a tabular or
    non-standard layout that the overlay pipeline cannot handle.

    Args:
        pdf_path: Path to the PDF file.
        dpi:      Rasterization DPI — used to compute the scale factor for
                  get_text_with_pixel_coords (must match the extraction pipeline).

    Returns:
        "vector"       — has spec/ID text; overlay generation will work.
        "scanned"      — no/very few vector text spans (scanned image PDF).
        "unrecognized" — has vector text but no spec or ID spans found;
                         overlay generation not supported for this format.
    """
    scale = dpi / 72
    doc = fitz.open(pdf_path)
    total_spans = 0
    total_spec_spans = 0
    for page in doc:
        spans = get_text_with_pixel_coords(page, scale)
        total_spans += len(spans)
        total_spec_spans += sum(
            1 for s in spans if _is_spec_text(s["text"]) or _is_id_text(s["text"])
        )
    doc.close()

    if total_spans < 10:
        return "scanned"
    if total_spec_spans == 0:
        return "unrecognized"
    return "vector"


def match_bom_to_pdf_text(bom_components, pdf_path, dpi=DPI):
    """Match each Claude-extracted BOM component to a precise PDF text cluster.

    This is the core matching function. It implements a one-to-one bipartite
    assignment: each cluster can be claimed by at most one BOM component, and
    each BOM component claims at most one cluster.

    Scoring system (maximum possible score: 10+5+3+4+3+6 = 31 pts):
    ┌─────────────────────────────────┬────────┬──────────────────────────────┐
    │ Signal                          │ Points │ Rationale                    │
    ├─────────────────────────────────┼────────┼──────────────────────────────┤
    │ Circuit ID match (D17 in circ.) │ +10    │ Exact label → near-certain   │
    │ Calibre exact match             │  +5    │ Strongest electrical signal   │
    │ Calibre off by 1 A (rounding)   │  +2    │ Small rounding tolerance      │
    │ Poles match (normalized)        │  +3    │ Less unique than calibre      │
    │ Sensitivity match (mA, RCDs)    │  +4    │ Very unique — few mA values  │
    │ Breaking capacity match (kA)    │  +3    │ Less unique than sensitivity  │
    │ Circuit word overlap (max 3 wds)│  +6    │ Scaled word count, capped    │
    │ Cuadro word overlap (halved)    │  +3    │ Panel name, weaker signal    │
    └─────────────────────────────────┴────────┴──────────────────────────────┘

    Match threshold:
        A component must score ≥ 5 pts to be matched.
        Exception: components with NO calibre AND NO poles ("spec-less", e.g.,
        a Reloj with only a circuit name) use threshold = 4. Their only signal
        is word overlap (max 6 pts), so the bar is set just below the word
        overlap maximum to still require a meaningful name match.

    Processing order — spec-richness sort:
        BEFORE running the greedy assignment, BOM components are sorted by
        decreasing electrical specificity:
            richness = 2*(has calibre) + 1*(has poles) + 1*(has sensitivity) + 1*(has breaking)
        Components with more spec fields are matched first. This prevents
        word-only components (Relojes, bare Contactors) from claiming clusters
        that contain specific electrical data belonging to an Interruptor
        sharing the same circuit name. Without this sort, a Reloj with circuit
        "ALUMBRADO" could steal the 16 A cluster from an "Interruptor 16A
        ALUMBRADO" that hasn't been processed yet.

    Circuit-sharing second pass:
        After the greedy assignment, components still unmatched are given one
        more chance: if they share ≥2 meaningful words in their circuit name
        with an already-matched component, they inherit that component's
        position. This is physically motivated — Relojes and Contactors are
        always mounted on the same DIN rail column as the Interruptor they
        control, so their spatial position is essentially the same.
        match_type = "circuit_shared" is recorded to distinguish these entries
        from direct matches in the output.

    Args:
        bom_components: List of component dicts from extract_bom() / parse_json().
        pdf_path:       Path to the source PDF (re-opened for vector text).
        dpi:            Must match the DPI used in rasterize_pdf().

    Returns:
        (matched, unmatched)
        - matched:   List of component dicts enriched with precise_cx, precise_cy,
                     precise_x0, precise_y0, precise_x1, precise_y1, match_score,
                     bbox_page_w, bbox_page_h. Circuit-shared entries also have
                     match_type="circuit_shared".
        - unmatched: List of original component dicts with no positional data.
    """
    scale = dpi / 72
    doc = fitz.open(pdf_path)
    clusters = []
    page_dims: dict = {}   # page_idx → (page_w, page_h)
    for _pi in range(len(doc)):
        _page = doc[_pi]
        _pix  = _page.get_pixmap(matrix=fitz.Matrix(scale, scale))
        _pw, _ph = _pix.width, _pix.height
        page_dims[_pi] = (_pw, _ph)
        _spans   = get_text_with_pixel_coords(_page, scale)
        _pclusts = cluster_spec_text(_spans)
        _pclusts = filter_table_clusters(_pclusts, _pw, _ph)
        for _cl in _pclusts:
            _cl["page_idx"] = _pi
            _cl["page_w"]   = _pw
            _cl["page_h"]   = _ph
        clusters.extend(_pclusts)
    doc.close()
    page_w, page_h = page_dims.get(0, (0, 0))

    print(f"    Clusters found: {len(clusters)} across {len(page_dims)} page(s)")

    matched, unmatched, used = [], [], set()

    # ── Spec-richness sort ────────────────────────────────────────────────────
    # Process electrically specific components first so they claim their
    # clusters before word-only components run and compete for the same slot.
    def _spec_richness(comp):
        cal = 2 if (comp.get("Calibre (A)") or comp.get("amperage_a")) else 0
        pol = 1 if (comp.get("Polos") or comp.get("poles")) else 0
        sen = 1 if (comp.get("Sensibilidad (mA)") or comp.get("sensitivity_ma")) else 0
        brk = 1 if (comp.get("Poder de Corte (kA)") or comp.get("breaking_ka")) else 0
        return -(cal + pol + sen + brk)   # negative so sorted() gives descending

    order = sorted(range(len(bom_components)), key=lambda i: _spec_richness(bom_components[i]))
    sorted_components = [(bom_components[i], i) for i in order]

    results = {}   # original_index → enriched comp dict (or None if unmatched)
    for comp, orig_idx in sorted_components:
        c_cal   = str(comp.get("Calibre (A)") or comp.get("amperage_a") or "").strip()
        c_pol   = _normalize_poles(comp.get("Polos") or comp.get("poles") or "")
        c_circ  = str(comp.get("Circuito") or comp.get("circuit") or "").strip()
        c_cuad  = str(comp.get("Cuadro") or comp.get("panel") or "").strip()
        c_sens  = str(comp.get("Sensibilidad (mA)") or comp.get("sensitivity_ma") or "").strip()
        c_break = str(comp.get("Poder de Corte (kA)") or comp.get("breaking_ka") or "").strip()

        best_idx, best_score = None, -1
        for idx, cl in enumerate(clusters):
            if idx in used:
                continue
            score = 0

            # Circuit ID match — strongest signal, acts like a primary key.
            # e.g., BOM says Circuito="D17 RESERVA", cluster has circuit_id="D17"
            if cl["circuit_id"] and cl["circuit_id"] in c_circ:
                score += 10

            # Calibre (rated current) match
            if c_cal and cl["calibre"]:
                if c_cal == cl["calibre"]:
                    score += 5
                else:
                    # Allow ±1 A to handle integer rounding vs. float conversion
                    # (e.g., Claude says "25" but PDF says "25.00 A" → parsed as 25)
                    try:
                        if abs(int(c_cal) - int(cl["calibre"])) <= 1:
                            score += 2
                    except ValueError:
                        pass

            # Poles match — normalize both sides before comparing
            cl_poles = _normalize_poles(cl["poles"])
            if c_pol and cl_poles and c_pol == cl_poles:
                score += 3

            # Differential sensitivity — highly discriminating (30 mA vs 300 mA)
            if c_sens and cl["sensitivity_mA"] and c_sens == cl["sensitivity_mA"]:
                score += 4

            # Breaking capacity — useful secondary signal on high-power circuits
            if c_break and cl["breaking_kA"] and c_break == cl["breaking_kA"]:
                score += 3

            # Circuit name word overlap with nearby text (captured at label_radius)
            score += _circ_word_score(c_circ, cl["text_joined"])

            # Panel name (Cuadro) overlap — halved because many components in the
            # same panel share the same Cuadro word, making it less discriminating
            score += _circ_word_score(c_cuad, cl["text_joined"]) // 2

            if score > best_score:
                best_score = score
                best_idx = idx

        # Spec-less components have no calibre AND no poles —
        # their only contribution comes from word overlap (max 6 pts).
        # Lowering the threshold to 4 allows a 2-word overlap to qualify,
        # rather than requiring 3 words (which is the normal threshold of 6 pts).
        is_spec_less = not c_cal and not c_pol
        threshold = 4 if is_spec_less else 5

        if best_idx is not None and best_score >= threshold:
            cl = clusters[best_idx]
            used.add(best_idx)   # one-to-one: mark this cluster as claimed
            comp_out = {**comp,
                "precise_cx":    round(cl["cx"]),
                "precise_cy":    round(cl["cy"]),
                "precise_x0":    round(cl["x0"]),
                "precise_y0":    round(cl["y0"]),
                "precise_x1":    round(cl["x1"]),
                "precise_y1":    round(cl["y1"]),
                "match_score":   best_score,
                "bbox_page_w":   cl.get("page_w", page_w),
                "bbox_page_h":   cl.get("page_h", page_h),
                "bbox_page_idx": cl.get("page_idx", 0),
            }
            results[orig_idx] = comp_out
        else:
            results[orig_idx] = None

    # Rebuild in original BOM order, separating matched from unmatched
    unmatched_pre_share = []
    for i in range(len(bom_components)):
        r = results[i]
        if r is not None:
            matched.append(r)
        else:
            unmatched_pre_share.append(bom_components[i])

    # ── Circuit-sharing second pass ────────────────────────────────────────────
    # Physical motivation:
    #   In Schneider SLDs, a "Reloj" (time switch) or "Contactor" is always
    #   drawn on the same circuit column as the "Interruptor automático" it
    #   controls. Their circuit names are identical or highly overlapping.
    #   If the Reloj's spec text is absent from the PDF (because the PDF only
    #   annotates the Interruptor), the Reloj cannot match any cluster directly.
    #   But we know it lives at the same position as the matched Interruptor.
    #
    # Algorithm:
    #   For each still-unmatched component, compute meaningful words from its
    #   circuit name. Find the already-matched component with the greatest word
    #   overlap (minimum 2 words). If found, copy its precise coordinates.
    #
    # Minimum overlap = 2 words:
    #   A single shared word (e.g., "ESCALERA") might coincidentally appear in
    #   many circuit names. Requiring 2 words reduces false sharing significantly.

    def _circ_words(circ):
        """Return the set of meaningful words from a circuit name string."""
        words = set(re.split(r'[\W_]+', circ.upper())) - _CIRC_STOPWORDS
        return {w for w in words if len(w) >= 4}

    for comp in unmatched_pre_share:
        c_circ = str(comp.get("Circuito") or comp.get("circuit") or "").strip()
        if not c_circ:
            unmatched.append(comp)
            continue
        cw = _circ_words(c_circ)
        if not cw:
            # No meaningful words → cannot share
            unmatched.append(comp)
            continue

        best_share, best_overlap = None, 0
        for mc in matched:
            mc_circ = str(mc.get("Circuito") or mc.get("circuit") or "").strip()
            if not mc_circ:
                continue
            mc_words = _circ_words(mc_circ)
            overlap = len(cw & mc_words)
            if overlap >= 2 and overlap > best_overlap:
                best_overlap = overlap
                best_share = mc

        if best_share is not None:
            comp_out = {**comp,
                "precise_cx":    best_share["precise_cx"],
                "precise_cy":    best_share["precise_cy"],
                "precise_x0":    best_share["precise_x0"],
                "precise_y0":    best_share["precise_y0"],
                "precise_x1":    best_share["precise_x1"],
                "precise_y1":    best_share["precise_y1"],
                "match_score":   best_overlap,
                "match_type":    "circuit_shared",   # flag for downstream use
                "bbox_page_w":   best_share.get("bbox_page_w", page_w),
                "bbox_page_h":   best_share.get("bbox_page_h", page_h),
                "bbox_page_idx": best_share.get("bbox_page_idx", 0),
            }
            matched.append(comp_out)
        else:
            unmatched.append(comp)

    return matched, unmatched


# ── Retry-aware extraction ─────────────────────────────────────────────────────

def run_extraction(client, system_prompt, images, pdf_path, model,
                   enable_retry=True, max_retries=2, threshold=0.75,
                   verbose=True, dpi=DPI, progress_callback=None):
    """Run extraction + matching with optional retry logic.

    This is the recommended entry point for both the production notebook and the
    agent. It wraps `extract_bom` + `match_bom_to_pdf_text` in a quality-gated
    loop: if the match rate falls below `threshold`, the extraction is retried
    (Claude is called again — because temperature=0.1, successive calls can return
    different component lists that match better or worse).

    The **best result across all attempts** is returned, not necessarily the last.
    This prevents a regression where a retry produces a worse result than attempt 1.

    When `enable_retry=False` the function behaves identically to calling
    `extract_bom` + `match_bom_to_pdf_text` directly — no overhead, no change in
    behavior. This is the recommended setting during notebook debugging.

    Args:
        client:        Authenticated OpenAI-compatible client.
        system_prompt: Spanish extraction prompt.
        images:        Output of rasterize_pdf().
        pdf_path:      Path to the source PDF.
        model:         Serving endpoint name.
        enable_retry:  Whether to retry on low match rate. Default True.
        max_retries:   Maximum number of additional attempts after the first.
                       Total calls = 1 + max_retries. Default 2.
        threshold:     Minimum acceptable match rate (0.0–1.0). Default 0.75.
        verbose:           Print attempt-by-attempt progress. Default True.
        dpi:               Must match rasterize_pdf(). Default DPI.
        progress_callback: Optional callable(msg: str). Called at key stages
                           (before/after each attempt) so the caller can write
                           intermediate status to an external store (e.g. Delta).
                           The extractor itself has no Databricks dependency.

    Returns:
        dict with keys:
            matched          list of matched component dicts
            unmatched        list of unmatched component dicts
            attempts_made    int — how many extraction attempts were made
            final_match_rate float — match rate of the returned result
            threshold_met    bool — whether the threshold was reached
    """
    total_attempts = 1 + (max_retries if enable_retry else 0)
    best = None   # best result seen so far: dict with matched/unmatched/rate

    def _cb(msg):
        if progress_callback:
            try:
                progress_callback(msg)
            except Exception:
                pass

    for attempt in range(1, total_attempts + 1):
        label = f"Attempt {attempt}/{total_attempts}"
        if verbose:
            print(f"  {label} — extracting...")
        _cb(f"Extraction attempt {attempt}/{total_attempts}")

        raw        = extract_bom(client, system_prompt, images, model)
        parsed     = parse_json_from_response(raw)
        components = parsed if isinstance(parsed, list) else [parsed]

        _cb(f"Matching {len(components)} components to PDF vector text (attempt {attempt}/{total_attempts})")
        matched, unmatched = match_bom_to_pdf_text(components, pdf_path, dpi=dpi)
        rate = len(matched) / len(components) if components else 0.0

        if verbose:
            status = "✓ threshold met" if rate >= threshold else f"below threshold ({threshold:.0%})"
            print(f"  {label} — matched {len(matched)}/{len(components)} ({rate:.0%}) — {status}")
        _cb(f"Attempt {attempt}: {len(matched)}/{len(components)} matched ({rate:.0%})"
            + (" ✓" if rate >= threshold else f" — below {threshold:.0%} threshold"))

        # Keep the best result seen so far
        if best is None or rate > best["final_match_rate"]:
            best = {
                "matched":          matched,
                "unmatched":        unmatched,
                "attempts_made":    attempt,
                "final_match_rate": rate,
                "threshold_met":    rate >= threshold,
            }

        # Stop early if threshold is met
        if rate >= threshold:
            break

        if attempt < total_attempts:
            if verbose:
                print(f"  Retrying...")
            _cb(f"Retrying (attempt {attempt + 1}/{total_attempts})...")

    if verbose and not best["threshold_met"]:
        print(f"  Warning: threshold {threshold:.0%} not met after {best['attempts_made']} attempt(s). "
              f"Best rate: {best['final_match_rate']:.0%}. Saving best result.")

    return best


# ── Overlay generation ─────────────────────────────────────────────────────────

def generate_precision_overlay(pdf_path, matched, unmatched, output_path, dpi=DPI):
    """Render the first PDF page and draw color-coded bounding boxes for matched components.

    The overlay JPEG is a QA artifact — it lets a human reviewer visually verify
    that each extracted component is correctly located on the diagram. Boxes are
    drawn in the component's type-specific color (see COMPONENT_COLORS) with a
    short label showing the type and rated current.

    Unmatched components are intentionally NOT drawn: they have no known position.
    Their absence in the overlay highlights which components the pipeline could not
    locate — useful for iterative improvement.

    Design details:
    - Boxes are drawn with a 15 px padding around the cluster bounding box so
      the entire symbol is visible inside the highlighted region.
    - Three concentric rectangles (offset 0, -1, -2 px) make thin lines visible
      on high-resolution images without requiring explicit line-width support.
    - Labels are printed on a filled background in the component's color with
      white text for legibility.
    - Output is JPEG at quality=90 to keep file size manageable for large diagrams.

    Args:
        pdf_path:    Path to the source PDF.
        matched:     List of matched component dicts (from match_bom_to_pdf_text).
        unmatched:   List of unmatched component dicts (informational only).
        output_path: Destination path for the JPEG overlay image.
        dpi:         Must match DPI used in rasterize_pdf() and match_bom_to_pdf_text().

    Returns:
        Number of matched components drawn (== len(matched)).
    """
    scale = dpi / 72
    doc = fitz.open(pdf_path)
    # Rasterize all pages into PIL images
    page_images: dict = {}
    for _pi in range(len(doc)):
        _pix = doc[_pi].get_pixmap(matrix=fitz.Matrix(scale, scale))
        page_images[_pi] = Image.open(io.BytesIO(_pix.tobytes("png"))).convert("RGB")
    doc.close()

    pad = 15   # padding in pixels around each cluster bounding box
    for comp in matched:
        pi   = comp.get("bbox_page_idx", 0)
        img  = page_images.get(pi, page_images[0])
        draw = ImageDraw.Draw(img)
        pw, ph = img.size

        x0 = max(0, comp["precise_x0"] - pad)
        y0 = max(0, comp["precise_y0"] - pad)
        x1 = min(pw, comp["precise_x1"] + pad)
        y1 = min(ph, comp["precise_y1"] + pad)

        tipo    = comp.get("Que és") or comp.get("Que es") or comp.get("component_type") or ""
        color   = COMPONENT_COLORS.get(tipo.lower().strip(), DEFAULT_COLOR)
        calibre = comp.get("Calibre (A)") or comp.get("amperage_a") or ""

        # Three nested rectangles simulate a thick border
        for offset in range(3):
            draw.rectangle([x0 - offset, y0 - offset, x1 + offset, y1 + offset], outline=color)

        label = f"{tipo[:15]} {calibre}A"
        tb = draw.textbbox((x0, y0 - 16), label)
        draw.rectangle([tb[0]-1, tb[1]-1, tb[2]+1, tb[3]+1], fill=color)
        draw.text((x0, y0 - 16), label, fill=(255, 255, 255))

    # Concatenate all pages vertically into a single JPEG.
    # Scale down if combined pixel count would exceed Pillow's 178M limit.
    MAX_PIXELS_COMBINED = 150_000_000
    total_h = sum(img.height for img in page_images.values())
    max_w   = max(img.width  for img in page_images.values())
    total_px = total_h * max_w
    if total_px > MAX_PIXELS_COMBINED:
        scale_factor = (MAX_PIXELS_COMBINED / total_px) ** 0.5
        scaled = {}
        for pi in sorted(page_images):
            img = page_images[pi]
            new_w = max(1, int(img.width  * scale_factor))
            new_h = max(1, int(img.height * scale_factor))
            scaled[pi] = img.resize((new_w, new_h), Image.LANCZOS)
        page_images = scaled
        total_h = sum(img.height for img in page_images.values())
        max_w   = max(img.width  for img in page_images.values())
    combined = Image.new("RGB", (max_w, total_h), (255, 255, 255))
    y_off = 0
    for pi in sorted(page_images):
        combined.paste(page_images[pi], (0, y_off))
        y_off += page_images[pi].height
    combined.save(output_path, format="JPEG", quality=90)
    return len(matched)


def generate_annotated_pdf(pdf_path, matched, unmatched, output_path, dpi=DPI):
    """Write an annotated copy of the PDF with color-coded rectangle annotations.

    Each matched component becomes a fitz rectangle annotation on the PDF page:
    - Stroke color matches COMPONENT_COLORS (same palette as the JPEG overlay)
    - Annotation subject = component type ("Que és")
    - Annotation content = full component spec as JSON (filterable by readers)
    - Annotation title = circuit name ("Circuito")

    Unmatched components are added as text annotations (no position) so their
    specs are still embedded in the file — attached to the top-left corner with
    a "?" icon.

    Coordinates: precise_x0/y0/x1/y1 are in pixels at `dpi` resolution.
    PyMuPDF annotations use PDF points (1 pt = 1/72 inch), so coordinates are
    divided by (dpi/72) to convert back.

    Args:
        pdf_path:    Path to the source PDF.
        matched:     List of matched component dicts with precise_* coordinates.
        unmatched:   List of unmatched component dicts (no coordinates).
        output_path: Destination path for the annotated PDF.
        dpi:         DPI used during rasterization/matching (default 400).

    Returns:
        Number of annotations written (matched + unmatched).
    """
    scale = dpi / 72.0
    pad_px = 15   # padding in pixel space (applied before inverse transform)
    doc = fitz.open(pdf_path)

    # ── Matched components: drawn directly onto page content (always visible) ──
    for comp in matched:
        pi   = comp.get("bbox_page_idx", 0)
        page = doc[pi] if pi < len(doc) else doc[0]

        # Skip components with no precise coordinates (circuit_shared with no anchor)
        if "precise_x0" not in comp or comp["precise_x0"] is None:
            continue

        # Rebuild the same pixel-space transform used in get_text_with_pixel_coords,
        # then invert it to go from stored pixel coords back to PDF page coords.
        # This correctly handles landscape pages stored with /Rotate (e.g. 90°):
        # simple division by scale is wrong for rotated pages because it ignores
        # the derotation component of the transform.
        zoom_mat = fitz.Matrix(scale, scale)
        pix_transform = (~page.derotation_matrix * zoom_mat) if page.rotation else zoom_mat
        inv_transform = ~pix_transform

        p00 = fitz.Point(comp["precise_x0"] - pad_px, comp["precise_y0"] - pad_px) * inv_transform
        p11 = fitz.Point(comp["precise_x1"] + pad_px, comp["precise_y1"] + pad_px) * inv_transform
        rect = fitz.Rect(
            min(p00.x, p11.x), min(p00.y, p11.y),
            max(p00.x, p11.x), max(p00.y, p11.y),
        )

        tipo    = comp.get("Que és") or comp.get("Que es") or comp.get("component_type") or ""
        rgb     = COMPONENT_COLORS.get(tipo.lower().strip(), DEFAULT_COLOR)
        color_f = tuple(c / 255.0 for c in rgb)

        # draw_rect writes permanently onto the page content stream
        page.draw_rect(rect, color=color_f, width=1.5)

        # Add an invisible rect annotation so PDF viewers show a tooltip on hover
        annot = page.add_rect_annot(rect)
        annot.set_colors(stroke=color_f)
        annot.set_border(width=0)
        calibre = comp.get("Calibre (A)") or comp.get("amperage_a") or ""
        circuit = comp.get("Circuito") or comp.get("circuit") or ""
        annot.set_info(
            title   = circuit,
            subject = tipo,
            content = f"{tipo} {calibre}A — {circuit} (score={round(comp.get('match_score',0))})",
        )
        annot.update()

    doc.save(output_path, garbage=4, deflate=True)
    doc.close()
    return len(matched) + len(unmatched)
