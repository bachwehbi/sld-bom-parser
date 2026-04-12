"""
sld_bom_catalog.py — Reference matching module
===============================================

Matches extracted BOM components against the Schneider Electric product catalog.

Supports TWO input schemas transparently:
  - Legacy (Spanish): {"Que és": "Interruptor automático", "Calibre (A)": "25", ...}
  - Canonical (English): {"component_type": "circuit_breaker", "amperage_a": 25, ...}

Both are normalised to the canonical schema at the start of match_references().

Matching algorithm
------------------
1. Normalise component fields to canonical English
2. Resolve catalog component_type (canonical → catalog Spanish)
3. Score each catalog candidate on technical property overlap
4. Keep top_n by score (ties → tier priority: standard > economy > premium)
5. Enrich with stock / work-order data

Public API
----------
  normalize_component_fields(comp)  → canonical dict
  build_vs_query(comp)              → search string for Vector Search
  match_references(components, catalog, stock_rows, work_orders, top_n)
"""

import json
import unicodedata


# ── Canonical component type maps ─────────────────────────────────────────────

# Maps ANY representation → canonical English enum
COMPONENT_TYPE_EN = {
    # canonical pass-through
    "circuit_breaker":             "circuit_breaker",
    "rcd":                         "rcd",
    "contactor":                   "contactor",
    "timer":                       "timer",
    "surge_protector":             "surge_protector",
    "fuse_holder":                 "fuse_holder",
    "energy_meter":                "energy_meter",
    "ats":                         "ats",
    "load_break_switch":           "load_break_switch",
    # Spanish (with and without accents)
    "interruptor automatico":      "circuit_breaker",
    "interruptor automático":      "circuit_breaker",
    "interruptor diferencial":     "rcd",
    "contactor":                   "contactor",
    "reloj":                       "timer",
    "limitador de sobretension":   "surge_protector",
    "limitador de sobretensión":   "surge_protector",
    "portafusibles":               "fuse_holder",
    "contador de energia":         "energy_meter",
    "contador de energía":         "energy_meter",
    "inversor y conmutador de redes": "ats",
    "interruptor de corte en carga":  "load_break_switch",
    # French
    "disjoncteur":                 "circuit_breaker",
    "différentiel":                "rcd",
    "differentiel":                "rcd",
    "contacteur":                  "contactor",
    "horloge":                     "timer",
    "parafoudre":                  "surge_protector",
    "sectionneur":                 "load_break_switch",
    "inverseur de sources":        "ats",
    "compteur d'energie":          "energy_meter",
    "compteur d energie":          "energy_meter",
    # German
    "leitungsschutzschalter":      "circuit_breaker",
    "fi-schutzschalter":           "rcd",
    "schütz":                      "contactor",
    "schutz":                      "contactor",
    "zeitschalter":                "timer",
    "überspannungsschutz":         "surge_protector",
    "uberspannungsschutz":         "surge_protector",
    "lasttrennschalter":           "load_break_switch",
    "energiezähler":               "energy_meter",
    "energiezahler":               "energy_meter",
}

# Maps canonical English → material table's component_type (Spanish, no accents)
CANONICAL_TO_CATALOG = {
    "circuit_breaker":   "interruptor automatico",
    "rcd":               "interruptor diferencial",
    "contactor":         "contactor",
    "timer":             "reloj",
    "surge_protector":   "limitador de sobretension",
    "fuse_holder":       "portafusibles",
    "energy_meter":      "contador de energia",
    "ats":               "inversor y conmutador de redes",
    "load_break_switch": "interruptor de corte en carga",
}

# Human-readable labels for VS search text + agent prompts
COMPONENT_TYPE_LABEL = {
    "circuit_breaker":   "circuit breaker (MCB/MCCB)",
    "rcd":               "residual current device (RCD/RCCB)",
    "contactor":         "contactor",
    "timer":             "time switch / timer relay",
    "surge_protector":   "surge protection device (SPD)",
    "fuse_holder":       "fuse holder",
    "energy_meter":      "energy meter",
    "ats":               "automatic transfer switch (ATS/TransferPacT)",
    "load_break_switch": "load break switch / disconnector",
}

# Old Spanish field name → canonical English field name
_OLD_FIELD_MAP = {
    "Que és":                              "component_type",
    "Que es":                              "component_type",
    "Calibre (A)":                         "amperage_a",
    "Curva":                               "curve",
    "Poder de Corte (kA)":                 "breaking_ka",
    "Polos":                               "poles",
    "Tensión (V)":                         "voltage_v",
    "Sensibilidad (mA)":                   "sensitivity_ma",
    "Tipo (Diferencial)":                  "rcd_type",
    "Tipo de Sensibilidad (Diferencial)":  "rcd_sensitivity_class",
    "Tipo de Selectividad (Diferencial)":  "rcd_selectivity",
    "Bloque (Diferencial)":                "rcd_block_type",
    "Función (Reloj)":                     "timer_function",
    "I max (Limitador)":                   "max_current_ka",
    "Cuadro":                              "panel",
    "Circuito":                            "circuit",
}

# Old Spanish value → canonical English for component_type only
_OLD_RCD_SELECTIVITY = {
    "selectivo":      "selective",
    "instantáneo":    "instantaneous",
    "instantaneo":    "instantaneous",
}
_OLD_RCD_SENSITIVITY_CLASS = {
    "estándar":           "standard",
    "estandar":           "standard",
    "super immunizado":   "super_immunized",
    "super immunizado":   "super_immunized",
}
_OLD_RCD_BLOCK_TYPE = {
    "solo diferencial": "standalone",
    "vigi":             "vigi_block",
}
_OLD_TIMER_FUNCTION = {
    "astro":    "astro",
    "horario":  "hourly",
}


# ── Field normalisation ───────────────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _to_float(val):
    if val is None:
        return None
    try:
        return float(str(val).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def _to_int(val):
    f = _to_float(val)
    return int(f) if f is not None else None


def normalize_component_fields(comp: dict) -> dict:
    """
    Translate a component dict to canonical English schema.

    Accepts BOTH:
      - Legacy Spanish:   {"Que és": "Interruptor automático", "Calibre (A)": "25", ...}
      - Canonical:        {"component_type": "circuit_breaker", "amperage_a": 25, ...}

    Returns a new dict with canonical English keys and properly typed values.
    Non-schema keys (precise_cx, match_score, etc.) are passed through unchanged.
    """
    # Already canonical if it has "component_type"
    if "component_type" in comp:
        out = dict(comp)
        # Still normalise the type string itself (might be Spanish)
        raw = (out.get("component_type") or "").strip().lower()
        raw_stripped = _strip_accents(raw)
        out["component_type"] = (
            COMPONENT_TYPE_EN.get(raw)
            or COMPONENT_TYPE_EN.get(raw_stripped)
            or raw
        )
        # Ensure numeric fields are numbers
        for f in ("amperage_a", "poles", "breaking_ka", "sensitivity_ma",
                  "max_current_ka", "voltage_v"):
            out[f] = _to_float(out.get(f))
        out["poles"] = _to_int(out.get("poles"))
        return out

    # Legacy Spanish schema → translate field by field
    out = {}
    for old_key, new_key in _OLD_FIELD_MAP.items():
        if old_key in comp:
            out[new_key] = comp[old_key]

    # Carry through non-schema keys (precise_cx, match_score, bbox_*, etc.)
    schema_old_keys = set(_OLD_FIELD_MAP.keys())
    for k, v in comp.items():
        if k not in schema_old_keys and k not in out:
            out[k] = v

    # Normalise component_type
    raw_type = (out.get("component_type") or "").strip().lower()
    raw_stripped = _strip_accents(raw_type)
    out["component_type"] = (
        COMPONENT_TYPE_EN.get(raw_type)
        or COMPONENT_TYPE_EN.get(raw_stripped)
        or raw_type
    )

    # Numeric coercions
    out["amperage_a"]    = _to_float(out.get("amperage_a"))
    out["poles"]         = _to_int(out.get("poles"))
    out["breaking_ka"]   = _to_float(out.get("breaking_ka"))
    out["sensitivity_ma"] = _to_float(out.get("sensitivity_ma"))
    out["max_current_ka"] = _to_float(out.get("max_current_ka"))
    out["voltage_v"]     = _to_float(out.get("voltage_v"))

    # Normalise enumerated string values
    sel = (out.get("rcd_selectivity") or "").strip().lower()
    out["rcd_selectivity"] = _OLD_RCD_SELECTIVITY.get(sel, sel or None)

    sens = (out.get("rcd_sensitivity_class") or "").strip().lower()
    out["rcd_sensitivity_class"] = _OLD_RCD_SENSITIVITY_CLASS.get(sens, sens or None)

    blk = (out.get("rcd_block_type") or "").strip().lower()
    out["rcd_block_type"] = _OLD_RCD_BLOCK_TYPE.get(blk, blk or None)

    tmr = (out.get("timer_function") or "").strip().lower()
    out["timer_function"] = _OLD_TIMER_FUNCTION.get(tmr, tmr or None)

    return out


# ── Vector Search query text ──────────────────────────────────────────────────

def build_vs_query(comp: dict) -> str:
    """
    Build a natural-language search string for Vector Search from a canonical component.

    Example: "circuit breaker (MCB/MCCB) 4-pole 25A C-curve 10kA breaking capacity"
    """
    c = normalize_component_fields(comp)
    ctype = c.get("component_type", "")
    label = COMPONENT_TYPE_LABEL.get(ctype, ctype.replace("_", " "))

    parts = [label]
    if c.get("poles"):
        parts.append(f"{c['poles']}-pole")
    if c.get("amperage_a") is not None:
        parts.append(f"{int(c['amperage_a'])}A")
    if c.get("curve"):
        parts.append(f"{c['curve']}-curve")
    if c.get("breaking_ka") is not None:
        parts.append(f"{c['breaking_ka']}kA breaking capacity")
    if c.get("sensitivity_ma") is not None:
        parts.append(f"{int(c['sensitivity_ma'])}mA")
    if c.get("rcd_type"):
        parts.append(f"type {c['rcd_type']}")
    if c.get("rcd_selectivity"):
        parts.append(c["rcd_selectivity"])
    if c.get("timer_function"):
        parts.append(f"{c['timer_function']} function")
    if c.get("max_current_ka") is not None:
        parts.append(f"{c['max_current_ka']}kA max discharge")

    return " ".join(parts)


# ── Scoring ───────────────────────────────────────────────────────────────────

_TIER_PRIORITY = {"standard": 0, "economy": 1, "premium": 2}


def score_candidate(comp: dict, props: dict) -> int:
    """
    Score a catalog candidate against a (canonical) component.
    comp must already be normalised via normalize_component_fields().
    props is the parsed JSON from material.properties.
    """
    score = 0

    # Amperage
    if comp.get("amperage_a") is not None and _to_float(props.get("calibre_A")) is not None:
        if comp["amperage_a"] == _to_float(props["calibre_A"]):
            score += 3

    # Poles
    if comp.get("poles") is not None and _to_int(props.get("poles")) is not None:
        if comp["poles"] == _to_int(props["poles"]):
            score += 3

    # Curve
    comp_curve = (comp.get("curve") or "").strip().upper()
    cand_curve = (props.get("curve") or "").strip().upper()
    if comp_curve and cand_curve and comp_curve == cand_curve:
        score += 2

    # Breaking capacity
    if comp.get("breaking_ka") is not None and _to_float(props.get("breaking_kA")) is not None:
        if comp["breaking_ka"] == _to_float(props["breaking_kA"]):
            score += 2

    # Sensitivity
    if comp.get("sensitivity_ma") is not None and _to_float(props.get("sensitivity_mA")) is not None:
        if comp["sensitivity_ma"] == _to_float(props["sensitivity_mA"]):
            score += 2

    # RCD type
    comp_rtype = (comp.get("rcd_type") or "").strip().upper()
    cand_rtype = (props.get("type") or "").strip().upper()
    if comp_rtype and cand_rtype and comp_rtype == cand_rtype:
        score += 1

    return score


# ── Stock resolution ──────────────────────────────────────────────────────────

_DC_PRIORITY = ["MADRID", "BARCELONA", "VALENCIA", "SEVILLA", "BILBAO"]


def resolve_stock(reference: str, stock_map: dict, wo_map: dict) -> dict:
    dc_stock = stock_map.get(reference, {})

    for dc in _DC_PRIORITY:
        if dc_stock.get(dc, 0) >= 10:
            return {"stock_status": "IN_STOCK", "qty_available": dc_stock[dc],
                    "distribution_center": dc, "expected_date": None}

    for dc in _DC_PRIORITY:
        qty = dc_stock.get(dc, 0)
        if 0 < qty < 10:
            eta = _earliest_eta(reference, dc, wo_map)
            return {"stock_status": "LOW_STOCK", "qty_available": qty,
                    "distribution_center": dc, "expected_date": eta}

    eta = _earliest_eta_any(reference, wo_map)
    return {"stock_status": "OUT_OF_STOCK", "qty_available": 0,
            "distribution_center": None, "expected_date": eta}


def _earliest_eta(reference, dc, wo_map):
    orders = [w for w in wo_map.get(reference, []) if w["distribution_center"] == dc]
    return min(orders, key=lambda w: w["expected_date"])["expected_date"] if orders else None


def _earliest_eta_any(reference, wo_map):
    orders = wo_map.get(reference, [])
    return min(orders, key=lambda w: w["expected_date"])["expected_date"] if orders else None


# ── Main entry point (catalog-based, backward compat) ─────────────────────────

def match_references(
    components:  list[dict],
    catalog:     list[dict],
    stock_rows:  list[dict],
    work_orders: list[dict],
    top_n:       int = 3,
) -> list[dict]:
    """
    Enrich BOM components with product reference suggestions.

    Works with BOTH legacy Spanish and canonical English component schemas.
    Catalog must come from the material Delta table (uses Spanish component_type).
    """
    # Build catalog lookup by (Spanish) component_type
    catalog_by_type: dict[str, list[dict]] = {}
    for row in catalog:
        t = row.get("component_type", "")
        catalog_by_type.setdefault(t, []).append(row)

    # Stock map: { reference: { dc: qty } }
    stock_map: dict[str, dict[str, int]] = {}
    for s in stock_rows:
        ref = s["reference"]
        dc  = s["distribution_center"]
        stock_map.setdefault(ref, {})[dc] = int(s.get("qty_available", 0))

    # Work order map: { reference: [{dc, qty_incoming, expected_date}] }
    wo_map: dict[str, list[dict]] = {}
    for w in work_orders:
        ref = w["reference"]
        wo_map.setdefault(ref, []).append({
            "distribution_center": w["distribution_center"],
            "qty_incoming":        int(w.get("qty_incoming", 0)),
            "expected_date":       w.get("expected_date"),
        })

    enriched = []
    for comp in components:
        # Normalise to canonical schema (handles both old Spanish and new English)
        c = normalize_component_fields(comp)

        # Map canonical type → catalog (Spanish) type
        catalog_type = CANONICAL_TO_CATALOG.get(c.get("component_type", ""), "")

        candidates = catalog_by_type.get(catalog_type, [])

        scored = []
        for cand in candidates:
            if cand.get("status") == "DISCONTINUED":
                continue
            try:
                props = json.loads(cand.get("properties") or "{}")
            except (json.JSONDecodeError, TypeError):
                props = {}
            s = score_candidate(c, props)
            scored.append((s, cand))

        scored.sort(key=lambda x: (-x[0], _TIER_PRIORITY.get(x[1].get("tier", ""), 9)))

        references = []
        for sc, cand in scored[:top_n]:
            ref  = cand["reference"]
            info = resolve_stock(ref, stock_map, wo_map)
            references.append({
                "reference":               ref,
                "product_description":     cand.get("product_description"),
                "product_long_description": cand.get("product_long_description"),
                "range":                   cand.get("range"),
                "tier":                    cand.get("tier"),
                "status":                  cand.get("status"),
                "superseded_by":           cand.get("superseded_by"),
                "list_price_eur":          cand.get("list_price_eur"),
                "score":                   sc,
                **info,
            })

        # Return original comp dict extended with references + canonical fields
        enriched.append({**comp, "_canonical": c, "references": references})

    return enriched


# ── Kept for backward compatibility ───────────────────────────────────────────

# Legacy private aliases used by old notebooks
_normalize_type    = lambda val: CANONICAL_TO_CATALOG.get(
    COMPONENT_TYPE_EN.get((val or "").strip().lower(), ""),
    (val or "").strip().lower()
)
_score_candidate   = score_candidate
_resolve_stock     = resolve_stock
_TIER_PRIORITY     = _TIER_PRIORITY
