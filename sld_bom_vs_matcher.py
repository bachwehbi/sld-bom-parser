"""
sld_bom_vs_matcher.py — Semantic reference matching via Databricks Vector Search
=================================================================================

Phase 2: VS retrieval → property re-ranking
Phase 3: LLM agentic fallback for low-confidence components

Entry point: match_all_components()

Architecture
------------
For each BOM component:
  1. Normalize fields to canonical schema (via sld_bom_catalog)
  2. Build natural-language query text
  3. Query VS index (semantic similarity, returns top-K candidates)
  4. Re-rank with exact property scoring (calibre, poles, curve, etc.)
  5. If best score < AGENT_THRESHOLD → call LLM to reason & pick the best ref
  6. Enrich top-N with stock / work-order data
  7. Attach _confidence flag for the app UI

Confidence levels
-----------------
  high   : best property score ≥ 6  (multiple exact matches)
  medium : best property score 3–5   (amperage or poles matched)
  low    : best property score 0–2   (type only, or agent-resolved)
  none   : no VS candidates found
"""

import json
import logging
import time
from typing import Optional

import httpx

from sld_bom_catalog import (
    normalize_component_fields,
    build_vs_query,
    score_candidate,
    resolve_stock,
    COMPONENT_TYPE_LABEL,
    CANONICAL_TO_CATALOG,
    _TIER_PRIORITY,
)

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

# Score threshold below which the LLM agent is triggered (Phase 3)
AGENT_THRESHOLD = 3

# Columns we need back from the VS index
_VS_COLUMNS = [
    "reference",
    "component_type",
    "component_type_en",
    "product_description",
    "product_long_description",
    "range",
    "tier",
    "status",
    "superseded_by",
    "list_price_eur",
    "properties",
]


# ── VS query ──────────────────────────────────────────────────────────────────

def query_vs(
    workspace_client,
    index_name: str,
    query_text: str,
    num_results: int = 20,
    filter_json: Optional[str] = None,
) -> list[dict]:
    """
    Query a Databricks Vector Search index.
    Returns a list of row dicts with the columns requested in _VS_COLUMNS.
    """
    kwargs = dict(
        index_name=index_name,
        query_text=query_text,
        num_results=num_results,
        columns=_VS_COLUMNS,
    )
    if filter_json:
        kwargs["filters_json"] = filter_json

    try:
        resp = workspace_client.vector_search_indexes.query_index(**kwargs)
        col_names = [c.name for c in (resp.manifest.columns or [])]
        rows = resp.result.data_array or []
        return [dict(zip(col_names, row)) for row in rows]
    except Exception as e:
        logger.warning(f"VS query failed for '{query_text[:60]}…': {e}")
        return []


# ── LLM agentic fallback (Phase 3) ───────────────────────────────────────────

def _agent_resolve(
    comp_canonical: dict,
    vs_candidates: list[dict],
    workspace_host: str,
    workspace_token: str,
    model_endpoint: str = "databricks-claude-sonnet-4-6",
) -> Optional[dict]:
    """
    Call Claude to select the best reference from VS candidates when
    property scoring is insufficient (score < AGENT_THRESHOLD).

    Returns the chosen candidate dict (from vs_candidates) or None.
    """
    if not vs_candidates:
        return None

    ctype   = comp_canonical.get("component_type", "unknown")
    label   = COMPONENT_TYPE_LABEL.get(ctype, ctype)
    amp     = comp_canonical.get("amperage_a")
    poles   = comp_canonical.get("poles")
    curve   = comp_canonical.get("curve")
    bka     = comp_canonical.get("breaking_ka")
    sens    = comp_canonical.get("sensitivity_ma")
    rtype   = comp_canonical.get("rcd_type")
    panel   = comp_canonical.get("panel", "")
    circuit = comp_canonical.get("circuit", "")

    # Build component description
    comp_desc_parts = [f"Type: {label}"]
    if amp   is not None: comp_desc_parts.append(f"Rated current: {amp}A")
    if poles is not None: comp_desc_parts.append(f"Poles: {poles}")
    if curve:             comp_desc_parts.append(f"Trip curve: {curve}")
    if bka   is not None: comp_desc_parts.append(f"Breaking capacity: {bka}kA")
    if sens  is not None: comp_desc_parts.append(f"Sensitivity: {sens}mA")
    if rtype:             comp_desc_parts.append(f"RCD type: {rtype}")
    if panel:             comp_desc_parts.append(f"Panel: {panel}")
    if circuit:           comp_desc_parts.append(f"Circuit: {circuit}")
    comp_desc = "\n".join(f"  - {p}" for p in comp_desc_parts)

    # Build candidates list
    cand_lines = []
    for i, c in enumerate(vs_candidates[:8], 1):
        try:
            props = json.loads(c.get("properties") or "{}")
        except Exception:
            props = {}
        sc = score_candidate(comp_canonical, props)
        price = c.get("list_price_eur")
        price_str = f"€{price:.2f}" if price is not None else "N/A"
        cand_lines.append(
            f"{i}. {c.get('reference')} — {c.get('product_description')} "
            f"({c.get('tier', '?')}, {price_str}, property score {sc})"
        )
        if c.get("product_long_description"):
            cand_lines.append(f"   {c['product_long_description'][:120]}")
    cands_text = "\n".join(cand_lines)

    system = (
        "You are an expert in Schneider Electric electrical products. "
        "Select the single best catalog reference for the electrical component described. "
        "Respond ONLY with a JSON object — no markdown, no explanation."
    )
    user = f"""Component to match:
{comp_desc}

Catalog candidates (from semantic search):
{cands_text}

Choose the best reference. Consider: technical spec match, product tier appropriateness,
whether it's ACTIVE (not discontinued), and product family fit.

Respond with exactly:
{{"reference": "<ref>", "confidence": "high|medium|low", "reasoning": "<one sentence>"}}"""

    url = f"{workspace_host.rstrip('/')}/serving-endpoints/{model_endpoint}/invocations"
    headers = {"Authorization": f"Bearer {workspace_token}", "Content-Type": "application/json"}
    payload = {
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        "max_tokens": 256,
        "temperature": 0.0,
    }

    try:
        resp = httpx.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown fences if any
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        parsed = json.loads(content)
        chosen_ref = parsed.get("reference")
        reasoning  = parsed.get("reasoning", "")
        confidence = parsed.get("confidence", "low")

        # Find the chosen candidate in the list
        chosen = next((c for c in vs_candidates if c.get("reference") == chosen_ref), None)
        if chosen:
            logger.info(f"Agent resolved → {chosen_ref} ({confidence}): {reasoning}")
            chosen["_agent_resolved"]  = True
            chosen["_agent_reasoning"] = reasoning
            chosen["_confidence"]      = confidence
            return chosen

    except Exception as e:
        logger.warning(f"Agent fallback failed: {e}")

    return None


# ── Match one component ───────────────────────────────────────────────────────

def match_component(
    comp: dict,
    vs_index_name: str,
    workspace_client,
    stock_map: dict,
    wo_map: dict,
    top_n: int = 3,
    agent_threshold: int = AGENT_THRESHOLD,
    workspace_host: Optional[str] = None,
    workspace_token: Optional[str] = None,
    model_endpoint: str = "databricks-claude-sonnet-4-6",
) -> dict:
    """
    Match a single component using VS + property re-ranking + optional LLM fallback.
    Returns the original comp dict extended with 'references' and '_confidence'.
    """
    c = normalize_component_fields(comp)

    # Build VS query
    query_text = build_vs_query(c)

    # Optional filter: restrict to same component_type_en for precision
    ctype_en = c.get("component_type", "")
    filter_json = json.dumps({"component_type_en": [ctype_en]}) if ctype_en else None

    vs_results = query_vs(
        workspace_client, vs_index_name, query_text,
        num_results=20, filter_json=filter_json,
    )

    # If filtered results are sparse, fall back to unfiltered
    if len(vs_results) < 3 and filter_json:
        vs_results = query_vs(workspace_client, vs_index_name, query_text, num_results=20)

    # Score each VS candidate by exact property match
    scored = []
    for cand in vs_results:
        if cand.get("status") == "DISCONTINUED":
            continue
        try:
            props = json.loads(cand.get("properties") or "{}")
        except Exception:
            props = {}
        sc = score_candidate(c, props)
        scored.append((sc, cand))

    scored.sort(key=lambda x: (-x[0], _TIER_PRIORITY.get(x[1].get("tier", ""), 9)))

    best_score = scored[0][0] if scored else -1

    # Phase 3: LLM fallback for low-confidence
    agent_resolved = False
    if best_score < agent_threshold and workspace_host and workspace_token:
        all_cands = [cand for _, cand in scored] + [
            c for c in vs_results if c not in [cand for _, cand in scored]
        ]
        chosen = _agent_resolve(
            c, all_cands, workspace_host, workspace_token, model_endpoint
        )
        if chosen:
            # Re-score and put agent choice first
            try:
                props = json.loads(chosen.get("properties") or "{}")
            except Exception:
                props = {}
            agent_score = score_candidate(c, props)
            # Remove from scored list if present, re-insert at top
            scored = [(sc, cand) for sc, cand in scored
                      if cand.get("reference") != chosen.get("reference")]
            scored.insert(0, (agent_score, chosen))
            agent_resolved = True

    # Determine overall confidence
    bs = scored[0][0] if scored else -1
    if agent_resolved:
        overall_confidence = scored[0][1].get("_confidence", "low")
    elif bs >= 6:
        overall_confidence = "high"
    elif bs >= 3:
        overall_confidence = "medium"
    elif bs >= 0:
        overall_confidence = "low"
    else:
        overall_confidence = "none"

    # Build references list (top_n)
    references = []
    for sc, cand in scored[:top_n]:
        ref  = cand["reference"]
        info = resolve_stock(ref, stock_map, wo_map)
        entry = {
            "reference":               ref,
            "product_description":     cand.get("product_description"),
            "product_long_description": cand.get("product_long_description"),
            "range":                   cand.get("range"),
            "tier":                    cand.get("tier"),
            "status":                  cand.get("status"),
            "superseded_by":           cand.get("superseded_by"),
            "list_price_eur":          _safe_float(cand.get("list_price_eur")),
            "score":                   sc,
            "confidence":              overall_confidence,
            **info,
        }
        if cand.get("_agent_resolved"):
            entry["agent_resolved"] = True
            entry["agent_reasoning"] = cand.get("_agent_reasoning", "")
        references.append(entry)

    return {
        **comp,
        "_canonical":   c,
        "_vs_query":    query_text,
        "_confidence":  overall_confidence,
        "references":   references,
    }


def _safe_float(val):
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


# ── Match all components ──────────────────────────────────────────────────────

def match_all_components(
    components: list[dict],
    vs_index_name: str,
    workspace_client,
    stock_rows: list[dict],
    wo_rows: list[dict],
    top_n: int = 3,
    agent_threshold: int = AGENT_THRESHOLD,
    workspace_host: Optional[str] = None,
    workspace_token: Optional[str] = None,
    model_endpoint: str = "databricks-claude-sonnet-4-6",
) -> list[dict]:
    """
    Match all BOM components using VS semantic retrieval.

    Parameters
    ----------
    components       : list of component dicts (legacy Spanish or canonical English)
    vs_index_name    : fully-qualified VS index name
    workspace_client : Databricks WorkspaceClient
    stock_rows       : rows from stock Delta table
    wo_rows          : rows from work_orders Delta table
    top_n            : number of reference candidates to return per component
    agent_threshold  : property score below which LLM fallback is triggered
    workspace_host   : Databricks workspace URL (needed for LLM fallback)
    workspace_token  : Databricks PAT or OAuth token (needed for LLM fallback)
    model_endpoint   : Foundation Model endpoint name for LLM fallback

    Returns
    -------
    List of enriched component dicts, each with:
      references     : list of top_n reference dicts with stock info
      _confidence    : "high" | "medium" | "low" | "none"
      _vs_query      : the query string sent to VS (for debugging)
      _canonical     : the normalised canonical component dict
    """
    # Build stock + WO maps once (O(n) per table, O(1) per component lookup)
    stock_map: dict[str, dict[str, int]] = {}
    for s in stock_rows:
        ref = s["reference"]
        dc  = s["distribution_center"]
        stock_map.setdefault(ref, {})[dc] = int(s.get("qty_available", 0))

    wo_map: dict[str, list[dict]] = {}
    for w in wo_rows:
        ref = w["reference"]
        wo_map.setdefault(ref, []).append({
            "distribution_center": w["distribution_center"],
            "qty_incoming":        int(w.get("qty_incoming", 0)),
            "expected_date":       w.get("expected_date"),
        })

    enriched = []
    high = medium = low = none_cnt = agent_cnt = 0

    for i, comp in enumerate(components):
        result = match_component(
            comp, vs_index_name, workspace_client,
            stock_map, wo_map, top_n, agent_threshold,
            workspace_host, workspace_token, model_endpoint,
        )
        enriched.append(result)

        conf = result.get("_confidence", "none")
        if conf == "high":   high += 1
        elif conf == "medium": medium += 1
        elif conf == "low":  low += 1
        else:                none_cnt += 1

        if any(r.get("agent_resolved") for r in result.get("references", [])):
            agent_cnt += 1

        if (i + 1) % 20 == 0:
            logger.info(f"  Matched {i+1}/{len(components)} components…")

    logger.info(
        f"VS matching complete: {len(components)} components | "
        f"high={high} medium={medium} low={low} none={none_cnt} "
        f"agent_resolved={agent_cnt}"
    )
    return enriched
