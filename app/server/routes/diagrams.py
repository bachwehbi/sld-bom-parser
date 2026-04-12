import json
import logging
import os
import time

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from server.config import (
    get_config, get_workspace_client, exec_sql,
    get_run_id, clear_run_id,
    VOLUME_PATH, OVERLAY_PATH, TABLE_NAME,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _coerce_bool(val) -> bool | None:
    """Coerce SQL boolean values (may arrive as string 'true'/'false') to Python bool."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() == "true"
    return bool(val)


def _parse_bom(row: dict) -> dict:
    """Enrich a raw bom_extractions row with computed fields."""
    bom_json = row.get("bom_json")
    components = []
    if bom_json:
        try:
            components = json.loads(bom_json) if isinstance(bom_json, str) else bom_json
        except Exception:
            components = []

    matched   = [c for c in components if c.get("precise_cx") is not None]
    total     = len(components)
    match_pct = round(len(matched) / total * 100) if total > 0 else 0

    return {
        "file_name":      row.get("file_name"),
        "file_path":      row.get("file_path"),
        "status":         row.get("status"),
        "progress_msg":   row.get("progress_msg"),
        "processed_at":   row.get("processed_at"),
        "attempts_made":  row.get("attempts_made"),
        "threshold_met":  _coerce_bool(row.get("threshold_met")),
        "error_message":  row.get("error_message"),
        "pdf_type":       row.get("pdf_type"),
        "component_count": total,
        "matched_count":   len(matched),
        "match_pct":       match_pct,
        "components":      components,
    }


@router.get("/api/diagrams")
def get_diagrams():
    try:
        rows = exec_sql(
            f"SELECT file_name, file_path, status, progress_msg, processed_at, "
            f"attempts_made, threshold_met, error_message, bom_json, pdf_type "
            f"FROM {TABLE_NAME} ORDER BY processed_at DESC NULLS LAST"
        )
        return [_parse_bom(r) for r in rows]
    except Exception as e:
        logger.exception("get_diagrams failed")
        raise HTTPException(500, str(e))


@router.get("/api/diagrams/{file_name}")
def get_diagram(file_name: str):
    try:
        rows = exec_sql(
            f"SELECT file_name, file_path, status, progress_msg, processed_at, "
            f"attempts_made, threshold_met, error_message, bom_json, pdf_type "
            f"FROM {TABLE_NAME} WHERE file_name = '{file_name.replace(chr(39), '')}'"
        )
        if not rows:
            raise HTTPException(404, f"{file_name} not found")
        return _parse_bom(rows[0])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_diagram failed")
        raise HTTPException(500, str(e))


@router.get("/api/unprocessed")
def get_unprocessed():
    """PDFs in the volume that have no entry in bom_extractions."""
    try:
        # Use SQL LIST to enumerate volume contents
        list_rows = exec_sql(f"LIST '{VOLUME_PATH}'")
        all_pdfs  = [r["name"] for r in list_rows if r.get("name", "").lower().endswith(".pdf")]

        # Get already-processed file names
        processed_rows = exec_sql(f"SELECT DISTINCT file_name FROM {TABLE_NAME}")
        processed      = {r["file_name"] for r in processed_rows}

        unprocessed = [fn for fn in all_pdfs if fn not in processed]
        return {"files": unprocessed}
    except Exception as e:
        logger.exception("get_unprocessed failed")
        raise HTTPException(500, str(e))


@router.get("/api/progress/{file_name}")
def get_progress(file_name: str):
    """Return live status for a file. Checks Jobs API first, then Delta."""
    w      = get_workspace_client()
    config = get_config()
    run_id = get_run_id(file_name)

    job_state    = None
    job_result   = None

    if run_id:
        try:
            host  = w.config.host.rstrip("/")
            hdrs  = {**w.config.authenticate()}
            resp  = httpx.get(
                f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
                headers=hdrs,
                timeout=10,
            )
            if resp.status_code == 200:
                run    = resp.json()
                state  = run.get("state", {})
                job_state  = state.get("life_cycle_state")
                job_result = state.get("result_state")
                if job_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                    clear_run_id(file_name)
        except Exception:
            pass

    # Always read latest progress_msg from Delta
    try:
        rows = exec_sql(
            f"SELECT status, progress_msg, processed_at, threshold_met, error_message "
            f"FROM {TABLE_NAME} WHERE file_name = '{file_name.replace(chr(39), '')}'"
        )
        if rows:
            row = rows[0]
            return {
                "status":        row.get("status"),
                "progress_msg":  row.get("progress_msg"),
                "processed_at":  row.get("processed_at"),
                "threshold_met": _coerce_bool(row.get("threshold_met")),
                "error_message": row.get("error_message"),
                "job_state":     job_state,
                "job_result":    job_result,
            }
    except Exception:
        pass

    # No Delta row yet (job just submitted)
    return {
        "status":       "IN_PROGRESS",
        "progress_msg": "Job submitted, waiting to start…",
        "processed_at": None,
        "threshold_met": None,
        "error_message": None,
        "job_state":    job_state,
        "job_result":   None,
    }


@router.get("/api/overlay/{file_name}")
def get_overlay(file_name: str):
    """Stream overlay JPEG from UC volume via Files API."""
    w    = get_workspace_client()
    stem = os.path.splitext(file_name)[0]
    path = f"{OVERLAY_PATH}/{stem}_overlay.jpg"

    host = w.config.host.rstrip("/")
    hdrs = w.config.authenticate()
    url  = f"{host}/api/2.0/fs/files{path}"

    def stream():
        with httpx.stream("GET", url, headers=hdrs, timeout=30) as resp:
            if resp.status_code == 404:
                raise HTTPException(404, f"No overlay for {file_name}")
            resp.raise_for_status()
            for chunk in resp.iter_bytes(chunk_size=65536):
                yield chunk

    return StreamingResponse(stream(), media_type="image/jpeg")


@router.get("/api/annotated/{file_name}")
def get_annotated(file_name: str):
    """Stream annotated PDF from UC volume via Files API."""
    w    = get_workspace_client()
    stem = os.path.splitext(file_name)[0]
    path = f"{OVERLAY_PATH}/{stem}_annotated.pdf"

    host = w.config.host.rstrip("/")
    hdrs = w.config.authenticate()
    url  = f"{host}/api/2.0/fs/files{path}"

    safe_name = f"{stem}_annotated.pdf"

    def stream():
        with httpx.stream("GET", url, headers=hdrs, timeout=30) as resp:
            if resp.status_code == 404:
                raise HTTPException(404, f"No annotated PDF for {file_name}")
            resp.raise_for_status()
            for chunk in resp.iter_bytes(chunk_size=65536):
                yield chunk

    return StreamingResponse(
        stream(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{safe_name}"'},
    )
