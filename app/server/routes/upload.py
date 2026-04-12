import logging

import httpx
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel

from server.config import (
    get_config, get_workspace_client,
    set_run_id, VOLUME_PATH, EXTRACTION_JOB_ID,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/api/upload")
async def upload_pdf(file: UploadFile = File(...)):
    """Upload a PDF to the UC volume via Files API."""
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted")

    w    = get_workspace_client()
    host = w.config.host.rstrip("/")
    hdrs = w.config.authenticate()

    dest_path = f"{VOLUME_PATH}/{file.filename}"
    url       = f"{host}/api/2.0/fs/files{dest_path}?overwrite=true"

    content = await file.read()

    try:
        with httpx.Client(timeout=120) as client:
            resp = client.put(
                url,
                headers={**hdrs, "Content-Type": "application/octet-stream"},
                content=content,
            )
            resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.exception("Upload failed")
        raise HTTPException(500, f"Upload failed: {e.response.text[:200]}")

    return {"file_name": file.filename, "path": dest_path}


class ExtractRequest(BaseModel):
    file_name: str
    model: str = "databricks-claude-sonnet-4-6"
    enable_retry: bool = True
    max_retries: int = 2
    threshold: float = 0.75


@router.post("/api/extract")
def trigger_extraction(req: ExtractRequest):
    """Submit an extraction job run for a specific file."""
    w    = get_workspace_client()
    host = w.config.host.rstrip("/")
    hdrs = {**w.config.authenticate(), "Content-Type": "application/json"}
    url  = f"{host}/api/2.1/jobs/run-now"

    payload = {
        "job_id": EXTRACTION_JOB_ID,
        "notebook_params": {
            "file_name":     req.file_name,
            "model":         req.model,
            "enable_retry":  str(req.enable_retry).lower(),
            "max_retries":   str(req.max_retries),
            "threshold":     str(req.threshold),
        },
    }

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(url, headers=hdrs, json=payload)
            resp.raise_for_status()
            run_id = resp.json()["run_id"]
    except httpx.HTTPStatusError as e:
        logger.exception("Job trigger failed")
        raise HTTPException(500, f"Job trigger failed: {e.response.text[:200]}")

    set_run_id(req.file_name, run_id)
    return {"run_id": run_id, "file_name": req.file_name, "status": "submitted"}
