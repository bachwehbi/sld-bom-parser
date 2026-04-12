import re
import json
import logging

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from server.config import get_config, get_workspace_client, exec_sql

logger = logging.getLogger(__name__)
router = APIRouter()


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    active_file: str | None = None


def _extract_file_refs(text: str, known_files: set[str]) -> list[dict]:
    """Scan agent response text for *.pdf names that match known extractions."""
    found = re.findall(r'\b[\w\-\.]+\.pdf\b', text, re.IGNORECASE)
    refs  = []
    seen  = set()
    for fn in found:
        if fn in known_files and fn not in seen:
            refs.append({"file_name": fn})
            seen.add(fn)
    return refs


def _get_known_files() -> set[str]:
    try:
        rows = exec_sql("SELECT DISTINCT file_name FROM serverless_stable_bach_catalog.bom_parser.bom_extractions")
        return {r["file_name"] for r in rows}
    except Exception:
        return set()


@router.post("/api/chat")
async def chat(request: ChatRequest):
    config = get_config()
    w      = get_workspace_client()

    headers = {**w.config.authenticate(), "Content-Type": "application/json"}
    url     = f"{w.config.host.rstrip('/')}/serving-endpoints/{config.agent_endpoint_name}/invocations"

    history = [{"role": m.role, "content": m.content} for m in request.messages]

    # Inject active diagram context as a system message at position 0
    if request.active_file:
        context = (
            f"The user currently has the diagram '{request.active_file}' open in the viewer. "
            f"When they ask questions without specifying a diagram, assume they are referring to this diagram."
        )
        history = [{"role": "system", "content": context}] + history

    payload = {"input": history}

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        # ResponsesAgent format: {"output": [{"type": "message", "role": "assistant", "content": "..."}]}
        # Content may be a string or a list of content blocks.
        output_items = [item for item in data.get("output", []) if item.get("type") == "message"]
        if output_items:
            raw_content = output_items[-1].get("content", "")
            if isinstance(raw_content, list):
                # content is a list of blocks: [{"type": "output_text", "text": "..."}]
                content = " ".join(
                    block.get("text", "") for block in raw_content
                    if block.get("type") == "output_text"
                )
            else:
                content = raw_content or ""
        else:
            # Fallback: old ChatModel format still supported during rollover
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

        known_files = _get_known_files()
        file_refs   = _extract_file_refs(content, known_files)

        return {"content": content, "file_refs": file_refs}

    except httpx.HTTPStatusError as e:
        logger.exception("Agent endpoint error: %s", e.response.text)
        return {"content": f"Agent error: {e.response.status_code} — {e.response.text[:200]}", "file_refs": []}
    except Exception as e:
        logger.exception("Chat error")
        return {"content": f"Error: {e}", "file_refs": []}
