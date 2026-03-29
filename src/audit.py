"""Operational audit and review helpers for QuestBoard."""

from __future__ import annotations

import datetime as dt

from .config import get_config
from .workspace_data import filter_properties_for_database

REVIEW_STATES = ("Draft", "Needs Review", "Approved", "Rejected", "Locked")
GENERATION_MODE_LABELS = {
    "player": "Player",
    "llm": "LLM",
    "fallback": "Fallback Template",
    "hybrid": "Hybrid",
    "operational": "Operational",
}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_timestamp(value: dt.datetime | None = None) -> str:
    current = value or utc_now()
    return current.isoformat(timespec="seconds").replace("+00:00", "Z")


def duration_ms(started_at: dt.datetime, finished_at: dt.datetime | None = None) -> int:
    end = finished_at or utc_now()
    return max(0, int((end - started_at).total_seconds() * 1000))


def build_run_ref(run_type: str, started_at: dt.datetime | None = None) -> str:
    current = started_at or utc_now()
    slug = "".join(char.lower() if char.isalnum() else "-" for char in run_type).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return f"{slug}:{current.strftime('%Y%m%dT%H%M%SZ')}"


def generation_mode_label(mode: str) -> str:
    return GENERATION_MODE_LABELS.get((mode or "").strip().lower(), mode or "Operational")


def review_state_for_mode(mode: str) -> str:
    normalized = (mode or "").strip().lower()
    if normalized in {"player"}:
        return "Approved"
    if normalized in {"operational"}:
        return "Approved"
    return "Needs Review"


def llm_model_label(mode: str) -> str:
    if (mode or "").strip().lower() not in {"llm", "hybrid", "fallback"}:
        return ""
    config = get_config()
    if not config.llm_provider:
        return ""
    return f"{config.llm_provider}:{config.llm_model}"


def summarize_exception(exc: Exception | None, *, limit: int = 180) -> str:
    if exc is None:
        return ""
    text = f"{exc.__class__.__name__}: {exc}".strip()
    return text[:limit]


async def log_run(
    mcp,
    db_ids: dict[str, str],
    *,
    run_ref: str,
    run_type: str,
    status: str,
    started_at: dt.datetime,
    finished_at: dt.datetime | None = None,
    triggered_by: str = "CLI",
    target_entity: str = "",
    model: str = "",
    generation_mode: str = "Operational",
    fallback_reason: str = "",
    prompt_version: str = "",
    replayable: bool = True,
    error_summary: str = "",
    records_created: int = 0,
    records_updated: int = 0,
):
    runs_db_id = db_ids.get("Runs", "")
    if not runs_db_id:
        return None

    finished = finished_at or utc_now()
    payload, _ = await filter_properties_for_database(mcp, runs_db_id, {
        "Run": run_ref,
        "Type": run_type,
        "Status": status,
        "Started At": iso_timestamp(started_at),
        "Finished At": iso_timestamp(finished),
        "Duration Ms": duration_ms(started_at, finished),
        "Triggered By": triggered_by,
        "Target Entity": target_entity,
        "Model": model,
        "Generation Mode": generation_mode_label(generation_mode),
        "Fallback Reason": fallback_reason,
        "Prompt Version": prompt_version,
        "Replayable": "Yes" if replayable else "No",
        "Error Summary": error_summary,
        "Records Created": records_created,
        "Records Updated": records_updated,
    })
    if not payload:
        return None
    return await mcp.create_db_page(runs_db_id, payload, icon="🛰️")


async def queue_review_item(
    mcp,
    db_ids: dict[str, str],
    *,
    item: str,
    item_type: str,
    source_run: str,
    target_page_id: str = "",
    review_state: str = "Needs Review",
    correction_notes: str = "",
    reviewer: str = "",
    approved_at: str = "",
    locked: bool = False,
    generation_mode: str = "",
    fallback_reason: str = "",
):
    review_db_id = db_ids.get("Review Queue", "")
    if not review_db_id:
        return None

    payload, _ = await filter_properties_for_database(mcp, review_db_id, {
        "Item": item,
        "Item Type": item_type,
        "Source Run": source_run,
        "Review State": review_state,
        "Correction Notes": correction_notes,
        "Reviewer": reviewer,
        "Approved At": approved_at,
        "Locked": "Yes" if locked else "No",
        "Target Page ID": target_page_id,
        "Generation Mode": generation_mode_label(generation_mode),
        "Fallback Reason": fallback_reason,
    })
    if not payload:
        return None
    return await mcp.create_db_page(review_db_id, payload, icon="🧭")
