"""Operational list and review workflow helpers for QuestBoard."""

from __future__ import annotations

import datetime as dt
from typing import Any

from .audit import REVIEW_STATES
from .setup_workspace import load_workspace_ids
from .workspace_data import (
    fetch_pages,
    filter_known_properties,
    filter_properties_for_database,
    get_select,
    get_text,
    normalize_review_item,
    normalize_run,
    search_database,
)

OPEN_REVIEW_STATES = ("Draft", "Needs Review", "Rejected")
REVIEW_ITEM_TYPES = ("Quest", "Boss Battle", "Adventure Recap", "Stale Quest", "Sync Repair")
RUN_TYPES = ("Quest Generation", "Boss Generation", "Weekly Recap", "Quest Sync", "Stale Patrol")
RUN_STATUSES = ("Succeeded", "Partial", "Failed")
REVISION_ITEM_TYPES = ("Quest", "Boss Battle", "Adventure Recap")


def _sort_desc(items: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    def _key(item: dict[str, Any]) -> tuple[str, ...]:
        return tuple(str(item.get(key, "") or "") for key in keys)

    return sorted(items, key=_key, reverse=True)


async def list_review_items(
    mcp,
    *,
    states: list[str] | None = None,
    limit: int = 15,
    include_closed: bool = False,
) -> list[dict[str, Any]]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    query_states = list(REVIEW_STATES if include_closed or states is None else states)
    queries = [*query_states, *REVIEW_ITEM_TYPES]
    pages = await search_database(mcp, db_ids.get("Review Queue", ""), queries)
    items = [normalize_review_item(page) for page in await fetch_pages(mcp, pages)]

    if states:
        requested = {state.casefold() for state in states}
        items = [item for item in items if item.get("review_state", "").casefold() in requested]
    elif not include_closed:
        items = [item for item in items if item.get("review_state") in OPEN_REVIEW_STATES]

    items = _sort_desc(items, "approved_at", "last_edited_time", "created_time")
    return items[:limit]


async def list_runs(
    mcp,
    *,
    statuses: list[str] | None = None,
    run_types: list[str] | None = None,
    limit: int = 15,
) -> list[dict[str, Any]]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    queries = [*(statuses or RUN_STATUSES), *(run_types or RUN_TYPES)]
    pages = await search_database(mcp, db_ids.get("Runs", ""), queries)
    records = [normalize_run(page) for page in await fetch_pages(mcp, pages)]

    if statuses:
        requested_statuses = {status.casefold() for status in statuses}
        records = [record for record in records if record.get("status", "").casefold() in requested_statuses]
    if run_types:
        requested_types = {run_type.casefold() for run_type in run_types}
        records = [record for record in records if record.get("type", "").casefold() in requested_types]

    records = _sort_desc(records, "started_at", "created_time")
    return records[:limit]


def _parse_timestamp(value: str) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _page_timestamp(page: dict[str, Any]) -> dt.datetime | None:
    return _parse_timestamp(str(page.get("last_edited_time", ""))) or _parse_timestamp(str(page.get("created_time", "")))


async def sync_review_items_for_target(
    mcp,
    target_page_id: str,
    *,
    review_state: str,
    correction_notes: str = "",
    reviewer: str = "",
    approved_at: str = "",
    locked: bool | None = None,
) -> list[str]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    review_db_id = db_ids.get("Review Queue", "")
    if not review_db_id or not target_page_id:
        return []

    pages = await search_database(mcp, review_db_id, [target_page_id])
    review_pages = await fetch_pages(mcp, pages)
    updated_ids: list[str] = []
    effective_locked = locked if locked is not None else review_state == "Locked"
    effective_approved_at = approved_at or (dt.date.today().isoformat() if review_state in {"Approved", "Locked"} else "")

    for review_page in review_pages:
        review_item = normalize_review_item(review_page)
        if review_item.get("target_page_id") != target_page_id:
            continue
        review_updates, _ = await filter_properties_for_database(
            mcp,
            review_db_id,
            {
                "Review State": review_state,
                "Correction Notes": correction_notes,
                "Reviewer": reviewer,
                "Approved At": effective_approved_at,
                "Locked": "Yes" if effective_locked else "No",
            },
        )
        if not review_updates:
            continue
        await mcp.update_page(review_item["id"], review_updates)
        updated_ids.append(review_item["id"])

    return updated_ids


async def reconcile_review_surfaces(
    mcp,
    *,
    triggered_by: str = "Runtime",
) -> list[dict[str, Any]]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    review_db_id = db_ids.get("Review Queue", "")
    if not review_db_id:
        return []

    review_pages = await fetch_pages(
        mcp,
        await search_database(
            mcp,
            review_db_id,
            ["Needs Review", "Draft", "Rejected", "Approved", "Locked", "Quest", "Recap", "Boss"],
        ),
    )
    quest_pages = await fetch_pages(
        mcp,
        await search_database(
            mcp,
            db_ids.get("Quest Board", ""),
            ["Available", "In Progress", "Completed", "Failed", "Expired", "Boss Battle", "AI Generated", "Player"],
        ),
    )
    recap_pages = await fetch_pages(
        mcp,
        await search_database(mcp, db_ids.get("Adventure Recaps", ""), ["Week of", "Recap", "Story Archive"]),
    )
    targets = {page.get("id", ""): page for page in [*quest_pages, *recap_pages] if page.get("id")}

    changes: list[dict[str, Any]] = []
    for review_page in review_pages:
        review_item = normalize_review_item(review_page)
        target_page_id = review_item.get("target_page_id", "")
        if not target_page_id:
            continue

        target_page = targets.get(target_page_id)
        if not target_page:
            continue

        queue_state = review_item.get("review_state", "")
        queue_notes = review_item.get("correction_notes", "")
        queue_locked = review_item.get("locked", False)

        target_state = get_select(target_page, "Review State", "")
        target_notes = get_text(target_page, "Correction Notes", "")

        queue_ts = _page_timestamp(review_page)
        target_ts = _page_timestamp(target_page)

        if (
            queue_state == target_state
            and queue_notes == target_notes
            and queue_locked == (queue_state == "Locked")
        ):
            continue

        target_is_newer = bool(target_ts and (not queue_ts or target_ts > queue_ts))
        if (target_state and not queue_state) or target_is_newer:
            review_updates, _ = await filter_properties_for_database(
                mcp,
                review_db_id,
                {
                    "Review State": target_state,
                    "Correction Notes": target_notes,
                    "Approved At": dt.date.today().isoformat() if target_state in {"Approved", "Locked"} else "",
                    "Locked": "Yes" if target_state == "Locked" else "No",
                },
            )
            if review_updates:
                await mcp.update_page(review_item["id"], review_updates)
                changes.append(
                    {
                        "direction": "target_to_queue",
                        "review_page_id": review_item["id"],
                        "target_page_id": target_page_id,
                        "review_state": target_state,
                    }
                )
            continue

        target_updates, _ = filter_known_properties(
            target_page,
            {
                "Review State": queue_state,
                "Correction Notes": queue_notes,
            },
        )
        if target_updates:
            await mcp.update_page(target_page_id, target_updates)
            changes.append(
                {
                    "direction": "queue_to_target",
                    "review_page_id": review_item["id"],
                    "target_page_id": target_page_id,
                    "review_state": queue_state,
                }
            )

    return changes


async def apply_review_decision(
    mcp,
    review_page_id: str,
    *,
    new_state: str,
    notes: str | None = None,
    reviewer: str | None = None,
    apply_to_target: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    if new_state not in REVIEW_STATES:
        raise ValueError(f"Unsupported review state: {new_state}")

    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    review_page = await mcp.fetch_page(review_page_id)
    review_item = normalize_review_item(review_page)
    if not review_item.get("id"):
        raise RuntimeError("Review item could not be found.")

    if review_item.get("locked") and new_state != "Locked" and not force:
        raise RuntimeError("This review item is locked. Use `--force` only if you intentionally want to override it.")

    effective_notes = review_item.get("correction_notes", "") if notes is None else notes
    effective_reviewer = review_item.get("reviewer", "") if reviewer is None else reviewer
    approved_at = review_item.get("approved_at") or ""
    if new_state in {"Approved", "Locked"}:
        approved_at = dt.date.today().isoformat()
    elif new_state in {"Draft", "Needs Review", "Rejected"}:
        approved_at = ""

    review_updates, _ = await filter_properties_for_database(
        mcp,
        db_ids["Review Queue"],
        {
            "Review State": new_state,
            "Correction Notes": effective_notes,
            "Reviewer": effective_reviewer,
            "Approved At": approved_at,
            "Locked": "Yes" if new_state == "Locked" else "No",
        },
    )
    if review_updates:
        await mcp.update_page(review_page_id, review_updates)

    target_page_id = review_item.get("target_page_id", "")
    target_applied = False
    if apply_to_target and target_page_id:
        target_page = await mcp.fetch_page(target_page_id)
        target_updates, _ = filter_known_properties(
            target_page,
            {
                "Review State": new_state,
                "Correction Notes": effective_notes,
            },
        )
        if target_updates:
            await mcp.update_page(target_page_id, target_updates)
            target_applied = True
        if hasattr(mcp, "create_comment"):
            summary = f"Review state set to {new_state}"
            if effective_reviewer:
                summary += f" by {effective_reviewer}"
            if effective_notes:
                summary += f". Notes: {effective_notes}"
            else:
                summary += "."
            await mcp.create_comment(target_page_id, summary)

    return {
        "review_page_id": review_page_id,
        "item": review_item.get("item", "Unknown Item"),
        "item_type": review_item.get("item_type", ""),
        "old_state": review_item.get("review_state", ""),
        "new_state": new_state,
        "reviewer": effective_reviewer,
        "notes": effective_notes,
        "approved_at": approved_at,
        "locked": new_state == "Locked",
        "target_page_id": target_page_id,
        "target_applied": target_applied,
    }


async def revise_review_item(
    mcp,
    review_page_id: str,
    *,
    notes: str | None = None,
    reviewer: str | None = None,
    allow_llm: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    review_page = await mcp.fetch_page(review_page_id)
    review_item = normalize_review_item(review_page)
    if not review_item.get("id"):
        raise RuntimeError("Review item could not be found.")

    if review_item.get("item_type") not in REVISION_ITEM_TYPES:
        raise RuntimeError(
            f"Revision is only supported for: {', '.join(REVISION_ITEM_TYPES)}."
        )
    if review_item.get("locked") and not force:
        raise RuntimeError("This review item is locked. Use `--force` only if you intentionally want to override it.")

    effective_notes = (notes if notes is not None else review_item.get("correction_notes", "")).strip()
    if not effective_notes:
        raise RuntimeError("Revision needs correction notes. Use `questboard review ... --state Rejected --notes ...` first or pass `--notes` directly.")
    effective_reviewer = reviewer if reviewer is not None else review_item.get("reviewer", "")
    target_page_id = review_item.get("target_page_id", "")
    if not target_page_id:
        raise RuntimeError("Review item does not point to a target page.")

    if review_item.get("item_type") == "Adventure Recap":
        from .revision import revise_recap_page

        revised = await revise_recap_page(
            mcp,
            target_page_id,
            effective_notes,
            allow_llm=allow_llm,
            triggered_by="CLI Revision",
        )
    else:
        from .revision import revise_quest_page

        revised = await revise_quest_page(
            mcp,
            target_page_id,
            effective_notes,
            allow_llm=allow_llm,
            triggered_by="CLI Revision",
        )

    review_updates, _ = await filter_properties_for_database(
        mcp,
        db_ids["Review Queue"],
        {
            "Item": revised.get("title", review_item.get("item", "Unknown Item")),
            "Source Run": revised.get("source_run", review_item.get("source_run", "")),
            "Review State": revised.get("review_state", "Needs Review"),
            "Correction Notes": effective_notes,
            "Reviewer": effective_reviewer,
            "Approved At": "",
            "Locked": "No",
            "Target Page ID": target_page_id,
            "Generation Mode": revised.get("generation_mode", review_item.get("generation_mode", "")),
            "Fallback Reason": revised.get("fallback_reason", ""),
        },
    )
    if review_updates:
        await mcp.update_page(review_page_id, review_updates)

    return {
        "review_page_id": review_page_id,
        "item": revised.get("title", review_item.get("item", "Unknown Item")),
        "item_type": review_item.get("item_type", ""),
        "new_state": revised.get("review_state", "Needs Review"),
        "reviewer": effective_reviewer,
        "notes": effective_notes,
        "source_run": revised.get("source_run", review_item.get("source_run", "")),
        "target_page_id": target_page_id,
        "generation_mode": revised.get("generation_mode", ""),
        "fallback_reason": revised.get("fallback_reason", ""),
    }
