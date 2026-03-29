"""Quest Runtime orchestration for automated, idempotent control loops."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import asdict, dataclass
from typing import Any

from .audit import queue_review_item
from .engines.quest_generator import generate_boss_battle, generate_quests
from .engines.recap_writer import detect_stale_quests, generate_weekly_recap
from .engines.xp_engine import reconcile_progress_state, sync_completed_quests
from .operations import reconcile_review_surfaces
from .setup_workspace import load_workspace_ids, load_workspace_player_name
from .workspace_data import (
    fetch_pages,
    get_player_page,
    get_quest_pages,
    get_title,
    normalize_player,
    normalize_quest,
    search_database,
)


@dataclass(slots=True)
class RuntimePolicy:
    min_available_quests: int = 3
    target_available_quests: int = 5
    allow_boss: bool = False
    min_level_for_boss: int = 3
    allow_recap: bool = True
    allow_llm: bool = True
    triggered_by: str = "Runtime"


def current_week_window(today: dt.date | None = None) -> tuple[dt.date, dt.date, str]:
    anchor = today or dt.date.today()
    start_of_week = anchor - dt.timedelta(days=anchor.weekday())
    end_of_week = start_of_week + dt.timedelta(days=6)
    week_label = f"Week of {start_of_week.strftime('%b %d')} - {end_of_week.strftime('%b %d, %Y')}"
    return start_of_week, end_of_week, week_label


async def _get_review_queue_pages(mcp, db_ids: dict[str, str]) -> list[dict[str, Any]]:
    review_db_id = db_ids.get("Review Queue", "")
    if not review_db_id:
        return []
    pages = await search_database(
        mcp,
        review_db_id,
        ["Needs Review", "Draft", "Rejected", "Approved", "Locked", "Quest", "Recap", "Boss"],
    )
    return await fetch_pages(mcp, pages)


async def _get_recap_pages(mcp, db_ids: dict[str, str]) -> list[dict[str, Any]]:
    recap_db_id = db_ids.get("Adventure Recaps", "")
    if not recap_db_id:
        return []
    pages = await search_database(mcp, recap_db_id, ["Week of", "Story Archive", "Recap"])
    return await fetch_pages(mcp, pages)


async def gather_runtime_snapshot(mcp) -> dict[str, Any]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    player_name = load_workspace_player_name()
    player_page = await get_player_page(mcp, db_ids, player_name)
    player = normalize_player(player_page) if player_page else {}

    quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
    review_pages = await _get_review_queue_pages(mcp, db_ids)
    recap_pages = await _get_recap_pages(mcp, db_ids)

    start_of_week, end_of_week, week_label = current_week_window()
    available_standard = [
        quest
        for quest in quests
        if quest.get("status") == "Available" and quest.get("difficulty") != "Boss"
    ]
    in_progress_standard = [
        quest
        for quest in quests
        if quest.get("status") == "In Progress" and quest.get("difficulty") != "Boss"
    ]
    active_bosses = [
        quest
        for quest in quests
        if quest.get("status") in {"Available", "In Progress"}
        and (quest.get("difficulty") == "Boss" or quest.get("source") == "Boss Battle")
    ]
    completed_this_week = []
    stale_candidates = []
    today = dt.date.today()
    for quest in quests:
        completed_at = quest.get("completed_at")
        if completed_at:
            try:
                completed_date = dt.date.fromisoformat(completed_at)
            except ValueError:
                completed_date = None
            if completed_date and start_of_week <= completed_date <= end_of_week:
                completed_this_week.append(quest)

        if quest.get("status") not in {"Available", "In Progress"}:
            continue
        due_date = quest.get("due_date")
        if not due_date:
            continue
        try:
            due = dt.date.fromisoformat(due_date)
        except ValueError:
            continue
        if due < today:
            stale_candidates.append(quest)

    open_review_states = {"Draft", "Needs Review", "Rejected"}
    open_reviews = 0
    for page in review_pages:
        state = str(page.get("properties", {}).get("Review State", {}).get("select", {}).get("name", ""))
        if state in open_review_states:
            open_reviews += 1

    recap_titles = [get_title(page, "Week", "") for page in recap_pages]
    has_current_week_recap = any(week_label in title for title in recap_titles)

    return {
        "player_name": player.get("name") or player_name,
        "player_level": player.get("level", 1),
        "quests_completed_total": player.get("quests_completed", 0),
        "week_label": week_label,
        "available_standard_quests": len(available_standard),
        "in_progress_standard_quests": len(in_progress_standard),
        "active_bosses": len(active_bosses),
        "completed_this_week": len(completed_this_week),
        "stale_candidates": len(stale_candidates),
        "open_reviews": open_reviews,
        "has_current_week_recap": has_current_week_recap,
    }


async def plan_runtime_tick(mcp, policy: RuntimePolicy | None = None) -> dict[str, Any]:
    runtime_policy = policy or RuntimePolicy()
    snapshot = await gather_runtime_snapshot(mcp)

    quest_shortfall = max(0, runtime_policy.target_available_quests - snapshot["available_standard_quests"])
    should_top_up = snapshot["available_standard_quests"] < runtime_policy.min_available_quests and quest_shortfall > 0
    boss_eligible = (
        runtime_policy.allow_boss
        and snapshot["active_bosses"] == 0
        and snapshot["player_level"] >= runtime_policy.min_level_for_boss
        and snapshot["quests_completed_total"] >= 3
    )
    should_recap = (
        runtime_policy.allow_recap
        and snapshot["completed_this_week"] > 0
        and not snapshot["has_current_week_recap"]
    )

    actions = [
        {
            "name": "sync",
            "will_run": True,
            "reason": "Process completed quests before every other decision.",
            "planned_count": 0,
        },
        {
            "name": "review_sync",
            "will_run": True,
            "reason": "Keep review rows and source pages aligned when people edit directly in Notion.",
            "planned_count": 0,
        },
        {
            "name": "patrol",
            "will_run": True,
            "reason": "Flag overdue quests so the board stays trustworthy.",
            "planned_count": 0,
        },
        {
            "name": "top_up_quests",
            "will_run": should_top_up,
            "reason": (
                f"Only {snapshot['available_standard_quests']} standard quest(s) are available; target is {runtime_policy.target_available_quests}."
                if should_top_up
                else f"Quest inventory is healthy at {snapshot['available_standard_quests']} available quest(s)."
            ),
            "planned_count": quest_shortfall if should_top_up else 0,
        },
        {
            "name": "boss",
            "will_run": boss_eligible,
            "reason": (
                "Player has enough momentum and no active boss is waiting."
                if boss_eligible
                else "Boss generation is gated by active boss count, player level, and completed-quest momentum."
            ),
            "planned_count": 1 if boss_eligible else 0,
        },
        {
            "name": "recap",
            "will_run": should_recap,
            "reason": (
                "This week has completed quests and no recap page yet."
                if should_recap
                else "Weekly recap is skipped when there is no new progress or the week already has a recap."
            ),
            "planned_count": 1 if should_recap else 0,
        },
    ]

    return {
        "policy": asdict(runtime_policy),
        "snapshot": snapshot,
        "actions": actions,
    }


async def _queue_generated_quest_reviews(mcp, db_ids: dict[str, str], quests: list[dict[str, Any]]) -> None:
    for quest in quests:
        await queue_review_item(
            mcp,
            db_ids,
            item=quest["quest"],
            item_type="Quest",
            source_run=quest.get("source_run", "quest-generation"),
            target_page_id=quest.get("id", ""),
            review_state=quest.get("review_state", "Needs Review"),
            correction_notes=(
                "Review the quest framing before execution."
                if quest.get("generation_mode") == "llm"
                else "Fallback template used. Confirm it still matches the current goal."
            ),
            generation_mode=quest.get("generation_mode", "llm"),
            fallback_reason=quest.get("fallback_reason", ""),
        )


async def _queue_boss_review(mcp, db_ids: dict[str, str], boss: dict[str, Any]) -> None:
    await queue_review_item(
        mcp,
        db_ids,
        item=boss.get("quest_title", boss.get("boss_name", "Boss Battle")),
        item_type="Boss Battle",
        source_run=boss.get("source_run", "boss-generation"),
        target_page_id=boss.get("id", ""),
        review_state=boss.get("review_state", "Needs Review"),
        correction_notes=(
            "Review the boss framing and pressure point before starting."
            if boss.get("generation_mode") == "llm"
            else "Fallback boss template used. Confirm the challenge still feels right."
        ),
        generation_mode=boss.get("generation_mode", "llm"),
        fallback_reason=boss.get("fallback_reason", ""),
    )


async def run_runtime_tick(
    mcp,
    *,
    policy: RuntimePolicy | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    runtime_policy = policy or RuntimePolicy()
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    plan = await plan_runtime_tick(mcp, runtime_policy)
    if dry_run:
        return {
            "dry_run": True,
            "plan": plan,
            "snapshot_before": plan["snapshot"],
            "snapshot_after": plan["snapshot"],
            "actions": [],
        }

    result = {
        "dry_run": False,
        "plan": plan,
        "snapshot_before": plan["snapshot"],
        "snapshot_after": None,
        "actions": [],
    }

    synced = await sync_completed_quests(mcp, triggered_by=runtime_policy.triggered_by)
    progress_summary = await reconcile_progress_state(mcp)
    result["actions"].append({
        "name": "sync",
        "status": "ran",
        "count": len(synced),
        "detail": (
            f"Processed {len(synced)} completed quest(s) and rebuilt player totals "
            f"(Level {progress_summary['level']}, {progress_summary['total_xp']} XP)."
        ),
    })

    review_changes = await reconcile_review_surfaces(mcp, triggered_by=runtime_policy.triggered_by)
    result["actions"].append({
        "name": "review_sync",
        "status": "ran",
        "count": len(review_changes),
        "detail": f"Reconciled {len(review_changes)} review surface mismatch(es).",
    })

    stale = await detect_stale_quests(mcp, triggered_by=runtime_policy.triggered_by)
    result["actions"].append({
        "name": "patrol",
        "status": "ran",
        "count": len(stale),
        "detail": f"Flagged {len(stale)} stale quest(s).",
    })

    snapshot_after_maintenance = await gather_runtime_snapshot(mcp)
    quest_shortfall = max(0, runtime_policy.target_available_quests - snapshot_after_maintenance["available_standard_quests"])
    if snapshot_after_maintenance["available_standard_quests"] < runtime_policy.min_available_quests and quest_shortfall > 0:
        created_quests = await generate_quests(
            mcp,
            count=quest_shortfall,
            allow_llm=runtime_policy.allow_llm,
            triggered_by=runtime_policy.triggered_by,
        )
        await _queue_generated_quest_reviews(mcp, db_ids, created_quests)
        result["actions"].append({
            "name": "top_up_quests",
            "status": "ran",
            "count": len(created_quests),
            "detail": f"Generated {len(created_quests)} quest(s) to restore quest inventory.",
        })
    else:
        result["actions"].append({
            "name": "top_up_quests",
            "status": "skipped",
            "count": 0,
            "detail": f"Skipped; {snapshot_after_maintenance['available_standard_quests']} standard quest(s) are already available.",
        })

    snapshot_after_top_up = await gather_runtime_snapshot(mcp)
    boss_eligible = (
        runtime_policy.allow_boss
        and snapshot_after_top_up["active_bosses"] == 0
        and snapshot_after_top_up["player_level"] >= runtime_policy.min_level_for_boss
        and snapshot_after_top_up["quests_completed_total"] >= 3
    )
    if boss_eligible:
        boss = await generate_boss_battle(
            mcp,
            allow_llm=runtime_policy.allow_llm,
            triggered_by=runtime_policy.triggered_by,
        )
        await _queue_boss_review(mcp, db_ids, boss)
        result["actions"].append({
            "name": "boss",
            "status": "ran",
            "count": 1,
            "detail": f"Generated boss battle: {boss.get('quest_title', boss.get('boss_name', 'Boss Battle'))}.",
        })
    else:
        result["actions"].append({
            "name": "boss",
            "status": "skipped",
            "count": 0,
            "detail": "Skipped; boss generation is still gated by level, momentum, or an existing active boss.",
        })

    snapshot_before_recap = await gather_runtime_snapshot(mcp)
    should_recap = (
        runtime_policy.allow_recap
        and snapshot_before_recap["completed_this_week"] > 0
        and not snapshot_before_recap["has_current_week_recap"]
    )
    if should_recap:
        recap = await generate_weekly_recap(
            mcp,
            allow_llm=runtime_policy.allow_llm,
            triggered_by=runtime_policy.triggered_by,
        )
        result["actions"].append({
            "name": "recap",
            "status": "ran",
            "count": 1,
            "detail": f"Generated recap for {recap['week']}.",
        })
    else:
        result["actions"].append({
            "name": "recap",
            "status": "skipped",
            "count": 0,
            "detail": "Skipped; no new week progress was found or the current week already has a recap.",
        })

    result["snapshot_after"] = await gather_runtime_snapshot(mcp)
    return result


async def watch_runtime(
    mcp,
    *,
    policy: RuntimePolicy | None = None,
    interval_seconds: int = 300,
    iterations: int = 1,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    runtime_policy = policy or RuntimePolicy()
    if interval_seconds < 1:
        raise ValueError("interval_seconds must be at least 1")
    if iterations < 0:
        raise ValueError("iterations must be 0 or greater")

    results: list[dict[str, Any]] = []
    tick = 0
    while iterations == 0 or tick < iterations:
        results.append(await run_runtime_tick(mcp, policy=runtime_policy, dry_run=dry_run))
        tick += 1
        if iterations and tick >= iterations:
            break
        await asyncio.sleep(interval_seconds)
    return results
