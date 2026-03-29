"""Workspace-scoped helpers for reading QuestBoard data from Notion."""

from __future__ import annotations

import datetime as dt
from typing import Any

from .config import SKILL_TREES


def _normalized_id(value: str) -> str:
    return (value or "").replace("-", "").strip().lower()


def _properties(container: dict[str, Any]) -> dict[str, Any]:
    if isinstance(container, dict) and isinstance(container.get("properties"), dict):
        return container["properties"]
    return container if isinstance(container, dict) else {}


def filter_known_properties(container: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Keep only properties that exist on the given page/database container."""
    known = _properties(container)
    if not known:
        return values, []

    filtered = {name: value for name, value in values.items() if name in known}
    dropped = [name for name in values if name not in known]
    return filtered, dropped


def _page_title(page: dict[str, Any]) -> str:
    if not isinstance(page, dict):
        return ""

    title_prop = page.get("title")
    if isinstance(title_prop, list) and title_prop:
        first = title_prop[0]
        if isinstance(first, dict):
            return first.get("plain_text", "")

    props = _properties(page)
    for field in ("Quest", "Name", "Achievement", "Adventurer", "Week", "Skill"):
        text = get_title(props, field)
        if text:
            return text
    return ""


def in_database(page: dict[str, Any], database_id: str) -> bool:
    if not database_id or not isinstance(page, dict):
        return False

    parent = page.get("parent", {})
    parent_id = parent.get("database_id") or parent.get("data_source_id") or ""
    return _normalized_id(parent_id) == _normalized_id(database_id)


def get_number(container: dict[str, Any], name: str, default: int = 0) -> int:
    prop = _properties(container).get(name, {})
    if isinstance(prop, dict) and "number" in prop:
        return prop["number"] if prop["number"] is not None else default
    if isinstance(prop, dict) and isinstance(prop.get("rollup"), dict):
        rollup = prop["rollup"]
        if "number" in rollup and rollup["number"] is not None:
            return int(rollup["number"])
    if isinstance(prop, dict) and isinstance(prop.get("formula"), dict):
        formula = prop["formula"]
        if formula.get("type") == "number" and formula.get("number") is not None:
            return int(formula["number"])
    return default


def get_select(container: dict[str, Any], name: str, default: str = "") -> str:
    prop = _properties(container).get(name, {})
    if not isinstance(prop, dict):
        return default

    if prop.get("select"):
        return prop["select"].get("name", default)
    if prop.get("status"):
        return prop["status"].get("name", default)

    title = get_title(container, name)
    if title:
        return title

    rich = get_text(container, name)
    return rich or default


def get_title(container: dict[str, Any], name: str, default: str = "") -> str:
    prop = _properties(container).get(name, {})
    if isinstance(prop, dict):
        if isinstance(prop.get("title"), list) and prop["title"]:
            return prop["title"][0].get("plain_text", default)
        if isinstance(prop.get("rich_text"), list) and prop["rich_text"]:
            return prop["rich_text"][0].get("plain_text", default)
        if prop.get("select"):
            return prop["select"].get("name", default)
        if isinstance(prop.get("formula"), dict) and prop["formula"].get("type") == "string":
            return prop["formula"].get("string", default) or default
    return default


def get_text(container: dict[str, Any], name: str, default: str = "") -> str:
    prop = _properties(container).get(name, {})
    if isinstance(prop, dict) and isinstance(prop.get("rich_text"), list) and prop["rich_text"]:
        return prop["rich_text"][0].get("plain_text", default)
    if isinstance(prop, dict) and isinstance(prop.get("formula"), dict) and prop["formula"].get("type") == "string":
        return prop["formula"].get("string", default) or default
    return default


def get_date(container: dict[str, Any], name: str) -> str | None:
    prop = _properties(container).get(name, {})
    if isinstance(prop, dict) and prop.get("date"):
        return prop["date"].get("start")
    if isinstance(prop, dict) and isinstance(prop.get("formula"), dict) and prop["formula"].get("type") == "date":
        formula_date = prop["formula"].get("date") or {}
        return formula_date.get("start")
    return None


def extract_skill_name(value: str) -> str:
    if not value:
        return ""

    cleaned = value.strip()
    for skill_name in SKILL_TREES:
        if cleaned == skill_name or cleaned.endswith(skill_name):
            return skill_name
    return cleaned


def normalize_quest(page: dict[str, Any]) -> dict[str, Any]:
    quest_name = get_title(page, "Quest") or _page_title(page) or "Unknown Quest"
    skill = extract_skill_name(get_select(page, "Skill", get_text(page, "Skill", "")))
    live_xp = get_number(page, "Live XP", get_number(page, "Awarded XP", get_number(page, "XP Reward", 0)))
    completed_value = get_number(page, "Completed Value", 1 if get_select(page, "Status", "") == "Completed" else 0)
    boss_completion_value = get_number(
        page,
        "Boss Completion Value",
        1
        if (
            get_select(page, "Status", "") == "Completed"
            and (get_select(page, "Difficulty", "") == "Boss" or get_select(page, "Source", "") == "Boss Battle")
        )
        else 0,
    )

    return {
        "id": page.get("id", ""),
        "quest": quest_name,
        "status": get_select(page, "Status", ""),
        "skill": skill,
        "rarity": get_select(page, "Rarity", "Common"),
        "source": get_select(page, "Source", ""),
        "difficulty": get_select(page, "Difficulty", "Medium"),
        "xp_reward": get_number(page, "XP Reward", 0),
        "awarded_xp": get_number(page, "Awarded XP", live_xp),
        "live_xp": live_xp,
        "completed_value": completed_value,
        "boss_completion_value": boss_completion_value,
        "description": get_text(page, "Description", ""),
        "why_this_quest": get_text(page, "Why This Quest", ""),
        "generation_mode": get_select(page, "Generation Mode", get_text(page, "Generation Mode", "")),
        "review_state": get_select(page, "Review State", ""),
        "correction_notes": get_text(page, "Correction Notes", ""),
        "source_run": get_text(page, "Source Run", ""),
        "prompt_version": get_text(page, "Prompt Version", ""),
        "fallback_reason": get_text(page, "Fallback Reason", ""),
        "due_date": get_date(page, "Due Date"),
        "completed_at": get_date(page, "Completed At"),
    }


def normalize_skill(page: dict[str, Any]) -> dict[str, Any]:
    raw_skill = get_title(page, "Skill") or get_select(page, "Category", "")
    skill_name = extract_skill_name(raw_skill) or get_select(page, "Category", "Endurance")
    return {
        "id": page.get("id", ""),
        "skill": skill_name,
        "xp": get_number(page, "Current XP", 0),
        "level": get_number(page, "Level", 1),
        "last_activity": get_date(page, "Last Activity"),
        "quests_completed": get_number(page, "Quests Completed", 0),
    }


def normalize_player(page: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": page.get("id", ""),
        "name": get_title(page, "Name", _page_title(page)),
        "level": get_number(page, "Level", 1),
        "total_xp": get_number(page, "Total XP", 0),
        "title": get_text(page, "Title", "Novice Adventurer"),
        "hp": get_number(page, "HP", 100),
        "streak_days": get_number(page, "Streak Days", 0),
        "quests_completed": get_number(page, "Quests Completed", 0),
        "boss_kills": get_number(page, "Boss Kills", 0),
        "primary_goal": get_text(page, "Primary Goal", ""),
        "available_time": get_text(page, "Available Time", ""),
        "preferred_challenge_style": get_select(page, "Preferred Challenge Style", ""),
        "focus_area": get_text(page, "Focus Area", ""),
        "constraints": get_text(page, "Constraints", ""),
        "motivation": get_text(page, "Motivation", ""),
        "context_brief": get_text(page, "Context Brief", ""),
        "context_sources": get_text(page, "Context Sources", ""),
    }


def normalize_review_item(page: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": page.get("id", ""),
        "item": get_title(page, "Item", _page_title(page)),
        "item_type": get_select(page, "Item Type", ""),
        "source_run": get_text(page, "Source Run", ""),
        "review_state": get_select(page, "Review State", ""),
        "correction_notes": get_text(page, "Correction Notes", ""),
        "reviewer": get_text(page, "Reviewer", ""),
        "approved_at": get_date(page, "Approved At"),
        "locked": get_select(page, "Locked", "No") == "Yes",
        "target_page_id": get_text(page, "Target Page ID", ""),
        "generation_mode": get_select(page, "Generation Mode", ""),
        "fallback_reason": get_text(page, "Fallback Reason", ""),
        "created_time": str(page.get("created_time", "")),
        "last_edited_time": str(page.get("last_edited_time", "")),
    }


def normalize_run(page: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": page.get("id", ""),
        "run": get_title(page, "Run", _page_title(page)),
        "type": get_select(page, "Type", ""),
        "status": get_select(page, "Status", ""),
        "started_at": get_date(page, "Started At"),
        "finished_at": get_date(page, "Finished At"),
        "duration_ms": get_number(page, "Duration Ms", 0),
        "triggered_by": get_text(page, "Triggered By", ""),
        "target_entity": get_text(page, "Target Entity", ""),
        "model": get_text(page, "Model", ""),
        "generation_mode": get_select(page, "Generation Mode", ""),
        "fallback_reason": get_text(page, "Fallback Reason", ""),
        "prompt_version": get_text(page, "Prompt Version", ""),
        "replayable": get_select(page, "Replayable", "No") == "Yes",
        "error_summary": get_text(page, "Error Summary", ""),
        "records_created": get_number(page, "Records Created", 0),
        "records_updated": get_number(page, "Records Updated", 0),
        "created_time": str(page.get("created_time", "")),
    }


async def search_database(mcp, database_id: str, queries: list[str]) -> list[dict[str, Any]]:
    """Search Notion and keep only rows that belong to the given database."""
    if not database_id:
        return []

    is_self_hosted = getattr(getattr(mcp, "config", None), "is_self_hosted", True)
    seen: dict[str, dict[str, Any]] = {}
    data_source_url = ""
    if not is_self_hosted:
        try:
            database = await mcp.fetch_page(database_id)
            data_source_url = database.get("data_source_url", "")
        except Exception:
            data_source_url = ""

    for query in queries:
        if query is None:
            continue
        try:
            if is_self_hosted:
                results = await mcp.search(query)
            else:
                search_args = {"data_source_url": data_source_url} if data_source_url else {}
                results = await mcp.search(query, **search_args)
        except Exception:
            continue

        for item in results.get("results", []):
            item_id = item.get("id", "")
            if not item_id:
                continue
            if not is_self_hosted or in_database(item, database_id):
                seen[item_id] = item

    return list(seen.values())


async def fetch_pages(mcp, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fetched: list[dict[str, Any]] = []
    for page in pages:
        page_id = page.get("id", "")
        if not page_id:
            continue
        try:
            refreshed = await mcp.fetch_page(page_id)
            if not refreshed.get("created_time") and page.get("created_time"):
                refreshed["created_time"] = page["created_time"]
            if not refreshed.get("last_edited_time") and page.get("last_edited_time"):
                refreshed["last_edited_time"] = page["last_edited_time"]
            fetched.append(refreshed)
        except Exception:
            fetched.append(page)
    return fetched


async def filter_properties_for_database(mcp, database_id: str, values: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Keep only properties supported by the target database schema."""
    if not database_id:
        return values, []
    try:
        database = await mcp.fetch_page(database_id)
    except Exception:
        return values, []
    return filter_known_properties(database, values)


async def get_player_page(mcp, db_ids: dict[str, str], player_name: str) -> dict[str, Any] | None:
    pages = await search_database(
        mcp,
        db_ids.get("Player Profile", ""),
        [player_name, "Novice Adventurer", "Player Profile"],
    )
    pages = await fetch_pages(mcp, pages)

    for page in pages:
        if get_title(page, "Name", "").casefold() == player_name.casefold():
            return page
    return pages[0] if pages else None


async def get_skill_pages(mcp, db_ids: dict[str, str]) -> list[dict[str, Any]]:
    pages = await search_database(mcp, db_ids.get("Skill Trees", ""), list(SKILL_TREES.keys()))
    return await fetch_pages(mcp, pages)


async def get_skill_page(mcp, db_ids: dict[str, str], skill_name: str) -> dict[str, Any] | None:
    pages = await search_database(mcp, db_ids.get("Skill Trees", ""), [skill_name])
    pages = await fetch_pages(mcp, pages)
    for page in pages:
        if extract_skill_name(get_title(page, "Skill")) == skill_name or get_select(page, "Category") == skill_name:
            return page
    return pages[0] if pages else None


async def get_quest_pages(mcp, db_ids: dict[str, str]) -> list[dict[str, Any]]:
    queries = [
        "Available",
        "In Progress",
        "Completed",
        "Failed",
        "Expired",
        "Boss Battle",
        "AI Generated",
        "Player",
        *SKILL_TREES.keys(),
    ]
    pages = await search_database(mcp, db_ids.get("Quest Board", ""), queries)
    return await fetch_pages(mcp, pages)


async def get_party_page(mcp, db_ids: dict[str, str], player_name: str) -> dict[str, Any] | None:
    pages = await search_database(mcp, db_ids.get("Party Board", ""), [player_name, "Warrior", "Mage"])
    pages = await fetch_pages(mcp, pages)
    for page in pages:
        if get_title(page, "Adventurer", "").casefold() == player_name.casefold():
            return page
    return pages[0] if pages else None


async def get_achievement_pages(mcp, db_ids: dict[str, str]) -> list[dict[str, Any]]:
    queries = [
        "Quest Milestones",
        "Skill Mastery",
        "Streak",
        "Boss Slayer",
        "Explorer",
        "Special",
    ]
    pages = await search_database(mcp, db_ids.get("Achievement Log", ""), queries)
    return await fetch_pages(mcp, pages)


def quests_completed_between(
    quests: list[dict[str, Any]],
    start_date: dt.date,
    end_date: dt.date,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for quest in quests:
        completed_at = quest.get("completed_at")
        if not completed_at:
            continue
        try:
            completed_date = dt.date.fromisoformat(completed_at)
        except ValueError:
            continue
        if start_date <= completed_date <= end_date:
            filtered.append(quest)
    return filtered


def calculate_streak(quests: list[dict[str, Any]], today: dt.date | None = None) -> int:
    completed_days: set[dt.date] = set()
    for quest in quests:
        completed_at = quest.get("completed_at")
        if not completed_at:
            continue
        try:
            completed_days.add(dt.date.fromisoformat(completed_at))
        except ValueError:
            continue

    if not completed_days:
        return 0

    anchor = today or max(completed_days)
    if anchor not in completed_days:
        anchor = max(completed_days)

    streak = 0
    current = anchor
    while current in completed_days:
        streak += 1
        current -= dt.timedelta(days=1)
    return streak
