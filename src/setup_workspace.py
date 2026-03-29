"""One-command workspace setup: creates all QuestBoard databases and views in Notion."""

import asyncio
import json
import os

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .mcp_client import NotionMCP
from .config import get_config, SKILL_TREES, QUEST_RARITIES
from .workspace_data import (
    get_party_page,
    get_player_page,
    get_quest_pages,
    get_skill_pages,
    normalize_skill,
)

console = Console(force_terminal=True)
WORKSPACE_VERSION = 5
REVIEW_STATE_OPTIONS = ["Draft", "Needs Review", "Approved", "Rejected", "Locked"]
ICON_HUB = "\u2694\ufe0f"
ICON_PLAYER_PROFILE = "\U0001f6e1\ufe0f"
ICON_QUEST_BOARD = "\U0001f4dc"
ICON_SKILL_TREES = "\U0001f333"
ICON_ACHIEVEMENT_LOG = "\U0001f3c6"
ICON_PARTY_BOARD = "\U0001f465"
ICON_ADVENTURE_RECAPS = "\U0001f4d6"
ICON_RECEIPT = "\U0001f9fe"
ICON_MAP = "\U0001f5fa\ufe0f"
ICON_CALENDAR = "\U0001f4c6"
ICON_HOURGLASS = "\u23f3"
ICON_CARDS = "\U0001f3b4"
ICON_MEMO = "\U0001f4dd"
ICON_CHART = "\U0001f4ca"
ICON_CLIPBOARD = "\U0001f4cb"
ICON_WORKOUT = "\U0001f3cb\ufe0f"
ICON_BOOKS = "\U0001f4da"
ICON_ART = "\U0001f3a8"
ICON_SOCIAL = "\U0001f5e3\ufe0f"
ICON_MEDITATION = "\U0001f9d8"
ICON_RUNNING = "\U0001f3c3"
MOJIBAKE_HINTS = ("\xf0", "\u0178", "\xe2", "\u0153", "\u20ac", "\u2122", "\u0161", "\u017e", "\x8f", "\x9d")

# Database schemas
PLAYER_PROFILE_PROPS = {
    "Name": {"type": "title"},
    "Level": {"type": "number"},
    "Total XP": {"type": "number"},
    "Title": {"type": "rich_text"},
    "Class": {"type": "select", "options": [
        "Warrior", "Mage", "Ranger", "Bard", "Paladin", "Rogue"
    ]},
    "HP": {"type": "number"},
    "Streak Days": {"type": "number"},
    "Quests Completed": {"type": "number"},
    "Boss Kills": {"type": "number"},
    "Joined": {"type": "date"},
    "Primary Goal": {"type": "rich_text"},
    "Available Time": {"type": "rich_text"},
    "Preferred Challenge Style": {"type": "select", "options": [
        "Quick Wins", "Balanced", "Deep Work", "Stretch Me"
    ]},
    "Focus Area": {"type": "rich_text"},
    "Constraints": {"type": "rich_text"},
    "Motivation": {"type": "rich_text"},
    "Context Brief": {"type": "rich_text"},
    "Context Sources": {"type": "rich_text"},
}

QUEST_BOARD_PROPS = {
    "Quest": {"type": "title"},
    "Status": {"type": "select", "options": [
        "Available", "In Progress", "Completed", "Failed", "Expired"
    ]},
    "Rarity": {"type": "select", "options": list(QUEST_RARITIES.keys())},
    "Skill": {"type": "select", "options": list(SKILL_TREES.keys())},
    "XP Reward": {"type": "number"},
    "Due Date": {"type": "date"},
    "Description": {"type": "rich_text"},
    "Difficulty": {"type": "select", "options": ["Easy", "Medium", "Hard", "Boss"]},
    "Source": {"type": "select", "options": ["Player", "AI Generated", "Boss Battle", "Daily"]},
    "Completed At": {"type": "date"},
    "Awarded XP": {"type": "number"},
    "Why This Quest": {"type": "rich_text"},
    "Generation Mode": {"type": "select", "options": ["Player", "LLM", "Fallback Template"]},
    "Review State": {"type": "select", "options": REVIEW_STATE_OPTIONS},
    "Correction Notes": {"type": "rich_text"},
    "Source Run": {"type": "rich_text"},
    "Prompt Version": {"type": "rich_text"},
    "Fallback Reason": {"type": "rich_text"},
}

HOSTED_CONNECTED_QUEST_BOARD_PROPS = {
    "Live XP": {
        "type": "formula",
        "expression": 'if(prop("Status") == "Completed", if(empty(prop("Awarded XP")), prop("XP Reward"), prop("Awarded XP")), 0)',
    },
    "Completed Value": {
        "type": "formula",
        "expression": 'if(prop("Status") == "Completed", 1, 0)',
    },
    "Boss Completion Value": {
        "type": "formula",
        "expression": 'if(and(prop("Status") == "Completed", or(prop("Difficulty") == "Boss", prop("Source") == "Boss Battle")), 1, 0)',
    },
}

SKILL_TREE_PROPS = {
    "Skill": {"type": "title"},
    "Current XP": {"type": "number"},
    "Level": {"type": "number"},
    "Category": {"type": "select", "options": list(SKILL_TREES.keys())},
    "Description": {"type": "rich_text"},
    "Last Activity": {"type": "date"},
    "Quests Completed": {"type": "number"},
}

ACHIEVEMENT_LOG_PROPS = {
    "Achievement": {"type": "title"},
    "Description": {"type": "rich_text"},
    "Unlocked At": {"type": "date"},
    "XP Bonus": {"type": "number"},
    "Rarity": {"type": "select", "options": ["Bronze", "Silver", "Gold", "Diamond"]},
    "Category": {"type": "select", "options": [
        "Quest Milestones", "Skill Mastery", "Streak", "Boss Slayer", "Explorer", "Special"
    ]},
}

PARTY_BOARD_PROPS = {
    "Adventurer": {"type": "title"},
    "Level": {"type": "number"},
    "Total XP": {"type": "number"},
    "Class": {"type": "select", "options": [
        "Warrior", "Mage", "Ranger", "Bard", "Paladin", "Rogue"
    ]},
    "Quests Completed": {"type": "number"},
    "Current Streak": {"type": "number"},
    "Title": {"type": "rich_text"},
}

ADVENTURE_RECAP_PROPS = {
    "Week": {"type": "title"},
    "Period": {"type": "date"},
    "Quests Completed": {"type": "number"},
    "XP Earned": {"type": "number"},
    "Levels Gained": {"type": "number"},
    "Achievements Unlocked": {"type": "number"},
    "MVP Skill": {"type": "select", "options": list(SKILL_TREES.keys())},
    "Narrative": {"type": "rich_text"},
    "Generation Mode": {"type": "select", "options": ["LLM", "Fallback Template"]},
    "Review State": {"type": "select", "options": REVIEW_STATE_OPTIONS},
    "Correction Notes": {"type": "rich_text"},
    "Source Run": {"type": "rich_text"},
    "Prompt Version": {"type": "rich_text"},
    "Fallback Reason": {"type": "rich_text"},
}

RUNS_PROPS = {
    "Run": {"type": "title"},
    "Type": {"type": "select", "options": ["Quest Generation", "Boss Generation", "Weekly Recap", "Quest Sync", "Stale Patrol"]},
    "Status": {"type": "select", "options": ["Succeeded", "Partial", "Failed"]},
    "Started At": {"type": "date"},
    "Finished At": {"type": "date"},
    "Duration Ms": {"type": "number"},
    "Triggered By": {"type": "rich_text"},
    "Target Entity": {"type": "rich_text"},
    "Model": {"type": "rich_text"},
    "Generation Mode": {"type": "select", "options": ["Player", "Operational", "LLM", "Fallback Template", "Hybrid"]},
    "Fallback Reason": {"type": "rich_text"},
    "Prompt Version": {"type": "rich_text"},
    "Replayable": {"type": "select", "options": ["Yes", "No"]},
    "Error Summary": {"type": "rich_text"},
    "Records Created": {"type": "number"},
    "Records Updated": {"type": "number"},
}

REVIEW_QUEUE_PROPS = {
    "Item": {"type": "title"},
    "Item Type": {"type": "select", "options": ["Quest", "Boss Battle", "Adventure Recap", "Stale Quest", "Sync Repair"]},
    "Source Run": {"type": "rich_text"},
    "Review State": {"type": "select", "options": REVIEW_STATE_OPTIONS},
    "Correction Notes": {"type": "rich_text"},
    "Reviewer": {"type": "rich_text"},
    "Approved At": {"type": "date"},
    "Locked": {"type": "select", "options": ["Yes", "No"]},
    "Target Page ID": {"type": "rich_text"},
    "Generation Mode": {"type": "select", "options": ["Player", "Operational", "LLM", "Fallback Template", "Hybrid"]},
    "Fallback Reason": {"type": "rich_text"},
}

DATABASE_BLUEPRINTS = [
    ("Player Profile", "Player Profile", PLAYER_PROFILE_PROPS, ICON_PLAYER_PROFILE, "Your hero stats and progression"),
    ("Quest Board", "Quest Board", QUEST_BOARD_PROPS, ICON_QUEST_BOARD, "Active and completed quests"),
    ("Skill Trees", "Skill Trees", SKILL_TREE_PROPS, ICON_SKILL_TREES, "Track XP across 6 skill categories"),
    ("Achievement Log", "Achievement Log", ACHIEVEMENT_LOG_PROPS, ICON_ACHIEVEMENT_LOG, "Unlocked achievements and badges"),
    ("Party Board", "Hero Roster", PARTY_BOARD_PROPS, ICON_PARTY_BOARD, "Your hero and any future companions"),
    ("Adventure Recaps", "Adventure Recaps", ADVENTURE_RECAP_PROPS, ICON_ADVENTURE_RECAPS, "Weekly narrative summaries"),
    ("Runs", "Runs", RUNS_PROPS, "[Runs]", "Operational run history, durations, and fallback reasons"),
    ("Review Queue", "Review Queue", REVIEW_QUEUE_PROPS, "[Review]", "Human-in-the-loop review for generated output"),
]

HOSTED_CONNECTED_PROPS = {
    "Quest Board": HOSTED_CONNECTED_QUEST_BOARD_PROPS,
}

QUESTBOARD_HOSTED_VIEW_TYPES = (
    "table",
    "board",
    "list",
    "calendar",
    "timeline",
    "gallery",
    "chart",
    "dashboard",
)

QUESTBOARD_HOSTED_VIEWS = {
    "Quest Board": [
        {"type": "table", "name": f"{ICON_RECEIPT} Quest Ledger"},
        {"type": "board", "name": f"{ICON_MAP} Quest Map", "config": {"group_by": "Status"}},
        {"type": "calendar", "name": f"{ICON_CALENDAR} Quest Calendar", "config": {"calendar_by": "Due Date"}},
        {"type": "timeline", "name": f"{ICON_HOURGLASS} Quest Timeline"},
        {"type": "gallery", "name": f"{ICON_CARDS} Quest Cards"},
    ],
    "Skill Trees": [
        {"type": "chart", "name": f"{ICON_CHART} XP Progress", "config": {"chart": "column", "group_by": "Category"}},
        {"type": "list", "name": f"{ICON_CLIPBOARD} Skill Focus"},
        {"type": "board", "name": f"{ICON_SKILL_TREES} Skill Board", "config": {"group_by": "Category"}},
    ],
    "Achievement Log": [
        {"type": "gallery", "name": f"{ICON_ACHIEVEMENT_LOG} Trophy Case"},
    ],
    "Party Board": [
        {"type": "board", "name": f"{ICON_PARTY_BOARD} Hero Roster", "config": {"group_by": "Class"}},
    ],
    "Player Profile": [
        {"type": "dashboard", "name": f"{ICON_CLIPBOARD} Hero Dashboard"},
    ],
    "Adventure Recaps": [
        {"type": "list", "name": f"{ICON_ADVENTURE_RECAPS} Story Archive"},
    ],
    "Runs": [
        {"type": "table", "name": "Run Center"},
    ],
    "Review Queue": [
        {"type": "board", "name": "Review Queue", "config": {"group_by": "Review State"}},
    ],
}


def _expected_props_for_database(db_key: str, base_props: dict, *, is_self_hosted: bool) -> dict:
    expected = dict(base_props)
    if not is_self_hosted:
        expected.update(HOSTED_CONNECTED_PROPS.get(db_key, {}))
    return expected


def _hub_markdown(player_name: str, hosted_views: bool) -> str:
    open_section = (
        "## Open These Views First\n"
        "- **Hero Dashboard** for your current stats\n"
        "- **Quest Ledger** for a clean table of live quests\n"
        "- **Quest Map** for active quest flow\n"
        "- **XP Progress** for visible skill growth\n"
        "- **Run Center** to inspect automation history, timing, and fallback reasons\n"
        "- **Review Queue** to approve, correct, or lock generated content\n"
        "- **Story Archive** for recap pages once your weekly runs begin\n\n"
        if hosted_views else
        "## Open These Databases First\n"
        "- **Quest Board** for active quests\n"
        "- **Player Profile** for hero stats\n"
        "- **Skill Trees** for your current XP totals\n"
        "- **Runs** for runtime history and observability\n"
        "- **Review Queue** for human-in-the-loop checks\n"
        "- **Achievement Log** once badges start unlocking\n\n"
    )
    daily_loop = (
        "## The Daily Loop\n"
        "- Start with `questboard onboard <PAGE_ID_OR_URL>` if you want a guided setup that captures your real goal, constraints, and context\n"
        "- Generate new quests with `questboard quests`\n"
        "- Turn your own task into a quest with `questboard intake \"Task\" --skill Endurance`\n"
        "- Do the task in real life\n"
        "- Mark the quest **Completed** in Notion\n"
        "- Run `questboard sync` to award XP and unlock achievements\n"
        "- Run `questboard runtime --dry-run` to preview the automated control loop\n"
        "- Run `questboard runtime` or `questboard watch --interval 300` to automate sync, patrols, quest top-ups, and recaps\n"
        "- Use `questboard boss` for a high-stakes challenge\n"
        "- Use `questboard recap` for your weekly story\n"
        "- Check **Review Queue** when you want explicit approval and correction loops\n"
    )
    if hosted_views:
        daily_loop += "- Or add a new page directly in **Quest Board** if you prefer Notion-first capture\n"
    daily_loop += "\n"
    return (
        f"# Welcome to QuestBoard, {player_name}!\n\n"
        "Your Notion workspace is now an RPG. Real tasks become quests, progress becomes XP, "
        "and consistency becomes visible.\n\n"
        "## Start Here\n"
        "1. Run `questboard onboard <PAGE_ID_OR_URL>` for a guided personalization flow, or use `questboard calibrate` later.\n"
        "2. Open **Quest Board** to see your current quests.\n"
        "3. Open **Player Profile** to track your level, title, streak, and saved context.\n"
        "4. Open **Skill Trees** to see which real-life abilities you are leveling.\n\n"
        f"{daily_loop}"
        f"{open_section}"
        "## Skill Legend\n"
        "- **Strength**: fitness, exercise, physical health\n"
        "- **Intelligence**: learning, reading, study, courses\n"
        "- **Charisma**: social, networking, communication\n"
        "- **Creativity**: art, writing, music, design\n"
        "- **Endurance**: habits, consistency, discipline\n"
        "- **Wisdom**: reflection, journaling, meditation\n\n"
        "## Rarity Guide\n"
        "- **Common**: quick wins\n"
        "- **Uncommon**: meaningful progress\n"
        "- **Rare**: deeper effort\n"
        "- **Epic**: major milestones\n"
        "- **Legendary**: boss-tier challenges\n"
    )


async def _try_create_view(mcp: NotionMCP, database_id: str, view_type: str, name: str, config: dict | None = None) -> bool:
    try:
        result = await mcp.create_view(database_id, view_type, name, config)
    except Exception as exc:
        console.print(f"[yellow]Skipped view `{name}` ({view_type}): {exc}[/yellow]")
        return False

    if isinstance(result, dict) and result.get("warning"):
        return False
    return True


async def _create_hosted_views_for_database(mcp: NotionMCP, db_ids: dict[str, str], db_key: str) -> int:
    specs = QUESTBOARD_HOSTED_VIEWS.get(db_key, [])
    created = 0
    for spec in specs:
        if await _try_create_view(
            mcp,
            db_ids[db_key],
            spec["type"],
            spec["name"],
            spec.get("config"),
        ):
            created += 1
    return created


async def _create_database_from_blueprint(mcp: NotionMCP, hub_id: str, db_key: str, display_name: str, props: dict, icon: str, desc: str) -> str:
    result = await mcp.create_database(
        parent_id=hub_id,
        title=f"{icon} {display_name}",
        properties=props,
        description=desc,
    )
    return _extract_id(result)


def _extract_data_source_id(database: dict) -> str:
    if not isinstance(database, dict):
        return ""

    for key in ("data_source_id", "default_data_source_id"):
        value = database.get(key)
        if isinstance(value, str) and value:
            return value

    for key in ("data_sources", "dataSources"):
        value = database.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and item.get("id"):
                    return item["id"]

    if database.get("object") == "data_source" and database.get("id"):
        return database["id"]
    if isinstance(database.get("id"), str):
        return database["id"]
    return ""


def _looks_like_mojibake(text: str) -> bool:
    return isinstance(text, str) and any(marker in text for marker in MOJIBAKE_HINTS)


def _reconstruct_mojibake_bytes(text: str) -> bytes:
    raw = bytearray()
    for char in text:
        codepoint = ord(char)
        if codepoint <= 0xFF:
            raw.append(codepoint)
            continue
        try:
            raw.extend(char.encode("cp1252"))
        except UnicodeEncodeError:
            return b""
    return bytes(raw)


def _repair_mojibake_text(text: str) -> str:
    if not _looks_like_mojibake(text):
        return text

    raw = _reconstruct_mojibake_bytes(text)
    if not raw:
        return text
    try:
        repaired = raw.decode("utf-8")
    except UnicodeError:
        return text
    return repaired if repaired != text else text


def _database_title(database: dict) -> str:
    title = database.get("title")
    if isinstance(title, list) and title:
        first = title[0]
        if isinstance(first, dict):
            return str(first.get("plain_text", ""))
    return ""


def _page_text_value(page: dict, name: str) -> str:
    prop = (page.get("properties") or {}).get(name, {})
    if not isinstance(prop, dict):
        return ""

    if isinstance(prop.get("title"), list) and prop["title"]:
        return str(prop["title"][0].get("plain_text", ""))
    if isinstance(prop.get("rich_text"), list) and prop["rich_text"]:
        return str(prop["rich_text"][0].get("plain_text", ""))
    return ""


async def _repair_page_text_fields(mcp: NotionMCP, pages: list[dict], fields: tuple[str, ...]) -> int:
    repaired_pages = 0
    for page in pages:
        page_id = page.get("id")
        if not page_id:
            continue

        updates: dict[str, str] = {}
        for field in fields:
            current = _page_text_value(page, field)
            repaired = _repair_mojibake_text(current)
            if repaired != current:
                updates[field] = repaired

        if updates:
            await mcp.update_page(page_id, updates)
            repaired_pages += 1

    return repaired_pages


def _is_hosted_hub_refresh_protected(exc: Exception) -> bool:
    message = str(exc)
    return (
        "allow_deleting_content" in message
        or ("would delete" in message and "child page" in message)
    )


async def _repair_existing_workspace(mcp: NotionMCP, workspace_state: dict, config) -> dict:
    db_ids = dict(workspace_state.get("databases") or {})
    player_name = workspace_state.get("player_name") or config.player_name
    previous_version = int(workspace_state.get("workspace_version", 0) or 0)
    summary = {
        "fatal": False,
        "hub_updated": False,
        "hub_refresh_skipped": False,
        "schema_updates": [],
        "schema_warnings": [],
        "seeded": [],
        "created_databases": [],
        "created_views": 0,
        "title_repairs": [],
        "page_repairs": [],
        "db_ids": db_ids,
    }

    desired_markdown = _hub_markdown(player_name, hosted_views=not config.is_self_hosted)
    try:
        hub = await mcp.fetch_page(workspace_state["hub_id"])
        if hub.get("content_markdown") != desired_markdown:
            await mcp.update_page(workspace_state["hub_id"], content_markdown=desired_markdown)
            summary["hub_updated"] = True
    except Exception as exc:
        if not config.is_self_hosted and _is_hosted_hub_refresh_protected(exc):
            summary["hub_refresh_skipped"] = True
        else:
            summary["schema_warnings"].append(f"Hub page could not be refreshed: {exc}")

    for db_key, display_name, base_props, icon, desc in DATABASE_BLUEPRINTS:
        expected_props = _expected_props_for_database(db_key, base_props, is_self_hosted=config.is_self_hosted)
        database_id = db_ids.get(db_key)
        if not database_id:
            if previous_version < WORKSPACE_VERSION:
                created_id = await _create_database_from_blueprint(
                    mcp,
                    workspace_state["hub_id"],
                    db_key,
                    display_name,
                    expected_props,
                    icon,
                    desc,
                )
                db_ids[db_key] = created_id
                summary["created_databases"].append(db_key)
                if not config.is_self_hosted:
                    summary["created_views"] += await _create_hosted_views_for_database(mcp, db_ids, db_key)
                continue
            summary["schema_warnings"].append(f"{db_key}: missing database ID in local metadata")
            summary["fatal"] = True
            continue

        try:
            database = await mcp.fetch_page(database_id)
        except Exception as exc:
            if previous_version < WORKSPACE_VERSION:
                created_id = await _create_database_from_blueprint(
                    mcp,
                    workspace_state["hub_id"],
                    db_key,
                    display_name,
                    expected_props,
                    icon,
                    desc,
                )
                db_ids[db_key] = created_id
                summary["created_databases"].append(db_key)
                if not config.is_self_hosted:
                    summary["created_views"] += await _create_hosted_views_for_database(mcp, db_ids, db_key)
                continue
            summary["schema_warnings"].append(f"{db_key}: could not fetch database ({exc})")
            summary["fatal"] = True
            continue

        if not database.get("id"):
            if previous_version < WORKSPACE_VERSION:
                created_id = await _create_database_from_blueprint(
                    mcp,
                    workspace_state["hub_id"],
                    db_key,
                    display_name,
                    expected_props,
                    icon,
                    desc,
                )
                db_ids[db_key] = created_id
                summary["created_databases"].append(db_key)
                if not config.is_self_hosted:
                    summary["created_views"] += await _create_hosted_views_for_database(mcp, db_ids, db_key)
                continue
            summary["schema_warnings"].append(f"{db_key}: database could not be resolved from Notion")
            summary["fatal"] = True
            continue

        current_title = _database_title(database)
        desired_title = f"{icon} {display_name}"
        if current_title != desired_title and _repair_mojibake_text(current_title) == desired_title:
            if config.is_self_hosted:
                summary["schema_warnings"].append(
                    f"{db_key}: detected mojibake title but self-hosted MCP cannot rename databases automatically"
                )
            else:
                data_source_id = _extract_data_source_id(database)
                if data_source_id:
                    try:
                        result = await mcp.update_data_source(data_source_id, title=desired_title)
                        if isinstance(result, dict) and result.get("warning"):
                            summary["schema_warnings"].append(f"{db_key}: {result['warning']}")
                        else:
                            summary["title_repairs"].append(db_key)
                    except Exception as exc:
                        summary["schema_warnings"].append(f"{db_key}: title repair failed ({exc})")

        existing_props = database.get("properties", {}) if isinstance(database, dict) else {}
        missing_props = {name: value for name, value in expected_props.items() if name not in existing_props}
        if not missing_props:
            continue

        if config.is_self_hosted:
            summary["schema_warnings"].append(
                f"{db_key}: missing schema fields ({', '.join(sorted(missing_props))}) on self-hosted MCP"
            )
            continue

        data_source_id = _extract_data_source_id(database)
        if not data_source_id:
            summary["schema_warnings"].append(f"{db_key}: no data source ID available for schema upgrade")
            continue

        try:
            result = await mcp.update_data_source(data_source_id, properties=missing_props)
            if isinstance(result, dict) and result.get("warning"):
                summary["schema_warnings"].append(f"{db_key}: {result['warning']}")
                continue
            summary["schema_updates"].append((db_key, sorted(missing_props)))
        except Exception as exc:
            summary["schema_warnings"].append(f"{db_key}: schema upgrade failed ({exc})")

    player_page = await get_player_page(mcp, db_ids, player_name)
    if not player_page:
        await _seed_player(mcp, db_ids, player_name)
        summary["seeded"].append("player profile")
        player_page = await get_player_page(mcp, db_ids, player_name)

    party_page = await get_party_page(mcp, db_ids, player_name)
    if not party_page:
        await _seed_party_board(mcp, db_ids, player_name)
        summary["seeded"].append("hero roster")
        party_page = await get_party_page(mcp, db_ids, player_name)

    existing_skills = {normalize_skill(page).get("skill") for page in await get_skill_pages(mcp, db_ids)}
    missing_skills = [skill_name for skill_name in SKILL_TREES if skill_name not in existing_skills]
    if missing_skills:
        await _seed_skills(mcp, db_ids, skill_names=missing_skills)
        summary["seeded"].append(f"{len(missing_skills)} missing skill entries")

    quest_pages = await get_quest_pages(mcp, db_ids)
    if not quest_pages:
        await _seed_starter_quests(mcp, db_ids)
        summary["seeded"].append("starter quests")
        quest_pages = await get_quest_pages(mcp, db_ids)

    player_repairs = await _repair_page_text_fields(
        mcp,
        [player_page] if player_page else [],
        ("Name", "Title", "Primary Goal", "Available Time", "Focus Area", "Constraints", "Motivation", "Context Brief", "Context Sources"),
    )
    if player_repairs:
        summary["page_repairs"].append(("Player Profile", player_repairs))

    party_repairs = await _repair_page_text_fields(
        mcp,
        [party_page] if party_page else [],
        ("Adventurer", "Title"),
    )
    if party_repairs:
        summary["page_repairs"].append(("Party Board", party_repairs))

    skill_repairs = await _repair_page_text_fields(
        mcp,
        await get_skill_pages(mcp, db_ids),
        ("Skill", "Description"),
    )
    if skill_repairs:
        summary["page_repairs"].append(("Skill Trees", skill_repairs))

    quest_repairs = await _repair_page_text_fields(
        mcp,
        quest_pages,
        ("Quest", "Description", "Why This Quest"),
    )
    if quest_repairs:
        summary["page_repairs"].append(("Quest Board", quest_repairs))

    return summary


def _print_repair_summary(summary: dict) -> None:
    if not summary:
        return

    if summary.get("fatal"):
        console.print("\n[bold yellow]Saved QuestBoard metadata is incomplete; creating a fresh workspace instead.[/bold yellow]")
        for warning in summary["schema_warnings"]:
            console.print(f"[yellow]- {warning}[/yellow]")
        return

    if (
        not summary["hub_updated"]
        and not summary["hub_refresh_skipped"]
        and not summary["schema_updates"]
        and not summary["schema_warnings"]
        and not summary["seeded"]
    ):
        console.print("\n[bold yellow]Reusing existing QuestBoard workspace from local metadata.[/bold yellow]")
        return

    console.print("\n[bold yellow]Reusing and repairing existing QuestBoard workspace from local metadata.[/bold yellow]")
    if summary["hub_updated"]:
        console.print("[cyan]- Refreshed the QuestBoard hub instructions[/cyan]")
    elif summary["hub_refresh_skipped"]:
        console.print("[cyan]- Kept the existing hub page blocks intact on hosted Notion[/cyan]")
    for db_key in summary.get("title_repairs", []):
        console.print(f"[cyan]- Repaired mojibake database title: {db_key}[/cyan]")
    for db_key in summary.get("created_databases", []):
        console.print(f"[cyan]- Added missing database: {db_key}[/cyan]")
    if summary.get("created_views"):
        console.print(f"[cyan]- Created {summary['created_views']} hosted view(s) for the upgraded workspace[/cyan]")
    for db_key, added_props in summary["schema_updates"]:
        console.print(f"[cyan]- Upgraded {db_key} schema: {', '.join(added_props)}[/cyan]")
    for seeded_item in summary["seeded"]:
        console.print(f"[cyan]- Reseeded {seeded_item}[/cyan]")
    for db_key, count in summary.get("page_repairs", []):
        console.print(f"[cyan]- Repaired mojibake text on {count} page(s) in {db_key}[/cyan]")
    for warning in summary["schema_warnings"]:
        console.print(f"[yellow]- {warning}[/yellow]")


async def setup_workspace(parent_page_id: str, player_name: str = None, force_new: bool = False):
    """Create the full QuestBoard workspace in Notion."""
    config = get_config()
    if player_name:
        config.player_name = player_name

    db_ids = {}

    async with NotionMCP(config) as mcp:
        existing_state = load_workspace_state()
        if (
            existing_state
            and not force_new
            and existing_state.get("parent_page_id") == parent_page_id
            and existing_state.get("server_url") == config.mcp_server_url
        ):
            try:
                existing_hub = await mcp.fetch_page(existing_state["hub_id"])
                if existing_hub.get("id") == existing_state["hub_id"]:
                    summary = await _repair_existing_workspace(mcp, existing_state, config)
                    _print_repair_summary(summary)
                    if summary.get("fatal"):
                        raise RuntimeError("saved workspace metadata is incomplete")
                    repaired_db_ids = summary.get("db_ids") or existing_state["databases"]
                    effective_player_name = existing_state.get("player_name") or config.player_name
                    _save_workspace_state(
                        hub_id=existing_state["hub_id"],
                        db_ids=repaired_db_ids,
                        parent_page_id=parent_page_id,
                        server_url=config.mcp_server_url,
                        player_name=effective_player_name,
                    )
                    return existing_state["hub_id"], repaired_db_ids
            except Exception:
                pass

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            # 1. Create the QuestBoard hub page
            task = progress.add_task("Creating QuestBoard hub page...", total=1)
            hub = await mcp.create_page(
                parent_id=parent_page_id,
                title="QuestBoard",
                icon=ICON_HUB,
                content_markdown=_hub_markdown(config.player_name, hosted_views=not config.is_self_hosted),
            )
            hub_id = _extract_id(hub)
            progress.update(task, completed=1)

            # 2. Create databases
            databases = DATABASE_BLUEPRINTS

            for db_key, display_name, base_props, icon, desc in databases:
                props = _expected_props_for_database(db_key, base_props, is_self_hosted=config.is_self_hosted)
                task = progress.add_task(f"Creating {display_name} database...", total=1)
                db_ids[db_key] = await _create_database_from_blueprint(
                    mcp,
                    hub_id,
                    db_key,
                    display_name,
                    props,
                    icon,
                    desc,
                )
                progress.update(task, completed=1)

            if not config.is_self_hosted:
                view_sections = [
                    ("Quest Board views", "Quest Board"),
                    ("Skill Tree views", "Skill Trees"),
                    ("Achievement views", "Achievement Log"),
                    ("Party views", "Party Board"),
                    ("Player Dashboard", "Player Profile"),
                    ("Recap views", "Adventure Recaps"),
                    ("Run Center", "Runs"),
                    ("Review Queue", "Review Queue"),
                ]

                for label, db_name in view_sections:
                    specs = QUESTBOARD_HOSTED_VIEWS[db_name]
                    task = progress.add_task(f"Creating {label}...", total=len(specs))
                    for spec in specs:
                        await _try_create_view(mcp, db_ids[db_name], spec["type"], spec["name"], spec.get("config"))
                        progress.advance(task)

            # 9. Seed initial data
            task = progress.add_task("Seeding player profile...", total=1)
            await _seed_player(mcp, db_ids, config.player_name)
            progress.update(task, completed=1)

            task = progress.add_task("Seeding party board...", total=1)
            await _seed_party_board(mcp, db_ids, config.player_name)
            progress.update(task, completed=1)

            task = progress.add_task("Seeding skill trees...", total=1)
            await _seed_skills(mcp, db_ids)
            progress.update(task, completed=1)

            task = progress.add_task("Seeding starter quests...", total=1)
            await _seed_starter_quests(mcp, db_ids)
            progress.update(task, completed=1)

    console.print("\n[bold green]\u2705 QuestBoard workspace created![/bold green]")
    console.print(f"[dim]Hub page ID: {hub_id}[/dim]")
    console.print(f"[dim]Databases: {json.dumps({k: v for k, v in db_ids.items()}, indent=2)}[/dim]\n")

    # Save IDs for later use
    _save_workspace_state(
        hub_id=hub_id,
        db_ids=db_ids,
        parent_page_id=parent_page_id,
        server_url=config.mcp_server_url,
        player_name=config.player_name,
    )
    return hub_id, db_ids


async def _seed_player(mcp: NotionMCP, db_ids: dict, name: str):
    """Create the initial player profile."""
    import datetime
    await mcp.create_db_page(db_ids["Player Profile"], {
        "Name": name,
        "Level": 1,
        "Total XP": 0,
        "Title": "Novice Adventurer",
        "Class": "Warrior",
        "HP": 100,
        "Streak Days": 0,
        "Quests Completed": 0,
        "Boss Kills": 0,
        "Joined": datetime.date.today().isoformat(),
        "Primary Goal": "Build momentum and make personal growth visible in Notion.",
        "Available Time": "30-45 minutes on weekdays, 60 minutes on weekends.",
        "Preferred Challenge Style": "Balanced",
        "Focus Area": "Strength and Endurance",
        "Constraints": "Avoid tasks that require special equipment or long setup time.",
        "Motivation": "I stay engaged when progress feels concrete and slightly game-like.",
        "Context Brief": "This hero is using QuestBoard to turn a real goal into visible progress inside Notion.",
        "Context Sources": "Starter seed",
    }, icon=ICON_PLAYER_PROFILE)


async def _seed_skills(mcp: NotionMCP, db_ids: dict, skill_names: list[str] | None = None):
    """Create initial skill tree entries."""
    import datetime
    selected_skills = skill_names or list(SKILL_TREES.keys())
    for skill_name in selected_skills:
        info = SKILL_TREES[skill_name]
        await mcp.create_db_page(db_ids["Skill Trees"], {
            "Skill": f"{info['emoji']} {skill_name}",
            "Current XP": 0,
            "Level": 1,
            "Category": skill_name,
            "Description": info["desc"],
            "Last Activity": datetime.date.today().isoformat(),
            "Quests Completed": 0,
        }, icon=info["emoji"])


async def _seed_party_board(mcp: NotionMCP, db_ids: dict, name: str):
    """Create an initial hero roster row so the party board is not empty."""
    await mcp.create_db_page(db_ids["Party Board"], {
        "Adventurer": name,
        "Level": 1,
        "Total XP": 0,
        "Class": "Warrior",
        "Quests Completed": 0,
        "Current Streak": 0,
        "Title": "Novice Adventurer",
    }, icon=ICON_PARTY_BOARD)


async def _seed_starter_quests(mcp: NotionMCP, db_ids: dict):
    """Create a set of starter quests for the player."""
    import datetime
    today = datetime.date.today()
    week_later = today + datetime.timedelta(days=7)

    starter_quests = [
        {
            "Quest": f"{ICON_WORKOUT} First Steps: Complete a 15-minute workout",
            "Status": "Available",
            "Rarity": "Common",
            "Skill": "Strength",
            "XP Reward": 20,
            "Due Date": week_later.isoformat(),
            "Description": "Every hero's journey begins with a single push-up. Complete any 15-minute workout.",
            "Difficulty": "Easy",
            "Source": "Player",
            "Why This Quest": "A fast opening win to create momentum and make the first XP gain feel immediate.",
            "Generation Mode": "Player",
        },
        {
            "Quest": f"{ICON_BOOKS} Knowledge Seeker: Read for 30 minutes",
            "Status": "Available",
            "Rarity": "Common",
            "Skill": "Intelligence",
            "XP Reward": 25,
            "Due Date": week_later.isoformat(),
            "Description": "A wise hero reads daily. Pick up a book and read for at least 30 minutes.",
            "Difficulty": "Easy",
            "Source": "Player",
            "Why This Quest": "This starter quest builds a visible Intelligence baseline without needing extra setup.",
            "Generation Mode": "Player",
        },
        {
            "Quest": f"{ICON_ART} Creative Spark: Make something with your hands",
            "Status": "Available",
            "Rarity": "Uncommon",
            "Skill": "Creativity",
            "XP Reward": 40,
            "Due Date": week_later.isoformat(),
            "Description": "Draw, paint, code, cook, build \u2014 create something that did not exist before.",
            "Difficulty": "Medium",
            "Source": "Player",
            "Why This Quest": "A mid-tier creative quest keeps the board from feeling one-dimensional and adds variety early.",
            "Generation Mode": "Player",
        },
        {
            "Quest": f"{ICON_SOCIAL} Social Butterfly: Reach out to an old friend",
            "Status": "Available",
            "Rarity": "Uncommon",
            "Skill": "Charisma",
            "XP Reward": 35,
            "Due Date": week_later.isoformat(),
            "Description": "Send a meaningful message to someone you haven't spoken to in a while.",
            "Difficulty": "Medium",
            "Source": "Player",
            "Why This Quest": "A social quest balances the opening board and demonstrates Charisma progression right away.",
            "Generation Mode": "Player",
        },
        {
            "Quest": f"{ICON_MEDITATION} Inner Peace: Meditate for 10 minutes",
            "Status": "Available",
            "Rarity": "Common",
            "Skill": "Wisdom",
            "XP Reward": 20,
            "Due Date": week_later.isoformat(),
            "Description": "Find a quiet place. Close your eyes. Breathe. 10 minutes of stillness.",
            "Difficulty": "Easy",
            "Source": "Player",
            "Why This Quest": "Wisdom quests prevent the system from only rewarding output and intensity.",
            "Generation Mode": "Player",
        },
        {
            "Quest": f"{ICON_RUNNING} Endurance Trial: Maintain a habit for 3 days",
            "Status": "Available",
            "Rarity": "Rare",
            "Skill": "Endurance",
            "XP Reward": 75,
            "Due Date": (today + datetime.timedelta(days=3)).isoformat(),
            "Description": "Pick any habit. Do it for 3 consecutive days. Consistency is power.",
            "Difficulty": "Hard",
            "Source": "Player",
            "Why This Quest": "This longer quest proves the system can reward consistency, not just one-off actions.",
            "Generation Mode": "Player",
        },
    ]

    for quest in starter_quests:
        quest.setdefault("Review State", "Approved")
        quest.setdefault("Correction Notes", "")
        quest.setdefault("Source Run", "starter-seed")
        quest.setdefault("Prompt Version", "starter-seed-v1")
        quest.setdefault("Fallback Reason", "")
        await mcp.create_db_page(db_ids["Quest Board"], quest, icon=ICON_QUEST_BOARD)


WORKSPACE_FILE = os.path.join(os.path.dirname(__file__), "..", ".questboard_workspace.json")


def _save_workspace_state(hub_id: str, db_ids: dict, parent_page_id: str, server_url: str, player_name: str):
    data = {
        "hub_id": hub_id,
        "databases": db_ids,
        "parent_page_id": parent_page_id,
        "server_url": server_url,
        "player_name": player_name,
        "workspace_version": WORKSPACE_VERSION,
    }
    with open(WORKSPACE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_workspace_state() -> dict | None:
    if not os.path.exists(WORKSPACE_FILE):
        return None
    with open(WORKSPACE_FILE) as f:
        return json.load(f)


def load_workspace_ids() -> tuple[str, dict] | None:
    data = load_workspace_state()
    if not data:
        return None
    return data["hub_id"], data["databases"]


def load_workspace_player_name(default: str | None = None) -> str:
    data = load_workspace_state()
    if data and data.get("player_name"):
        return data["player_name"]
    if default:
        return default
    return get_config().player_name


def _extract_id(result: dict) -> str:
    """Extract page/database ID from MCP tool result."""
    if isinstance(result, dict):
        if "id" in result:
            return result["id"]
        if "page_id" in result:
            return result["page_id"]
        if "database_id" in result:
            return result["database_id"]
        if "results" in result and len(result["results"]) > 0:
            return result["results"][0].get("id", "")
        # Try to find ID in nested structure
        for key, val in result.items():
            if isinstance(val, dict) and "id" in val:
                return val["id"]
            if isinstance(val, str) and len(val) == 36 and "-" in val:
                return val
    return str(result)


