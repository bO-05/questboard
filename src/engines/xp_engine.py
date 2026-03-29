"""XP calculation, level-up detection, and skill tree progression."""

from __future__ import annotations

import datetime
import re

from ..audit import build_run_ref, log_run
from ..config import DIFFICULTY_MULTIPLIERS, LEVEL_THRESHOLDS, get_config
from ..mcp_client import NotionMCP
from ..setup_workspace import load_workspace_ids, load_workspace_player_name
from ..workspace_data import (
    calculate_streak,
    filter_properties_for_database,
    get_achievement_pages,
    get_party_page,
    get_player_page,
    get_quest_pages,
    get_skill_page,
    get_skill_pages,
    normalize_player,
    normalize_quest,
    normalize_skill,
)


def calculate_level(total_xp: int) -> int:
    """Determine level from total XP."""
    for level, threshold in enumerate(LEVEL_THRESHOLDS):
        if total_xp < threshold:
            return max(1, level)
    return len(LEVEL_THRESHOLDS)


def xp_to_next_level(total_xp: int) -> int:
    """XP remaining until next level."""
    current_level = calculate_level(total_xp)
    if current_level >= len(LEVEL_THRESHOLDS):
        return 0
    return LEVEL_THRESHOLDS[current_level] - total_xp


def get_title_for_level(level: int) -> str:
    """Generate a title based on level."""
    titles = {
        1: "Novice Adventurer",
        3: "Apprentice Hero",
        5: "Journeyman Warrior",
        7: "Seasoned Quester",
        10: "Veteran Champion",
        13: "Elite Vanguard",
        15: "Master Pathfinder",
        18: "Grandmaster Legend",
        20: "Mythic Overlord",
    }
    result = "Novice Adventurer"
    for lvl, title in sorted(titles.items()):
        if level >= lvl:
            result = title
    return result


def _extract_completion_metadata_from_comments(comments: list[dict], fallback_xp: int) -> tuple[str, int] | None:
    for comment in comments:
        text = str(comment.get("text", ""))
        if not text and isinstance(comment.get("rich_text"), list) and comment["rich_text"]:
            text = str(comment["rich_text"][0].get("plain_text", ""))
        if "Quest completed!" not in text:
            continue
        xp_match = re.search(r"Earned[^0-9]*(\d+)\s*XP", text)
        awarded_xp = int(xp_match.group(1)) if xp_match else fallback_xp
        timestamp = str(comment.get("datetime", "") or comment.get("created_time", "")).strip()
        completed_at = timestamp[:10] or datetime.date.today().isoformat()
        return completed_at, awarded_xp
    return None


async def repair_completed_quest_metadata(mcp: NotionMCP) -> list[dict]:
    """Backfill quest completion metadata for legacy hosted runs without re-awarding XP."""
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    repaired: list[dict] = []
    for item in [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]:
        if item.get("status") != "Completed" or item.get("completed_at"):
            continue
        page_id = item.get("id", "")
        if not page_id or not hasattr(mcp, "get_comments"):
            continue
        try:
            comments = await mcp.get_comments(page_id)
        except Exception:
            continue
        metadata = _extract_completion_metadata_from_comments(comments, item.get("xp_reward", 0))
        if not metadata:
            continue
        completed_at, awarded_xp = metadata
        updates, _ = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
            "Completed At": completed_at,
            "Awarded XP": awarded_xp,
        })
        if not updates:
            continue
        await mcp.update_page(page_id, updates)
        repaired.append({
            "quest_id": page_id,
            "quest_name": item.get("quest", "Unknown Quest"),
            "completed_at": completed_at,
            "awarded_xp": awarded_xp,
        })

    return repaired


async def complete_quest(mcp: NotionMCP, quest_page_id: str) -> dict:
    """Mark a quest as completed and award XP. Returns level-up info."""
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    quest = await mcp.fetch_page(quest_page_id)
    quest_name = _extract_title(quest, "Quest", "Unknown Quest")
    existing_status = _extract_select(quest, "Status", "Available")
    existing_completed_at = _extract_date(quest, "Completed At")
    if existing_status == "Completed" and existing_completed_at:
        return {
            "already_completed": True,
            "quest_name": quest_name,
            "xp_earned": 0,
            "skill": _extract_select(quest, "Skill", "Endurance"),
            "new_total_xp": 0,
            "new_level": 0,
            "leveled_up": False,
            "achievements_unlocked": [],
            "bonus_xp": 0,
            "boss_kill": False,
            "streak_days": 0,
        }

    quest_xp = _extract_number(quest, "XP Reward", 20)
    quest_skill = _extract_select(quest, "Skill", "Endurance")
    quest_rarity = _extract_select(quest, "Rarity", "Common")
    quest_source = _extract_select(quest, "Source", "")
    quest_difficulty = _extract_select(quest, "Difficulty", "Medium")

    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    multiplier = DIFFICULTY_MULTIPLIERS.get(config.difficulty, 1.0)
    rarity_bonus = {"Common": 1.0, "Uncommon": 1.1, "Rare": 1.25, "Epic": 1.5, "Legendary": 2.0}
    final_xp = int(quest_xp * multiplier * rarity_bonus.get(quest_rarity, 1.0))
    today = datetime.date.today().isoformat()

    quest_updates, _ = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
        "Status": "Completed",
        "Completed At": today,
        "Awarded XP": final_xp,
        "Review State": "Locked",
    })
    await mcp.update_page(quest_page_id, quest_updates)
    try:
        from ..operations import sync_review_items_for_target

        await sync_review_items_for_target(
            mcp,
            quest_page_id,
            review_state="Locked",
            locked=True,
            approved_at=today,
        )
    except Exception:
        pass

    player_page = await get_player_page(mcp, db_ids, player_name)
    player_id = player_page.get("id") if player_page else None
    old_xp = 0
    old_level = 1
    old_quests = 0
    old_bosses = 0
    streak_days = 0
    achievements: list[dict] = []
    bonus_xp = 0
    boss_kill = quest_source == "Boss Battle" or quest_difficulty == "Boss"
    final_total_xp = final_xp
    final_level = 1
    final_title = get_title_for_level(1)
    leveled_up = False

    if player_id:
        player_state = normalize_player(player_page)
        old_xp = player_state["total_xp"]
        old_level = player_state["level"]
        old_quests = player_state["quests_completed"]
        old_bosses = player_state["boss_kills"]

        base_total_xp = old_xp + final_xp
        base_level = calculate_level(base_total_xp)
        achievements = _build_achievements(base_level, old_level, old_quests + 1, today)
        bonus_xp = sum(achievement["XP Bonus"] for achievement in achievements)
        final_total_xp = base_total_xp + bonus_xp
        final_level = calculate_level(final_total_xp)
        final_title = get_title_for_level(final_level)
        leveled_up = final_level > old_level

        completed_quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
        completed_quests.append({
            "quest": quest_name,
            "status": "Completed",
            "skill": quest_skill,
            "completed_at": today,
        })
        streak_days = calculate_streak(completed_quests)

        player_updates, _ = await filter_properties_for_database(mcp, db_ids["Player Profile"], {
            "Total XP": final_total_xp,
            "Level": final_level,
            "Title": final_title,
            "Quests Completed": old_quests + 1,
            "Boss Kills": old_bosses + (1 if boss_kill else 0),
            "Streak Days": streak_days,
        })
        await mcp.update_page(player_id, player_updates)

        if leveled_up:
            await mcp.create_comment(
                player_id,
                f"🎉 LEVEL UP! {player_name} reached Level {final_level}! "
                f"New title: **{final_title}**. Keep questing, hero!"
            )

        for achievement in achievements:
            await mcp.create_comment(
                player_id,
                f"🏆 Achievement unlocked: **{achievement['Achievement']}** (+{achievement['XP Bonus']} XP)"
            )

    skill_page = await get_skill_page(mcp, db_ids, quest_skill)
    if skill_page and skill_page.get("id"):
        skill_xp = _extract_number(skill_page, "Current XP", 0)
        skill_quests = _extract_number(skill_page, "Quests Completed", 0)
        new_skill_xp = skill_xp + final_xp
        skill_updates, _ = await filter_properties_for_database(mcp, db_ids["Skill Trees"], {
            "Current XP": new_skill_xp,
            "Level": calculate_level(new_skill_xp),
            "Last Activity": today,
            "Quests Completed": skill_quests + 1,
        })
        await mcp.update_page(skill_page["id"], skill_updates)

    party_page = await get_party_page(mcp, db_ids, player_name)
    if party_page and player_id:
        party_updates, _ = await filter_properties_for_database(mcp, db_ids["Party Board"], {
            "Level": final_level,
            "Total XP": final_total_xp,
            "Quests Completed": old_quests + 1,
            "Current Streak": streak_days,
            "Title": final_title,
        })
        await mcp.update_page(party_page.get("id", ""), party_updates)

    await mcp.create_comment(
        quest_page_id,
        f"✅ Quest completed! Earned **{final_xp} XP** "
        f"(base: {quest_xp}, rarity: {quest_rarity}, difficulty: {config.difficulty}). "
        f"Skill: {quest_skill}."
    )

    if achievements:
        await _save_achievements(mcp, db_ids, achievements)

    return {
        "already_completed": False,
        "quest_name": quest_name,
        "xp_earned": final_xp,
        "skill": quest_skill,
        "new_total_xp": final_total_xp,
        "new_level": final_level,
        "leveled_up": leveled_up,
        "achievements_unlocked": [achievement["Achievement"] for achievement in achievements],
        "bonus_xp": bonus_xp,
        "boss_kill": boss_kill,
        "streak_days": streak_days,
    }


def _build_achievements(new_level: int, old_level: int, total_quests: int, today: str) -> list[dict]:
    """Build achievement payloads based on the updated player state."""
    achievements = []

    quest_milestones = {1: "First Blood", 5: "Questaholic", 10: "Decadent", 25: "Quarter Century", 50: "Half-Centurion", 100: "Centurion"}
    if total_quests in quest_milestones:
        achievements.append({
            "Achievement": f"🎖️ {quest_milestones[total_quests]}",
            "Description": f"Completed {total_quests} quests!",
            "Unlocked At": today,
            "XP Bonus": total_quests * 5,
            "Rarity": "Bronze" if total_quests <= 5 else "Silver" if total_quests <= 25 else "Gold",
            "Category": "Quest Milestones",
        })

    level_milestones = {5: "Rising Star", 10: "Veteran", 15: "Master", 20: "Mythic"}
    if new_level > old_level and new_level in level_milestones:
        achievements.append({
            "Achievement": f"⭐ {level_milestones[new_level]}",
            "Description": f"Reached Level {new_level}!",
            "Unlocked At": today,
            "XP Bonus": new_level * 20,
            "Rarity": "Silver" if new_level <= 10 else "Gold" if new_level <= 15 else "Diamond",
            "Category": "Skill Mastery",
        })

    return achievements


async def _save_achievements(mcp: NotionMCP, db_ids: dict, achievements: list[dict]):
    """Persist achievements in Notion."""
    for achievement in achievements:
        achievement_payload, _ = await filter_properties_for_database(mcp, db_ids["Achievement Log"], achievement)
        await mcp.create_db_page(db_ids["Achievement Log"], achievement_payload, icon="🏆")


def _extract_number(page_data: dict, prop_name: str, default: int = 0) -> int:
    """Extract a number property from page data."""
    try:
        props = page_data.get("properties", {})
        prop = props.get(prop_name, {})
        if isinstance(prop, dict) and "number" in prop:
            return prop["number"] if prop["number"] is not None else default
        return default
    except (KeyError, TypeError):
        return default


def _extract_select(page_data: dict, prop_name: str, default: str = "") -> str:
    """Extract a select-like property from page data."""
    try:
        props = page_data.get("properties", {})
        prop = props.get(prop_name, {})
        if isinstance(prop, dict):
            if "select" in prop and prop["select"]:
                return prop["select"].get("name", default)
            if "status" in prop and prop["status"]:
                return prop["status"].get("name", default)
            if "rich_text" in prop and prop["rich_text"]:
                return prop["rich_text"][0].get("plain_text", default)
        return default
    except (KeyError, TypeError):
        return default


def _extract_title(page_data: dict, prop_name: str, default: str = "") -> str:
    """Extract a title property from page data."""
    try:
        props = page_data.get("properties", {})
        prop = props.get(prop_name, {})
        if isinstance(prop, dict) and "title" in prop and prop["title"]:
            return prop["title"][0].get("plain_text", default)
        return default
    except (KeyError, TypeError):
        return default


def _extract_text(page_data: dict, prop_name: str, default: str = "") -> str:
    """Extract a rich-text property from page data."""
    try:
        props = page_data.get("properties", {})
        prop = props.get(prop_name, {})
        if isinstance(prop, dict) and "rich_text" in prop and prop["rich_text"]:
            return prop["rich_text"][0].get("plain_text", default)
        return default
    except (KeyError, TypeError):
        return default


def _extract_date(page_data: dict, prop_name: str) -> str | None:
    """Extract a date property from page data."""
    try:
        props = page_data.get("properties", {})
        prop = props.get(prop_name, {})
        if isinstance(prop, dict) and "date" in prop and prop["date"]:
            return prop["date"].get("start")
    except (KeyError, TypeError):
        return None
    return None


async def sync_completed_quests(mcp: NotionMCP, *, triggered_by: str = "CLI") -> list[dict]:
    """Find quests marked Completed in Notion and award XP for any not yet processed."""
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_ref = build_run_ref("Quest Sync", started_at)
    repaired_ids = {entry["quest_id"] for entry in await repair_completed_quest_metadata(mcp)}
    processed = []
    for item in [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]:
        if item.get("status") != "Completed":
            continue
        page_id = item.get("id", "")
        if page_id in repaired_ids:
            continue
        if item.get("completed_at"):
            continue
        if not page_id:
            continue
        try:
            result = await complete_quest(mcp, page_id)
        except Exception:
            continue
        if result.get("already_completed"):
            continue
        processed.append({
            "quest_name": item.get("quest", "Unknown Quest"),
            "skill": result.get("skill", "Unknown"),
            "xp_earned": result.get("xp_earned", 0),
            "leveled_up": result.get("leveled_up", False),
            "new_level": result.get("new_level", 1),
            "new_total_xp": result.get("new_total_xp", 0),
            "achievements_unlocked": result.get("achievements_unlocked", []),
            "streak_days": result.get("streak_days", 0),
        })

    await reconcile_progress_state(mcp)

    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Quest Sync",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=f"{len(processed)} synced / {len(repaired_ids)} repaired",
        model="",
        generation_mode="operational",
        fallback_reason="",
        prompt_version="quest-sync-v1",
        replayable=True,
        records_updated=len(processed) + len(repaired_ids),
    )
    return processed


def _effective_completed_xp(quest: dict) -> int:
    return int(quest.get("awarded_xp") or quest.get("live_xp") or quest.get("xp_reward") or 0)


async def reconcile_progress_state(mcp: NotionMCP) -> dict:
    """Rebuild player, party, and skill totals from the Quest Board as the source of truth."""
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    player_page = await get_player_page(mcp, db_ids, player_name)
    party_page = await get_party_page(mcp, db_ids, player_name)
    skill_pages = await get_skill_pages(mcp, db_ids)
    achievement_pages = await get_achievement_pages(mcp, db_ids)
    quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
    completed_quests = [quest for quest in quests if quest.get("status") == "Completed"]

    quest_xp = sum(_effective_completed_xp(quest) for quest in completed_quests)
    achievement_bonus = 0
    for page in achievement_pages:
        achievement_bonus += _extract_number(page, "XP Bonus", 0)
    total_xp = quest_xp + achievement_bonus
    quests_completed = len(completed_quests)
    boss_kills = sum(
        1
        for quest in completed_quests
        if quest.get("difficulty") == "Boss" or quest.get("source") == "Boss Battle" or quest.get("boss_completion_value")
    )
    streak_days = calculate_streak(completed_quests)
    level = calculate_level(total_xp)
    title = get_title_for_level(level)

    player_updated = False
    party_updated = False
    skill_updates = 0

    if player_page and player_page.get("id"):
        current_player = normalize_player(player_page)
        desired_player = {
            "Total XP": total_xp,
            "Level": level,
            "Title": title,
            "Quests Completed": quests_completed,
            "Boss Kills": boss_kills,
            "Streak Days": streak_days,
        }
        changed_player = {
            key: value
            for key, value in desired_player.items()
            if (
                (key == "Total XP" and current_player.get("total_xp") != value)
                or (key == "Level" and current_player.get("level") != value)
                or (key == "Title" and current_player.get("title") != value)
                or (key == "Quests Completed" and current_player.get("quests_completed") != value)
                or (key == "Boss Kills" and current_player.get("boss_kills") != value)
                or (key == "Streak Days" and current_player.get("streak_days") != value)
            )
        }
        if changed_player:
            player_updates, _ = await filter_properties_for_database(mcp, db_ids["Player Profile"], changed_player)
            if player_updates:
                await mcp.update_page(player_page["id"], player_updates)
                player_updated = True

    if party_page and party_page.get("id"):
        party_updates_values = {
            "Level": level,
            "Total XP": total_xp,
            "Quests Completed": quests_completed,
            "Current Streak": streak_days,
            "Title": title,
        }
        changed_party = {
            key: value
            for key, value in party_updates_values.items()
            if (
                (key == "Level" and _extract_number(party_page, "Level", 1) != value)
                or (key == "Total XP" and _extract_number(party_page, "Total XP", 0) != value)
                or (key == "Quests Completed" and _extract_number(party_page, "Quests Completed", 0) != value)
                or (key == "Current Streak" and _extract_number(party_page, "Current Streak", 0) != value)
                or (key == "Title" and _extract_text(party_page, "Title", "") != value)
            )
        }
        if changed_party:
            party_updates, _ = await filter_properties_for_database(mcp, db_ids["Party Board"], changed_party)
            if party_updates:
                await mcp.update_page(party_page["id"], party_updates)
                party_updated = True

    normalized_skills = {normalize_skill(page).get("skill", ""): page for page in skill_pages}
    for skill_name, skill_page in normalized_skills.items():
        skill_quests = [quest for quest in completed_quests if quest.get("skill") == skill_name]
        skill_xp = sum(_effective_completed_xp(quest) for quest in skill_quests)
        skill_level = calculate_level(skill_xp)
        last_activity = ""
        dated = [quest.get("completed_at", "") for quest in skill_quests if quest.get("completed_at")]
        if dated:
            last_activity = max(dated)
        current_skill = normalize_skill(skill_page)
        changed_skill = {
            "Current XP": skill_xp,
            "Level": skill_level,
            "Quests Completed": len(skill_quests),
            "Last Activity": last_activity or current_skill.get("last_activity"),
        }
        filtered_skill_changes = {
            key: value
            for key, value in changed_skill.items()
            if (
                (key == "Current XP" and current_skill.get("xp") != value)
                or (key == "Level" and current_skill.get("level") != value)
                or (key == "Quests Completed" and current_skill.get("quests_completed") != value)
                or (key == "Last Activity" and value and current_skill.get("last_activity") != value)
            )
        }
        if filtered_skill_changes:
            skill_updates_payload, _ = await filter_properties_for_database(mcp, db_ids["Skill Trees"], filtered_skill_changes)
            if skill_updates_payload:
                await mcp.update_page(skill_page["id"], skill_updates_payload)
                skill_updates += 1

    return {
        "player_updated": player_updated,
        "party_updated": party_updated,
        "skill_updates": skill_updates,
        "total_xp": total_xp,
        "quests_completed": quests_completed,
        "boss_kills": boss_kills,
        "streak_days": streak_days,
        "level": level,
        "title": title,
    }
