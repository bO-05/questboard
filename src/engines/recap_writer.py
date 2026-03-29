"""Weekly adventure recap generator and stale quest detection."""

from __future__ import annotations

import datetime
import json
from collections import defaultdict

from ..audit import (
    build_run_ref,
    llm_model_label,
    log_run,
    queue_review_item,
    review_state_for_mode,
    summarize_exception,
)
from ..config import SKILL_TREES, get_config
from ..mcp_client import NotionMCP
from ..setup_workspace import load_workspace_ids, load_workspace_player_name
from ..workspace_data import (
    filter_properties_for_database,
    get_achievement_pages,
    get_player_page,
    get_quest_pages,
    get_skill_pages,
    normalize_player,
    normalize_quest,
    normalize_skill,
    quests_completed_between,
)
from .llm_provider import get_llm_response
from .xp_engine import calculate_level

RECAP_PROMPT_VERSION = "weekly-recap-v2"
STALE_PATROL_VERSION = "stale-patrol-v1"
RECAP_LLM_MAX_TOKENS = 900


RECAP_PROMPT = """You are the Chronicler of QuestBoard, an RPG-ified life management system.
Write an epic weekly adventure recap for the player. This should read like a chapter from a
fantasy novel, but describe real-world accomplishments.

PLAYER: {player_name}
PERIOD: {start_date} to {end_date}

STATS THIS WEEK:
- Quests completed: {quests_completed}
- XP earned: {xp_earned}
- Levels gained: {levels_gained}
- Current level: {current_level}
- Current title: {current_title}

QUESTS COMPLETED:
{completed_quests}

SKILL PROGRESS:
{skill_progress}

ACHIEVEMENTS UNLOCKED:
{achievements}

Write a 3-5 paragraph narrative recap in RPG/fantasy style. Include:
1. An epic chapter title (e.g., "Chapter XII: The Week of Burning Resolve")
2. Reference specific quests completed as "battles" or "adventures"
3. Mention skill growth as "training" or "mastering new arts"
4. End with a cliffhanger or motivation for next week
5. Include a "Stats Scroll" section at the end with the numbers

Keep it fun, dramatic, and motivating. Max 400 words."""


async def generate_weekly_recap(
    mcp: NotionMCP,
    allow_llm: bool = True,
    *,
    triggered_by: str = "CLI",
) -> dict:
    """Generate and create a weekly adventure recap page."""
    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up.")
    _, db_ids = workspace
    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_ref = build_run_ref("Weekly Recap", started_at)
    fallback_reason = ""

    today = datetime.date.today()
    start_of_week = today - datetime.timedelta(days=today.weekday())
    end_of_week = start_of_week + datetime.timedelta(days=6)

    player_data = await _get_player_data(mcp, player_name)
    completed_quests = await _get_completed_quests_this_week(mcp, start_of_week, end_of_week)
    skill_progress = await _get_skill_progress(mcp, start_of_week, end_of_week)
    achievements = await _get_achievements_this_week(mcp, start_of_week, end_of_week)

    quests_completed = len(completed_quests)
    xp_earned = sum(quest.get("xp", 0) for quest in completed_quests)
    current_level = player_data.get("level", 1)
    start_level = calculate_level(max(0, player_data.get("total_xp", 0) - xp_earned))
    levels_gained = max(0, current_level - start_level)

    prompt = RECAP_PROMPT.format(
        player_name=player_name,
        start_date=start_of_week.isoformat(),
        end_date=end_of_week.isoformat(),
        quests_completed=quests_completed,
        xp_earned=xp_earned,
        levels_gained=levels_gained,
        current_level=current_level,
        current_title=player_data.get("title", "Novice Adventurer"),
        completed_quests=_compact_json(_recap_quest_context(completed_quests)),
        skill_progress=_compact_json(_recap_skill_context(skill_progress)),
        achievements=_compact_json(_recap_achievement_context(achievements)) if achievements else "None this week",
    )

    generation_mode = "llm"
    if allow_llm:
        try:
            narrative = get_llm_response(prompt, max_tokens=RECAP_LLM_MAX_TOKENS)
        except Exception as exc:
            generation_mode = "fallback"
            fallback_reason = summarize_exception(exc)
            narrative = _fallback_recap(
                player_name,
                completed_quests,
                skill_progress,
                achievements,
                start_of_week,
                end_of_week,
                current_level,
                player_data.get("title", "Novice Adventurer"),
                xp_earned,
            )
    else:
        generation_mode = "fallback"
        fallback_reason = "template-only requested"
        narrative = _fallback_recap(
            player_name,
            completed_quests,
            skill_progress,
            achievements,
            start_of_week,
            end_of_week,
            current_level,
            player_data.get("title", "Novice Adventurer"),
            xp_earned,
        )

    mvp_skill = _pick_mvp_skill(skill_progress)
    week_label = f"Week of {start_of_week.strftime('%b %d')} - {end_of_week.strftime('%b %d, %Y')}"

    recap_payload, _ = await filter_properties_for_database(mcp, db_ids["Adventure Recaps"], {
        "Week": f"📖 {week_label}",
        "Period": start_of_week.isoformat(),
        "Quests Completed": quests_completed,
        "XP Earned": xp_earned,
        "Levels Gained": levels_gained,
        "Achievements Unlocked": len(achievements),
        "MVP Skill": mvp_skill,
        "Narrative": narrative[:2000],
        "Generation Mode": "Fallback Template" if generation_mode == "fallback" else "LLM",
        "Review State": review_state_for_mode(generation_mode),
        "Correction Notes": "",
        "Source Run": run_ref,
        "Prompt Version": RECAP_PROMPT_VERSION,
        "Fallback Reason": fallback_reason if generation_mode == "fallback" else "",
    })
    page = await mcp.create_db_page(db_ids["Adventure Recaps"], recap_payload, content_markdown=narrative, icon="📖")

    page_id = _extract_id(page)
    await mcp.create_comment(
        page_id,
        f"📜 *The Chronicler has recorded this chapter in the Book of {player_name}.* "
        f"This week: {quests_completed} quests, {xp_earned} XP earned. "
        f"The saga continues..."
        f"{' [fallback template]' if generation_mode == 'fallback' else ''}"
    )

    await queue_review_item(
        mcp,
        db_ids,
        item=week_label,
        item_type="Adventure Recap",
        source_run=run_ref,
        target_page_id=page_id,
        review_state=review_state_for_mode(generation_mode),
        correction_notes=(
            "Review the recap tone before sharing."
            if generation_mode == "llm"
            else "Fallback recap used. Confirm the narrative still reflects the week."
        ),
        generation_mode=generation_mode,
        fallback_reason=fallback_reason if generation_mode == "fallback" else "",
    )
    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Weekly Recap",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=week_label,
        model=llm_model_label(generation_mode),
        generation_mode=generation_mode,
        fallback_reason=fallback_reason,
        prompt_version=RECAP_PROMPT_VERSION,
        replayable=True,
        records_created=1,
    )

    return {
        "week": week_label,
        "quests_completed": quests_completed,
        "xp_earned": xp_earned,
        "mvp_skill": mvp_skill,
        "narrative_length": len(narrative),
        "generation_mode": generation_mode,
        "source_run": run_ref,
        "review_state": review_state_for_mode(generation_mode),
        "fallback_reason": fallback_reason if generation_mode == "fallback" else "",
    }


async def detect_stale_quests(mcp: NotionMCP, *, triggered_by: str = "CLI") -> list[dict]:
    """Find quests that are overdue or haven't been touched, and add warning comments."""
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up.")
    _, db_ids = workspace

    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_ref = build_run_ref("Stale Patrol", started_at)
    stale = []
    today = datetime.date.today()

    for quest in [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]:
        if quest.get("status") not in ("Available", "In Progress"):
            continue

        due_date_str = quest.get("due_date")
        if not due_date_str:
            continue

        try:
            due_date = datetime.date.fromisoformat(due_date_str)
        except ValueError:
            continue

        days_overdue = (today - due_date).days
        if days_overdue <= 0:
            continue

        page_id = quest.get("id", "")
        await mcp.create_comment(
            page_id,
            f"⚠️ **Quest Overdue!** This quest is {days_overdue} day(s) past its due date. "
            f"The Procrastination Dragon grows stronger with every passing day... "
            f"Complete it now for full XP, or it may expire!"
        )

        expired = days_overdue > 7
        if expired:
            await mcp.update_page(page_id, {"Status": "Expired"})
            await mcp.create_comment(
                page_id,
                "💀 **Quest Expired.** The opportunity has passed. "
                "New quests await - don't let the next one slip away."
            )

        review_updates, _ = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
            "Review State": "Needs Review",
            "Correction Notes": f"Quest is overdue by {days_overdue} day(s). Decide whether to reschedule, complete, or retire it.",
            "Source Run": run_ref,
            "Prompt Version": STALE_PATROL_VERSION,
            "Fallback Reason": "",
        })
        if review_updates:
            await mcp.update_page(page_id, review_updates)
        await queue_review_item(
            mcp,
            db_ids,
            item=quest.get("quest", "Unknown Quest"),
            item_type="Stale Quest",
            source_run=run_ref,
            target_page_id=page_id,
            review_state="Needs Review",
            correction_notes=f"Quest is overdue by {days_overdue} day(s). Decide whether to reschedule, complete, or retire it.",
            generation_mode="operational",
        )

        stale.append({
            "quest": quest.get("quest", "Unknown Quest"),
            "days_overdue": days_overdue,
            "expired": expired,
        })

    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Stale Patrol",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=f"{len(stale)} stale quest(s)",
        model="",
        generation_mode="operational",
        fallback_reason="",
        prompt_version=STALE_PATROL_VERSION,
        replayable=True,
        records_updated=len(stale),
    )
    return stale


async def _get_player_data(mcp: NotionMCP, name: str) -> dict:
    workspace = load_workspace_ids()
    _, db_ids = workspace
    page = await get_player_page(mcp, db_ids, name)
    if page:
        return normalize_player(page)
    return {"level": 1, "total_xp": 0, "title": "Novice Adventurer", "levels_gained": 0}


async def _get_completed_quests_this_week(mcp: NotionMCP, start_date: datetime.date, end_date: datetime.date) -> list[dict]:
    workspace = load_workspace_ids()
    _, db_ids = workspace
    quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
    completed = quests_completed_between(quests, start_date, end_date)
    return [{
        "name": quest.get("quest", "Quest"),
        "skill": quest.get("skill", ""),
        "xp": quest.get("awarded_xp", quest.get("xp_reward", 0)),
        "completed_at": quest.get("completed_at"),
    } for quest in completed]


async def _get_skill_progress(mcp: NotionMCP, start_date: datetime.date, end_date: datetime.date) -> list[dict]:
    workspace = load_workspace_ids()
    _, db_ids = workspace
    skills = {entry["skill"]: entry for entry in [normalize_skill(page) for page in await get_skill_pages(mcp, db_ids)]}
    quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
    completed = quests_completed_between(quests, start_date, end_date)

    xp_by_skill: defaultdict[str, int] = defaultdict(int)
    for quest in completed:
        skill = quest.get("skill")
        if skill:
            xp_by_skill[skill] += quest.get("awarded_xp", quest.get("xp_reward", 0))

    progress = []
    for skill_name in SKILL_TREES:
        entry = skills.get(skill_name, {"skill": skill_name, "xp": 0, "level": 1})
        progress.append({
            "skill": skill_name,
            "total_xp": entry.get("xp", 0),
            "level": entry.get("level", 1),
            "xp_this_week": xp_by_skill.get(skill_name, 0),
        })
    return progress


async def _get_achievements_this_week(mcp: NotionMCP, start_date: datetime.date, end_date: datetime.date) -> list[dict]:
    workspace = load_workspace_ids()
    _, db_ids = workspace

    achievements = []
    for page in await get_achievement_pages(mcp, db_ids):
        unlocked_at = _extract_date(page.get("properties", {}), "Unlocked At")
        if not unlocked_at:
            continue
        try:
            unlocked_date = datetime.date.fromisoformat(unlocked_at)
        except ValueError:
            continue
        if not start_date <= unlocked_date <= end_date:
            continue
        achievements.append({
            "achievement": _extract_title(page.get("properties", {}), "Achievement", "Achievement"),
            "bonus_xp": _extract_number(page.get("properties", {}), "XP Bonus", 0),
            "rarity": _extract_select(page.get("properties", {}), "Rarity", "Bronze"),
        })
    return achievements


def _recap_quest_context(completed_quests: list[dict]) -> list[dict]:
    return [
        {
            "name": quest.get("name", ""),
            "skill": quest.get("skill", ""),
            "xp": quest.get("xp", 0),
        }
        for quest in completed_quests[:10]
    ]


def _recap_skill_context(skill_progress: list[dict]) -> list[dict]:
    return [
        {
            "skill": item.get("skill", ""),
            "level": item.get("level", 1),
            "xp_this_week": item.get("xp_this_week", 0),
        }
        for item in skill_progress
    ]


def _recap_achievement_context(achievements: list[dict]) -> list[dict]:
    return [
        {
            "achievement": item.get("achievement", ""),
            "bonus_xp": item.get("bonus_xp", 0),
            "rarity": item.get("rarity", ""),
        }
        for item in achievements[:8]
    ]


def _compact_json(value: dict | list) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _pick_mvp_skill(skill_progress: list[dict]) -> str:
    if not skill_progress:
        return "Endurance"

    ranked = max(skill_progress, key=lambda skill: (skill.get("xp_this_week", 0), skill.get("total_xp", 0)))
    return ranked.get("skill", "Endurance")


def _fallback_recap(
    player_name: str,
    completed_quests: list[dict],
    skill_progress: list[dict],
    achievements: list[dict],
    start_date: datetime.date,
    end_date: datetime.date,
    current_level: int,
    current_title: str,
    xp_earned: int,
) -> str:
    quest_lines = completed_quests[:3]
    quest_summary = "\n".join(
        f"- {quest['name']} ({quest['skill']}, +{quest['xp']} XP)" for quest in quest_lines
    ) or "- No completed quests yet."

    training_summary = "\n".join(
        f"- {item['skill']}: +{item['xp_this_week']} XP this week, total level {item['level']}"
        for item in skill_progress if item.get("xp_this_week", 0) > 0
    ) or "- Your skills wait for their next trial."

    achievement_summary = "\n".join(
        f"- {item['achievement']} (+{item['bonus_xp']} XP)" for item in achievements
    ) or "- No achievements unlocked this week."

    return (
        f"# Chapter: The Week of Steady Momentum\n\n"
        f"{player_name} closed the chapter spanning {start_date.isoformat()} to {end_date.isoformat()} "
        f"with {len(completed_quests)} completed quests and {xp_earned} XP earned. "
        f"The path remains demanding, but the hero now stands at Level {current_level} as a {current_title}.\n\n"
        f"## Deeds Recorded\n{quest_summary}\n\n"
        f"## Training Grounds\n{training_summary}\n\n"
        f"## Honors\n{achievement_summary}\n\n"
        f"## Stats Scroll\n"
        f"- Quests completed: {len(completed_quests)}\n"
        f"- XP earned: {xp_earned}\n"
        f"- Current level: {current_level}\n"
        f"- Current title: {current_title}\n\n"
        f"The next week already stirs beyond the horizon. New trials are coming."
    )


def _extract_id(result: dict) -> str:
    if isinstance(result, dict):
        for key in ("id", "page_id"):
            if key in result:
                return result[key]
    return str(result)


def _extract_number(props: dict, name: str, default: int = 0) -> int:
    prop = props.get(name, {})
    if isinstance(prop, dict) and "number" in prop:
        return prop["number"] if prop["number"] is not None else default
    return default


def _extract_select(props: dict, name: str, default: str = "") -> str:
    prop = props.get(name, {})
    if isinstance(prop, dict):
        if "select" in prop and prop["select"]:
            return prop["select"].get("name", default)
        if "status" in prop and prop["status"]:
            return prop["status"].get("name", default)
    return default


def _extract_title(props: dict, name: str, default: str = "") -> str:
    prop = props.get(name, {})
    if isinstance(prop, dict) and "title" in prop and prop["title"]:
        return prop["title"][0].get("plain_text", default)
    return default


def _extract_date(props: dict, name: str) -> str | None:
    prop = props.get(name, {})
    if isinstance(prop, dict) and "date" in prop and prop["date"]:
        return prop["date"].get("start")
    return None
