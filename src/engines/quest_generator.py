"""AI-powered quest generation based on player behavior and neglected skills."""

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
from ..config import SKILL_TREES, QUEST_RARITIES, get_config
from ..mcp_client import NotionMCP
from ..setup_workspace import load_workspace_ids, load_workspace_player_name
from ..workspace_data import (
    filter_properties_for_database,
    get_player_page,
    get_quest_pages,
    get_skill_pages,
    normalize_player,
    normalize_quest,
    normalize_skill,
)
from .llm_provider import get_llm_response, parse_json_response

QUEST_PROMPT_VERSION = "quest-generator-v3"
BOSS_PROMPT_VERSION = "boss-generator-v2"
QUEST_LLM_MAX_TOKENS = 1200
QUEST_REPAIR_MAX_TOKENS = 900
BOSS_LLM_MAX_TOKENS = 700
BOSS_REPAIR_MAX_TOKENS = 700


QUEST_GENERATION_PROMPT = """You are the Quest Master of QuestBoard, an RPG-ified life management system.

Analyze the player's stats and generate {count} new quests. Each quest should be a real-world task
that helps the player improve in areas they've been neglecting.

PLAYER STATS:
{player_stats}

PLAYER PREFERENCES:
{player_preferences}

SKILL TREE STATUS:
{skill_stats}

RECENT QUEST HISTORY:
{recent_quests}

RULES:
1. Prioritize skills with LOW XP or NO recent activity — these are neglected areas
2. Each quest must be a specific, actionable real-world task (not vague)
3. Mix difficulties: include some easy wins and some challenges
4. Assign rarity based on difficulty and impact
5. Write quest names in RPG style with an emoji prefix
6. Include a flavorful description (1-2 sentences, RPG-themed)
7. XP rewards should match the effort: Easy=10-30, Medium=30-60, Hard=60-100, Boss=100-200
8. Set due dates between 1-7 days from now
9. Add a short "why_this_quest" explanation grounded in neglected skills, player preferences, or recent gaps

Return a JSON array of quests. Each quest object:
{{
  "quest": "emoji + RPG-style quest name",
  "skill": "one of: {skills}",
  "rarity": "one of: Common, Uncommon, Rare, Epic, Legendary",
  "xp_reward": number,
  "difficulty": "one of: Easy, Medium, Hard",
  "description": "1-2 sentence flavorful description",
  "why_this_quest": "short explanation of why this quest is a good fit right now",
  "due_days": number (1-7)
}}

Return ONLY the JSON array, no other text."""


JSON_REPAIR_PROMPT = """Convert the following model output into valid strict JSON only.

Target shape:
{schema_hint}

Original output:
{response_text}

Return only corrected JSON. No commentary."""


async def generate_quests(
    mcp: NotionMCP,
    count: int = 5,
    allow_llm: bool = True,
    *,
    triggered_by: str = "CLI",
) -> list[dict]:
    """Generate AI-powered quests based on player behavior."""
    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace
    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_ref = build_run_ref("Quest Generation", started_at)
    fallback_reason = ""

    # Gather player context
    player_stats = await _get_player_stats(mcp, db_ids, player_name)
    skill_stats = await _get_skill_stats(mcp, db_ids)
    recent_quests = await _get_recent_quests(mcp, db_ids)

    # Generate quests via configured LLM provider
    prompt = QUEST_GENERATION_PROMPT.format(
        count=count,
        player_stats=_compact_json(_quest_player_context(player_stats)),
        player_preferences=_compact_json(_player_preferences(player_stats)),
        skill_stats=_compact_json(_quest_skill_context(skill_stats)),
        recent_quests=_compact_json(_quest_history_context(recent_quests)),
        skills=", ".join(SKILL_TREES.keys()),
    )

    quests = []
    if allow_llm:
        try:
            response_text = get_llm_response(prompt, max_tokens=QUEST_LLM_MAX_TOKENS)
            quests = _parse_json_with_repair(
                response_text,
                "JSON array of quest objects with quest, skill, rarity, xp_reward, difficulty, description, why_this_quest, due_days",
                max_tokens=QUEST_REPAIR_MAX_TOKENS,
            )
        except Exception as exc:
            fallback_reason = summarize_exception(exc)
            quests = []
    else:
        fallback_reason = "template-only requested"

    if not isinstance(quests, list):
        quests = [quests] if isinstance(quests, dict) else []
        fallback_reason = fallback_reason or "LLM returned an unsupported payload shape"

    normalized = [_normalize_generated_quest(q) for q in quests]
    normalized = [q for q in normalized if q]
    for quest in normalized:
        quest["generation_mode"] = "llm"
    if len(normalized) < count:
        fallback_reason = fallback_reason or "LLM output was incomplete or malformed"
        fallback_quests = _fallback_quests(
            skill_stats,
            recent_quests,
            player_stats,
            count - len(normalized),
        )
        for quest in fallback_quests:
            quest["generation_mode"] = "fallback"
            quest["fallback_reason"] = fallback_reason
        normalized.extend(fallback_quests)
    quests = normalized[:count]
    if any(q.get("generation_mode") == "fallback" for q in quests) and any(q.get("generation_mode") == "llm" for q in quests):
        run_generation_mode = "hybrid"
    elif any(q.get("generation_mode") == "fallback" for q in quests):
        run_generation_mode = "fallback"
    else:
        run_generation_mode = "llm"

    # Create quests in Notion
    today = datetime.date.today()
    created = []
    for q in quests:
        due_date = today + datetime.timedelta(days=q.get("due_days", 7))
        quest_payload, _ = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
            "Quest": q["quest"],
            "Status": "Available",
            "Rarity": q.get("rarity", "Common"),
            "Skill": q["skill"],
            "XP Reward": q.get("xp_reward", 20),
            "Due Date": due_date.isoformat(),
            "Description": q.get("description", ""),
            "Difficulty": q.get("difficulty", "Medium"),
            "Source": "AI Generated",
            "Why This Quest": q.get("why_this_quest", ""),
            "Generation Mode": _generation_mode_label(q.get("generation_mode", "llm")),
            "Review State": review_state_for_mode(q.get("generation_mode", "llm")),
            "Correction Notes": "",
            "Source Run": run_ref,
            "Prompt Version": QUEST_PROMPT_VERSION,
            "Fallback Reason": q.get("fallback_reason", fallback_reason if q.get("generation_mode") == "fallback" else ""),
        })
        page = await mcp.create_db_page(db_ids["Quest Board"], quest_payload, icon="📜")

        # Add flavor comment
        mode_text = " [fallback template]" if q.get("generation_mode") == "fallback" else ""
        await mcp.create_comment(
            _extract_id(page),
            f"🗡️ *The Quest Master speaks{mode_text}:* \"{q.get('description', 'A new challenge awaits...')}\" "
            f"— Reward: {q.get('xp_reward', 20)} XP ({q.get('rarity', 'Common')}). "
            f"Why now: {q.get('why_this_quest', 'It matches your current progression needs.')}"
        )
        q["id"] = _extract_id(page)
        q["source_run"] = run_ref
        q["prompt_version"] = QUEST_PROMPT_VERSION
        q["review_state"] = review_state_for_mode(q.get("generation_mode", "llm"))
        q["fallback_reason"] = q.get("fallback_reason", fallback_reason if q.get("generation_mode") == "fallback" else "")
        created.append(q)

    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Quest Generation",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=f"{player_name}:{count} quests",
        model=llm_model_label(run_generation_mode),
        generation_mode=run_generation_mode,
        fallback_reason=fallback_reason,
        prompt_version=QUEST_PROMPT_VERSION,
        replayable=True,
        records_created=len(created),
    )
    return created


async def generate_boss_battle(
    mcp: NotionMCP,
    allow_llm: bool = True,
    *,
    triggered_by: str = "CLI",
) -> dict:
    """Generate a boss battle based on the player's weakest skill."""
    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up.")
    _, db_ids = workspace
    started_at = datetime.datetime.now(datetime.timezone.utc)
    run_ref = build_run_ref("Boss Generation", started_at)
    fallback_reason = ""

    skill_stats = await _get_skill_stats(mcp, db_ids)
    player_stats = await _get_player_stats(mcp, db_ids, player_name)
    player_preferences = _player_preferences(player_stats)

    # Find weakest skill
    weakest = min(skill_stats, key=lambda s: s.get("xp", 0)) if skill_stats else {"skill": "Endurance", "xp": 0}

    # Generate boss via configured LLM provider
    boss_prompt = f"""You are the Quest Master. Create a BOSS BATTLE quest.

The player's weakest skill is: {weakest.get('skill', 'Endurance')} (XP: {weakest.get('xp', 0)})
Player level: {weakest.get('level', 1)}
Player preferences: {_compact_json(player_preferences)}

Create an epic boss battle that forces the player to face their weakness. The boss should:
1. Have a dramatic RPG name (e.g., "The Procrastination Dragon", "The Comfort Zone Golem")
2. Require 2-3 specific real-world actions to "defeat"
3. Offer massive XP rewards (150-300)
4. Have a dramatic description
5. Include a short "why_this_quest" explanation

Return JSON:
{{
  "boss_name": "The [Name]",
  "quest_title": "emoji Boss Battle: [title]",
  "skill": "{weakest.get('skill', 'Endurance')}",
  "xp_reward": number,
  "description": "2-3 sentence dramatic description",
  "defeat_conditions": ["action 1", "action 2", "action 3"],
  "why_this_quest": "why this boss is the right pressure point right now"
}}

Return ONLY JSON."""

    generation_mode = "llm"
    if allow_llm:
        try:
            boss_text = get_llm_response(boss_prompt, max_tokens=BOSS_LLM_MAX_TOKENS)
            boss = _parse_json_with_repair(
                boss_text,
                "JSON object with boss_name, quest_title, skill, xp_reward, description, defeat_conditions, why_this_quest",
                max_tokens=BOSS_REPAIR_MAX_TOKENS,
            )
        except Exception as exc:
            # Fallback: return a minimal boss rather than crash
            generation_mode = "fallback"
            fallback_reason = summarize_exception(exc)
            boss = _fallback_boss(weakest, player_name)
    else:
        generation_mode = "fallback"
        fallback_reason = "template-only requested"
        boss = _fallback_boss(weakest, player_name)

    if not isinstance(boss, dict):
        generation_mode = "fallback"
        fallback_reason = fallback_reason or "LLM returned an unsupported payload shape"
        boss = _fallback_boss(weakest, player_name)

    # Create boss quest in Notion
    today = datetime.date.today()
    defeat_text = "\n".join(f"- [ ] {c}" for c in boss.get("defeat_conditions", []))
    content = f"""# ⚔️ BOSS BATTLE\n\n{boss.get('description', '')}\n\n## Defeat Conditions\n{defeat_text}\n\n---\n*Defeat this boss to earn {boss.get('xp_reward', 200)} XP and prove your valor!*"""

    boss_payload, _ = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
        "Quest": boss.get("quest_title", "⚔️ Boss Battle"),
        "Status": "Available",
        "Rarity": "Legendary",
        "Skill": boss.get("skill", weakest.get("skill", "Endurance")),
        "XP Reward": boss.get("xp_reward", 200),
        "Due Date": (today + datetime.timedelta(days=3)).isoformat(),
        "Description": boss.get("description", "A fearsome boss awaits..."),
        "Difficulty": "Boss",
        "Source": "Boss Battle",
        "Why This Quest": boss.get("why_this_quest", f"Your lowest-XP skill is {weakest.get('skill', 'Endurance')}, so this boss pressures the weakest link."),
        "Generation Mode": _generation_mode_label(generation_mode),
        "Review State": review_state_for_mode(generation_mode),
        "Correction Notes": "",
        "Source Run": run_ref,
        "Prompt Version": BOSS_PROMPT_VERSION,
        "Fallback Reason": fallback_reason if generation_mode == "fallback" else "",
    })
    page = await mcp.create_db_page(db_ids["Quest Board"], boss_payload, content_markdown=content, icon="🐉")

    boss_page_id = _extract_id(page)
    mode_text = " [fallback template]" if generation_mode == "fallback" else ""
    await mcp.create_comment(
        boss_page_id,
        f"🐉 **{boss.get('boss_name', 'The Boss')} has appeared!**{mode_text} "
        f"{player_name}'s weakness in {weakest.get('skill', 'Endurance')} has drawn its attention. "
        f"Complete the defeat conditions to vanquish this foe! "
        f"Why now: {boss.get('why_this_quest', 'It targets the weakest part of your current build.')}"
    )

    boss["id"] = boss_page_id
    boss["generation_mode"] = generation_mode
    boss["source_run"] = run_ref
    boss["prompt_version"] = BOSS_PROMPT_VERSION
    boss["review_state"] = review_state_for_mode(generation_mode)
    boss["fallback_reason"] = fallback_reason if generation_mode == "fallback" else ""
    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Boss Generation",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=boss.get("quest_title", boss.get("boss_name", "Boss Battle")),
        model=llm_model_label(generation_mode),
        generation_mode=generation_mode,
        fallback_reason=fallback_reason,
        prompt_version=BOSS_PROMPT_VERSION,
        replayable=True,
        records_created=1,
    )
    return boss


async def _get_player_stats(mcp: NotionMCP, db_ids: dict, name: str) -> dict:
    page = await get_player_page(mcp, db_ids, name)
    if page:
        return normalize_player(page)
    return {"level": 1, "total_xp": 0, "name": name}


async def _get_skill_stats(mcp: NotionMCP, db_ids: dict) -> list[dict]:
    stats_by_skill = {skill_name: {"skill": skill_name, "xp": 0, "level": 1} for skill_name in SKILL_TREES}
    for page in await get_skill_pages(mcp, db_ids):
        normalized = normalize_skill(page)
        stats_by_skill[normalized["skill"]] = normalized
    return [stats_by_skill[skill_name] for skill_name in SKILL_TREES]


async def _get_recent_quests(mcp: NotionMCP, db_ids: dict) -> list[dict]:
    quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
    quests.sort(key=lambda quest: quest.get("completed_at") or quest.get("due_date") or "", reverse=True)
    return quests[:8]


def _normalize_generated_quest(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None

    quest_name = str(raw.get("quest", "")).strip()
    skill = str(raw.get("skill", "")).strip()
    if not quest_name or skill not in SKILL_TREES:
        return None

    difficulty = str(raw.get("difficulty", "Medium")).title()
    if difficulty not in {"Easy", "Medium", "Hard"}:
        difficulty = "Medium"

    rarity = str(raw.get("rarity", "Common")).title()
    if rarity not in QUEST_RARITIES:
        rarity = "Common"

    xp_reward = raw.get("xp_reward", QUEST_RARITIES[rarity]["xp_range"][0])
    try:
        xp_reward = int(xp_reward)
    except (TypeError, ValueError):
        xp_reward = QUEST_RARITIES[rarity]["xp_range"][0]

    due_days = raw.get("due_days", 3)
    try:
        due_days = max(1, min(7, int(due_days)))
    except (TypeError, ValueError):
        due_days = 3

    description = str(raw.get("description", "")).strip() or "A worthy challenge awaits."
    why_this_quest = str(raw.get("why_this_quest", "")).strip() or "It fits the skill area that currently needs the most reinforcement."
    return {
        "quest": quest_name,
        "skill": skill,
        "rarity": rarity,
        "xp_reward": xp_reward,
        "difficulty": difficulty,
        "description": description,
        "why_this_quest": why_this_quest,
        "due_days": due_days,
    }


def _parse_json_with_repair(response_text: str, schema_hint: str, *, max_tokens: int) -> dict | list:
    try:
        return parse_json_response(response_text)
    except Exception:
        repaired = get_llm_response(
            JSON_REPAIR_PROMPT.format(schema_hint=schema_hint, response_text=response_text),
            max_tokens=max_tokens,
        )
        return parse_json_response(repaired)


def _fallback_quests(skill_stats: list[dict], recent_quests: list[dict], player_stats: dict, count: int) -> list[dict]:
    if count <= 0:
        return []

    recent_names = {quest.get("quest", "").casefold() for quest in recent_quests}
    recent_skill_counts: defaultdict[str, int] = defaultdict(int)
    for quest in recent_quests:
        skill = quest.get("skill")
        if skill in SKILL_TREES:
            recent_skill_counts[skill] += 1

    ranked_skills = sorted(
        skill_stats or [{"skill": skill_name, "xp": 0} for skill_name in SKILL_TREES],
        key=lambda item: (item.get("xp", 0), recent_skill_counts.get(item.get("skill", ""), 0)),
    )

    fallback_templates = {
        "Strength": [
            ("Iron Will Warmup", "Easy", "Common", 20, 2, "Train your body with a 20-minute strength or mobility session."),
            ("Forge of Momentum", "Medium", "Uncommon", 45, 3, "Finish one focused workout that makes you break a real sweat."),
        ],
        "Intelligence": [
            ("Scholar's Sprint", "Easy", "Common", 25, 2, "Read, watch, or study one focused topic for 30 minutes and write down three takeaways."),
            ("Codex Deep Dive", "Medium", "Rare", 60, 4, "Complete one deliberate learning block and summarize what changed in your understanding."),
        ],
        "Charisma": [
            ("Signal Fire", "Easy", "Common", 20, 1, "Send one thoughtful message that reopens a real relationship."),
            ("Council Audience", "Medium", "Uncommon", 40, 3, "Start one conversation that could unblock an opportunity or strengthen trust."),
        ],
        "Creativity": [
            ("Spark Ritual", "Easy", "Common", 25, 2, "Create one small artifact in code, writing, music, or design and share or save it."),
            ("Maker's Trial", "Medium", "Rare", 65, 4, "Ship one creative output from draft to done instead of polishing forever."),
        ],
        "Endurance": [
            ("Streak Keeper", "Easy", "Common", 20, 2, "Choose one habit and complete it today before your usual friction arrives."),
            ("Discipline Loop", "Hard", "Rare", 80, 5, "Repeat one useful habit for three straight days and log the result."),
        ],
        "Wisdom": [
            ("Quiet Chamber", "Easy", "Common", 20, 1, "Take 10 minutes to reflect, journal, or meditate without multitasking."),
            ("Oracle Review", "Medium", "Uncommon", 35, 3, "Review the past week and extract one lesson you can apply immediately."),
        ],
    }

    created: list[dict] = []
    used_names = set(recent_names)
    cursor = 0
    while len(created) < count and ranked_skills:
        skill = ranked_skills[cursor % len(ranked_skills)].get("skill", "Endurance")
        templates = fallback_templates.get(skill, fallback_templates["Endurance"])
        template = templates[(cursor // len(ranked_skills)) % len(templates)]
        quest_name = f"🎯 {template[0]}"
        if quest_name.casefold() not in used_names:
            created.append({
                "quest": quest_name,
                "skill": skill,
                "difficulty": template[1],
                "rarity": template[2],
                "xp_reward": template[3],
                "due_days": template[4],
                "description": template[5],
                "why_this_quest": _reason_for_skill(skill, player_stats, recent_skill_counts.get(skill, 0)),
            })
            used_names.add(quest_name.casefold())
        cursor += 1

    return created


def _player_preferences(player_stats: dict) -> dict:
    return {
        "primary_goal": player_stats.get("primary_goal", ""),
        "available_time": player_stats.get("available_time", ""),
        "preferred_challenge_style": player_stats.get("preferred_challenge_style", ""),
        "focus_area": player_stats.get("focus_area", ""),
        "constraints": player_stats.get("constraints", ""),
        "motivation": player_stats.get("motivation", ""),
        "context_brief": player_stats.get("context_brief", ""),
        "context_sources": player_stats.get("context_sources", ""),
    }


def _quest_player_context(player_stats: dict) -> dict:
    return {
        "name": player_stats.get("name", ""),
        "level": player_stats.get("level", 1),
        "total_xp": player_stats.get("total_xp", 0),
        "title": player_stats.get("title", ""),
        "focus_area": player_stats.get("focus_area", ""),
        "primary_goal": player_stats.get("primary_goal", ""),
        "available_time": player_stats.get("available_time", ""),
        "preferred_challenge_style": player_stats.get("preferred_challenge_style", ""),
        "constraints": player_stats.get("constraints", ""),
        "motivation": player_stats.get("motivation", ""),
        "context_brief": player_stats.get("context_brief", ""),
    }


def _quest_skill_context(skill_stats: list[dict]) -> list[dict]:
    return [
        {
            "skill": item.get("skill", ""),
            "xp": item.get("xp", 0),
            "level": item.get("level", 1),
            "quests_completed": item.get("quests_completed", 0),
            "last_activity": item.get("last_activity", ""),
        }
        for item in skill_stats
    ]


def _quest_history_context(recent_quests: list[dict]) -> list[dict]:
    return [
        {
            "quest": quest.get("quest", ""),
            "skill": quest.get("skill", ""),
            "status": quest.get("status", ""),
            "difficulty": quest.get("difficulty", ""),
            "due_date": quest.get("due_date", ""),
            "completed_at": quest.get("completed_at", ""),
        }
        for quest in recent_quests
    ]


def _compact_json(value: dict | list) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _reason_for_skill(skill: str, player_stats: dict, recent_count: int) -> str:
    focus_area = (player_stats.get("focus_area") or "").lower()
    available_time = player_stats.get("available_time") or "your current schedule"
    style = player_stats.get("preferred_challenge_style") or "Balanced"

    if skill.lower() in focus_area:
        return f"This supports your stated focus area in {skill} while still fitting {available_time}."
    if recent_count == 0:
        return f"You have not touched {skill} recently, so this closes a visible gap without breaking your {style.lower()} pacing."
    return f"{skill} still trails your other trees, and this quest matches your {style.lower()} challenge preference."


def _fallback_boss(weakest: dict, player_name: str) -> dict:
    weakest_skill = weakest.get("skill", "Endurance")
    return {
        "boss_name": "The Unknown Terror",
        "quest_title": "⚔️ Boss Battle: Face the Unknown",
        "skill": weakest_skill,
        "xp_reward": 200,
        "description": "A mysterious foe has appeared from the shadows and is feeding on your weakest discipline.",
        "defeat_conditions": [
            f"Complete one meaningful {weakest_skill} task today",
            "Record a short note about what made that task hard to start",
            "Commit to the next repeat while your momentum is still alive",
        ],
        "why_this_quest": f"{player_name} is weakest in {weakest_skill} right now, so the boss fight forces progress where the build is most fragile.",
    }


def _generation_mode_label(mode: str) -> str:
    return "Fallback Template" if mode == "fallback" else "LLM"


def _extract_id(result: dict) -> str:
    if isinstance(result, dict):
        for key in ("id", "page_id", "database_id"):
            if key in result:
                return result[key]
        if "results" in result and result["results"]:
            return result["results"][0].get("id", "")
    return str(result)
