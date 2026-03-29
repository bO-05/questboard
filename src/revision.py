"""Revision helpers for human-in-the-loop correction loops."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from .audit import build_run_ref, llm_model_label, log_run, review_state_for_mode, summarize_exception
from .config import QUEST_RARITIES, SKILL_TREES, get_config
from .engines.llm_provider import get_llm_response, parse_json_response
from .setup_workspace import load_workspace_ids, load_workspace_player_name
from .workspace_data import filter_properties_for_database, normalize_quest

QUEST_REVISION_PROMPT_VERSION = "quest-revision-v1"
BOSS_REVISION_PROMPT_VERSION = "boss-revision-v1"
RECAP_REVISION_PROMPT_VERSION = "weekly-recap-revision-v1"


QUEST_REVISION_PROMPT = """You are revising a QuestBoard quest after explicit human review.

PLAYER: {player_name}
CURRENT QUEST:
{current_quest}

REVIEW NOTES:
{correction_notes}

Revise this quest so it is clearer, more intuitive, and more engaging inside Notion.
Keep it actionable and grounded in a real task.
Preserve the same skill unless the notes clearly imply a better fit.
Keep difficulty, rarity, and XP in roughly the same effort band unless the notes imply a change.

Return JSON:
{{
  "quest": "revised quest title",
  "skill": "one of: {skills}",
  "rarity": "one of: Common, Uncommon, Rare, Epic, Legendary",
  "xp_reward": number,
  "difficulty": "one of: Easy, Medium, Hard",
  "description": "revised description",
  "why_this_quest": "why this revised quest is better now",
  "due_days": number (1-7)
}}

Return ONLY JSON."""


BOSS_REVISION_PROMPT = """You are revising a QuestBoard boss battle after explicit human review.

PLAYER: {player_name}
CURRENT BOSS:
{current_boss}

REVIEW NOTES:
{correction_notes}

Revise this boss battle so it feels sharper, clearer, and more compelling while still pointing to a real-world task.
Keep the same target skill unless the review notes clearly call for a better pressure point.

Return JSON:
{{
  "boss_name": "revised boss name",
  "quest_title": "revised boss quest title",
  "skill": "one of: {skills}",
  "xp_reward": number,
  "description": "revised description",
  "defeat_conditions": ["condition 1", "condition 2", "condition 3"],
  "why_this_quest": "why this revised boss is stronger"
}}

Return ONLY JSON."""


RECAP_REVISION_PROMPT = """You are revising a QuestBoard weekly recap after explicit human review.

PLAYER: {player_name}
CURRENT METRICS:
{metrics}

CURRENT NARRATIVE:
{current_narrative}

REVIEW NOTES:
{correction_notes}

Revise the narrative so it better addresses the review notes while preserving the real metrics.
Keep it vivid, coherent, and readable inside Notion.
Return only the revised markdown narrative. Do not change the numbers."""


def _days_until_due(due_date: str | None) -> int:
    if not due_date:
        return 3
    try:
        target = dt.date.fromisoformat(due_date)
    except ValueError:
        return 3
    return max(1, min(7, (target - dt.date.today()).days or 1))


def _normalize_revision_quest(raw: dict[str, Any], existing: dict[str, Any]) -> dict[str, Any]:
    skill = str(raw.get("skill") or existing.get("skill") or "Endurance").strip()
    if skill not in SKILL_TREES:
        skill = existing.get("skill") if existing.get("skill") in SKILL_TREES else "Endurance"

    difficulty = str(raw.get("difficulty") or existing.get("difficulty") or "Medium").title()
    if difficulty not in {"Easy", "Medium", "Hard"}:
        difficulty = existing.get("difficulty", "Medium")
        if difficulty not in {"Easy", "Medium", "Hard"}:
            difficulty = "Medium"

    rarity = str(raw.get("rarity") or existing.get("rarity") or "Common").title()
    if rarity not in QUEST_RARITIES:
        rarity = existing.get("rarity", "Common")
        if rarity not in QUEST_RARITIES:
            rarity = "Common"

    xp_reward = raw.get("xp_reward", existing.get("xp_reward", QUEST_RARITIES[rarity]["xp_range"][0]))
    try:
        xp_reward = int(xp_reward)
    except (TypeError, ValueError):
        xp_reward = int(existing.get("xp_reward", QUEST_RARITIES[rarity]["xp_range"][0]))

    due_days = raw.get("due_days", _days_until_due(existing.get("due_date")))
    try:
        due_days = max(1, min(7, int(due_days)))
    except (TypeError, ValueError):
        due_days = _days_until_due(existing.get("due_date"))

    return {
        "quest": str(raw.get("quest") or existing.get("quest") or "Unknown Quest").strip(),
        "skill": skill,
        "rarity": rarity,
        "xp_reward": xp_reward,
        "difficulty": difficulty,
        "description": str(raw.get("description") or existing.get("description") or "A worthy challenge awaits.").strip(),
        "why_this_quest": str(raw.get("why_this_quest") or existing.get("why_this_quest") or "It better matches the current progression needs.").strip(),
        "due_days": due_days,
    }


def _fallback_revised_quest(existing: dict[str, Any], correction_notes: str) -> dict[str, Any]:
    summary = correction_notes.strip().rstrip(".")
    if len(summary) > 140:
        summary = summary[:137].rstrip() + "..."
    return {
        "quest": existing.get("quest", "Unknown Quest"),
        "skill": existing.get("skill", "Endurance"),
        "rarity": existing.get("rarity", "Common"),
        "xp_reward": existing.get("xp_reward", 20),
        "difficulty": existing.get("difficulty", "Medium"),
        "description": (
            f"{existing.get('description') or 'A worthy challenge awaits.'} "
            f"Revision focus: {summary}."
        ).strip(),
        "why_this_quest": f"Updated after review feedback to address: {summary}.",
        "due_days": _days_until_due(existing.get("due_date")),
    }


def _fallback_revised_boss(existing: dict[str, Any], correction_notes: str, player_name: str) -> dict[str, Any]:
    summary = correction_notes.strip().rstrip(".")
    if len(summary) > 140:
        summary = summary[:137].rstrip() + "..."
    weakest_skill = existing.get("skill", "Endurance")
    return {
        "boss_name": existing.get("quest", "The Unknown Terror"),
        "quest_title": existing.get("quest", "Boss Battle"),
        "skill": weakest_skill,
        "xp_reward": existing.get("xp_reward", 200),
        "description": (
            f"{existing.get('description') or 'A fearsome foe waits in the shadows.'} "
            f"Revision focus: {summary}."
        ).strip(),
        "defeat_conditions": [
            f"Complete one meaningful {weakest_skill} task today",
            f"Address this reviewer concern directly: {summary}",
            f"Write one sentence on how {player_name} will avoid the same weakness next time",
        ],
        "why_this_quest": f"This boss was revised after review to better pressure {weakest_skill} and address: {summary}.",
    }


def _fallback_revised_recap(current_narrative: str, correction_notes: str, metrics: dict[str, Any]) -> str:
    summary = correction_notes.strip()
    if len(summary) > 220:
        summary = summary[:217].rstrip() + "..."
    base = current_narrative.strip() or "# Chapter: Revised Chronicle"
    return (
        f"{base}\n\n"
        f"## Revision Notes Addressed\n"
        f"- {summary}\n\n"
        f"## Stats Scroll (Unchanged)\n"
        f"- Quests completed: {metrics.get('quests_completed', 0)}\n"
        f"- XP earned: {metrics.get('xp_earned', 0)}\n"
        f"- Levels gained: {metrics.get('levels_gained', 0)}\n"
        f"- MVP skill: {metrics.get('mvp_skill', 'Endurance')}\n"
    )


async def revise_quest_page(
    mcp,
    quest_page_id: str,
    correction_notes: str,
    *,
    allow_llm: bool = True,
    triggered_by: str = "CLI Review",
) -> dict[str, Any]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    started_at = dt.datetime.now(dt.timezone.utc)
    page = await mcp.fetch_page(quest_page_id)
    existing = normalize_quest(page)
    is_boss = existing.get("difficulty") == "Boss" or existing.get("source") == "Boss Battle"
    fallback_reason = ""
    generation_mode = "llm"

    if is_boss:
        run_ref = build_run_ref("Boss Generation", started_at)
        prompt_version = BOSS_REVISION_PROMPT_VERSION
        current_payload = {
            "quest_title": existing.get("quest", ""),
            "skill": existing.get("skill", ""),
            "xp_reward": existing.get("xp_reward", 0),
            "description": existing.get("description", ""),
            "why_this_quest": existing.get("why_this_quest", ""),
            "existing_content": page.get("content_markdown", ""),
        }
        if allow_llm:
            try:
                response = get_llm_response(
                    BOSS_REVISION_PROMPT.format(
                        player_name=player_name,
                        current_boss=json.dumps(current_payload, indent=2),
                        correction_notes=correction_notes,
                        skills=", ".join(SKILL_TREES.keys()),
                    ),
                    max_tokens=1200,
                )
                parsed = parse_json_response(response)
            except Exception as exc:
                generation_mode = "fallback"
                fallback_reason = summarize_exception(exc)
                parsed = _fallback_revised_boss(existing, correction_notes, player_name)
        else:
            generation_mode = "fallback"
            fallback_reason = "template-only requested"
            parsed = _fallback_revised_boss(existing, correction_notes, player_name)

        boss = {
            "boss_name": str(parsed.get("boss_name") or existing.get("quest") or "The Unknown Terror").strip(),
            "quest_title": str(parsed.get("quest_title") or existing.get("quest") or "Boss Battle").strip(),
            "skill": str(parsed.get("skill") or existing.get("skill") or "Endurance").strip(),
            "xp_reward": int(parsed.get("xp_reward", existing.get("xp_reward", 200))),
            "description": str(parsed.get("description") or existing.get("description") or "A fearsome foe waits in the shadows.").strip(),
            "defeat_conditions": parsed.get("defeat_conditions") if isinstance(parsed.get("defeat_conditions"), list) else [
                f"Complete one meaningful {existing.get('skill', 'Endurance')} task today",
                "Record why this task matters now",
                "Commit the next repeat while momentum is fresh",
            ],
            "why_this_quest": str(parsed.get("why_this_quest") or existing.get("why_this_quest") or "It addresses the current weakest point in the build.").strip(),
        }
        if boss["skill"] not in SKILL_TREES:
            boss["skill"] = existing.get("skill") if existing.get("skill") in SKILL_TREES else "Endurance"
        content = (
            f"# Boss Battle\n\n"
            f"{boss['description']}\n\n"
            f"## Defeat Conditions\n"
            + "\n".join(f"- [ ] {item}" for item in boss["defeat_conditions"])
            + f"\n\nRevision notes addressed: {correction_notes}\n"
        )
        updates, _ = await filter_properties_for_database(
            mcp,
            db_ids["Quest Board"],
            {
                "Quest": boss["quest_title"],
                "Skill": boss["skill"],
                "XP Reward": boss["xp_reward"],
                "Description": boss["description"],
                "Why This Quest": boss["why_this_quest"],
                "Generation Mode": "Fallback Template" if generation_mode == "fallback" else "LLM",
                "Review State": review_state_for_mode(generation_mode),
                "Correction Notes": correction_notes,
                "Source Run": run_ref,
                "Prompt Version": prompt_version,
                "Fallback Reason": fallback_reason if generation_mode == "fallback" else "",
            },
        )
        await mcp.update_page(quest_page_id, updates, content_markdown=content)
        await mcp.create_comment(
            quest_page_id,
            f"Revision applied to boss battle for {player_name}. Notes addressed: {correction_notes}",
        )
        await log_run(
            mcp,
            db_ids,
            run_ref=run_ref,
            run_type="Boss Generation",
            status="Succeeded",
            started_at=started_at,
            triggered_by=triggered_by,
            target_entity=f"revision:{boss['quest_title']}",
            model=llm_model_label(generation_mode),
            generation_mode=generation_mode,
            fallback_reason=fallback_reason,
            prompt_version=prompt_version,
            replayable=True,
            records_updated=1,
        )
        return {
            "id": quest_page_id,
            "item_type": "Boss Battle",
            "title": boss["quest_title"],
            "source_run": run_ref,
            "review_state": review_state_for_mode(generation_mode),
            "generation_mode": generation_mode,
            "fallback_reason": fallback_reason if generation_mode == "fallback" else "",
        }

    run_ref = build_run_ref("Quest Generation", started_at)
    prompt_version = QUEST_REVISION_PROMPT_VERSION
    if allow_llm:
        try:
            response = get_llm_response(
                QUEST_REVISION_PROMPT.format(
                    player_name=player_name,
                    current_quest=json.dumps(existing, indent=2),
                    correction_notes=correction_notes,
                    skills=", ".join(SKILL_TREES.keys()),
                ),
                max_tokens=1200,
            )
            parsed = parse_json_response(response)
            if not isinstance(parsed, dict):
                raise ValueError("Quest revision response was not an object")
            revised = _normalize_revision_quest(parsed, existing)
        except Exception as exc:
            generation_mode = "fallback"
            fallback_reason = summarize_exception(exc)
            revised = _fallback_revised_quest(existing, correction_notes)
    else:
        generation_mode = "fallback"
        fallback_reason = "template-only requested"
        revised = _fallback_revised_quest(existing, correction_notes)

    due_date = (dt.date.today() + dt.timedelta(days=revised["due_days"])).isoformat()
    updates, _ = await filter_properties_for_database(
        mcp,
        db_ids["Quest Board"],
        {
            "Quest": revised["quest"],
            "Skill": revised["skill"],
            "Rarity": revised["rarity"],
            "XP Reward": revised["xp_reward"],
            "Due Date": due_date,
            "Description": revised["description"],
            "Difficulty": revised["difficulty"],
            "Why This Quest": revised["why_this_quest"],
            "Generation Mode": "Fallback Template" if generation_mode == "fallback" else "LLM",
            "Review State": review_state_for_mode(generation_mode),
            "Correction Notes": correction_notes,
            "Source Run": run_ref,
            "Prompt Version": prompt_version,
            "Fallback Reason": fallback_reason if generation_mode == "fallback" else "",
        },
    )
    await mcp.update_page(quest_page_id, updates)
    await mcp.create_comment(
        quest_page_id,
        f"Revision applied to quest for {player_name}. Notes addressed: {correction_notes}",
    )
    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Quest Generation",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=f"revision:{revised['quest']}",
        model=llm_model_label(generation_mode),
        generation_mode=generation_mode,
        fallback_reason=fallback_reason,
        prompt_version=prompt_version,
        replayable=True,
        records_updated=1,
    )
    return {
        "id": quest_page_id,
        "item_type": "Quest",
        "title": revised["quest"],
        "source_run": run_ref,
        "review_state": review_state_for_mode(generation_mode),
        "generation_mode": generation_mode,
        "fallback_reason": fallback_reason if generation_mode == "fallback" else "",
    }


async def revise_recap_page(
    mcp,
    recap_page_id: str,
    correction_notes: str,
    *,
    allow_llm: bool = True,
    triggered_by: str = "CLI Review",
) -> dict[str, Any]:
    workspace = load_workspace_ids()
    if not workspace:
        raise RuntimeError("Workspace not set up. Run `questboard setup` first.")
    _, db_ids = workspace

    config = get_config()
    player_name = load_workspace_player_name(config.player_name)
    started_at = dt.datetime.now(dt.timezone.utc)
    run_ref = build_run_ref("Weekly Recap", started_at)
    fallback_reason = ""
    generation_mode = "llm"

    page = await mcp.fetch_page(recap_page_id)
    props = page.get("properties", {})
    week_label = props.get("Week", {}).get("title", [{}])[0].get("plain_text", "") if isinstance(props.get("Week"), dict) else ""
    metrics = {
        "quests_completed": props.get("Quests Completed", {}).get("number", 0),
        "xp_earned": props.get("XP Earned", {}).get("number", 0),
        "levels_gained": props.get("Levels Gained", {}).get("number", 0),
        "achievements_unlocked": props.get("Achievements Unlocked", {}).get("number", 0),
        "mvp_skill": props.get("MVP Skill", {}).get("select", {}).get("name", "Endurance"),
    }
    current_narrative = page.get("content_markdown", "")

    if allow_llm:
        try:
            revised_narrative = get_llm_response(
                RECAP_REVISION_PROMPT.format(
                    player_name=player_name,
                    metrics=json.dumps(metrics, indent=2),
                    current_narrative=current_narrative,
                    correction_notes=correction_notes,
                ),
                max_tokens=1600,
            )
        except Exception as exc:
            generation_mode = "fallback"
            fallback_reason = summarize_exception(exc)
            revised_narrative = _fallback_revised_recap(current_narrative, correction_notes, metrics)
    else:
        generation_mode = "fallback"
        fallback_reason = "template-only requested"
        revised_narrative = _fallback_revised_recap(current_narrative, correction_notes, metrics)

    updates, _ = await filter_properties_for_database(
        mcp,
        db_ids["Adventure Recaps"],
        {
            "Narrative": revised_narrative[:2000],
            "Generation Mode": "Fallback Template" if generation_mode == "fallback" else "LLM",
            "Review State": review_state_for_mode(generation_mode),
            "Correction Notes": correction_notes,
            "Source Run": run_ref,
            "Prompt Version": RECAP_REVISION_PROMPT_VERSION,
            "Fallback Reason": fallback_reason if generation_mode == "fallback" else "",
        },
    )
    await mcp.update_page(recap_page_id, updates, content_markdown=revised_narrative)
    await mcp.create_comment(
        recap_page_id,
        f"Revision applied to recap for {player_name}. Notes addressed: {correction_notes}",
    )
    await log_run(
        mcp,
        db_ids,
        run_ref=run_ref,
        run_type="Weekly Recap",
        status="Succeeded",
        started_at=started_at,
        triggered_by=triggered_by,
        target_entity=f"revision:{week_label or 'weekly recap'}",
        model=llm_model_label(generation_mode),
        generation_mode=generation_mode,
        fallback_reason=fallback_reason,
        prompt_version=RECAP_REVISION_PROMPT_VERSION,
        replayable=True,
        records_updated=1,
    )
    return {
        "id": recap_page_id,
        "item_type": "Adventure Recap",
        "title": week_label or "Adventure Recap",
        "source_run": run_ref,
        "review_state": review_state_for_mode(generation_mode),
        "generation_mode": generation_mode,
        "fallback_reason": fallback_reason if generation_mode == "fallback" else "",
    }
