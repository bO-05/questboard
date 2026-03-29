"""Code-first quest intake helpers for player-authored quests."""

from __future__ import annotations

from .config import QUEST_RARITIES, SKILL_TREES


def normalize_skill_input(value: str, fallback: str = "Endurance") -> str:
    if not value:
        return fallback

    cleaned = value.strip().lower()
    for skill_name in SKILL_TREES:
        if cleaned == skill_name.lower():
            return skill_name
    for skill_name in SKILL_TREES:
        if skill_name.lower().startswith(cleaned):
            return skill_name
    return fallback


def estimate_quest_profile(minutes: int, importance: str = "standard") -> dict:
    minutes = max(5, minutes)
    importance = (importance or "standard").strip().lower()

    if minutes <= 20:
        difficulty = "Easy"
        rarity = "Common"
        xp_reward = 20
    elif minutes <= 45:
        difficulty = "Medium"
        rarity = "Uncommon"
        xp_reward = 45
    elif minutes <= 90:
        difficulty = "Hard"
        rarity = "Rare"
        xp_reward = 75
    else:
        difficulty = "Hard"
        rarity = "Epic"
        xp_reward = 110

    if importance == "high":
        xp_reward = int(xp_reward * 1.3)
        if rarity == "Common":
            rarity = "Uncommon"
        elif rarity == "Uncommon":
            rarity = "Rare"
        elif rarity == "Rare":
            rarity = "Epic"

    xp_min, xp_max = QUEST_RARITIES[rarity]["xp_range"]
    xp_reward = max(xp_min, min(xp_max, xp_reward))
    return {
        "difficulty": difficulty,
        "rarity": rarity,
        "xp_reward": xp_reward,
    }


def build_player_quest(
    title: str,
    skill: str,
    *,
    minutes: int = 30,
    due_days: int = 3,
    notes: str = "",
    importance: str = "standard",
    focus_area: str = "",
) -> dict:
    skill_name = normalize_skill_input(skill)
    profile = estimate_quest_profile(minutes, importance=importance)
    due_days = max(1, min(14, int(due_days)))
    focus_area_text = (focus_area or "").strip().lower()

    why_this_quest = (
        f"You explicitly added this quest for {skill_name}, which aligns with your current focus on {focus_area}."
        if focus_area and skill_name.lower() in focus_area_text
        else f"You explicitly added this quest for {skill_name}, so QuestBoard is turning your own priority into trackable progression."
    )

    description = notes.strip() or (
        f"A player-authored quest in {skill_name}. Estimated effort: {minutes} minutes."
    )

    return {
        "quest": title.strip(),
        "skill": skill_name,
        "difficulty": profile["difficulty"],
        "rarity": profile["rarity"],
        "xp_reward": profile["xp_reward"],
        "due_days": due_days,
        "description": description,
        "why_this_quest": why_this_quest,
        "generation_mode": "player",
        "source": "Player",
    }
