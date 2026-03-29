"""Configuration and constants for QuestBoard."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

HOSTED_MCP_URL = "https://mcp.notion.com/mcp"


@dataclass
class Config:
    notion_token: str = field(default_factory=lambda: os.getenv("NOTION_TOKEN", ""))
    mcp_server_url: str = field(default_factory=lambda: os.getenv("MCP_SERVER_URL", "http://localhost:3100/mcp"))
    mcp_auth_token: str = field(default_factory=lambda: os.getenv("MCP_AUTH_TOKEN", ""))
    mcp_access_token: str = field(default_factory=lambda: os.getenv("MCP_ACCESS_TOKEN", ""))
    mcp_refresh_token: str = field(default_factory=lambda: os.getenv("MCP_REFRESH_TOKEN", ""))
    mcp_client_id: str = field(default_factory=lambda: os.getenv("MCP_CLIENT_ID", ""))
    mcp_client_secret: str = field(default_factory=lambda: os.getenv("MCP_CLIENT_SECRET", ""))
    mcp_token_expires_at: str = field(default_factory=lambda: os.getenv("MCP_TOKEN_EXPIRES_AT", ""))
    anthropic_api_key: str = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", ""))
    player_name: str = field(default_factory=lambda: os.getenv("PLAYER_NAME", "Hero"))
    difficulty: str = field(default_factory=lambda: os.getenv("DIFFICULTY", "normal"))

    # LLM provider: "anthropic" (default) or "openai" (for OpenRouter and other OpenAI-compatible APIs)
    llm_provider: str = field(default_factory=lambda: os.getenv("LLM_PROVIDER", "anthropic"))
    llm_model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", ""))
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_base_url: str = field(default_factory=lambda: os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1"))
    exa_api_key: str = field(default_factory=lambda: os.getenv("EXA_API_KEY", ""))
    perplexity_api_key: str = field(default_factory=lambda: os.getenv("PERPLEXITY_API_KEY", ""))

    def __post_init__(self):
        if not self.llm_model:
            self.llm_model = (
                "claude-sonnet-4-20250514" if self.llm_provider == "anthropic"
                else "openrouter/auto"
            )

    @property
    def is_self_hosted(self) -> bool:
        return "mcp.notion.com" not in self.mcp_server_url


# Skill tree definitions
SKILL_TREES = {
    "Strength": {"emoji": "💪", "desc": "Fitness, exercise, physical health"},
    "Intelligence": {"emoji": "🧠", "desc": "Learning, reading, study, courses"},
    "Charisma": {"emoji": "🗣️", "desc": "Social, networking, communication"},
    "Creativity": {"emoji": "🎨", "desc": "Art, writing, music, design"},
    "Endurance": {"emoji": "🏃", "desc": "Habits, consistency, discipline"},
    "Wisdom": {"emoji": "📖", "desc": "Reflection, journaling, meditation"},
}

# XP multipliers by difficulty
DIFFICULTY_MULTIPLIERS = {
    "easy": 1.5,
    "normal": 1.0,
    "hard": 0.75,
    "legendary": 0.5,
}

# Level thresholds (XP needed for each level)
LEVEL_THRESHOLDS = [
    0, 100, 250, 500, 800, 1200, 1700, 2300, 3000, 3800,       # 1-10
    4700, 5700, 6800, 8000, 9300, 10700, 12200, 13800, 15500, 17500,  # 11-20
]

# Quest rarities
QUEST_RARITIES = {
    "Common": {"emoji": "⚪", "xp_range": (10, 30), "color": "gray"},
    "Uncommon": {"emoji": "🟢", "xp_range": (30, 60), "color": "green"},
    "Rare": {"emoji": "🔵", "xp_range": (60, 100), "color": "blue"},
    "Epic": {"emoji": "🟣", "xp_range": (100, 200), "color": "purple"},
    "Legendary": {"emoji": "🟠", "xp_range": (200, 500), "color": "orange"},
}

# Boss battle types
BOSS_TYPES = {
    "The Procrastination Dragon": "Complete 5 overdue quests in 24 hours",
    "The Comfort Zone Golem": "Tackle a quest in your weakest skill tree",
    "The Burnout Phoenix": "Complete a Wisdom quest (rest/reflect)",
    "The Distraction Hydra": "Complete 3 quests without breaks",
    "The Perfectionism Lich": "Complete a quest with 'good enough' quality",
}


def get_config() -> Config:
    return Config()
