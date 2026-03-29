"""Configurable LLM provider supports Anthropic and OpenAI-compatible APIs."""

import json
from functools import lru_cache

from ..config import get_config


def get_llm_response(prompt: str, max_tokens: int = 2000) -> str:
    """Send a prompt to the configured LLM provider and return the text response."""
    config = get_config()
    provider = config.llm_provider

    if provider == "anthropic":
        return _call_anthropic(prompt, max_tokens, config)
    if provider == "openai":
        return _call_openai_compatible(prompt, max_tokens, config)
    raise ValueError(f"Unknown LLM provider: {provider}. Use 'anthropic' or 'openai'.")


def _call_anthropic(prompt: str, max_tokens: int, config) -> str:
    client = _anthropic_client(config.anthropic_api_key)
    response = client.messages.create(
        model=config.llm_model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def _call_openai_compatible(prompt: str, max_tokens: int, config) -> str:
    client = _openai_client(config.openai_api_key, config.openai_base_url)
    response = client.chat.completions.create(
        model=config.llm_model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip()


@lru_cache(maxsize=2)
def _anthropic_client(api_key: str):
    import anthropic

    return anthropic.Anthropic(api_key=api_key)


@lru_cache(maxsize=4)
def _openai_client(api_key: str, base_url: str):
    from openai import OpenAI

    return OpenAI(api_key=api_key, base_url=base_url)


def parse_json_response(text: str) -> dict | list:
    """Parse JSON from an LLM response, handling markdown code blocks and extra text."""
    text = text.strip()

    if "```" in text:
        parts = text.split("```")
        for part in parts:
            cleaned = part.strip()
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
            try:
                return json.loads(cleaned)
            except (json.JSONDecodeError, ValueError):
                continue

    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        pass

    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except (json.JSONDecodeError, ValueError):
                continue

    raise ValueError(f"Could not parse JSON from LLM response: {text[:200]}")
