"""Guided onboarding helpers for turning user goals into grounded QuestBoard context."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from urllib.parse import urlparse
from typing import Any

import httpx
from openai import OpenAI

from .config import Config, get_config
from .engines.llm_provider import get_llm_response, parse_json_response

ALLOWED_RESEARCH_PROVIDERS = {"none", "auto", "exa", "perplexity"}
RESEARCH_PROVIDER_ALIASES = {
    "pplx": "perplexity",
    "perplexity.ai": "perplexity",
}
PERPLEXITY_BASE_URL = "https://api.perplexity.ai"
PERPLEXITY_MODEL = "sonar-pro"
LOW_SIGNAL_SOURCE_DOMAINS = (
    "reddit.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
)
HIGH_SIGNAL_TITLE_TERMS = (
    "guide",
    "how to",
    "tutorial",
    "reference",
    "docs",
    "documentation",
    "playbook",
    "production",
    "setup",
)
LOW_SIGNAL_TITLE_TERMS = (
    "reddit",
    "facebook",
    "thread",
    "comments",
)
INLINE_CITATION_RE = re.compile(r"\[(?:\d+|source)\]")


@dataclass(slots=True)
class OnboardingAnswers:
    player_name: str
    goal: str
    success_criteria: str
    available_time: str
    style: str
    focus: str
    constraints: str
    motivation: str
    domain_notes: str = ""


@dataclass(slots=True)
class ResearchBrief:
    provider: str = "none"
    summary: str = ""
    citations: list[str] = field(default_factory=list)


def llm_available(config: Config | None = None) -> bool:
    cfg = config or get_config()
    if cfg.llm_provider == "anthropic":
        return bool(cfg.anthropic_api_key)
    if cfg.llm_provider == "openai":
        return bool(cfg.openai_api_key)
    return False


def available_research_providers(config: Config | None = None) -> list[str]:
    cfg = config or get_config()
    providers: list[str] = []
    if cfg.perplexity_api_key:
        providers.append("perplexity")
    if cfg.exa_api_key:
        providers.append("exa")
    return providers


def _extract_requested_provider_tokens(requested: str | None) -> list[str]:
    text = (requested or "").strip().lower()
    if not text:
        return []

    normalized = (
        text.replace("/", ",")
        .replace("+", ",")
        .replace("&", ",")
        .replace(" and ", ",")
        .replace(" or ", ",")
    )
    tokens = []
    for raw_token in normalized.split(","):
        token = raw_token.strip()
        if not token:
            continue
        token = RESEARCH_PROVIDER_ALIASES.get(token, token)
        tokens.append(token)
    return tokens


def resolve_research_provider(requested: str | None, config: Config | None = None) -> str:
    cfg = config or get_config()
    raw_text = (requested or "auto").strip()
    tokens = _extract_requested_provider_tokens(raw_text)
    if not tokens:
        choice = "auto"
    elif len(tokens) == 1:
        choice = tokens[0]
    else:
        available = available_research_providers(cfg)
        for token in tokens:
            if token in available:
                return token
        for token in tokens:
            if token in ALLOWED_RESEARCH_PROVIDERS:
                choice = token
                break
        else:
            raise ValueError(f"Unknown research provider: {requested}. Use one of: {', '.join(sorted(ALLOWED_RESEARCH_PROVIDERS))}")

    if choice not in ALLOWED_RESEARCH_PROVIDERS:
        raise ValueError(f"Unknown research provider: {requested}. Use one of: {', '.join(sorted(ALLOWED_RESEARCH_PROVIDERS))}")
    if choice == "none":
        return "none"

    available = available_research_providers(cfg)
    if choice == "auto":
        return available[0] if available else "none"
    if choice not in available:
        raise ValueError(f"Research provider `{choice}` is not configured in the environment.")
    return choice


def build_context_sources(answers: OnboardingAnswers, research: ResearchBrief) -> str:
    parts: list[str] = []
    if answers.domain_notes.strip():
        parts.append("User context notes")
    if research.provider != "none":
        if research.citations:
            parts.append(f"{research.provider.title()}: {', '.join(research.citations[:3])}")
        else:
            parts.append(research.provider.title())
    return _trim_text_safely(" | ".join(parts), 1800)


def build_context_brief(
    answers: OnboardingAnswers,
    research: ResearchBrief,
    *,
    prefer_llm_summary: bool = True,
    config: Config | None = None,
) -> str:
    cfg = config or get_config()
    if prefer_llm_summary and llm_available(cfg):
        try:
            return _summarize_context_with_llm(answers, research).strip()
        except Exception:
            pass
    return _deterministic_context_brief(answers, research)


def run_optional_research(
    answers: OnboardingAnswers,
    provider: str | None,
    *,
    config: Config | None = None,
) -> ResearchBrief:
    cfg = config or get_config()
    resolved = resolve_research_provider(provider, cfg)
    if resolved == "none":
        return ResearchBrief()
    if resolved == "perplexity":
        return _research_with_perplexity(answers, cfg)
    if resolved == "exa":
        return _research_with_exa(answers, cfg)
    return ResearchBrief()


def _summarize_context_with_llm(answers: OnboardingAnswers, research: ResearchBrief) -> str:
    prompt = f"""You are helping onboard a user into QuestBoard.

Your job is to produce a concise grounded context brief that future quest generation can rely on.
Do not invent facts. Use only the user's answers and the research summary below.

USER ANSWERS:
- Goal: {answers.goal}
- Success criteria: {answers.success_criteria}
- Available time: {answers.available_time}
- Preferred challenge style: {answers.style}
- Focus area: {answers.focus}
- Constraints: {answers.constraints}
- Motivation: {answers.motivation}
- Domain notes: {answers.domain_notes or "None"}

RESEARCH PROVIDER: {research.provider}
RESEARCH SUMMARY:
{research.summary or "None"}

Return strict JSON with one field:
{{
  "context_brief": "4-6 sentence grounded brief that explains what the user is trying to do, any important domain terms, what success looks like, and what future quests should respect"
}}

Return ONLY JSON."""
    parsed = parse_json_response(get_llm_response(prompt, max_tokens=500))
    if isinstance(parsed, dict):
        brief = str(parsed.get("context_brief", "")).strip()
        if brief:
            return _trim_text_safely(brief, 1800)
    raise ValueError("LLM onboarding summary did not return context_brief")


def _deterministic_context_brief(answers: OnboardingAnswers, research: ResearchBrief) -> str:
    parts = [
        f"Primary outcome: {answers.goal}.",
        f"Success looks like: {answers.success_criteria}.",
        f"Time budget: {answers.available_time}.",
        f"Preferred pacing: {answers.style}.",
        f"Focus area: {answers.focus}.",
    ]
    if answers.constraints.strip():
        parts.append(f"Constraints: {answers.constraints}.")
    if answers.motivation.strip():
        parts.append(f"Motivation: {answers.motivation}.")
    if answers.domain_notes.strip():
        parts.append(f"Important domain context: {answers.domain_notes}.")
    if research.summary.strip():
        parts.append(f"Grounded brief: {research.summary}.")
    return _trim_text_safely(" ".join(parts), 1800)


def _research_query(answers: OnboardingAnswers) -> str:
    return (
        "Help ground this user goal for a personal productivity system.\n"
        f"Goal: {answers.goal}\n"
        f"Success criteria: {answers.success_criteria}\n"
        f"Focus area: {answers.focus}\n"
        f"Domain notes: {answers.domain_notes or 'None'}\n\n"
        "Explain unfamiliar tools, products, or terms and what a practical first milestone would likely involve. "
        "Prefer official docs, product guides, technical documentation, and practical implementation writeups. "
        "Avoid social chatter, discussion threads, or generic AI listicles."
    )


def _exa_summary_query(answers: OnboardingAnswers) -> str:
    return (
        "Summarize only the facts that help ground this goal. "
        f"User goal: {answers.goal}. "
        f"Success criteria: {answers.success_criteria}. "
        f"Focus area: {answers.focus}. "
        "Explain what the named tools or systems appear to be, whether the source is describing a real product ecosystem or implementation pattern, "
        "and what one concrete first business automation milestone makes sense. "
        "Ignore social chatter, migration anecdotes, community comments, boilerplate navigation text, and generic AI advice."
    )


def _result_url(result: dict[str, Any]) -> str:
    return str(result.get("url", "")).strip()


def _domain_host(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.casefold().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_low_signal_source(url: str) -> bool:
    host = _domain_host(url)
    return any(host == domain or host.endswith(f".{domain}") for domain in LOW_SIGNAL_SOURCE_DOMAINS)


def _source_quality_score(result: dict[str, Any]) -> int:
    url = _result_url(result)
    title = str(result.get("title", "")).strip().casefold()
    host = _domain_host(url)
    score = 0
    if _is_low_signal_source(url):
        score -= 100
    if any(term in title for term in HIGH_SIGNAL_TITLE_TERMS):
        score += 4
    if any(term in title for term in LOW_SIGNAL_TITLE_TERMS):
        score -= 6
    if any(term in host for term in ("docs", "guide", "guides", "playbook", "reference")):
        score += 2
    if host.endswith(".gov") or host.endswith(".edu") or host.endswith(".org"):
        score += 1
    return score


def _prioritize_exa_results(results: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    candidates = [result for result in results if isinstance(result, dict)]
    candidates.sort(key=_source_quality_score, reverse=True)

    selected: list[dict[str, Any]] = []
    seen_hosts: set[str] = set()
    for result in candidates:
        host = _domain_host(_result_url(result))
        if host and host in seen_hosts:
            continue
        selected.append(result)
        if host:
            seen_hosts.add(host)
        if len(selected) >= limit:
            return selected[:limit]

    for result in candidates:
        if result in selected:
            continue
        selected.append(result)
        if len(selected) >= limit:
            break
    return selected[:limit]


def _clean_research_text(text: str) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    for noise in (
        "Skip to main content",
        "Go to",
        "SelectionCalm",
        "r/openclaw",
        "Read more",
        "Continue reading",
    ):
        cleaned = cleaned.replace(noise, "").strip()
    return _trim_text_safely(cleaned, 600)


def _trim_text_safely(text: str, limit: int) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if len(cleaned) <= limit:
        return cleaned

    sentence_cut = max(cleaned.rfind(marker, 0, limit) for marker in (". ", "! ", "? ", "; "))
    if sentence_cut >= int(limit * 0.6):
        return f"{cleaned[: sentence_cut + 1].rstrip()}..."

    word_cut = cleaned.rfind(" ", 0, limit)
    if word_cut >= int(limit * 0.6):
        return f"{cleaned[:word_cut].rstrip()}..."

    return f"{cleaned[:limit].rstrip()}..."


def _strip_markdown_noise(text: str) -> str:
    cleaned = str(text or "")
    cleaned = cleaned.replace("**", "")
    cleaned = cleaned.replace("###", "")
    cleaned = cleaned.replace("##", "")
    cleaned = cleaned.replace("#", "")
    cleaned = cleaned.replace("`", "")
    cleaned = INLINE_CITATION_RE.sub("", cleaned)
    cleaned = re.sub(r"^\s*[-*]\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\n{2,}", "\n", cleaned)
    return " ".join(cleaned.split()).strip()


def _compact_research_summary(text: str, *, limit: int = 1200) -> str:
    cleaned = _strip_markdown_noise(text)
    cleaned = _trim_text_safely(cleaned, limit)
    return cleaned


def _parse_structured_summary(summary: Any) -> dict[str, Any]:
    if isinstance(summary, dict):
        return summary
    if isinstance(summary, list):
        return {"raw_summary": _clean_research_text(" ".join(str(item) for item in summary))}

    text = str(summary or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"raw_summary": _clean_research_text(text)}
    return parsed if isinstance(parsed, dict) else {"raw_summary": _clean_research_text(text)}


def _structured_items(summary: dict[str, Any], key: str) -> list[str]:
    values = summary.get(key)
    if not isinstance(values, list):
        return []
    cleaned: list[str] = []
    for item in values:
        text = _clean_research_text(str(item).strip())
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _format_exa_summary_snippet(title: str, summary: dict[str, Any]) -> str:
    explanations = _structured_items(summary, "term_explanations")
    milestone = _clean_research_text(str(summary.get("practical_milestone", "")).strip())
    notes = _structured_items(summary, "notes")
    raw_summary = _clean_research_text(str(summary.get("raw_summary", "")).strip())

    parts: list[str] = []
    if explanations:
        parts.append("; ".join(explanations[:2]))
    if milestone:
        parts.append(f"First milestone: {milestone}")
    if notes:
        parts.append(f"Notes: {'; '.join(notes[:2])}")
    if not parts and raw_summary:
        parts.append(raw_summary)

    snippet = " ".join(parts).strip()
    if title and snippet:
        return _clean_research_text(f"{title}: {snippet}")
    return snippet[:600]


def _aggregate_exa_summary(selected: list[dict[str, Any]], parsed_by_url: dict[str, dict[str, Any]]) -> str:
    explanations: list[str] = []
    milestones: list[str] = []
    notes: list[str] = []

    for result in selected:
        url = _result_url(result)
        parsed = parsed_by_url.get(url, {})
        for item in _structured_items(parsed, "term_explanations"):
            if item not in explanations:
                explanations.append(item)
        milestone = _clean_research_text(str(parsed.get("practical_milestone", "")).strip())
        if milestone and milestone not in milestones:
            milestones.append(milestone)
        for item in _structured_items(parsed, "notes"):
            if item not in notes:
                notes.append(item)

    parts: list[str] = []
    if explanations:
        parts.append(f"Grounded terms: {'; '.join(explanations[:3])}.")
    if milestones:
        parts.append(f"Practical first milestone: {milestones[0]}.")
    if notes:
        parts.append(f"Operational notes: {'; '.join(notes[:2])}.")
    return _trim_text_safely(" ".join(parts), 1200)


def _research_with_exa(answers: OnboardingAnswers, config: Config) -> ResearchBrief:
    if not config.exa_api_key:
        raise ValueError("EXA_API_KEY is not configured.")

    response = httpx.post(
        "https://api.exa.ai/search",
        headers={"x-api-key": config.exa_api_key},
        json={
            "query": _research_query(answers),
            "type": "auto",
            "numResults": 8,
            "excludeDomains": list(LOW_SIGNAL_SOURCE_DOMAINS),
        },
        timeout=30.0,
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results", []) if isinstance(payload, dict) else []
    selected = _prioritize_exa_results(results, limit=3)
    urls = [_result_url(result) for result in selected if _result_url(result)]

    summaries_by_url: dict[str, str] = {}
    if urls:
        contents_response = httpx.post(
            "https://api.exa.ai/contents",
            headers={"x-api-key": config.exa_api_key},
            json={
                "urls": urls,
                "summary": {
                    "query": _exa_summary_query(answers),
                    "schema": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "title": "QuestBoardGrounding",
                        "type": "object",
                        "properties": {
                            "term_explanations": {
                                "type": "array",
                                "items": {"type": "string"},
                                "maxItems": 4,
                            },
                            "practical_milestone": {"type": "string"},
                            "notes": {
                                "type": "array",
                                "items": {"type": "string"},
                                "maxItems": 4,
                            },
                        },
                        "required": ["term_explanations", "practical_milestone"],
                    },
                },
            },
            timeout=30.0,
        )
        contents_response.raise_for_status()
        contents_payload = contents_response.json()
        for item in contents_payload.get("results", []) if isinstance(contents_payload, dict) else []:
            if not isinstance(item, dict):
                continue
            item_url = _result_url(item)
            raw_summary = item.get("summary", "")
            if isinstance(raw_summary, (dict, list)):
                summary = json.dumps(raw_summary)
            else:
                summary = str(raw_summary).strip()
            if item_url and summary:
                summaries_by_url[item_url] = summary

    citations: list[str] = []
    snippets: list[str] = []
    parsed_by_url: dict[str, dict[str, Any]] = {}
    for result in selected:
        if not isinstance(result, dict):
            continue
        url = _result_url(result)
        title = str(result.get("title", "")).strip()
        structured_summary = _parse_structured_summary(summaries_by_url.get(url, ""))
        if url:
            parsed_by_url[url] = structured_summary
        snippet = _format_exa_summary_snippet(title, structured_summary)
        if title and snippet:
            snippets.append(snippet)
        elif snippet:
            snippets.append(snippet)
        if url:
            citations.append(url)

    brief = _aggregate_exa_summary(selected, parsed_by_url) or _trim_text_safely(" ".join(snippets), 1800)
    return ResearchBrief(provider="exa", summary=brief, citations=citations[:5])


def _research_with_perplexity(answers: OnboardingAnswers, config: Config) -> ResearchBrief:
    if not config.perplexity_api_key:
        raise ValueError("PERPLEXITY_API_KEY is not configured.")

    client = OpenAI(api_key=config.perplexity_api_key, base_url=PERPLEXITY_BASE_URL)
    response = client.chat.completions.create(
        model=PERPLEXITY_MODEL,
        max_tokens=500,
        temperature=0.1,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are grounding an ambiguous project goal for a personal productivity system. "
                    "Explain unfamiliar tools or concepts, keep it concise, and avoid inventing facts. "
                    "Return plain text with three short sections: Grounded terms, Practical first milestone, and Operational notes. "
                    "Do not use markdown headings, bullets, or inline citation markers."
                ),
            },
            {
                "role": "user",
                "content": _research_query(answers),
            },
        ],
    )

    message = _compact_research_summary(response.choices[0].message.content.strip(), limit=1100)
    dumped = response.model_dump() if hasattr(response, "model_dump") else {}
    search_results = dumped.get("search_results") or dumped.get("citations") or []
    citations: list[str] = []
    for item in search_results[:5]:
        if isinstance(item, dict):
            url = str(item.get("url", "")).strip()
            if url:
                citations.append(url)
        elif isinstance(item, str):
            citations.append(item)

    return ResearchBrief(provider="perplexity", summary=message, citations=citations[:5])
