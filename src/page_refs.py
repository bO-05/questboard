"""Helpers for normalizing Notion page references from URLs or raw IDs."""

from __future__ import annotations

import re


PAGE_ID_RE = re.compile(r"([0-9a-fA-F]{32}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})")


def normalize_page_reference(value: str) -> str:
    """Return a hyphenated Notion page ID from a raw ID or Notion URL."""
    if not value or not value.strip():
        raise ValueError("A Notion page reference is required.")

    candidate = value.strip()
    match = PAGE_ID_RE.search(candidate)
    if not match:
        raise ValueError(
            "Could not find a valid Notion page ID. Paste a 32-character page ID, a hyphenated UUID, or a full Notion URL."
        )

    page_id = match.group(1).replace("-", "").lower()
    if len(page_id) != 32:
        raise ValueError("The Notion page ID must contain 32 hex characters.")

    return f"{page_id[:8]}-{page_id[8:12]}-{page_id[12:16]}-{page_id[16:20]}-{page_id[20:]}"
