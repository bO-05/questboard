"""Notion MCP client wrapper for QuestBoard."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from .config import Config, get_config
from .hosted_auth import maybe_refresh_hosted_access_token

logger = logging.getLogger(__name__)

TITLE_FIELDS = {"Name", "Quest", "Achievement", "Adventurer", "Week", "Run", "Item", "title"}
SELECT_FIELDS = {
    "Class",
    "Category",
    "Status",
    "Rarity",
    "Type",
    "Difficulty",
    "Source",
    "MVP Skill",
    "Preferred Challenge Style",
    "Generation Mode",
    "Review State",
    "Item Type",
    "Locked",
    "Replayable",
}
DATE_FIELDS = {"Joined", "Due Date", "Last Activity", "Unlocked At", "Completed At", "Period", "Started At", "Finished At", "Approved At"}

UUID36_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
UUID32_RE = re.compile(r"(?<![0-9a-fA-F])[0-9a-fA-F]{32}(?![0-9a-fA-F])")

# Self-hosted @notionhq/notion-mcp-server tool names (operationId style)
SELF_HOSTED_TOOLS = {
    "search": "post-search",
    "fetch": "retrieve-a-page",
    "create_page": "post-page",
    "update_page": "patch-page",
    "create_database": "create-a-data-source",
    "update_data_source": None,  # Not available on self-hosted
    "create_view": None,  # Not available on self-hosted
    "create_comment": "create-a-comment",
    "get_comments": None,  # Not available on self-hosted
    "move_page": "move-page",
    "get_users": "get-users",
    "get_self": "get-self",
}

# Hosted Notion MCP tool names (notion-* style)
HOSTED_TOOLS = {
    "search": "notion-search",
    "fetch": "notion-fetch",
    "create_page": "notion-create-pages",
    "update_page": "notion-update-page",
    "create_database": "notion-create-database",
    "update_data_source": "notion-update-data-source",
    "create_view": "notion-create-view",
    "create_comment": "notion-create-comment",
    "get_comments": "notion-get-comments",
    "move_page": "notion-move-pages",
    "get_users": "notion-get-users",
    "get_self": "notion-get-self",
}


class NotionMCP:
    """Wrapper around the MCP client for Notion operations."""

    def __init__(self, config: Config | None = None):
        self.config = config or get_config()
        self._session: ClientSession | None = None
        self._read_stream = None
        self._write_stream = None
        self._cm = None
        self._session_cm = None

    def _get_headers(self) -> dict[str, str]:
        headers = {"User-Agent": "QuestBoard-MCP-Client/0.1.0"}
        if self.config.is_self_hosted and self.config.mcp_auth_token:
            headers["Authorization"] = f"Bearer {self.config.mcp_auth_token}"
        elif self.config.mcp_access_token:
            headers["Authorization"] = f"Bearer {self.config.mcp_access_token}"
        return headers

    def _tool_name(self, operation: str) -> str | None:
        """Pick the right tool name based on server type."""
        tools = SELF_HOSTED_TOOLS if self.config.is_self_hosted else HOSTED_TOOLS
        return tools.get(operation)

    @staticmethod
    def _hosted_icon_payload(icon: Any) -> Any:
        if isinstance(icon, dict):
            return icon
        if isinstance(icon, str) and icon:
            return {"type": "emoji", "emoji": icon}
        return icon

    async def connect(self) -> "NotionMCP":
        try:
            maybe_refresh_hosted_access_token(self.config)
            self._cm = streamablehttp_client(
                self.config.mcp_server_url,
                headers=self._get_headers(),
            )
            self._read_stream, self._write_stream, _ = await self._cm.__aenter__()
            self._session_cm = ClientSession(self._read_stream, self._write_stream)
            self._session = await self._session_cm.__aenter__()
            await self._session.initialize()
            return self
        except BaseException:
            await self.disconnect()
            raise

    async def disconnect(self):
        try:
            if self._session_cm:
                try:
                    await self._session_cm.__aexit__(None, None, None)
                except BaseException as exc:
                    logger.debug("Ignoring MCP session close error: %s", exc)
            if self._cm:
                try:
                    await self._cm.__aexit__(None, None, None)
                except BaseException as exc:
                    logger.debug("Ignoring MCP transport close error: %s", exc)
        finally:
            self._session = None
            self._session_cm = None
            self._cm = None
            self._read_stream = None
            self._write_stream = None

    async def __aenter__(self):
        return await self.connect()

    async def __aexit__(self, *args):
        await self.disconnect()

    async def list_tools(self) -> list[str]:
        if self._session is None:
            raise RuntimeError("Not connected to the MCP server.")
        result = await self._session.list_tools()
        return [t.name for t in result.tools]

    async def call_tool(self, name: str, arguments: dict[str, Any] = None) -> dict:
        if self._session is None:
            raise RuntimeError("Not connected to the MCP server.")

        result = await self._session.call_tool(name, arguments=arguments or {})
        if result.structuredContent:
            payload = result.structuredContent
            self._raise_for_tool_error(payload)
            return payload

        if result.content:
            for block in result.content:
                if not hasattr(block, "text"):
                    continue
                if isinstance(block.text, str) and block.text.startswith("MCP error"):
                    raise RuntimeError(block.text)
                try:
                    payload = json.loads(block.text)
                except (json.JSONDecodeError, TypeError):
                    return {"text": block.text}
                self._raise_for_tool_error(payload)
                return payload

        return {}

    # --- High-level Notion operations ---

    async def search(self, query: str, **kwargs) -> dict:
        args: dict[str, Any] = {"query": query}
        if not self.config.is_self_hosted:
            args.update({key: value for key, value in kwargs.items() if value})
        return await self.call_tool(self._tool_name("search"), args)

    async def fetch_page(self, page_id: str) -> dict:
        tool = self._tool_name("fetch")
        if self.config.is_self_hosted:
            return await self.call_tool(tool, {"page_id": page_id})

        payload = await self.call_tool(tool, {"id": page_id})
        return self._normalize_hosted_fetch(payload, page_id)

    async def create_database(self, parent_id: str, title: str, properties: dict, description: str = "") -> dict:
        tool = self._tool_name("create_database")
        if self.config.is_self_hosted:
            payload: dict[str, Any] = {
                "parent": {"type": "page_id", "page_id": parent_id},
                "title": [{"type": "text", "text": {"content": title}}],
                "properties": {},
            }
            for name, prop in properties.items():
                prop_type = prop.get("type")
                if prop_type == "title":
                    payload["properties"][name] = {"title": {}}
                elif prop_type == "select":
                    opts = prop.get("options", [])
                    if opts and isinstance(opts[0], str):
                        opts = [{"name": opt} for opt in opts]
                    payload["properties"][name] = {"select": {"options": opts}}
                elif prop_type == "number":
                    payload["properties"][name] = {"number": {"format": "number"}}
                elif prop_type == "date":
                    payload["properties"][name] = {"date": {}}
                elif prop_type == "rich_text":
                    payload["properties"][name] = {"rich_text": {}}
                else:
                    payload["properties"][name] = prop
            if description:
                payload["description"] = [{"type": "text", "text": {"content": description}}]
            return await self.call_tool(tool, payload)

        args: dict[str, Any] = {
            "parent": {"page_id": parent_id},
            "title": title,
            "schema": self._properties_to_hosted_schema(properties),
        }
        if description:
            args["description"] = description
        payload = await self.call_tool(tool, args)
        return self._normalize_hosted_created_database(payload)

    async def create_page(
        self,
        parent_id: str,
        title: str,
        properties: dict = None,
        content_markdown: str = "",
        icon: str = "",
    ) -> dict:
        tool = self._tool_name("create_page")
        if self.config.is_self_hosted:
            payload: dict[str, Any] = {
                "parent": {"type": "page_id", "page_id": parent_id},
                "properties": {
                    "title": {
                        "title": [{"type": "text", "text": {"content": title}}],
                    }
                },
            }
            if icon:
                payload["icon"] = {"type": "emoji", "emoji": icon}
            if content_markdown:
                payload["children"] = [
                    {"type": "heading_1", "heading_1": {"rich_text": [{"type": "text", "text": {"content": title}}]}},
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": content_markdown}}]}},
                ]
            return await self.call_tool(tool, payload)

        page_payload: dict[str, Any] = {"properties": {"title": title}}
        if content_markdown:
            page_payload["content"] = content_markdown
        payload = await self.call_tool(
            tool,
            {
                "parent": {"page_id": parent_id},
                "pages": [page_payload],
            },
        )
        return self._normalize_hosted_created_pages(payload)

    async def create_db_page(self, database_id: str, properties: dict, content_markdown: str = "", icon: str = "") -> dict:
        tool = self._tool_name("create_page")
        if self.config.is_self_hosted:
            notion_props = self._convert_properties(properties)
            payload: dict[str, Any] = {
                "parent": {"type": "database_id", "database_id": database_id},
                "properties": notion_props,
            }
            if content_markdown:
                payload["children"] = [
                    {"type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": content_markdown}}]}}
                ]
            if icon:
                payload["icon"] = {"type": "emoji", "emoji": icon}
            return await self.call_tool(tool, payload)

        data_source_id = await self._resolve_hosted_data_source_id(database_id)
        page_payload: dict[str, Any] = {"properties": self._convert_hosted_properties(properties)}
        if content_markdown:
            page_payload["content"] = content_markdown
        payload = await self.call_tool(
            tool,
            {
                "parent": {"data_source_id": data_source_id},
                "pages": [page_payload],
            },
        )
        return self._normalize_hosted_created_pages(payload)

    async def update_page(self, page_id: str, properties: dict = None, content_markdown: str = None) -> dict:
        tool = self._tool_name("update_page")
        if self.config.is_self_hosted:
            payload: dict[str, Any] = {"page_id": page_id}
            if properties:
                payload["properties"] = self._convert_properties(properties)
            return await self.call_tool(tool, payload)

        result: dict[str, Any] = {"id": page_id, "page_id": page_id}
        if properties:
            result = await self.call_tool(
                tool,
                {
                    "page_id": page_id,
                    "command": "update_properties",
                    "properties": self._convert_hosted_properties(properties),
                },
            )
        if content_markdown is not None:
            result = await self.call_tool(
                tool,
                {
                    "page_id": page_id,
                    "command": "replace_content",
                    "new_str": content_markdown,
                },
            )
        if isinstance(result, dict) and "page_id" in result and "id" not in result:
            result["id"] = result["page_id"]
        return result

    async def create_view(self, database_id: str, view_type: str, name: str, config: dict = None) -> dict:
        tool = self._tool_name("create_view")
        if tool is None:
            logger.warning("create_view is not available on self-hosted MCP server")
            return {
                "warning": "View creation not available on self-hosted MCP server",
                "note": "Create views manually in the Notion UI",
            }

        database = await self.fetch_page(database_id)
        args: dict[str, Any] = {
            "database_id": database_id,
            "data_source_id": self._extract_data_source_id(database),
            "name": name,
            "type": view_type,
        }
        configure = self._view_config_to_hosted_dsl(config, view_type)
        if configure:
            args["configure"] = configure
        return await self.call_tool(tool, args)

    async def update_data_source(
        self,
        data_source_id: str,
        *,
        properties: dict | None = None,
        title: str | None = None,
    ) -> dict:
        tool = self._tool_name("update_data_source")
        if tool is None:
            logger.warning("update_data_source is not available on self-hosted MCP server")
            return {
                "warning": "Data source updates not available on self-hosted MCP server",
                "note": "Create a fresh workspace or update schema manually in the Notion UI",
            }

        args: dict[str, Any] = {"data_source_id": data_source_id}
        if properties:
            args["statements"] = self._properties_to_hosted_alter_statements(properties)
        if title:
            args["title"] = title
        return await self.call_tool(tool, args)

    async def create_comment(self, page_id: str, text: str) -> dict:
        tool = self._tool_name("create_comment")
        if self.config.is_self_hosted:
            return await self.call_tool(tool, {"page_id": page_id, "text": text})
        return await self.call_tool(
            tool,
            {
                "page_id": page_id,
                "rich_text": [{"type": "text", "text": {"content": text}}],
            },
        )

    async def get_comments(self, page_id: str) -> list[dict[str, Any]]:
        tool = self._tool_name("get_comments")
        if tool is None:
            return []
        payload = await self.call_tool(tool, {"page_id": page_id})
        if isinstance(payload, dict) and isinstance(payload.get("comments"), list):
            return payload["comments"]
        return self._parse_hosted_comments(payload.get("text", ""))

    async def move_page(self, page_id: str, new_parent_id: str) -> dict:
        tool = self._tool_name("move_page")
        if self.config.is_self_hosted:
            return await self.call_tool(tool, {"page_id": page_id, "new_parent_id": new_parent_id})
        return await self.call_tool(
            tool,
            {
                "page_or_database_ids": [page_id],
                "new_parent": {"page_id": new_parent_id},
            },
        )

    async def get_users(self) -> dict:
        return await self.call_tool(self._tool_name("get_users"))

    async def get_self(self) -> dict:
        tool = self._tool_name("get_self")
        if self.config.is_self_hosted:
            return await self.call_tool(tool)
        try:
            return await self.call_tool(tool)
        except RuntimeError as exc:
            if "Tool notion-get-self not found" not in str(exc):
                raise
            return await self.call_tool(self._tool_name("get_users"), {"user_id": "self"})

    @staticmethod
    def _convert_properties(properties: dict) -> dict:
        """Convert simple property values to Notion API format for self-hosted server."""
        notion_props: dict[str, Any] = {}
        for name, value in properties.items():
            if name in ("Name", "Quest", "Achievement", "Adventurer", "Week"):
                notion_props[name] = {"title": [{"type": "text", "text": {"content": str(value)}}]}
            elif name == "Skill":
                if isinstance(value, str) and ("🛡" in value or "🌳" in value or " " in value):
                    notion_props[name] = {"title": [{"type": "text", "text": {"content": value}}]}
                else:
                    notion_props[name] = {"select": {"name": value}}
            elif isinstance(value, int):
                notion_props[name] = {"number": value}
            elif isinstance(value, dict):
                notion_props[name] = value
            elif name in SELECT_FIELDS:
                notion_props[name] = {"select": {"name": value}}
            elif name in DATE_FIELDS:
                notion_props[name] = {"date": {"start": value}}
            elif isinstance(value, str) and len(value) > 0:
                notion_props[name] = {"rich_text": [{"type": "text", "text": {"content": value}}]}
            else:
                notion_props[name] = {"rich_text": [{"type": "text", "text": {"content": str(value)}}]}
        return notion_props

    @staticmethod
    def _raise_for_tool_error(payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("text", "").startswith("MCP error"):
            raise RuntimeError(payload["text"])
        if isinstance(payload.get("error"), str):
            raise RuntimeError(payload["error"])
        if payload.get("name") == "APIResponseError" or ("code" in payload and "status" in payload and "body" in payload):
            message = payload.get("body") or payload.get("message") or payload.get("code") or "Tool call failed"
            try:
                decoded = json.loads(message)
                message = decoded.get("message") or decoded.get("code") or message
            except (json.JSONDecodeError, TypeError):
                pass
            raise RuntimeError(str(message))

    @staticmethod
    def _extract_uuid(value: str) -> str:
        text = str(value or "")
        match = UUID36_RE.search(text)
        if match:
            return match.group(0).lower()
        match = UUID32_RE.search(text)
        if not match:
            return ""
        raw = match.group(0).lower()
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"

    @staticmethod
    def _extract_tag_content(text: str, tag: str) -> str:
        match = re.search(rf"<{tag}>\s*(.*?)\s*</{tag}>", text or "", re.S)
        return match.group(1).strip() if match else ""

    def _normalize_hosted_fetch(self, payload: dict, requested_id: str) -> dict:
        if not isinstance(payload, dict):
            return payload
        if payload.get("id") and payload.get("properties"):
            return payload

        metadata_type = (payload.get("metadata") or {}).get("type")
        text = payload.get("text", "")
        title = payload.get("title", "")
        url = payload.get("url", "")

        if metadata_type == "page":
            simple_props = self._parse_json_block(text, "properties") or {"title": title}
            return {
                "id": self._extract_uuid(url or requested_id),
                "object": "page",
                "url": url,
                "title": [{"plain_text": title}] if title else [],
                "properties": self._normalize_simple_properties(simple_props),
                "content_markdown": self._extract_page_content(text),
                "parent": self._extract_page_parent(text),
            }

        if metadata_type in {"database", "data_source"}:
            state = self._parse_json_block(text, "data-source-state") or {}
            data_source_url = state.get("url") or self._extract_data_source_url(text)
            data_source_id = self._extract_uuid(data_source_url or requested_id)
            database_id = self._extract_uuid(url or requested_id)
            if metadata_type == "data_source":
                database_id = self._extract_uuid(url)

            response = {
                "id": database_id if metadata_type == "database" else data_source_id,
                "object": metadata_type,
                "url": url,
                "title": [{"plain_text": title}] if title else [],
                "properties": self._schema_to_property_map(state.get("schema") or {}),
                "data_source_id": data_source_id,
                "data_source_url": data_source_url,
            }
            if metadata_type == "database":
                response["default_data_source_id"] = data_source_id
                response["data_sources"] = [{"id": data_source_id, "name": state.get("name", title)}] if data_source_id else []
            else:
                response["database_id"] = database_id
            return response

        return payload

    @staticmethod
    def _normalize_hosted_created_pages(payload: dict) -> dict:
        pages = payload.get("pages")
        if isinstance(pages, list) and pages:
            page = pages[0]
            if isinstance(page, dict) and page.get("id"):
                return {"id": page["id"], **page}
        return payload

    def _normalize_hosted_created_database(self, payload: dict) -> dict:
        if payload.get("id"):
            return payload

        text = payload.get("result", "")
        database_url_match = re.search(r'<database url="\{\{([^}]+)\}\}"', text)
        data_source_url = self._extract_data_source_url(text)
        state = self._parse_json_block(text, "data-source-state") or {}
        database_url = database_url_match.group(1) if database_url_match else ""
        return {
            "id": self._extract_uuid(database_url),
            "object": "database",
            "url": database_url,
            "title": [{"plain_text": state.get("name", "")}] if state.get("name") else [],
            "properties": self._schema_to_property_map(state.get("schema") or {}),
            "data_source_id": self._extract_uuid(data_source_url),
            "data_source_url": data_source_url,
            "data_sources": [{"id": self._extract_uuid(data_source_url), "name": state.get("name", "")}] if data_source_url else [],
        }

    @staticmethod
    def _parse_json_block(text: str, tag: str) -> dict | None:
        block = NotionMCP._extract_tag_content(text, tag)
        if not block:
            return None
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _extract_page_content(text: str) -> str:
        if "<blank-page>" in (text or "") or "<empty-block/>" in (text or ""):
            return ""
        content = NotionMCP._extract_tag_content(text, "content")
        return content

    def _extract_page_parent(self, text: str) -> dict[str, str]:
        parent: dict[str, str] = {}
        data_source_match = re.search(r'<parent-data-source url="collection://([^"]+)"', text or "")
        if data_source_match:
            parent["data_source_id"] = self._extract_uuid(data_source_match.group(1))

        database_match = re.search(r'<ancestor-\d+-database url="([^"]+)"', text or "")
        if database_match:
            parent["database_id"] = self._extract_uuid(database_match.group(1))

        page_match = re.search(r'<parent-page url="([^"]+)"', text or "")
        if page_match:
            parent["page_id"] = self._extract_uuid(page_match.group(1))

        return parent

    @staticmethod
    def _extract_data_source_url(text: str) -> str:
        match = re.search(r"collection://[0-9a-fA-F-]{36}", text or "")
        return match.group(0) if match else ""

    @staticmethod
    def _parse_hosted_comments(text: str) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        for match in re.finditer(
            r'<comment id="([^"]+)"(?:[^>]*datetime="([^"]+)")?[^>]*>(.*?)</comment>',
            text or "",
            re.S,
        ):
            comments.append(
                {
                    "id": match.group(1),
                    "datetime": match.group(2) or "",
                    "text": match.group(3).strip(),
                }
            )
        return comments

    @staticmethod
    def _schema_to_property_map(schema: dict[str, Any]) -> dict[str, Any]:
        mapped: dict[str, Any] = {}
        for name, definition in schema.items():
            if not isinstance(definition, dict):
                mapped[name] = definition
                continue
            prop_type = definition.get("type", "rich_text")
            if prop_type == "select":
                mapped[name] = {
                    "type": "select",
                    "options": [opt.get("name", "") for opt in definition.get("options", []) if isinstance(opt, dict)],
                }
            elif prop_type == "formula":
                mapped[name] = {
                    "type": "formula",
                    "expression": definition.get("expression", ""),
                }
            elif prop_type == "rollup":
                mapped[name] = {
                    "type": "rollup",
                    "relation_property_name": definition.get("relation_property_name", ""),
                    "rollup_property_name": definition.get("rollup_property_name", ""),
                    "function": definition.get("function", ""),
                }
            elif prop_type == "relation":
                mapped[name] = {
                    "type": "relation",
                    "data_source_id": (
                        ((definition.get("relation") or {}).get("data_source_id"))
                        or definition.get("data_source_id", "")
                    ),
                }
            else:
                mapped[name] = {"type": prop_type}
        return mapped

    def _normalize_simple_properties(self, props: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        date_parts: dict[str, dict[str, Any]] = {}
        for raw_name, value in props.items():
            name = raw_name.split(":", 1)[1] if raw_name.lower().startswith("userdefined:") else raw_name
            if name.startswith("date:"):
                _, field_name, part = name.split(":", 2)
                date_parts.setdefault(field_name, {})[part] = value
                continue

            if isinstance(value, dict):
                normalized[name] = value
                continue

            if name == "Skill":
                text = str(value)
                if " " in text or any(ch in text for ch in ("🛡", "🌳", "💪", "🧠", "🗣", "🎨", "🏃", "📖")):
                    normalized[name] = {"title": [{"plain_text": text}]}
                else:
                    normalized[name] = {"select": {"name": text}}
                continue

            if name in TITLE_FIELDS:
                normalized[name] = {"title": [{"plain_text": str(value)}]}
            elif isinstance(value, (int, float)):
                normalized[name] = {"number": value}
            elif isinstance(value, str) and value in {"__YES__", "__NO__"}:
                normalized[name] = {"checkbox": value == "__YES__"}
            elif name in DATE_FIELDS and isinstance(value, str):
                normalized[name] = {"date": {"start": value}}
            elif name in SELECT_FIELDS:
                normalized[name] = {"select": {"name": str(value)}}
            elif value is None:
                normalized[name] = {"rich_text": []}
            else:
                normalized[name] = {"rich_text": [{"plain_text": str(value)}]}

        for field_name, parts in date_parts.items():
            start = parts.get("start")
            if start:
                entry = {"start": start}
                if parts.get("end"):
                    entry["end"] = parts["end"]
                normalized[field_name] = {"date": entry}

        return normalized

    @staticmethod
    def _convert_hosted_properties(properties: dict[str, Any]) -> dict[str, Any]:
        hosted_props: dict[str, Any] = {}
        for name, value in properties.items():
            if isinstance(value, dict):
                hosted_props[name] = value
                continue

            safe_name = f"userDefined:{name}" if name.lower() in {"id", "url"} else name
            if isinstance(value, bool):
                hosted_props[safe_name] = "__YES__" if value else "__NO__"
            elif isinstance(value, (int, float)) or value is None:
                hosted_props[safe_name] = value
            elif safe_name in DATE_FIELDS and isinstance(value, str):
                hosted_props[f"date:{safe_name}:start"] = value
                hosted_props[f"date:{safe_name}:is_datetime"] = 0
            else:
                hosted_props[safe_name] = value
        return hosted_props

    async def _resolve_hosted_data_source_id(self, database_id: str) -> str:
        database = await self.fetch_page(database_id)
        data_source_id = self._extract_data_source_id(database)
        if not data_source_id:
            raise RuntimeError(f"Could not resolve a hosted data source ID for {database_id}")
        return data_source_id

    @staticmethod
    def _extract_data_source_id(database: dict[str, Any]) -> str:
        if not isinstance(database, dict):
            return ""
        for key in ("data_source_id", "default_data_source_id"):
            value = database.get(key)
            if isinstance(value, str) and value:
                return value
        for item in database.get("data_sources", []):
            if isinstance(item, dict) and item.get("id"):
                return item["id"]
        if database.get("object") == "data_source":
            return database.get("id", "")
        return ""

    @staticmethod
    def _escape_sql_name(name: str) -> str:
        return str(name).replace('"', '""')

    @staticmethod
    def _escape_sql_string(value: str) -> str:
        return str(value).replace("'", "''")

    def _property_to_hosted_sql_type(self, definition: dict[str, Any]) -> str:
        prop_type = (definition or {}).get("type", "rich_text")
        if prop_type == "title":
            return "TITLE"
        if prop_type == "rich_text":
            return "RICH_TEXT"
        if prop_type == "text":
            return "RICH_TEXT"
        if prop_type == "date":
            return "DATE"
        if prop_type == "number":
            return "NUMBER"
        if prop_type == "select":
            options = definition.get("options") or []
            rendered = []
            for option in options:
                if isinstance(option, dict):
                    label = option.get("name", "")
                else:
                    label = str(option)
                rendered.append(f"'{self._escape_sql_string(label)}':default")
            return f"SELECT({', '.join(rendered)})" if rendered else "SELECT()"
        if prop_type == "formula":
            expression = definition.get("expression", "0")
            return f"FORMULA('{self._escape_sql_string(expression)}')"
        if prop_type == "rollup":
            relation_name = definition.get("relation_property_name", "")
            rollup_name = definition.get("rollup_property_name", "")
            function_name = definition.get("function", "show_original")
            return (
                f"ROLLUP('{self._escape_sql_string(relation_name)}', "
                f"'{self._escape_sql_string(rollup_name)}', "
                f"'{self._escape_sql_string(function_name)}')"
            )
        if prop_type == "relation":
            target = definition.get("data_source_id", "")
            dual = definition.get("dual_property_name", "")
            dual_id = definition.get("dual_property_id", "")
            if dual and dual_id:
                return (
                    f"RELATION('{self._escape_sql_string(target)}', "
                    f"DUAL '{self._escape_sql_string(dual)}' "
                    f"'{self._escape_sql_string(dual_id)}')"
                )
            if dual:
                return f"RELATION('{self._escape_sql_string(target)}', DUAL '{self._escape_sql_string(dual)}')"
            return f"RELATION('{self._escape_sql_string(target)}')"
        return "RICH_TEXT"

    def _properties_to_hosted_schema(self, properties: dict[str, Any]) -> str:
        columns = []
        for name, definition in properties.items():
            columns.append(f"\"{self._escape_sql_name(name)}\" {self._property_to_hosted_sql_type(definition)}")
        return f"CREATE TABLE ({', '.join(columns)})"

    def _properties_to_hosted_alter_statements(self, properties: dict[str, Any]) -> str:
        statements = []
        for name, definition in properties.items():
            statements.append(f"ADD COLUMN \"{self._escape_sql_name(name)}\" {self._property_to_hosted_sql_type(definition)}")
        return "; ".join(statements)

    @staticmethod
    def _view_config_to_hosted_dsl(config: dict[str, Any] | None, view_type: str) -> str | None:
        if not config:
            return None
        clauses: list[str] = []
        if config.get("configure"):
            clauses.append(str(config["configure"]))
        if view_type == "chart":
            chart_type = config.get("chart") or "column"
            clauses.append(f"CHART {chart_type}")
        if config.get("group_by"):
            clauses.append(f'GROUP BY "{config["group_by"]}"')
        if config.get("calendar_by"):
            clauses.append(f'CALENDAR BY "{config["calendar_by"]}"')
        if config.get("timeline_by"):
            clauses.append(f'TIMELINE BY "{config["timeline_by"]}"')
        if not clauses and view_type == "board" and config.get("groupBy"):
            clauses.append(f'GROUP BY "{config["groupBy"]}"')
        return "; ".join(clauses) if clauses else None
