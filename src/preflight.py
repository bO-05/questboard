"""Preflight checks - validate QuestBoard connectivity and write paths."""

import socket
import time
from urllib.parse import urlparse

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import SKILL_TREES, get_config
from .mcp_client import NotionMCP
from .setup_workspace import (
    ACHIEVEMENT_LOG_PROPS,
    ADVENTURE_RECAP_PROPS,
    DATABASE_BLUEPRINTS,
    PARTY_BOARD_PROPS,
    PLAYER_PROFILE_PROPS,
    QUEST_BOARD_PROPS,
    QUESTBOARD_HOSTED_VIEW_TYPES,
    REVIEW_QUEUE_PROPS,
    RUNS_PROPS,
    SKILL_TREE_PROPS,
    load_workspace_state,
)
from .workspace_data import (
    get_party_page,
    get_player_page,
    get_quest_pages,
    get_skill_pages,
    normalize_player,
    normalize_quest,
    normalize_skill,
)

console = Console()


class CheckResult:
    def __init__(self, name: str, passed: bool, message: str, duration_ms: float = 0):
        self.name = name
        self.passed = passed
        self.message = message
        self.duration_ms = duration_ms


def _probe_mcp_socket(server_url: str, timeout_s: float = 1.5) -> str | None:
    parsed = urlparse(server_url or "")
    host = parsed.hostname
    if not host:
        return "MCP server URL is missing a hostname"

    if parsed.port:
        port = parsed.port
    elif parsed.scheme == "https":
        port = 443
    else:
        port = 80

    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return None
    except OSError:
        return f"Cannot reach MCP server at {host}:{port}. Is it running and reachable?"


async def run_all_checks(parent_page_id: str = None) -> list[CheckResult]:
    """Run all preflight checks and return results."""
    results = []
    config = get_config()

    results.append(_check_env_vars(config))

    result = await _check_mcp_connection(config)
    results.append(result)
    if not result.passed:
        console.print("[red]Cannot proceed without MCP connection.[/red]")
        return results

    results.append(await _check_mcp_tools(config))

    if parent_page_id:
        results.append(await _check_create_database(config, parent_page_id))
        results.append(await _check_view_types(config, parent_page_id))
        results.append(await _check_page_crud(config, parent_page_id))
        results.append(await _check_comments(config, parent_page_id))

    results.append(await _check_search(config))

    if config.llm_provider == "anthropic" and config.anthropic_api_key:
        results.append(await _check_anthropic(config))
    elif config.llm_provider == "openai" and config.openai_api_key:
        results.append(await _check_llm(config))

    return results


async def run_doctor_checks(parent_page_id: str = None) -> list[CheckResult]:
    """Run non-destructive readiness checks for setup and workspace reuse."""
    results = []
    config = get_config()

    results.append(_check_env_vars(config))

    result = await _check_mcp_connection(config)
    results.append(result)
    if not result.passed:
        console.print("[red]Cannot proceed without MCP connection.[/red]")
        return results

    results.append(await _check_mcp_tools(config))
    results.append(await _check_search(config))

    if parent_page_id:
        results.append(await _check_page_access(config, parent_page_id))

    workspace_state = load_workspace_state()
    metadata_result = _check_workspace_metadata(config, workspace_state, parent_page_id)
    results.append(metadata_result)

    workspace_matches_target = _workspace_matches_target(config, workspace_state, parent_page_id)
    if workspace_state and metadata_result.passed and workspace_matches_target:
        results.append(await _check_workspace_schema(config, workspace_state))
        results.extend(await _check_workspace_contents(config, workspace_state))
    elif workspace_state and metadata_result.passed and not workspace_matches_target:
        results.extend(_workspace_skip_results())

    if config.llm_provider == "anthropic" and config.anthropic_api_key:
        results.append(await _check_anthropic(config))
    elif config.llm_provider == "openai" and config.openai_api_key:
        results.append(await _check_llm(config))

    return results


def _check_env_vars(config) -> CheckResult:
    """Check all required environment variables are set."""
    missing = []
    optional = []
    if config.is_self_hosted:
        if not config.notion_token and not config.mcp_auth_token:
            missing.append("NOTION_TOKEN or MCP_AUTH_TOKEN")
    elif not config.mcp_access_token:
        missing.append("MCP_ACCESS_TOKEN (run `questboard hosted-login`)")
    if not config.mcp_server_url:
        missing.append("MCP_SERVER_URL")
    if config.llm_provider == "anthropic" and not config.anthropic_api_key:
        optional.append("ANTHROPIC_API_KEY")
    elif config.llm_provider == "openai" and not config.openai_api_key:
        optional.append("OPENAI_API_KEY")

    if missing:
        return CheckResult("Environment Variables", False, f"Missing: {', '.join(missing)}")
    if optional:
        return CheckResult(
            "Environment Variables",
            True,
            f"Core config is set; AI checks will be skipped until {', '.join(optional)} is provided",
        )
    return CheckResult("Environment Variables", True, "All set")


async def _check_mcp_connection(config) -> CheckResult:
    """Test MCP server connectivity."""
    start = time.monotonic()
    probe_error = _probe_mcp_socket(config.mcp_server_url)
    if probe_error:
        duration = (time.monotonic() - start) * 1000
        return CheckResult("MCP Connection", False, probe_error, duration)
    try:
        async with NotionMCP(config) as mcp:
            await mcp.get_self()
            duration = (time.monotonic() - start) * 1000
            return CheckResult("MCP Connection", True, f"Connected in {duration:.0f}ms", duration)
    except BaseException as e:
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        duration = (time.monotonic() - start) * 1000
        message = str(e)[:100]
        if e.__class__.__name__ == "CancelledError":
            message = "Connection attempt was cancelled while opening the MCP transport"
        return CheckResult("MCP Connection", False, f"Failed: {message}", duration)


async def _check_mcp_tools(config) -> CheckResult:
    """Verify all required MCP tools are available."""
    if config.is_self_hosted:
        server_type = "self-hosted"
        required_tools = [
            "post-search",
            "retrieve-a-page",
            "post-page",
            "patch-page",
            "create-a-data-source",
            "create-a-comment",
            "move-page",
            "get-users",
            "get-self",
        ]
    else:
        server_type = "hosted (mcp.notion.com)"
        required_tools = [
            "notion-search",
            "notion-fetch",
            "notion-create-pages",
            "notion-update-page",
            "notion-create-database",
            "notion-create-view",
            "notion-create-comment",
            "notion-move-pages",
            "notion-get-users",
            "notion-get-teams",
            "notion-update-data-source",
        ]

    try:
        async with NotionMCP(config) as mcp:
            tools = await mcp.list_tools()
            missing = [tool for tool in required_tools if tool not in tools]
            if missing:
                return CheckResult("MCP Tools", False, f"[{server_type}] Missing tools: {', '.join(missing)}")
            return CheckResult(
                "MCP Tools",
                True,
                f"[{server_type}] {len(tools)} tools available, all {len(required_tools)} required tools found",
            )
    except Exception as e:
        return CheckResult("MCP Tools", False, f"Error: {str(e)[:100]}")


async def _check_create_database(config, parent_id: str) -> CheckResult:
    """Test database creation."""
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            result = await mcp.create_database(
                parent_id=parent_id,
                title="Preflight Test DB (safe to delete)",
                properties={"Name": {"type": "title"}, "Score": {"type": "number"}},
                description="Created by QuestBoard preflight check",
            )
            duration = (time.monotonic() - start) * 1000
            db_id = result.get("id", "unknown")
            return CheckResult("Database Creation", True, f"Created test DB ({db_id[:8]}...) in {duration:.0f}ms", duration)
    except Exception as e:
        return CheckResult("Database Creation", False, f"Failed: {str(e)[:100]}")


async def _check_view_types(config, parent_id: str) -> CheckResult:
    """Test that QuestBoard's hosted view suite can be created."""
    if config.is_self_hosted:
        return CheckResult("View Types", True, "N/A (self-hosted) - view creation not available via self-hosted MCP server")

    view_types = list(QUESTBOARD_HOSTED_VIEW_TYPES)
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            db = await mcp.create_database(
                parent_id=parent_id,
                title="View Test DB (safe to delete)",
                properties={
                    "Name": {"type": "title"},
                    "Status": {"type": "select", "options": ["Available", "Completed"]},
                    "Due Date": {"type": "date"},
                    "XP Reward": {"type": "number"},
                },
            )
            db_id = db.get("id", "")

            created = []
            failed = []
            for view_type in view_types:
                try:
                    view_config = None
                    if view_type == "board":
                        view_config = {"group_by": "Status"}
                    elif view_type == "calendar":
                        view_config = {"calendar_by": "Due Date"}
                    await mcp.create_view(db_id, view_type, f"Test {view_type}", view_config)
                    created.append(view_type)
                except Exception:
                    failed.append(view_type)

            duration = (time.monotonic() - start) * 1000
            if failed:
                return CheckResult(
                    "View Types",
                    False,
                    f"Created {len(created)}/{len(view_types)} QuestBoard hosted view types. Failed: {', '.join(failed)}",
                    duration,
                )
            return CheckResult(
                "View Types",
                True,
                f"All {len(view_types)} QuestBoard hosted view types created successfully in {duration:.0f}ms",
                duration,
            )
    except Exception as e:
        return CheckResult("View Types", False, f"Failed: {str(e)[:100]}")


async def _check_page_crud(config, parent_id: str) -> CheckResult:
    """Test database page create, read, update cycle."""
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            db = await mcp.create_database(
                parent_id=parent_id,
                title="CRUD Test DB (safe to delete)",
                properties={
                    "Quest": {"type": "title"},
                    "Status": {"type": "select", "options": ["Available", "Tested"]},
                },
                description="Created by QuestBoard preflight CRUD check",
            )
            db_id = db.get("id", "")

            page = await mcp.create_db_page(
                db_id,
                {
                    "Quest": "Preflight Test Quest",
                    "Status": "Available",
                },
                icon="🧪",
            )
            page_id = page.get("id", "")
            fetched = await mcp.fetch_page(page_id)
            if not page_id or fetched.get("id") != page_id:
                return CheckResult("Page CRUD", False, "Failed to fetch the created page")

            await mcp.update_page(page_id, {"Status": "Tested"})

            duration = (time.monotonic() - start) * 1000
            return CheckResult("Page CRUD", True, f"Database page create/read/update OK in {duration:.0f}ms", duration)
    except Exception as e:
        return CheckResult("Page CRUD", False, f"Failed: {str(e)[:100]}")


async def _check_comments(config, parent_id: str) -> CheckResult:
    """Test comment creation."""
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            page = await mcp.create_page(parent_id=parent_id, title="Comment Test")
            page_id = page.get("id", "")
            await mcp.create_comment(page_id, "Preflight check: comments work!")
            duration = (time.monotonic() - start) * 1000
            return CheckResult("Comments", True, f"Comment created in {duration:.0f}ms", duration)
    except Exception as e:
        return CheckResult("Comments", False, f"Failed: {str(e)[:100]}")


async def _check_search(config) -> CheckResult:
    """Test search functionality."""
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            results = await mcp.search("test")
            duration = (time.monotonic() - start) * 1000
            count = len(results.get("results", []))
            return CheckResult("Search", True, f"Search returned {count} results in {duration:.0f}ms", duration)
    except Exception as e:
        return CheckResult("Search", False, f"Failed: {str(e)[:100]}")


async def _check_page_access(config, page_id: str) -> CheckResult:
    """Verify that the target parent page is reachable."""
    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            page = await mcp.fetch_page(page_id)
        duration = (time.monotonic() - start) * 1000
        if not page.get("id"):
            return CheckResult("Target Page Access", False, "Could not read the target Notion page", duration)
        return CheckResult("Target Page Access", True, f"Page is reachable ({page_id[:8]}...) in {duration:.0f}ms", duration)
    except Exception as e:
        return CheckResult("Target Page Access", False, f"Failed: {str(e)[:100]}")


def _check_workspace_metadata(config, workspace_state: dict | None, parent_page_id: str | None) -> CheckResult:
    """Validate saved workspace metadata before deeper checks."""
    if not workspace_state:
        return CheckResult(
            "Workspace Metadata",
            True,
            "N/A - no local workspace metadata yet. Run `questboard setup <PAGE_ID_OR_URL>` when you are ready to create a workspace.",
        )

    saved_server = workspace_state.get("server_url")
    if saved_server and saved_server != config.mcp_server_url:
        return CheckResult(
            "Workspace Metadata",
            False,
            "Local workspace metadata points to a different MCP server. Switch `MCP_SERVER_URL` or rerun "
            "`questboard setup <PAGE_ID_OR_URL> --force-new`.",
        )

    target_note = ""
    saved_parent = workspace_state.get("parent_page_id")
    if parent_page_id and saved_parent and saved_parent != parent_page_id:
        target_note = " Local workspace points to a different parent page; run setup with `--force-new` for a fresh workspace page."

    required_databases = {db_key for db_key, *_ in DATABASE_BLUEPRINTS}
    saved_databases = set((workspace_state.get("databases") or {}).keys())
    missing_databases = sorted(required_databases - saved_databases)
    if missing_databases and not target_note:
        return CheckResult(
            "Workspace Metadata",
            False,
            f"Local workspace metadata is incomplete. Missing database IDs for: {', '.join(missing_databases)}",
        )

    player_name = workspace_state.get("player_name") or config.player_name
    return CheckResult(
        "Workspace Metadata",
        True,
        f"Saved workspace found for hero `{player_name}` with {len(saved_databases)} database IDs.{target_note}",
    )


def _workspace_matches_target(config, workspace_state: dict | None, parent_page_id: str | None) -> bool:
    if not workspace_state:
        return False

    saved_server = workspace_state.get("server_url")
    if saved_server and saved_server != config.mcp_server_url:
        return False

    saved_parent = workspace_state.get("parent_page_id")
    if parent_page_id and saved_parent and saved_parent != parent_page_id:
        return False

    return True


def _workspace_skip_results() -> list[CheckResult]:
    message = "N/A - saved workspace belongs to a different parent page. Run `questboard setup <PAGE_ID_OR_URL> --force-new` on this page for a fresh workspace."
    return [
        CheckResult("Workspace Schema", True, message),
        CheckResult("Workspace Hub", True, message),
        CheckResult("Player Profile", True, message),
        CheckResult("Skill Trees", True, message),
        CheckResult("Quest Inventory", True, message),
        CheckResult("Party Board", True, message),
        CheckResult("Runs", True, message),
        CheckResult("Review Queue", True, message),
    ]


async def _check_workspace_contents(config, workspace_state: dict) -> list[CheckResult]:
    """Inspect the saved workspace without mutating it."""
    db_ids = workspace_state.get("databases") or {}
    player_name = workspace_state.get("player_name") or config.player_name
    results: list[CheckResult] = []

    async with NotionMCP(config) as mcp:
        hub_start = time.monotonic()
        try:
            hub = await mcp.fetch_page(workspace_state.get("hub_id", ""))
            hub_duration = (time.monotonic() - hub_start) * 1000
            if hub.get("id"):
                results.append(CheckResult("Workspace Hub", True, f"Hub page is reachable in {hub_duration:.0f}ms", hub_duration))
            else:
                results.append(CheckResult("Workspace Hub", False, "Saved QuestBoard hub page is no longer accessible", hub_duration))
        except Exception as e:
            hub_duration = (time.monotonic() - hub_start) * 1000
            results.append(CheckResult("Workspace Hub", False, f"Failed: {str(e)[:100]}", hub_duration))
            return results

        player_start = time.monotonic()
        try:
            player_page = await get_player_page(mcp, db_ids, player_name)
            player_duration = (time.monotonic() - player_start) * 1000
            if not player_page:
                results.append(CheckResult("Player Profile", False, "No player profile row found in the saved workspace", player_duration))
            else:
                player = normalize_player(player_page)
                results.append(
                    CheckResult(
                        "Player Profile",
                        True,
                        f"Hero `{player.get('name') or player_name}` at level {player['level']} with {player['total_xp']} XP",
                        player_duration,
                    )
                )
        except Exception as e:
            player_duration = (time.monotonic() - player_start) * 1000
            results.append(CheckResult("Player Profile", False, f"Failed: {str(e)[:100]}", player_duration))

        skills_start = time.monotonic()
        try:
            skill_pages = await get_skill_pages(mcp, db_ids)
            skills_duration = (time.monotonic() - skills_start) * 1000
            skill_names = set()
            for page in skill_pages:
                skill_name = normalize_skill(page).get("skill")
                if skill_name:
                    skill_names.add(skill_name)
            missing_skills = sorted(set(SKILL_TREES.keys()) - skill_names)
            if missing_skills:
                results.append(
                    CheckResult(
                        "Skill Trees",
                        False,
                        f"Missing skill entries for: {', '.join(missing_skills)}",
                        skills_duration,
                    )
                )
            else:
                results.append(CheckResult("Skill Trees", True, f"All {len(skill_names)} skill trees are present", skills_duration))
        except Exception as e:
            skills_duration = (time.monotonic() - skills_start) * 1000
            results.append(CheckResult("Skill Trees", False, f"Failed: {str(e)[:100]}", skills_duration))

        quests_start = time.monotonic()
        try:
            quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
            quests_duration = (time.monotonic() - quests_start) * 1000
            available = sum(1 for quest in quests if quest.get("status") == "Available")
            starter = sum(1 for quest in quests if quest.get("source") == "Player")
            if not quests:
                results.append(CheckResult("Quest Inventory", False, "Quest Board has no quests yet", quests_duration))
            elif available == 0:
                results.append(CheckResult("Quest Inventory", False, "No available quests found in the saved workspace", quests_duration))
            elif starter == 0:
                results.append(CheckResult("Quest Inventory", False, "No starter quests found; the guided walkthrough will be sparse", quests_duration))
            else:
                results.append(
                    CheckResult(
                        "Quest Inventory",
                        True,
                        f"{len(quests)} quests found ({available} available, {starter} starter quests)",
                        quests_duration,
                    )
                )
        except Exception as e:
            quests_duration = (time.monotonic() - quests_start) * 1000
            results.append(CheckResult("Quest Inventory", False, f"Failed: {str(e)[:100]}", quests_duration))

        party_start = time.monotonic()
        try:
            party_page = await get_party_page(mcp, db_ids, player_name)
            party_duration = (time.monotonic() - party_start) * 1000
            if not party_page:
                results.append(CheckResult("Party Board", False, "No party board row found for the current hero", party_duration))
            else:
                results.append(CheckResult("Party Board", True, "Party board row is present", party_duration))
        except Exception as e:
            party_duration = (time.monotonic() - party_start) * 1000
            results.append(CheckResult("Party Board", False, f"Failed: {str(e)[:100]}", party_duration))

    return results


async def _check_workspace_schema(config, workspace_state: dict) -> CheckResult:
    """Verify that the saved workspace contains the current recommended schema."""
    expected_props = {
        "Player Profile": set(PLAYER_PROFILE_PROPS.keys()),
        "Quest Board": set(QUEST_BOARD_PROPS.keys()),
        "Skill Trees": set(SKILL_TREE_PROPS.keys()),
        "Achievement Log": set(ACHIEVEMENT_LOG_PROPS.keys()),
        "Party Board": set(PARTY_BOARD_PROPS.keys()),
        "Adventure Recaps": set(ADVENTURE_RECAP_PROPS.keys()),
        "Runs": set(RUNS_PROPS.keys()),
        "Review Queue": set(REVIEW_QUEUE_PROPS.keys()),
    }
    db_ids = workspace_state.get("databases") or {}
    missing_by_db: list[str] = []

    start = time.monotonic()
    try:
        async with NotionMCP(config) as mcp:
            for db_name, expected in expected_props.items():
                database_id = db_ids.get(db_name)
                if not database_id:
                    missing_by_db.append(f"{db_name}: database ID missing")
                    continue
                database = await mcp.fetch_page(database_id)
                actual = set((database.get("properties") or {}).keys())
                missing = sorted(expected - actual)
                if missing:
                    missing_by_db.append(f"{db_name}: {', '.join(missing)}")
        duration = (time.monotonic() - start) * 1000
        if missing_by_db:
            return CheckResult(
                "Workspace Schema",
                False,
                "Saved workspace is missing recommended properties. Recreate it with "
                "`questboard setup <PAGE_ID_OR_URL> --force-new` for the latest recommended workspace. "
                + " | ".join(missing_by_db),
                duration,
            )
        return CheckResult("Workspace Schema", True, "Saved workspace matches the current recommended schema", duration)
    except Exception as e:
        duration = (time.monotonic() - start) * 1000
        return CheckResult("Workspace Schema", False, f"Failed: {str(e)[:100]}", duration)


async def _check_llm(config) -> CheckResult:
    """Test OpenAI-compatible LLM API connectivity."""
    start = time.monotonic()
    try:
        from openai import OpenAI

        client = OpenAI(api_key=config.openai_api_key, base_url=config.openai_base_url)
        response = client.chat.completions.create(
            model=config.llm_model,
            max_tokens=50,
            messages=[{"role": "user", "content": "Say 'QuestBoard ready!' in 3 words or less."}],
        )
        duration = (time.monotonic() - start) * 1000
        text = response.choices[0].message.content.strip()
        return CheckResult("LLM API", True, f'LLM says: "{text}" ({duration:.0f}ms)', duration)
    except Exception as e:
        return CheckResult("LLM API", False, f"Failed: {str(e)[:100]}")


async def _check_anthropic(config) -> CheckResult:
    """Test Anthropic API connectivity."""
    start = time.monotonic()
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=config.anthropic_api_key)
        response = client.messages.create(
            model=config.llm_model,
            max_tokens=50,
            messages=[{"role": "user", "content": "Say 'QuestBoard ready!' in 3 words or less."}],
        )
        duration = (time.monotonic() - start) * 1000
        text = response.content[0].text.strip()
        return CheckResult("Anthropic API", True, f'Claude says: "{text}" ({duration:.0f}ms)', duration)
    except Exception as e:
        return CheckResult("Anthropic API", False, f"Failed: {str(e)[:100]}")


def _print_results(
    results: list[CheckResult],
    *,
    title: str,
    success_title: str,
    success_body: str,
    failure_body: str,
):
    """Print check results as a formatted table."""
    table = Table(title=title, border_style="bold")
    table.add_column("Check", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("Details")
    table.add_column("Time", justify="right")

    passed = 0
    total = len(results)

    for result in results:
        if result.passed and result.message.startswith("N/A"):
            status = "[yellow]SKIP[/yellow]"
        elif result.passed:
            status = "[green]PASS[/green]"
        else:
            status = "[red]FAIL[/red]"

        time_str = f"{result.duration_ms:.0f}ms" if result.duration_ms > 0 else "-"
        table.add_row(result.name, status, result.message, time_str)
        if result.passed:
            passed += 1

    console.print(table)

    if passed == total:
        console.print(Panel(
            f"[bold green]{success_title}[/bold green]\n\n"
            f"{success_body}",
            border_style="green",
        ))
    else:
        failed = total - passed
        console.print(Panel(
            f"[bold red]{failed}/{total} checks failed.[/bold red]\n\n"
            f"{failure_body}",
            border_style="red",
        ))

    return passed == total


def print_results(results: list[CheckResult]):
    """Print preflight results as a formatted table."""
    return _print_results(
        results,
        title="QuestBoard Preflight Check",
        success_title=f"All {len(results)} checks passed.",
        success_body=(
            "QuestBoard is ready for workspace creation and live operations.\n"
            "Run `questboard setup <PAGE_ID_OR_URL>` to create your workspace."
        ),
        failure_body="Fix the issues above before proceeding.",
    )


def print_doctor_results(results: list[CheckResult]):
    """Print doctor results as a formatted table."""
    return _print_results(
        results,
        title="QuestBoard Doctor",
        success_title=f"All {len(results)} checks passed.",
        success_body=(
            "Doctor says the current setup should be smooth for normal QuestBoard operations.\n"
            "For a full write-path rehearsal on a disposable Notion page, run "
            "`questboard preflight <PAGE_ID_OR_URL>`."
        ),
        failure_body="Doctor found issues that will make QuestBoard brittle. Fix them before continuing.",
    )
