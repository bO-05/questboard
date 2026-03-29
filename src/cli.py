"""QuestBoard CLI — Turn your Notion workspace into an RPG."""

import asyncio
import os
import subprocess
import sys
import time
from typing import Optional

# Ensure UTF-8 on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .config import get_config, SKILL_TREES, QUEST_RARITIES
from .page_refs import normalize_page_reference

app = typer.Typer(
    name="questboard",
    help="QuestBoard -- Turn your Notion workspace into an RPG",
    add_completion=False,
)
service_app = typer.Typer(help="Manage the local background sync service.")
app.add_typer(service_app, name="service")
console = Console(force_terminal=True)
VALID_CHALLENGE_STYLES = {"Quick Wins", "Balanced", "Deep Work", "Stretch Me"}
STYLE_ALIASES = {
    "quickwins": "Quick Wins",
    "quick wins": "Quick Wins",
    "balanced": "Balanced",
    "deepwork": "Deep Work",
    "deep work": "Deep Work",
    "stretchme": "Stretch Me",
    "stretch me": "Stretch Me",
}


def _run(coro):
    """Run an async coroutine."""
    try:
        return asyncio.run(coro)
    except typer.Exit:
        raise
    except KeyboardInterrupt:
        console.print("\n[yellow]QuestBoard cancelled.[/yellow]")
        raise typer.Exit(code=130)
    except Exception as exc:
        console.print(f"[bold red]QuestBoard failed:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc


def _normalize_page_reference_or_exit(value: str, label: str) -> str:
    try:
        return normalize_page_reference(value)
    except ValueError as exc:
        console.print(f"[bold red]Invalid {label}:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc


def _build_preference_updates(
    goal: Optional[str] = None,
    available_time: Optional[str] = None,
    style: Optional[str] = None,
    focus: Optional[str] = None,
    constraints: Optional[str] = None,
    motivation: Optional[str] = None,
    context_brief: Optional[str] = None,
    context_sources: Optional[str] = None,
) -> dict[str, str]:
    updates: dict[str, str] = {}
    if goal is not None:
        updates["Primary Goal"] = goal
    if available_time is not None:
        updates["Available Time"] = available_time
    if style is not None:
        updates["Preferred Challenge Style"] = style
    if focus is not None:
        updates["Focus Area"] = focus
    if constraints is not None:
        updates["Constraints"] = constraints
    if motivation is not None:
        updates["Motivation"] = motivation
    if context_brief is not None:
        updates["Context Brief"] = context_brief
    if context_sources is not None:
        updates["Context Sources"] = context_sources
    return updates


async def _apply_player_preferences(db_ids: dict[str, str], player_name: str, updates: dict[str, str]) -> dict | None:
    if not updates:
        return None

    from .mcp_client import NotionMCP
    from .workspace_data import filter_properties_for_database, get_player_page, normalize_player

    async with NotionMCP() as mcp:
        player_page = await get_player_page(mcp, db_ids, player_name)
        if not player_page or not player_page.get("id"):
            raise RuntimeError("Player profile not found in the saved workspace.")
        filtered_updates, dropped = await filter_properties_for_database(mcp, db_ids["Player Profile"], updates)
        if not filtered_updates:
            raise RuntimeError(
                "The current workspace is missing the new preference fields. Run `questboard setup <PAGE_ID_OR_URL> --force-new` for the latest schema."
            )
        await mcp.update_page(player_page["id"], filtered_updates)
        refreshed = await mcp.fetch_page(player_page["id"])
        normalized = normalize_player(refreshed)
        normalized["_dropped_updates"] = dropped
        return normalized


async def _create_and_queue_quests(mcp, count: int, *, allow_llm: bool, triggered_by: str) -> list[dict]:
    from .audit import queue_review_item
    from .engines.quest_generator import generate_quests
    from .setup_workspace import load_workspace_ids

    created = await generate_quests(mcp, count=count, allow_llm=allow_llm, triggered_by=triggered_by)
    workspace = load_workspace_ids()
    db_ids = workspace[1] if workspace else {}
    for quest in created:
        await queue_review_item(
            mcp,
            db_ids,
            item=quest["quest"],
            item_type="Quest",
            source_run=quest.get("source_run", "quest-generation"),
            target_page_id=quest.get("id", ""),
            review_state=quest.get("review_state", "Needs Review"),
            correction_notes=(
                "Review the quest framing before execution."
                if quest.get("generation_mode") == "llm"
                else "Fallback template used. Confirm it still matches the current goal."
            ),
            generation_mode=quest.get("generation_mode", "llm"),
            fallback_reason=quest.get("fallback_reason", ""),
        )
    return created


def _prompt_required(value: Optional[str], label: str, *, default: Optional[str] = None) -> str:
    if value is not None and value.strip():
        return value.strip()
    while True:
        response = typer.prompt(label, default=default or "", show_default=default is not None).strip()
        if response:
            return response
        console.print("[yellow]Please enter a value.[/yellow]")


def _prompt_optional(value: Optional[str], label: str, *, default: str = "") -> str:
    if value is not None:
        return value.strip()
    return typer.prompt(label, default=default, show_default=bool(default)).strip()


def _normalize_style_choice(value: str | None) -> str:
    choice = (value or "").strip()
    if not choice:
        raise ValueError(f"Invalid style. Choose one of: {', '.join(sorted(VALID_CHALLENGE_STYLES))}")
    if choice in VALID_CHALLENGE_STYLES:
        return choice

    lowered = " ".join(choice.casefold().split())
    compact = lowered.replace(" ", "")
    normalized = STYLE_ALIASES.get(lowered) or STYLE_ALIASES.get(compact)
    if normalized:
        return normalized
    raise ValueError(f"Invalid style. Choose one of: {', '.join(sorted(VALID_CHALLENGE_STYLES))}")


def _prompt_style(value: Optional[str]) -> str:
    if value is not None:
        return _normalize_style_choice(value)

    while True:
        choice = typer.prompt(
            f"Preferred challenge style ({', '.join(sorted(VALID_CHALLENGE_STYLES))})",
            default="Balanced",
        ).strip()
        try:
            return _normalize_style_choice(choice)
        except ValueError:
            pass
        console.print(f"[yellow]Choose one of: {', '.join(sorted(VALID_CHALLENGE_STYLES))}[/yellow]")


def _ensure_background_service(*, enabled: bool, interval_seconds: int = 15):
    if not enabled:
        return None
    from .service_manager import start_service

    try:
        return start_service(interval_seconds=interval_seconds)
    except Exception as exc:
        console.print(f"[yellow]QuestBoard could not start the background sync service automatically:[/yellow] {exc}")
        return None


def _print_service_status(service_state: Optional[dict]):
    if not service_state:
        return
    if service_state.get("running"):
        console.print(
            f"[cyan]Background sync service is running[/cyan] "
            f"(PID {service_state.get('pid')}, every {service_state.get('interval_seconds')}s)."
        )
        return
    if service_state.get("pid"):
        console.print("[yellow]Background sync service metadata exists, but the process is not running.[/yellow]")


def _workspace_view_tour(is_self_hosted: bool) -> str:
    if is_self_hosted:
        return (
            "Self-hosted MCP cannot auto-create QuestBoard's showcase views.\n\n"
            "For the smoothest demo, open Notion and manually verify or add:\n"
            "- Quest Ledger\n"
            "- Quest Map\n"
            "- Quest Calendar\n"
            "- Quest Timeline\n"
            "- Quest Cards\n"
            "- Quest Intake Form\n"
            "- XP Progress\n"
            "- Hero Dashboard\n"
            "- Run Center\n"
            "- Review Queue\n"
            "- Story Archive"
        )
    return (
        "Open these views in Notion for the cleanest walkthrough:\n"
        "- Hero Dashboard\n"
        "- Quest Ledger\n"
        "- Quest Map\n"
        "- XP Progress\n"
        "- Quest Intake Form\n"
        "- Run Center\n"
        "- Review Queue\n"
        "- Story Archive"
    )


def _pick_demo_skill(focus_area: Optional[str]) -> str:
    focus_text = (focus_area or "").casefold()
    for skill_name in SKILL_TREES:
        if skill_name.casefold() in focus_text:
            return skill_name
    return "Endurance"


def _demo_task_title(goal: Optional[str]) -> str:
    trimmed = (goal or "").strip()
    if not trimmed:
        return "🛠️ Ship one visible milestone"
    if len(trimmed) > 50:
        trimmed = trimmed[:47].rstrip() + "..."
    return f"🛠️ Ship a visible step toward: {trimmed}"


def _hosted_page_access_tip() -> str:
    return (
        "For QuestBoard on hosted MCP, the safest page sharing choice is:\n"
        "- General access: Only people invited\n"
        "- Page access: Can edit\n"
        "- Link expiration: leave off\n\n"
        "Use `Everyone at my workspace` only if you intentionally want the page visible to your whole workspace.\n"
        "Avoid `Anyone on the web with link` for setup or demos. QuestBoard does not need a public page.\n"
        "The Notion account you authorize in `questboard hosted-login` must be able to edit that page."
    )


def _print_runtime_result(result: dict, title: str) -> None:
    snapshot_before = result.get("snapshot_before") or {}
    snapshot_after = result.get("snapshot_after") or snapshot_before
    plan = result.get("plan") or {}

    metrics = Table(title=title, border_style="cyan")
    metrics.add_column("Metric", style="bold")
    metrics.add_column("Before", style="yellow")
    metrics.add_column("After", style="green")
    metrics.add_row("Available quests", str(snapshot_before.get("available_standard_quests", 0)), str(snapshot_after.get("available_standard_quests", 0)))
    metrics.add_row("Active bosses", str(snapshot_before.get("active_bosses", 0)), str(snapshot_after.get("active_bosses", 0)))
    metrics.add_row("Completed this week", str(snapshot_before.get("completed_this_week", 0)), str(snapshot_after.get("completed_this_week", 0)))
    metrics.add_row("Open reviews", str(snapshot_before.get("open_reviews", 0)), str(snapshot_after.get("open_reviews", 0)))
    metrics.add_row(
        "Current week recap",
        "Yes" if snapshot_before.get("has_current_week_recap") else "No",
        "Yes" if snapshot_after.get("has_current_week_recap") else "No",
    )
    console.print(metrics)

    if result.get("dry_run"):
        action_table = Table(title="Planned Actions", border_style="yellow")
        action_table.add_column("Action", style="bold")
        action_table.add_column("Decision", style="cyan")
        action_table.add_column("Count", justify="right", style="green")
        action_table.add_column("Reason")
        for action in plan.get("actions", []):
            action_table.add_row(
                action.get("name", ""),
                "run" if action.get("will_run") else "skip",
                str(action.get("planned_count", 0)),
                action.get("reason", ""),
            )
        console.print(action_table)
        return

    action_table = Table(title="Executed Actions", border_style="green")
    action_table.add_column("Action", style="bold")
    action_table.add_column("Status", style="cyan")
    action_table.add_column("Count", justify="right", style="green")
    action_table.add_column("Detail")
    for action in result.get("actions", []):
        action_table.add_row(
            action.get("name", ""),
            action.get("status", ""),
            str(action.get("count", 0)),
            action.get("detail", ""),
        )
    console.print(action_table)


@app.command("hosted-login")
def hosted_login(
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser", help="Open the Notion OAuth page automatically"),
    callback_host: str = typer.Option("127.0.0.1", "--callback-host", help="Loopback host for the OAuth callback"),
    callback_port: int = typer.Option(0, "--callback-port", min=0, help="Loopback port for the OAuth callback (0 picks a free port)"),
    timeout: int = typer.Option(300, "--timeout", min=30, help="Seconds to wait for the Notion OAuth callback"),
):
    """Authenticate QuestBoard against the hosted Notion MCP server and save tokens to .env."""
    from .config import Config, HOSTED_MCP_URL
    from .hosted_auth import (
        LoopbackCallbackServer,
        apply_hosted_tokens_to_config,
        build_authorization_url,
        discover_oauth_metadata,
        exchange_authorization_code,
        generate_code_challenge,
        generate_code_verifier,
        generate_state,
        open_browser as open_system_browser,
        persist_hosted_tokens,
        register_client,
        resolve_env_path,
    )
    from .mcp_client import NotionMCP

    console.print(Panel(
        "[bold cyan]Hosted Notion MCP Login[/bold cyan]\n"
        "QuestBoard will register a local OAuth client, open the Notion consent screen, and save the resulting hosted MCP tokens into `.env`.",
        border_style="cyan",
    ))

    env_path = resolve_env_path()
    metadata = discover_oauth_metadata(HOSTED_MCP_URL)
    code_verifier = generate_code_verifier()
    code_challenge = generate_code_challenge(code_verifier)
    state = generate_state()

    with LoopbackCallbackServer(callback_host, callback_port) as callback_server:
        redirect_uri = callback_server.redirect_uri
        credentials = register_client(metadata, redirect_uri)
        authorization_url = build_authorization_url(
            metadata,
            client_id=credentials.client_id,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            state=state,
        )

        console.print(Panel(
            f"Redirect URI: [bold]{redirect_uri}[/bold]\n"
            f"Authorization URL:\n[bold cyan]{authorization_url}[/bold cyan]\n\n"
            f"Waiting up to [bold]{timeout}[/bold] seconds for the Notion callback...",
            border_style="yellow",
            title="OAuth Ready",
        ))
        browser_opened = open_system_browser(authorization_url) if open_browser else False
        if open_browser and not browser_opened:
            console.print("[yellow]Could not open the system browser automatically. Open the authorization URL above manually.[/yellow]")

        try:
            callback = callback_server.wait_for_callback(timeout)
        except TimeoutError as exc:
            console.print("[red]Hosted login timed out before Notion redirected back to QuestBoard.[/red]")
            raise typer.Exit(code=1) from exc

    if callback.get("error"):
        description = callback.get("error_description") or callback["error"]
        console.print(f"[bold red]Hosted login failed:[/bold red] {description}")
        raise typer.Exit(code=1)
    if callback.get("state") != state:
        console.print("[bold red]Hosted login failed:[/bold red] OAuth state mismatch.")
        raise typer.Exit(code=1)
    code = callback.get("code")
    if not code:
        console.print("[bold red]Hosted login failed:[/bold red] Authorization code missing from callback.")
        raise typer.Exit(code=1)

    tokens = exchange_authorization_code(
        metadata,
        credentials,
        code=code,
        redirect_uri=redirect_uri,
        code_verifier=code_verifier,
    )
    persist_hosted_tokens(
        tokens,
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        server_url=HOSTED_MCP_URL,
        env_path=env_path,
    )

    verify_config = apply_hosted_tokens_to_config(
        Config(),
        tokens,
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        server_url=HOSTED_MCP_URL,
    )

    async def _verify():
        async with NotionMCP(verify_config) as mcp:
            return await mcp.get_self()

    _run(_verify())

    console.print(Panel(
        f"[bold green]Hosted MCP login complete.[/bold green]\n\n"
        f"Saved tokens to [bold]{env_path}[/bold]\n"
        f"MCP server: [bold]{HOSTED_MCP_URL}[/bold]\n"
        f"Access token lifetime: about 1 hour\n"
        f"Refresh token: saved for automatic refresh\n\n"
        f"{_hosted_page_access_tip()}\n\n"
        f"Next: run [bold cyan]questboard doctor <PAGE_ID_OR_URL>[/bold cyan] and then [bold cyan]questboard setup <PAGE_ID_OR_URL> --name \"YourName\"[/bold cyan].",
        border_style="green",
    ))


@app.command()
def setup(
    parent_page_id: str = typer.Argument(..., help="Notion page ID or URL to create QuestBoard under"),
    player_name: Optional[str] = typer.Option(None, "--name", "-n", help="Your hero name"),
    force_new: bool = typer.Option(False, "--force-new", help="Create a fresh workspace instead of reusing local QuestBoard metadata"),
    goal: Optional[str] = typer.Option(None, "--goal", help="Primary real-life goal QuestBoard should optimize for"),
    available_time: Optional[str] = typer.Option(None, "--time", help="How much time you can usually spend on quests"),
    style: Optional[str] = typer.Option(None, "--style", help="Preferred challenge style: Quick Wins, Balanced, Deep Work, Stretch Me"),
    focus: Optional[str] = typer.Option(None, "--focus", help="Current focus area or skills you care about most"),
    constraints: Optional[str] = typer.Option(None, "--constraints", help="Things the quest generator should avoid or respect"),
    motivation: Optional[str] = typer.Option(None, "--motivation", help="What keeps you engaged so quests feel rewarding"),
    start_service: bool = typer.Option(True, "--start-service/--no-start-service", help="Start the local background sync service after setup so Notion edits propagate automatically"),
):
    """Create the full QuestBoard workspace in Notion."""
    from .setup_workspace import load_workspace_state, setup_workspace

    normalized_style = None
    if style is not None:
        try:
            normalized_style = _normalize_style_choice(style)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

    console.print(Panel(
        "[bold yellow]⚔️ QuestBoard Setup[/bold yellow]\n"
        "Creating your RPG workspace in Notion...",
        border_style="yellow",
    ))
    if not get_config().is_self_hosted:
        console.print(Panel(_hosted_page_access_tip(), border_style="blue", title="Hosted Page Access"))

    normalized_parent_id = _normalize_page_reference_or_exit(parent_page_id, "parent page reference")
    name = player_name or get_config().player_name
    hub_id, db_ids = _run(setup_workspace(normalized_parent_id, name, force_new=force_new))
    preference_updates = _build_preference_updates(goal, available_time, normalized_style, focus, constraints, motivation)
    if preference_updates:
        _run(_apply_player_preferences(db_ids, name, preference_updates))
    workspace_state = load_workspace_state() or {}
    effective_name = workspace_state.get("player_name") or name
    service_state = _ensure_background_service(enabled=start_service)

    console.print(Panel(
        f"[bold green]✅ QuestBoard is ready![/bold green]\n\n"
        f"Hero: [bold]{effective_name}[/bold]\n"
        f"Databases created: [bold]{len(db_ids)}[/bold]\n"
        f"Starter quests: [bold]6[/bold]\n\n"
        f"Open your Notion workspace to see your QuestBoard!\n"
        f"Run [bold cyan]questboard calibrate[/bold cyan] to tune goals and play style.\n"
        f"Run [bold cyan]questboard quests[/bold cyan] to generate AI quests.\n"
        f"Run [bold cyan]questboard doctor[/bold cyan] to verify demo readiness.\n"
        f"{'[yellow]QuestBoard cannot auto-create showcase views when you use the self-hosted MCP server, so add or customize them manually in Notion.[/yellow]' if get_config().is_self_hosted else ''}",
        border_style="green",
    ))
    console.print(Panel(
        _workspace_view_tour(get_config().is_self_hosted),
        border_style="cyan",
        title="🎬 Open These Views",
    ))
    _print_service_status(service_state)


@app.command()
def onboard(
    parent_page_id: str = typer.Argument(..., help="Notion page ID or URL where QuestBoard should live"),
    player_name: Optional[str] = typer.Option(None, "--name", "-n", help="Your hero name"),
    goal: Optional[str] = typer.Option(None, "--goal", help="What you are actually trying to achieve"),
    success_criteria: Optional[str] = typer.Option(None, "--success", help="What success looks like in concrete terms"),
    available_time: Optional[str] = typer.Option(None, "--time", help="How much time you can realistically spend"),
    style: Optional[str] = typer.Option(None, "--style", help="Preferred challenge style: Quick Wins, Balanced, Deep Work, Stretch Me"),
    focus: Optional[str] = typer.Option(None, "--focus", help="What area matters most right now"),
    constraints: Optional[str] = typer.Option(None, "--constraints", help="Anything QuestBoard should avoid or respect"),
    motivation: Optional[str] = typer.Option(None, "--motivation", help="What keeps you engaged when the work gets hard"),
    domain_notes: Optional[str] = typer.Option(None, "--domain-notes", help="Tools, jargon, products, or context QuestBoard should understand"),
    research_provider: Optional[str] = typer.Option(None, "--research-provider", help="Optional grounding provider: auto, none, exa, or perplexity"),
    initial_quests: int = typer.Option(3, "--initial-quests", min=0, help="How many personalized quests to generate after onboarding"),
    template_only: bool = typer.Option(False, "--template-only", help="Use deterministic template quests instead of LLM generation for the first personalized quests"),
    force_new: bool = typer.Option(False, "--force-new", help="Create a fresh workspace instead of reusing saved QuestBoard metadata"),
    start_service: bool = typer.Option(True, "--start-service/--no-start-service", help="Start the local background sync service after onboarding so Notion edits propagate automatically"),
    yes: bool = typer.Option(False, "--yes", help="Skip the final confirmation prompt"),
):
    """Run a guided onboarding flow that personalizes QuestBoard for a real goal."""
    from .mcp_client import NotionMCP
    from .onboarding import (
        OnboardingAnswers,
        available_research_providers,
        build_context_brief,
        build_context_sources,
        resolve_research_provider,
        run_optional_research,
    )
    from .setup_workspace import load_workspace_state, setup_workspace

    config = get_config()
    saved_state = load_workspace_state() or {}
    default_name = saved_state.get("player_name") or config.player_name

    normalized_parent_id = _normalize_page_reference_or_exit(parent_page_id, "parent page reference")

    try:
        chosen_style = _prompt_style(style)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    answers = OnboardingAnswers(
        player_name=_prompt_required(player_name, "Hero name", default=default_name),
        goal=_prompt_required(goal, "What are you trying to achieve over the next few weeks?"),
        success_criteria=_prompt_required(success_criteria, "What does success look like in concrete terms?"),
        available_time=_prompt_required(available_time, "How much time can you realistically spend?"),
        style=chosen_style,
        focus=_prompt_required(focus, "What area matters most right now?"),
        constraints=_prompt_optional(constraints, "What should QuestBoard avoid or respect?", default=""),
        motivation=_prompt_required(motivation, "What keeps you engaged when the work gets hard?"),
        domain_notes=_prompt_optional(domain_notes, "Any tools, products, jargon, or domain context QuestBoard should understand?", default=""),
    )

    provider_input = research_provider
    if provider_input is None:
        available = available_research_providers(config)
        if available:
            options = ", ".join(["auto", "none", *available])
            suggested = available[0] if answers.domain_notes.strip() else "none"
            provider_input = typer.prompt(
                f"Optional web grounding provider ({options})",
                default=suggested,
            ).strip().lower()
        else:
            provider_input = "none"

    try:
        resolved_provider = resolve_research_provider(provider_input, config)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    console.print(Panel(
        "[bold cyan]QuestBoard Onboarding[/bold cyan]\n"
        "This flow captures your real goal, clarifies ambiguous context, and can generate your first personalized quests.",
        border_style="cyan",
    ))

    if resolved_provider != "none":
        console.print(f"[cyan]Grounding goal context with {resolved_provider.title()}...[/cyan]")
    research = run_optional_research(answers, resolved_provider, config=config)
    context_brief = build_context_brief(
        answers,
        research,
        prefer_llm_summary=not template_only,
        config=config,
    )
    context_sources = build_context_sources(answers, research)

    summary_lines = [
        f"Hero: [bold]{answers.player_name}[/bold]",
        f"Goal: {answers.goal}",
        f"Success looks like: {answers.success_criteria}",
        f"Time budget: {answers.available_time}",
        f"Style: {answers.style}",
        f"Focus: {answers.focus}",
        f"Constraints: {answers.constraints or 'None'}",
        f"Motivation: {answers.motivation}",
        f"Domain context: {answers.domain_notes or 'None'}",
        f"Grounding: {resolved_provider.title() if resolved_provider != 'none' else 'None'}",
        f"Initial personalized quests: {initial_quests}",
    ]
    console.print(Panel("\n".join(summary_lines), border_style="green", title="Onboarding Summary"))
    console.print(Panel(context_brief or "No context brief generated.", border_style="blue", title="Stored Context Brief"))
    if research.citations:
        for citation in research.citations[:3]:
            console.print(f"[dim]- {citation}[/dim]")

    if not yes and not typer.confirm("Create or update QuestBoard with this profile?", default=True):
        console.print("[yellow]Onboarding cancelled before any Notion writes.[/yellow]")
        raise typer.Exit(code=1)

    hub_id, db_ids = _run(setup_workspace(normalized_parent_id, answers.player_name, force_new=force_new))
    preference_updates = _build_preference_updates(
        goal=answers.goal,
        available_time=answers.available_time,
        style=answers.style,
        focus=answers.focus,
        constraints=answers.constraints,
        motivation=answers.motivation,
        context_brief=context_brief,
        context_sources=context_sources,
    )
    player = _run(_apply_player_preferences(db_ids, answers.player_name, preference_updates))

    created: list[dict] = []
    if initial_quests > 0:
        console.print("[cyan]Generating the first personalized quests...[/cyan]")

        async def _generate_initial():
            async with NotionMCP() as mcp:
                return await _create_and_queue_quests(
                    mcp,
                    initial_quests,
                    allow_llm=not template_only,
                    triggered_by="Onboarding",
                )

        created = _run(_generate_initial())
    service_state = _ensure_background_service(enabled=start_service)

    console.print(Panel(
        f"[bold green]QuestBoard onboarding complete![/bold green]\n\n"
        f"Hero: [bold]{player.get('name') or answers.player_name}[/bold]\n"
        f"Goal: {player.get('primary_goal') or answers.goal}\n"
        f"Context saved: {'Yes' if context_brief else 'No'}\n"
        f"Research provider: {resolved_provider.title() if resolved_provider != 'none' else 'None'}\n"
        f"Personalized quests created: [bold]{len(created)}[/bold]\n\n"
        f"Next: open Notion, review your Player Profile context, and start with the newest quests on the board.",
        border_style="green",
    ))
    _print_service_status(service_state)

    if created:
        table = Table(title="First Personalized Quests", border_style="yellow")
        table.add_column("Quest", style="bold")
        table.add_column("Skill", style="cyan")
        table.add_column("Rarity")
        table.add_column("Why Now")
        for quest in created:
            rarity_info = QUEST_RARITIES.get(quest.get("rarity", "Common"), {})
            table.add_row(
                quest["quest"],
                quest["skill"],
                f"{rarity_info.get('emoji', '')} {quest.get('rarity', 'Common')}",
                quest.get("why_this_quest", ""),
            )
        console.print(table)


@app.command()
def quests(
    count: int = typer.Option(5, "--count", "-c", help="Number of quests to generate"),
    template_only: bool = typer.Option(False, "--template-only", help="Use deterministic template quests instead of calling the LLM"),
):
    """Generate AI-powered quests based on your skill gaps."""
    from .mcp_client import NotionMCP

    console.print(
        "[yellow]🎲 The Quest Master is conjuring new challenges...[/yellow]\n"
        if not template_only else
        "[yellow]🎲 The Quest Master is preparing deterministic template quests...[/yellow]\n"
    )

    async def _gen():
        async with NotionMCP() as mcp:
            return await _create_and_queue_quests(mcp, count, allow_llm=not template_only, triggered_by="CLI")

    created = _run(_gen())
    if not created:
        console.print("[yellow]No quests were created. Check your workspace data or LLM configuration and try again.[/yellow]")
        raise typer.Exit(code=1)

    table = Table(title="📜 New Quests Generated", border_style="yellow")
    table.add_column("Quest", style="bold")
    table.add_column("Skill", style="cyan")
    table.add_column("Rarity", style="magenta")
    table.add_column("XP", justify="right", style="green")
    table.add_column("Difficulty")

    for q in created:
        rarity_info = QUEST_RARITIES.get(q.get("rarity", "Common"), {})
        table.add_row(
            q["quest"],
            q["skill"],
            f"{rarity_info.get('emoji', '')} {q.get('rarity', 'Common')}",
            str(q.get("xp_reward", 20)),
            q.get("difficulty", "Medium"),
        )

    console.print(table)
    fallback_count = sum(1 for q in created if q.get("generation_mode") == "fallback")
    reasons = [f"- [bold]{q['quest']}[/bold]: {q.get('why_this_quest', '')}" for q in created if q.get("why_this_quest")]
    if reasons:
        console.print("\n[cyan]Why these quests:[/cyan]")
        for reason in reasons:
            console.print(reason)
    if fallback_count:
        console.print(f"[yellow]Used fallback templates for {fallback_count} quest(s) because the LLM output was unavailable or invalid.[/yellow]")
    console.print(f"\n[green]✅ {len(created)} quests added to your Quest Board![/green]")


@app.command()
def complete(
    quest_id: str = typer.Argument(..., help="Notion page ID or URL of the quest to complete"),
):
    """Complete a quest and earn XP."""
    from .engines.xp_engine import sync_completed_quests
    from .workspace_data import get_quest_pages, normalize_quest
    from .mcp_client import NotionMCP

    console.print("[yellow]⚔️ Completing quest...[/yellow]\n")

    normalized_quest_id = _normalize_page_reference_or_exit(quest_id, "quest reference")

    async def _complete():
        async with NotionMCP() as mcp:
            return await complete_quest(mcp, normalized_quest_id)

    result = _run(_complete())

    if result.get("already_completed"):
        console.print(Panel(
            f"[bold yellow]Quest already processed[/bold yellow]\n\n"
            f"{result.get('quest_name', 'This quest')} already has XP recorded.\n"
            f"Use [bold cyan]questboard sync[/bold cyan] for the Notion-first completion flow.",
            border_style="yellow",
        ))
        return

    if result.get("leveled_up"):
        console.print(Panel(
            f"[bold yellow]🎉 LEVEL UP![/bold yellow]\n\n"
            f"Level [bold]{result['new_level']}[/bold] reached!\n"
            f"XP earned: [green]+{result['xp_earned']}[/green]\n"
            f"Total XP: [bold]{result['new_total_xp']}[/bold]\n"
            f"Skill: [cyan]{result['skill']}[/cyan]\n"
            f"Streak: [bold cyan]{result.get('streak_days', 0)}[/bold cyan] day(s)",
            border_style="yellow",
            title="⭐ Level Up!",
        ))
    else:
        console.print(Panel(
            f"[bold green]✅ Quest Complete![/bold green]\n\n"
            f"XP earned: [green]+{result['xp_earned']}[/green]\n"
            f"Total XP: [bold]{result['new_total_xp']}[/bold]\n"
            f"Skill: [cyan]{result['skill']}[/cyan]\n"
            f"Streak: [bold cyan]{result.get('streak_days', 0)}[/bold cyan] day(s)",
            border_style="green",
        ))

    if result.get("achievements_unlocked"):
        console.print(f"[yellow]Achievements unlocked:[/yellow] {', '.join(result['achievements_unlocked'])}")


@app.command()
def sync():
    """Sync completed quests from Notion and award XP automatically."""
    from .engines.xp_engine import reconcile_progress_state, sync_completed_quests
    from .mcp_client import NotionMCP
    from .operations import reconcile_review_surfaces

    console.print("[yellow]🔄 Syncing completed quests from Notion...[/yellow]\n")

    async def _sync():
        async with NotionMCP() as mcp:
            synced = await sync_completed_quests(mcp)
            progress = await reconcile_progress_state(mcp)
            review_changes = await reconcile_review_surfaces(mcp, triggered_by="CLI Sync")
            return synced, progress, review_changes

    results, progress_summary, review_changes = _run(_sync())

    if not results:
        console.print("[green]✅ All caught up! No new completed quests to process.[/green]")
        if review_changes:
            console.print(f"[cyan]Reconciled {len(review_changes)} review mismatch(es).[/cyan]")
        console.print(
            f"[cyan]Player totals rebuilt:[/cyan] Level {progress_summary['level']} | "
            f"{progress_summary['total_xp']} XP | {progress_summary['quests_completed']} completed quest(s)"
        )
        return

    table = Table(title="🎉 Quests Synced", border_style="green")
    table.add_column("Quest", style="bold")
    table.add_column("Skill", style="cyan")
    table.add_column("XP Earned", justify="right", style="green")
    table.add_column("Level Up?", style="yellow")

    total_xp = 0
    for r in results:
        table.add_row(
            r["quest_name"],
            r["skill"],
            f"+{r['xp_earned']}",
            "⭐ YES!" if r.get("leveled_up") else "",
        )
        total_xp += r["xp_earned"]

    console.print(table)
    console.print(f"\n[bold green]✅ Synced {len(results)} quests! Total: +{total_xp} XP[/bold green]")
    if review_changes:
        console.print(f"[cyan]Reconciled {len(review_changes)} review mismatch(es).[/cyan]")
    console.print(
        f"[cyan]Player totals rebuilt:[/cyan] Level {progress_summary['level']} | "
        f"{progress_summary['total_xp']} XP | {progress_summary['quests_completed']} completed quest(s)"
    )


@app.command()
def doctor(
    parent_page_id: Optional[str] = typer.Argument(
        None,
        help="Optional Notion page ID or URL to validate read-only before setup or demo",
    ),
):
    """Run non-destructive readiness checks for setup and demo."""
    from .preflight import print_doctor_results, run_doctor_checks

    console.print(Panel(
        "[bold cyan]🩺 QuestBoard Doctor[/bold cyan]\n"
        "Checking connection, workspace metadata, and demo readiness without creating test objects...",
        border_style="cyan",
    ))
    if not get_config().is_self_hosted:
        console.print(Panel(_hosted_page_access_tip(), border_style="blue", title="Hosted Page Access"))

    normalized_parent_id = None
    if parent_page_id:
        normalized_parent_id = _normalize_page_reference_or_exit(parent_page_id, "doctor page reference")

    results = _run(run_doctor_checks(normalized_parent_id))
    all_passed = print_doctor_results(results)

    if not all_passed:
        raise typer.Exit(code=1)


@app.command()
def boss(
    template_only: bool = typer.Option(False, "--template-only", help="Use a deterministic template boss instead of calling the LLM"),
):
    """Summon a boss battle targeting your weakest skill."""
    from .audit import queue_review_item
    from .engines.quest_generator import generate_boss_battle
    from .mcp_client import NotionMCP
    from .setup_workspace import load_workspace_ids

    console.print("[red]🐉 Summoning a boss...[/red]\n" if not template_only else "[red]🐉 Summoning a template boss...[/red]\n")

    async def _boss():
        async with NotionMCP() as mcp:
            result = await generate_boss_battle(mcp, allow_llm=not template_only)
            workspace = load_workspace_ids()
            db_ids = workspace[1] if workspace else {}
            await queue_review_item(
                mcp,
                db_ids,
                item=result.get("quest_title", result.get("boss_name", "Boss Battle")),
                item_type="Boss Battle",
                source_run=result.get("source_run", "boss-generation"),
                target_page_id=result.get("id", ""),
                review_state=result.get("review_state", "Needs Review"),
                correction_notes=(
                    "Review the boss framing and pressure point before starting."
                    if result.get("generation_mode") == "llm"
                    else "Fallback boss template used. Confirm the challenge still feels right."
                ),
                generation_mode=result.get("generation_mode", "llm"),
                fallback_reason=result.get("fallback_reason", ""),
            )
            return result

    result = _run(_boss())

    console.print(Panel(
        f"[bold red]🐉 BOSS BATTLE![/bold red]\n\n"
        f"[bold]{result.get('boss_name', 'A fearsome foe')}[/bold] has appeared!\n\n"
        f"{result.get('description', 'A mighty challenge awaits...')}\n\n"
        f"Reward: [yellow]{result.get('xp_reward', 200)} XP[/yellow]\n"
        f"Target skill: [cyan]{result.get('skill', 'Unknown')}[/cyan]\n"
        f"Generation: [bold]{result.get('generation_mode', 'llm')}[/bold]\n"
        f"Why now: {result.get('why_this_quest', 'It targets the weakest part of the current build.')}",
        border_style="red",
        title="⚔️ Boss Battle",
    ))


@app.command()
def recap(
    template_only: bool = typer.Option(False, "--template-only", help="Use the deterministic recap template instead of calling the LLM"),
):
    """Generate a weekly adventure recap."""
    from .engines.recap_writer import generate_weekly_recap
    from .mcp_client import NotionMCP

    console.print("[blue]📖 The Chronicler is writing your story...[/blue]\n" if not template_only else "[blue]📖 The Chronicler is using the deterministic story template...[/blue]\n")

    async def _recap():
        async with NotionMCP() as mcp:
            return await generate_weekly_recap(mcp, allow_llm=not template_only)

    result = _run(_recap())

    console.print(Panel(
        f"[bold blue]📖 Adventure Recap Created![/bold blue]\n\n"
        f"Week: {result['week']}\n"
        f"Quests completed: [green]{result['quests_completed']}[/green]\n"
        f"XP earned: [yellow]{result['xp_earned']}[/yellow]\n"
        f"MVP skill: [cyan]{result['mvp_skill']}[/cyan]\n"
        f"Generation: [bold]{result.get('generation_mode', 'llm')}[/bold]",
        border_style="blue",
    ))


@app.command()
def patrol():
    """Scan for stale/overdue quests and flag them."""
    from .engines.recap_writer import detect_stale_quests
    from .mcp_client import NotionMCP

    console.print("[yellow]🔍 Patrolling for stale quests...[/yellow]\n")

    async def _patrol():
        async with NotionMCP() as mcp:
            return await detect_stale_quests(mcp)

    stale = _run(_patrol())

    if not stale:
        console.print("[green]✅ No stale quests found! Your quest log is clean.[/green]")
        return

    table = Table(title="⚠️ Stale Quests Detected", border_style="red")
    table.add_column("Quest", style="bold")
    table.add_column("Days Overdue", justify="right", style="red")
    table.add_column("Status")

    for q in stale:
        status = "💀 Expired" if q["expired"] else "⚠️ Overdue"
        table.add_row(q["quest"], str(q["days_overdue"]), status)

    console.print(table)
    console.print(f"\n[yellow]{len(stale)} quests need attention![/yellow]")


@app.command()
def status():
    """Show your current hero status."""
    from .mcp_client import NotionMCP
    from .engines.xp_engine import xp_to_next_level
    from .setup_workspace import load_workspace_ids, load_workspace_state
    from .workspace_data import get_player_page, normalize_player

    workspace = load_workspace_ids()
    if not workspace:
        console.print("[red]No local workspace metadata found. Run `questboard setup` first.[/red]")
        return
    _, db_ids = workspace
    workspace_state = load_workspace_state() or {}
    saved_player_name = workspace_state.get("player_name") or get_config().player_name

    async def _status():
        async with NotionMCP() as mcp:
            page = await get_player_page(mcp, db_ids, saved_player_name)
            if page:
                return normalize_player(page)
        return None

    player = _run(_status())

    if not player:
        console.print("[red]No player profile found. Run `questboard setup` first.[/red]")
        return

    level = player["level"]
    total_xp = player["total_xp"]
    title = player["title"]
    hp = player["hp"]
    streak = player["streak_days"]
    quests = player["quests_completed"]
    bosses = player["boss_kills"]
    next_lvl = xp_to_next_level(total_xp)
    display_name = player.get("name") or saved_player_name
    focus_area = player.get("focus_area") or "Not set"
    challenge_style = player.get("preferred_challenge_style") or "Not set"
    primary_goal = player.get("primary_goal") or "Not set"

    # Build XP bar
    from .config import LEVEL_THRESHOLDS
    if level < len(LEVEL_THRESHOLDS):
        prev_threshold = LEVEL_THRESHOLDS[level - 1] if level > 1 else 0
        next_threshold = LEVEL_THRESHOLDS[level]
        progress = (total_xp - prev_threshold) / max(1, next_threshold - prev_threshold)
    else:
        progress = 1.0

    bar_width = 20
    filled = int(progress * bar_width)
    xp_bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"

    console.print(Panel(
        f"[bold]{display_name}[/bold] — {title}\n\n"
        f"Level: [bold yellow]{level}[/bold yellow]    "
        f"HP: [bold red]{hp}[/bold red]    "
        f"Streak: [bold cyan]{streak}[/bold cyan] days\n\n"
        f"XP: {xp_bar} {total_xp} / {total_xp + next_lvl}\n\n"
        f"Quests: [green]{quests}[/green]    "
        f"Boss Kills: [red]{bosses}[/red]\n\n"
        f"Goal: {primary_goal}\n"
        f"Focus: {focus_area}\n"
        f"Style: {challenge_style}",
        border_style="yellow",
        title="🛡️ Hero Status",
    ))

    # Suggest next action
    suggestions = []
    if total_xp == 0:
        suggestions.append("Generate your first quests: [bold cyan]questboard quests[/bold cyan]")
    elif quests == 0:
        suggestions.append("Generate quests: [bold cyan]questboard quests[/bold cyan]")
    else:
        suggestions.append("Sync completed quests: [bold cyan]questboard sync[/bold cyan]")

    if level >= 3 and bosses == 0:
        suggestions.append("Ready for a boss battle: [bold cyan]questboard boss[/bold cyan]")
    elif level >= 2:
        suggestions.append("Try a boss battle: [bold cyan]questboard boss[/bold cyan]")

    if quests >= 5:
        suggestions.append("Get your weekly story: [bold cyan]questboard recap[/bold cyan]")

    if suggestions:
        next_text = "\n".join(f"  → {s}" for s in suggestions[:2])
        console.print(Panel(
            f"[bold]Next actions:[/bold]\n{next_text}",
            border_style="cyan",
            title="💡 What to do next",
        ))


@app.command()
def calibrate(
    goal: Optional[str] = typer.Option(None, "--goal", help="Primary real-life goal QuestBoard should optimize for"),
    available_time: Optional[str] = typer.Option(None, "--time", help="How much time you can usually spend on quests"),
    style: Optional[str] = typer.Option(None, "--style", help="Preferred challenge style: Quick Wins, Balanced, Deep Work, Stretch Me"),
    focus: Optional[str] = typer.Option(None, "--focus", help="Current focus area or skills you care about most"),
    constraints: Optional[str] = typer.Option(None, "--constraints", help="Things the quest generator should avoid or respect"),
    motivation: Optional[str] = typer.Option(None, "--motivation", help="What keeps you engaged so quests feel rewarding"),
):
    """Update the player's quest-generation preferences without editing Notion manually."""
    from .mcp_client import NotionMCP
    from .setup_workspace import load_workspace_ids, load_workspace_state
    from .workspace_data import filter_properties_for_database, get_player_page, normalize_player

    normalized_style = None
    if style is not None:
        try:
            normalized_style = _normalize_style_choice(style)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

    workspace = load_workspace_ids()
    if not workspace:
        console.print("[red]No local workspace metadata found. Run `questboard setup` first.[/red]")
        raise typer.Exit(code=1)
    _, db_ids = workspace
    workspace_state = load_workspace_state() or {}
    saved_player_name = workspace_state.get("player_name") or get_config().player_name

    updates = {}
    if goal is not None:
        updates["Primary Goal"] = goal
    if available_time is not None:
        updates["Available Time"] = available_time
    if normalized_style is not None:
        updates["Preferred Challenge Style"] = normalized_style
    if focus is not None:
        updates["Focus Area"] = focus
    if constraints is not None:
        updates["Constraints"] = constraints
    if motivation is not None:
        updates["Motivation"] = motivation

    async def _calibrate():
        async with NotionMCP() as mcp:
            player_page = await get_player_page(mcp, db_ids, saved_player_name)
            if not player_page or not player_page.get("id"):
                raise RuntimeError("Player profile not found in the saved workspace.")
            dropped: list[str] = []
            if updates:
                filtered_updates, dropped = await filter_properties_for_database(mcp, db_ids["Player Profile"], updates)
                if not filtered_updates:
                    raise RuntimeError(
                        "The current workspace is missing the new preference fields. Run `questboard setup <PAGE_ID_OR_URL> --force-new` for the latest schema."
                    )
                await mcp.update_page(player_page["id"], filtered_updates)
                player_page = await mcp.fetch_page(player_page["id"])
            normalized = normalize_player(player_page)
            normalized["_dropped_updates"] = dropped
            return normalized

    player = _run(_calibrate())
    dropped_updates = player.get("_dropped_updates", []) if isinstance(player, dict) else []

    console.print(Panel(
        f"[bold green]Calibration saved for {player.get('name') or saved_player_name}[/bold green]\n\n"
        f"Goal: {player.get('primary_goal') or 'Not set'}\n"
        f"Available time: {player.get('available_time') or 'Not set'}\n"
        f"Style: {player.get('preferred_challenge_style') or 'Not set'}\n"
        f"Focus: {player.get('focus_area') or 'Not set'}\n"
        f"Constraints: {player.get('constraints') or 'Not set'}\n"
        f"Motivation: {player.get('motivation') or 'Not set'}"
        f"{f'\\n\\n[yellow]Skipped fields not present in this workspace schema:[/yellow] {', '.join(dropped_updates)}' if dropped_updates else ''}",
        border_style="green",
        title="🎯 Quest Calibration",
    ))


@app.command()
def intake(
    title: str = typer.Argument(..., help="The real-world task you want to turn into a quest"),
    skill: str = typer.Option("Endurance", "--skill", help="Target skill tree"),
    minutes: int = typer.Option(30, "--minutes", help="Estimated effort in minutes"),
    due_days: int = typer.Option(3, "--days", help="Due date offset in days"),
    notes: str = typer.Option("", "--notes", help="Optional player note or context"),
    importance: str = typer.Option("standard", "--importance", help="standard or high"),
):
    """Create a player-authored quest without touching the raw Notion schema."""
    import datetime

    from .mcp_client import NotionMCP
    from .player_intake import build_player_quest
    from .setup_workspace import load_workspace_ids, load_workspace_state
    from .workspace_data import (
        filter_properties_for_database,
        get_player_page,
        normalize_player,
    )

    if importance not in {"standard", "high"}:
        console.print("[red]Invalid importance.[/red] Use `standard` or `high`.")
        raise typer.Exit(code=1)

    workspace = load_workspace_ids()
    if not workspace:
        console.print("[red]No local workspace metadata found. Run `questboard setup` first.[/red]")
        raise typer.Exit(code=1)
    _, db_ids = workspace
    workspace_state = load_workspace_state() or {}
    saved_player_name = workspace_state.get("player_name") or get_config().player_name

    async def _intake():
        async with NotionMCP() as mcp:
            player_page = await get_player_page(mcp, db_ids, saved_player_name)
            player = normalize_player(player_page) if player_page else {}
            quest = build_player_quest(
                title,
                skill,
                minutes=minutes,
                due_days=due_days,
                notes=notes,
                importance=importance,
                focus_area=player.get("focus_area", ""),
            )
            due_date = (datetime.date.today() + datetime.timedelta(days=quest["due_days"])).isoformat()
            payload, dropped = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
                "Quest": quest["quest"],
                "Status": "Available",
                "Rarity": quest["rarity"],
                "Skill": quest["skill"],
                "XP Reward": quest["xp_reward"],
                "Due Date": due_date,
                "Description": quest["description"],
                "Difficulty": quest["difficulty"],
                "Source": quest["source"],
                "Why This Quest": quest["why_this_quest"],
                "Generation Mode": "Player",
                "Review State": "Approved",
                "Correction Notes": "",
                "Source Run": "player-intake",
                "Prompt Version": "player-intake-v1",
                "Fallback Reason": "",
            })
            page = await mcp.create_db_page(db_ids["Quest Board"], payload, icon="📝")
            await mcp.create_comment(
                page.get("id", ""),
                f"📝 *Player-authored quest created.* Why now: {quest['why_this_quest']}"
            )
            quest["id"] = page.get("id", "")
            quest["due_date"] = due_date
            quest["dropped_properties"] = dropped
            return quest

    quest = _run(_intake())

    console.print(Panel(
        f"[bold green]Player quest created[/bold green]\n\n"
        f"Quest: [bold]{quest['quest']}[/bold]\n"
        f"Skill: [cyan]{quest['skill']}[/cyan]\n"
        f"Difficulty: {quest['difficulty']}\n"
        f"Rarity: {quest['rarity']}\n"
        f"XP: [green]{quest['xp_reward']}[/green]\n"
        f"Due: {quest['due_date']}\n"
        f"Why this quest: {quest['why_this_quest']}"
        f"{f'\\n\\n[yellow]Skipped unsupported properties:[/yellow] {', '.join(quest['dropped_properties'])}' if quest.get('dropped_properties') else ''}",
        border_style="green",
        title="📝 Quest Intake",
    ))


@app.command("runtime")
def runtime(
    min_available: int = typer.Option(3, "--min-available", min=1, help="Top up quests when available standard quests fall below this count"),
    target_available: int = typer.Option(5, "--target-available", min=1, help="Generate enough quests to restore this many available standard quests"),
    with_boss: bool = typer.Option(False, "--with-boss/--no-boss", help="Allow runtime to summon a boss when momentum gates are met"),
    min_level_for_boss: int = typer.Option(3, "--boss-level", min=1, help="Minimum player level before runtime can summon a boss"),
    with_recap: bool = typer.Option(True, "--with-recap/--no-recap", help="Allow runtime to generate a weekly recap when needed"),
    template_only: bool = typer.Option(False, "--template-only", help="Use deterministic fallbacks instead of LLM calls during runtime"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview runtime decisions without writing to Notion"),
):
    """Run QuestBoard's policy-based runtime tick."""
    from .mcp_client import NotionMCP
    from .runtime import RuntimePolicy, run_runtime_tick

    if target_available < min_available:
        console.print("[red]`--target-available` must be greater than or equal to `--min-available`.[/red]")
        raise typer.Exit(code=1)

    policy = RuntimePolicy(
        min_available_quests=min_available,
        target_available_quests=target_available,
        allow_boss=with_boss,
        min_level_for_boss=min_level_for_boss,
        allow_recap=with_recap,
        allow_llm=not template_only,
        triggered_by="CLI Runtime",
    )

    console.print(Panel(
        "[bold cyan]Quest Runtime[/bold cyan]\n"
        "Running the policy-based control loop: sync, patrol, quest top-up, optional boss, and weekly recap.",
        border_style="cyan",
    ))

    async def _runtime():
        async with NotionMCP() as mcp:
            return await run_runtime_tick(mcp, policy=policy, dry_run=dry_run)

    result = _run(_runtime())
    _print_runtime_result(result, "Quest Runtime")

    if dry_run:
        console.print("[yellow]Dry run only. No Notion pages were changed.[/yellow]")
        return

    console.print("[green]Quest runtime tick complete.[/green]")


@app.command()
def watch(
    interval: int = typer.Option(300, "--interval", min=1, help="Seconds between runtime ticks"),
    iterations: int = typer.Option(3, "--iterations", min=0, help="How many ticks to run (0 means until interrupted)"),
    min_available: int = typer.Option(3, "--min-available", min=1, help="Top up quests when available standard quests fall below this count"),
    target_available: int = typer.Option(5, "--target-available", min=1, help="Generate enough quests to restore this many available standard quests"),
    with_boss: bool = typer.Option(False, "--with-boss/--no-boss", help="Allow watch mode to summon a boss when momentum gates are met"),
    min_level_for_boss: int = typer.Option(3, "--boss-level", min=1, help="Minimum player level before runtime can summon a boss"),
    with_recap: bool = typer.Option(True, "--with-recap/--no-recap", help="Allow watch mode to generate a weekly recap when needed"),
    template_only: bool = typer.Option(False, "--template-only", help="Use deterministic fallbacks instead of LLM calls during watch mode"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview each tick without writing to Notion"),
):
    """Run QuestBoard in watch mode for repeated runtime ticks."""
    from .mcp_client import NotionMCP
    from .runtime import RuntimePolicy, watch_runtime

    if target_available < min_available:
        console.print("[red]`--target-available` must be greater than or equal to `--min-available`.[/red]")
        raise typer.Exit(code=1)

    policy = RuntimePolicy(
        min_available_quests=min_available,
        target_available_quests=target_available,
        allow_boss=with_boss,
        min_level_for_boss=min_level_for_boss,
        allow_recap=with_recap,
        allow_llm=not template_only,
        triggered_by="Runtime Watch",
    )
    iteration_text = "until interrupted" if iterations == 0 else f"{iterations} tick(s)"
    console.print(Panel(
        f"[bold cyan]Quest Runtime Watch[/bold cyan]\n"
        f"Interval: {interval} second(s)\n"
        f"Iterations: {iteration_text}",
        border_style="cyan",
    ))

    async def _watch():
        async with NotionMCP() as mcp:
            return await watch_runtime(
                mcp,
                policy=policy,
                interval_seconds=interval,
                iterations=iterations,
                dry_run=dry_run,
            )

    results = _run(_watch())
    for index, result in enumerate(results, start=1):
        _print_runtime_result(result, f"Quest Runtime Tick {index}")
    if dry_run:
        console.print("[yellow]Watch dry run complete. No Notion pages were changed.[/yellow]")
        return
    console.print("[green]Quest runtime watch completed.[/green]")


@service_app.command("start")
def service_start(
    interval: int = typer.Option(15, "--interval", min=5, help="Seconds between background sync ticks"),
    restart: bool = typer.Option(False, "--restart", help="Restart the background sync service if it is already running"),
):
    """Start the local background sync service."""
    from .service_manager import start_service

    state = start_service(interval_seconds=interval, force_restart=restart)
    if state.get("running"):
        console.print(
            f"[green]Background sync service running.[/green] "
            f"PID {state.get('pid')} | interval {state.get('interval_seconds')}s"
        )
        if state.get("log_path"):
            console.print(f"[dim]Log: {state['log_path']}[/dim]")


@service_app.command("stop")
def service_stop():
    """Stop the local background sync service."""
    from .service_manager import stop_service

    result = stop_service()
    if result.get("stopped"):
        console.print("[green]Background sync service stopped.[/green]")
        return
    console.print("[yellow]Background sync service was not running.[/yellow]")


@service_app.command("status")
def service_status():
    """Show the local background sync service status."""
    from .service_manager import get_service_status

    state = get_service_status()
    if state.get("running"):
        console.print(
            f"[green]Background sync service running.[/green] "
            f"PID {state.get('pid')} | interval {state.get('interval_seconds')}s"
        )
        if state.get("log_path"):
            console.print(f"[dim]Log: {state['log_path']}[/dim]")
        return
    console.print("[yellow]Background sync service is not running.[/yellow]")


@app.command()
def reviews(
    state: Optional[str] = typer.Option(None, "--state", help="Filter by a specific review state"),
    limit: int = typer.Option(15, "--limit", min=1, help="Maximum review items to show"),
    all_states: bool = typer.Option(False, "--all", help="Include approved and locked review items"),
):
    """List review queue items from the current QuestBoard workspace."""
    from .audit import REVIEW_STATES
    from .mcp_client import NotionMCP
    from .operations import list_review_items

    if state is not None and state not in REVIEW_STATES:
        console.print(f"[red]Invalid review state.[/red] Choose one of: {', '.join(REVIEW_STATES)}")
        raise typer.Exit(code=1)

    async def _reviews():
        async with NotionMCP() as mcp:
            return await list_review_items(
                mcp,
                states=[state] if state else None,
                limit=limit,
                include_closed=all_states,
            )

    items = _run(_reviews())
    if not items:
        console.print("[green]No review queue items matched that filter.[/green]")
        return

    table = Table(title="Review Queue", border_style="yellow")
    table.add_column("Item", style="bold")
    table.add_column("Type", style="cyan")
    table.add_column("State", style="magenta")
    table.add_column("Reviewer", style="green")
    table.add_column("Source Run")
    table.add_column("Fallback")
    for item in items:
        table.add_row(
            item.get("item", "")[:44],
            item.get("item_type", ""),
            item.get("review_state", ""),
            item.get("reviewer", "") or "-",
            item.get("source_run", "")[:30],
            (item.get("fallback_reason", "") or "-")[:32],
        )
    console.print(table)


@app.command()
def review(
    review_id: str = typer.Argument(..., help="Review queue page ID or URL"),
    state: str = typer.Option(..., "--state", help="New review state"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Correction notes or approval notes"),
    reviewer: Optional[str] = typer.Option(None, "--reviewer", help="Who reviewed the item"),
    apply_to_target: bool = typer.Option(True, "--apply/--no-apply", help="Propagate review state and notes to the linked target page"),
    force: bool = typer.Option(False, "--force", help="Override locked review items"),
):
    """Apply a review decision to a review queue item and optionally its source page."""
    from .audit import REVIEW_STATES
    from .mcp_client import NotionMCP
    from .operations import apply_review_decision
    from .setup_workspace import load_workspace_state

    if state not in REVIEW_STATES:
        console.print(f"[red]Invalid review state.[/red] Choose one of: {', '.join(REVIEW_STATES)}")
        raise typer.Exit(code=1)

    normalized_review_id = _normalize_page_reference_or_exit(review_id, "review item reference")
    effective_reviewer = reviewer or (load_workspace_state() or {}).get("player_name") or get_config().player_name

    async def _review():
        async with NotionMCP() as mcp:
            return await apply_review_decision(
                mcp,
                normalized_review_id,
                new_state=state,
                notes=notes,
                reviewer=effective_reviewer,
                apply_to_target=apply_to_target,
                force=force,
            )

    result = _run(_review())
    console.print(Panel(
        f"Item: [bold]{result['item']}[/bold]\n"
        f"Type: {result['item_type'] or 'Unknown'}\n"
        f"State: [cyan]{result['old_state'] or 'Untracked'}[/cyan] -> [green]{result['new_state']}[/green]\n"
        f"Reviewer: {result['reviewer'] or '-'}\n"
        f"Target updated: {'Yes' if result['target_applied'] else 'No'}\n"
        f"Notes: {result['notes'] or '-'}",
        border_style="green" if result["new_state"] in {"Approved", "Locked"} else "yellow",
        title="Review Applied",
    ))


@app.command()
def runs(
    limit: int = typer.Option(15, "--limit", min=1, help="Maximum run records to show"),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by run status"),
    run_type: Optional[str] = typer.Option(None, "--type", help="Filter by run type"),
):
    """List recent run records from QuestBoard's Run Center."""
    from .mcp_client import NotionMCP
    from .operations import RUN_STATUSES, RUN_TYPES, list_runs

    if status is not None and status not in RUN_STATUSES:
        console.print(f"[red]Invalid run status.[/red] Choose one of: {', '.join(RUN_STATUSES)}")
        raise typer.Exit(code=1)
    if run_type is not None and run_type not in RUN_TYPES:
        console.print(f"[red]Invalid run type.[/red] Choose one of: {', '.join(RUN_TYPES)}")
        raise typer.Exit(code=1)

    async def _runs():
        async with NotionMCP() as mcp:
            return await list_runs(
                mcp,
                statuses=[status] if status else None,
                run_types=[run_type] if run_type else None,
                limit=limit,
            )

    records = _run(_runs())
    if not records:
        console.print("[green]No run records matched that filter.[/green]")
        return

    table = Table(title="Run Center", border_style="cyan")
    table.add_column("Started", style="bold")
    table.add_column("Type", style="cyan")
    table.add_column("Status", style="magenta")
    table.add_column("Triggered By", style="green")
    table.add_column("Created", justify="right", style="green")
    table.add_column("Updated", justify="right", style="yellow")
    table.add_column("Fallback")
    for record in records:
        table.add_row(
            (record.get("started_at") or record.get("created_time") or "-")[:19],
            record.get("type", ""),
            record.get("status", ""),
            record.get("triggered_by", "") or "-",
            str(record.get("records_created", 0)),
            str(record.get("records_updated", 0)),
            (record.get("fallback_reason", "") or "-")[:30],
        )
    console.print(table)


@app.command()
def run_all():
    """Run a full QuestBoard cycle: patrol, generate quests, generate recap."""
    console.print("[yellow]`questboard run-all` now routes through `questboard runtime`.[/yellow]")
    runtime()
    return

    from .engines.quest_generator import generate_quests
    from .engines.recap_writer import generate_weekly_recap, detect_stale_quests
    from .engines.xp_engine import sync_completed_quests
    from .mcp_client import NotionMCP

    console.print(Panel(
        "[bold yellow]⚔️ QuestBoard Full Cycle[/bold yellow]\n"
        "Running sync → patrol → quest generation → recap...",
        border_style="yellow",
    ))

    async def _full_cycle():
        async with NotionMCP() as mcp:
            # 0. Sync completed quests
            console.print("\n[bold]0/4 — 🔄 Syncing completed quests...[/bold]")
            synced = await sync_completed_quests(mcp)
            console.print(f"   Synced {len(synced)} completed quests.")

            # 1. Patrol
            console.print("\n[bold]1/4 — 🔍 Patrolling for stale quests...[/bold]")
            stale = await detect_stale_quests(mcp)
            console.print(f"   Found {len(stale)} stale quests.")

            # 2. Generate quests
            console.print("\n[bold]2/4 — 🎲 Generating new quests...[/bold]")
            new_quests = await generate_quests(mcp, count=5)
            console.print(f"   Created {len(new_quests)} new quests.")

            # 3. Weekly recap
            console.print("\n[bold]3/4 — 📖 Writing adventure recap...[/bold]")
            recap = await generate_weekly_recap(mcp)
            console.print(f"   Recap created: {recap['week']}")

            return {"synced": len(synced), "stale": len(stale), "quests": len(new_quests), "recap": recap}

    result = _run(_full_cycle())

    console.print(Panel(
        f"[bold green]✅ Full cycle complete![/bold green]\n\n"
        f"Quests synced: {result['synced']}\n"
        f"Stale quests flagged: {result['stale']}\n"
        f"New quests generated: {result['quests']}\n"
        f"Recap: {result['recap']['week']}\n"
        f"XP tracked: {result['recap']['xp_earned']}",
        border_style="green",
    ))


@app.command()
def demo(
    parent_page_id: str = typer.Argument(..., help="Notion page ID or URL for the demo workspace"),
    player_name: Optional[str] = typer.Option("Hero", "--name", "-n", help="Your hero name"),
    skip_ai: bool = typer.Option(False, "--skip-ai", help="Use deterministic templates instead of LLM calls for the demo"),
    force_new: bool = typer.Option(
        True,
        "--force-new/--reuse-existing",
        help="Create a fresh demo workspace by default; use --reuse-existing to keep the saved one",
    ),
    goal: Optional[str] = typer.Option(None, "--goal", help="Primary real-life goal QuestBoard should optimize for"),
    available_time: Optional[str] = typer.Option(None, "--time", help="How much time you can usually spend on quests"),
    style: Optional[str] = typer.Option(None, "--style", help="Preferred challenge style: Quick Wins, Balanced, Deep Work, Stretch Me"),
    focus: Optional[str] = typer.Option(None, "--focus", help="Current focus area or skills you care about most"),
    constraints: Optional[str] = typer.Option(None, "--constraints", help="Things the quest generator should avoid or respect"),
    motivation: Optional[str] = typer.Option(None, "--motivation", help="What keeps you engaged so quests feel rewarding"),
):
    """Run a full QuestBoard demo — perfect for recording. Just hit record and run this."""
    import datetime

    from .engines.xp_engine import sync_completed_quests
    from .player_intake import build_player_quest
    from .setup_workspace import load_workspace_state, setup_workspace
    from .mcp_client import NotionMCP
    from .workspace_data import (
        filter_properties_for_database,
        get_player_page,
        get_quest_pages,
        normalize_player,
        normalize_quest,
    )

    normalized_style = None
    if style is not None:
        try:
            normalized_style = _normalize_style_choice(style)
        except ValueError as exc:
            console.print(f"[red]{exc}[/red]")
            raise typer.Exit(code=1) from exc

    console.print()
    console.print(Panel(
        "[bold yellow]⚔️  Q U E S T B O A R D[/bold yellow]\n\n"
        "[dim]Turn your Notion workspace into an RPG[/dim]\n"
        "[dim]Built with Notion MCP[/dim]",
        border_style="yellow",
        padding=(1, 4),
    ))
    if not get_config().is_self_hosted:
        console.print(Panel(_hosted_page_access_tip(), border_style="blue", title="Hosted Page Access"))
    time.sleep(1)

    # Step 1: Setup
    console.print("\n[bold cyan]━━━ ACT 1: Creating the RPG Workspace ━━━[/bold cyan]\n")
    console.print("[dim]One command. One blank Notion page. Watch it transform.[/dim]\n")

    normalized_parent_id = _normalize_page_reference_or_exit(parent_page_id, "demo page reference")
    name = player_name or "Hero"
    hub_id, db_ids = _run(setup_workspace(normalized_parent_id, name, force_new=force_new))
    preference_updates = _build_preference_updates(goal, available_time, normalized_style, focus, constraints, motivation)
    calibrated_player = None
    if preference_updates:
        calibrated_player = _run(_apply_player_preferences(db_ids, name, preference_updates))
    workspace_state = load_workspace_state() or {}
    effective_name = workspace_state.get("player_name") or name

    console.print(Panel(
        f"[green]✅ Workspace created![/green]\n\n"
        f"  📦 Databases: [bold]{len(db_ids)}[/bold]\n"
        f"  📜 Starter quests: [bold]6[/bold]\n"
        f"  🌳 Skill trees: [bold]6[/bold]\n"
        f"  🛡️ Hero: [bold]{effective_name}[/bold] (Level 1)",
        border_style="green",
    ))
    time.sleep(1)

    if calibrated_player:
        console.print(Panel(
            f"Goal: {calibrated_player.get('primary_goal') or 'Not set'}\n"
            f"Focus: {calibrated_player.get('focus_area') or 'Not set'}\n"
            f"Style: {calibrated_player.get('preferred_challenge_style') or 'Not set'}",
            border_style="cyan",
            title="🎯 Hero Calibration",
        ))
        time.sleep(1)

    console.print(Panel(
        _workspace_view_tour(get_config().is_self_hosted),
        border_style="cyan",
        title="🎬 Open These Views in Notion",
    ))
    time.sleep(1)

    # Step 2: AI Quests (optional)
    console.print(
        "\n[bold cyan]━━━ ACT 2: Quest Generation ━━━[/bold cyan]\n"
        if not skip_ai else "\n[bold cyan]━━━ ACT 2: Template Quest Generation ━━━[/bold cyan]\n"
    )
    console.print(
        "[dim]The Quest Master analyzes your skill gaps...[/dim]\n"
        if not skip_ai else "[dim]Using deterministic template quests for a low-risk demo path...[/dim]\n"
    )

    try:
        from .engines.quest_generator import generate_quests

        async def _gen():
            async with NotionMCP() as mcp:
                return await generate_quests(mcp, count=3, allow_llm=not skip_ai)

        created = _run(_gen())

        table = Table(title="📜 Generated Quests", border_style="yellow")
        table.add_column("Quest", style="bold")
        table.add_column("Skill", style="cyan")
        table.add_column("Rarity")
        table.add_column("XP", justify="right", style="green")

        for q in created:
            rarity_info = QUEST_RARITIES.get(q.get("rarity", "Common"), {})
            table.add_row(
                q["quest"],
                q["skill"],
                f"{rarity_info.get('emoji', '')} {q.get('rarity', 'Common')}",
                str(q.get("xp_reward", 20)),
            )
        console.print(table)
        reasons = [f"- [bold]{q['quest']}[/bold]: {q.get('why_this_quest', '')}" for q in created if q.get("why_this_quest")]
        if reasons:
            console.print("\n[cyan]Quest logic:[/cyan]")
            for reason in reasons:
                console.print(reason)
        fallback_count = sum(1 for q in created if q.get("generation_mode") == "fallback")
        if fallback_count:
            console.print(f"[yellow]Fallback templates were used for {fallback_count} quest(s).[/yellow]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Quest generation skipped: {e}[/yellow]")
        console.print("[dim]Tip: Set ANTHROPIC_API_KEY or use --skip-ai[/dim]")

    time.sleep(1)

    # Step 3: Code-first intake
    console.print("\n[bold cyan]━━━ ACT 3: Code-First Quest Intake ━━━[/bold cyan]\n")
    console.print("[dim]Now turn one real task into a scored quest without touching raw Notion fields...[/dim]\n")

    try:
        async def _demo_intake():
            async with NotionMCP() as mcp:
                player_page = await get_player_page(mcp, db_ids, effective_name)
                player = normalize_player(player_page) if player_page else {}
                focus_area = player.get("focus_area") or focus or ""
                skill_name = _pick_demo_skill(focus_area)
                quest = build_player_quest(
                    _demo_task_title(player.get("primary_goal") or goal),
                    skill_name,
                    minutes=45,
                    due_days=2,
                    notes="Added during the demo to show QuestBoard's code-first intake path.",
                    importance="high",
                    focus_area=focus_area,
                )
                due_date = (datetime.date.today() + datetime.timedelta(days=quest["due_days"])).isoformat()
                payload, dropped = await filter_properties_for_database(mcp, db_ids["Quest Board"], {
                    "Quest": quest["quest"],
                    "Status": "Available",
                    "Rarity": quest["rarity"],
                    "Skill": quest["skill"],
                    "XP Reward": quest["xp_reward"],
                    "Due Date": due_date,
                    "Description": quest["description"],
                    "Difficulty": quest["difficulty"],
                    "Source": quest["source"],
                    "Why This Quest": quest["why_this_quest"],
                    "Generation Mode": "Player",
                    "Review State": "Approved",
                    "Correction Notes": "",
                    "Source Run": "player-intake",
                    "Prompt Version": "player-intake-v1",
                    "Fallback Reason": "",
                })
                page = await mcp.create_db_page(db_ids["Quest Board"], payload, icon="📝")
                await mcp.create_comment(
                    page.get("id", ""),
                    f"📝 *Demo intake quest created.* Why now: {quest['why_this_quest']}"
                )
                quest["due_date"] = due_date
                quest["dropped_properties"] = dropped
                return quest

        intake_result = _run(_demo_intake())
        console.print(Panel(
            f"[bold green]{intake_result['quest']}[/bold green]\n\n"
            f"Skill: [cyan]{intake_result['skill']}[/cyan]\n"
            f"Difficulty: {intake_result['difficulty']}\n"
            f"Rarity: {intake_result['rarity']}\n"
            f"XP: [green]{intake_result['xp_reward']}[/green]\n"
            f"Due: {intake_result['due_date']}\n"
            f"Why this quest: {intake_result['why_this_quest']}"
            f"{f'\\n\\n[yellow]Skipped unsupported properties:[/yellow] {', '.join(intake_result['dropped_properties'])}' if intake_result.get('dropped_properties') else ''}",
            border_style="green",
            title="📝 Code-First Intake",
        ))
    except Exception as e:
        console.print(f"[yellow]⚠️ Code-first intake skipped: {e}[/yellow]")

    time.sleep(1)

    # Step 4: Boss Battle (optional)
    console.print("\n[bold cyan]━━━ ACT 4: Boss Battle ━━━[/bold cyan]\n")
    console.print("[dim]A boss targets your weakest skill...[/dim]\n" if not skip_ai else "[dim]Using the deterministic boss template for a safer reveal...[/dim]\n")

    try:
        from .engines.quest_generator import generate_boss_battle

        async def _boss():
            async with NotionMCP() as mcp:
                return await generate_boss_battle(mcp, allow_llm=not skip_ai)

        result = _run(_boss())
        console.print(Panel(
            f"[bold red]🐉 {result.get('boss_name', 'A fearsome foe')}[/bold red]\n\n"
            f"{result.get('description', 'A mighty challenge awaits...')}\n\n"
            f"Reward: [yellow]{result.get('xp_reward', 200)} XP[/yellow]  |  "
            f"Skill: [cyan]{result.get('skill', 'Unknown')}[/cyan]\n"
            f"Generation: [bold]{result.get('generation_mode', 'llm')}[/bold]\n"
            f"Why now: {result.get('why_this_quest', 'It targets the weakest part of the current build.')}",
            border_style="red",
            title="⚔️ BOSS BATTLE",
        ))
    except Exception as e:
        console.print(f"[yellow]⚠️ Boss battle skipped: {e}[/yellow]")

    time.sleep(1)

    console.print("\n[dim]→ Open Notion now to see the fully primed QuestBoard workspace. ←[/dim]\n")
    time.sleep(2)

    # Step 5: Complete a starter quest to show XP flow
    console.print("\n[bold cyan]━━━ ACT 5: Quest Completion & XP ━━━[/bold cyan]\n")
    console.print("[dim]Completing a starter quest to show the XP engine...[/dim]\n")

    try:
        async def _complete_starter():
            async with NotionMCP() as mcp:
                available_quests = [normalize_quest(page) for page in await get_quest_pages(mcp, db_ids)]
                starter = next(
                    (
                        quest for quest in available_quests
                        if quest.get("status") == "Available" and quest.get("source") == "Player"
                    ),
                    None,
                )
                if starter is None:
                    starter = next((quest for quest in available_quests if quest.get("status") == "Available"), None)
                if starter and starter.get("id"):
                    await mcp.update_page(starter["id"], {"Status": "Completed"})
                    synced = await sync_completed_quests(mcp)
                    if synced:
                        return synced[0]
                return None

        xp_result = _run(_complete_starter())
        if xp_result:
            achievements = xp_result.get("achievements_unlocked") or []
            achievements_line = (
                f"\nAchievements: [yellow]{', '.join(achievements)}[/yellow]"
                if achievements else ""
            )
            if xp_result.get("leveled_up"):
                console.print(Panel(
                    f"[bold yellow]🎉 LEVEL UP![/bold yellow]\n\n"
                    f"XP earned: [green]+{xp_result['xp_earned']}[/green]\n"
                    f"Total XP: [bold]{xp_result['new_total_xp']}[/bold]\n"
                    f"Level: [bold]{xp_result['new_level']}[/bold]\n"
                    f"Skill: [cyan]{xp_result['skill']}[/cyan]\n"
                    f"Streak: [bold cyan]{xp_result.get('streak_days', 0)}[/bold cyan] day(s)"
                    f"{achievements_line}",
                    border_style="yellow",
                    title="⭐ Level Up!",
                ))
            else:
                console.print(Panel(
                    f"[bold green]✅ Quest Complete![/bold green]\n\n"
                    f"XP earned: [green]+{xp_result['xp_earned']}[/green]\n"
                    f"Total XP: [bold]{xp_result['new_total_xp']}[/bold]\n"
                    f"Skill: [cyan]{xp_result['skill']}[/cyan]\n"
                    f"Streak: [bold cyan]{xp_result.get('streak_days', 0)}[/bold cyan] day(s)"
                    f"{achievements_line}",
                    border_style="green",
                ))
        else:
            console.print("[yellow]No quests found to complete.[/yellow]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Quest completion skipped: {e}[/yellow]")

    time.sleep(1)

    # Step 6: Weekly recap
    console.print("\n[bold cyan]━━━ ACT 6: Weekly Recap ━━━[/bold cyan]\n")
    console.print("[dim]The Chronicler turns your progress into a story...[/dim]\n")

    try:
        from .engines.recap_writer import generate_weekly_recap

        async def _recap():
            async with NotionMCP() as mcp:
                return await generate_weekly_recap(mcp, allow_llm=not skip_ai)

        recap_result = _run(_recap())
        console.print(Panel(
            f"[bold blue]📖 Adventure Recap Created![/bold blue]\n\n"
            f"Week: [bold]{recap_result['week']}[/bold]\n"
            f"Quests completed: [green]{recap_result['quests_completed']}[/green]\n"
            f"XP earned: [yellow]{recap_result['xp_earned']}[/yellow]\n"
            f"MVP skill: [cyan]{recap_result['mvp_skill']}[/cyan]\n"
            f"Generation: [bold]{recap_result.get('generation_mode', 'llm')}[/bold]",
            border_style="blue",
        ))
    except Exception as e:
        console.print(f"[yellow]⚠️ Weekly recap skipped: {e}[/yellow]")

    time.sleep(1)

    # Step 6: Summary
    console.print("\n[bold cyan]━━━ SUMMARY ━━━[/bold cyan]\n")

    summary_table = Table(title="🎮 QuestBoard Demo Complete", border_style="yellow")
    summary_table.add_column("Metric", style="bold")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Databases created", str(len(db_ids)))
    summary_table.add_row("Views created", "15 hosted" if not get_config().is_self_hosted else "Manual on self-hosted")
    summary_table.add_row("MCP operation types", "8")
    summary_table.add_row("AI engines", "3 (Quest, Boss, Recap)")
    summary_table.add_row("Code-first flow", "Quest intake shown")
    summary_table.add_row("Server type", "Hosted" if not get_config().is_self_hosted else "Self-hosted")
    console.print(summary_table)

    console.print(Panel(
        "[bold yellow]⚔️ Your Notion workspace is now an RPG.[/bold yellow]\n\n"
        "[dim]Mark quests ✅ in Notion → run `questboard sync` → earn XP[/dim]\n"
        "[dim]Generate AI quests → `questboard quests`[/dim]\n"
        "[dim]Face a boss → `questboard boss`[/dim]\n"
        "[dim]Weekly recap → `questboard recap`[/dim]",
        border_style="yellow",
    ))


@app.command()
def revise(
    review_id: str = typer.Argument(..., help="Review queue page ID or URL"),
    notes: Optional[str] = typer.Option(None, "--notes", help="Override or provide correction notes for the revision pass"),
    reviewer: Optional[str] = typer.Option(None, "--reviewer", help="Who requested the revision"),
    template_only: bool = typer.Option(False, "--template-only", help="Use deterministic fallbacks instead of LLM calls for the revision pass"),
    force: bool = typer.Option(False, "--force", help="Override locked review items"),
):
    """Revise a reviewed quest or recap using the stored correction notes."""
    from .mcp_client import NotionMCP
    from .operations import revise_review_item
    from .setup_workspace import load_workspace_state

    normalized_review_id = _normalize_page_reference_or_exit(review_id, "review item reference")
    effective_reviewer = reviewer or (load_workspace_state() or {}).get("player_name") or get_config().player_name

    async def _revise():
        async with NotionMCP() as mcp:
            return await revise_review_item(
                mcp,
                normalized_review_id,
                notes=notes,
                reviewer=effective_reviewer,
                allow_llm=not template_only,
                force=force,
            )

    result = _run(_revise())
    console.print(Panel(
        f"Item: [bold]{result['item']}[/bold]\n"
        f"Type: {result['item_type'] or 'Unknown'}\n"
        f"State: [green]{result['new_state']}[/green]\n"
        f"Generation: {result.get('generation_mode') or 'llm'}\n"
        f"Reviewer: {result['reviewer'] or '-'}\n"
        f"Source run: {result.get('source_run') or '-'}\n"
        f"Fallback: {result.get('fallback_reason') or '-'}\n"
        f"Notes: {result['notes'] or '-'}",
        border_style="cyan",
        title="Revision Applied",
    ))


@app.command()
def preflight(
    parent_page_id: Optional[str] = typer.Argument(
        None, help="Notion page ID or URL to test against (enables full checks)"),
):
    """Run preflight checks to verify everything works before demo."""
    from .preflight import run_all_checks, print_results

    console.print(Panel(
        "[bold cyan]🛫 QuestBoard Preflight Check[/bold cyan]\n"
        "Validating MCP connection, tools, and capabilities...",
        border_style="cyan",
    ))
    if not get_config().is_self_hosted:
        console.print(Panel(_hosted_page_access_tip(), border_style="blue", title="Hosted Page Access"))

    normalized_parent_id = None
    if parent_page_id:
        normalized_parent_id = _normalize_page_reference_or_exit(parent_page_id, "preflight page reference")

    results = _run(run_all_checks(normalized_parent_id))
    all_passed = print_results(results)

    if not all_passed:
        raise typer.Exit(code=1)


@app.command()
def test(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed test output"),
):
    """Run the offline test suite (no Notion connection needed)."""
    console.print("[cyan]🧪 Running QuestBoard test suite...[/cyan]\n")

    cmd = [sys.executable, "-m", "pytest", "tests/", "-x"]
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")

    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(__file__)))

    if result.returncode == 0:
        console.print("\n[bold green]✅ All tests passed![/bold green]")
    else:
        console.print("\n[bold red]❌ Some tests failed. Fix before submitting.[/bold red]")
        raise typer.Exit(code=1)


def _extract_number(props: dict, name: str, default: int = 0) -> int:
    val = props.get(name, {})
    if isinstance(val, dict) and "number" in val:
        return val["number"] or default
    return default


def _extract_text(props: dict, name: str, default: str = "") -> str:
    val = props.get(name, {})
    if isinstance(val, dict) and "rich_text" in val:
        texts = val["rich_text"]
        if isinstance(texts, list) and texts:
            return texts[0].get("plain_text", default)
    return default


if __name__ == "__main__":
    app()
