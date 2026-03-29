# QuestBoard

Turn your Notion workspace into an RPG. QuestBoard is a Python app that uses Notion MCP to create and operate a gamified workspace with quests, boss battles, skill trees, XP, recaps, runtime automation, and human review workflows.

## What It Does

- Creates a full QuestBoard workspace inside Notion
- Guides first-time setup with `questboard onboard` so a real goal, time budget, motivation, and constraints shape the workspace
- Generates quests from your focus areas and skill gaps
- Creates boss battles for neglected skills or habits
- Tracks XP, leveling, streaks, and achievements
- Writes weekly recaps with AI or deterministic fallbacks
- Can optionally ground ambiguous goals with Exa or Perplexity before the first quests are generated
- Logs automation runs in `Runs`
- Routes generated output through a `Review Queue`
- Supports revision loops for reviewed quests and recaps
- Runs a policy-based runtime loop with `questboard runtime` and `questboard watch`

## Notion MCP Integration

QuestBoard is MCP-native. Notion remains the main interface, while the CLI acts as the control plane.

| Operation | Self-hosted tool | Hosted tool | QuestBoard usage |
|---|---|---|---|
| Create database | `create-a-data-source` | `notion-create-database` | 8 interconnected databases |
| Create pages | `post-page` | `notion-create-pages` | Quests, boss battles, recaps, achievements |
| Update page | `patch-page` | `notion-update-page` | XP, levels, quest status, skill progression |
| Create view | - | `notion-create-view` | Table, Board, List, Calendar, Timeline, Gallery, Chart, Dashboard |
| Search | `post-search` | `notion-search` | Find quests, player data, stale work, skill gaps |
| Fetch | `retrieve-a-page` | `notion-fetch` | Read player, quest, skill, and recap data |
| Comment | `create-a-comment` | `notion-create-comment` | Flavor text, warnings, and progression feedback |
| Move pages | `move-page` | `notion-move-pages` | Archival and reorganization flows |

## Hosted vs Self-hosted

### Hosted Notion MCP

Use [mcp.notion.com](https://mcp.notion.com) for the richest QuestBoard experience. The hosted path creates 14 named views across 8 hosted view types, including chart, dashboard, Run Center, and Review Queue surfaces.

### Self-hosted Notion MCP

Use the self-hosted server for fast local validation and token-based auth. Core gameplay works well, but hosted-only views are not auto-created there, so you may need to add or customize some views manually in Notion.

## Prerequisites

- Python 3.11+
- Node.js 18+ if you want the self-hosted MCP server
- A Notion page you can edit
- An LLM API key if you want live AI generation

QuestBoard supports:

- Anthropic
- Mistral through the OpenAI-compatible path
- Other OpenAI-compatible providers such as OpenRouter

If you do not want live AI generation, use `--template-only` or `--skip-ai`.

## Quick Start

### Hosted Setup

```bash
git clone https://github.com/bO-05/questboard.git
cd questboard
pip install -e .

questboard hosted-login
questboard onboard <YOUR_NOTION_PAGE_ID_OR_URL> --name "YourName"
questboard doctor <YOUR_NOTION_PAGE_ID_OR_URL>
```

For the page you pass into QuestBoard on the hosted path, use:

- General access: `Only people invited`
- Page permission: `Can edit`
- Link expiration: off

The same Notion account you authorize during `questboard hosted-login` must be able to edit that page.

### Self-hosted Setup

```bash
git clone https://github.com/bO-05/questboard.git
cd questboard
pip install -e .

npx @notionhq/notion-mcp-server --transport http --port 3100 --auth-token questboard-secret

export NOTION_TOKEN=ntn_your_token
export MCP_SERVER_URL=http://localhost:3100/mcp
export MCP_AUTH_TOKEN=questboard-secret

questboard setup <YOUR_NOTION_PAGE_ID_OR_URL> --name "YourName"
questboard doctor <YOUR_NOTION_PAGE_ID_OR_URL>
```

### Guided Onboarding

If you want QuestBoard to personalize the workspace around a real goal instead of just creating the default schema, start here:

```bash
questboard onboard <PAGE_ID_OR_URL> --name "YourName"
```

`questboard onboard` asks for:

- your actual goal
- what success looks like
- how much time you realistically have
- your preferred challenge style
- your current focus area
- constraints and motivation
- optional domain notes for niche tools or jargon

If you configure `EXA_API_KEY` or `PERPLEXITY_API_KEY`, onboarding can also ground ambiguous goals before it stores a `Context Brief` in the Player Profile and generates the first personalized quests.

Example for a niche or jargon-heavy goal:

```bash
questboard onboard <PAGE_ID_OR_URL> \
  --goal "Set up my business with OpenClaw and Hermes agents to automate operations" \
  --success "A weekly operating system with repeatable agent handoffs" \
  --focus "Operations and systems" \
  --domain-notes "OpenClaw is my agent stack and Hermes handles orchestration" \
  --research-provider perplexity
```

## Core Commands

```bash
questboard hosted-login
questboard onboard <PAGE_ID_OR_URL> --name "YourName"
questboard setup <PAGE_ID_OR_URL> --name "YourName"
questboard doctor [PAGE_ID_OR_URL]
questboard calibrate --goal "Ship the product" --focus "Endurance" --style "Balanced"
questboard quests --count 5
questboard quests --count 5 --template-only
questboard intake "Ship the changelog" --skill Intelligence --minutes 40 --days 2
questboard sync
questboard runtime --dry-run
questboard runtime
questboard watch --interval 300 --iterations 3 --template-only
questboard reviews
questboard review <REVIEW_ID_OR_URL> --state Approved --notes "Looks good"
questboard revise <REVIEW_ID_OR_URL> --notes "Make it clearer"
questboard runs
questboard boss
questboard boss --template-only
questboard recap
questboard recap --template-only
questboard patrol
questboard status
questboard run-all
questboard preflight [PAGE_ID_OR_URL]
```

## Workspace Shape

QuestBoard creates 8 databases:

- Player Profile
- Quest Board
- Skill Trees
- Achievement Log
- Party Board
- Adventure Recaps
- Runs
- Review Queue

On hosted MCP, QuestBoard also creates these named views:

- Hero Dashboard
- Quest Ledger
- Quest Map
- Quest Calendar
- Quest Timeline
- Quest Cards
- XP Progress
- Skill Focus
- Skill Board
- Trophy Case
- Hero Roster
- Story Archive
- Run Center
- Review Queue

## Everyday Flow

1. Run `questboard onboard` on a blank Notion page to capture your real goal and context.
2. Run `questboard calibrate` later if you want to retune goal, focus, or play style.
3. Run `questboard quests` or `questboard intake`.
4. Do the work in real life.
5. Mark quests completed in Notion.
6. Run `questboard sync`.
7. Run `questboard runtime` or `questboard watch`.
8. Review generated output with `questboard reviews` and `questboard review`.
9. Regenerate from reviewer notes with `questboard revise`.
10. Inspect automation history with `questboard runs`.

If you want to remove LLM variability for quests, bosses, or recaps, use `--template-only` or `--skip-ai` where available.

## Architecture

```text
+----------------------------------------------------------+
|                    Notion Workspace                      |
|  Player Profile   -> Hero Dashboard                      |
|  Quest Board      -> Quest Ledger, Quest Map, Quest      |
|                      Calendar, Quest Timeline, Quest     |
|                      Cards                               |
|  Skill Trees      -> XP Progress, Skill Focus, Skill     |
|                      Board                               |
|  Achievement Log  -> Trophy Case                         |
|  Party Board      -> Hero Roster                         |
|  Adventure Recaps -> Story Archive                       |
|  Runs             -> Run Center                          |
|  Review Queue     -> Review Queue                        |
+--------------------------^-------------------------------+
                           | Notion MCP
+--------------------------+-------------------------------+
|                 QuestBoard Core (Python)                 |
|  XP Engine        Quest Generator        Recap Writer    |
|  Audit Layer      Runtime Layer          Review Ops      |
|                          |                               |
|                LLM Provider (optional)                   |
+----------------------------------------------------------+
```

## Health Checks

Use these commands before creating or reusing a workspace:

```bash
questboard doctor <PAGE_ID_OR_URL>
questboard preflight <PAGE_ID_OR_URL>
```

- `doctor` is read-only.
- `preflight` creates temporary test objects and should be run only on a disposable page.
