# Hermes Snow Search

<p align="center"><img src="https://avatars.githubusercontent.com/u/286937193?v=4" width="500" alt="Snow"></p>

> [![GitHub](https://img.shields.io/badge/GitHub-mlinquan%2Fhermes--snow--search-blue?logo=github)](https://github.com/mlinquan/hermes-snow-search)
> English | [Chinese](README_CN.md) | [Changelog](CHANGELOG.md)

In-memory parallel search plugin for [Hermes Agent](https://hermes-agent.nousresearch.com).
Loads session history, holographic facts (fact_store), built-in memory (MEMORY.md / USER.md), and skill metadata (SKILL.md) into RAM.
Deep search (message bodies) queries the existing FTS5 database index — near-instant startup, millisecond search, no memory overhead.

## Why snow_search? (vs session_search)

`session_search` searches chat history. `snow_search` is Hermes' **global memory retrieval layer** — making the AI remember you across devices, return complete answers in one shot, and keep its personality persistent.

| Value | snow_search | session_search |
|-------|-------------|----------------|
| **Cross-device memory** | Switch devices / start a new session — the AI still "remembers", picks up where you left off | Only the current DB's messages |
| **One-shot recall** | Cross-source aggregation + ranking + confidence. No repeated queries, no paging — the agent gets the answer directly | Returns raw messages; agent must re-query, combine, judge relevance itself |
| **Persistent personality** | Searches memory (USER.md) + soul + facts — the AI remembers *who you are*, your preferences, your hard rules | Only searches *what was said* |

**The core difference:** session_search finds chat; snow_search makes the AI genuinely remember you.

## Key Advantages

| # | Advantage | Detail |
|---|-----------|--------|
| 1 | **Cross-device memory** | Switch devices, clear context — the AI "picks up where you left off". Not a reintroduction, persistent personality |
| 2 | **One-shot recall** | 5 sources in parallel, ranked + confidence-labeled. Saves tokens, saves round-trips, answers sooner |
| 3 | **Persistent personality** | Unified search over memory + soul + facts. The AI remembers who you are and how to treat you |
| 4 | **<3s startup** | One SQL probe; deep search reuses the FTS5 index |
| 5 | **~MB memory** | Only lightweight stores in RAM; message bodies stay in the DB |
| 6 | **Precise total** | FTS5 COUNT(*) — agents can answer "how many times" accurately |
| 7 | **Auto incremental updates** | fact_store/memory writes append instantly; FTS5 triggers keep the message index live |
| 8 | **Context-safe** | post_llm_call auto-clears search output — conversation stays smooth |

## Examples

Ask the AI in plain language — snow_search triggers automatically. No parameters to remember, just ask like you'd ask a person:

**Time recall**
- "Remind me what we talked about yesterday"
- "What have I been working on for the past two weeks?"
- "That bug we discussed last Wednesday — how did we end up fixing it?"
- "When did this project actually start?"

**Cross-device memory**
- "I switched devices — where did we leave off last time?"
- "Pick up where we got to in that discussion"

**Cross-source recall (answers aren't only in chat)**
- "Where's the cdog config file?" → hits facts / memory
- "What hard rules did I set?" → hits memory / soul
- "What's the current progress on snow-agent?" → hits facts
- "How do I use the cdog skill?" → hits skills

**Precise counts ("how many times")**
- "How many times did the 502 error come up this week?"
- "How often did I bring up refactoring snow-search this month?"

**Role filtering ("did I say / did you say")**
- "Did I ever mention refactoring snow-agent?" → user messages only
- "How did you teach me to use cdog last time?" → assistant messages only

## How it works

1. **Eager load (lightweight)** — session summaries, facts, memory entries, skill metadata loaded in background thread
2. **Keep in RAM (lightweight only)** — sessions, facts, memory, skills live in Python lists
3. **FTS5 for deep search** — message bodies stay in SQLite; `messages_fts` (unicode61) and `messages_fts_trigram` (CJK) are queried at search time
4. **Parallel search** — `ThreadPoolExecutor` runs lightweight stores concurrently; deep search runs FTS5 query inline
5. **Incremental updates** — `post_tool_call` hook catches `fact_store add` and `memory add` → appends to cache
6. **CJK routing** — ≥3 CJK chars → trigram table; English/mixed → unicode61; short CJK (1-2 chars) → LIKE fallback

## Installation

```bash
pip install hermes-snow-search
hermes plugins enable hermes-snow-search
# Restart Hermes (/new or re-launch)
```

## Configuration

```yaml
plugins:
  hermes-snow-search:
    memory_limit_mb: 500          # cap for lightweight stores (sessions/facts/memory/skills)
    session_max: 7000
    fact_max: 10000
    deep_search_load_mode: startup  # off | startup | ondemand
```

| Key | Default | Description |
|-----|---------|-------------|
| `memory_limit_mb` | 500 | Cap for lightweight stores. Deep search uses FTS5 (DB-side) and doesn't count against this |
| `session_max` | 7000 | Max session entries in lightweight cache |
| `fact_max` | 10000 | Max fact entries in cache |
| `deep_search_load_mode` | `startup` | Deep search behavior: `off` (disabled), `startup` (preload at boot), `ondemand` (lazy on first query) |

> `memory_limit_mb` applies to lightweight stores only. Deep search queries the existing FTS5 index in the database — zero additional RAM.

## Context Cleanup (post_llm_call)

After every LLM response, the `post_llm_call` hook clears snow_search tool output from conversation history. This prevents search results from accumulating across turns — one search round adds ~9K–18K chars to context, but the hook nullifies it before the next user message.

> **Note:** Only snow_search tool output is cleared — other tool results and the search index itself stay intact.

## Deep Search

Enabled by default (`deep_search_load_mode: startup`). Queries the existing FTS5 index in the database for full message-body search — no load step, no memory overhead. Results include `session_id`, `timestamp`, `role`, `snippet`, and `search_info`.

### FTS5 routing

| Query | Table | Tokenizer |
|-------|-------|-----------|
| English / mixed | `messages_fts` | unicode61 (word-boundary) |
| CJK ≥ 3 chars | `messages_fts_trigram` | trigram (3-char sliding window) |
| CJK 1-2 chars | (LIKE fallback) | substring match |

The FTS5 tables are maintained automatically by triggers in `hermes_state` — every message insert/update/delete updates the index. No reload needed when new messages arrive.

Startup output:

```
  ┊ ❄️ [Hermes Snow Search] Deep search ready (FTS5) | 222500 messages | 44 days (May 13 ~ Jun 26) | ~147 MB indexed on disk | 2.4s
```

### Sort modes

| `sort` | Behavior |
|--------|----------|
| `relevance` (default) | FTS5 rank (BM25) first, then source priority as tiebreaker |
| `oldest` | Earliest timestamp first — answer "when did X first happen" |
| `newest` | Latest timestamp first — answer "when was the last X" |

### Performance

| Mode | Searches | Latency | Memory |
|------|----------|---------|--------|
| Lightweight | Session summaries | <1ms | ~3 MB |
| Deep (FTS5) | Full message bodies | 0.1–0.2s | ~0 (DB-side index) |

Startup: <3s (one stats probe). No index build, no message load. Previously this was ~125s (full load + in-memory index build) and ~147 MB RAM.

## Action Modes

Say **"snow reload"** to rebuild the index from disk, or **"snow status"** to inspect current index state. The tool description guides the agent to pass the correct action parameter (`action=reload` or `action=status`).

> **Note:** `snow reload` rebuilds the RAM search index (sessions, skills, facts, memory). It does NOT affect the LLM context — context is managed separately by Hermes system prompt injection.

The `action` parameter controls what `snow_search` does:

| `action` | Behavior | Returns |
|----------|----------|---------|
| `search` (default) | Run a query across all stores | Hits + search_info |
| `reload` | Clear and reload the entire index from disk | Full status JSON |
| `status` | Return current index state (zero I/O) | Full status JSON |

### Status / Reload response

```json
{
  "success": true,
  "action": "status",
  "counts": {"sessions": 263, "facts": 310, "memory": 64, "deep_messages": 222500, "skills": 105},
  "memory": {"current_mb": 0.2, "deep_mb": 0},
  "coverage": {"full_coverage": true, "date_range": "May 13 ~ Jun 26", "fts_mode": true},
  "ready": true,
  "deep_ready": true
}
```

## Skills Cache

Skill metadata from `~/.hermes/skills/*/SKILL.md` is pre-loaded on startup as a 5th data source (`"skills"` in `stores_available`). Each skill entry includes `name`, `description`, `tags`, and `category` (directory name). Enabled by default — set `include_skills: false` to skip.

Use `snow_search` to discover available skills. Never read SKILL.md files or Hermes core tool descriptions directly.

## Full Coverage

Check `search_info.full_coverage` — if `true`, snow_search covers everything. If `false`, `session_search` may be needed for older sessions. In FTS5 mode, `full_coverage` is always `true` (the DB index covers all messages).

## Caveats

- **Startup:** <3s to probe DB stats. Searches are 0.1–0.2s via FTS5.
- **Root sessions only:** Deep search filters `parent_session_id IS NULL`. Subagent sessions (delegate_task children) are excluded.
- **Tool messages excluded:** Only `user` and `assistant` role messages are stored.
- **FTS5 required:** Deep search requires SQLite FTS5 + trigram tokenizer (standard in Python 3.11+). Falls back to in-memory index if unavailable.

## Usage Tips

- **"Latest" questions match naturally** — newest-first sort with recency in FTS5 rank.
- **"First time" questions use `sort="oldest"`** — the earliest hit moves to the top.
- **Specific keywords win** — "database migration schema users" beats "that database thing".
- **Cross-process auto-sync** — FTS5 triggers keep the index current; no manual reload needed.
- **Trust the result** — snow_search sweeps everything. If it found nothing, there's no record.

## Author

LinQuan & Snow (AI Girl)

## Star History

<a href="https://www.star-history.com/?repos=mlinquan%2Fhermes-snow-search&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=mlinquan/hermes-snow-search&type=date&legend=top-left" />
 </picture>
</a>
