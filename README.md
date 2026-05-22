# Hermes Snow Search

<p align="center"><img src="assets/avator_default_png8.png" width="500" alt="Snow"></p>

> [![GitHub](https://img.shields.io/badge/GitHub-mlinquan%2Fhermes--snow--search-blue?logo=github)](https://github.com/mlinquan/hermes-snow-search)
> English | [Chinese](README_CN.md)

In-memory parallel search plugin for [Hermes Agent](https://hermes-agent.nousresearch.com).
Loads session history, holographic facts (fact_store), built-in memory (MEMORY.md / USER.md), and skill metadata (SKILL.md) into RAM.
Searches all stores in parallel — results in <1ms. Supports full message-body deep search, hot-reload, and status inspection.

## Key Advantages

| # | Advantage | Detail |
|---|-----------|--------|
| 1 | **Sub-millisecond** | RAM-resident search. No I/O, no SQLite — results in <1ms |
| 2 | **5 sources in parallel** | Sessions + holographic facts + built-in memory + skill metadata + full message bodies. Searched concurrently via `ThreadPoolExecutor` |
| 3 | **Deep search** | Full message-body index with session_id, timestamp, role. Covers 12K+ messages across all sessions |
| 4 | **Auto-cleanup** | `post_llm_call` hook clears tool output from context. 107 hits / ~34K chars → clean slate (~7K fixed overhead) before next turn |
| 5 | **Cross-session** | Not limited to current conversation. Searches every past session in one go |
| 6 | **Hot reload** | `snow reload` rebuilds the RAM index from disk. No Hermes restart needed |
| 7 | **Zero-I/O status** | `snow status` returns full index snapshot without touching disk |
| 8 | **Incremental updates** | Writes to fact_store / memory are appended to cache instantly — no full reload |
| 9 | **Auto-eviction** | When >80% of memory limit, oldest/lowest-trust entries are evicted automatically |
|10 | **Full coverage guarantee** | When `full_coverage` is `true`, deep search covers every stored message — no fallback needed |

## How it works

1. **Eager load** — sessions, facts, memory entries, and skill metadata are loaded in a background thread right after Hermes starts
2. **Keep in RAM** — sessions, facts, memory, and skills live in Python lists, no I/O on search
3. **Parallel search** — `ThreadPoolExecutor` runs all stores concurrently
4. **Incremental updates** — `post_tool_call` hook catches `fact_store add` and `memory add` → appends to cache
5. **Eviction** — `pre_llm_call` hook checks memory usage; evicts oldest/lowest-trust entries when >80% of limit
6. **Deep search** — full message-body index with session_id + timestamp + role. Incremental refresh via `SELECT MAX(id)`
7. **Skills cache** — `~/.hermes/skills/*/SKILL.md` frontmatter (name, description, tags) pre-loaded on startup

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
    memory_limit_mb: 200          # safety cap, not actual usage
    session_max: 7000
    fact_max: 10000
    deep_search_enabled: true     # set false to use lightweight only
    deep_search_load_mode: "ondemand"   # "ondemand" | "startup"
```

| Key | Default | Description |
|-----|---------|-------------|
| `memory_limit_mb` | 200 | Hard memory cap; eviction triggers at 80% |
| `session_max` | 7000 | Max session entries in lightweight cache |
| `fact_max` | 10000 | Max fact entries in cache |
| `deep_search_enabled` | true | Enables full message-body search. Set `false` for lightweight-only mode |
| `deep_search_load_mode` | ondemand | `ondemand` = load on first search, `startup` = background at boot |

> `memory_limit_mb` (200 MB) is a safety cap, not actual usage. One week of real conversation (~230 sessions, ~10,000 messages) fits in ~6 MB of lightweight data, ~6 MB additional for deep search.

### Memory Recommendations

- **Lightweight mode:** 20 MB is sufficient for session summaries, facts, memory, and skills. Set `deep_search_enabled: false` to stay in lightweight mode.
- **Deep search (default):** Full message bodies consume ~6 MB/week. 200 MB covers ~6 months, 500 MB covers ~1 year.
- **Multiple profiles:** When running multiple Hermes profiles, budget N × `memory_limit_mb` since each process has its own in-memory index.

## Context Cleanup (post_llm_call)

After every LLM response, `on_post_llm_call` hook clears snow_search tool output from conversation history. This prevents search results from accumulating across turns — one search round adds ~9K–18K chars to context, but the hook nullifies it before the next user message.

**Empirical verification:** Two sequential deep searches (107 hits, ~34K chars total) were injected into context. After the LLM replied, `post_llm_call` cleared all search output — next turn carried only the fixed ~7K chars of memory + user profile.

```python
# Hook logic
for msg in history:
    if msg.get("role") == "tool" and msg.get("name") == "snow_search":
        msg["content"] = ""  # clear from context
```

> **Note:** The hook clears snow_search tool output only. It does not touch other tool results or the search index itself (which stays in RAM for the next call).

## Deep Search

Enabled by default (`deep_search_enabled: true`). When active, full message-body search replaces lightweight session summaries automatically. Results include `session_id`, `timestamp`, `role`, and `search_info`.

### Load modes

| Mode | When | Behavior |
|------|------|----------|
| `ondemand` (default) | On first deep search | Blocks until index is built, shows progress |
| `startup` | Background, 2.5s after startup | Non-blocking, prints progress at ~0/50/100% |

Progress is written to stderr:

```
[Hermes Snow Search] Loading deep search index...
[Hermes Snow Search] Session 65/263 | 3,000 messages | 15/200 MB | ~0.5s remaining
[Hermes Snow Search] Deep search ready | 12,000 messages | 10 days (May 13 ~ May 22) | 7.5 MB
```

Index builds from newest sessions backwards, stops at 85% of `memory_limit_mb`. Subsequent calls use `SELECT MAX(id)` for incremental refresh — cross-process sync is automatic (shared state.db).

### Sort modes

| `sort` | Behavior |
|--------|----------|
| `relevance` (default) | Best match first (recency + keyword score) |
| `oldest` | Earliest timestamp first — answer "when did X first happen" |
| `newest` | Latest timestamp first — answer "when was the last X" |

### Performance

| Mode | Searches | Latency | Memory (1 week) |
|------|----------|---------|-----------------|
| Lightweight | Session summaries | <0.5ms | ~3 MB |
| Deep | Full message bodies | ~1-5ms | ~6 MB |

Lightweight and deep mode never load simultaneously — deep mode skips sessions and loads facts + memory + messages.

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
  "counts": {"sessions": 263, "facts": 310, "memory": 64, "deep_messages": 12000, "skills": 105},
  "memory": {"current_mb": 0.2, "deep_mb": 7.5},
  "coverage": {"full_coverage": true, "date_range": "May 13 ~ May 22"},
  "ready": true,
  "deep_ready": true
}
```

## Skills Cache

Skill metadata from `~/.hermes/skills/*/SKILL.md` is pre-loaded on startup as a 5th data source (`"skills"` in `stores_available`). Each skill entry includes `name`, `description`, `tags`, and `category` (directory name). Enabled by default — set `include_skills: false` to skip.

Use `snow_search` to discover available skills. Never read SKILL.md files or Hermes core tool descriptions directly.

## Full Coverage

Check `search_info.full_coverage` — if `true`, snow_search covers everything. If `false`, `session_search` may be needed for older sessions.

## Caveats

- **First use delay (ondemand):** First deep search triggers index building (~1s for ~1 week).
- **Root sessions only:** Deep search indexes user ↔ assistant conversations. Subagent sessions (delegate_task children) are excluded.
- **Tool messages excluded:** Only `user` and `assistant` role messages are stored.
- **Partial coverage:** When `full_coverage` is false, combine with `session_search` for complete results.

## Usage Tips

- **"Latest" questions match naturally** — snow_search ranks by relevance with recency boost.
- **"First time" questions use `sort="oldest"`** — the earliest hit moves to the top.
- **Specific keywords win** — "database migration schema users" beats "that database thing".
- **Cross-process auto-sync** — no manual reload needed between CLI and Gateway.
- **Trust the result** — snow_search sweeps everything in RAM. If it found nothing, there's no record.

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
