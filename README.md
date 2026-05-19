# Herme Snow Search

> [![GitHub](https://img.shields.io/badge/GitHub-mlinquan%2Fhermes--snow--search-blue?logo=github)](https://github.com/mlinquan/hermes-snow-search)
> English | [中文版](README_CN.md)

In-memory parallel search plugin for [Hermes Agent](https://hermes-agent.nousresearch.com).
Loads session history, holographic facts (fact_store), and built-in memory (MEMORY.md / USER.md) into RAM at startup.
Searches all three stores in parallel — results in <1ms.

## How it works

1. **Eager load** — data is loaded in a background thread right after Hermes starts
2. **Keep in RAM** — sessions, facts, and memory entries live in Python lists, no I/O on search
3. **Parallel search** — `ThreadPoolExecutor` runs the three stores concurrently
4. **Incremental updates** — `post_tool_call` hook catches `fact_store add` and `memory add` → appends to cache
5. **Eviction** — `pre_llm_call` hook checks memory usage; evicts oldest/lowest-trust entries when >80% of limit

> **Memory provider note:** Currently supports Hermes' built-in holographic memory (fact_store). Support for other memory providers (mem0, supermemory, Honcho, etc.) is on the roadmap.

## Performance

| | Before | After |
|--|--------|-------|
| Query path | 3 serial DB queries | 1 parallel RAM search |
| Typical latency | ~350–700ms | **<0.5ms** |

Data lives in RAM from startup. No disk I/O, no serial waits. Just one parallel fetch across all three stores.

## Startup

Silent background load — no terminal output. Data is ready before the first `snow_search` call.

> **Memory note:** Each Hermes instance loads its own copy. CLI and Gateway
> run as separate processes, so memory usage can reach 2× your configured
> `memory_limit_mb` (e.g. 40 MB total if set to 20 MB).

On first use, the terminal shows:

```
preparing snow_search…
  ┊ ⚡ snow_sear <query>  0.0s
  ┊ 🔍 recall    "<query>"  0.3s
```

Key signs of success:
- `preparing snow_search…` confirms the tool is registered and ready
- Response comes back instantly (RAM-speed)

## Usage

`snow_search` is an AI-side tool. When you ask Hermes Agent about past conversations —
"what did we discuss last week" or "I remember we talked about…" — she calls it automatically.

You can also explicitly mention it: `snow_search query="database migration"`

On first use, the terminal shows:

```
preparing snow_search…
  ┊ ⚡ snow_sear <query>  0.0s
  ┊ 🔍 recall    "<query>"  0.3s
```

That's it — results come back in the same response.

> **Tip:** snow_search is fast enough (<0.5ms) that your AI may instinctively
> double-check results with slower tools (session_search, sqlite3). To prevent this,
> add a rule to your agent's behavioral config (e.g. `SOUL.md`, `MEMORY.md`, or `agent.personalities` in `config.yaml`):
> _snow_search results are final — trust them, don't re-query._

## Installation

```bash
# 1. Install in Hermes venv (editable mode, one-time)
pip3 install -e ~/works/hermes-snow-search

# 2. Enable the plugin
hermes plugins enable hermes-snow-search

# 3. Restart Hermes session (/new or re-launch)
```

## Configuration

```yaml
plugins:
  enabled:
    - hermes-bus-plugin
    - hermes-snow-search
  hermes-snow-search:
    memory_limit_mb: 20
    session_max: 7000
    fact_max: 10000
    memory_max: 100
```

| Key | Default | Description | Retention |
|-----|---------|-------------|-----------|
| `memory_limit_mb` | 20 | Hard memory cap; triggers eviction at 80% | 6+ months at ~35 sessions/day |
| `session_max` | 7000 | Max session entries in cache | 6+ months (~200 days) at ~35 sessions/day |
| `fact_max` | 10000 | Max fact entries in cache | Rarely hit before session eviction |
| `memory_max` | 100 | Max MEMORY.md/USER.md entries in cache | ~<1 KB, never a bottleneck |

Both primary limits are balanced for ~6 months of daily usage at ~1 billion tokens/day
(heavy use, ~35 sessions/day). At ~86 KB/day total growth, `session_max` (7000) and
`memory_limit_mb` (20 MB) expire around the same time — sessions are the slightly
tighter bottleneck by design. 6 months of data at this rate totals ~13 MB.

### Recommended tiers for longer retention

| Tier | Retention | `session_max` | `memory_limit_mb` | Search speed |
|------|-----------|---------------|-------------------|--------------|
| Default | 6 months | 7,000 | 20 | <1ms |
| 1 year | 1 year | 13,000 | 35 | <1ms |
| 2 years | 2 years | 26,000 | 70 | ~1ms |
| Unlimited | Never evict | 50,000 | 150 | ~2ms |

RAM search scales linearly with data — 10x more data ≈ 5x slower, but still under 2ms even for 2+ years. Speed is never the bottleneck; adjust retention to your comfort.
