# Changelog

## [0.7.0] — 2026-06-27

### Fixed
- **`_deep_mode` AttributeError crashed the startup eager-load thread** — v0.6.0 removed the `deep_search_load_mode` config and the `_deep_mode` attribute, but `__init__.py` still referenced `_engine._deep_mode == "startup"`. Restored `deep_search_load_mode` (`off` | `startup` | `ondemand`, default `startup`) and derived `_deep_enabled` / `_deep_mode` from it.
- **`role_filter='tool'` always returned empty results** — the FTS/LIKE WHERE clauses hard-coded `m.role IN ('user','assistant')` *and* appended `m.role = ?`, so a `tool` filter contradicted itself. Reworked into mutually exclusive branches: an explicit `role_filter` wins; otherwise the default excludes tool output.
- **`stores_available.deep_messages` falsely reported `false` in FTS mode** — used `bool(self._deep_messages)`, which is empty when deep data lives in the DB. Now reflects `_deep_enabled and (_use_fts or ...)`.
- **Deep path never loaded sessions / skills / soul** — `handle_search(deep=True)` only called `_ensure_facts_and_memory()`, relying entirely on the background eager-load thread; if it failed or was slow these stores stayed empty forever. Added an `_ensure_loaded()` fallback in the query path.

### Added
- **Empty query + time range** — snow_search now accepts an empty `query` together with `start_timestamp` / `end_timestamp` to browse a time window. New `_search_deep_range` scans by timestamp + role with no MATCH/LIKE predicate.

### Changed
- **Removed `deep_search_enabled` config** — redundant with `deep_search_load_mode: off`. Behavior is now controlled by a single key: `off` (disabled) / `startup` (preload at boot) / `ondemand` (lazy on first query).
- Schema descriptions updated: `query` notes it may be empty with a range; `role_filter` documents the default excludes tool output; `relevance` sort notes it falls back to time order on empty queries.

### Documentation
- README / README_CN config tables updated for `deep_search_load_mode` (removed `deep_search_enabled`).

## [0.6.1] — 2026-06-26

### Added
- CHANGELOG.md for v0.1.0 through v0.6.0 (Keep a Changelog format).
- Changelog link (`[Changelog](CHANGELOG.md)`) in both README.md and README_CN.md.

### Fixed
- README/README_CN.md content restored after rebase (PR #1 merge had them, reset didn't lose them).

## [0.6.0] — 2026-06-26

### Added
- **FTS5 deep search** — queries the existing DB FTS5 index at search time instead of loading all messages into RAM. Startup <3s, search 0.1–0.2s, ~0 MB memory overhead (was 125s + 147 MB).
- **CJK routing** — three-tier FTS5 table selection:
  - CJK ≥ 3 chars → `messages_fts_trigram` (3-char sliding window)
  - English/mixed → `messages_fts` (unicode61 word-boundary)
  - Short CJK (1–2 chars) → LIKE fallback (substring match)
- `role_filter` parameter — `"user"` / `"assistant"` / `"tool"` filtering for deep search.
- `_SOURCE_PRIORITY` tiebreaker — `soul > memory > facts > deep_messages > sessions` applied when scores are equal.
- `fts_mode` flag in `status` / `search_info` output.
- Trigger keywords in tool description (`回忆`, `上次`, `最近`, `recall`, `last time`...) — guides the agent to call snow_search on memory-related queries.
- Memory fallback path (`_load_deep_memory`) for environments without FTS5.

### Fixed
- **`_count_cjk` returned runs count instead of char count** — Chinese queries ≥3 chars were routed to unicode61 instead of trigram, degrading CJK search quality. Bug existed since v0.5.0 CJK routing was introduced.
- **`_status` showed 0 messages in FTS mode** — used `len(self._deep_messages)` which is empty in FTS mode; now reports `_deep_total_messages`.
- **`_deep_total_messages` not initialized in `__init__`** — would crash if `_status` was called before first `_load_deep`.
- **`hits`/`exact_total` duplicate declaration** in `handle_search`.
- Removed deprecated `deep_search_load_mode` config (no load step in FTS mode).

### Changed
- Tool description rewritten: "Search across ALL memory stores...FTS5 ~0.1s" with full result field docs.
- `limit_per_source` default 5→10, max 20→50.
- Sort: `newest` is now the default (was `relevance`).
- `import datetime` hoisted to top-level — no longer lazy-imported in hot paths.

### Documentation
- README rewritten: product-value driven, 5 scenario categories, bilingual (EN/CN).
- `post_llm_call` section simplified — no Python code snippet.
- Config table stripped of removed options.

---

## [0.5.0] — 2026-06-16

### Added
- **Hybrid CJK tokenizer** — 2-gram sliding window for contiguous CJK runs, whitespace-split for English/Latin. Eliminates false positives from single-character English substrings.
- **Inverted-index intersection candidate filtering** — pre-filters candidate messages by intersecting index token lists before scoring. ~100x search speedup over v0.4.0 on large datasets.
- **Bulk SQL query** — replaces chunked `IN` clauses with a single query via TEMP table, eliminating N round-trips for large session sets.
- **Time range filter** — `start_timestamp` / `end_timestamp` parameters for bounded searches.
- **Timing debug log** — `handle_search` logs slow queries (>1s) for performance diagnosis.

### Fixed
- `_build_ngram_index` was referenced in `_match_score` but never defined — caused AttributeError on ngram partial-match paths.

### Performance
- Full index rebuild: ~125s → ~30s (bulk SQL + parallel chunking).
- Search latency: seconds → tens of milliseconds for most queries.

---

## [0.4.0] — 2026-05-22

### Added
- **6 data sources** — sessions, holographic facts, built-in memory (MEMORY.md/USER.md), skill metadata (SKILL.md), SOUL.md, and deep messages (message body search).
- **Action routing** — `action=search` / `action=reload` / `action=status` with `snow reload` / `snow status` CLI-style triggers.
- **Profile isolation** — all paths resolve via `get_hermes_home()`, automatically supporting `~/.hermes/profiles/<name>/` subdirectories.
- **`post_llm_call` context cleanup** — auto-zeros snow_search tool output after each LLM response to prevent inter-turn accumulation.
- **Skills cache** — `~/.hermes/skills/*/SKILL.md` frontmatter pre-loaded as a 5th data source.
- **`include_*` flags** — granular source enable/disable.

---

## [0.3.1] — 2026-05-20

### Fixed
- Added `readme` field in `pyproject.toml` for PyPI project description rendering.

---

## [0.3.0] — 2026-05-20

### Added
- **Deep search** — query full message bodies in addition to session summaries.
- **Sort modes** — `newest`, `oldest`, `relevance`.
- **`search_info`** — sessions_scanned, messages_scanned, date_range, full_coverage.
- **Confidence labels** — high / medium / low based on normalized match score.

---

## [0.2.0] — 2026-05-20

### Added
- **Incremental cache updates** — `post_tool_call` hook detects fact_store / memory writes and updates the in-memory cache instantly.

---

## [0.1.2] — 2026-05-20

### Changed
- Memory note in README references the config value (`memory_limit_mb`) instead of current volume.

---

## [0.1.1] — 2026-05-20

### Added
- Trust tip in README explaining when to rely on snow_search results.

### Fixed
- Config references in README corrected to match `config.yaml` key names.

---

## [0.1.0] — 2026-05-20

### Added
- Initial release: in-memory parallel search plugin for Hermes Agent.
- Searches across session history, holographic facts, built-in memory.
- Lazy full load on first call, cached in RAM (~<1ms search).
- Eviction check on `pre_llm_call` at >80% memory usage.
- Thread-safe loading with double-checked locking.
- Holographic memory availability detection (skips facts silently if disabled).
