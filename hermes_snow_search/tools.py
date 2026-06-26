"""Snow Search Engine — in-memory parallel memory search.

Design
------
All data lives in RAM: session summaries, holographic facts, and built-in
memory entries. Searches traverse Python lists — no SQLite, no I/O, no syscall.

Lifecycle
  1. Lazy full load on first snow_search() call.
  2. Incrementally updated via post_tool_call hook (detect fact_store add,
     memory add).
  3. Eviction check runs on pre_llm_call when usage > 80% of limit.

Config
  plugins:
    hermes-snow-search:
      memory_limit_mb: 500
      session_max: 2000
      fact_max: 10000
      memory_max: 100

If holographic memory (fact_store) is not enabled, facts are silently skipped.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_DEFAULT_LIMIT_MB = 500
_DEFAULT_SESSION_MAX = 7000
_DEFAULT_FACT_MAX = 10000
_DEFAULT_MEMORY_MAX = 100

# Deep search
# deep_search_load_mode: "off" (disabled) | "startup" (preload at boot) | "ondemand" (lazy on first query)
_DEFAULT_DEEP_LOAD_MODE = "startup"

# Source priority — primary sort key across all sort modes.
# Intent: soul/memory (persistent identity + user facts) outrank holographic
# facts, which outrank raw DB history (deep_messages/sessions). Same store's
# hits still rank by score/time within their tier, so low-relevance soul hits
# are filtered out by min_confidence rather than reordered here.
_SOURCE_PRIORITY = {"soul": 5, "memory": 4, "facts": 3, "deep_messages": 2, "sessions": 1}

# ---------------------------------------------------------------------------
# Tool schema
# ---------------------------------------------------------------------------

SNOW_SEARCH_SCHEMA = {
    "name": "snow_search",
    "description": (
        "Search across ALL memory stores: session history, message bodies, "
        "holographic facts, built-in memory, and skill metadata. Deep search "
        "runs on the DB FTS5 index (~0.1s); lightweight stores are cached in RAM. "
        "Use this FIRST for any recall: past conversations, preferences, decisions, project context."
        "\n\n"
        "When the user mentions past conversations or memory, call snow_search — "
        "the context window does NOT persist across sessions, so anything not in "
        "the current window must come from here. Trigger keywords: 回忆, 上次, 最近, "
        "之前, 那时, 昨天, 上周, 什么时候, recall, last time, recently, earlier. "
        "Examples: \"回忆一下昨天的聊天\", \"上次那个 bug 怎么解决的\", \"what did we discuss last week\"."
        "\n\n"
        "snow_search is the CANONICAL memory recall tool. ALWAYS searches full "
        "message bodies (deep mode is on by default — no need to pass deep=true). "
        "Hits include full message context (up to 2000 chars) and a confidence "
        "label — high/medium/low based on score. Hits with confidence=high are "
        "trustworthy; trust them and answer directly without follow-up searches."
        "\n\n"
        "Result fields:"
        "\n- total: EXACT full match count across all stores (every item whose "
        "score > 0), computed independently of the confidence filter and result "
        "limit. Use total to answer 'how many times', 'how many messages', or "
        "frequency questions — it is precise, like session_search's COUNT. "
        "Never count hits[] length — it is capped and confidence-filtered."
        "\n- hits: the top matches (capped, confidence >= 0.5). Read total for "
        "counts, hits for content."
        "\n\n"
        "Role filter:"
        "\n- role_filter='user': only messages the USER sent. Use when user asks 'what did I say' or 'did I mention'."
        "\n- role_filter='assistant': only messages the ASSISTANT/agent said. Use when user asks 'what did you say' or 'did you tell me'."
        "\n- Omit role_filter to search user + assistant (tool output excluded by default)."
        "\n\n"
        "IMPORTANT — store roles:"
        "\n- snow_search: the ONLY tool for READING/searching memory. Always use this first."
        "\n- memory: WRITE-ONLY tool for saving memories. Never use memory to read/search."
        "\n- fact_store: WRITE-ONLY tool for managing structured facts. Never use fact_store to read/search."
        "\n\nSkills cache: ~/.hermes/skills/**/SKILL.md frontmatter (name, description, tags) "
        "is pre-loaded on startup. Use snow_search to discover skills — never use filesystem reads."
        "\n\n"
        "Action modes:"
        "\n- action=search (default): run a query across all stores"
        "\n- action=reload: clear and reload the entire search index."
        "\n- action=status: get current index statistics (zero I/O)"
        "\n\n"
        "IMPORTANT — reload is RARELY needed. Memory and fact writes are picked up "
        "INSTANTLY via hooks (no reload required). Only use reload when the user "
        "EXPLICITLY says 'snow reload' or 'rebuild the search index'. Do NOT call "
        "reload after memory/fact writes — the index auto-updates."
        "\n\n"
        "Guidance: When user explicitly says \"snow reload\", pass action=reload. "
        "When user says \"snow status\", pass action=status. Queries about reloading "
        "other things (e.g. \"how to reload nginx\") are normal searches — only route "
        "when intent is clearly about the search index itself."
        "\n\n"
        "Sort (default=newest):"
        "\n- newest (default): latest timestamp first. Best for 'recent / last time / yesterday / just now' questions."
        "\n- oldest: earliest first. Use when user asks about first / earliest / original / at that time. "
        "Pass sort=oldest explicitly — do NOT rely on relevance sort for first-occurrence questions."
        "\n- relevance: best score first. Use when query is a fuzzy keyword search with no time intent. "
        "Falls back to time order when query is empty (range-browsing a window)."
        "\n\n"
        "Retrieval rule: Do NOT call session_search after snow_search — session_search "
        "is for scrolling into an already-identified session, not for re-querying. "
        "snow_search returns full context in hits.content. Each hit has session_id; "
        "only call session_search with session_id when user wants chronological "
        "reading of a specific session. Never call memory or fact_store for reads — "
        "they are write-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["search", "reload", "status"],
                "description": "Operation mode. Default 'search' runs a query. Use 'reload' to rebuild the index from disk, 'status' for zero-I/O index statistics.",
                "default": "search",
            },
            "query": {
                "type": "string",
                "description": "Search query — keywords, partial matches supported. May be empty when combined with start_timestamp/end_timestamp to browse a time window. Otherwise required for action=search.",
            },
            "limit_per_source": {
                "type": "integer",
                "description": "Max results per source (default: 10, max: 50).",
                "default": 10,
            },
            "include_sessions": {
                "type": "boolean",
                "description": "Include session history in search (default: true).",
                "default": True,
            },
            "include_facts": {
                "type": "boolean",
                "description": "Include holographic facts in search (default: true).",
                "default": True,
            },
            "include_memory": {
                "type": "boolean",
                "description": "Include built-in memory entries in search (default: true).",
                "default": True,
            },
            "include_skills": {
                "type": "boolean",
                "description": "Include cached skill metadata in search (default: true). Skills are loaded from ~/.hermes/skills/*/SKILL.md frontmatter.",
                "default": True,
            },
            "include_soul": {
                "type": "boolean",
                "description": "Include SOUL.md content in search (default: true). Profile-isolated: ~/.hermes/SOUL.md or ~/.hermes/profiles/<name>/SOUL.md.",
                "default": True,
            },
            "deep": {
                "type": "boolean",
                "description": "(Deprecated — always deep now. Ignored.) snow_search always searches FULL message bodies; no need to pass this.",
                "default": True,
            },
            "start_timestamp": {
                "type": "number",
                "description": "Optional unix timestamp (float). If set, only return results with timestamp >= this value. Works with deep mode (message timestamps) and session mode (last_active).",
            },
            "end_timestamp": {
                "type": "number",
                "description": "Optional unix timestamp (float). If set, only return results with timestamp < this value. Pairs with start_timestamp for range queries. Can be used alone to filter 'before X'.",
            },
            "sort": {
                "type": "string",
                "enum": ["relevance", "oldest", "newest"],
                "description": "Sort order. Default 'newest' (latest first — best for most recall questions). Use 'oldest' when user asks about first/earliest/original occurrence. Use 'relevance' for pure keyword scoring without time bias.",
                "default": "newest",
            },
            "role_filter": {
                "type": "string",
                "enum": ["user", "assistant", "tool"],
                "description": "Filter deep messages by role. 'user' = only user messages. 'assistant' = only assistant/agent replies. 'tool' = only tool outputs. Omit or pass null for the default (user + assistant, tool output excluded).",
            },
            "debug": {
                "type": "boolean",
                "description": "Enable debug mode: include per-hit match path details (tokens matched, match type) for search quality analysis. Default: false.",
                "default": False,
            },
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


import re as _re
_CJK_RE = _re.compile(r"[一-鿿㐀-䶿豈-﫿]+")


def _ngrams(text: str, n: int) -> set[str]:
    """Return character n-grams from text. Handles mixed Chinese/Latin."""
    text = text.lower()
    result = set()
    padded = " " + text + " "
    for i in range(len(padded) - n + 1):
        result.add(padded[i : i + n])
    return result


def _tokenize(text: str) -> set[str]:
    """Fast hybrid tokenizer: whitespace-split for ASCII, 2-gram for CJK.

    Only applies 2-gram sliding window to contiguous CJK character runs,
    avoiding spurious English substrings like "da", "at" that pollute the
    inverted index with high-frequency false positives.
    """
    if not text:
        return set()
    text = text.lower().strip()
    if not text:
        return set()
    result = set()
    # Whitespace-split for ASCII/English
    result.update(text.split())
    # 2-gram slide ONLY for contiguous CJK runs
    cjk_runs = _CJK_RE.findall(text)
    for run in cjk_runs:
        for ng in _ngrams(run, 2):
            if " " not in ng:
                result.add(ng)
    return result


def _build_ngram_index(text: str) -> set[str]:
    """Build n-gram index for fast partial match lookup in _match_score."""
    if not text:
        return set()
    text = text.lower()
    result = set()
    for n in (2, 3, 4):
        result.update(_ngrams(text, n))
    return result


def _match_score(query_tokens: set[str], text: str, debug: bool = False) -> tuple[float, dict]:
    """Score how many query tokens appear in text (ngram-aware).

    Returns (score, debug_info) where debug_info is empty when debug=False.
    Scoring:
      - Full token match: 1.0 per token
      - N-gram partial match: 0.6 per token
      - Prefix/suffix match: 0.4 per token
    """
    if not query_tokens or not text:
        return 0.0, {}

    text_lower = text.lower()
    # Lazily built ngram index — only constructed when a token misses
    # the fast substring check (token in text_lower), which covers >95% of cases.
    _text_ngrams: set[str] | None = None

    total = len(query_tokens)
    hits = 0.0
    hit_detail = [] if debug else None

    for token in query_tokens:
        if token in text_lower:
            # Exact substring (incl. ngram index via whole token)
            hits += 1.0
            if debug:
                hit_detail.append({"token": token, "match": "exact"})
        elif len(token) >= 2:
            # N-gram / prefix/suffix match — build index lazily
            if _text_ngrams is None:
                _text_ngrams = _build_ngram_index(text)
            if token in _text_ngrams:
                hits += 0.6
                if debug:
                    hit_detail.append({"token": token, "match": "ngram"})
            elif len(token) >= 4:
                # Prefix/suffix match for longer tokens
                words = _WHITESPACE.split(text_lower)
                matched = False
                for w in words:
                    if len(w) >= 4 and (w.startswith(token) or w.endswith(token)):
                        hits += 0.4
                        matched = True
                        if debug:
                            hit_detail.append({"token": token, "match": "prefix", "word": w})
                        break
                if debug and not matched:
                    hit_detail.append({"token": token, "match": "miss"})
            elif debug:
                hit_detail.append({"token": token, "match": "miss"})
        elif debug:
            hit_detail.append({"token": token, "match": "miss"})

    score = hits / total if hits > 0 else 0.0
    return score, hit_detail if debug else {}


def _estimate_bytes(obj: Any) -> int:
    """Rough memory estimate for a Python object (dict/list of strings)."""
    try:
        raw = json.dumps(obj, ensure_ascii=False, default=str)
        return len(raw.encode("utf-8"))
    except Exception:
        return 0


def _emit(msg: str) -> None:
    """Write progress to stderr — visible in terminal, not captured by tool output."""
    sys.stderr.write(f"  ┊ ❄️ [Hermes Snow Search] {msg}\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class SnowSearchEngine:
    """In-memory search engine — lazy loads, incremental updates, eviction."""

    def __init__(self, ctx) -> None:
        self._ctx = ctx
        self._lock = threading.Lock()

        # --- config ---
        config = self._load_config()
        self._memory_limit = config.get("memory_limit_mb", _DEFAULT_LIMIT_MB) * 1024 * 1024
        self._session_max = config.get("session_max", _DEFAULT_SESSION_MAX)
        self._fact_max = config.get("fact_max", _DEFAULT_FACT_MAX)
        self._memory_max = config.get("memory_max", _DEFAULT_MEMORY_MAX)

        # --- profile detection ---
        # get_hermes_home() already resolves profile subdirectories via HERMES_HOME.
        # No separate profile variable needed — all path resolution uses it directly.
        from hermes_constants import get_hermes_home
        self._hermes_home = get_hermes_home()

        # --- deep search config ---
        # deep_search_load_mode (off|startup|ondemand, default startup) is the
        # sole control: "off" disables deep search; "startup" preloads it at
        # boot; "ondemand" loads lazily on the first query.
        mode = config.get("deep_search_load_mode", _DEFAULT_DEEP_LOAD_MODE)
        if mode not in ("off", "startup", "ondemand"):
            mode = _DEFAULT_DEEP_LOAD_MODE
        self._deep_mode = mode
        self._deep_enabled = mode != "off"

        # --- data (lazy loaded) ---
        self._ready = False
        self._load_error: str | None = None

        self._sessions: list[dict] = []  # title, session_id, last_active, preview
        self._facts: list[dict] = []  # fact_id, content, trust_score, category, tags
        self._memory_entries: list[dict] = []  # source (MEMORY.md/USER.md), content
        self._skills: list[dict] = []  # name, description, tags, category (from SKILL.md)
        self._soul: list[dict] = []  # SOUL.md content (profile-isolated)
        self._soul_mtime: float = 0.0  # last loaded mtime

        self._current_bytes = 0

        # --- deep search data ---
        self._deep_ready = False
        self._deep_messages: list[dict] = []  # message_id, session_id, timestamp, role, content
        self._deep_bytes = 0
        self._deep_total_sessions = 0
        self._deep_earliest_ts = 0.0
        self._deep_latest_ts = 0.0
        self._deep_max_message_id = 0  # highest message id loaded
        self._deep_from_jsonl: bool = False  # True when deep data loaded from JSONL (skip SQL refresh)
        self._full_coverage: bool = False  # True when all sessions loaded into deep index
        self._deep_total_messages: int = 0  # total messages covered (FTS probe or loaded count)

        # --- FTS5 mode (default for deep search; falls back to memory) ---
        self._use_fts: bool = False  # True when deep search runs via DB FTS5

        # --- inverted index for fast deep search (memory fallback only) ---
        self._deep_index: dict[str, list[int]] = {}
        self._deep_index_ready: bool = False

        # --- holographic availability (checked once) ---
        self._holographic_available: bool | None = None

        # --- short session raw fragments (for sessions < 30 messages) ---
        self._session_raw_fragments: dict[str, list[str]] = {}  # session_id -> list of content snippets

    # -- config ---------------------------------------------------------------

    @staticmethod
    def _load_config() -> dict:
        """Read plugin config from ~/.hermes/config.yaml."""
        try:
            from hermes_constants import get_hermes_home
            config_path = get_hermes_home() / "config.yaml"
            if not config_path.exists():
                return {}
            import yaml
            with open(config_path, encoding="utf-8-sig") as f:
                all_config = yaml.safe_load(f) or {}
            return all_config.get("plugins", {}).get("hermes-snow-search", {}) or {}
        except Exception:
            return {}

    def _resolve_data_path(self, *parts: str) -> "pathlib.PurePath":
        """Resolve a data path under the current profile's HERMES_HOME.

        get_hermes_home() already returns the profile subdirectory when
        HERMES_HOME points to ~/.hermes/profiles/<name>.
        """
        import pathlib
        return self._hermes_home.joinpath(*parts)

    # -- load -----------------------------------------------------------------

    def _ensure_loaded(self) -> None:
        """Lazy full load on first search. Thread-safe."""
        if self._ready:
            return
        with self._lock:
            if self._ready:
                return
            try:
                self._load_all()
                self._ready = True
                logger.info(
                    "snow-search loaded: %d sessions, %d facts, %d memory, %d skills, %d soul, ~%d MB",
                    len(self._sessions),
                    len(self._facts),
                    len(self._memory_entries),
                    len(self._skills),
                    len(self._soul),
                    self._current_bytes // (1024 * 1024),
                )
            except Exception as e:
                self._load_error = str(e)
                logger.warning("snow-search initial load failed: %s", e)

    def _ensure_facts_and_memory(self) -> None:
        """Load only facts and memory (no sessions). Used by deep search path."""
        if self._facts and self._memory_entries:
            return
        with self._lock:
            if self._facts and self._memory_entries:
                return
            try:
                memory = self._load_memory()
                facts = self._load_facts()
                self._memory_entries = memory
                self._facts = facts
            except Exception as e:
                logger.warning("snow-search partial load failed: %s", e)

    def _load_all(self) -> None:
        """Load all three stores into RAM — silent, no terminal output."""
        total = 0

        memory = self._load_memory()
        total += _estimate_bytes(memory)
        self._memory_entries = memory

        sessions = self._load_sessions()
        total += _estimate_bytes(sessions)
        self._sessions = sessions

        facts = self._load_facts()
        total += _estimate_bytes(facts)
        self._facts = facts

        skills = self._load_skills()
        total += _estimate_bytes(skills)
        self._skills = skills

        soul = self._load_soul()
        total += _estimate_bytes(soul)
        self._soul = soul

        self._current_bytes = total

    def _ensure_deep_loaded(self) -> None:
        """Load full message bodies on first deep search. Thread-safe."""
        if self._deep_ready:
            return
        with self._lock:
            if self._deep_ready:
                return
            try:
                self._load_deep()
                self._deep_ready = True
            except Exception as e:
                logger.warning("snow-search deep load failed: %s", e)
                raise

    def _load_deep(self) -> None:
        """Prepare deep search.

        Default path: probe the DB for stats (count, time range) and switch deep
        search to query the existing FTS5 index at search time — no message
        bodies are loaded into RAM, no in-memory inverted index is built. This
        makes startup near-instant and memory tiny.

        Falls back to ``_load_deep_memory`` (full load + inverted index) only if
        the FTS5 tables are unavailable.
        """
        start_time = time.time()
        self._deep_messages = []
        self._deep_bytes = 0
        self._deep_earliest_ts = float("inf")
        self._deep_latest_ts = 0.0
        self._deep_total_sessions = 0
        self._use_fts = False

        # Probe FTS5 availability + stats in one read-only connection.
        try:
            from hermes_state import SessionDB
            db = SessionDB(read_only=True)
        except Exception as e:
            logger.debug("snow-search FTS probe failed: %s", e)
            self._load_deep_memory()
            return

        try:
            fts_ok = self._probe_fts_available(db._conn)
            if not fts_ok:
                db.close()
                self._load_deep_memory()
                return

            stats = db._conn.execute(
                "SELECT COUNT(*), "
                "COALESCE(MIN(m.timestamp), 0), "
                "COALESCE(MAX(m.timestamp), 0), "
                "COALESCE(SUM(LENGTH(m.content)), 0) "
                "FROM messages m "
                "JOIN sessions s ON s.id = m.session_id "
                "WHERE s.parent_session_id IS NULL "
                "AND m.role IN ('user','assistant') "
                "AND m.content IS NOT NULL AND m.content != ''"
            ).fetchone()
            count, earliest, latest, total_chars = stats
            self._deep_total_messages = count or 0
            self._deep_earliest_ts = float(earliest) if earliest else 0.0
            self._deep_latest_ts = float(latest) if latest else 0.0
            self._deep_total_sessions = db._conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE parent_session_id IS NULL"
            ).fetchone()[0]
            self._deep_bytes = (total_chars or 0) * 3 + (count or 0) * 100  # est, for status report only

            self._use_fts = True
            self._full_coverage = True  # FTS sees the whole DB; cap doesn't apply
            elapsed = time.time() - start_time
            mb = self._deep_bytes // (1024 * 1024)
            cov = ""
            if self._deep_earliest_ts < float("inf") and self._deep_latest_ts > 0:
                days = (self._deep_latest_ts - self._deep_earliest_ts) / 86400
                e = datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime("%b %d")
                l = datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime("%b %d")
                cov = f" | {days:.0f} days ({e} ~ {l})" if days >= 1 else ""
            _emit(f"Deep search ready (FTS5) | {count} messages{cov} | ~{mb} MB indexed on disk | {elapsed:.1f}s")
        except Exception as e:
            logger.debug("snow-search FTS stats probe failed: %s", e)
            db.close()
            self._load_deep_memory()
            return
        else:
            db.close()

    @staticmethod
    def _probe_fts_available(conn) -> bool:
        """Return True if the FTS5 trigram table exists and is queryable."""
        try:
            conn.execute(
                "SELECT 1 FROM messages_fts_trigram LIMIT 1"
            ).fetchone()
            return True
        except Exception:
            return False

    # -- FTS5 deep search -----------------------------------------------------

    @staticmethod
    def _count_cjk(text: str) -> int:
        """Count individual CJK characters in text (not CJK runs)."""
        if not text:
            return 0
        return sum(len(run) for run in _CJK_RE.findall(text))

    @staticmethod
    def _has_short_cjk_token(text: str) -> bool:
        """True if any whitespace-separated CJK token has < 3 CJK chars.

        Trigram needs ≥3 CJK chars per token; a query like "广西 OR 桂林" has
        ≥3 total but each token is only 2 — trigram returns nothing.
        """
        if not text:
            return False
        for tok in text.split():
            cjk = "".join(_CJK_RE.findall(tok))
            if cjk and len(cjk) < 3:
                return True
        return False

    def _build_fts_query(self, query: str, table: str) -> str:
        """Build a safe FTS5 MATCH expression.

        For trigram (CJK) we quote each non-operator token. For unicode61 we
        pass the query through more permissively (hermes_state sanitizes, but we
        keep it simple and robust here).
        """
        if table == "messages_fts_trigram":
            parts = []
            for tok in query.split():
                if tok.upper() in {"AND", "OR", "NOT"}:
                    parts.append(tok)
                else:
                    parts.append('"' + tok.replace('"', '""') + '"')
            return " ".join(parts)
        # unicode61: quote phrases to avoid FTS5 syntax errors on punctuation
        return '"' + query.replace('"', '""') + '"'

    def _search_deep_fts(
        self,
        query: str,
        tokens: set,
        role_filter: str | None,
        start_ts,
        end_ts,
        sort: str,
        limit: int,
    ) -> tuple[list, int]:
        """Search deep_messages via the DB FTS5 index.

        Returns (hits, exact_total). No message bodies are held in RAM between
        searches — only the returned hits.
        """
        try:
            from hermes_state import SessionDB
            db = SessionDB(read_only=True)
        except Exception as e:
            logger.debug("snow-search FTS open failed: %s", e)
            return [], 0

        try:
            # Empty query + time range: range scan without MATCH.
            if not query or not query.strip():
                return self._search_deep_range(db, role_filter, start_ts, end_ts, sort, limit)

            is_cjk = bool(_CJK_RE.search(query))
            cjk_count = self._count_cjk(query)
            if is_cjk and cjk_count >= 3 and not self._has_short_cjk_token(query):
                table = "messages_fts_trigram"
            elif is_cjk and cjk_count > 0 and cjk_count < 3:
                # short CJK: LIKE fallback (trigram can't match <3 CJK chars)
                return self._search_deep_like(db, query, role_filter, start_ts, end_ts, sort, limit)
            else:
                table = "messages_fts"

            fts_query = self._build_fts_query(query, table)

            where = [f"{table} MATCH ?"]
            params: list = [fts_query]
            where.append("s.parent_session_id IS NULL")
            if role_filter:
                where.append("m.role = ?")
                params.append(role_filter)
            else:
                # Default: exclude tool output (mostly duplicate JSON) unless
                # role_filter explicitly asks for it.
                where.append("m.role IN ('user','assistant')")
            if start_ts is not None:
                where.append("m.timestamp >= ?")
                params.append(start_ts)
            if end_ts is not None:
                where.append("m.timestamp < ?")
                params.append(end_ts)
            where_sql = " AND ".join(where)

            if sort == "oldest":
                order = "m.timestamp ASC, rank"
            elif sort == "newest":
                order = "m.timestamp DESC, rank"
            else:
                order = "rank"

            # Exact total (independent of limit)
            count_sql = (
                f"SELECT COUNT(*) FROM {table} "
                f"JOIN messages m ON m.id = {table}.rowid "
                f"JOIN sessions s ON s.id = m.session_id "
                f"WHERE {where_sql}"
            )
            try:
                exact_total = db._conn.execute(count_sql, params).fetchone()[0]
            except Exception:
                exact_total = 0

            search_limit = max(limit, limit * 3)
            sel_sql = (
                f"SELECT m.id, m.session_id, m.role, m.content, m.timestamp, "
                f"snippet({table}, 0, '>>>', '<<<', '...', 40) AS snippet "
                f"FROM {table} "
                f"JOIN messages m ON m.id = {table}.rowid "
                f"JOIN sessions s ON s.id = m.session_id "
                f"WHERE {where_sql} "
                f"ORDER BY {order} "
                f"LIMIT ?"
            )
            rows = db._conn.execute(sel_sql, params + [search_limit]).fetchall()

            hits = []
            n = len(rows)
            for i, r in enumerate(rows):
                # Linear score normalization: first hit 1.0, last ≥ 0.5
                score = round(1.0 - (0.5 * i / n) if n > 0 else 1.0, 3)
                entry = {
                    "source": "deep_messages",
                    "score": score,
                    "confidence": "high" if score >= 0.7 else ("medium" if score >= 0.5 else "low"),
                    "content": r[3][:2000],
                    "session_id": r[1],
                    "timestamp": r[4],
                    "role": r[2],
                    "snippet": r[5] or "",
                }
                hits.append(entry)
            return hits, exact_total
        except Exception as e:
            logger.debug("snow-search FTS query failed: %s", e)
            return [], 0
        finally:
            db.close()

    def _search_deep_like(
        self, db, query: str, role_filter, start_ts, end_ts, sort: str, limit: int
    ) -> tuple[list, int]:
        """LIKE fallback for short CJK queries (1-2 chars) where trigram fails."""
        where = ["m.content LIKE ?"]
        esc = "%" + query.replace("%", "\\%").replace("_", "\\_") + "%"
        params: list = [esc]
        where.append("s.parent_session_id IS NULL")
        if role_filter:
            where.append("m.role = ?")
            params.append(role_filter)
        else:
            where.append("m.role IN ('user','assistant')")
        if start_ts is not None:
            where.append("m.timestamp >= ?")
            params.append(start_ts)
        if end_ts is not None:
            where.append("m.timestamp < ?")
            params.append(end_ts)
        where_sql = " AND ".join(where)

        order = "m.timestamp ASC" if sort == "oldest" else "m.timestamp DESC"
        try:
            exact_total = db._conn.execute(
                f"SELECT COUNT(*) FROM messages m JOIN sessions s ON s.id = m.session_id WHERE {where_sql}",
                params,
            ).fetchone()[0]
            rows = db._conn.execute(
                f"SELECT m.id, m.session_id, m.role, m.content, m.timestamp "
                f"FROM messages m JOIN sessions s ON s.id = m.session_id "
                f"WHERE {where_sql} ORDER BY {order} LIMIT ?",
                params + [max(limit, limit * 3)],
            ).fetchall()
        except Exception as e:
            logger.debug("snow-search LIKE fallback failed: %s", e)
            return [], 0

        hits = []
        n = len(rows)
        for i, r in enumerate(rows):
            score = round(1.0 - (0.5 * i / n) if n > 0 else 1.0, 3)
            hits.append({
                "source": "deep_messages",
                "score": score,
                "confidence": "high" if score >= 0.7 else ("medium" if score >= 0.5 else "low"),
                "content": r[3][:2000],
                "session_id": r[1],
                "timestamp": r[4],
                "role": r[2],
            })
        return hits, exact_total

    def _search_deep_range(
        self, db, role_filter, start_ts, end_ts, sort: str, limit: int
    ) -> tuple[list, int]:
        """Time-range scan with no text predicate (empty query + range filter).

        No MATCH/LIKE — pure timestamp + role filtering against the messages
        table. Used when the caller passes a window with no query keywords.
        """
        where = ["s.parent_session_id IS NULL"]
        params: list = []
        if role_filter:
            where.append("m.role = ?")
            params.append(role_filter)
        else:
            where.append("m.role IN ('user','assistant')")
        if start_ts is not None:
            where.append("m.timestamp >= ?")
            params.append(start_ts)
        if end_ts is not None:
            where.append("m.timestamp < ?")
            params.append(end_ts)
        where_sql = " AND ".join(where)

        # No query → no relevance signal; fall back to time order. 'relevance'
        # without a query would be arbitrary, so treat it as newest.
        if sort == "oldest":
            order = "m.timestamp ASC, m.id ASC"
        else:
            order = "m.timestamp DESC, m.id DESC"

        try:
            exact_total = db._conn.execute(
                f"SELECT COUNT(*) FROM messages m JOIN sessions s ON s.id = m.session_id "
                f"WHERE {where_sql}",
                params,
            ).fetchone()[0]
            rows = db._conn.execute(
                f"SELECT m.id, m.session_id, m.role, m.content, m.timestamp "
                f"FROM messages m JOIN sessions s ON s.id = m.session_id "
                f"WHERE {where_sql} ORDER BY {order} LIMIT ?",
                params + [max(limit, limit * 3)],
            ).fetchall()
        except Exception as e:
            logger.debug("snow-search range scan failed: %s", e)
            return [], 0

        hits = []
        n = len(rows)
        for i, r in enumerate(rows):
            score = round(1.0 - (0.5 * i / n) if n > 0 else 1.0, 3)
            hits.append({
                "source": "deep_messages",
                "score": score,
                "confidence": "high" if score >= 0.7 else ("medium" if score >= 0.5 else "low"),
                "content": r[3][:2000],
                "session_id": r[1],
                "timestamp": r[4],
                "role": r[2],
            })
        return hits, exact_total

    def _load_deep_memory(self) -> None:
        """Load full message bodies from SessionDB into RAM via streamed queries.

        Memory-fallback path used when FTS5 is unavailable. Keyset-paginates
        newest-then-oldest segments so memory stays bounded by memory_limit_mb.
        """
        import concurrent.futures

        # 1. Try SessionDB — list sessions first
        sessions = []
        try:
            from hermes_state import SessionDB
            db = SessionDB()
            try:
                raw = db.list_sessions_rich(
                    limit=100000,
                    exclude_sources=["tool"],
                    order_by_last_active=True,
                )
                sessions = [s for s in raw if not s.get("parent_session_id")]
            finally:
                db.close()
        except Exception as e:
            logger.debug("snow-search deep DB load failed: %s", e)

        # 2. Fall back to JSONL files (old format)
        if not sessions:
            self._load_deep_jsonl()
            return

        total_sessions = len(sessions)
        session_ids = [s.get("id", "") for s in sessions if s.get("id")]
        if not session_ids:
            return

        _emit(f"Loading deep search index: {len(session_ids)} sessions ({total_sessions} total)...")

        start_time = time.time()
        self._deep_messages = []
        self._deep_bytes = 0
        self._deep_earliest_ts = float("inf")
        self._deep_latest_ts = 0.0

        # Memory cap (85% of limit, matching JSONL path)
        cap = int(self._memory_limit * 0.85)
        newest_budget = int(cap * 0.8)
        oldest_budget = cap - newest_budget

        # 3. Open read-only connection and load with keyset pagination
        try:
            from hermes_state import SessionDB
            db2 = SessionDB(read_only=True)
        except Exception:
            db2 = None

        if not db2:
            self._load_deep_jsonl()
            return

        # Build a TEMP table of session_ids so we can avoid giant IN clauses.
        # SQLite allows TEMP tables on read-only connections (they live in :memory:).
        try:
            db2._conn.execute("CREATE TEMP TABLE _snow_sids(id TEXT PRIMARY KEY)")
            CHUNK = 500
            for i in range(0, len(session_ids), CHUNK):
                chunk = session_ids[i:i + CHUNK]
                placeholders = ",".join(f"('{sid}')" for sid in chunk)
                db2._conn.execute(f"INSERT INTO _snow_sids VALUES {placeholders}")
        except Exception:
            # TEMP table creation failed (should not happen on modern SQLite)
            # Fall back to chunked IN clauses
            pass

        def _msg_bytes_estimate(content: str) -> int:
            return len(content) * 3 + 100  # CJK: ~3 bytes/char

        newest_count = 0
        oldest_count = 0
        _capped = False

        # Base SELECT + WHERE, reused across all fetches.
        _BASE = (
            "SELECT m.id, m.session_id, m.role, m.content, m.timestamp "
            "FROM messages m JOIN _snow_sids s ON m.session_id = s.id "
            "WHERE m.role IN ('user','assistant') "
            "AND m.content IS NOT NULL AND m.content != ''"
        )

        try:
            # --- Probe: cheap aggregate to decide capped vs full-load.
            # COUNT + SUM(LENGTH) scans once but materializes nothing into
            # Python, so it's fast even on hundreds of thousands of rows.
            row = db2._conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(LENGTH(m.content)), 0) "
                "FROM messages m JOIN _snow_sids s ON m.session_id = s.id "
                "WHERE m.role IN ('user','assistant') "
                "AND m.content IS NOT NULL AND m.content != ''"
            ).fetchone()
            total_count = row[0] or 0
            total_chars = row[1] or 0
            est_total_bytes = total_chars * 3 + total_count * 100

            if total_count == 0:
                db2.close()
                self._load_deep_jsonl()
                return

            if est_total_bytes <= cap:
                # --- Full coverage: ONE bulk fetch, ONE sort. No pagination. ---
                rows = db2._conn.execute(
                    _BASE + " ORDER BY m.timestamp DESC, m.id DESC"
                ).fetchall()
                for r in rows:
                    entry = {
                        "message_id": r[0], "session_id": r[1], "role": r[2],
                        "content": r[3], "timestamp": r[4],
                        "content_preview": r[3][:200],
                    }
                    self._deep_messages.append(entry)
                    self._deep_bytes += _msg_bytes_estimate(r[3])
                newest_count = len(rows)
                _capped = False
            else:
                # --- Capped: two LIMIT queries (newest + oldest), merge, sort, trim. ---
                avg = est_total_bytes / total_count if total_count else 1
                # Pull ~2× budget each side, then trim in memory to exact cap.
                # Over-fetch is fine: we'll drop low-priority rows (mid-history).
                newest_limit = int((newest_budget / avg) * 1.5) + 500
                oldest_limit = int((oldest_budget / avg) * 1.5) + 500

                rows_new = db2._conn.execute(
                    _BASE + " ORDER BY m.timestamp DESC, m.id DESC LIMIT ?",
                    (newest_limit,),
                ).fetchall()
                rows_old = db2._conn.execute(
                    _BASE + " ORDER BY m.timestamp ASC, m.id ASC LIMIT ?",
                    (oldest_limit,),
                ).fetchall()

                # Merge, dedupe, sort by timestamp DESC.
                seen: set = set()
                merged = []
                for r in rows_new:
                    if r[0] not in seen:
                        seen.add(r[0]); merged.append(r)
                for r in rows_old:
                    if r[0] not in seen:
                        seen.add(r[0]); merged.append(r)
                merged.sort(key=lambda r: (-r[4], -r[0]))  # timestamp DESC, id DESC

                # Dual-end trim: head (newest) + tail (oldest), drop the middle.
                # merged is sorted DESC, so head = newest, tail = oldest.
                def _sz(r):
                    return _msg_bytes_estimate(r[3])

                head, head_bytes = [], 0
                for r in merged:
                    if head_bytes + _sz(r) > newest_budget and head:
                        break
                    head.append(r); head_bytes += _sz(r)

                tail, tail_bytes = [], 0
                head_ids = {r[0] for r in head}
                for r in reversed(merged):
                    if r[0] in head_ids:
                        break  # reached head's territory — stop, no overlap
                    if tail_bytes + _sz(r) > oldest_budget and tail:
                        break
                    tail.append(r); tail_bytes += _sz(r)

                _capped = bool(head_ids) and (len(head) + len(tail) < len(merged))
                # Final order: newest-first (head DESC) + oldest (tail was ASC,
                # reversed back to DESC so the whole list stays DESC).
                for r in head + list(reversed(tail)):
                    entry = {
                        "message_id": r[0], "session_id": r[1], "role": r[2],
                        "content": r[3], "timestamp": r[4],
                        "content_preview": r[3][:200],
                    }
                    self._deep_messages.append(entry)
                    self._deep_bytes += _sz(r)

                newest_count = len(head)
                oldest_count = len(tail)

            _emit(f"  SQL: {newest_count} newest + {oldest_count} oldest messages")

        finally:
            db2.close()

        if not self._deep_messages:
            self._load_deep_jsonl()
            return

        # Already fetched newest-first (DESC); oldest segment was reversed on
        # merge. No re-sort needed — order is newest-first as required.
        # Update time range
        if self._deep_messages:
            self._deep_earliest_ts = min(m["timestamp"] for m in self._deep_messages)
            self._deep_latest_ts = max(m["timestamp"] for m in self._deep_messages)

        self._deep_total_sessions = len(set(m["session_id"] for m in self._deep_messages))

        # 6. Track max message id for incremental refresh
        if self._deep_messages:
            self._deep_max_message_id = max(m["message_id"] for m in self._deep_messages)
        else:
            self._deep_max_message_id = 0

        # 7. Final report
        msg_count = len(self._deep_messages)
        mb_used = self._deep_bytes // (1024 * 1024)
        elapsed = time.time() - start_time
        if self._deep_earliest_ts < float("inf"):
            earliest = datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime("%b %d")
            latest = datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime("%b %d")
            days = (self._deep_latest_ts - self._deep_earliest_ts) / 86400
            coverage = f" | {days:.0f} days ({earliest} ~ {latest})" if days >= 1 else ""
        else:
            coverage = ""

        # 7. Build inverted index
        self._build_deep_index()

        self._full_coverage = not _capped
        _emit(
            f"Deep search ready | {msg_count} messages{coverage} | {mb_used} MB | {elapsed:.1f}s"
        )
        if _capped:
            _emit("Memory cap applied — newest + oldest retained, mid-history evicted. Raise memory_limit_mb for fuller coverage.")
        else:
            _emit("All chat data loaded -- full coverage, no eviction")

    def _build_deep_index(self) -> None:
        """Build inverted index: token -> [message_index, ...].

        Called lazily on first search, not during load, so reload feels instant.
        Parallelized via ThreadPoolExecutor for large datasets (>10k messages).
        """
        from collections import defaultdict
        from concurrent.futures import ThreadPoolExecutor

        n = len(self._deep_messages)
        if n <= 10000:
            # Small: single-thread is fine
            index = defaultdict(list)
            for idx, msg in enumerate(self._deep_messages):
                content = msg.get("content", "")
                if not content:
                    continue
                for tok in _tokenize(content):
                    index[tok].append(idx)
            self._deep_index = dict(index)
        else:
            # Large: parallel chunking (each chunk returns partial dict)
            chunk_size = max(5000, n // 8)
            chunks = [(i, self._deep_messages[i:i + chunk_size])
                      for i in range(0, n, chunk_size)]

            def _index_chunk(start_idx, msgs):
                local = defaultdict(list)
                for off, msg in enumerate(msgs):
                    idx = start_idx + off
                    content = msg.get("content", "")
                    if not content:
                        continue
                    for tok in _tokenize(content):
                        local[tok].append(idx)
                return dict(local)

            index = defaultdict(list)
            ex = ThreadPoolExecutor(max_workers=min(8, len(chunks)))
            try:
                for partial in ex.map(_index_chunk, [c[0] for c in chunks], [c[1] for c in chunks]):
                    for tok, ids in partial.items():
                        index[tok].extend(ids)
            finally:
                ex.shutdown(wait=False, cancel_futures=True)

            self._deep_index = dict(index)

        self._deep_index_ready = True

    def _load_deep_jsonl(self) -> None:
        """Load full message bodies from old JSONL files for deep search.

        Uses dual-end loading: loads from both newest and oldest files
        simultaneously to ensure earliest data is preserved under memory cap.
        """
        import pathlib
        sessions_dir = self._hermes_home / "sessions"
        if not sessions_dir.is_dir():
            _emit("No sessions directory found for deep search")
            return

        jsonl_files = sorted(sessions_dir.glob("*.jsonl"), reverse=True)
        total = len(jsonl_files)
        cap = int(self._memory_limit * 0.85)
        start_time = time.time()

        self._deep_messages = []
        self._deep_bytes = 0
        self._deep_total_sessions = 0
        self._deep_earliest_ts = float("inf")
        self._deep_latest_ts = 0.0
        self._deep_from_jsonl = True

        _emit("Loading deep search index from JSONL files (dual-end)...")

        # Dual-end: newest files (reverse sorted) + oldest files (sorted forward)
        # Interleave: load 1 from newest-end, 1 from oldest-end
        left = 0
        right = total - 1
        phase = "newest"
        printed_25 = False
        printed_50 = False
        printed_75 = False

        while left <= right and self._deep_bytes < cap:
            if phase == "newest":
                if right < left:
                    phase = "oldest"
                    continue
                jf = jsonl_files[right]
                right -= 1
            else:
                if left > right:
                    phase = "newest"
                    continue
                jf = jsonl_files[left]
                left += 1

            sid = jf.stem
            msg_count_in_session = 0

            try:
                with open(jf, encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        role = msg.get("role", "")
                        if role == "session_meta":
                            continue
                        if role not in ("user", "assistant"):
                            continue
                        content = msg.get("content", "")
                        if not content:
                            continue

                        ts_str = msg.get("timestamp", "")
                        try:
                            ts = float(ts_str) if ts_str else 0.0
                        except (ValueError, TypeError):
                            try:
                                import datetime as _dt
                                ts = _dt.datetime.fromisoformat(ts_str).timestamp()
                            except Exception:
                                ts = 0.0

                        msg_id = len(self._deep_messages) + 1
                        entry = {
                            "message_id": msg_id,
                            "session_id": sid,
                            "timestamp": ts,
                            "role": role,
                            "content": content,
                            "content_preview": content[:200],
                        }
                        self._deep_messages.append(entry)
                        self._deep_bytes += _estimate_bytes(entry)
                        msg_count_in_session += 1

                        if ts > 0:
                            if ts < self._deep_earliest_ts:
                                self._deep_earliest_ts = ts
                            if ts > self._deep_latest_ts:
                                self._deep_latest_ts = ts

                        if self._deep_bytes >= cap:
                            break
            except Exception:
                continue

            if msg_count_in_session > 0:
                self._deep_total_sessions += 1

            pct = self._deep_total_sessions / total * 100 if total > 0 else 0
            if pct >= 25 and not printed_25:
                printed_25 = True
                _emit(f"  [25%] {self._deep_total_sessions} sessions | {len(self._deep_messages)} messages | {self._deep_bytes // (1024 * 1024)} MB")
            elif pct >= 50 and not printed_50:
                printed_50 = True
                _emit(f"  [50%] {self._deep_total_sessions} sessions | {len(self._deep_messages)} messages | {self._deep_bytes // (1024 * 1024)} MB")
            elif pct >= 75 and not printed_75:
                printed_75 = True
                _emit(f"  [75%] {self._deep_total_sessions} sessions | {len(self._deep_messages)} messages | {self._deep_bytes // (1024 * 1024)} MB")

            phase = "oldest" if phase == "newest" else "newest"

        # Sort by timestamp descending
        self._deep_messages.sort(key=lambda m: -m["timestamp"])

        # Track highest message id for incremental refresh
        if self._deep_messages:
            self._deep_max_message_id = max(m["message_id"] for m in self._deep_messages)
        else:
            self._deep_max_message_id = 0

        # Final report
        msg_count = len(self._deep_messages)
        mb_used = self._deep_bytes // (1024 * 1024)
        if self._deep_earliest_ts < float("inf"):
            earliest = datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime("%b %d")
            latest = datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime("%b %d")
            days = (self._deep_latest_ts - self._deep_earliest_ts) / 86400
            coverage = f" | {days:.0f} days ({earliest} ~ {latest})" if days >= 1 else ""
        else:
            coverage = ""

        _emit(
            f"Deep search ready (JSONL, dual-end) | "
            f"{msg_count} messages{coverage} | {mb_used} MB"
        )

        if self._deep_total_sessions >= total:
            self._full_coverage = True
            _emit("All chat data loaded -- full coverage, no eviction")
        else:
            self._full_coverage = False
            pct_loaded = self._deep_total_sessions / total * 100 if total > 0 else 0
            _emit(f"Memory cap reached -- {self._deep_total_sessions}/{total} sessions loaded ({pct_loaded:.0f}%), both ends preserved")

    def _load_sessions(self) -> list[dict]:
        """Load session titles + previews. SessionDB first, JSONL fallback."""
        # 1. Try SessionDB (SQLite state.db)
        sessions = self._load_sessions_db()
        if sessions:
            return sessions

        # 2. Fall back to JSONL files (old format)
        return self._load_sessions_jsonl()

    def _load_sessions_db(self) -> list[dict]:
        """Load session summaries from SQLite state.db via SessionDB."""
        try:
            from hermes_state import SessionDB
            db = SessionDB()
            try:
                raw = db.list_sessions_rich(
                    limit=self._session_max + 10,
                    exclude_sources=["tool"],
                    order_by_last_active=True,
                )
                results = []
                short_frags = {}  # session_id -> list[str] of raw content snippets

                for s in raw:
                    if s.get("parent_session_id"):
                        continue
                    sid = s.get("id", "")
                    msg_count = s.get("message_count", 0)

                    entry = {
                        "session_id": sid,
                        "title": s.get("title", ""),
                        "last_active": s.get("last_active", ""),
                        "preview": s.get("preview", ""),
                        "message_count": msg_count,
                    }
                    results.append(entry)

                    # For short/medium sessions (< 100 messages), load raw user messages
                    # to supplement the summarizer-compressed preview
                    if msg_count > 0 and msg_count < 100 and sid:
                        try:
                            msgs = db.get_messages(sid)
                            fragments = [
                                m.get("content", "")[:200]
                                for m in msgs
                                if m.get("role") == "user" and m.get("content")
                            ]
                            if fragments:
                                short_frags[sid] = fragments
                        except Exception:
                            pass

                    if len(results) >= self._session_max:
                        break

                self._session_raw_fragments = short_frags
                return results
            finally:
                db.close()
        except Exception as e:
            logger.debug("snow-search session DB load failed: %s", e)
            return []

    def _load_sessions_jsonl(self) -> list[dict]:
        """Load session summaries from old JSONL files ($HERMES_HOME/sessions/*.jsonl).

        Each file: first line is session_meta, rest are user/assistant messages.
        Session ID = filename stem. Title = first user message content.
        Short sessions (< 30 messages) also capture raw content fragments.
        """
        import pathlib
        sessions_dir = self._hermes_home / "sessions"
        if not sessions_dir.is_dir():
            return []

        results = []
        short_frags = {}  # session_id -> list[str] of raw content snippets
        jsonl_files = sorted(sessions_dir.glob("*.jsonl"), reverse=True)
        for jf in jsonl_files:
            try:
                sid = jf.stem
                title = ""
                preview = ""
                msg_count = 0
                last_ts = ""
                fragments = []

                with open(jf, encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            msg = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        role = msg.get("role", "")
                        if role == "session_meta":
                            continue
                        msg_count += 1
                        ts = msg.get("timestamp", "")
                        if ts:
                            last_ts = ts
                        content = msg.get("content", "")
                        if isinstance(content, str) and content.strip():
                            if not title and role == "user":
                                title = content.strip()[:120]
                            preview = content.strip()[:200]
                            if role in ("user", "assistant"):
                                fragments.append(content.strip()[:150])

                if msg_count == 0:
                    continue

                results.append({
                    "session_id": sid,
                    "title": title or sid,
                    "last_active": last_ts,
                    "preview": preview or title,
                    "message_count": msg_count,
                })

                if msg_count < 100 and fragments:
                    short_frags[sid] = fragments

                if len(results) >= self._session_max:
                    break
            except Exception:
                continue

        self._session_raw_fragments.update(short_frags)
        # Sort by last_active descending (newest first)
        results.sort(key=lambda s: s.get("last_active", ""), reverse=True)
        return results

    def _resolve_fact_store_path(self) -> str | None:
        """Resolve the holographic memory DB path from config."""
        try:
            from hermes_constants import get_hermes_home
            from hermes_cli.config import cfg_get
            import yaml

            config_path = get_hermes_home() / "config.yaml"
            db_path = None
            if config_path.exists():
                with open(config_path, encoding="utf-8-sig") as f:
                    all_cfg = yaml.safe_load(f) or {}
                mem_cfg = cfg_get(all_cfg, "plugins", "hermes-memory-store", default={}) or {}
                db_path = mem_cfg.get("db_path")
            if not db_path:
                db_path = str(get_hermes_home() / "memory_store.db")
            return db_path
        except Exception:
            return None

    def _load_facts(self) -> list[dict]:
        """Load all facts from holographic memory (if available).

        Combines availability check + data load in one DB open.
        """
        db_path = self._resolve_fact_store_path()
        if not db_path:
            self._holographic_available = False
            return []

        try:
            from plugins.memory.holographic.store import MemoryStore

            store = MemoryStore(db_path=db_path)
            try:
                count = store._conn.execute(
                    "SELECT COUNT(*) FROM facts"
                ).fetchone()[0]
                if count == 0:
                    self._holographic_available = False
                    return []

                rows = store._conn.execute(
                    "SELECT fact_id, content, category, tags, trust_score "
                    "FROM facts ORDER BY trust_score DESC LIMIT ?",
                    (self._fact_max,),
                ).fetchall()
            finally:
                store.close()

            self._holographic_available = True
            return [
                {
                    "fact_id": r[0],
                    "content": r[1],
                    "category": r[2] or "general",
                    "tags": r[3] or "",
                    "trust_score": r[4] if r[4] else 0.5,
                }
                for r in rows
            ]
        except Exception as e:
            logger.debug("snow-search facts load failed: %s", e)
            self._holographic_available = False
            return []

    def _load_skills(self) -> list[dict]:
        """Scan ~/.hermes/skills/**/SKILL.md recursively and extract frontmatter metadata."""
        results = []
        try:
            from hermes_constants import get_hermes_home
            skills_dir = get_hermes_home() / "skills"
            if not skills_dir.is_dir():
                return results
            for skill_md in sorted(skills_dir.rglob("SKILL.md")):
                try:
                    text = skill_md.read_text(encoding="utf-8", errors="replace")
                    fm = self._parse_frontmatter(text)
                    if not fm:
                        continue
                    skill_dir = skill_md.parent
                    if skill_dir.parent == skills_dir:
                        category = skill_dir.name
                    else:
                        category = skill_dir.parent.name
                    name = fm.get("name", skill_dir.name)
                    desc = fm.get("description", "")
                    tags = []
                    metadata = fm.get("metadata", {}) or {}
                    hermes_md = metadata.get("hermes", {}) or {}
                    raw_tags = hermes_md.get("tags", []) or []
                    tags = [t for t in raw_tags if isinstance(t, str)]
                    results.append({
                        "name": name,
                        "description": desc,
                        "tags": tags,
                        "category": category,
                    })
                except Exception:
                    continue
        except Exception as e:
            logger.debug("snow-search skills load failed: %s", e)
        return results

    @staticmethod
    def _parse_frontmatter(text: str) -> dict | None:
        """Extract YAML frontmatter between --- delimiters."""
        if not text.startswith("---"):
            return None
        end = text.find("---", 3)
        if end == -1:
            return None
        try:
            import yaml
            return yaml.safe_load(text[3:end]) or {}
        except Exception:
            return None

    def _load_memory(self) -> list[dict]:
        """Load built-in memory entries from MEMORY.md and USER.md (profile-isolated)."""
        results = []
        try:
            for source in ("MEMORY.md", "USER.md"):
                path = self._resolve_data_path("memories", source)
                if not path.exists():
                    continue
                text = path.read_text(encoding="utf-8", errors="replace")
                entries = [e.strip() for e in text.split("§") if e.strip()]
                for entry in entries[:self._memory_max]:
                    results.append({
                        "source": source.replace(".md", ""),
                        "content": entry,
                    })
        except Exception as e:
            logger.debug("snow-search memory load failed: %s", e)
        return results

    def _load_soul(self) -> list[dict]:
        """Load SOUL.md content (profile-isolated). Single document, no § delimiter."""
        results = []
        try:
            path = self._resolve_data_path("SOUL.md")
            if not path.exists():
                return results
            self._soul_mtime = path.stat().st_mtime
            text = path.read_text(encoding="utf-8", errors="replace").strip()
            if text:
                results.append({
                    "source": "SOUL.md",
                    "content": text,
                })
        except Exception as e:
            logger.debug("snow-search soul load failed: %s", e)
        return results

    def _refresh_soul_if_changed(self) -> None:
        """Reload SOUL.md if modified since last load."""
        try:
            path = self._resolve_data_path("SOUL.md")
            if not path.exists():
                if self._soul:
                    self._soul = []
                    self._soul_mtime = 0.0
                return
            mtime = path.stat().st_mtime
            if mtime == self._soul_mtime:
                return
            self._soul = self._load_soul()
        except Exception:
            pass

    # -- search ---------------------------------------------------------------

    def handle_search(
        self,
        args: dict,
        **kwargs,
    ) -> str:
        """Tool handler: parallel search across all loaded stores."""
        query = args.get("query", "")
        limit = min(int(args.get("limit_per_source", 10)), 50)
        include_sessions = args.get("include_sessions", True)
        include_facts = args.get("include_facts", True)
        include_memory = args.get("include_memory", True)
        include_skills = args.get("include_skills", True)
        include_soul = args.get("include_soul", True)
        deep = args.get("deep", True)
        role_filter = args.get("role_filter")
        debug = args.get("debug", False)

        start_ts = args.get("start_timestamp")
        end_ts = args.get("end_timestamp")
        has_time_filter = start_ts is not None or end_ts is not None

        # Action routing
        action = args.get("action", "search")
        if action == "reload":
            return self._reload()
        elif action == "status":
            return self._status()

        if deep:
            self._ensure_facts_and_memory()
            self._ensure_deep_loaded()
            # Ensure sessions/skills/soul are also loaded (bail out if background
            # eager load failed or hasn't finished yet).
            self._ensure_loaded()
        else:
            self._ensure_loaded()

        # Empty query: if no time filter, return early
        if not query or not query.strip():
            if not has_time_filter:
                return json.dumps({
                    "success": True,
                    "query": "",
                    "hits": [],
                    "total": 0,
                    "stores_available": {
                        "sessions": bool(self._sessions),
                        "facts": bool(self._facts),
                        "memory": bool(self._memory_entries),
                        "deep_messages": bool(self._deep_messages),
                        "skills": bool(self._skills),
                        "soul": bool(self._soul),
                    },
                    "message": "No query provided. Use query= to search across all stores.",
                })

        query_tokens = _tokenize(query) if query else set()
        if not query_tokens and not has_time_filter:
            return json.dumps({"success": True, "query": query, "hits": [], "total": 0})

        stores = {}

        if deep:
            if not self._deep_enabled:
                if include_sessions and self._sessions:
                    stores["sessions"] = self._sessions
            else:
                if self._use_fts:
                    # FTS mode: deep_messages are queried from the DB at search
                    # time, not held in RAM. Register a sentinel so the store is
                    # included; the searcher routes to _search_deep_fts.
                    if include_sessions:
                        stores["deep_messages"] = []  # sentinel; FTS ignores it
                elif include_sessions and self._deep_messages:
                    data = self._deep_messages
                    if has_time_filter:
                        data = [m for m in data
                                if (start_ts is None or m["timestamp"] >= start_ts)
                                and (end_ts is None or m["timestamp"] < end_ts)]
                    if data:
                        stores["deep_messages"] = data
        else:
            if include_sessions and self._sessions:
                stores["sessions"] = self._sessions

        if include_facts and self._facts:
            stores["facts"] = self._facts
        if include_memory and self._memory_entries:
            stores["memory"] = self._memory_entries
        if include_skills and self._skills:
            stores["skills"] = self._skills
        if include_soul and self._soul:
            stores["soul"] = self._soul

        if not stores:
            return json.dumps({
                "success": True,
                "query": query,
                "hits": [],
                "total": 0,
                "message": "No stores available — all data sources are empty or disabled.",
            })

        # Confidence threshold — drop low-quality hits so agent trusts results
        MIN_CONFIDENCE = 0.5

        sort = args.get("sort", "relevance")
        if has_time_filter and not query_tokens:
            searcher_limit = max(limit * 10, 200)
        else:
            searcher_limit = limit * 10 if sort in ("oldest", "newest") else limit

        # Source priority: soul > memory > facts > deep_messages > sessions
        # (see _SOURCE_PRIORITY). Primary key in every sort mode.

        hits = []
        exact_total = 0
        # NOTE: do NOT use `with ThreadPoolExecutor(...)` — its __exit__ calls
        # shutdown(wait=True), which blocks until every worker finishes and
        # ignores KeyboardInterrupt. That made Ctrl-C unable to interrupt a
        # running search. Manage the executor manually so we can bail out fast
        # on interrupt: cancel pending futures and skip waiting on in-flight ones.
        # deep_messages in FTS mode is queried directly from the DB — no
        # in-memory dataset, so it can't go through the chunk searcher. Run it
        # inline, then run the remaining (in-memory) stores in parallel.
        fts_stores = []
        mem_stores = {}
        for store_name, data in stores.items():
            if store_name == "deep_messages" and self._use_fts:
                fts_stores.append(store_name)
            else:
                mem_stores[store_name] = data

        if fts_stores and (query_tokens or has_time_filter):
            try:
                fts_hits, fts_exact = self._search_deep_fts(
                    query=query,
                    tokens=query_tokens,
                    role_filter=role_filter,
                    start_ts=start_ts if has_time_filter else None,
                    end_ts=end_ts if has_time_filter else None,
                    sort=sort,
                    limit=limit,
                )
                hits.extend(fts_hits)
                exact_total += fts_exact
            except Exception as e:
                logger.debug("snow-search deep FTS failed: %s", e)

        ex = ThreadPoolExecutor(max_workers=max(1, len(mem_stores)))
        future_map = {}
        try:
            for store_name, data in mem_stores.items():
                fn = self._make_searcher(store_name, data, query_tokens, searcher_limit, debug=debug, min_confidence=MIN_CONFIDENCE, role_filter=role_filter)
                future_map[ex.submit(fn)] = store_name
            for f in as_completed(future_map):
                store_name = future_map[f]
                try:
                    scored, exact = f.result()
                    hits.extend(scored)
                    exact_total += exact
                except Exception as e:
                    logger.debug("snow-search %s failed: %s", store_name, e)
        except BaseException:
            for f in future_map:
                f.cancel()
            raise
        finally:
            ex.shutdown(wait=False, cancel_futures=True)

        # Sorting — snow_search replaces session_search, so deep_messages is
        # the main stage: rank by true relevance/time first. _SOURCE_PRIORITY
        # is a TIEBREAKER only (same score/timestamp), never the primary key —
        # otherwise soul/memory bury real chat matches ("soul 霸榜").
        if sort == "oldest":
            hits.sort(key=lambda h: (h.get("timestamp", float("inf")) if h.get("timestamp") else float("inf"),
                                     -_SOURCE_PRIORITY.get(h.get("source", ""), 0)))
        elif sort == "newest":
            hits.sort(key=lambda h: (-(h.get("timestamp", 0) if h.get("timestamp") else 0),
                                     -_SOURCE_PRIORITY.get(h.get("source", ""), 0)))
        else:
            # relevance: pure score, no source bias
            hits.sort(key=lambda h: -h.get("score", 0))
        # total = full match count (any score > 0) across all stores, NOT
        # capped by min_confidence or limit. This mirrors session_search's
        # precise COUNT semantics — agents can trust it for frequency questions.
        total = exact_total
        hits = hits[:limit * 3]

        search_info = {
            "sessions_scanned": self._deep_total_sessions if deep and self._deep_enabled else len(self._sessions),
            "full_coverage": getattr(self, "_full_coverage", False),
        }
        if deep and self._deep_enabled:
            search_info["messages_scanned"] = getattr(self, "_deep_total_messages", len(self._deep_messages))
            if self._deep_earliest_ts < float("inf") and self._deep_latest_ts > 0:
                search_info["date_range"] = f"{datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime('%b %d')} ~ {datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime('%b %d')}"
            if self._use_fts:
                search_info["fts_mode"] = True
        if debug:
            search_info["debug"] = True
            search_info["tokens"] = list(query_tokens)

        result = {
            "success": True,
            "query": query,
            "hits": hits,
            "total": total,
            "stores_available": {
                "sessions": bool(self._sessions),
                "facts": bool(self._facts),
                "memory": bool(self._memory_entries),
                "deep_messages": self._deep_enabled and (self._use_fts or bool(self._deep_messages)),
            },
            "search_info": search_info,
        }

        if has_time_filter:
            result["filtered_time_range"] = {
                "start_timestamp": start_ts,
                "end_timestamp": end_ts,
            }

        return json.dumps(result, ensure_ascii=False)

    def _make_searcher(self, store_name: str, data: list[dict], tokens: set[str], limit: int, debug: bool = False, min_confidence: float = 0.5, role_filter: str | None = None):
        """Return a callable that searches one store.

        When tokens is empty (time-filter-only mode), every item gets score=1.0.
        When debug=True, each hit carries match path details.
        Hits below min_confidence are dropped — prevents low-quality matches
        that cause agents to distrust results.

        For deep_messages, uses inverted index when available to narrow candidates
        from O(N) to O(hits) before scoring. Also splits work across parallel chunks.
        """
        # Pre-filter via inverted index for deep_messages.
        # Use INTERSECTION when multiple tokens — rare tokens first.
        if store_name == "deep_messages" and tokens and self._deep_index_ready:
            sorted_tokens = sorted(
                [t for t in tokens if t in self._deep_index],
                key=lambda t: len(self._deep_index[t]),
            )
            if sorted_tokens:
                candidates = set(self._deep_index[sorted_tokens[0]])
                for tok in sorted_tokens[1:]:
                    candidates &= set(self._deep_index[tok])
                    if not candidates:
                        break  # empty intersection — stop early
                if candidates:
                    data = [self._deep_messages[i] for i in candidates
                            if i < len(self._deep_messages)]

        # role_filter applies to deep_messages regardless of which path produced
        # `data` above (inverted-index prefilter, time-only mode, or index-not-ready
        # fallback). Filtering here guarantees it is never silently dropped.
        if store_name == "deep_messages" and role_filter and data:
            data = [m for m in data if m.get("role") == role_filter]

        def _search_chunk(chunk):
            chunk_scored = []
            chunk_exact = 0  # full match count (any score > 0), NOT capped by min_confidence
            for item in chunk:
                score, debug_info = (
                    self._score_item(store_name, tokens, item, debug) if tokens
                    else (1.0, {})
                )
                if score > 0:
                    chunk_exact += 1
                if score >= min_confidence:
                    entry = {"source": store_name, "score": round(score, 3)}
                    if score >= 0.7:
                        entry["confidence"] = "high"
                    elif score >= 0.5:
                        entry["confidence"] = "medium"
                    else:
                        entry["confidence"] = "low"
                    entry["content"] = self._format_item(store_name, item)
                    if debug and debug_info:
                        entry["_debug"] = debug_info
                    if store_name == "facts":
                        entry["trust_score"] = item.get("trust_score", 0.5)
                        entry["category"] = item.get("category", "general")
                    elif store_name == "sessions":
                        entry["session_id"] = item.get("session_id", "")
                        entry["title"] = item.get("title", "Untitled")
                        entry["last_active"] = item.get("last_active", "")
                        sid = item.get("session_id", "")
                        if sid and sid in self._session_raw_fragments:
                            entry["raw_fragments"] = self._session_raw_fragments[sid]
                    elif store_name == "deep_messages":
                        entry["session_id"] = item.get("session_id", "")
                        entry["timestamp"] = item.get("timestamp", 0)
                        entry["role"] = item.get("role", "")
                    elif store_name == "skills":
                        entry["name"] = item.get("name", "")
                        entry["category"] = item.get("category", "")
                        entry["tags"] = item.get("tags", [])
                    elif store_name == "soul":
                        entry["source"] = item.get("source", "")
                    chunk_scored.append(entry)
            return chunk_scored, chunk_exact

        def _search():
            # Only deep_messages benefits from chunked parallelism — it is the
            # one store large enough (tens of thousands of messages) to justify
            # the inner thread pool. Every other store is small enough that the
            # pool overhead + GIL contention outweighs any gain, so score them
            # single-threaded.
            n = len(data)
            if store_name != "deep_messages" or n <= 1000:
                return _search_chunk(data)

            chunk_size = max(1000, n // 8)
            chunks = [data[i:i + chunk_size] for i in range(0, n, chunk_size)]
            scored = []
            exact = 0
            # Manual executor control (see handle_search): the `with` form's
            # shutdown(wait=True) would block Ctrl-C. ex.map drains results in
            # order; on interrupt we cancel pending chunks and stop waiting.
            ex = ThreadPoolExecutor(max_workers=min(8, len(chunks)))
            try:
                for chunk_scored, chunk_exact in ex.map(_search_chunk, chunks):
                    scored.extend(chunk_scored)
                    exact += chunk_exact
            except BaseException:
                raise
            finally:
                ex.shutdown(wait=False, cancel_futures=True)

            scored.sort(key=lambda x: -x["score"])
            return scored[:limit], exact
        return _search

    def _score_item(self, store: str, tokens: set[str], item: dict, debug: bool = False) -> tuple[float, dict]:
        """Compute relevance score for one item.

        Returns (score, debug_info). debug_info is non-empty only when debug=True.
        """
        debug_info = {"store": store} if debug else {}

        if store == "sessions":
            title = item.get("title", "")
            preview = item.get("preview", "")
            raw_frags = item.get("raw_fragments", [])
            # Score title highest, then raw fragments (for short sessions), then preview
            score_title, di_title = _match_score(tokens, title, debug)
            score_prev, di_prev = _match_score(tokens, preview, debug)
            score = score_title * 3.0 + score_prev * 1.5
            # Also score raw fragments if present (short session supplement)
            score_frags = 0.0
            di_frags = []
            for frag in raw_frags:
                sf, df = _match_score(tokens, frag, debug)
                score_frags += sf
                di_frags.extend(df)
            if raw_frags:
                score += (score_frags / len(raw_frags)) * 1.0
            if debug:
                debug_info["title_score"] = round(score_title, 3)
                debug_info["preview_score"] = round(score_prev, 3)
                debug_info["title_hits"] = di_title
                debug_info["preview_hits"] = di_prev
                debug_info["fragments_score"] = round(score_frags / len(raw_frags), 3) if raw_frags else 0
            return score, debug_info

        elif store == "facts":
            content = item.get("content", "")
            tags = item.get("tags", "")
            score_con, di_con = _match_score(tokens, content, debug)
            score_tag, di_tag = _match_score(tokens, tags, debug)
            score = score_con * 2.0 + score_tag * 3.0
            trust = item.get("trust_score", 0.5)
            score *= trust
            if debug:
                debug_info["content_score"] = round(score_con, 3)
                debug_info["tags_score"] = round(score_tag, 3)
                debug_info["trust"] = trust
                debug_info["content_hits"] = di_con
                debug_info["tags_hits"] = di_tag
            return score, debug_info

        elif store == "memory":
            content = item.get("content", "")
            score, di = _match_score(tokens, content, debug)
            if debug:
                debug_info["content_score"] = round(score / 2.0, 3)
                debug_info["content_hits"] = di
            return score, debug_info

        elif store == "deep_messages":
            content = item.get("content", "")
            preview = item.get("content_preview", "")
            score_con, di_con = _match_score(tokens, content, debug)
            score_prev, di_prev = _match_score(tokens, preview, debug)
            score = score_con * 2.0 + score_prev * 1.0
            # Recency boost: messages within last 24h get +0.5
            age = time.time() - item.get("timestamp", 0)
            if age < 86400:
                score += 0.5
            elif age < 604800:
                score += 0.2
            if debug:
                debug_info["content_score"] = round(score_con, 3)
                debug_info["preview_score"] = round(score_prev, 3)
                debug_info["recency_boost"] = 0.5 if age < 86400 else (0.2 if age < 604800 else 0)
                debug_info["content_hits"] = di_con
                debug_info["preview_hits"] = di_prev
            return score, debug_info

        elif store == "skills":
            name = item.get("name", "")
            desc = item.get("description", "")
            tags = " ".join(item.get("tags", []))
            score_name, di_name = _match_score(tokens, name, debug)
            score_desc, di_desc = _match_score(tokens, desc, debug)
            score_tags, di_tags = _match_score(tokens, tags, debug)
            score = score_name * 4.0 + score_desc * 2.0 + score_tags * 3.0
            if debug:
                debug_info["name_score"] = round(score_name, 3)
                debug_info["desc_score"] = round(score_desc, 3)
                debug_info["tags_score"] = round(score_tags, 3)
                debug_info["name_hits"] = di_name
                debug_info["desc_hits"] = di_desc
                debug_info["tags_hits"] = di_tags
            return score, debug_info

        elif store == "soul":
            content = item.get("content", "")
            score, di = _match_score(tokens, content, debug)
            if debug:
                debug_info["content_score"] = round(score / 1.5, 3)
                debug_info["content_hits"] = di
            return score, debug_info

        return 0.0, {}

    @staticmethod
    def _format_item(store: str, item: dict) -> str:
        """Display string for one item. Deep messages include surrounding context
        so agent has full picture without follow-up session_search calls.
        """
        if store == "sessions":
            return item.get("preview", "") or item.get("title", "")
        elif store == "facts":
            return item.get("content", "")
        elif store == "memory":
            return item.get("content", "")
        elif store == "deep_messages":
            # Return full content + nearby context (truncated to 2000 chars)
            content = item.get("content", "") or item.get("content_preview", "")
            return content[:2000]
        elif store == "skills":
            return f"{item.get('name', '')}: {item.get('description', '')}"
        elif store == "soul":
            return item.get("content", "")[:500]
        return ""

    # -- incremental updates via hooks ----------------------------------------

    def on_post_tool_call(
        self,
        tool_name: str = "",
        args: dict | None = None,
        result: str = "",
        **kwargs,
    ) -> None:
        """Detect fact_store/memory writes → update in-memory cache."""
        if not self._ready:
            return
        if not args:
            args = {}

        try:
            action = args.get("action", "")
            if tool_name == "fact_store":
                if action == "add":
                    self._on_fact_added(args, result)
                elif action == "remove":
                    self._on_fact_removed(args)
                elif action in ("replace", "update"):
                    self._on_fact_updated(args)
            elif tool_name == "memory":
                if action == "add":
                    self._on_memory_added(args)
                elif action in ("replace", "remove"):
                    self._on_memory_removed_or_replaced(args)

            # Check SOUL.md for external edits
            self._refresh_soul_if_changed()
        except Exception:
            pass  # Never let a hook crash the agent loop

    def _on_fact_added(self, args: dict, result: str) -> None:
        """Append new fact to in-memory list."""
        content = args.get("content", "")
        if not content:
            return
        try:
            parsed = json.loads(result)
            fact_id = parsed.get("fact_id")
        except Exception:
            fact_id = None

        entry = {
            "fact_id": fact_id or 0,
            "content": content,
            "category": args.get("category", "general"),
            "tags": args.get("tags", ""),
            "trust_score": 0.5,
        }
        with self._lock:
            self._facts.insert(0, entry)
            self._current_bytes += _estimate_bytes(entry)
        # Don't trigger eviction here — wait for pre_llm_call

    def _on_memory_added(self, args: dict) -> None:
        """Append new memory entry to in-memory list."""
        target = args.get("target", "memory")  # "memory" or "user"
        content = args.get("content", "")
        if not content:
            return

        entry = {
            "source": "MEMORY.md" if target == "memory" else "USER.md",
            "content": content,
        }
        with self._lock:
            self._memory_entries.insert(0, entry)
            self._current_bytes += _estimate_bytes(entry)

    # -- fact remove / update -------------------------------------------------

    def _on_fact_removed(self, args: dict) -> None:
        """Remove fact from in-memory cache by fact_id."""
        fact_id = args.get("fact_id")
        if fact_id is None:
            return
        with self._lock:
            before = len(self._facts)
            self._facts = [f for f in self._facts if f.get("fact_id") != fact_id]
            removed = before - len(self._facts)
            if removed:
                self._current_bytes = (
                    _estimate_bytes(self._sessions)
                    + _estimate_bytes(self._facts)
                    + _estimate_bytes(self._memory_entries)
                )

    def _on_fact_updated(self, args: dict) -> None:
        """Update fact content in in-memory cache."""
        fact_id = args.get("fact_id")
        content = args.get("content", "")
        if fact_id is None or not content:
            return
        with self._lock:
            for f in self._facts:
                if f.get("fact_id") == fact_id:
                    old_bytes = _estimate_bytes(f)
                    f["content"] = content
                    f["category"] = args.get("category", f.get("category", "general"))
                    f["tags"] = args.get("tags", f.get("tags", ""))
                    self._current_bytes += _estimate_bytes(f) - old_bytes
                    break

    # -- memory remove / replace ----------------------------------------------

    def _on_memory_removed_or_replaced(self, args: dict) -> None:
        """Remove or replace a memory entry by target+old_text match."""
        target = args.get("target", "memory")
        old_text = args.get("old_text", "")
        source = "MEMORY.md" if target == "memory" else "USER.md"
        action = args.get("action", "")

        if not old_text:
            return

        with self._lock:
            before = len(self._memory_entries)
            # Filter out the old entry
            self._memory_entries = [
                m for m in self._memory_entries
                if not (m.get("source") == source and old_text in m.get("content", ""))
            ]
            if action == "replace":
                # Add the new content
                new_content = args.get("content", "")
                if new_content:
                    self._memory_entries.insert(0, {
                        "source": source,
                        "content": new_content,
                    })
            if before != len(self._memory_entries):
                self._current_bytes = (
                    _estimate_bytes(self._sessions)
                    + _estimate_bytes(self._facts)
                    + _estimate_bytes(self._memory_entries)
                )

    def on_pre_llm_call(self, **kwargs) -> dict | str | None:
        """Eviction check before each LLM call."""
        if not self._ready:
            return None
        limit_mb = self._memory_limit
        if limit_mb <= 0:
            return None
        threshold = int(limit_mb * 0.8)

        with self._lock:
            if self._current_bytes < threshold:
                return None
            self._evict()
        return None  # No context injection needed

    def on_post_llm_call(self, **kwargs) -> None:
        """Clear snow_search results from conversation history to save context tokens."""
        history = kwargs.get("conversation_history")
        if not history:
            return
        for msg in history:
            if msg.get("role") != "tool":
                continue
            name = msg.get("name") or msg.get("tool_name") or ""
            if name == "snow_search":
                msg["content"] = ""

    def _evict(self) -> None:
        """Evict least valuable entries until under threshold."""
        target = int(self._memory_limit * 0.6)

        # 1. Sessions: keep most recent
        self._sessions.sort(key=lambda s: float(s.get("last_active", 0) or 0), reverse=True)
        self._sessions = self._sessions[:self._session_max]

        # 2. Facts: keep highest trust
        self._facts.sort(key=lambda f: f.get("trust_score", 0.5), reverse=True)
        self._facts = self._facts[:self._fact_max]

        # 3. Memory entries: keep most recent (inserted at head on update)
        self._memory_entries = self._memory_entries[:self._memory_max]

        self._current_bytes = (
            _estimate_bytes(self._sessions)
            + _estimate_bytes(self._facts)
            + _estimate_bytes(self._memory_entries)
            + _estimate_bytes(self._skills)
        )

    # -- reload / status ------------------------------------------------------

    def _reload(self) -> str:
        """Clear all data, reload from DB, return full status JSON."""
        with self._lock:
            self._ready = False
            self._deep_ready = False
            self._sessions = []
            self._facts = []
            self._memory_entries = []
            self._current_bytes = 0
            self._deep_messages = []
            self._deep_bytes = 0
            self._deep_from_jsonl = False
            self._skills = []
            self._soul = []
            self._holographic_available = None
            self._session_raw_fragments.clear()
            self._deep_index.clear()
            self._deep_index_ready = False

        _emit("Reloading snow search index...")
        self._load_all()
        self._ready = True

        if self._deep_enabled:
            try:
                self._load_deep()
                self._deep_ready = True
            except Exception as e:
                _emit(f"Deep reload failed: {e}")

        _emit("Reload complete")
        return self._status()

    def _status(self) -> str:
        """Return current index state — zero I/O, memory-only."""
        import datetime

        counts = {
            "sessions": len(self._sessions),
            "facts": len(self._facts),
            "memory": len(self._memory_entries),
            "deep_messages": getattr(self, "_deep_total_messages", len(self._deep_messages)),
            "skills": len(self._skills),
            "soul": len(self._soul),
        }

        memory_mb = {
            "current_mb": round(self._current_bytes / (1024 * 1024), 1),
            "deep_mb": round(self._deep_bytes / (1024 * 1024), 1) if not self._use_fts else 0,
        }

        coverage = {
            "full_coverage": getattr(self, "_full_coverage", False),
        }
        if self._use_fts:
            coverage["fts_mode"] = True
        if self._deep_earliest_ts < float("inf") and self._deep_latest_ts > 0:
            coverage["date_range"] = (
                f"{datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime('%b %d')}"
                f" ~ {datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime('%b %d')}"
            )

        return json.dumps(
            {
                "success": True,
                "action": "status",
                "counts": counts,
                "memory": memory_mb,
                "coverage": coverage,
                "ready": self._ready,
                "deep_ready": self._deep_ready,
            },
            ensure_ascii=False,
        )

    # -- reload (public) ------------------------------------------------------

    def reload(self) -> str:
        """Force a full reload of all stores. Usable from /snow-reload command."""
        with self._lock:
            self._ready = False
            self._deep_ready = False
            self._holographic_available = None
        self._ensure_loaded()
        msg = f"snow-search reloaded: {len(self._sessions)} sessions, {len(self._facts)} facts, {len(self._memory_entries)} memory, ~{self._current_bytes // (1024 * 1024)} MB"
        if self._deep_enabled:
            try:
                self._ensure_deep_loaded()
                msg += f" | deep: {getattr(self, '_deep_total_messages', len(self._deep_messages))} messages"
            except Exception as e:
                msg += f" | deep: error ({e})"
        if self._load_error:
            return f"snow-search reloaded with error: {self._load_error}"
        return msg
