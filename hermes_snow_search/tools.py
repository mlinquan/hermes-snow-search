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
_DEFAULT_DEEP_ENABLED = True
_DEFAULT_DEEP_LOAD_MODE = "ondemand"  # "startup" | "ondemand"

# ---------------------------------------------------------------------------
# Tool schema
# ---------------------------------------------------------------------------

SNOW_SEARCH_SCHEMA = {
    "name": "snow_search",
    "description": (
        "Fast in-memory search across ALL stores: session history, holographic facts, "
        "built-in memory, and skill metadata. Cached in RAM — results in <1ms. Use this "
        "FIRST for any recall: past conversations, preferences, decisions, project context."
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
        "\n- action=reload: clear and reload the entire search index"
        "\n- action=status: get current index statistics (zero I/O)"
        "\n\n"
        "Guidance: When user says \"snow reload\" or asks to reload/refresh the index, "
        "pass action=reload. When user says \"snow status\" or asks for index stats, "
        "pass action=status. Queries about reloading/status of other things "
        "(e.g. \"how to reload nginx\") are normal searches — only route when "
        "intent is clearly about the search index itself."
        "\n\n"
        "Deep search: deep=true searches full message bodies. Check search_info.full_coverage "
        "— if false, fall back to session_search for older sessions."
        "\n\n"
        "Retrieval rule: Check search_info.full_coverage — if true, snow_search covers "
        "everything. If false or absent, session_search may be needed for older sessions. "
        "For ANY recall question, ALWAYS call this tool with relevant search terms. "
        "Do NOT call session_search for retrieval — it only scrolls into already-identified "
        "sessions. Never call memory or fact_store for reads — they are write-only."
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
                "description": "Search query — keywords, partial matches supported. Required for action=search.",
            },
            "limit_per_source": {
                "type": "integer",
                "description": "Max results per source (default: 5, max: 20).",
                "default": 5,
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
                "description": "Search full message bodies instead of session summaries. Requires deep search index (loaded on first use). Default: false.",
                "default": False,
            },
            "sort": {
                "type": "string",
                "enum": ["relevance", "oldest", "newest"],
                "description": "Sort order: 'relevance' (default, best match first), 'oldest' (earliest timestamp first — use when user asks about FIRST / EARLIEST occurrence), 'newest' (latest timestamp first — use when user asks about LAST / MOST RECENT).",
                "default": "relevance",
            },
        },
        "required": [],
    },
}

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


def _tokenize(text: str) -> set[str]:
    """Lowercase keyword tokens."""
    return set(_WHITESPACE.split(text.lower().strip())) if text else set()


def _match_score(query_tokens: set[str], text: str) -> float:
    """Score how many query tokens appear in text."""
    if not query_tokens or not text:
        return 0.0
    lower = text.lower()
    hits = sum(1 for t in query_tokens if t in lower)
    return hits / len(query_tokens) if hits > 0 else 0.0


def _estimate_bytes(obj: Any) -> int:
    """Rough memory estimate for a Python object (dict/list of strings)."""
    try:
        raw = json.dumps(obj, ensure_ascii=False, default=str)
        return len(raw.encode("utf-8"))
    except Exception:
        return 0


def _emit(msg: str) -> None:
    """Write progress to stderr — visible in terminal, not captured by tool output."""
    sys.stderr.write(f"[Hermes Snow Search] {msg}\n")
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
        self._profile = os.environ.get("HERMES_PROFILE") or config.get("orchestrator_profile") or None

        # --- deep search config ---
        self._deep_enabled = config.get("deep_search_enabled", _DEFAULT_DEEP_ENABLED)
        self._deep_mode = config.get("deep_search_load_mode", _DEFAULT_DEEP_LOAD_MODE)

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
        self._full_coverage: bool = False  # True when all sessions loaded into deep index

        # --- holographic availability (checked once) ---
        self._holographic_available: bool | None = None

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
        """Resolve a data path respecting profile isolation.

        If a profile is active, paths resolve under profiles/<name>/.
        Otherwise they resolve under the Hermes home root.
        """
        from hermes_constants import get_hermes_home
        import pathlib
        home = get_hermes_home()
        if self._profile:
            base = home / "profiles" / self._profile
        else:
            base = home
        return base.joinpath(*parts)

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

    def _refresh_deep_if_needed(self) -> None:
        """Check for new messages since last load and incrementally update.

        Runs before every deep search. Queries MAX(id) from messages table
        (~0.1ms), only fetches new rows if max_id changed.
        Thread-safe via _deep_ready being True — called only after
        _ensure_deep_loaded() has completed.
        """
        if not self._deep_messages:
            return
        try:
            from hermes_state import SessionDB
            db = SessionDB()
            try:
                row = db._conn.execute(
                    "SELECT MAX(id) FROM messages "
                    "WHERE role IN ('user','assistant') AND content IS NOT NULL"
                ).fetchone()
                if not row or row[0] is None:
                    return
                current_max = row[0]
                if current_max <= self._deep_max_message_id:
                    return

                # Fetch new messages since last load
                new_rows = db._conn.execute(
                    "SELECT id, session_id, role, content, timestamp "
                    "FROM messages WHERE id > ? "
                    "AND role IN ('user','assistant') AND content IS NOT NULL",
                    (self._deep_max_message_id,),
                ).fetchall()

                added = 0
                for r in new_rows:
                    entry = {
                        "message_id": r[0],
                        "session_id": r[1],
                        "role": r[2],
                        "content": r[3],
                        "timestamp": r[4],
                        "content_preview": r[3][:200],
                    }
                    self._deep_messages.append(entry)
                    self._deep_bytes += _estimate_bytes(entry)
                    ts = r[4]
                    if ts < self._deep_earliest_ts:
                        self._deep_earliest_ts = ts
                    if ts > self._deep_latest_ts:
                        self._deep_latest_ts = ts
                    added += 1
                    if r[0] > self._deep_max_message_id:
                        self._deep_max_message_id = r[0]

                if added > 0:
                    # Re-sort by timestamp descending
                    self._deep_messages.sort(key=lambda m: -m["timestamp"])
                    self._deep_total_sessions = len(
                        set(m["session_id"] for m in self._deep_messages)
                    )
                    logger.debug(
                        "snow-search deep refresh: +%d messages, now %d total, ~%d MB",
                        added, len(self._deep_messages),
                        self._deep_bytes // (1024 * 1024),
                    )
            finally:
                db.close()
        except Exception as e:
            logger.debug("snow-search deep refresh failed: %s", e)

    def _load_deep(self) -> None:
        """Load full message bodies from SessionDB into RAM.

        Iterates sessions newest-first, pulls all messages per session,
        stores user/assistant messages with session_id and timestamp.
        Stops at 85% of memory_limit.
        Prints progress to terminal.
        """
        from hermes_state import SessionDB
        db = SessionDB()

        raw = db.list_sessions_rich(
            limit=100000,
            exclude_sources=["tool"],
            order_by_last_active=True,
        )

        sessions = [s for s in raw if not s.get("parent_session_id")]
        total = len(sessions)
        cap = int(self._memory_limit * 0.85)
        start_time = time.time()
        last_print_pct = -1

        self._deep_messages = []
        self._deep_bytes = 0
        self._deep_total_sessions = 0
        self._deep_earliest_ts = float("inf")
        self._deep_latest_ts = 0.0

        _emit("Loading deep search index...")

        for i, s in enumerate(sessions):
            sid = s.get("id", "")
            if not sid:
                continue

            try:
                msgs = db.get_messages(sid)
            except Exception:
                continue

            for m in msgs:
                role = m.get("role", "")
                if role not in ("user", "assistant"):
                    continue
                content = m.get("content", "")
                if not content:
                    continue

                entry = {
                    "message_id": m["id"],
                    "session_id": sid,
                    "timestamp": m["timestamp"],
                    "role": role,
                    "content": content,
                    "content_preview": content[:200],
                }
                self._deep_messages.append(entry)
                self._deep_bytes += _estimate_bytes(entry)
                ts = m["timestamp"]
                if ts < self._deep_earliest_ts:
                    self._deep_earliest_ts = ts
                if ts > self._deep_latest_ts:
                    self._deep_latest_ts = ts

            self._deep_total_sessions += 1

            # Progress printing at milestones
            pct = (i + 1) / total * 100
            milestone = min(int(pct / 25) * 25, 100) if pct >= 5 else 0
            if milestone != last_print_pct and pct >= milestone:
                last_print_pct = milestone
                elapsed = time.time() - start_time
                msg_count = len(self._deep_messages)
                mb_used = self._deep_bytes // (1024 * 1024)
                avg_per_session = elapsed / (i + 1)
                remaining_s = (total - i - 1) * avg_per_session if i > 0 else 0
                if remaining_s >= 1:
                    remaining_str = f" | ~{remaining_s:.0f}s remaining"
                elif remaining_s > 0:
                    remaining_str = f" | ~{int(remaining_s * 1000)}ms remaining"
                else:
                    remaining_str = ""
                _emit(
                    f"Session {i + 1}/{total} | {msg_count} messages | "
                    f"{mb_used}/{self._memory_limit // (1024 * 1024)} MB{remaining_str}"
                )

            # Memory cap check
            if self._deep_bytes >= cap:
                break

        db.close()

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
            import datetime
            earliest = datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime("%b %d")
            latest = datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime("%b %d")
            days = (self._deep_latest_ts - self._deep_earliest_ts) / 86400
            if days >= 1:
                coverage = f" | {days:.0f} days ({earliest} ~ {latest})"
            else:
                coverage = ""
        else:
            coverage = ""

        _emit(
            f"Deep search ready | "
            f"{msg_count} messages{coverage} | {mb_used} MB"
        )

        # Full coverage indicator
        if self._deep_total_sessions >= total:
            self._full_coverage = True
            _emit(f"All chat data loaded -- full coverage, no eviction")
        else:
            self._full_coverage = False
            pct_loaded = self._deep_total_sessions / total * 100 if total > 0 else 0
            _emit(f"Memory cap reached -- {self._deep_total_sessions}/{total} sessions loaded ({pct_loaded:.0f}%), oldest evicted")

    def _load_sessions(self) -> list[dict]:
        """Load session titles + previews from SessionDB."""
        try:
            from hermes_state import SessionDB
            db = SessionDB()
            raw = db.list_sessions_rich(
                limit=self._session_max + 10,
                exclude_sources=["tool"],
                order_by_last_active=True,
            )
            results = []
            for s in raw:
                if s.get("parent_session_id"):
                    continue
                results.append({
                    "session_id": s.get("id", ""),
                    "title": s.get("title", ""),
                    "last_active": s.get("last_active", ""),
                    "preview": s.get("preview", ""),
                    "message_count": s.get("message_count", 0),
                })
                if len(results) >= self._session_max:
                    break
            return results
        except Exception as e:
            logger.debug("snow-search session load failed: %s", e)
            return []

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
        limit = min(int(args.get("limit_per_source", 5)), 20)
        include_sessions = args.get("include_sessions", True)
        include_facts = args.get("include_facts", True)
        include_memory = args.get("include_memory", True)
        include_skills = args.get("include_skills", True)
        include_soul = args.get("include_soul", True)
        deep = args.get("deep", False)

        # Action routing
        action = args.get("action", "search")
        if action == "reload":
            return self._reload()
        elif action == "status":
            return self._status()

        if deep:
            # Deep mode: load full message bodies + facts + memory (skip sessions)
            self._ensure_facts_and_memory()
            self._ensure_deep_loaded()
            self._refresh_deep_if_needed()
        else:
            # Light mode: load all lightweight stores
            self._ensure_loaded()

        if not query or not query.strip():
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

        query_tokens = _tokenize(query)
        if not query_tokens:
            return json.dumps({"success": True, "query": query, "hits": [], "total": 0})

        # Determine which stores to search
        stores = {}

        if deep:
            if not self._deep_enabled:
                # Deep search not configured — fall back to session search
                if include_sessions and self._sessions:
                    stores["sessions"] = self._sessions
            else:
                # Deep mode: ensure loaded + refresh, then search message bodies
                self._ensure_deep_loaded()
                self._refresh_deep_if_needed()
                if include_sessions and self._deep_messages:
                    stores["deep_messages"] = self._deep_messages
        else:
            # Light mode: search session summaries
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

        # Parallel search
        # Determine sort mode — affects how many hits we pull per searcher
        sort = args.get("sort", "relevance")
        # Chronological sort needs more raw data to avoid score-based pre-cutting
        searcher_limit = limit * 10 if sort in ("oldest", "newest") else limit

        hits = []
        with ThreadPoolExecutor(max_workers=len(stores)) as ex:
            future_map = {}
            for store_name, data in stores.items():
                fn = self._make_searcher(store_name, data, query_tokens, searcher_limit)
                future_map[ex.submit(fn)] = store_name
            for f in as_completed(future_map):
                store_name = future_map[f]
                try:
                    results = f.result()
                    hits.extend(results)
                except Exception as e:
                    logger.debug("snow-search %s failed: %s", store_name, e)

        if sort == "oldest":
            # Chronological ascending — keep all hits, sort by time, trim at end
            hits.sort(key=lambda h: h.get("timestamp", float("inf")) if h.get("timestamp") else float("inf"))
            total = len(hits)
            hits = hits[:limit * 3]
        elif sort == "newest":
            # Chronological descending — keep all hits, sort by time desc, trim at end
            hits.sort(key=lambda h: -(h.get("timestamp", 0) if h.get("timestamp") else 0))
            total = len(hits)
            hits = hits[:limit * 3]
        else:
            # Default: sort by score desc, then trim
            hits.sort(key=lambda h: (-h.get("score", 0), h.get("source", "")))
            total = len(hits)
            hits = hits[:limit * 3]

        # Search coverage metadata
        search_info = {
            "sessions_scanned": len(self._sessions) if not deep and self._sessions else self._deep_total_sessions if deep and self._deep_messages else 0,
            "full_coverage": getattr(self, "_full_coverage", False),
        }
        if deep and self._deep_messages:
            search_info["messages_scanned"] = len(self._deep_messages)
            if self._deep_earliest_ts < float("inf") and self._deep_latest_ts > 0:
                import datetime
                search_info["date_range"] = f"{datetime.datetime.fromtimestamp(self._deep_earliest_ts).strftime('%b %d')} ~ {datetime.datetime.fromtimestamp(self._deep_latest_ts).strftime('%b %d')}"

        return json.dumps({
            "success": True,
            "query": query,
            "hits": hits,
            "total": total,
            "stores_available": {
                "sessions": bool(self._sessions),
                "facts": bool(self._facts),
                "memory": bool(self._memory_entries),
                "deep_messages": bool(self._deep_messages),
            },
            "search_info": search_info,
        }, ensure_ascii=False)

    def _make_searcher(self, store_name: str, data: list[dict], tokens: set[str], limit: int):
        """Return a callable that searches one store."""
        def _search():
            scored = []
            for item in data:
                score = self._score_item(store_name, tokens, item)
                if score > 0:
                    entry = {"source": store_name, "score": round(score, 3)}
                    entry["content"] = self._format_item(store_name, item)
                    if store_name == "facts":
                        entry["trust_score"] = item.get("trust_score", 0.5)
                        entry["category"] = item.get("category", "general")
                    elif store_name == "sessions":
                        entry["session_id"] = item.get("session_id", "")
                        entry["title"] = item.get("title", "Untitled")
                        entry["last_active"] = item.get("last_active", "")
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
                    scored.append(entry)

            scored.sort(key=lambda x: -x["score"])
            return scored[:limit]
        return _search

    @staticmethod
    def _score_item(store: str, tokens: set[str], item: dict) -> float:
        """Compute relevance score for one item."""
        if store == "sessions":
            title = item.get("title", "")
            preview = item.get("preview", "")
            score = _match_score(tokens, title) * 3.0
            score += _match_score(tokens, preview) * 1.5
            return score

        elif store == "facts":
            content = item.get("content", "")
            tags = item.get("tags", "")
            score = _match_score(tokens, content) * 2.0
            score += _match_score(tokens, tags) * 3.0
            trust = item.get("trust_score", 0.5)
            score *= trust
            return score

        elif store == "memory":
            content = item.get("content", "")
            return _match_score(tokens, content) * 2.0

        elif store == "deep_messages":
            content = item.get("content", "")
            preview = item.get("content_preview", "")
            score = _match_score(tokens, content) * 2.0
            score += _match_score(tokens, preview) * 1.0
            # Recency boost: messages within last 24h get +0.5
            import time as _time
            age = _time.time() - item.get("timestamp", 0)
            if age < 86400:
                score += 0.5
            elif age < 604800:
                score += 0.2
            return score

        elif store == "skills":
            name = item.get("name", "")
            desc = item.get("description", "")
            tags = " ".join(item.get("tags", []))
            score = _match_score(tokens, name) * 4.0
            score += _match_score(tokens, desc) * 2.0
            score += _match_score(tokens, tags) * 3.0
            return score

        elif store == "soul":
            content = item.get("content", "")
            return _match_score(tokens, content) * 1.5

        return 0.0

    @staticmethod
    def _format_item(store: str, item: dict) -> str:
        """Short display string for one item."""
        if store == "sessions":
            return item.get("preview", "") or item.get("title", "")
        elif store == "facts":
            return item.get("content", "")
        elif store == "memory":
            return item.get("content", "")
        elif store == "deep_messages":
            return item.get("content_preview", item.get("content", ""))
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
            self._skills = []
            self._soul = []
            self._holographic_available = None

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
            "deep_messages": len(self._deep_messages),
            "skills": len(self._skills),
            "soul": len(self._soul),
        }

        memory_mb = {
            "current_mb": round(self._current_bytes / (1024 * 1024), 1),
            "deep_mb": round(self._deep_bytes / (1024 * 1024), 1),
        }

        coverage = {
            "full_coverage": getattr(self, "_full_coverage", False),
        }
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
                msg += f" | deep: {len(self._deep_messages)} messages, ~{self._deep_bytes // (1024 * 1024)} MB"
            except Exception as e:
                msg += f" | deep: error ({e})"
        if self._load_error:
            return f"snow-search reloaded with error: {self._load_error}"
        return msg
