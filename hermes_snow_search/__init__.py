"""Snow Search — in-memory parallel search for Hermes.

Loads session summaries, holographic facts, and built-in memory into RAM.
Searches all three in parallel. Incrementally updated via hooks.
Evicts oldest/lowest-trust entries when approaching memory limit.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

_engine: "SnowSearchEngine | None" = None


def register(ctx) -> None:
    """Plugin entry point: register tool + hooks."""
    global _engine
    # Import lazily so the plugin module can be loaded before SnowSearchEngine's
    # heavy dependencies (session DB, holographic store, etc.) are ready.
    from .tools import SnowSearchEngine, SNOW_SEARCH_SCHEMA

    _engine = SnowSearchEngine(ctx)
    ctx.register_tool(
        name="snow_search",
        toolset="hermes",
        schema=SNOW_SEARCH_SCHEMA,
        handler=_engine.handle_search,
    )
    ctx.register_hook("pre_llm_call", _engine.on_pre_llm_call)
    ctx.register_hook("post_tool_call", _engine.on_post_tool_call)
    ctx.register_hook("post_llm_call", _engine.on_post_llm_call)

    # Background eager load with terminal progress
    def _eager_load():
        time.sleep(2.5)  # Wait for CLI startup banner + Bus messages
        _engine._ensure_loaded()
        # Startup deep search if configured
        if _engine._deep_enabled and _engine._deep_mode == "startup":
            try:
                _engine._ensure_deep_loaded()
            except Exception:
                pass

    threading.Thread(target=_eager_load, daemon=True).start()
    logger.info("hermes-snow-search registered — eager loading in background")
