# app/services/guide_data.py
"""
Data loader for the Roam Guide knowledge base.

All guide knowledge (regions, towns, seasons, vehicles, etc.) lives in
JSON files under app/data/guide/. This module loads them lazily on first
access and caches the result for the lifetime of the process.
"""
from __future__ import annotations

import json
import functools
from pathlib import Path
from typing import Any, Dict, List, Tuple

_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "guide"


def _load(filename: str) -> Any:
    with open(_DATA_DIR / filename, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Regions ──────────────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_regions() -> List[Dict[str, Any]]:
    """18 Australian regions with id, name, bbox, and knowledge."""
    return _load("regions.json")


# ── Towns ────────────────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_towns() -> Dict[str, Tuple[float, float]]:
    """100+ Australian towns → (lat, lng)."""
    raw: Dict[str, List[float]] = _load("towns.json")
    return {k: (v[0], v[1]) for k, v in raw.items()}


# ── Seasonal knowledge ───────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_seasonal_blocks() -> List[Dict[str, Any]]:
    """Seasonal knowledge blocks with condition rules."""
    return _load("seasonal.json")


# ── Vehicle profiles ─────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_vehicle_profiles() -> Dict[str, Dict[str, Any]]:
    """Vehicle profile advice keyed by profile type."""
    return _load("vehicles.json")


# ── Companion hints ──────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_companion_types() -> List[Dict[str, Any]]:
    """Companion/travel-party types with detection terms and advice."""
    return _load("companions.json")


# ── Intent definitions ───────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_intent_definitions() -> List[Dict[str, Any]]:
    """Intent types with keyword terms and guidance."""
    return _load("intents.json")


# ── Time context ─────────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_time_context_blocks() -> List[Dict[str, Any]]:
    """Time-of-day blocks with hour ranges, labels, and advice."""
    return _load("time_context.json")


# ── Deep knowledge ───────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_deep_knowledge() -> str:
    """Static deep Australian road trip knowledge."""
    return _load("deep_knowledge.json")["knowledge"]
