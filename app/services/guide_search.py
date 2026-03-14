# app/services/guide_search.py
"""
Web search executor for the Roam Guide.

Supports Tavily (primary, built for LLM consumption) and Google Custom
Search Engine (fallback). Returns clean [{"title", "content", "url"}]
dicts ready for injection into the LLM context.

Graceful degradation: returns [] on any failure so the guide can still
answer from its base knowledge.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import httpx

from app.core.settings import settings

logger = logging.getLogger(__name__)


async def web_search(
    query: str, max_results: int | None = None
) -> List[Dict[str, str]]:
    """
    Search the web and return clean results for LLM consumption.

    Returns a list of dicts with keys: title, content, url.
    Returns [] on failure (never raises).
    """
    if not query or not query.strip():
        return []

    provider = settings.guide_search_provider.lower()

    if provider == "tavily" and settings.tavily_api_key:
        return await _tavily_search(query, max_results)
    elif provider == "google_cse" and settings.google_cse_api_key and settings.google_cse_cx:
        return await _google_cse_search(query, max_results)
    elif provider == "none":
        return []
    else:
        # Auto-detect: try tavily first, then google
        if settings.tavily_api_key:
            return await _tavily_search(query, max_results)
        if settings.google_cse_api_key and settings.google_cse_cx:
            return await _google_cse_search(query, max_results)
        return []


async def _tavily_search(
    query: str, max_results: int | None = None
) -> List[Dict[str, str]]:
    """Search via Tavily API (https://tavily.com)."""
    n = max_results or settings.tavily_max_results
    timeout = settings.guide_search_timeout_s

    body: Dict[str, Any] = {
        "query": query,
        "max_results": n,
        "include_answer": False,
        "search_depth": "basic",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(
                "https://api.tavily.com/search",
                json=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {settings.tavily_api_key}",
                },
            )
            if r.status_code >= 400:
                logger.warning("Tavily search failed: %d %s", r.status_code, r.text[:200])
                return []
            data = r.json()
    except Exception as e:
        logger.warning("Tavily search error: %s", e)
        return []

    results: List[Dict[str, str]] = []
    for item in data.get("results", []):
        results.append({
            "title": item.get("title", ""),
            "content": item.get("content", ""),
            "url": item.get("url", ""),
        })
    return results


async def _google_cse_search(
    query: str, max_results: int | None = None
) -> List[Dict[str, str]]:
    """Search via Google Custom Search Engine JSON API."""
    n = min(max_results or 5, 10)  # Google CSE max is 10 per request
    timeout = settings.guide_search_timeout_s

    params: Dict[str, Any] = {
        "key": settings.google_cse_api_key,
        "cx": settings.google_cse_cx,
        "q": query,
        "num": n,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(
                "https://www.googleapis.com/customsearch/v1",
                params=params,
            )
            if r.status_code >= 400:
                logger.warning("Google CSE failed: %d %s", r.status_code, r.text[:200])
                return []
            data = r.json()
    except Exception as e:
        logger.warning("Google CSE error: %s", e)
        return []

    results: List[Dict[str, str]] = []
    for item in data.get("items", []):
        results.append({
            "title": item.get("title", ""),
            "content": item.get("snippet", ""),
            "url": item.get("link", ""),
        })
    return results
