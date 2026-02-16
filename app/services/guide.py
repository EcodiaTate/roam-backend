# app/services/guide.py
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.core.settings import settings
from app.core.contracts import (
    PlacesRequest,
    CorridorPlacesRequest,
    PlacesSuggestRequest,
)

GuideToolName = Literal["places_search", "places_corridor", "places_suggest"]


class GuideMsg(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    resolved_tool_id: Optional[str] = None


class TripProgress(BaseModel):
    user_lat: float
    user_lng: float
    user_accuracy_m: float = 0.0
    user_heading: Optional[float] = None
    user_speed_mps: Optional[float] = None

    current_stop_idx: int = 0
    current_leg_idx: int = 0
    visited_stop_ids: List[str] = Field(default_factory=list)

    km_from_start: float = 0.0
    km_remaining: float = 0.0
    total_km: float = 0.0

    local_time_iso: Optional[str] = None
    timezone: str = "Australia/Brisbane"
    updated_at: Optional[str] = None


class WirePlace(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    category: str
    dist_km: Optional[float] = None
    ahead: bool = True
    locality: Optional[str] = None
    hours: Optional[str] = None
    phone: Optional[str] = None


class GuideContext(BaseModel):
    plan_id: Optional[str] = None
    label: Optional[str] = None
    profile: Optional[str] = None
    route_key: Optional[str] = None
    corridor_key: Optional[str] = None
    geometry: Optional[str] = None
    bbox: Optional[Dict[str, Any]] = None
    stops: List[Dict[str, Any]] = Field(default_factory=list)
    total_distance_m: Optional[float] = None
    total_duration_s: Optional[float] = None
    manifest_route_key: Optional[str] = None
    offline_stale: Optional[bool] = None
    progress: Optional[TripProgress] = None
    traffic_summary: Optional[Dict[str, Any]] = None
    hazards_summary: Optional[Dict[str, Any]] = None


class GuideToolCall(BaseModel):
    id: str
    tool: GuideToolName
    req: Dict[str, Any]


class GuideToolResult(BaseModel):
    id: str
    tool: GuideToolName
    ok: bool = True
    result: Dict[str, Any]


class GuideTurnRequest(BaseModel):
    context: GuideContext
    thread: List[GuideMsg] = Field(default_factory=list)
    tool_results: List[GuideToolResult] = Field(default_factory=list)
    preferred_categories: List[str] = Field(default_factory=list)
    relevant_places: List[WirePlace] = Field(default_factory=list)


class GuideTurnResponse(BaseModel):
    assistant: str = ""
    tool_calls: List[GuideToolCall] = Field(default_factory=list)
    done: bool = False


def _response_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "assistant": {"type": "string"},
            "done": {"type": "boolean"},
            "tool_calls": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "id": {"type": "string"},
                        "tool": {
                            "type": "string",
                            "enum": ["places_search", "places_corridor", "places_suggest"],
                        },
                        "req": {"type": "object"},
                    },
                    "required": ["tool", "req"],
                },
            },
        },
        "required": ["done", "tool_calls"],
    }


# ──────────────────────────────────────────────────────────────
# System prompt
# ──────────────────────────────────────────────────────────────

def _format_stops(stops: List[Dict[str, Any]], visited_ids: List[str] | None = None) -> str:
    visited = set(visited_ids or [])
    lines: List[str] = []
    for i, s in enumerate(stops):
        sid = s.get("id", f"p{i}")
        stype = s.get("type", "poi")
        name = s.get("name", "Unnamed")
        lat = s.get("lat", 0)
        lng = s.get("lng", 0)
        marker = "" if sid in visited else "⬜"
        lines.append(f"  {marker} [{i}] {name} ({stype}) @ {lat:.4f},{lng:.4f}")
    return "\n".join(lines) if lines else "  (no stops)"


def _time_advice(local_iso: str | None) -> str:
    if not local_iso:
        return "Unknown time."
    try:
        dt = datetime.fromisoformat(local_iso.replace("Z", "+00:00"))
        hour = dt.hour
    except Exception:
        return "Unknown time."

    if 5 <= hour < 7:
        return f"Early morning ({hour:02d}:00). Wildlife on roads. Suggest fuel/coffee."
    elif 7 <= hour < 12:
        return f"Morning ({hour:02d}:00). Good driving time."
    elif 12 <= hour < 14:
        return f"Midday ({hour:02d}:00). Consider a lunch stop."
    elif 14 <= hour < 17:
        return f"Afternoon ({hour:02d}:00). Plan accommodation if long distance."
    elif 17 <= hour < 19:
        return f"Late afternoon ({hour:02d}:00). Look for overnight stops. Wildlife active at dusk."
    elif 19 <= hour < 21:
        return f"Evening ({hour:02d}:00). Night driving risky in rural areas. Suggest stopping."
    else:
        return f"Night ({hour:02d}:00). Dangerous to drive in rural/outback. Stop if possible."


def _format_speed(speed_mps: float | None) -> str:
    if speed_mps is None or speed_mps < 0:
        return "unknown"
    return f"{speed_mps * 3.6:.0f} km/h"


def _format_relevant_places(places: List[WirePlace]) -> str:
    """Format pre-filtered relevant places for the LLM prompt."""
    if not places:
        return "  (none pre-loaded — use tools to search)"

    lines: List[str] = []
    for p in places:
        parts = [f"{p.name} ({p.category})"]
        if p.locality:
            parts.append(p.locality)
        if p.dist_km is not None:
            parts.append(f"{p.dist_km:.1f}km away")
        if not p.ahead:
            parts.append("BEHIND")
        if p.hours:
            parts.append(f"hrs: {p.hours[:30]}")
        if p.phone:
            parts.append(f"ph: {p.phone}")
        lines.append("  " + " · ".join(parts))

    return "\n".join(lines)


def _build_system_prompt(ctx: GuideContext, relevant_places: List[WirePlace]) -> str:
    total_km = (ctx.total_distance_m or 0) / 1000 if ctx.total_distance_m else None
    total_h = int((ctx.total_duration_s or 0) // 3600) if ctx.total_duration_s else None
    total_m = int(((ctx.total_duration_s or 0) % 3600) // 60) if ctx.total_duration_s else None

    route_info = ""
    if total_km is not None:
        route_info = f"{total_km:.0f}km"
        if total_h is not None:
            route_info += f" ~{total_h}h{total_m}m"

    progress = ctx.progress
    visited_ids = progress.visited_stop_ids if progress else []
    stops_str = _format_stops(ctx.stops, visited_ids)

    pos_str = "Location unavailable."
    if progress:
        stop_name = "unknown"
        if 0 <= progress.current_stop_idx < len(ctx.stops):
            stop_name = ctx.stops[progress.current_stop_idx].get("name", "unnamed")
        pos_str = (
            f"At ({progress.user_lat:.5f},{progress.user_lng:.5f}) ±{progress.user_accuracy_m:.0f}m, "
            f"near [{progress.current_stop_idx}] \"{stop_name}\", "
            f"{progress.km_from_start:.0f}km done / {progress.km_remaining:.0f}km left, "
            f"{_format_speed(progress.user_speed_mps)}"
        )

    time_str = _time_advice(progress.local_time_iso if progress else None)

    # Conditions
    cond_parts: List[str] = []
    ts = ctx.traffic_summary
    if ts and ts.get("total", 0) > 0:
        cond_parts.append(f"Traffic: {ts['total']} events")
        for s in ts.get("sample", [])[:2]:
            cond_parts.append(f"  {s.get('type','?')}: {s.get('headline','')[:60]}")
    hs = ctx.hazards_summary
    if hs and hs.get("total", 0) > 0:
        cond_parts.append(f"Hazards: {hs['total']} alerts")
        for s in hs.get("sample", [])[:2]:
            cond_parts.append(f"  {s.get('kind','?')}: {s.get('title','')[:60]}")
    conditions = "\n".join(cond_parts) if cond_parts else "No alerts."

    # Tool availability
    tools_info = ""
    if ctx.corridor_key:
        tools_info += f"corridor_key=\"{ctx.corridor_key}\" → places_corridor OK\n"
    else:
        tools_info += "No corridor_key → use places_search\n"
    if ctx.geometry and len(ctx.geometry) > 10:
        tools_info += "Geometry available → places_suggest OK (auto-injected)\n"
    else:
        tools_info += "No geometry → places_suggest unavailable\n"

    km_rule = ""
    if progress and progress.km_from_start > 0:
        km_rule = f"IMPORTANT: User at km {progress.km_from_start:.0f}. Only suggest places AHEAD."

    # Pre-filtered relevant places
    places_str = _format_relevant_places(relevant_places)
    places_count = len(relevant_places)

    # How the LLM should use them
    if places_count > 0:
        places_instruction = (
            f"RELEVANT PLACES ({places_count} pre-filtered from corridor, sorted nearest-first):\n"
            f"{places_str}\n\n"
            f"These places match the user's query and are already filtered/ranked. "
            f"You can recommend directly from this list — set done=true and present your picks. "
            f"Only use a tool call if you need DIFFERENT categories or a wider/narrower search."
        )
    else:
        places_instruction = (
            "No pre-filtered places available for this query. "
            "Use a tool call to search: places_corridor for along-route, "
            "places_search for nearby, places_suggest for route sampling."
        )

    return f"""You are Roam Guide — road trip companion for an Australian nav app.

TRIP: {ctx.label or 'Unnamed'} | {ctx.profile or 'drive'} | {route_info}
STOPS:
{stops_str}
POSITION: {pos_str}
TIME: {time_str}
CONDITIONS: {conditions}
{km_rule}

{places_instruction}

TOOLS (use ONLY if the pre-filtered places above don't answer the query):
{tools_info}
1. places_search — bbox/center/query/categories. For "near me" or "in [town]".
2. places_corridor — needs corridor_key. For "along my route".
3. places_suggest — needs geometry (auto-filled). For sampling stops at intervals.

RULES:
- If relevant places are provided and answer the query, set done=true and recommend from them directly. No tool call needed.
- If you need different data, emit ONE tool call with brief assistant text (e.g. "Searching for camps…").
- Keep responses SHORT — mobile screen. Mention name, category, distance, locality.
- Never invent places. Only use data from the pre-filtered list or tool results.
- If user asks about something behind them, note they've passed it and suggest ahead."""


# ──────────────────────────────────────────────────────────────
# User message builder
# ──────────────────────────────────────────────────────────────

_MAX_THREAD = 10
_MAX_TOOL_RESULTS = 3
_MAX_PLACES_PER_RESULT = 12


def _summarize_tool_result(tr: GuideToolResult) -> Dict[str, Any]:
    out: Dict[str, Any] = {"id": tr.id, "tool": tr.tool, "ok": tr.ok}
    if not tr.ok:
        out["error"] = str(tr.result.get("error", "?"))[:200]
        return out

    result = tr.result
    if tr.tool in ("places_search", "places_corridor"):
        raw_items = result.get("items", [])
        compact = []
        for p in raw_items[:_MAX_PLACES_PER_RESULT]:
            entry: Dict[str, Any] = {
                "name": p.get("name", "?"),
                "cat": p.get("category", "?"),
                "lat": round(p.get("lat", 0), 4),
                "lng": round(p.get("lng", 0), 4),
            }
            compact.append(entry)
        out["total"] = len(raw_items)
        out["places"] = compact
    elif tr.tool == "places_suggest":
        clusters = result.get("clusters", [])
        compact_clusters = []
        for cl in clusters[:5]:
            cl_items = cl.get("places", {}).get("items", [])
            sample = [{"name": p.get("name", "?"), "cat": p.get("category", "?")} for p in cl_items[:4]]
            compact_clusters.append({"km": cl.get("km_from_start", 0), "n": len(cl_items), "sample": sample})
        out["clusters"] = compact_clusters
    return out


def _build_user_message(req: GuideTurnRequest) -> str:
    parts: List[str] = []

    if req.thread:
        for m in req.thread[-_MAX_THREAD:]:
            label = "U" if m.role == "user" else "G"
            text = m.content[:300] + "…" if len(m.content) > 300 else m.content
            parts.append(f"[{label}]: {text}")

    if req.tool_results:
        parts.append("\nTool results:")
        for tr in req.tool_results[-_MAX_TOOL_RESULTS:]:
            parts.append(json.dumps(_summarize_tool_result(tr), ensure_ascii=False, separators=(",", ":")))

    if req.preferred_categories:
        parts.append(f"\nFilter: {','.join(req.preferred_categories)}")

    parts.append("\nNext: recommend from pre-filtered places (done=true), or emit ONE tool_call if you need different data.")
    return "\n".join(parts)


# ──────────────────────────────────────────────────────────────
# Normalization / repair helpers
# ──────────────────────────────────────────────────────────────

def _snake_key_aliases() -> Dict[str, str]:
    return {
        "corridorKey": "corridor_key", "corridorId": "corridor_key",
        "routeKey": "route_key", "limitPerSample": "limit_per_sample",
        "intervalKm": "interval_km", "radiusM": "radius_m",
        "minLng": "minLng", "minLat": "minLat", "maxLng": "maxLng", "maxLat": "maxLat",
    }


def _repair_req(tool: GuideToolName, req: Dict[str, Any], ctx: GuideContext) -> Dict[str, Any]:
    if not isinstance(req, dict):
        return {}

    aliases = _snake_key_aliases()
    fixed: Dict[str, Any] = {aliases.get(k, k): v for k, v in req.items()}

    if tool == "places_corridor":
        if not fixed.get("corridor_key") and ctx.corridor_key:
            fixed["corridor_key"] = ctx.corridor_key
        cats = fixed.get("categories")
        if cats is None: fixed["categories"] = []
        elif isinstance(cats, str): fixed["categories"] = [cats]
        if "limit" in fixed and fixed["limit"] is not None:
            try: fixed["limit"] = int(fixed["limit"])
            except: pass

    elif tool == "places_suggest":
        if not fixed.get("geometry") and ctx.geometry:
            fixed["geometry"] = ctx.geometry
        for k in ("interval_km", "radius_m", "limit_per_sample"):
            if k in fixed and fixed[k] is not None:
                try: fixed[k] = int(fixed[k])
                except: pass
        cats = fixed.get("categories")
        if cats is None: fixed["categories"] = []
        elif isinstance(cats, str): fixed["categories"] = [cats]

    elif tool == "places_search":
        if "limit" in fixed and fixed["limit"] is not None:
            try: fixed["limit"] = int(fixed["limit"])
            except: pass
        cats = fixed.get("categories")
        if cats is None: fixed["categories"] = []
        elif isinstance(cats, str): fixed["categories"] = [cats]
        if fixed.get("query") == "": fixed.pop("query", None)

        if not fixed.get("center") and not fixed.get("bbox") and ctx.progress and ctx.progress.user_lat:
            if not fixed.get("query"):
                fixed["center"] = {"lat": ctx.progress.user_lat, "lng": ctx.progress.user_lng}
                if not fixed.get("radius_m"): fixed["radius_m"] = 50000

    return fixed


def _validate_tool_req(tool: GuideToolName, req: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        if tool == "places_search": PlacesRequest.model_validate(req)
        elif tool == "places_corridor": CorridorPlacesRequest.model_validate(req)
        elif tool == "places_suggest": PlacesSuggestRequest.model_validate(req)
        else: return False, f"Unknown tool: {tool}"
        return True, ""
    except ValidationError as ve:
        return False, str(ve)


def _normalize_model_output(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"assistant": "", "tool_calls": [], "done": False}
    raw.setdefault("assistant", "")
    raw.setdefault("tool_calls", [])
    raw.setdefault("done", False)
    if not isinstance(raw.get("tool_calls"), list):
        raw["tool_calls"] = []
    norm_calls: List[Dict[str, Any]] = []
    for i, tc in enumerate(raw["tool_calls"]):
        if not isinstance(tc, dict): continue
        tc.setdefault("id", f"tc_{i}_{uuid.uuid4().hex[:8]}")
        norm_calls.append(tc)
    raw["tool_calls"] = norm_calls
    return raw


# ──────────────────────────────────────────────────────────────
# Service
# ──────────────────────────────────────────────────────────────

class GuideService:
    def __init__(self) -> None:
        self._api_key = settings.openai_api_key
        self._model = settings.openai_model
        self._base = settings.openai_base_url.rstrip("/")
        self._timeout = float(getattr(settings, "guide_timeout_s", None) or getattr(settings, "explore_timeout_s", None) or 25.0)

    async def turn(self, req: GuideTurnRequest) -> GuideTurnResponse:
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY missing")

        sys_prompt = _build_system_prompt(req.context, req.relevant_places)
        user_msg = _build_user_message(req)

        body = {
            "model": self._model,
            "input": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_msg},
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "GuideTurnResponse",
                    "strict": False,
                    "schema": _response_schema(),
                }
            },
        }

        url = f"{self._base}/responses"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(url, headers=headers, json=body)
            if r.status_code >= 400:
                raise RuntimeError(f"OpenAI /responses {r.status_code}: {r.text}")
            data = r.json()

        out_text: str | None = None
        for item in data.get("output", []) or []:
            if item.get("type") != "message": continue
            for c in item.get("content", []) or []:
                if c.get("type") == "output_text":
                    out_text = c.get("text")
                    break
            if out_text: break

        if not out_text:
            raise RuntimeError(f"Guide LLM: missing output_text. Raw: {json.dumps(data)[:2000]}")

        try:
            raw = json.loads(out_text)
        except Exception as e:
            raise RuntimeError(f"Guide LLM: invalid JSON: {e}. text={out_text[:400]}")

        norm = _normalize_model_output(raw)

        tool_calls = norm.get("tool_calls") or []
        if tool_calls:
            tc0 = tool_calls[0]
            tool = tc0.get("tool")
            req_obj = tc0.get("req")
            tc_id = tc0.get("id") or f"tc_{uuid.uuid4().hex[:8]}"

            if tool not in ("places_search", "places_corridor", "places_suggest"):
                norm["assistant"] = (norm.get("assistant") or "") + "\n\n(I couldn't pick a valid tool.)"
                norm["tool_calls"] = []
                norm["done"] = False
            else:
                fixed_req = _repair_req(tool, req_obj if isinstance(req_obj, dict) else {}, req.context)
                ok, err = _validate_tool_req(tool, fixed_req)
                if not ok:
                    norm["assistant"] = (norm.get("assistant") or "") + "\n\n(Tool request didn't validate. Let me try differently.)"
                    norm["tool_calls"] = []
                    norm["done"] = False
                else:
                    norm["tool_calls"] = [{"id": str(tc_id), "tool": tool, "req": fixed_req}]

        return GuideTurnResponse.model_validate(norm)