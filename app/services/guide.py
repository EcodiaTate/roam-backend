# app/services/guide.py
"""
Roam Guide — AI road trip companion.

Design principles:
  1. Safety first: fuel range, fatigue, wildlife, weather → always surface these
  2. Context-aware: time of day, km driven, family/solo, vehicle type
  3. Concise: mobile screen, one-handed, glancing while passenger reads aloud
  4. Proactive: don't wait to be asked — nudge about upcoming needs
  5. Local knowledge: use place data richly (hours, phone, website, fuel types)
  6. Never invent: only recommend from data or tool results
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.core.settings import settings
from app.core.contracts import (
    PlacesRequest,
    CorridorPlacesRequest,
    PlacesSuggestRequest,
    GuideToolName,
    GuideActionType,
    GuideMsg,
    TripProgress,
    WirePlace,
    GuideContext,
    GuideAction,
    GuideToolCall,
    GuideToolResult,
    GuideTurnRequest,
    GuideTurnResponse,
)


def _response_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "assistant": {"type": "string"},
            "done": {"type": "boolean"},
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "type": {"type": "string", "enum": ["web", "call"]},
                        "label": {"type": "string"},
                        "place_id": {"type": "string"},
                        "place_name": {"type": "string"},
                        "url": {"type": "string"},
                        "tel": {"type": "string"},
                    },
                    "required": ["type", "label"],
                },
            },
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
# System prompt helpers
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
        marker = "✅" if sid in visited else "⬜"
        lines.append(f"  {marker} [{i}] {name} ({stype}) @ {lat:.4f},{lng:.4f}")
    return "\n".join(lines) if lines else "  (no stops yet)"


def _trip_phase(progress: TripProgress | None, total_km: float | None) -> str:
    """
    Determine what phase of the trip the user is in, so the Guide
    can adjust its tone and priorities.
    """
    if not progress or not total_km or total_km <= 0:
        return "planning"

    pct = progress.km_from_start / total_km if total_km > 0 else 0

    if pct < 0.05:
        return "departing"        # Just left — excitement, check fuel/supplies
    elif pct < 0.35:
        return "early_cruise"     # Settled in — discovery mode
    elif pct < 0.65:
        return "midway"           # Might need lunch, fatigue break, fuel top-up
    elif pct < 0.90:
        return "home_stretch"     # Getting tired — accommodation, last fuel
    else:
        return "arriving"         # Almost there — focus on destination


def _time_context(local_iso: str | None) -> Tuple[str, str]:
    """
    Returns (time_label, driving_advice) based on local time.
    Tuned for Australian road conditions.
    """
    if not local_iso:
        return "Unknown time", ""

    try:
        dt = datetime.fromisoformat(local_iso.replace("Z", "+00:00"))
        hour = dt.hour
    except Exception:
        return "Unknown time", ""

    if 4 <= hour < 6:
        return (
            f"Pre-dawn ({hour:02d}:00)",
            "Very early. Roos and wallabies active. Drive with extreme caution. "
            "Consider waiting for daylight if rural."
        )
    elif 6 <= hour < 8:
        return (
            f"Early morning ({hour:02d}:00)",
            "Dawn — wildlife still active near roads. Good time to fuel up and grab a coffee."
        )
    elif 8 <= hour < 12:
        return (
            f"Morning ({hour:02d}:00)",
            "Prime driving hours. Good visibility, cool temps."
        )
    elif 12 <= hour < 14:
        return (
            f"Midday ({hour:02d}:00)",
            "Lunch time. Good moment for a proper break — stretch, eat, hydrate."
        )
    elif 14 <= hour < 16:
        return (
            f"Afternoon ({hour:02d}:00)",
            "Afternoon driving. If you've been going since morning, you're due for a break."
        )
    elif 16 <= hour < 18:
        return (
            f"Late afternoon ({hour:02d}:00)",
            "Start thinking about where you'll stop tonight. Book ahead in remote areas."
        )
    elif 18 <= hour < 20:
        return (
            f"Dusk ({hour:02d}:00)",
            "CAUTION: Dusk is peak wildlife collision time. Roos, wombats, cattle. "
            "Strongly recommend finding accommodation. Do not drive rural roads after dark if avoidable."
        )
    elif 20 <= hour < 22:
        return (
            f"Evening ({hour:02d}:00)",
            "Night driving is dangerous on rural/outback roads. If you must drive, "
            "reduce speed and watch for eyes in headlights."
        )
    else:
        return (
            f"Night ({hour:02d}:00)",
            "Avoid driving if possible. Fatigue + wildlife = extreme risk. "
            "Pull into a rest area or safe shoulder if tired."
        )


def _format_speed(speed_mps: float | None) -> str:
    if speed_mps is None or speed_mps < 0:
        return "unknown"
    return f"{speed_mps * 3.6:.0f} km/h"


def _fatigue_check(progress: TripProgress | None) -> str:
    """
    Australian road safety rule: stop every 2 hours.
    We can estimate driving time from km_from_start and speed,
    but we don't have session start time — so we nudge based on
    distance as a proxy (~100km/h average = 2hr ≈ 200km).
    """
    if not progress:
        return ""

    km = progress.km_from_start
    if km < 150:
        return ""

    # Every ~200km, suggest a break
    remainder = km % 200
    if remainder > 180 or remainder < 20:
        return (
            f"⚠ You've driven ~{km:.0f}km. Australian road safety recommends "
            f"stopping every 2 hours. Look for a rest area or town for a break."
        )

    return ""


def _format_relevant_places(places: List[WirePlace]) -> str:
    """
    Format pre-filtered places for the LLM context window.
    Rich enough to recommend from, compact enough to not bloat tokens.
    """
    if not places:
        return "  (none pre-loaded — use tools to search)"

    lines: List[str] = []
    for p in places:
        parts = [f"{p.id} | {p.name} ({p.category})"]
        if p.locality:
            parts.append(p.locality)
        if p.dist_km is not None:
            direction = "ahead" if p.ahead else "BEHIND"
            parts.append(f"{p.dist_km:.1f}km {direction}")
        if p.hours:
            parts.append(f"hrs: {p.hours[:40]}")
        if p.phone:
            parts.append(f"ph: {p.phone}")
        if p.website:
            parts.append(f"web: {p.website}")
        lines.append("  " + " · ".join(parts))

    return "\n".join(lines)


def _build_system_prompt(ctx: GuideContext, relevant_places: List[WirePlace]) -> str:
    total_km = (ctx.total_distance_m or 0) / 1000 if ctx.total_distance_m else None
    total_h = int((ctx.total_duration_s or 0) // 3600) if ctx.total_duration_s else None
    total_m = int(((ctx.total_duration_s or 0) % 3600) // 60) if ctx.total_duration_s else None

    route_info = ""
    if total_km is not None:
        route_info = f"{total_km:.0f} km"
        if total_h is not None:
            route_info += f", ~{total_h}h{total_m:02d}m drive"

    progress = ctx.progress
    visited_ids = progress.visited_stop_ids if progress else []
    stops_str = _format_stops(ctx.stops, visited_ids)

    # Position & progress
    pos_str = "Location unavailable."
    if progress:
        stop_name = "unknown"
        if 0 <= progress.current_stop_idx < len(ctx.stops):
            stop_name = ctx.stops[progress.current_stop_idx].get("name", "unnamed")
        pos_str = (
            f"({progress.user_lat:.5f}, {progress.user_lng:.5f}) "
            f"±{progress.user_accuracy_m:.0f}m, "
            f"near stop [{progress.current_stop_idx}] \"{stop_name}\", "
            f"speed {_format_speed(progress.user_speed_mps)}, "
            f"{progress.km_from_start:.0f} km done / {progress.km_remaining:.0f} km to go"
        )

    # Time & driving advice
    time_label, driving_advice = _time_context(
        progress.local_time_iso if progress else None
    )

    # Trip phase
    phase = _trip_phase(progress, total_km)

    # Fatigue nudge
    fatigue = _fatigue_check(progress)

    # Conditions (traffic + hazards)
    cond_parts: List[str] = []
    ts = ctx.traffic_summary
    if ts and ts.get("total", 0) > 0:
        cond_parts.append(f"Traffic: {ts['total']} events on route")
        for s in ts.get("sample", [])[:3]:
            cond_parts.append(f"  • {s.get('type', '?')}: {s.get('headline', '')[:80]}")
    hs = ctx.hazards_summary
    if hs and hs.get("total", 0) > 0:
        cond_parts.append(f"Hazards: {hs['total']} active alerts")
        for s in hs.get("sample", [])[:3]:
            cond_parts.append(f"  • {s.get('kind', '?')}: {s.get('title', '')[:80]}")
    conditions = "\n".join(cond_parts) if cond_parts else "No active alerts."

    # Available tools
    tools_info = ""
    if ctx.corridor_key:
        tools_info += f"  corridor_key=\"{ctx.corridor_key}\" → places_corridor available\n"
    else:
        tools_info += "  No corridor_key → use places_search instead\n"
    if ctx.geometry and len(ctx.geometry) > 10:
        tools_info += "  Route geometry available → places_suggest available (geometry auto-injected)\n"
    else:
        tools_info += "  No geometry → places_suggest unavailable\n"

    # Direction rule
    direction_rule = ""
    if progress and progress.km_from_start > 0:
        direction_rule = (
            f"DIRECTION: User is at km {progress.km_from_start:.0f} of {total_km or '?':.0f}. "
            f"Prioritise places AHEAD. If they ask about something behind them, "
            f"note they've passed it and suggest the next equivalent ahead."
        )

    # Pre-filtered places
    places_str = _format_relevant_places(relevant_places)
    places_count = len(relevant_places)

    if places_count > 0:
        places_block = (
            f"PRE-FILTERED PLACES ({places_count}, nearest-first, already corridor-filtered):\n"
            f"{places_str}\n\n"
            f"These match the user's query and are sorted by distance. "
            f"Recommend directly from this list (done=true). "
            f"Only use a tool call if you need DIFFERENT categories, "
            f"a different area, or the list doesn't answer their question."
        )
    else:
        places_block = (
            "No pre-filtered places for this query. Use a tool call:\n"
            "  • places_corridor → search along the route (needs corridor_key)\n"
            "  • places_search → search near a point or by text query\n"
            "  • places_suggest → sample highlights at intervals along route"
        )

    # ── Phase-specific guidance ──────────────────────────────
    phase_guidance = {
        "planning": (
            "Trip is being planned. Help with route decisions, "
            "what to pack, key stops to plan for, estimated drive times between stops."
        ),
        "departing": (
            "Just departed! Check they're fuelled up and have water. "
            "Mention the first interesting stop or landmark coming up. "
            "Set the tone — excited, adventure ahead."
        ),
        "early_cruise": (
            "Settled into the drive. Good time for discovery — "
            "interesting detours, scenic lookouts, local bakeries, "
            "swimming holes. They're fresh and open to suggestions."
        ),
        "midway": (
            "Midway through. Might need fuel, food, or a proper break. "
            "Proactively mention fuel stations if the next town is far. "
            "Suggest a lunch spot or rest area. Check fatigue."
        ),
        "home_stretch": (
            "Getting closer to destination. They might be tired. "
            "Prioritise accommodation if overnight, fuel if needed, "
            "and mention last-chance stops for supplies."
        ),
        "arriving": (
            "Nearly there! Help with arrival — "
            "where to park, what's nearby at the destination, "
            "any last info about the final stop."
        ),
    }
    phase_text = phase_guidance.get(phase, "")

    return f"""You are Roam Guide — the road trip companion for an Australian navigation app.

You're like a well-traveled mate riding shotgun: you know the roads, the good stops, and the safety stuff — but you're not preachy about it. You're helpful, warm, and concise. This is a mobile screen; every word must earn its place.

═══ TRIP CONTEXT ═══
Trip: {ctx.label or 'Unnamed trip'} | Profile: {ctx.profile or 'drive'} | {route_info}
Phase: {phase}
{phase_text}

Planned stops:
{stops_str}

═══ LIVE STATUS ═══
Position: {pos_str}
Time: {time_label}
{driving_advice}
{fatigue}

Conditions:
{conditions}

{direction_rule}

═══ WHAT YOU KNOW ABOUT PLACES ═══
{places_block}

═══ TOOLS (only when pre-filtered places don't answer the query) ═══
{tools_info}
1. places_search — search by bbox, center+radius, text query, or categories. Good for "near me", "in [town name]".
2. places_corridor — search along the route corridor. Needs corridor_key. Good for "along my route", "between here and [stop]".
3. places_suggest — sample highlights at regular intervals along route. Needs geometry (auto-injected). Good for "what are the highlights?", "plan my stops".

═══ CATEGORY VOCABULARY ═══
When using tools, these are valid category filters:
  Safety: fuel, ev_charging, rest_area, toilet, water, dump_point, mechanic, hospital, pharmacy
  Supplies: grocery, town, atm, laundromat
  Food: bakery, cafe, restaurant, fast_food, pub, bar
  Accommodation: camp, hotel, motel, hostel
  Nature: viewpoint, waterfall, swimming_hole, beach, national_park, hiking, picnic, hot_spring
  Family: playground, pool, zoo, theme_park
  Culture: visitor_info, museum, gallery, heritage, winery, brewery, attraction, market, park

═══ RESPONSE RULES ═══
1. CONCISE: 2-4 sentences max for simple queries. Mobile screen. No essays.
2. STRUCTURED: When recommending multiple places, use a compact list with name, distance, and one useful detail.
3. SAFETY: Proactively mention fuel/water/fatigue/wildlife when relevant — but once per topic, don't repeat.
4. ACTIONS: Include UI actions for recommended places:
   - If place has website: {{"type":"web", "label":"Website · PlaceName", "place_id":"id", "place_name":"name", "url":"https://..."}}
   - If place has phone: {{"type":"call", "label":"Call PlaceName", "place_id":"id", "place_name":"name", "tel":"04..."}}
   - Max one web + one call action per place. Only for places you actually recommend.
5. NEVER INVENT: Only recommend places from the pre-filtered list or tool results. Never make up names, hours, or phone numbers.
6. DONE FLAG: Set done=true when you're giving a final recommendation. Set done=false only when emitting a tool_call.
7. TOOL CALLS: Max ONE per turn. Include brief text like "Searching for camps ahead…" while the tool runs.
8. If the user just says hi or asks a general question, be friendly and offer to help find stops, food, fuel, or things to see.

═══ AUSTRALIAN ROAD TRIP KNOWLEDGE ═══
- Fuel: In outback/remote areas, stations can be 200-500km+ apart. Always note distance to next fuel.
- Rest areas: Free roadside rest stops are everywhere on highways. Many have toilets, some have BBQs and shelters.
- Bakeries: The quintessential Aussie road stop. Country bakeries often have award-winning pies and vanilla slices.
- Wildlife: Kangaroos, wombats, echidnas on roads — worst at dawn/dusk/night. Cattle grids = livestock area.
- Free camps: Many are just a cleared area by the road with basic facilities. WikiCamps and Camps Australia Wide are popular guides.
- Dump points: Essential for caravan/RV travelers. Many fuel stations and caravan parks have them.
- Towns: Even tiny towns (pop. 50) may have a pub serving meals and a fuel bowser. They're social hubs.
- Wineries/breweries: Many regions have cellar doors open for tastings — designated driver needed.
- Swimming holes: Best found through locals or data — not all are safe (crocs in QLD/NT, currents, depth).
- Time zones: QLD has no daylight saving. NSW/VIC do. SA is +30min offset. WA is 2-3hrs behind east coast.
"""


# ──────────────────────────────────────────────────────────────
# User message builder
# ──────────────────────────────────────────────────────────────

_MAX_THREAD = 10
_MAX_TOOL_RESULTS = 3
_MAX_PLACES_PER_RESULT = 15


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
                "id": p.get("id", ""),
                "name": p.get("name", "?"),
                "cat": p.get("category", "?"),
                "lat": round(p.get("lat", 0), 4),
                "lng": round(p.get("lng", 0), 4),
            }
            # Rich data for the LLM to use in recommendations
            extra = p.get("extra", {})
            if extra.get("phone") or p.get("phone"):
                entry["ph"] = str(extra.get("phone") or p.get("phone", ""))[:40]
            if extra.get("website") or p.get("website"):
                entry["web"] = str(extra.get("website") or p.get("website", ""))[:120]
            if extra.get("opening_hours"):
                entry["hrs"] = str(extra["opening_hours"])[:50]
            if extra.get("address"):
                entry["addr"] = str(extra["address"])[:60]
            if extra.get("fuel_types"):
                entry["fuel"] = extra["fuel_types"]
            if extra.get("socket_types"):
                entry["sockets"] = extra["socket_types"]
            if extra.get("free"):
                entry["free"] = True
            if extra.get("has_water"):
                entry["water"] = True
            if extra.get("has_toilets"):
                entry["toilets"] = True
            if extra.get("powered_sites"):
                entry["powered"] = True
            compact.append(entry)
        out["total"] = len(raw_items)
        out["shown"] = len(compact)
        out["places"] = compact

    elif tr.tool == "places_suggest":
        clusters = result.get("clusters", [])
        compact_clusters = []
        for cl in clusters[:6]:
            cl_items = cl.get("places", {}).get("items", [])
            sample = []
            for p in cl_items[:5]:
                s: Dict[str, Any] = {
                    "name": p.get("name", "?"),
                    "cat": p.get("category", "?"),
                }
                extra = p.get("extra", {})
                if extra.get("phone") or p.get("phone"):
                    s["ph"] = str(extra.get("phone") or p.get("phone", ""))[:40]
                if extra.get("website") or p.get("website"):
                    s["web"] = str(extra.get("website") or p.get("website", ""))[:120]
                sample.append(s)
            compact_clusters.append({
                "km": cl.get("km_from_start", 0),
                "n": len(cl_items),
                "sample": sample,
            })
        out["clusters"] = compact_clusters

    return out


def _build_user_message(req: GuideTurnRequest) -> str:
    parts: List[str] = []

    if req.thread:
        for m in req.thread[-_MAX_THREAD:]:
            label = "User" if m.role == "user" else "Guide"
            text = m.content[:400] + "…" if len(m.content) > 400 else m.content
            parts.append(f"[{label}]: {text}")

    if req.tool_results:
        parts.append("\n── Tool results ──")
        for tr in req.tool_results[-_MAX_TOOL_RESULTS:]:
            parts.append(json.dumps(
                _summarize_tool_result(tr),
                ensure_ascii=False,
                separators=(",", ":"),
            ))

    if req.preferred_categories:
        parts.append(f"\nCategory filter: {', '.join(req.preferred_categories)}")

    parts.append(
        "\nRespond with JSON. Either recommend from pre-filtered places (done=true) "
        "or emit ONE tool_call (done=false). Include 'actions' array for web/call "
        "buttons on recommended places."
    )
    return "\n".join(parts)


# ──────────────────────────────────────────────────────────────
# Normalization / repair helpers
# ──────────────────────────────────────────────────────────────

def _snake_key_aliases() -> Dict[str, str]:
    return {
        "corridorKey": "corridor_key",
        "corridorId": "corridor_key",
        "routeKey": "route_key",
        "limitPerSample": "limit_per_sample",
        "intervalKm": "interval_km",
        "radiusM": "radius_m",
        "bufferKm": "buffer_km",
        "minLng": "minLng",
        "minLat": "minLat",
        "maxLng": "maxLng",
        "maxLat": "maxLat",
    }


def _repair_req(tool: GuideToolName, req: Dict[str, Any], ctx: GuideContext) -> Dict[str, Any]:
    if not isinstance(req, dict):
        return {}

    aliases = _snake_key_aliases()
    fixed: Dict[str, Any] = {aliases.get(k, k): v for k, v in req.items()}

    if tool == "places_corridor":
        if not fixed.get("corridor_key") and ctx.corridor_key:
            fixed["corridor_key"] = ctx.corridor_key
        # Auto-inject geometry for true corridor search
        if not fixed.get("geometry") and ctx.geometry and len(ctx.geometry) > 10:
            fixed["geometry"] = ctx.geometry
        cats = fixed.get("categories")
        if cats is None:
            fixed["categories"] = []
        elif isinstance(cats, str):
            fixed["categories"] = [cats]
        if "limit" in fixed and fixed["limit"] is not None:
            try:
                fixed["limit"] = int(fixed["limit"])
            except Exception:
                pass

    elif tool == "places_suggest":
        if not fixed.get("geometry") and ctx.geometry:
            fixed["geometry"] = ctx.geometry
        for k in ("interval_km", "radius_m", "limit_per_sample"):
            if k in fixed and fixed[k] is not None:
                try:
                    fixed[k] = int(fixed[k])
                except Exception:
                    pass
        cats = fixed.get("categories")
        if cats is None:
            fixed["categories"] = []
        elif isinstance(cats, str):
            fixed["categories"] = [cats]

    elif tool == "places_search":
        if "limit" in fixed and fixed["limit"] is not None:
            try:
                fixed["limit"] = int(fixed["limit"])
            except Exception:
                pass
        cats = fixed.get("categories")
        if cats is None:
            fixed["categories"] = []
        elif isinstance(cats, str):
            fixed["categories"] = [cats]
        if fixed.get("query") == "":
            fixed.pop("query", None)

        # Auto-inject user location as center if no spatial constraint given
        if (
            not fixed.get("center")
            and not fixed.get("bbox")
            and ctx.progress
            and ctx.progress.user_lat
        ):
            if not fixed.get("query"):
                fixed["center"] = {"lat": ctx.progress.user_lat, "lng": ctx.progress.user_lng}
                if not fixed.get("radius_m"):
                    fixed["radius_m"] = 50000

    return fixed


def _validate_tool_req(tool: GuideToolName, req: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        if tool == "places_search":
            PlacesRequest.model_validate(req)
        elif tool == "places_corridor":
            CorridorPlacesRequest.model_validate(req)
        elif tool == "places_suggest":
            PlacesSuggestRequest.model_validate(req)
        else:
            return False, f"Unknown tool: {tool}"
        return True, ""
    except ValidationError as ve:
        return False, str(ve)


def _normalize_model_output(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"assistant": "", "actions": [], "tool_calls": [], "done": False}

    raw.setdefault("assistant", "")
    raw.setdefault("actions", [])
    raw.setdefault("tool_calls", [])
    raw.setdefault("done", False)

    if not isinstance(raw.get("tool_calls"), list):
        raw["tool_calls"] = []
    if not isinstance(raw.get("actions"), list):
        raw["actions"] = []

    # Ensure tool_call ids exist
    norm_calls: List[Dict[str, Any]] = []
    for i, tc in enumerate(raw["tool_calls"]):
        if not isinstance(tc, dict):
            continue
        tc.setdefault("id", f"tc_{i}_{uuid.uuid4().hex[:8]}")
        norm_calls.append(tc)
    raw["tool_calls"] = norm_calls

    # Sanitize actions
    norm_actions: List[Dict[str, Any]] = []
    for a in raw["actions"]:
        if not isinstance(a, dict):
            continue
        at = a.get("type")
        label = a.get("label")
        if at not in ("web", "call"):
            continue
        if not isinstance(label, str) or not label.strip():
            continue
        keep = {
            "type": at,
            "label": label.strip()[:80],
            "place_id": a.get("place_id"),
            "place_name": a.get("place_name"),
            "url": a.get("url"),
            "tel": a.get("tel"),
        }
        norm_actions.append(keep)
    raw["actions"] = norm_actions

    return raw


# ──────────────────────────────────────────────────────────────
# Action normalization / dedupe
# ──────────────────────────────────────────────────────────────

_re_digits = re.compile(r"[^\d+]+")


def _canon_url(u: str) -> Optional[str]:
    if not isinstance(u, str):
        return None
    s = u.strip()
    if not s:
        return None
    if s.startswith("www."):
        s = "https://" + s
    if not (s.startswith("http://") or s.startswith("https://")):
        s = "https://" + s
    s = s.rstrip(").,;!\"'")
    return s


def _canon_tel(t: str) -> Optional[str]:
    if not isinstance(t, str):
        return None
    s = t.strip()
    if not s:
        return None
    s = _re_digits.sub("", s)
    if len(s) < 7:
        return None
    return s


def _dedupe_actions(actions: List[GuideAction]) -> List[GuideAction]:
    seen: set[str] = set()
    out: List[GuideAction] = []
    for a in actions:
        if a.type == "web":
            cu = _canon_url(a.url or "")
            if not cu:
                continue
            key = f"web::{cu}"
            if key in seen:
                continue
            seen.add(key)
            out.append(GuideAction(**{**a.model_dump(), "url": cu, "tel": None}))
        elif a.type == "call":
            ct = _canon_tel(a.tel or "")
            if not ct:
                continue
            key = f"call::{ct}"
            if key in seen:
                continue
            seen.add(key)
            out.append(GuideAction(**{**a.model_dump(), "tel": ct, "url": None}))
    return out


# ──────────────────────────────────────────────────────────────
# Service
# ──────────────────────────────────────────────────────────────

class GuideService:
    def __init__(self) -> None:
        self._api_key = settings.openai_api_key
        self._model = settings.openai_model
        self._base = settings.openai_base_url.rstrip("/")
        self._timeout = float(
            getattr(settings, "guide_timeout_s", None)
            or getattr(settings, "explore_timeout_s", None)
            or 25.0
        )

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
            if item.get("type") != "message":
                continue
            for c in item.get("content", []) or []:
                if c.get("type") == "output_text":
                    out_text = c.get("text")
                    break
            if out_text:
                break

        if not out_text:
            raise RuntimeError(
                f"Guide LLM: missing output_text. Raw: {json.dumps(data)[:2000]}"
            )

        try:
            raw = json.loads(out_text)
        except Exception as e:
            raise RuntimeError(f"Guide LLM: invalid JSON: {e}. text={out_text[:400]}")

        norm = _normalize_model_output(raw)

        # Tool call validation: allow ONE valid tool call
        tool_calls = norm.get("tool_calls") or []
        if tool_calls:
            tc0 = tool_calls[0]
            tool = tc0.get("tool")
            req_obj = tc0.get("req")
            tc_id = tc0.get("id") or f"tc_{uuid.uuid4().hex[:8]}"

            if tool not in ("places_search", "places_corridor", "places_suggest"):
                norm["assistant"] = (
                    (norm.get("assistant") or "")
                    + "\n\n(I couldn't pick a valid tool — let me try a different approach.)"
                )
                norm["tool_calls"] = []
                norm["done"] = False
            else:
                fixed_req = _repair_req(
                    tool,
                    req_obj if isinstance(req_obj, dict) else {},
                    req.context,
                )
                ok, _err = _validate_tool_req(tool, fixed_req)
                if not ok:
                    norm["assistant"] = (
                        (norm.get("assistant") or "")
                        + "\n\n(Search parameters didn't validate — let me try differently.)"
                    )
                    norm["tool_calls"] = []
                    norm["done"] = False
                else:
                    norm["tool_calls"] = [{"id": str(tc_id), "tool": tool, "req": fixed_req}]

        # Validate + dedupe actions through Pydantic
        try:
            resp = GuideTurnResponse.model_validate(norm)
        except ValidationError:
            safe = dict(norm)
            safe["actions"] = []
            resp = GuideTurnResponse.model_validate(safe)

        resp.actions = _dedupe_actions(resp.actions)
        return resp