# app/services/guide.py
"""
Roam Guide - Premium AI road trip companion for Australia.
Powered by DeepSeek-V3 via OpenAI-compatible /chat/completions API.

Design principles:
  1. Safety first: fuel range, fatigue, wildlife, weather, road conditions
  2. Regionally aware: hyper-local knowledge injected by GPS region
  3. Seasonally intelligent: month-based wet/dry/fire/whale/wildflower seasons
  4. Vehicle-aware: sedan vs 4WD vs caravan/RV changes all recommendations
  5. Phase-adaptive: departing → cruising → midway → home stretch → arriving
  6. Concise: mobile screen, one-handed, glancing while passenger reads aloud
  7. Proactive: nudges about fuel, fatigue, wildlife, weather without being asked
  8. Never invents: only recommends from data or tool results
  9. Culturally respectful: indigenous heritage, local history, community sensitivity
  10. Companion-aware: adapts for solo, couple, family, group, dog, budget traveller
  11. Intent-driven: infers WHAT the user really wants and delivers it completely
  12. Data-rich: feeds the model full stop details, hours, phones, distances
"""
from __future__ import annotations

import json
import math
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
from app.services.guide_data import (
    get_regions,
    get_towns,
    get_seasonal_blocks,
    get_vehicle_profiles,
    get_companion_types,
    get_intent_definitions,
    get_time_context_blocks,
    get_deep_knowledge,
)
from app.services.guide_search import web_search


# ══════════════════════════════════════════════════════════════
# REGIONAL INTELLIGENCE ENGINE
# ══════════════════════════════════════════════════════════════

def _detect_regions(lat: float, lng: float) -> List[Dict[str, Any]]:
    """Return all matching regions for the user's coordinates."""
    matches: List[Dict[str, Any]] = []
    for r in get_regions():
        bb = r["bbox"]
        if bb["s"] <= lat <= bb["n"] and bb["w"] <= lng <= bb["e"]:
            matches.append(r)
    return matches


# ══════════════════════════════════════════════════════════════
# SEASONAL INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _seasonal_knowledge(month: int, lat: float, lng: float) -> str:
    tropical = lat > -23.5  # above Tropic of Capricorn
    blocks: List[str] = []

    for entry in get_seasonal_blocks():
        cond = entry["condition"]

        # Check month
        if month not in cond.get("months", []):
            continue

        # Check tropical requirement
        if "tropical" in cond:
            if cond["tropical"] != tropical:
                continue

        # Check lat/lng bounds
        if "lat_gt" in cond and lat <= cond["lat_gt"]:
            continue
        if "lat_lt" in cond and lat >= cond["lat_lt"]:
            continue
        if "lng_lt" in cond and lng >= cond["lng_lt"]:
            continue

        blocks.append(entry["text"])

    return "\n\n".join(blocks) if blocks else ""


# ══════════════════════════════════════════════════════════════
# VEHICLE INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _vehicle_context(profile: str | None) -> str:
    p = (profile or "drive").lower()
    profiles = get_vehicle_profiles()

    for key, data in profiles.items():
        if p in data.get("aliases", []):
            return data["text"]

    # Default to standard
    return profiles["standard"]["text"]


# ══════════════════════════════════════════════════════════════
# COMPANION-MODE DETECTION
# ══════════════════════════════════════════════════════════════

def _companion_hints(thread: List[GuideMsg], stops: List[Dict[str, Any]]) -> str:
    all_text = " ".join(m.content.lower() for m in thread if m.role == "user")
    hints: List[str] = []

    for comp in get_companion_types():
        if any(t in all_text for t in comp["terms"]):
            hints.append(comp["advice"])

    return "\n\n".join(hints) if hints else ""


# ══════════════════════════════════════════════════════════════
# INTENT DETECTION
# Analyse the user's last message to determine what they really want
# ══════════════════════════════════════════════════════════════

def _detect_named_location(thread: List[GuideMsg], user_lat: float | None, user_lng: float | None) -> str:
    """
    Detect when the user mentions a specific Australian town/location that is
    likely DIFFERENT from their current GPS position. Returns a warning string
    to inject into the system prompt so the model doesn't conflate the two.
    """
    if not thread:
        return ""

    last_user = next(
        (m.content.lower() for m in reversed(thread) if m.role == "user"), ""
    )
    if not last_user:
        return ""

    towns = get_towns()

    # Find all mentioned towns, prefer longer names first (e.g. "sunshine coast" > "coast")
    found: List[Tuple[str, Tuple[float, float]]] = []
    for town, coords in sorted(towns.items(), key=lambda x: -len(x[0])):
        if town in last_user:
            found.append((town, coords))
            break  # only need the first/most specific match

    if not found:
        return ""

    town_key, (town_lat, town_lng) = found[0]
    town_display = town_key.title()

    # Check if user is already near this town (within ~30km) — if so, no mismatch
    if user_lat is not None and user_lng is not None:
        dist_deg = math.sqrt((user_lat - town_lat) ** 2 + (user_lng - town_lng) ** 2)
        dist_km_approx = dist_deg * 111.0
        if dist_km_approx < 30:
            return ""  # User is near the mentioned town — no mismatch

    lines = [
        "⚠️ LOCATION MISMATCH ALERT ⚠️",
        f"The user mentioned: {town_display}",
    ]
    if user_lat is not None and user_lng is not None:
        lines.append(
            f"The user's CURRENT GPS position is ({user_lat:.4f}, {user_lng:.4f}), "
            f"which is NOT near {town_display}."
        )
    lines += [
        f"The PRE-LOADED PLACES in this context are near the user's current GPS location — NOT in {town_display}.",
        f"DO NOT present pre-loaded places as if they are in {town_display}.",
        f"DO NOT say 'You're in {town_display}' or assume the user is there.",
        f"",
        f"If the user is asking about things IN {town_display}, you MUST use places_search first:",
        f'  Tool call: {{"tool": "places_search", "req": {{"center": {{"lat": {town_lat}, "lng": {town_lng}}}, "radius_m": 5000, "categories": ["restaurant", "cafe", "pub", "bakery", "fast_food"], "limit": 20}}}}',
        f"  Then respond ONLY using those search results. NEVER invent place names.",
        f"  Say something like: 'Searching for lunch spots in {town_display}...' while you search.",
    ]
    return "\n".join(lines)


def _detect_intent(thread: List[GuideMsg]) -> str:
    """Detect the user's intent from the latest message to guide response style."""
    if not thread:
        return ""

    last_user = next(
        (m.content.lower() for m in reversed(thread) if m.role == "user"), ""
    )
    if not last_user:
        return ""

    intents: List[str] = []
    intent_defs = get_intent_definitions()

    for defn in intent_defs:
        if defn["id"] == "general":
            continue  # handled as fallback below
        if any(w in last_user for w in defn["terms"]):
            intents.append(defn["guidance"])

    # General fallback
    if not intents:
        general = next((d for d in intent_defs if d["id"] == "general"), None)
        if general:
            intents.append(general["guidance"])

    return "\n".join(intents)


# ══════════════════════════════════════════════════════════════
# PROACTIVE SAFETY & NUDGE ENGINE
# ══════════════════════════════════════════════════════════════

def _proactive_nudges(
    progress: TripProgress | None,
    total_km: float | None,
    ctx: GuideContext,
    relevant_places: List[WirePlace],
) -> str:
    if not progress:
        return ""

    nudges: List[str] = []

    # Fuel nudge
    fuel_ahead = [
        p for p in relevant_places
        if p.ahead and p.category in ("fuel", "ev_charging") and p.dist_km is not None
    ]
    fuel_ahead.sort(key=lambda p: p.dist_km or 9999)

    if not fuel_ahead:
        nudges.append(
            "⛽ FUEL WARNING: No fuel stations detected in pre-loaded data. "
            "If topic is fuel or if the user seems to be in a remote stretch, proactively flag this "
            "and offer to search. Use places_corridor to find fuel stations."
        )
    elif fuel_ahead[0].dist_km and fuel_ahead[0].dist_km > 150:
        nf = fuel_ahead[0]
        nudges.append(
            f"⛽ FUEL GAP: Next fuel is {nf.name} at {nf.dist_km:.0f}km ahead "
            f"({'in ' + nf.locality if nf.locality else ''}). That's a significant gap. "
            f"Mention it naturally if the conversation allows."
        )

    # Fatigue nudge
    km_driven = progress.km_from_start
    drive_time_h: float | None = None
    if total_km and total_km > 0 and ctx.total_duration_s:
        fraction = min(km_driven / total_km, 1.0)
        drive_time_h = (fraction * ctx.total_duration_s) / 3600

    if drive_time_h is not None and drive_time_h >= 2.0:
        nudges.append(
            f"😴 FATIGUE: Estimated {drive_time_h:.1f}h driving so far. "
            f"Australian road safety: stop every 2 hours minimum. "
            f"If they ask about anything, gently weave in a suggestion to rest — "
            f"a bakery stop, rest area, or scenic pull-off."
        )
    elif km_driven > 250:
        nudges.append(
            f"😴 DISTANCE CHECK: {km_driven:.0f}km covered. "
            f"Fatigue is real — suggest a break opportunity if one arises."
        )

    # Wildlife timing
    if progress.local_time_iso:
        try:
            hour = datetime.fromisoformat(
                progress.local_time_iso.replace("Z", "+00:00")
            ).hour
            if hour in (4, 5, 6, 17, 18, 19):
                nudges.append(
                    "🦘 WILDLIFE ACTIVE: Dawn/dusk — kangaroos, wombats, echidnas on roads now. "
                    "Weave in a note about slowing down and watching roadsides if appropriate."
                )
            elif hour >= 20 or hour < 4:
                nudges.append(
                    "🌙 NIGHT DRIVING: Wildlife extremely active on rural roads at night. "
                    "Kangaroos are invisible until headlights hit their eyes at close range. "
                    "Strongly suggest stopping or at least reducing speed significantly. "
                    "This is the leading cause of serious crashes in rural Australia."
                )
        except Exception:
            pass

    # Arriving soon
    if progress.km_remaining is not None and progress.km_remaining < 30:
        nudges.append(
            f"📍 ARRIVING SOON: {progress.km_remaining:.0f}km to next stop. "
            f"Offer useful arrival info — parking, what's nearby, any last-minute needs."
        )

    return "\n".join(nudges) if nudges else ""


# ══════════════════════════════════════════════════════════════
# STOP DETAIL FORMATTER
# Feed full stop context to the model — not just lat/lng
# ══════════════════════════════════════════════════════════════

def _format_stops_rich(
    stops: List[Dict[str, Any]],
    visited_ids: List[str] | None = None,
    progress: TripProgress | None = None,
) -> str:
    visited = set(visited_ids or [])
    lines: List[str] = []
    current_idx = progress.current_stop_idx if progress else -1

    for i, s in enumerate(stops):
        sid = s.get("id", f"p{i}")
        stype = s.get("type", "poi")
        name = s.get("name", "Unnamed")
        lat = s.get("lat", 0)
        lng = s.get("lng", 0)

        if sid in visited:
            marker = "✅"
        elif i == current_idx:
            marker = "📍"
        else:
            marker = "⬜"

        line = f"  {marker} [{i}] {name} ({stype}) — {lat:.4f},{lng:.4f}"

        # Extra stop metadata if available
        if s.get("address"):
            line += f" | {s['address']}"
        if s.get("notes"):
            line += f" | note: {s['notes']}"
        if s.get("eta"):
            line += f" | ETA: {s['eta']}"

        lines.append(line)

    return "\n".join(lines) if lines else "  (no stops planned)"


# ══════════════════════════════════════════════════════════════
# PLACES FORMATTER
# Rich, detailed place descriptions that let the model make real recommendations
# ══════════════════════════════════════════════════════════════

def _format_relevant_places(places: List[WirePlace]) -> str:
    if not places:
        return "  (none pre-loaded — use tools to search if needed)"

    # Group by category for easier model comprehension
    by_cat: Dict[str, List[WirePlace]] = {}
    for p in places:
        by_cat.setdefault(p.category, []).append(p)

    lines: List[str] = []
    # Safety categories first
    cat_order = [
        "fuel", "ev_charging", "rest_area", "water", "dump_point",
        "mechanic", "hospital", "pharmacy", "toilet",
        "bakery", "cafe", "restaurant", "fast_food", "pub", "bar",
        "grocery", "town", "atm", "laundromat",
        "camp", "hotel", "motel", "hostel",
        "viewpoint", "waterfall", "swimming_hole", "beach",
        "national_park", "hiking", "picnic", "hot_spring",
        "playground", "pool", "zoo", "theme_park",
        "visitor_info", "museum", "gallery", "heritage",
        "winery", "brewery", "attraction", "market", "park",
    ]

    shown_cats = [c for c in cat_order if c in by_cat]
    shown_cats += [c for c in by_cat if c not in cat_order]

    for cat in shown_cats:
        cat_places = sorted(
            by_cat[cat], key=lambda p: (not p.ahead, p.dist_km or 9999)
        )
        lines.append(f"\n  [{cat.upper().replace('_', ' ')}]")
        for p in cat_places:
            parts: List[str] = [f"    • {p.name}"]
            if p.locality:
                parts.append(p.locality)
            if p.dist_km is not None:
                direction = "ahead" if p.ahead else "BEHIND (already passed)"
                parts.append(f"{p.dist_km:.1f}km {direction}")
            if p.hours:
                parts.append(f"open: {p.hours[:50]}")
            if p.phone:
                parts.append(f"ph: {p.phone}")
            if p.website:
                parts.append(f"web: {p.website}")
            lines.append(" | ".join(parts))

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
# TIME CONTEXT
# ══════════════════════════════════════════════════════════════

def _time_context(local_iso: str | None) -> Tuple[str, str]:
    if not local_iso:
        return "Unknown time", ""
    try:
        dt = datetime.fromisoformat(local_iso.replace("Z", "+00:00"))
        hour = dt.hour
    except Exception:
        return "Unknown time", ""

    for block in get_time_context_blocks():
        if hour in block["hours"]:
            label = f"{block['label']} ({hour:02d}:{dt.minute:02d})"
            return label, block["advice"]

    return f"({hour:02d}:{dt.minute:02d})", ""


def _trip_phase(progress: TripProgress | None, total_km: float | None) -> str:
    if not progress or not total_km or total_km <= 0:
        return "planning"
    pct = progress.km_from_start / total_km
    if pct < 0.05:
        return "departing"
    elif pct < 0.35:
        return "early_cruise"
    elif pct < 0.65:
        return "midway"
    elif pct < 0.90:
        return "home_stretch"
    else:
        return "arriving"


def _format_speed(speed_mps: float | None) -> str:
    if speed_mps is None or speed_mps < 0:
        return "unknown"
    kmh = speed_mps * 3.6
    return f"{kmh:.0f} km/h"


def _format_conditions(ctx: GuideContext) -> str:
    cond_parts: List[str] = []
    ts = ctx.traffic_summary
    if ts and ts.get("total", 0) > 0:
        cond_parts.append(f"⚠️ Traffic: {ts['total']} events on/near route")
        for s in ts.get("sample", [])[:4]:
            severity = s.get("severity", "")
            sev_str = f" [{severity.upper()}]" if severity and severity != "unknown" else ""
            cond_parts.append(
                f"  • {s.get('type', 'event')}{sev_str}: {s.get('headline', '')[:120]}"
            )
        cond_parts.append(
            "  → Mention relevant traffic issues to the user if they affect the route ahead."
        )
    hs = ctx.hazards_summary
    if hs and hs.get("total", 0) > 0:
        cond_parts.append(f"🔴 Hazards/Weather: {hs['total']} active warnings")
        for h in hs.get("sample", [])[:4]:
            priority = h.get("effective_priority")
            prio_str = f" [priority:{priority:.1f}]" if priority else ""
            kind = h.get("kind", "hazard")
            headline = h.get("headline", "")[:120]
            cond_parts.append(f"  • {kind}{prio_str}: {headline}")
        cond_parts.append(
            "  → IMPORTANT: Warn the user about relevant hazards, especially fires, floods, and severe weather."
        )
    return "\n".join(cond_parts) if cond_parts else "No active traffic, weather, or hazard alerts on route."


# ══════════════════════════════════════════════════════════════
# SYSTEM PROMPT BUILDER
# ══════════════════════════════════════════════════════════════

def _build_system_prompt(
    ctx: GuideContext,
    relevant_places: List[WirePlace],
    thread: List[GuideMsg] | None = None,
) -> str:
    total_km = (ctx.total_distance_m or 0) / 1000 if ctx.total_distance_m else None
    total_h = int((ctx.total_duration_s or 0) // 3600) if ctx.total_duration_s else None
    total_m = int(((ctx.total_duration_s or 0) % 3600) // 60) if ctx.total_duration_s else None

    route_info = ""
    if total_km is not None:
        route_info = f"{total_km:.0f} km"
        if total_h is not None:
            route_info += f", ~{total_h}h{total_m:02d}m estimated drive time"

    progress = ctx.progress
    visited_ids = progress.visited_stop_ids if progress else []
    stops_str = _format_stops_rich(ctx.stops, visited_ids, progress)

    # Position & progress
    pos_str = "Location unavailable."
    if progress:
        stop_name = "unknown"
        if 0 <= progress.current_stop_idx < len(ctx.stops):
            stop_name = ctx.stops[progress.current_stop_idx].get("name", "unnamed")

        speed_str = _format_speed(progress.user_speed_mps)
        heading_str = f", heading {progress.user_heading:.0f}°" if progress.user_heading is not None else ""

        pos_str = (
            f"({progress.user_lat:.5f}, {progress.user_lng:.5f}) "
            f"±{progress.user_accuracy_m:.0f}m accuracy"
            f"{heading_str}, {speed_str}\n"
            f"  Near stop [{progress.current_stop_idx}]: \"{stop_name}\"\n"
            f"  Progress: {progress.km_from_start:.0f} km done, {progress.km_remaining:.0f} km to next stop"
        )
        if total_km:
            total_pct = min(100, int(progress.km_from_start / total_km * 100))
            pos_str += f" ({total_pct}% of total trip)"

    time_label, driving_advice = _time_context(
        progress.local_time_iso if progress else None
    )

    phase = _trip_phase(progress, total_km)
    phase_guidance = {
        "planning": (
            "Trip is being planned — not yet underway. Help with route ideas, must-see stops, "
            "fuel planning, best time of year, accommodation options, and timing advice. "
            "Be inspiring AND practical. Mention the highlights that make this route special. "
            "Flag any seasonal concerns (wet season closures, fire risk, school holidays). "
            "If the route is long (500km+), suggest good overnight stops."
        ),
        "departing": (
            "Just departed! Set the tone — adventure ahead. "
            "Mention the first interesting thing coming up on the route. "
            "If they haven't asked anything, volunteer: the first good bakery, a scenic detour, "
            "or a heads-up about the road ahead. This is where you establish trust as a guide."
        ),
        "early_cruise": (
            "Settled into the drive — fresh and open to suggestions. "
            "This is the GOLDEN WINDOW for discovery. Suggest interesting detours, scenic lookouts, "
            "local bakeries with legendary pies, swimming holes, or hidden gems slightly off-route. "
            "They have time, energy, and curiosity. Make this stretch memorable."
        ),
        "midway": (
            "Halfway through. Fuel, food, and a proper break are likely needed. "
            "Proactively check fuel gap and suggest food. A country bakery or pub lunch stop "
            "can be the highlight of the day. If they've been driving 2+ hours, "
            "a 20-minute stop at a good spot recharges everything."
        ),
        "home_stretch": (
            "Getting closer. Driver may be tiring — watch for fatigue signals. "
            "Prioritise: accommodation if overnight, fuel if tank is getting low, "
            "last-chance supplies (grocery, bottled water) before remote stretches. "
            "Less suggesting detours — more helping them arrive safely and comfortably."
        ),
        "arriving": (
            "Nearly there — the last few kilometres. Help with arrival logistics: "
            "where to park (especially caravans), what's nearby for dinner/supplies, "
            "check-in times if they have accommodation. "
            "If it's a town they haven't been to, give them the 30-second local brief."
        ),
    }
    phase_text = phase_guidance.get(phase, "")

    conditions = _format_conditions(ctx)

    # Direction rule
    direction_rule = ""
    if progress and progress.km_from_start > 0:
        direction_rule = (
            f"⚠️ AHEAD ONLY: User is at {progress.km_from_start:.1f}km along the route. "
            f"NEVER suggest places they've already passed. Only recommend places AHEAD of them."
        )

    places_block = _format_relevant_places(relevant_places)

    # Tool availability
    tool_hints: List[str] = []
    if ctx.corridor_key:
        tool_hints.append(f"✅ corridor_key available ({ctx.corridor_key}) — places_corridor works")
    else:
        tool_hints.append("⚠️ No corridor_key — use places_search instead of places_corridor")
    if ctx.geometry:
        tool_hints.append(f"✅ Route geometry ({len(ctx.geometry)} chars) — places_suggest works")
    else:
        tool_hints.append("⚠️ No geometry — places_suggest not available")
    tools_info = "\n".join(f"  {h}" for h in tool_hints)

    # Search availability
    search_available = bool(settings.tavily_api_key or settings.google_cse_api_key)

    # Intelligence layers
    regional_block = ""
    if progress:
        regions = _detect_regions(progress.user_lat, progress.user_lng)
        if regions:
            regional_block = "\n\n".join(
                f"📍 {r['name']}:\n{r['knowledge']}"
                for r in regions[:2]
            )
    elif ctx.stops:
        first = ctx.stops[0]
        lat = first.get("lat", 0)
        lng = first.get("lng", 0)
        if lat and lng:
            regions = _detect_regions(lat, lng)
            if regions:
                regional_block = "\n\n".join(
                    f"📍 {r['name']}:\n{r['knowledge']}"
                    for r in regions[:2]
                )

    seasonal_block = ""
    try:
        if progress and progress.local_time_iso:
            dt = datetime.fromisoformat(progress.local_time_iso.replace("Z", "+00:00"))
            seasonal_block = _seasonal_knowledge(dt.month, progress.user_lat, progress.user_lng)
        else:
            now = datetime.now()
            lat = ctx.stops[0].get("lat", -27.5) if ctx.stops else -27.5
            lng = ctx.stops[0].get("lng", 153.0) if ctx.stops else 153.0
            seasonal_block = _seasonal_knowledge(now.month, lat, lng)
    except Exception:
        pass

    vehicle_block = _vehicle_context(ctx.profile)
    companion_block = _companion_hints(thread or [], ctx.stops)
    nudge_block = _proactive_nudges(progress, total_km, ctx, relevant_places)
    intent_block = _detect_intent(thread or [])
    location_mismatch_block = _detect_named_location(
        thread or [],
        progress.user_lat if progress else None,
        progress.user_lng if progress else None,
    )

    sections: List[str] = []

    sections.append(
        """You are Roam Guide — the definitive AI road trip companion for Australia.

You're the deeply knowledgeable mate riding shotgun who's driven every highway, camped every free camp, knows every legendary bakery, has swum in every gorge pool, and has stories about every pub from Broome to Byron. You combine the practical knowledge of a seasoned outback traveller with the warmth of a great companion.

You have THREE jobs that matter above all:
1. KEEP PEOPLE SAFE — fuel range, fatigue, wildlife, weather, crocs, heat, road conditions. Safety overrides everything.
2. HELP THEM FIND GENUINELY GREAT PLACES — not generic listings, but real recommendations with character. The bakery with the famous pepper steak pie, the swimming hole locals actually go to, the pub with the coldest beer and $22 counter meals.
3. BE THE COMPANION that makes a road trip feel like an adventure — not an AI, a mate.

You have access to real data: pre-loaded places with names, distances, opening hours, and contact info. USE IT. Be specific.
- DON'T say "there are some bakeries nearby"
- DO say "Bourke Street Bakery in Harrisville is 12km ahead — get the pepper steak pie, they close at 3pm"

DEPTH OF ANSWER — match the complexity of the question:
- Simple factual: "Where's the next fuel?" → 1–2 sentences, immediate answer
- Discovery: "What's worth seeing?" → 3–8 sentences, multiple options with colour and detail
- Planning: "Should I take the inland or coastal route?" → Full comparison with pros, cons, distances, highlights
- Safety: "Is it safe to drive to X at night?" → As long as it needs to be. Don't shortchange safety.

This is a mobile screen. Lead with the answer. Every word must earn its place — but don't be shallow when depth is warranted."""
    )

    sections.append(f"""═══ TRIP CONTEXT ═══
Trip: {ctx.label or 'Unnamed trip'} | Profile: {ctx.profile or 'drive'} | {route_info}
Phase: {phase} — {phase_text}

Stops:
{stops_str}""")

    sections.append(f"""═══ LIVE STATUS ═══
Position: {pos_str}
Time: {time_label}
{driving_advice}

═══ ROAD CONDITIONS & WEATHER ═══
{conditions}
NOTE: If the user asks about current road conditions, closures, or flooding that isn't covered above, use web_search to get the latest info. The above is from official state traffic feeds and BOM weather warnings but may not cover every road.

{vehicle_block}""")

    if direction_rule:
        sections.append(direction_rule)

    sections.append(f"""═══ PRE-LOADED PLACES ═══
These places are near/ahead on the route. Recommend from this list when possible.
{places_block}""")

    # Tools section — includes web_search if available
    web_search_line = ""
    if search_available:
        web_search_line = (
            '\n  web_search     — search the web for current, real-world information.'
            '\n    ALWAYS SEARCH for:'
            '\n      • Current road conditions, closures, or flooding'
            '\n      • Specific business details you don\'t have in pre-loaded data (menus, prices, reviews)'
            '\n      • "Is [road/attraction] open right now?" or "best [X] in [town] 2026"'
            '\n      • Recently opened businesses, events, festivals, markets'
            '\n      • Anything the user asks that is time-sensitive or specific to a business/location you can\'t answer from loaded knowledge'
            '\n    NEVER SEARCH for:'
            '\n      • General Australian knowledge (regions, seasons, wildlife, driving tips) — you already know this'
            '\n      • Things answerable from pre-loaded places data'
            '\n    Query tips: Be specific. Include "Australia" or the state. Include the year if time-relevant.'
            '\n    Example: {"tool": "web_search", "req": {"query": "Gibb River Road conditions March 2026 open or closed"}}'
        )

    sections.append(f"""═══ TOOLS ═══
{tools_info}
  places_search   — search by bbox, center+radius, or text query. Use for "near me", "in [town]".
  places_corridor — search the full route corridor (needs corridor_key). Best for "along my route".
  places_suggest  — sample highlights at intervals along route (needs geometry). Best for "what are the highlights".{web_search_line}

Category vocabulary: fuel, ev_charging, rest_area, toilet, water, dump_point, mechanic, hospital, pharmacy, grocery, town, atm, laundromat, bakery, cafe, restaurant, fast_food, pub, bar, camp, hotel, motel, hostel, viewpoint, waterfall, swimming_hole, beach, national_park, hiking, picnic, hot_spring, playground, pool, zoo, theme_park, visitor_info, museum, gallery, heritage, winery, brewery, attraction, market, park""")

    if location_mismatch_block:
        sections.append(f"═══ CRITICAL: LOCATION MISMATCH ═══\n{location_mismatch_block}")

    if intent_block:
        sections.append(f"═══ DETECTED INTENT ═══\n{intent_block}")

    if nudge_block:
        sections.append(f"═══ PROACTIVE NUDGES (weave in naturally, don't nag) ═══\n{nudge_block}")

    if seasonal_block:
        sections.append(f"═══ SEASONAL CONDITIONS ═══\n{seasonal_block}")

    if regional_block:
        sections.append(f"═══ REGIONAL KNOWLEDGE ═══\n{regional_block}")

    if companion_block:
        sections.append(f"═══ TRAVEL PARTY CONTEXT ═══\n{companion_block}")

    deep_knowledge = get_deep_knowledge()

    sections.append(
        f"""═══ RESPONSE RULES ═══
1. SPECIFIC — Use real names, distances, and details from the data. "BP Longreach, 3.2km ahead, open 24hr" beats "there's fuel nearby". Include opening hours and phone when you have them.
2. VIVID — One memorable detail per recommendation makes it feel like local knowledge, not a database query. "Famous for their curry pie — they sell 500 a week" or "the pool at the bottom is freezing cold and absolutely worth it". Make them WANT to stop.
3. ACTIONS — For EVERY place you specifically name and recommend, emit action buttons so the user can interact with it. This is critical — a recommendation without actions is useless on the road.
   REQUIRED for every named place:
   - Save: {{"type":"save","label":"PlaceName","place_id":"id","place_name":"PlaceName","lat":-27.5,"lng":153.0,"category":"cafe","description":"A 1-2 sentence description: what makes it worth stopping, what to order, any practical tips."}}
     The description should read like a local's recommendation — vivid, specific, helpful. Include prices, signature items, or practical details when you have them.
   - Map: {{"type":"map","label":"Map · PlaceName","place_id":"id","place_name":"PlaceName","lat":-27.5,"lng":153.0,"category":"cafe"}}
   OPTIONAL but include when data is available:
   - Web: {{"type":"web","label":"Website · PlaceName","place_id":"id","place_name":"name","url":"https://..."}}
   - Call: {{"type":"call","label":"Call PlaceName","place_id":"id","place_name":"name","tel":"0400..."}}
   Action order per place: save, map, web (if available), call (if available). Max 4 actions per place, max 5 places with actions per response.
4. SAFETY FIRST — Fuel, fatigue, wildlife, and weather safety always override discovery recommendations. But mention safety once per topic — don't nag. Be matter-of-fact: "Next fuel is 180km — I'd fill up at Shell Cloncurry before you leave."
5. NEVER INVENT — Only name places from the pre-loaded list, tool results, or web search results. NEVER fabricate business names, hours, phone numbers, websites, or prices. If you don't have real data, say so and offer to search. A search that returns real results is always better than an invented recommendation.
6. LOCATION HONESTY — Never tell a user "You're in [Town]" when their GPS is elsewhere. If they ask about a specific town, search for places there — don't reuse pre-loaded places from their current location.
7. DONE FLAG — done=true when giving a final answer. done=false ONLY when emitting a tool_call.
8. ONE TOOL PER TURN — Maximum one tool_call. Include brief explanatory text ("Searching for lunch spots in Byron Bay...").
9. LOCAL COLOUR — Weave in regional knowledge naturally. "That's the Tablelands — the waterfall circuit is worth a 2-hour detour. Millaa Millaa Falls is the postcard shot."
10. HONEST GAPS — If you don't have data on something, say so and offer to search. "I don't have current details on that — want me to search?"
11. INDIGENOUS RESPECT — At indigenous sites, acknowledge Traditional Owners. Note permit requirements and access restrictions clearly and practically.
12. WEATHER-AWARE — Don't recommend swimming holes in stinger season. Don't suggest unsealed roads in the wet. Cross-reference your seasonal knowledge with the current month.
13. PROACTIVE DISCOVERY — When the user hasn't asked a specific question but is mid-trip, volunteer something interesting: "You're about to pass through Childers — if you like macadamias, the Macadamia House is 2km off the highway and worth the detour." Don't do this every message — once every few exchanges when you have something genuinely good.
14. BAKERIES & PUBS — These are sacred institutions of Australian road tripping. When a bakery or pub is in the pre-loaded data near the user, you should know to mention it. Country bakery pies and pub counter meals are often the highlight of a road trip.
15. PRACTICAL DETAILS — When recommending accommodation/camping, mention: price range if known, whether booking is needed, pet-friendliness if the user has a dog, powered sites for caravans. When recommending food: mention cuisine type, price range, whether it's sit-down or takeaway.

═══ DEEP AUSTRALIAN KNOWLEDGE ═══
{deep_knowledge}"""
    )

    return "\n\n".join(sections)


# ══════════════════════════════════════════════════════════════
# USER MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════

_MAX_THREAD = 14
_MAX_TOOL_RESULTS = 4
_MAX_PLACES_PER_RESULT = 20


def _summarize_tool_result(tr: GuideToolResult) -> Dict[str, Any]:
    """Compress tool results — extract the useful bits, drop the bulk."""
    out: Dict[str, Any] = {"id": tr.id, "tool": tr.tool, "ok": tr.ok}
    if not tr.ok:
        out["error"] = str(tr.result.get("error", "?"))[:200]
        return out

    result = tr.result

    if tr.tool in ("places_search", "places_corridor"):
        raw_items = result.get("items", [])
        total = len(raw_items)
        compact: List[Dict[str, Any]] = []
        for p in raw_items[:_MAX_PLACES_PER_RESULT]:
            entry: Dict[str, Any] = {
                "name": p.get("name", "?"),
                "cat": p.get("category", "?"),
                "lat": round(p.get("lat", 0), 4),
                "lng": round(p.get("lng", 0), 4),
            }
            extra = p.get("extra", {})
            if isinstance(extra, dict):
                tags = extra.get("tags", extra)
                suburb = (
                    tags.get("addr:suburb") or tags.get("addr:city") or tags.get("addr:town")
                )
                if suburb:
                    entry["suburb"] = str(suburb)[:40]
                hours = tags.get("opening_hours")
                if hours:
                    entry["hours"] = str(hours)[:60]
                phone = tags.get("phone") or tags.get("contact:phone")
                if phone:
                    entry["phone"] = str(phone)[:20]
                website = tags.get("website") or tags.get("contact:website")
                if website:
                    entry["website"] = str(website)[:100]
                # Fuel specifics
                fuel_types = [
                    k.replace("fuel:", "") for k in tags
                    if k.startswith("fuel:") and tags[k] == "yes"
                ]
                if fuel_types:
                    entry["fuel_types"] = fuel_types[:5]
                # EV charging
                if tags.get("socket:type2") or tags.get("socket:chademo") or tags.get("amenity") == "charging_station":
                    entry["ev_charging"] = True
                # Camping specifics
                fee = tags.get("fee")
                if fee:
                    entry["fee"] = "free" if fee == "no" else ("paid" if fee == "yes" else str(fee)[:20])
                if tags.get("drinking_water"):
                    entry["water"] = tags["drinking_water"] == "yes"
                if tags.get("toilets"):
                    entry["toilets"] = tags["toilets"] == "yes"
                if tags.get("dump_station"):
                    entry["dump_point"] = True
                # Cuisine/speciality
                cuisine = tags.get("cuisine")
                if cuisine:
                    entry["cuisine"] = str(cuisine)[:40]
            compact.append(entry)

        out["total_found"] = total
        out["showing"] = len(compact)
        out["places"] = compact

    elif tr.tool == "places_suggest":
        clusters = result.get("clusters", [])
        compact_clusters: List[Dict[str, Any]] = []
        for cl in clusters[:8]:
            cl_places = cl.get("places", {}).get("items", [])
            compact_places: List[Dict[str, Any]] = []
            for p in cl_places[:6]:
                entry = {
                    "name": p.get("name", "?"),
                    "cat": p.get("category", "?"),
                }
                extra = p.get("extra", {})
                if isinstance(extra, dict):
                    tags = extra.get("tags", extra)
                    hours = tags.get("opening_hours")
                    if hours:
                        entry["hours"] = str(hours)[:50]
                    phone = tags.get("phone") or tags.get("contact:phone")
                    if phone:
                        entry["phone"] = str(phone)[:20]
                    website = tags.get("website") or tags.get("contact:website")
                    if website:
                        entry["website"] = str(website)[:100]
                compact_places.append(entry)
            compact_clusters.append({
                "km_from_start": cl.get("km_from_start", 0),
                "total": len(cl_places),
                "highlights": compact_places,
            })
        out["clusters"] = compact_clusters

    return out


def _build_user_message(req: GuideTurnRequest) -> str:
    parts: List[str] = []

    thread_msgs = req.thread[-_MAX_THREAD:]
    if thread_msgs:
        parts.append("=== CONVERSATION ===")
        for m in thread_msgs:
            role = "USER" if m.role == "user" else "GUIDE"
            parts.append(f"{role}: {m.content}")

    tool_results = req.tool_results[-_MAX_TOOL_RESULTS:]
    if tool_results:
        parts.append("\n=== TOOL RESULTS ===")
        for tr in tool_results:
            summary = _summarize_tool_result(tr)
            parts.append(json.dumps(summary, separators=(",", ":")))

    if req.preferred_categories:
        parts.append(
            f"\n[User has category filter active: {', '.join(req.preferred_categories)}]"
        )

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════
# RESPONSE SCHEMA (for JSON mode)
# ══════════════════════════════════════════════════════════════

def _response_schema() -> Dict[str, Any]:
    # Include web_search in tool enum if a search provider is configured
    tool_enum = ["places_search", "places_corridor", "places_suggest"]
    if settings.tavily_api_key or settings.google_cse_api_key:
        tool_enum.append("web_search")

    return {
        "type": "object",
        "properties": {
            "assistant": {"type": "string"},
            "done": {"type": "boolean"},
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["web", "call", "map", "save"]},
                        "label": {"type": "string"},
                        "place_id": {"type": "string"},
                        "place_name": {"type": "string"},
                        "url": {"type": "string"},
                        "tel": {"type": "string"},
                        "lat": {"type": "number"},
                        "lng": {"type": "number"},
                        "category": {"type": "string"},
                        "description": {"type": "string"},
                    },
                    "required": ["type", "label"],
                },
            },
            "tool_calls": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "tool": {
                            "type": "string",
                            "enum": tool_enum,
                        },
                        "req": {"type": "object"},
                    },
                    "required": ["tool", "req"],
                },
            },
        },
        "required": ["done", "tool_calls"],
    }


# ══════════════════════════════════════════════════════════════
# OUTPUT NORMALIZATION
# ══════════════════════════════════════════════════════════════

def _normalize_model_output(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"assistant": "", "tool_calls": [], "actions": [], "done": False}

    raw.setdefault("assistant", "")
    raw.setdefault("tool_calls", [])
    raw.setdefault("actions", [])
    raw.setdefault("done", False)

    if not isinstance(raw.get("tool_calls"), list):
        raw["tool_calls"] = []
    if not isinstance(raw.get("actions"), list):
        raw["actions"] = []

    norm_calls: List[Dict[str, Any]] = []
    for i, tc in enumerate(raw["tool_calls"]):
        if not isinstance(tc, dict):
            continue
        tc.setdefault("id", f"tc_{i}_{uuid.uuid4().hex[:8]}")
        norm_calls.append(tc)
    raw["tool_calls"] = norm_calls

    norm_actions: List[Dict[str, Any]] = []
    for a in raw["actions"]:
        if not isinstance(a, dict):
            continue
        if a.get("type") in ("web", "call", "map", "save") and a.get("label"):
            norm_actions.append(a)
    raw["actions"] = norm_actions

    return raw


# ══════════════════════════════════════════════════════════════
# TOOL REQUEST REPAIR & VALIDATION
# ══════════════════════════════════════════════════════════════

def _repair_req(tool: str, req: Dict[str, Any], ctx: GuideContext) -> Dict[str, Any]:
    req = dict(req)

    if tool == "places_corridor":
        if not req.get("corridor_key") and ctx.corridor_key:
            req["corridor_key"] = ctx.corridor_key
        req.setdefault("limit", 30)

    elif tool == "places_suggest":
        if not req.get("geometry") and ctx.geometry:
            req["geometry"] = ctx.geometry
        req.setdefault("interval_km", 50)
        req.setdefault("radius_m", 10000)
        req.setdefault("limit_per_sample", 10)

    elif tool == "places_search":
        if "lat" in req and "lng" in req and "center" not in req:
            req["center"] = {"lat": req.pop("lat"), "lng": req.pop("lng")}
        req.setdefault("limit", 20)
        if "center" in req:
            req.setdefault("radius_m", 15000)

    if "categories" in req and isinstance(req["categories"], list):
        req["categories"] = [str(c).lower().strip() for c in req["categories"] if c]

    return req


def _validate_tool_req(tool: str, req: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        if tool == "places_search":
            PlacesRequest(**req)
        elif tool == "places_corridor":
            CorridorPlacesRequest(**req)
        elif tool == "places_suggest":
            PlacesSuggestRequest(**req)
        else:
            return False, f"Unknown tool: {tool}"
        return True, ""
    except (ValidationError, Exception) as e:
        return False, str(e)[:200]


# ══════════════════════════════════════════════════════════════
# WEB SEARCH RESULT FORMATTER
# ══════════════════════════════════════════════════════════════

def _format_search_results(results: List[Dict[str, str]]) -> str:
    """Format web search results for injection into the LLM context."""
    if not results:
        return "(No results found. Answer using your existing knowledge.)"
    lines: List[str] = []
    for i, r in enumerate(results, 1):
        title = r.get("title", "")
        content = r.get("content", "")[:500]
        url = r.get("url", "")
        lines.append(f"[{i}] {title}\n{content}\nSource: {url}")
    return "\n\n".join(lines)


# ══════════════════════════════════════════════════════════════
# SERVICE
# Uses DeepSeek's OpenAI-compatible /chat/completions API
# ══════════════════════════════════════════════════════════════

class GuideService:
    def __init__(self) -> None:
        self._api_key = settings.deepseek_api_key
        self._model = settings.deepseek_model
        self._base = settings.deepseek_base_url.rstrip("/")
        self._timeout = float(getattr(settings, "guide_timeout_s", 30.0))

    async def _call_llm(
        self, sys_prompt: str, user_msg: str
    ) -> Dict[str, Any]:
        """Single LLM call to DeepSeek. Returns parsed JSON dict."""
        json_instruction = (
            "\n\nRespond ONLY with a valid JSON object matching this schema: "
            '{"assistant": string, "done": boolean, "actions": array, "tool_calls": array}'
        )
        body = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": sys_prompt + json_instruction},
                {"role": "user", "content": user_msg},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.5,
            "max_tokens": 2400,
        }

        url = f"{self._base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.post(url, headers=headers, json=body)
            if r.status_code >= 400:
                raise RuntimeError(
                    f"DeepSeek /chat/completions {r.status_code}: {r.text[:500]}"
                )
            data = r.json()

        try:
            out_text: str = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise RuntimeError(
                f"Guide LLM: unexpected response structure: {e}. Raw: {json.dumps(data)[:1000]}"
            )

        if not out_text:
            raise RuntimeError("Guide LLM: empty content in response")

        try:
            return json.loads(out_text)
        except Exception as e:
            # DeepSeek sometimes wraps in markdown — strip it
            stripped = re.sub(r"^```(?:json)?\s*|\s*```$", "", out_text.strip())
            try:
                return json.loads(stripped)
            except Exception:
                raise RuntimeError(
                    f"Guide LLM: invalid JSON: {e}. text={out_text[:400]}"
                )

    async def turn(self, req: GuideTurnRequest) -> GuideTurnResponse:
        if not self._api_key:
            raise RuntimeError("DEEPSEEK_API_KEY missing")

        sys_prompt = _build_system_prompt(req.context, req.relevant_places, req.thread)
        user_msg = _build_user_message(req)

        # Internal loop: handles web_search internally (max 2 iterations).
        # If LLM requests a places_* tool, return it to the frontend.
        # If LLM requests web_search, execute it and re-call the LLM.
        _MAX_INTERNAL_STEPS = 2
        norm: Dict[str, Any] = {}

        for _step in range(_MAX_INTERNAL_STEPS):
            raw = await self._call_llm(sys_prompt, user_msg)
            norm = _normalize_model_output(raw)

            tool_calls = norm.get("tool_calls") or []
            if not tool_calls:
                break  # No tools — final answer

            tc0 = tool_calls[0]
            tool_name = tc0.get("tool")

            if tool_name == "web_search":
                # Execute search internally and re-call LLM
                query = tc0.get("req", {}).get("query", "")
                if query:
                    results = await web_search(query)
                    search_text = _format_search_results(results)
                    user_msg = (
                        user_msg
                        + f"\n\n=== WEB SEARCH RESULTS ===\n"
                        f"Query: {query}\n{search_text}"
                    )
                    # Clear tool_calls so we don't return web_search to frontend
                    norm["tool_calls"] = []
                    norm["done"] = False
                    continue
                else:
                    # Empty query — skip and return what we have
                    norm["tool_calls"] = []
                    break
            else:
                # Frontend tool (places_*) — break and return to frontend
                break

        # Process tool calls — validate and repair (only places_* tools reach here)
        tool_calls = norm.get("tool_calls") or []
        validated_calls: List[Dict[str, Any]] = []

        if tool_calls:
            tc0 = tool_calls[0]
            tool = tc0.get("tool")
            req_obj = tc0.get("req") if isinstance(tc0.get("req"), dict) else {}
            tc_id = tc0.get("id") or f"tc_{uuid.uuid4().hex[:8]}"

            if tool not in ("places_search", "places_corridor", "places_suggest"):
                norm["assistant"] = (
                    (norm.get("assistant") or "")
                    + "\n\n(I couldn't find the right search tool. Let me try differently.)"
                )
            else:
                fixed_req = _repair_req(tool, req_obj, req.context)
                ok, err = _validate_tool_req(tool, fixed_req)

                if ok:
                    validated_calls.append({"id": tc_id, "tool": tool, "req": fixed_req})
                else:
                    norm["assistant"] = (
                        (norm.get("assistant") or "")
                        + "\n\n(Couldn't build a valid search — let me try again.)"
                    )

        response_calls: List[GuideToolCall] = [
            GuideToolCall(id=vc["id"], tool=vc["tool"], req=vc["req"])
            for vc in validated_calls
        ]

        response_actions: List[GuideAction] = []
        for a in norm.get("actions", []):
            try:
                response_actions.append(
                    GuideAction(
                        type=a.get("type", "web"),
                        label=a.get("label", ""),
                        place_id=a.get("place_id"),
                        place_name=a.get("place_name"),
                        url=a.get("url"),
                        tel=a.get("tel"),
                        lat=a.get("lat"),
                        lng=a.get("lng"),
                        category=a.get("category"),
                        description=a.get("description"),
                    )
                )
            except Exception:
                continue

        return GuideTurnResponse(
            assistant=norm.get("assistant", ""),
            tool_calls=response_calls,
            actions=response_actions,
            done=norm.get("done", not bool(response_calls)),
        )
