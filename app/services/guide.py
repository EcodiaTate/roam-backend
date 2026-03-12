# app/services/guide.py
"""
Roam Guide - God-tier AI road trip companion for Australia.

Design principles:
  1. Safety first: fuel range, fatigue, wildlife, weather, road conditions
  2. Regionally aware: knows what region of Australia the user is in and
     injects hyper-local knowledge (croc country, fire zones, flood plains, etc.)
  3. Seasonally intelligent: month determines wildflower season, whale watching,
     wet season road closures, bushfire risk, school holidays
  4. Vehicle-aware: sedan vs 4WD vs caravan/RV changes fuel, road access, dump points
  5. Phase-adaptive: departing → cruising → midway → home stretch → arriving
  6. Concise: mobile screen, one-handed, glancing while passenger reads aloud
  7. Proactive: nudges about fuel, fatigue, wildlife, weather without being asked
  8. Never invents: only recommends from data or tool results
  9. Culturally respectful: indigenous heritage, local history, community sensitivity
  10. Companion-aware: adapts for solo, couple, family, group
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


# ──────────────────────────────────────────────────────────────
# Response schema for structured JSON output
# ──────────────────────────────────────────────────────────────

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
                            "enum": [
                                "places_search",
                                "places_corridor",
                                "places_suggest",
                            ],
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
# REGIONAL INTELLIGENCE ENGINE
# ══════════════════════════════════════════════════════════════

# Each region defines a lat/lng bounding box and a knowledge block
# that gets injected into the system prompt when the user is in that region.
# This is the single biggest intelligence upgrade - hyper-local knowledge.

_REGIONS: List[Dict[str, Any]] = [
    # ── Queensland ──────────────────────────────────────────
    {
        "id": "qld_tropical_north",
        "name": "Tropical North Queensland",
        "bbox": {"s": -19.5, "n": -10.0, "w": 142.0, "e": 147.5},
        "knowledge": (
            "You're in Tropical North QLD - croc country. NEVER swim in rivers, creeks, or "
            "estuaries unless clearly signed safe. Saltwater crocs are in every waterway north "
            "of Rockhampton. Stingers (box jellyfish) in ocean Oct–May - swim only in stinger nets. "
            "Cassowaries cross roads in the Daintree/Mission Beach area - drive slowly. "
            "The Great Barrier Reef is accessible from Cairns, Port Douglas, Townsville, and Airlie Beach. "
            "Wet season (Nov–Apr) brings road closures on unsealed roads and potential flooding "
            "on the Gillies Highway, Palmerston Highway, and Cape Tribulation road. "
            "The Atherton Tablelands behind Cairns have world-class waterfalls, crater lakes, and farm gates. "
            "Kuranda markets are iconic but crowded on weekends. "
            "Local tip: the Waterfall Circuit (Millaa Millaa, Zillie, Ellinjaa) is a must-do detour."
        ),
    },
    {
        "id": "qld_cape_york",
        "name": "Cape York Peninsula",
        "bbox": {"s": -16.0, "n": -10.0, "w": 141.0, "e": 145.5},
        "knowledge": (
            "You're heading into Cape York - one of Australia's last great wilderness frontiers. "
            "4WD ESSENTIAL. Most tracks are impassable in the wet season (Nov–Apr). "
            "The Telegraph Track is legendary but brutal - only for prepared vehicles with recovery gear. "
            "Fuel is available at Coen, Archer River Roadhouse, Bamaga, and a few cattle stations. "
            "Carry extra fuel, water (min 20L per person), and satellite communication. "
            "River crossings change depth daily - check with other travellers or roadhouses. "
            "Respect indigenous communities - many areas require permits. "
            "The Tip (Pajinka) at the top of Australia is a highlight but tides affect access. "
            "Fruit Bat Falls and Eliot Falls (south of the Jardine) are spectacular camping/swim spots."
        ),
    },
    {
        "id": "qld_outback",
        "name": "Outback Queensland",
        "bbox": {"s": -29.0, "n": -19.5, "w": 138.0, "e": 149.0},
        "knowledge": (
            "You're in Outback QLD - big sky country. Towns are small and far apart. "
            "Longreach (Stockman's Hall of Fame, Qantas Founders Museum) and Winton (dinosaur trail, "
            "Waltzing Matilda Centre) are the cultural anchors. Birdsville is iconic but remote - "
            "fuel up at every opportunity. The Channel Country floods after rain and roads close fast. "
            "Boulia's Min Min light is a genuine phenomenon. "
            "Road trains up to 53m long - give them ALL the room and never overtake on narrow roads. "
            "Artesian bore baths at Mitchell, Cunnamulla, and Lightning Ridge (just over the NSW border) "
            "are free and wonderful. Stars at night are extraordinary - no light pollution. "
            "Many stations offer farmstay accommodation - worth the detour for authentic outback experience."
        ),
    },
    {
        "id": "qld_southeast",
        "name": "South East Queensland",
        "bbox": {"s": -28.5, "n": -25.5, "w": 150.5, "e": 154.0},
        "knowledge": (
            "You're in South East QLD - Brisbane, Gold Coast, Sunshine Coast corridor. "
            "Traffic is heavy on the M1/Pacific Motorway, especially Fri afternoons and Sun evenings. "
            "The hinterland (Tamborine Mountain, Springbrook, Lamington NP) has stunning rainforest walks "
            "and waterfalls - a world away from the coast. "
            "Noosa's Hastings Street is upscale; for a local vibe try the Eumundi Markets (Wed/Sat). "
            "The Glass House Mountains are sacred to the Gubbi Gubbi people - great climbing/walking. "
            "Stradbroke Island (Straddie) is the local's beach escape - take the ferry from Cleveland. "
            "Eat Street Northshore (Fri/Sat nights) is Brisbane's best food market. "
            "In summer, storms roll in fast around 3-5 PM - hail can be severe."
        ),
    },
    # ── New South Wales ─────────────────────────────────────
    {
        "id": "nsw_sydney_region",
        "name": "Greater Sydney & Blue Mountains",
        "bbox": {"s": -34.2, "n": -33.2, "w": 149.5, "e": 151.6},
        "knowledge": (
            "You're in the Greater Sydney region. Traffic is the main challenge - avoid the M1/M2/M5 "
            "during peak hours (7-9 AM, 4-7 PM). Toll roads are everywhere - make sure you have an "
            "e-tag or register plates for casual tolling. "
            "The Blue Mountains (Katoomba, Leura, Blackheath) are a 1.5hr drive west - world-class bushwalks, "
            "the Three Sisters, and cozy cafes. Go midweek to avoid crowds at Echo Point. "
            "The Royal National Park south of Sydney has the famous coastal walk (Bundeena to Otford). "
            "Wollongong is an easy day trip via the Grand Pacific Drive - Sea Cliff Bridge is iconic. "
            "Northern beaches (Palm Beach, Avalon, Whale Beach) are quieter than Bondi/Manly. "
            "Parking in the CBD is expensive - use the train from suburban park-and-ride if visiting the city."
        ),
    },
    {
        "id": "nsw_north_coast",
        "name": "NSW North Coast",
        "bbox": {"s": -32.5, "n": -28.2, "w": 151.5, "e": 154.0},
        "knowledge": (
            "You're on the NSW North Coast - one of Australia's great driving corridors. "
            "Byron Bay is iconic but parking is a nightmare in peak season - arrive before 9 AM or "
            "park at the rec grounds and walk. The lighthouse walk at dawn is unmissable. "
            "Coffs Harbour's Big Banana is kitsch but kids love it. The Bellingen/Dorrigo detour "
            "into the hinterland has spectacular rainforest and the Skywalk at Dorrigo NP. "
            "Port Macquarie has great beaches and the Koala Hospital (free entry, feeding time 3 PM). "
            "Yamba is the locals' pick for a quieter coastal town - exceptional fish and chips at Yamba Shores. "
            "Whale watching season is Jun-Nov along this coast - humpbacks migrate north then south. "
            "The Pacific Highway is mostly dual carriageway now but watch for the few remaining single-lane sections."
        ),
    },
    {
        "id": "nsw_outback",
        "name": "Outback NSW",
        "bbox": {"s": -34.0, "n": -28.5, "w": 140.5, "e": 149.0},
        "knowledge": (
            "You're in Outback NSW - Broken Hill, Lightning Ridge, White Cliffs, Bourke. "
            "Broken Hill feels like South Australia (it uses SA time!). The art scene is remarkable - "
            "Living Desert Sculptures at sunset is a must. "
            "Lightning Ridge is the opal capital - fossick for opals, swim in the artesian bore baths. "
            "White Cliffs has underground homes you can tour or stay in (dugouts). "
            "Mungo National Park and the Walls of China are deeply significant to the Barkindji, "
            "Mutthi Mutthi, and Nyiampaar peoples - guided tours are the respectful way to visit. "
            "Fuel up at EVERY town - distances are serious. Carry extra water always. "
            "The Darling River Run (Bourke to Wentworth) is a beautiful multi-day drive along the river."
        ),
    },
    # ── Victoria ────────────────────────────────────────────
    {
        "id": "vic_great_ocean_road",
        "name": "Great Ocean Road",
        "bbox": {"s": -39.0, "n": -38.0, "w": 143.0, "e": 145.0},
        "knowledge": (
            "You're on or near the Great Ocean Road - one of the world's great coastal drives. "
            "The GOR is SLOW - tight curves, steep grades, tourist traffic. Don't expect highway speed. "
            "Use pullover bays to let faster traffic pass. "
            "The Twelve Apostles car park gets packed by 10 AM in summer - go at sunrise for photos "
            "and fewer crowds. Loch Ard Gorge nearby is equally stunning and often less crowded. "
            "Kennett River is the best spot for wild koalas - look up in the eucalyptus along Grey River Road. "
            "Apollo Bay has great food (Chris's Beacon Point for views, Apollo Bay Bakery for pies). "
            "The Otway Fly Treetop Walk is worth the detour, especially with kids. "
            "In winter, whale watching from the cliffs at Warrnambool is free and frequent (Southern Rights). "
            "Bushfire risk is extreme in summer - know your exit routes and check CFA warnings."
        ),
    },
    {
        "id": "vic_high_country",
        "name": "Victorian High Country",
        "bbox": {"s": -37.8, "n": -36.2, "w": 145.5, "e": 148.5},
        "knowledge": (
            "You're in the Victorian High Country - alpine country, ski fields, and historic gold towns. "
            "In winter (Jun-Sep), snow chains are legally required to be carried on certain roads "
            "(B500/Great Alpine Road) even if not snowing. Check VicRoads for chain requirements. "
            "Mt Buller, Falls Creek, and Mt Hotham are the main ski resorts. "
            "In summer, the same mountains offer incredible mountain biking, hiking, and wildflowers. "
            "Beechworth and Bright are the gourmet towns - craft breweries, providores, and honey. "
            "The Murray to Mountains Rail Trail is one of Australia's best cycling paths. "
            "Ned Kelly country - Glenrowan, Beechworth courthouse, Old Melbourne Gaol. "
            "The Great Alpine Road (Wangaratta to Bairnsdale via Hotham) is spectacular but has "
            "serious hairpin sections - not recommended for caravans."
        ),
    },
    # ── South Australia ─────────────────────────────────────
    {
        "id": "sa_adelaide_region",
        "name": "Adelaide & Wine Regions",
        "bbox": {"s": -36.0, "n": -34.0, "w": 138.0, "e": 140.0},
        "knowledge": (
            "You're in the Adelaide region - Australia's wine capital. "
            "The Barossa Valley (45 min from Adelaide) has 150+ wineries. Penfolds, Jacob's Creek, "
            "and Henschke are the big names, but the small family cellar doors are often more memorable. "
            "McLaren Vale is the other major wine region - more intimate, excellent Shiraz and Grenache. "
            "The Adelaide Hills (Hahndorf, Stirling, Crafers) have German heritage, cool-climate wines, "
            "and excellent cafes. Hahndorf's German Arms is a good lunch stop. "
            "Adelaide Central Market is world-class - go hungry on a Saturday morning. "
            "Victor Harbor and Port Elliot on the Fleurieu Peninsula are great day trips - "
            "whale watching Jun-Oct from the bluff. "
            "SA has no daylight saving - check time differences if crossing from VIC."
        ),
    },
    {
        "id": "sa_outback",
        "name": "Outback South Australia",
        "bbox": {"s": -32.0, "n": -26.0, "w": 134.0, "e": 141.5},
        "knowledge": (
            "You're in Outback SA - some of Australia's most remote and dramatic landscape. "
            "Coober Pedy is the opal capital - underground hotels, churches, and homes carved from rock. "
            "The Oodnadatta Track is legendary but UNSEALED - check road conditions before departure "
            "(DIT road conditions hotline, Transport SA website). After rain it can close for weeks. "
            "The Birdsville Track runs from Marree to Birdsville - 520km of true outback, very remote. "
            "Fuel: Marree, William Creek, Oodnadatta, Coober Pedy. CARRY SPARE FUEL. "
            "Woomera has a fascinating space/military history museum. "
            "Lake Eyre (Kati Thanda) fills only a few times per century - if it's got water, "
            "scenic flights from William Creek are a once-in-a-lifetime experience. "
            "The Painted Desert near Arckaringa is spectacular - ask at William Creek for directions. "
            "Leigh Creek and Parachilna (Prairie Hotel - feral mixed grill) are outback icons."
        ),
    },
    {
        "id": "sa_flinders",
        "name": "Flinders Ranges",
        "bbox": {"s": -33.5, "n": -29.5, "w": 137.5, "e": 140.0},
        "knowledge": (
            "You're in the Flinders Ranges - ancient, sacred, and spectacular. "
            "Wilpena Pound (Ikara) is the centrepiece - the Adnyamathanha people's creation story. "
            "The scenic flight over the Pound is worth every dollar. "
            "Walking: St Mary Peak (Tanderra Saddle) is the big one (full day, 20km return). "
            "The Brachina Gorge geological trail drives through 500 million years of Earth history. "
            "Rawnsley Park Station and Wilpena Pound Resort are the main accommodation bases. "
            "Prairie Hotel in Parachilna - famous for feral mixed grill (emu, kangaroo, camel). "
            "Arkaroola in the northern Flinders has the Ridgetop Tour - one of Australia's great 4WD experiences. "
            "Stargazing is extraordinary - very low light pollution. "
            "Note: the Ranges are Adnyamathanha country. Some sacred sites are restricted - follow signage."
        ),
    },
    # ── Western Australia ───────────────────────────────────
    {
        "id": "wa_southwest",
        "name": "WA South West",
        "bbox": {"s": -35.5, "n": -31.0, "w": 114.0, "e": 117.5},
        "knowledge": (
            "You're in WA's South West - tall timber country, wine, and stunning coastline. "
            "Margaret River wine region is world-class - Cabernet Sauvignon and Chardonnay. "
            "Caves (Mammoth, Lake, Jewel, Ngilgi) are spectacular - book ahead in school holidays. "
            "The Bibbulmun Track is one of Australia's great long-distance walks (963km Albany to Kalamunda). "
            "Pemberton has the famous Gloucester Tree (climb to the top if you dare) and karri forests. "
            "Denmark and Albany on the south coast have dramatic coastline - The Gap, Natural Bridge, "
            "and Greens Pool (one of Australia's best beaches). "
            "Whale watching (Jun-Oct) is exceptional from Albany and Augusta - Southern Rights and Humpbacks. "
            "WA wildflower season (Aug-Nov) carpets the landscape - the Stirling Ranges are a hotspot. "
            "Drive times in WA are LONG - Perth to Margaret River is 3hrs, Perth to Albany is 4.5hrs."
        ),
    },
    {
        "id": "wa_northwest",
        "name": "WA Northwest & Kimberley",
        "bbox": {"s": -22.0, "n": -13.5, "w": 119.0, "e": 129.5},
        "knowledge": (
            "You're in the Kimberley / Northwest WA - one of Earth's last great wilderness regions. "
            "CROC COUNTRY: Saltwater crocodiles in every waterway. Do NOT swim unless clearly signed safe. "
            "The Gibb River Road (660km, Broome to Kununurra) is the quintessential outback adventure - "
            "4WD essential, multiple river crossings, stunning gorges (Bell, Galvans, Manning, El Questro). "
            "Dry season only (May-Oct) - the Gibb is CLOSED in the wet. "
            "Bungle Bungles (Purnululu NP) - beehive domes are extraordinary. Fly in from Kununurra "
            "or drive (4WD, rough 53km access track). Cathedral Gorge and Echidna Chasm are highlights. "
            "Broome: Cable Beach at sunset with camels, Staircase to the Moon at Town Beach (full moon "
            "Mar-Oct), pearl farms, and the outdoor cinema at Sun Pictures (world's oldest). "
            "Fuel is VERY expensive in the Kimberley - budget $2.50-3.50/L. Carry spare. "
            "Respect indigenous communities - many areas require transit or access permits."
        ),
    },
    {
        "id": "wa_pilbara",
        "name": "Pilbara",
        "bbox": {"s": -24.0, "n": -20.0, "w": 115.0, "e": 121.0},
        "knowledge": (
            "You're in the Pilbara - ancient, vast, and very hot. "
            "Karijini National Park is the crown jewel - Dales Gorge, Fortescue Falls, Hancock Gorge, "
            "Weano Gorge. Swimming in the gorge pools is magical. Eco Retreat is the premium camping. "
            "It gets EXTREMELY hot Nov-Mar (45°C+). Travel in the cooler months (Apr-Sep). "
            "Road trains are massive here - give them absolute right of way. "
            "Port Hedland is a mining town but the Courthouse Gallery has excellent indigenous art. "
            "Millstream-Chichester NP has Python Pool - a beautiful oasis swimming hole. "
            "Fuel: Newman, Tom Price, Port Hedland. Distances between are 200-300km. "
            "The Burrup Peninsula (Murujuga) has the world's largest collection of rock art - "
            "40,000+ petroglyphs, UNESCO nomination pending. Sacred to the Yaburara people."
        ),
    },
    # ── Northern Territory ──────────────────────────────────
    {
        "id": "nt_top_end",
        "name": "Top End NT",
        "bbox": {"s": -15.5, "n": -11.0, "w": 129.0, "e": 137.5},
        "knowledge": (
            "You're in the Top End - Darwin, Kakadu, Litchfield, Arnhem Land. "
            "CROC COUNTRY: Salties in every river, creek, estuary, and even beach. "
            "No swimming anywhere unless explicitly signed safe. "
            "Kakadu NP is vast - Ubirr rock art at sunset, Jim Jim and Twin Falls (4WD, dry season), "
            "Yellow Water cruise for crocs and birdlife. Allow 3+ days minimum. "
            "Litchfield NP (1.5hr from Darwin) is better for swimming - Wangi Falls, Florence Falls, "
            "Buley Rockhole. Check closure signs after rain. "
            "Darwin: Mindil Beach Sunset Market (Thu/Sun dry season) is a must. Crocosaurus Cove "
            "for the cage of death. The Museum and Art Gallery has Cyclone Tracy exhibits. "
            "Dry season (May-Oct) is THE time to visit. Wet season roads flood and tracks close. "
            "Arnhem Land requires a permit from the Northern Land Council - plan weeks ahead. "
            "Fuel is expensive but available at roadhouses along the Stuart Highway."
        ),
    },
    {
        "id": "nt_red_centre",
        "name": "Red Centre",
        "bbox": {"s": -26.0, "n": -22.0, "w": 129.0, "e": 138.0},
        "knowledge": (
            "You're in the Red Centre - the spiritual heart of Australia. "
            "Uluru (Ayers Rock) is sacred to the Anangu people. Climbing is permanently closed and "
            "strongly discouraged. The base walk (10.6km) and sunrise/sunset viewing are extraordinary. "
            "Kata Tjuta (the Olgas) - Valley of the Winds walk is arguably more spectacular than Uluru "
            "and much less crowded. Go early. "
            "Kings Canyon (Watarrka NP) has the Rim Walk - 6km, moderate, stunning. Leave by 9 AM in summer. "
            "Alice Springs: Desert Park is world-class for understanding arid ecology. "
            "The MacDonnell Ranges have gorgeous gorges - Ormiston, Glen Helen, Ellery Creek Big Hole "
            "(swimming). The Larapinta Trail is one of Australia's great multi-day hikes. "
            "West MacDonnell Ranges: Standley Chasm at midday when the sun hits the walls. "
            "Summer temps hit 45°C+ - carry minimum 5L water per person per day for any walking. "
            "Fuel: Alice Springs, Erldunda, Yulara (Uluru), Kings Canyon Resort. Plan carefully."
        ),
    },
    # ── Tasmania ────────────────────────────────────────────
    {
        "id": "tas_general",
        "name": "Tasmania",
        "bbox": {"s": -43.8, "n": -39.5, "w": 143.5, "e": 149.0},
        "knowledge": (
            "You're in Tasmania - compact, diverse, and dramatically beautiful. "
            "Distances are SHORT by mainland standards but roads are narrow and winding - "
            "don't underestimate travel times. A lot of single-lane roads even on 'highways'. "
            "Cradle Mountain: Dove Lake circuit (2hrs, easy) is the icon. Overland Track (6 days) "
            "is one of Australia's great walks - book months ahead. "
            "Freycinet NP: Wineglass Bay is famous - the lookout walk is 1.5hrs return. "
            "Bay of Fires on the east coast has the most incredible orange-lichen-covered boulders. "
            "MONA in Hobart is one of the world's great art museums - confronting, brilliant, unmissable. "
            "Salamanca Market (Sat) in Hobart is fantastic. Farm Gate Market (Sun) is smaller but foodie-focused. "
            "Bruny Island: Get the Cheese Company, Get Shucked oysters, and the Neck lookout. "
            "The west coast (Strahan, Queenstown) has a wild, mining-heritage feel - Gordon River cruise. "
            "Wildlife: Tasmanian devils at Cradle Mountain or Bonorong Sanctuary. Wombats on roads at dusk. "
            "Weather changes FAST - four seasons in one day is real. Pack layers always."
        ),
    },
]


def _detect_regions(lat: float, lng: float) -> List[Dict[str, Any]]:
    """Return all matching regions for the user's coordinates."""
    matches: List[Dict[str, Any]] = []
    for r in _REGIONS:
        bb = r["bbox"]
        if bb["s"] <= lat <= bb["n"] and bb["w"] <= lng <= bb["e"]:
            matches.append(r)
    return matches


# ══════════════════════════════════════════════════════════════
# SEASONAL INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _seasonal_knowledge(month: int, lat: float, lng: float) -> str:
    """
    Return seasonal advice based on month and latitude.
    Australian seasons are inverted from Northern Hemisphere.
    """
    tropical = lat > -23.5  # above Tropic of Capricorn

    blocks: List[str] = []

    # ── Wet/dry season (tropics) ───────────────────────────
    if tropical:
        if month in (11, 12, 1, 2, 3, 4):
            blocks.append(
                "WET SEASON in the tropics: expect heavy afternoon storms, potential flooding, "
                "and road closures on unsealed roads. Many outback tracks and river crossings "
                "are impassable. Stingers (box jellyfish) in coastal waters Oct-May."
            )
        else:
            blocks.append(
                "DRY SEASON in the tropics: perfect conditions. Blue skies, warm days, cool nights. "
                "Peak tourist season - book accommodation ahead. Waterfalls are flowing best early "
                "in the dry (May-Jul). By Oct, some waterholes dry up."
            )

    # ── Bushfire season (southern AU) ──────────────────────
    if not tropical and month in (11, 12, 1, 2, 3):
        blocks.append(
            "BUSHFIRE SEASON: Check fire danger ratings daily (CFA, RFS, CFS apps). "
            "On Total Fire Ban days, no open flames including campfires and some BBQs. "
            "Know your fire plan: identify shelter locations, keep car fuelled, carry water. "
            "Watch for sudden road closures - fires can move extremely fast."
        )

    # ── Whale watching ─────────────────────────────────────
    if month in (6, 7, 8, 9, 10, 11):
        blocks.append(
            "WHALE SEASON: Humpback whales migrate along the east and west coasts (Jun-Nov). "
            "Southern right whales in the Great Australian Bight, Head of Bight, Warrnambool (Jun-Oct). "
            "Best viewing from headlands - binoculars help but breaches are visible to naked eye."
        )

    # ── Wildflower season (WA) ─────────────────────────────
    if month in (8, 9, 10, 11) and lng < 125:
        blocks.append(
            "WA WILDFLOWER SEASON: The bush explodes with colour Aug-Nov. "
            "Hotspots: Stirling Ranges, Fitzgerald River NP, Lesueur NP, coalseam conservation park. "
            "The wave moves south to north - check wildflowerwa.com.au for current blooms."
        )

    # ── School holidays ────────────────────────────────────
    if month in (1, 4, 7, 10):
        blocks.append(
            "SCHOOL HOLIDAY PERIOD: Popular destinations, national parks, and caravan parks "
            "will be busy. Book accommodation ahead. Traffic heavier on coastal highways."
        )

    # ── Summer heat ────────────────────────────────────────
    if month in (12, 1, 2) and lat > -30:
        blocks.append(
            "EXTREME HEAT WARNING ZONE: Inland temperatures can exceed 45°C. "
            "Carry minimum 5L water per person per day. Never leave the vehicle if broken down. "
            "UV index is extreme - sunburn in 10 minutes. Drive early, rest in shade midday."
        )

    # ── Winter driving ─────────────────────────────────────
    if month in (6, 7, 8) and lat < -36:
        blocks.append(
            "WINTER: Snow possible in alpine areas of VIC, NSW, TAS. "
            "Carry chains if crossing alpine passes (legally required on some roads). "
            "Frost on roads at dawn - bridges and overpasses freeze first. "
            "Short daylight hours - plan to arrive before dark."
        )

    return "\n".join(blocks) if blocks else ""


# ══════════════════════════════════════════════════════════════
# VEHICLE-AWARE INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _vehicle_context(profile: str | None, stops: List[Dict[str, Any]]) -> str:
    """Inject vehicle-specific advice based on driving profile."""
    p = (profile or "drive").lower()

    if p in ("caravan", "rv", "camper", "motorhome", "towing"):
        return (
            "VEHICLE: Caravan/RV. Watch for height/width restrictions. Avoid steep mountain passes "
            "(Great Alpine Road, Gillies Highway). Look for dump points at fuel stations and caravan parks. "
            "Plan fuel stops earlier - range is lower when towing. "
            "Many national park campgrounds have size limits - check before committing. "
            "Free camps with facilities (dump points, water) are gold. "
            "Drive slower on gravel - stones damage everything. Pull over for overtaking traffic."
        )
    elif p in ("4wd", "4x4", "offroad"):
        return (
            "VEHICLE: 4WD. You have access to unsealed tracks, station stays, and remote spots "
            "that sedans can't reach. Check tyre pressures for different surfaces (lower for sand, "
            "higher for rocky tracks). Carry a recovery kit, air compressor, and satellite comms. "
            "River crossings: walk first, check depth, cross in low range. "
            "Fuel range is critical in remote areas - carry jerry cans for outback."
        )
    else:
        return (
            "VEHICLE: Standard car. Stick to sealed roads unless specifically noted as suitable. "
            "Some national park access roads are unsealed but graded - check conditions. "
            "Fuel range: plan for 600-800km between fills in remote areas as a safety margin."
        )


# ══════════════════════════════════════════════════════════════
# COMPANION-MODE DETECTION
# ══════════════════════════════════════════════════════════════

def _companion_hints(thread: List[GuideMsg], stops: List[Dict[str, Any]]) -> str:
    """
    Detect travel party composition from conversation context and adjust tone.
    """
    all_text = " ".join(m.content.lower() for m in thread if m.role == "user")

    hints: List[str] = []

    # Family signals
    family_terms = ["kids", "children", "family", "toddler", "baby", "playground",
                    "child-friendly", "kid-friendly", "little ones", "nappy", "pram"]
    if any(t in all_text for t in family_terms):
        hints.append(
            "FAMILY TRAVEL: Prioritize playgrounds, family-friendly cafes, short walks, "
            "swimming spots, wildlife parks. Suggest frequent breaks (kids need more stops). "
            "Warn about hazards (crocs, cliffs, deep water) more explicitly."
        )

    # Couple signals
    couple_terms = ["partner", "wife", "husband", "romantic", "anniversary", "honeymoon",
                    "date", "couples", "girlfriend", "boyfriend"]
    if any(t in all_text for t in couple_terms):
        hints.append(
            "COUPLE TRAVEL: Suggest romantic dining, wineries, scenic sunset spots, "
            "boutique accommodation, couples experiences (spa, hot springs, boat tours)."
        )

    # Dog/pet signals
    pet_terms = ["dog", "dogs", "puppy", "pet-friendly", "pet friendly", "furry"]
    if any(t in all_text for t in pet_terms):
        hints.append(
            "TRAVELING WITH DOG: Mention pet-friendly accommodation, off-leash beaches, "
            "cafes with outdoor seating. Note: dogs not allowed in national parks in most states. "
            "State forests and some free camps are usually dog-friendly."
        )

    # Budget signals
    budget_terms = ["budget", "cheap", "free", "affordable", "save money", "backpacker"]
    if any(t in all_text for t in budget_terms):
        hints.append(
            "BUDGET TRAVEL: Prioritize free camps, free activities (walks, beaches, lookouts), "
            "cheap eats (bakeries, fish & chips), and free attractions. "
            "Mention Wikicamps, iOverlander for free/cheap camping."
        )

    return "\n".join(hints) if hints else ""


# ══════════════════════════════════════════════════════════════
# PROACTIVE SAFETY & NUDGE ENGINE
# ══════════════════════════════════════════════════════════════

def _proactive_nudges(
    progress: TripProgress | None,
    total_km: float | None,
    ctx: GuideContext,
    relevant_places: List[WirePlace],
) -> str:
    """
    Generate proactive nudges based on current trip state.
    These are injected into the system prompt as HIGH-PRIORITY reminders
    that the LLM should weave into its response when appropriate.
    """
    if not progress:
        return ""

    nudges: List[str] = []

    # ── Fuel nudge ─────────────────────────────────────────
    # Check if there are any fuel places in the relevant_places ahead
    fuel_ahead = [
        p for p in relevant_places
        if p.ahead and p.category in ("fuel", "ev_charging")
        and p.dist_km is not None
    ]
    fuel_ahead.sort(key=lambda p: p.dist_km or 9999)

    if not fuel_ahead:
        nudges.append(
            "⛽ FUEL WARNING: No fuel stations detected ahead in the pre-loaded data. "
            "If the user hasn't fuelled recently, proactively mention this. "
            "Use places_corridor to search for fuel if needed."
        )
    elif fuel_ahead[0].dist_km and fuel_ahead[0].dist_km > 150:
        next_fuel = fuel_ahead[0]
        nudges.append(
            f"⛽ NEXT FUEL: {next_fuel.name} is {next_fuel.dist_km:.0f}km ahead. "
            f"That's a significant gap - mention it if the topic comes up."
        )

    # ── Fatigue nudge ──────────────────────────────────────
    km_driven = progress.km_from_start

    # Estimate drive time from progress fraction × total route duration
    drive_time_h: float | None = None
    if total_km and total_km > 0 and ctx.total_duration_s:
        fraction = km_driven / total_km
        drive_time_h = (fraction * ctx.total_duration_s) / 3600

    if drive_time_h is not None and drive_time_h >= 2.0:
        nudges.append(
            f"😴 FATIGUE: User has been driving ~{drive_time_h:.1f} hours (estimated). "
            f"Australian road safety says stop every 2 hours. "
            f"Suggest a break if they ask anything - weave it in naturally."
        )
    elif km_driven > 200:
        nudges.append(
            f"😴 DISTANCE CHECK: {km_driven:.0f}km driven so far. "
            f"If they haven't stopped recently, a break would be wise."
        )

    # ── Wildlife timing nudge ──────────────────────────────
    if progress.local_time_iso:
        try:
            hour = datetime.fromisoformat(
                progress.local_time_iso.replace("Z", "+00:00")
            ).hour
            if hour in (4, 5, 6, 17, 18, 19):
                nudges.append(
                    "🦘 WILDLIFE ACTIVE: Dawn/dusk is peak wildlife-on-road time. "
                    "Kangaroos, wombats, echidnas are active now. If user asks about "
                    "anything, mention driving slowly and watching the roadsides."
                )
            elif hour >= 20 or hour < 4:
                nudges.append(
                    "🦘 NIGHT DRIVING: Wildlife extremely active on roads at night. "
                    "Rural driving at night is high-risk in Australia. "
                    "Strongly suggest stopping if they haven't already."
                )
        except Exception:
            pass

    # ── Approaching next stop ──────────────────────────────
    if progress.km_remaining is not None and progress.km_remaining < 30:
        nudges.append(
            f"📍 ARRIVING SOON: Only {progress.km_remaining:.0f}km to next stop. "
            f"Offer helpful arrival info - parking, what's nearby, any last-minute needs."
        )

    return "\n".join(nudges) if nudges else ""


# ══════════════════════════════════════════════════════════════
# SYSTEM PROMPT BUILDER
# ══════════════════════════════════════════════════════════════

def _format_stops(
    stops: List[Dict[str, Any]], visited_ids: List[str] | None = None
) -> str:
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
    if not progress or not total_km or total_km <= 0:
        return "planning"
    pct = progress.km_from_start / total_km if total_km > 0 else 0
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


def _time_context(local_iso: str | None) -> Tuple[str, str]:
    """Returns (time_label, driving_advice) tuned for Australian conditions."""
    if not local_iso:
        return "Unknown time", ""
    try:
        dt = datetime.fromisoformat(local_iso.replace("Z", "+00:00"))
        hour = dt.hour
    except Exception:
        return "Unknown time", ""

    if 4 <= hour < 6:
        return (
            f"Pre-dawn ({hour:02d}:{dt.minute:02d})",
            "Very early. Roos and wallabies extremely active on roads. "
            "Drive with extreme caution. Fog possible in valleys."
        )
    elif 6 <= hour < 8:
        return (
            f"Early morning ({hour:02d}:{dt.minute:02d})",
            "Dawn - wildlife still active near roads. Good time to fuel up and grab a coffee. "
            "Frost on roads in southern AU winter - bridges freeze first."
        )
    elif 8 <= hour < 12:
        return (
            f"Morning ({hour:02d}:{dt.minute:02d})",
            "Prime driving hours. Good visibility, comfortable temps."
        )
    elif 12 <= hour < 14:
        return (
            f"Midday ({hour:02d}:{dt.minute:02d})",
            "Lunch time. Good moment for a proper break - stretch, eat, hydrate. "
            "In summer, shade and water are essential."
        )
    elif 14 <= hour < 16:
        return (
            f"Afternoon ({hour:02d}:{dt.minute:02d})",
            "Afternoon driving. If on the road since morning, fatigue is real. Consider a stop."
        )
    elif 16 <= hour < 18:
        return (
            f"Late afternoon ({hour:02d}:{dt.minute:02d})",
            "Golden hour approaching. Wildlife becoming active. "
            "If camping, secure a site before dark. In summer, afternoon storms in QLD/NT."
        )
    elif 18 <= hour < 20:
        return (
            f"Dusk ({hour:02d}:{dt.minute:02d})",
            "DANGER ZONE: Kangaroos and livestock most active at dusk. "
            "Reduce speed on rural roads. Look for accommodation if you haven't stopped."
        )
    elif 20 <= hour < 22:
        return (
            f"Evening ({hour:02d}:{dt.minute:02d})",
            "Night driving is risky in rural/outback Australia. "
            "Animals are invisible until headlights catch their eyes. Suggest stopping."
        )
    else:
        return (
            f"Night ({hour:02d}:{dt.minute:02d})",
            "Dangerous to drive in rural areas at night. Wildlife, road trains, fatigue - "
            "all compounded by darkness. Stop if at all possible."
        )


def _format_speed(speed_mps: float | None) -> str:
    if speed_mps is None or speed_mps < 0:
        return "unknown"
    return f"{speed_mps * 3.6:.0f} km/h"


def _format_relevant_places(places: List[WirePlace]) -> str:
    """Format pre-filtered places - rich enough to recommend from, compact for tokens."""
    if not places:
        return "  (none pre-loaded - use tools to search if needed)"

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


def _build_system_prompt(
    ctx: GuideContext,
    relevant_places: List[WirePlace],
    thread: List[GuideMsg] | None = None,
) -> str:
    total_km = (ctx.total_distance_m or 0) / 1000 if ctx.total_distance_m else None
    total_h = (
        int((ctx.total_duration_s or 0) // 3600) if ctx.total_duration_s else None
    )
    total_m = (
        int(((ctx.total_duration_s or 0) % 3600) // 60)
        if ctx.total_duration_s
        else None
    )

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

    # Time context
    time_label, driving_advice = _time_context(
        progress.local_time_iso if progress else None
    )

    # Trip phase
    phase = _trip_phase(progress, total_km)
    phase_guidance = {
        "planning": (
            "Trip is being planned. Help with route ideas, must-see stops, "
            "fuel planning, accommodation options, and timing advice."
        ),
        "departing": (
            "Just departed! Check they're fuelled up and have water. "
            "Mention the first interesting stop or landmark coming up. "
            "Set the tone - excited, adventure ahead."
        ),
        "early_cruise": (
            "Settled into the drive. Good time for discovery - "
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
            "Nearly there! Help with arrival - "
            "where to park, what's nearby at the destination, "
            "any last info about the final stop."
        ),
    }
    phase_text = phase_guidance.get(phase, "")

    # Conditions (traffic + hazards)
    cond_parts: List[str] = []
    ts = ctx.traffic_summary
    if ts and ts.get("total", 0) > 0:
        cond_parts.append(f"Traffic: {ts['total']} events on route")
        for s in ts.get("sample", [])[:3]:
            cond_parts.append(
                f"  • {s.get('type', '?')}: {s.get('headline', 'no detail')[:60]}"
            )
    hs = ctx.hazards_summary
    if hs and hs.get("total", 0) > 0:
        cond_parts.append(f"Hazards: {hs['total']} active warnings")
        for h in hs.get("sample", [])[:3]:
            cond_parts.append(
                f"  • {h.get('kind', '?')}: {h.get('headline', 'no detail')[:60]}"
            )
    conditions = "\n".join(cond_parts) if cond_parts else "No active alerts."

    # Only-ahead rule
    direction_rule = ""
    if progress and progress.km_from_start > 0:
        direction_rule = (
            f"⚠️ CRITICAL: The user is at km {progress.km_from_start:.1f}. "
            f"Do NOT suggest places behind them. Only recommend places AHEAD on the route."
        )

    # Places block
    places_block = _format_relevant_places(relevant_places)

    # Tool availability
    tool_hints: List[str] = []
    if ctx.corridor_key:
        tool_hints.append(
            f"✅ corridor_key: {ctx.corridor_key} - can use places_corridor"
        )
    else:
        tool_hints.append(
            "⚠️ No corridor_key - cannot use places_corridor. Use places_search instead."
        )
    if ctx.geometry:
        tool_hints.append(
            f"✅ Route geometry ({len(ctx.geometry)} chars) - can use places_suggest"
        )
    else:
        tool_hints.append("⚠️ No geometry - cannot use places_suggest.")
    tools_info = "\n".join(f"  {h}" for h in tool_hints)

    # ── INTELLIGENCE LAYERS ────────────────────────────────
    # Regional knowledge
    regional_block = ""
    if progress:
        regions = _detect_regions(progress.user_lat, progress.user_lng)
        if regions:
            regional_parts = [
                f"📍 REGIONAL KNOWLEDGE - {r['name']}:\n{r['knowledge']}"
                for r in regions[:2]  # max 2 overlapping regions
            ]
            regional_block = "\n\n".join(regional_parts)
    elif ctx.stops:
        # Use first stop coords as fallback for planning phase
        first = ctx.stops[0]
        lat = first.get("lat", 0)
        lng = first.get("lng", 0)
        if lat and lng:
            regions = _detect_regions(lat, lng)
            if regions:
                regional_parts = [
                    f"📍 REGIONAL KNOWLEDGE - {r['name']}:\n{r['knowledge']}"
                    for r in regions[:2]
                ]
                regional_block = "\n\n".join(regional_parts)

    # Seasonal intelligence
    seasonal_block = ""
    try:
        if progress and progress.local_time_iso:
            dt = datetime.fromisoformat(
                progress.local_time_iso.replace("Z", "+00:00")
            )
            seasonal_block = _seasonal_knowledge(
                dt.month, progress.user_lat, progress.user_lng
            )
        else:
            # Use current server time as fallback
            now = datetime.now()
            lat = ctx.stops[0].get("lat", -27.5) if ctx.stops else -27.5
            lng = ctx.stops[0].get("lng", 153.0) if ctx.stops else 153.0
            seasonal_block = _seasonal_knowledge(now.month, lat, lng)
    except Exception:
        pass

    # Vehicle context
    vehicle_block = _vehicle_context(ctx.profile, ctx.stops)

    # Companion hints (from conversation history)
    companion_block = _companion_hints(thread or [], ctx.stops)

    # Proactive nudges
    nudge_block = _proactive_nudges(progress, total_km, ctx, relevant_places)

    # ── ASSEMBLE THE PROMPT ────────────────────────────────
    return f"""You are Roam Guide - the ultimate road trip companion for an Australian navigation app.

You're like a deeply knowledgeable mate riding shotgun who's driven every highway, camped every free camp, knows every bakery, and has stories about every town. You know the roads, the hidden gems, the safety stuff, and the local culture - but you're warm, concise, and never preachy. This is a mobile screen; every word must earn its place.

You combine the practical knowledge of a seasoned outback driver with the curiosity of a great travel companion. You know when to suggest an epic detour and when to say "mate, just pull over and rest."

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

Conditions:
{conditions}

{vehicle_block}

{direction_rule}

═══ WHAT YOU KNOW ABOUT PLACES ═══
Pre-filtered places relevant to the conversation:
{places_block}

═══ TOOL AVAILABILITY ═══
{tools_info}
Available tools:
1. places_search - search by bbox, center+radius, text query, or categories. Good for "near me", "in [town name]".
2. places_corridor - search along the route corridor. Needs corridor_key. Good for "along my route", "between here and [stop]".
3. places_suggest - sample highlights at regular intervals along route. Needs geometry. Good for "what are the highlights?", "plan my stops".

═══ CATEGORY VOCABULARY ═══
Valid category filters for tools:
  Safety: fuel, ev_charging, rest_area, toilet, water, dump_point, mechanic, hospital, pharmacy
  Supplies: grocery, town, atm, laundromat
  Food: bakery, cafe, restaurant, fast_food, pub, bar
  Accommodation: camp, hotel, motel, hostel
  Nature: viewpoint, waterfall, swimming_hole, beach, national_park, hiking, picnic, hot_spring
  Family: playground, pool, zoo, theme_park
  Culture: visitor_info, museum, gallery, heritage, winery, brewery, attraction, market, park

{"═══ PROACTIVE NUDGES (weave in naturally) ═══" + chr(10) + nudge_block if nudge_block else ""}

{"═══ SEASONAL INTELLIGENCE ═══" + chr(10) + seasonal_block if seasonal_block else ""}

{"═══ REGIONAL KNOWLEDGE ═══" + chr(10) + regional_block if regional_block else ""}

{"═══ TRAVEL PARTY ═══" + chr(10) + companion_block if companion_block else ""}

═══ RESPONSE RULES ═══
1. CONCISE: 2-4 sentences max for simple queries. Mobile screen. No essays.
2. STRUCTURED: When recommending multiple places, use a compact list with name, distance, and one useful detail.
3. SAFETY: Proactively mention fuel/water/fatigue/wildlife when relevant - but once per topic, don't nag.
4. ACTIONS: Include UI actions for recommended places:
   - If place has website: {{"type":"web", "label":"Website · PlaceName", "place_id":"id", "place_name":"name", "url":"https://..."}}
   - If place has phone: {{"type":"call", "label":"Call PlaceName", "place_id":"id", "place_name":"name", "tel":"04..."}}
   - Max one web + one call action per place. Only for places you actually recommend.
5. NEVER INVENT: Only recommend places from the pre-filtered list or tool results. Never fabricate names, hours, or phone numbers.
6. DONE FLAG: Set done=true when giving a final recommendation. Set done=false ONLY when emitting a tool_call.
7. TOOL CALLS: Max ONE per turn. Include brief text like "Searching for camps ahead…" while the tool runs.
8. LOCAL COLOUR: When you know something about a place from the regional knowledge, share it! "That bakery in Yamba is famous for..." This is what makes you feel like a knowledgeable companion, not a search engine.
9. CONVERSATIONAL: If the user just says hi or asks general questions, be friendly. Offer to help find stops, food, fuel, or things to see based on what phase of the trip they're in.
10. HONEST: If you don't have data on something, say so and offer to search. Never guess at opening hours, phone numbers, or prices.
11. CULTURALLY RESPECTFUL: When mentioning indigenous heritage sites, acknowledge Traditional Owners. Some places are sacred - note access restrictions where known.
12. WEATHER-AWARE: Factor in the seasonal context when making suggestions. Don't recommend swimming holes in stinger season, don't suggest the Gibb in the wet.

═══ DEEP AUSTRALIAN ROAD TRIP KNOWLEDGE ═══
- Fuel: Outback stations 200-500km+ apart. Last fuel signs are serious - believe them. Some remote stations have limited hours or card-only after hours. Fuel is $0.50-1.00/L more in remote areas.
- Rest areas: Free roadside rest stops on every highway. Many have toilets, some BBQs and shelters. Some are stunning - shady river spots, lookouts. Rest areas are marked on maps.
- Bakeries: THE quintessential Aussie road stop. Country bakeries are cultural institutions. Pies (mince, pepper steak, curry), sausage rolls, vanilla slices, lamingtons. If a town has a bakery, stop there.
- Wildlife: Kangaroos, wombats, echidnas, emus on roads. Worst at dawn/dusk/night. Hitting a roo at 110km/h totals a car. Cattle grids = livestock area. Road trains have bull bars for a reason.
- Free camps: Many are beautiful - cleared areas by rivers/creeks with basic facilities. WikiCamps and Camps Australia Wide apps are the bibles. Some require self-registration or small fees. Leave no trace.
- Dump points: Essential for RV/caravan. Most service stations in towns have them. Some are free, some charge $5-10. The dump point directory on cmca.net.au is comprehensive.
- Towns: Even tiny towns (pop. 50-200) may have a pub serving meals, a fuel bowser, and local character. They're the social hubs of rural Australia. The pub is always the place to ask for local knowledge.
- Water: Tap water is safe in all towns. In the outback, bore water is often mineral-heavy (safe but tastes different). NEVER drink from rivers/creeks without purifying - giardia is common.
- Wineries/breweries: Many regions have cellar doors open for tastings. Designated driver needed. Some offer cheese/chocolate/olive oil tastings too. Regional food trails are excellent.
- Swimming holes: The best ones are local secrets. Not all are safe - check for crocs north of Rockhampton, check for blue-green algae in inland waterways, check for currents. If in doubt, ask locals.
- Time zones: QLD has no daylight saving. NSW/VIC/TAS/ACT do (Oct-Apr). SA is +30min offset. NT stays on CST. WA is 2-3hrs behind east coast. Crossing borders can be confusing - phones auto-adjust.
- Road trains: Up to 53.5m long. They cannot stop quickly. NEVER overtake unless you can see clear road for 1km+. Pull over to the left, slow down, and let them pass. The draft will shake your car.
- Gravel roads: Reduce speed to 80km/h or less. Stones chip paint and windscreens. Pull left when other vehicles approach to reduce stone damage. Dust takes time to settle after a vehicle passes.
- Indigenous communities: Many have alcohol restrictions. Some require entry permits (especially in NT and Cape York). Respect "no photos" signs. Ask before photographing people. Support indigenous-owned businesses and guided tours.
- Mobile coverage: Drops to zero between towns in rural Australia. Telstra has the best rural coverage. Download offline maps. Carry a satellite phone or PLB (Personal Locator Beacon) in truly remote areas.
- Emergency: 000 is the emergency number. In remote areas, EPIRB/PLB activation is often faster. Royal Flying Doctor Service covers the outback. Most roadhouses can relay emergency calls via HF radio.
"""


# ══════════════════════════════════════════════════════════════
# USER MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════

_MAX_THREAD = 12
_MAX_TOOL_RESULTS = 4
_MAX_PLACES_PER_RESULT = 15


def _summarize_tool_result(tr: GuideToolResult) -> Dict[str, Any]:
    """Compress tool results for LLM - extract the useful bits, drop the bulk."""
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
            # Rich extras
            extra = p.get("extra", {})
            if isinstance(extra, dict):
                tags = extra.get("tags", extra)
                suburb = (
                    tags.get("addr:suburb")
                    or tags.get("addr:city")
                    or tags.get("addr:town")
                )
                if suburb:
                    entry["suburb"] = str(suburb)[:40]
                # Hours
                hours = tags.get("opening_hours")
                if hours:
                    entry["hours"] = str(hours)[:50]
                # Phone
                phone = tags.get("phone") or tags.get("contact:phone")
                if phone:
                    entry["phone"] = str(phone)[:20]
                # Website
                website = tags.get("website") or tags.get("contact:website")
                if website:
                    entry["website"] = str(website)[:80]
                # Fuel types
                fuel_types = [
                    k.replace("fuel:", "")
                    for k in tags
                    if k.startswith("fuel:") and tags[k] == "yes"
                ]
                if fuel_types:
                    entry["fuel_types"] = fuel_types[:4]
                # EV sockets
                ev_sockets = tags.get("socket:type2") or tags.get("socket:chademo")
                if ev_sockets:
                    entry["ev"] = True
                # Camping extras
                fee = tags.get("fee")
                if fee:
                    entry["fee"] = "free" if fee == "no" else "paid"
                water = tags.get("drinking_water")
                if water:
                    entry["water"] = water == "yes"
                toilets = tags.get("toilets")
                if toilets:
                    entry["toilets"] = toilets == "yes"
            compact.append(entry)

        out["total_found"] = total
        out["showing"] = len(compact)
        out["places"] = compact

    elif tr.tool == "places_suggest":
        clusters = result.get("clusters", [])
        compact_clusters: List[Dict[str, Any]] = []
        for cl in clusters[:6]:
            cl_places = cl.get("places", {}).get("items", [])
            compact_places: List[Dict[str, Any]] = []
            for p in cl_places[:5]:
                entry = {
                    "name": p.get("name", "?"),
                    "cat": p.get("category", "?"),
                }
                extra = p.get("extra", {})
                if isinstance(extra, dict):
                    tags = extra.get("tags", extra)
                    hours = tags.get("opening_hours")
                    if hours:
                        entry["hours"] = str(hours)[:40]
                    phone = tags.get("phone") or tags.get("contact:phone")
                    if phone:
                        entry["phone"] = str(phone)[:20]
                    website = tags.get("website") or tags.get("contact:website")
                    if website:
                        entry["website"] = str(website)[:80]
                compact_places.append(entry)
            compact_clusters.append(
                {
                    "km": cl.get("km_from_start", 0),
                    "total": len(cl_places),
                    "sample": compact_places,
                }
            )
        out["clusters"] = compact_clusters

    return out


def _build_user_message(req: GuideTurnRequest) -> str:
    """Build a compact user message with thread + tool results."""
    parts: List[str] = []

    # Thread (last N messages)
    thread_msgs = req.thread[-_MAX_THREAD:]
    if thread_msgs:
        parts.append("=== CONVERSATION ===")
        for m in thread_msgs:
            role = "USER" if m.role == "user" else "GUIDE"
            parts.append(f"{role}: {m.content}")

    # Tool results
    tool_results = req.tool_results[-_MAX_TOOL_RESULTS:]
    if tool_results:
        parts.append("\n=== TOOL RESULTS ===")
        for tr in tool_results:
            summary = _summarize_tool_result(tr)
            parts.append(json.dumps(summary, separators=(",", ":")))

    # Category preferences
    if req.preferred_categories:
        parts.append(
            f"\n[User has filter active: {', '.join(req.preferred_categories)}]"
        )

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════
# OUTPUT NORMALIZATION
# ══════════════════════════════════════════════════════════════

def _normalize_model_output(raw: Any) -> Dict[str, Any]:
    """Normalize the LLM's JSON output to guaranteed shape."""
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

    # Normalize tool call IDs
    norm_calls: List[Dict[str, Any]] = []
    for i, tc in enumerate(raw["tool_calls"]):
        if not isinstance(tc, dict):
            continue
        tc.setdefault("id", f"tc_{i}_{uuid.uuid4().hex[:8]}")
        norm_calls.append(tc)
    raw["tool_calls"] = norm_calls

    # Normalize actions
    norm_actions: List[Dict[str, Any]] = []
    for a in raw["actions"]:
        if not isinstance(a, dict):
            continue
        if a.get("type") in ("web", "call") and a.get("label"):
            norm_actions.append(a)
    raw["actions"] = norm_actions

    return raw


# ══════════════════════════════════════════════════════════════
# TOOL REQUEST VALIDATION & REPAIR
# ══════════════════════════════════════════════════════════════

def _repair_req(
    tool: str, req: Dict[str, Any], ctx: GuideContext
) -> Dict[str, Any]:
    """
    Repair common LLM mistakes in tool requests:
    - Missing corridor_key for places_corridor
    - Missing geometry for places_suggest
    - Invalid category strings
    - Missing required fields
    """
    req = dict(req)  # shallow copy

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
        # If LLM provided lat/lng but no center object, wrap it
        if "lat" in req and "lng" in req and "center" not in req:
            req["center"] = {"lat": req.pop("lat"), "lng": req.pop("lng")}
        req.setdefault("limit", 20)
        if "center" in req:
            req.setdefault("radius_m", 15000)

    # Normalize categories
    if "categories" in req and isinstance(req["categories"], list):
        req["categories"] = [str(c).lower().strip() for c in req["categories"] if c]

    return req


def _validate_tool_req(tool: str, req: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate a repaired tool request. Returns (ok, error_message)."""
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
# SERVICE
# ══════════════════════════════════════════════════════════════

class GuideService:
    def __init__(self) -> None:
        self._api_key = settings.openai_api_key
        self._model = settings.openai_model
        self._base = settings.openai_base_url.rstrip("/")
        self._timeout = float(
            getattr(settings, "guide_timeout_s", None)
            or getattr(settings, "explore_timeout_s", None)
            or 30.0
        )

    async def turn(self, req: GuideTurnRequest) -> GuideTurnResponse:
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY missing")

        sys_prompt = _build_system_prompt(
            req.context, req.relevant_places, req.thread
        )
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
                raise RuntimeError(
                    f"OpenAI /responses {r.status_code}: {r.text[:500]}"
                )
            data = r.json()

        # Extract output text
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
            raise RuntimeError(
                f"Guide LLM: invalid JSON: {e}. text={out_text[:400]}"
            )

        norm = _normalize_model_output(raw)

        # Process tool calls - validate and repair
        tool_calls = norm.get("tool_calls") or []
        validated_calls: List[Dict[str, Any]] = []

        if tool_calls:
            # Take only the first tool call (one at a time rule)
            tc0 = tool_calls[0]
            tool = tc0.get("tool")
            req_obj = tc0.get("req") if isinstance(tc0.get("req"), dict) else {}
            tc_id = tc0.get("id") or f"tc_{uuid.uuid4().hex[:8]}"

            if tool not in ("places_search", "places_corridor", "places_suggest"):
                # Invalid tool - strip it
                norm["assistant"] = (
                    (norm.get("assistant") or "")
                    + "\n\n(I couldn't find the right tool. Let me try differently.)"
                )
            else:
                fixed_req = _repair_req(tool, req_obj, req.context)
                ok, err = _validate_tool_req(tool, fixed_req)

                if ok:
                    validated_calls.append(
                        {"id": tc_id, "tool": tool, "req": fixed_req}
                    )
                else:
                    norm["assistant"] = (
                        (norm.get("assistant") or "")
                        + "\n\n(Couldn't build a valid search - let me try again.)"
                    )

        # Build response
        response_calls: List[GuideToolCall] = []
        for vc in validated_calls:
            response_calls.append(
                GuideToolCall(
                    id=vc["id"],
                    tool=vc["tool"],
                    req=vc["req"],
                )
            )

        # Build actions
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