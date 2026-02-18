from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


# ──────────────────────────────────────────────────────────────
# Shared
# ──────────────────────────────────────────────────────────────

class NavCoord(BaseModel):
    lat: float
    lng: float


class TripStop(BaseModel):
    id: Optional[str] = None
    type: Literal["start", "poi", "via", "end"] = "poi"
    name: Optional[str] = None
    lat: float
    lng: float


class BBox4(BaseModel):
    minLng: float
    minLat: float
    maxLng: float
    maxLat: float


# ──────────────────────────────────────────────────────────────
# Navigation — Maneuvers & Steps (turn-by-turn)
# ──────────────────────────────────────────────────────────────

ManeuverType = Literal[
    "turn", "depart", "arrive",
    "merge", "fork", "on ramp", "off ramp",
    "roundabout", "rotary", "exit roundabout",
    "new name", "continue", "end of road",
    "notification",
]

ManeuverModifier = Literal[
    "left", "right",
    "slight left", "slight right",
    "sharp left", "sharp right",
    "straight", "uturn",
]


class NavManeuver(BaseModel):
    type: ManeuverType = "turn"
    modifier: Optional[ManeuverModifier] = None
    location: List[float]           # [lng, lat] — OSRM convention
    bearing_before: int = 0
    bearing_after: int = 0
    exit: Optional[int] = None      # roundabout exit number


class NavStep(BaseModel):
    maneuver: NavManeuver
    name: str                       # road name ("Bruce Highway", "")
    ref: Optional[str] = None       # route reference ("M1", "A1")
    distance_m: float
    duration_s: float
    geometry: str                   # polyline6 for this step's segment
    mode: str = "driving"
    pronunciation: Optional[str] = None  # phonetic road name for TTS


# ──────────────────────────────────────────────────────────────
# Navigation — Core route models
# ──────────────────────────────────────────────────────────────

class NavRequest(BaseModel):
    profile: str = "drive"
    prefs: Dict[str, Any] = Field(default_factory=dict)
    stops: List[TripStop]
    avoid: List[str] = Field(default_factory=list)
    depart_at: Optional[str] = None  # ISO8601 UTC recommended


class NavLeg(BaseModel):
    idx: int
    from_stop_id: Optional[str] = None
    to_stop_id: Optional[str] = None
    distance_m: int
    duration_s: int
    geometry: str                   # Polyline6 (this leg only)
    steps: List[NavStep] = Field(default_factory=list)


class NavRoute(BaseModel):
    route_key: str
    profile: str
    distance_m: int
    duration_s: int
    geometry: str                   # Polyline6 (full route)
    bbox: BBox4
    legs: List[NavLeg]
    provider: str                   # "osrm"
    created_at: str                 # ISO8601 UTC
    algo_version: str


class RouteAlternates(BaseModel):
    alternates: List[NavRoute] = Field(default_factory=list)


class NavPack(BaseModel):
    req: NavRequest
    primary: NavRoute
    alternates: RouteAlternates = Field(default_factory=RouteAlternates)


# ──────────────────────────────────────────────────────────────
# Elevation profiles
# ──────────────────────────────────────────────────────────────

class ElevationRequest(BaseModel):
    geometry: str                   # polyline6
    sample_interval_m: int = 500    # sample every N metres
    route_key: Optional[str] = None


class ElevationSample(BaseModel):
    km_along: float
    elevation_m: float
    lat: float
    lng: float


class ElevationProfile(BaseModel):
    route_key: Optional[str] = None
    samples: List[ElevationSample]
    min_elevation_m: float
    max_elevation_m: float
    total_ascent_m: float
    total_descent_m: float
    created_at: str


class GradeSegment(BaseModel):
    """Derived segment for elevation-aware fuel analysis."""
    from_km: float
    to_km: float
    avg_grade_pct: float            # positive = uphill, negative = downhill
    elevation_change_m: float
    fuel_penalty_factor: float      # 1.0 = flat, 1.35 = steep uphill, 0.85 = steep downhill


# ──────────────────────────────────────────────────────────────
# Corridor graphs
# ──────────────────────────────────────────────────────────────

class CorridorGraphMeta(BaseModel):
    corridor_key: str
    route_key: str
    profile: str
    buffer_m: int
    max_edges: int
    algo_version: str
    created_at: str
    bytes: int


class CorridorNode(BaseModel):
    id: int
    lat: float
    lng: float


class CorridorEdge(BaseModel):
    a: int
    b: int
    distance_m: int
    duration_s: int
    flags: int = 0


class CorridorGraphPack(BaseModel):
    corridor_key: str
    route_key: str
    profile: str
    algo_version: str
    bbox: BBox4
    nodes: List[CorridorNode] = Field(default_factory=list)
    edges: List[CorridorEdge] = Field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# Places — category taxonomy
# ──────────────────────────────────────────────────────────────
# Every category MUST map to at least one Overpass filter in
# places.py _FALLBACK_FILTERS.  Grouped by traveller need.
#
# ESSENTIALS & SAFETY — the "survive the outback" tier
#   fuel          Petrol/diesel stations
#   ev_charging   Electric vehicle charge points
#   rest_area     Roadside rest stops / driver reviver (fatigue!)
#   toilet        Public toilets (standalone, not inside a venue)
#   water         Drinking water taps, tanks, bores
#   dump_point    Caravan/RV waste dump stations
#   mechanic      Car repair, tyre shops, NRMA/RACQ
#   hospital      Hospitals & emergency departments
#   pharmacy      Chemists
#
# SUPPLIES
#   grocery       Supermarkets, IGA, convenience stores
#   town          Towns, villages, hamlets (anchor points)
#   atm           ATMs & bank branches (cash-only outback shops)
#   laundromat    Laundromats / coin laundry (multi-day trips)
#
# FOOD & DRINK
#   bakery        Bakeries (THE Aussie road trip stop — pies!)
#   cafe          Cafés & coffee shops
#   restaurant    Sit-down restaurants
#   fast_food     Fast food / takeaway
#   pub           Pubs & taverns (counter meals)
#   bar           Bars & cocktail lounges
#
# ACCOMMODATION
#   camp          Camp sites, caravan parks, free camps
#   hotel         Hotels
#   motel         Motels (roadside, quintessential road trip)
#   hostel        Hostels / backpackers
#
# NATURE & OUTDOORS
#   viewpoint     Lookouts & scenic viewpoints
#   waterfall     Waterfalls
#   swimming_hole Natural swimming holes & rock pools
#   beach         Beaches
#   national_park National parks & nature reserves
#   hiking        Walking tracks, trails, trailheads
#   picnic        Picnic areas, BBQ spots, shelters
#   hot_spring    Hot springs & thermal pools
#
# FAMILY & RECREATION
#   playground    Playgrounds & skate parks
#   pool          Public swimming pools & aquatic centres
#   zoo           Zoos, wildlife parks, sanctuaries
#   theme_park    Theme parks, water parks, mini golf
#
# CULTURE & SIGHTSEEING
#   visitor_info  Visitor information centres / i-sites
#   museum        Museums
#   gallery       Art galleries
#   heritage      Heritage-listed sites, historic buildings
#   winery        Wineries & cellar doors
#   brewery       Breweries, distilleries, cideries
#   attraction    Generic tourist attractions, "Big Things"
#   market        Markets (farmers, craft, weekend)
#   park          Urban parks & gardens
#
# GEOCODING (from Mapbox forward search — not Overpass)
#   address       Street address result
#   place         Named place / locality result
#   region        State / territory result
# ──────────────────────────────────────────────────────────────

PlaceCategory = Literal[
    # Essentials & safety
    "fuel", "ev_charging", "rest_area", "toilet", "water",
    "dump_point", "mechanic", "hospital", "pharmacy",
    # Supplies
    "grocery", "town", "atm", "laundromat",
    # Food & drink
    "bakery", "cafe", "restaurant", "fast_food", "pub", "bar",
    # Accommodation
    "camp", "hotel", "motel", "hostel",
    # Nature & outdoors
    "viewpoint", "waterfall", "swimming_hole", "beach",
    "national_park", "hiking", "picnic", "hot_spring",
    # Family & recreation
    "playground", "pool", "zoo", "theme_park",
    # Culture & sightseeing
    "visitor_info", "museum", "gallery", "heritage",
    "winery", "brewery", "attraction", "market", "park",
    # Geocoding (Mapbox)
    "address", "place", "region",
]


class PlacesRequest(BaseModel):
    bbox: Optional[BBox4] = None
    center: Optional[NavCoord] = None
    radius_m: Optional[int] = None
    categories: List[PlaceCategory] = Field(default_factory=list)
    query: Optional[str] = None
    limit: int = 50


class PlaceItem(BaseModel):
    id: str
    name: str
    lat: float
    lng: float
    category: PlaceCategory
    extra: Dict[str, Any] = Field(default_factory=dict)


class PlacesPack(BaseModel):
    places_key: str
    req: PlacesRequest
    items: List[PlaceItem]
    provider: str
    created_at: str
    algo_version: str


class CorridorPlacesRequest(BaseModel):
    corridor_key: str
    categories: Optional[List[PlaceCategory]] = None
    limit: Optional[int] = None
    # Route polyline for true corridor search
    geometry: Optional[str] = None          # Polyline6 of the route
    buffer_km: Optional[float] = 15.0       # Corridor buffer radius in km


class PlacesSuggestRequest(BaseModel):
    geometry: str  # polyline6
    interval_km: int = 50
    radius_m: int = 15000
    categories: List[PlaceCategory] = Field(default_factory=list)
    limit_per_sample: int = 150


class PlacesSuggestionCluster(BaseModel):
    idx: int
    lat: float
    lng: float
    km_from_start: float
    places: PlacesPack


class PlacesSuggestResponse(BaseModel):
    clusters: List[PlacesSuggestionCluster]


# ──────────────────────────────────────────────────────────────
# Guide (LLM-driven companion)
# ──────────────────────────────────────────────────────────────

GuideToolName = Literal["places_search", "places_corridor", "places_suggest"]
GuideActionType = Literal["web", "call"]


class GuideMsg(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class TripProgress(BaseModel):
    """Live position + progress telemetry sent from frontend."""
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
    """
    Pre-filtered "relevant places" the server hands to the LLM so it
    can recommend without a tool call.  Includes contact info for
    action buttons.
    """
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
    website: Optional[str] = None


class GuideContext(BaseModel):
    plan_id: Optional[str] = None
    label: Optional[str] = None

    profile: Optional[str] = None
    route_key: Optional[str] = None
    corridor_key: Optional[str] = None

    geometry: Optional[str] = None  # polyline6
    bbox: Optional[Dict[str, Any]] = None  # BBox4-ish dict

    stops: List[Dict[str, Any]] = Field(default_factory=list)
    total_distance_m: Optional[float] = None
    total_duration_s: Optional[float] = None

    manifest_route_key: Optional[str] = None
    offline_stale: Optional[bool] = None

    progress: Optional[TripProgress] = None

    traffic_summary: Optional[Dict[str, Any]] = None
    hazards_summary: Optional[Dict[str, Any]] = None


class GuideAction(BaseModel):
    """Structured UI action rendered as a button/pill in the chat."""
    type: GuideActionType
    label: str
    place_id: Optional[str] = None
    place_name: Optional[str] = None
    url: Optional[str] = None
    tel: Optional[str] = None


class GuideToolCall(BaseModel):
    id: Optional[str] = None
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
    actions: List[GuideAction] = Field(default_factory=list)
    tool_calls: List[GuideToolCall] = Field(default_factory=list)
    done: bool = False


# ──────────────────────────────────────────────────────────────
# Traffic + Hazards overlays
# ──────────────────────────────────────────────────────────────

TrafficSeverity = Literal["info", "minor", "moderate", "major", "unknown"]
TrafficType = Literal["hazard", "closure", "congestion", "roadworks", "flooding", "incident", "unknown"]

HazardSeverity = Literal["low", "medium", "high", "unknown"]
HazardKind = Literal["flood", "cyclone", "storm", "fire", "wind", "heat", "marine", "weather_warning", "unknown"]

# CAP-AU urgency and certainty levels (used for composite severity scoring)
CapUrgency = Literal["immediate", "expected", "future", "past", "unknown"]
CapCertainty = Literal["observed", "likely", "possible", "unlikely", "unknown"]

# Route impact classification — computed client-side by intersecting alert
# geometry with the route polyline buffer.
RouteImpact = Literal[
    "blocks_route",     # closure/flood geometry intersects route within 500m
    "affects_route",    # hazard zone covers part of route, road may be passable
    "nearby",           # within corridor but not directly on route
    "informational",    # in the region but irrelevant to this specific route
]

GeoJSON = Dict[str, Any]


class TrafficEvent(BaseModel):
    id: str
    source: str
    feed: str
    type: TrafficType = "unknown"
    severity: TrafficSeverity = "unknown"
    headline: str
    description: Optional[str] = None
    url: Optional[str] = None
    last_updated: Optional[str] = None
    start_at: Optional[str] = None
    end_at: Optional[str] = None
    geometry: Optional[GeoJSON] = None
    bbox: Optional[List[float]] = None
    region: Optional[str] = None  # "qld", "nsw", "vic", etc.
    raw: Dict[str, Any] = Field(default_factory=dict)


class TrafficOverlay(BaseModel):
    traffic_key: str
    bbox: BBox4
    provider: str
    algo_version: str
    created_at: str
    items: List[TrafficEvent] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class HazardEvent(BaseModel):
    id: str
    source: str
    kind: HazardKind = "unknown"
    severity: HazardSeverity = "unknown"
    # CAP-AU composite scoring fields
    urgency: CapUrgency = "unknown"
    certainty: CapCertainty = "unknown"
    effective_priority: float = 0.0  # 0.0 (lowest) to 1.0 (highest)
    title: str
    description: Optional[str] = None
    url: Optional[str] = None
    issued_at: Optional[str] = None
    start_at: Optional[str] = None
    end_at: Optional[str] = None
    geometry: Optional[GeoJSON] = None
    bbox: Optional[List[float]] = None
    region: Optional[str] = None  # "qld", "nsw", "vic", etc.
    raw: Dict[str, Any] = Field(default_factory=dict)


class HazardOverlay(BaseModel):
    hazards_key: str
    bbox: BBox4
    provider: str
    algo_version: str
    created_at: str
    items: List[HazardEvent] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# Offline bundles
# ──────────────────────────────────────────────────────────────

AssetStatus = Literal["missing", "ready", "error"]


class OfflineBundleManifest(BaseModel):
    plan_id: str
    route_key: str

    tiles_id: str = "australia"
    styles: List[str] = Field(default_factory=list)

    navpack_status: AssetStatus = "missing"
    corridor_status: AssetStatus = "missing"
    places_status: AssetStatus = "missing"
    traffic_status: AssetStatus = "missing"
    hazards_status: AssetStatus = "missing"
    elevation_status: AssetStatus = "missing"

    corridor_key: Optional[str] = None
    places_key: Optional[str] = None
    traffic_key: Optional[str] = None
    hazards_key: Optional[str] = None

    bytes_total: int = 0
    created_at: str


# ──────────────────────────────────────────────────────────────
# Sync (minimal placeholder)
# ──────────────────────────────────────────────────────────────

class SyncOp(BaseModel):
    id: str
    type: str
    payload: Dict[str, Any]
    created_at: str


class SyncOpsRequest(BaseModel):
    ops: List[SyncOp]


class SyncOpsResponse(BaseModel):
    accepted: int