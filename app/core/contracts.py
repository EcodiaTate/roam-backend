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
# Navigation
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
    geometry: str  # Polyline6


class NavRoute(BaseModel):
    route_key: str
    profile: str
    distance_m: int
    duration_s: int
    geometry: str  # Polyline6 (full)
    bbox: BBox4
    legs: List[NavLeg]
    provider: str  # "osrm"
    created_at: str  # ISO8601 UTC
    algo_version: str


class RouteAlternates(BaseModel):
    alternates: List[NavRoute] = Field(default_factory=list)


class NavPack(BaseModel):
    req: NavRequest
    primary: NavRoute
    alternates: RouteAlternates = Field(default_factory=RouteAlternates)


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
# Places
# ──────────────────────────────────────────────────────────────

PlaceCategory = Literal[
    "fuel", "camp", "water", "toilet", "town",
    "grocery", "mechanic", "hospital", "pharmacy",
    "viewpoint",
    "cafe", "restaurant", "fast_food",
    "pub", "bar",
    "hotel", "motel", "hostel",
    "attraction", "park", "beach",
    # Mapbox geocoding types
    "address", "place", "region",
     "waterfall",
    "swimming_hole",
    "national_park",
    "picnic",
    "hiking",
    "museum",
    "gallery",
    "zoo",
    "theme_park",
    "heritage",
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
    # ── NEW: route polyline for true corridor search ──
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


class GuideMsg(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class GuideContext(BaseModel):
    plan_id: Optional[str] = None
    label: Optional[str] = None

    profile: Optional[str] = None
    route_key: Optional[str] = None
    corridor_key: Optional[str] = None

    geometry: Optional[str] = None  # polyline6
    bbox: Optional[Dict[str, Any]] = None  # BBox4-ish dict

    stops: List[Dict[str, Any]] = Field(default_factory=list)

    manifest_route_key: Optional[str] = None
    offline_stale: Optional[bool] = None

    traffic_summary: Optional[Dict[str, Any]] = None
    hazards_summary: Optional[Dict[str, Any]] = None


class GuideToolCall(BaseModel):
    # allow missing id so model output doesn't hard-fail; we fill it server-side
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


class GuideTurnResponse(BaseModel):
    # allow missing assistant text; we fill to "" if absent
    assistant: str = ""
    tool_calls: List[GuideToolCall] = Field(default_factory=list)
    done: bool = False


# ──────────────────────────────────────────────────────────────
# Traffic + Hazards overlays
# ──────────────────────────────────────────────────────────────

TrafficSeverity = Literal["info", "minor", "moderate", "major", "unknown"]
TrafficType = Literal["hazard", "closure", "congestion", "roadworks", "flooding", "incident", "unknown"]

HazardSeverity = Literal["low", "medium", "high", "unknown"]
HazardKind = Literal["flood", "cyclone", "storm", "fire", "wind", "heat", "marine", "weather_warning", "unknown"]

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
    geometry: Optional[GeoJSON] = None
    bbox: Optional[List[float]] = None
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
    title: str
    description: Optional[str] = None
    url: Optional[str] = None
    issued_at: Optional[str] = None
    start_at: Optional[str] = None
    end_at: Optional[str] = None
    geometry: Optional[GeoJSON] = None
    bbox: Optional[List[float]] = None
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
