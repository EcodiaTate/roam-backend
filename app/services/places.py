from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple
import collections
import concurrent.futures
import hashlib
import logging
import math
import time
import random
import re
import functools

import httpx

from app.core.contracts import PlaceItem, PlacesPack, PlacesRequest, BBox4, PlaceCategory
from app.core.keying import places_key
from app.core.time import utc_now_iso
from app.core.storage import get_places_pack, put_places_pack
from app.core.settings import settings
from app.core.polyline6 import decode_polyline6

from app.services.places_store import PlacesStore
from app.services.places_supa import SupaPlacesRepo

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Geometry helpers
# ──────────────────────────────────────────────────────────────
def _bbox_from_req(req: PlacesRequest) -> Optional[BBox4]:
    if req.bbox:
        return req.bbox
    if req.center and req.radius_m:
        dlat = req.radius_m / 111_320.0
        cosv = max(0.2, math.cos(math.radians(req.center.lat)))
        dlng = req.radius_m / (111_320.0 * cosv)
        return BBox4(
            minLng=req.center.lng - dlng,
            minLat=req.center.lat - dlat,
            maxLng=req.center.lng + dlng,
            maxLat=req.center.lat + dlat,
        )
    return None


def _haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Distance in metres between two (lat, lng) points."""
    R = 6_371_000.0
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2.0 * R * math.asin(min(1.0, math.sqrt(x)))


def _bbox_around_points(
    points: List[Tuple[float, float]], buffer_km: float
) -> BBox4:
    """Build a tight BBox4 around a set of (lat, lng) points with a km buffer."""
    lats = [p[0] for p in points]
    lngs = [p[1] for p in points]
    min_lat, max_lat = min(lats), max(lats)
    min_lng, max_lng = min(lngs), max(lngs)

    buf_deg_lat = buffer_km / 111.32
    center_lat = (min_lat + max_lat) / 2.0
    cos_v = max(0.2, math.cos(math.radians(center_lat)))
    buf_deg_lng = buffer_km / (111.32 * cos_v)

    return BBox4(
        minLat=min_lat - buf_deg_lat,
        maxLat=max_lat + buf_deg_lat,
        minLng=min_lng - buf_deg_lng,
        maxLng=max_lng + buf_deg_lng,
    )


def _min_distance_to_samples_m(
    lat: float, lng: float, samples: List[Tuple[float, float]]
) -> float:
    best = float("inf")
    for s in samples:
        d = _haversine_m((lat, lng), s)
        if d < best:
            best = d
            if d < 500.0:
                break
    return best


# ──────────────────────────────────────────────────────────────
# Route sampling (shared between corridor + suggest)
# ──────────────────────────────────────────────────────────────

def _sample_polyline(
    poly6: str, interval_km: float, *, include_endpoints: bool = True
) -> List[Tuple[float, float]]:
    pts = decode_polyline6(poly6)

    logger.debug(
        "_sample_polyline polyline_chars=%d decoded_points=%d interval_km=%s",
        len(poly6), len(pts), interval_km,
    )

    if not pts or len(pts) < 2:
        logger.debug("_sample_polyline: fewer than 2 decoded points, returning empty")
        return []

    interval_m = max(1000.0, interval_km * 1000.0)
    samples: List[Tuple[float, float]] = []
    if include_endpoints:
        samples.append((float(pts[0][0]), float(pts[0][1])))

    dist_acc = 0.0
    next_mark = interval_m
    zero_segs = 0
    nan_segs = 0

    for i in range(1, len(pts)):
        p0 = (float(pts[i - 1][0]), float(pts[i - 1][1]))
        p1 = (float(pts[i][0]), float(pts[i][1]))
        seg = _haversine_m(p0, p1)

        if seg != seg:  # NaN check
            nan_segs += 1
            continue

        if seg <= 0:
            zero_segs += 1
            continue

        while dist_acc + seg >= next_mark:
            overshoot = next_mark - dist_acc
            t = max(0.0, min(1.0, overshoot / seg))
            lat = p0[0] + (p1[0] - p0[0]) * t
            lng = p0[1] + (p1[1] - p0[1]) * t
            samples.append((lat, lng))
            next_mark += interval_m

        dist_acc += seg

    if include_endpoints:
        last = (float(pts[-1][0]), float(pts[-1][1]))
        if not samples or _haversine_m(samples[-1], last) > 500.0:
            samples.append(last)

    logger.debug(
        "_sample_polyline result: samples=%d total_dist_km=%.2f zero_segs=%d nan_segs=%d",
        len(samples), dist_acc / 1000, zero_segs, nan_segs,
    )

    expected_min = max(2, int(dist_acc / 1000.0 / interval_km) - 1)
    if len(samples) < expected_min and dist_acc > interval_m * 2:
        logger.warning(
            "_sample_polyline: expected ~%d samples but got %d - falling back to uniform pick",
            expected_min, len(samples),
        )
        n_want = max(2, int(dist_acc / 1000.0 / interval_km) + 2)
        step = max(1, len(pts) // n_want)
        samples = [(float(pts[j][0]), float(pts[j][1])) for j in range(0, len(pts), step)]
        last = (float(pts[-1][0]), float(pts[-1][1]))
        if _haversine_m(samples[-1], last) > 500.0:
            samples.append(last)
        logger.debug("_sample_polyline fallback produced %d samples", len(samples))

    return samples


def _sample_route_points(poly6: str, interval_km: int) -> List[Tuple[int, float, float, float]]:
    """Legacy wrapper kept for suggest_along_route compatibility."""
    pts = decode_polyline6(poly6)
    if not pts or len(pts) < 2:
        return []

    interval_m = max(5000.0, float(interval_km) * 1000.0)
    samples: List[Tuple[int, float, float, float]] = []
    dist_acc = 0.0
    next_mark = 0.0

    samples.append((0, float(pts[0][0]), float(pts[0][1]), 0.0))

    for i in range(1, len(pts)):
        p0 = (float(pts[i - 1][0]), float(pts[i - 1][1]))
        p1 = (float(pts[i][0]), float(pts[i][1]))
        seg = _haversine_m(p0, p1)
        if seg <= 0:
            continue

        while dist_acc + seg >= next_mark + interval_m:
            target = (next_mark + interval_m) - dist_acc
            t = max(0.0, min(1.0, target / seg))
            lat = p0[0] + (p1[0] - p0[0]) * t
            lng = p0[1] + (p1[1] - p0[1]) * t
            km_from_start = (next_mark + interval_m) / 1000.0
            idx = len(samples)
            samples.append((idx, lat, lng, km_from_start))
            next_mark += interval_m

        dist_acc += seg

    return samples


# ──────────────────────────────────────────────────────────────
# Overpass mapping + classification
# ──────────────────────────────────────────────────────────────

_FALLBACK_FILTERS: Dict[str, List[str]] = {
    # ── ESSENTIALS & SAFETY ──────────────────────────────────
    "fuel": [
        '["amenity"="fuel"]',
    ],
    "ev_charging": [
        '["amenity"="charging_station"]',
    ],
    "rest_area": [
        '["highway"="rest_area"]',
        '["amenity"="rest_area"]',
    ],
    "toilet": [
        '["amenity"="toilets"]',
    ],
    "water": [
        '["amenity"="drinking_water"]',
        '["man_made"="water_well"]',
        '["man_made"="water_tap"]',
    ],
    "dump_point": [
        '["amenity"="sanitary_dump_station"]',
        '["amenity"="waste_disposal"]["waste"="chemical"]',
    ],
    "mechanic": [
        '["shop"="car_repair"]',
        '["amenity"="car_repair"]',
        '["shop"="tyres"]',
    ],
    "hospital": [
        '["amenity"="hospital"]',
    ],
    "pharmacy": [
        '["amenity"="pharmacy"]',
    ],

    # ── SUPPLIES ──────────────────────────────────────────────
    "grocery": [
        '["shop"="supermarket"]',
        '["shop"="convenience"]',
        '["shop"="general"]',
    ],
    "town": [
        '["place"~"^(city|town|village|hamlet)$"]',
    ],
    "atm": [
        '["amenity"="atm"]',
        '["amenity"="bank"]',
    ],
    "laundromat": [
        '["shop"="laundry"]',
        '["amenity"="laundry"]',
    ],

    # ── FOOD & DRINK ─────────────────────────────────────────
    "bakery": [
        '["shop"="bakery"]',
    ],
    "cafe": [
        '["amenity"="cafe"]',
    ],
    "restaurant": [
        '["amenity"="restaurant"]',
    ],
    "fast_food": [
        '["amenity"="fast_food"]',
    ],
    "pub": [
        '["amenity"="pub"]',
    ],
    "bar": [
        '["amenity"="bar"]',
    ],

    # ── ACCOMMODATION ─────────────────────────────────────────
    "camp": [
        '["tourism"="camp_site"]',
        '["tourism"="caravan_site"]',
        '["tourism"="camp_pitch"]',
        '["tourism"="alpine_hut"]',
    ],
    "hotel": [
        '["tourism"="hotel"]',
    ],
    "motel": [
        '["tourism"="motel"]',
    ],
    "hostel": [
        '["tourism"="hostel"]',
    ],

    # ── NATURE & OUTDOORS ────────────────────────────────────
    "viewpoint": [
        '["tourism"="viewpoint"]',
    ],
    "waterfall": [
        '["waterway"="waterfall"]',
    ],
    "swimming_hole": [
        '["leisure"="swimming_area"]',
        '["natural"="water"]["sport"="swimming"]',
        '["leisure"="swimming_pool"]["access"~"^(yes|public)$"]',
        '["natural"="spring"]["bathing"="yes"]',
        '["leisure"="bathing_place"]',
    ],
    "beach": [
        '["natural"="beach"]',
        '["leisure"="beach_resort"]',
    ],
    "national_park": [
        '["boundary"="national_park"]',
        '["leisure"="nature_reserve"]',
    ],
    "hiking": [
        '["highway"="path"]["foot"="designated"]',
        '["highway"="path"]["sac_scale"]',
        '["highway"="footway"]["designation"~"walking_track|bushwalking"]',
        '["route"="hiking"]',
        '["route"="foot"]',
        '["information"="guidepost"]',
        '["tourism"="information"]["information"="route_marker"]',
        '["tourism"="wilderness_hut"]',
    ],
    "picnic": [
        '["tourism"="picnic_site"]',
        '["leisure"="picnic_table"]',
        '["amenity"="bbq"]',
    ],
    "hot_spring": [
        '["natural"="hot_spring"]',
        '["leisure"="hot_spring"]',
        '["bath:type"="hot_spring"]',
    ],
    "cave": [
        '["natural"="cave_entrance"]',
        '["tourism"="attraction"]["cave"]',
    ],
    "fishing": [
        '["leisure"="fishing"]',
        '["sport"="fishing"]',
        '["leisure"="slipway"]',
    ],
    "surf": [
        '["sport"="surfing"]',
        '["leisure"="surfing"]',
    ],

    # ── FAMILY & RECREATION ──────────────────────────────────
    "playground": [
        '["leisure"="playground"]',
    ],
    "pool": [
        '["leisure"="swimming_pool"]["access"~"^(yes|public)$"]',
        '["leisure"="water_park"]',
        '["amenity"="public_bath"]',
    ],
    "zoo": [
        '["tourism"="zoo"]',
        '["attraction"="animal"]',
        '["zoo"="petting_zoo"]',
        '["tourism"="aquarium"]',
        '["attraction"="maze"]',
    ],
    "theme_park": [
        '["tourism"="theme_park"]',
        '["leisure"="amusement_arcade"]',
        '["leisure"="miniature_golf"]',
        '["leisure"="trampoline_park"]',
        '["sport"="karting"]',
    ],
    "dog_park": [
        '["leisure"="dog_park"]',
    ],
    "golf": [
        '["leisure"="golf_course"]',
    ],
    "cinema": [
        '["amenity"="cinema"]',
    ],

    # ── CULTURE & SIGHTSEEING ────────────────────────────────
    "visitor_info": [
        '["tourism"="information"]["information"="office"]',
        '["tourism"="information"]["information"="visitor_centre"]',
    ],
    "museum": [
        '["tourism"="museum"]',
    ],
    "gallery": [
        '["tourism"="gallery"]',
    ],
    "heritage": [
        '["heritage"]',
        '["historic"="monument"]',
        '["historic"="memorial"]',
        '["historic"="ruins"]',
        '["historic"="castle"]',
        '["historic"="fort"]',
        '["historic"="archaeological_site"]',
        '["historic"="wreck"]',
        '["historic"="mine"]',
        '["historic"="mine_shaft"]',
        '["historic"="bridge"]',
    ],
    "winery": [
        '["craft"="winery"]',
        '["tourism"="wine_cellar"]',
        '["shop"="wine"]',
    ],
    "brewery": [
        '["craft"="brewery"]',
        '["craft"="distillery"]',
        '["craft"="cider"]',
        '["microbrewery"="yes"]',
    ],
    "attraction": [
        '["tourism"="attraction"]',
        '["tourism"="artwork"]',
        '["tourism"="aquarium"]',
    ],
    "market": [
        '["amenity"="marketplace"]',
        '["shop"="farm"]',
        '["shop"="deli"]',
        '["shop"="greengrocer"]',
    ],
    "park": [
        '["leisure"="park"]',
        '["leisure"="garden"]',
    ],
    "library": [
        '["amenity"="library"]',
    ],
    "showground": [
        '["leisure"="showground"]',
        '["leisure"="horse_racing"]',
    ],
}


def _category_filters(category: str) -> List[str]:
    m = getattr(settings, "places_overpass_filters", None)
    if isinstance(m, dict):
        v = m.get(category)
        if isinstance(v, list) and v:
            return [str(x) for x in v]
    return _FALLBACK_FILTERS.get(category, [])


def _overpass_filters_for_categories(cats: List[PlaceCategory]) -> List[str]:
    out: List[str] = []
    for c in cats:
        out.extend(_category_filters(str(c)))

    seen = set()
    dedup: List[str] = []
    for f in out:
        if f not in seen:
            seen.add(f)
            dedup.append(f)
    return dedup


# ──────────────────────────────────────────────────────────────
# Category inference from OSM tags
# ──────────────────────────────────────────────────────────────

def _infer_category(tags: Dict[str, Any]) -> PlaceCategory:
    a = tags.get("amenity", "")
    t = tags.get("tourism", "")
    p = tags.get("place", "")
    s = tags.get("shop", "")
    mm = tags.get("man_made", "")
    le = tags.get("leisure", "")
    n = tags.get("natural", "")
    w = tags.get("waterway", "")
    b = tags.get("boundary", "")
    hw = tags.get("highway", "")
    cr = tags.get("craft", "")
    hi = tags.get("historic", "")
    info = tags.get("information", "")

    # ── ESSENTIALS & SAFETY (highest priority) ───────────────

    if a == "fuel":
        return "fuel"
    if a == "charging_station":
        return "ev_charging"
    if hw == "rest_area" or a == "rest_area":
        return "rest_area"
    if a == "toilets":
        return "toilet"
    if a == "drinking_water" or mm in ("water_well", "water_tap"):
        return "water"
    if a == "sanitary_dump_station":
        return "dump_point"
    if s == "car_repair" or a == "car_repair" or s == "tyres":
        return "mechanic"
    if a == "hospital":
        return "hospital"
    if a == "pharmacy":
        return "pharmacy"

    # ── SUPPLIES ──────────────────────────────────────────────

    if s in ("supermarket", "convenience", "general"):
        return "grocery"
    if a == "atm" or a == "bank":
        return "atm"
    if s == "laundry" or a == "laundry":
        return "laundromat"

    # ── FOOD & DRINK ─────────────────────────────────────────

    if s == "bakery":
        return "bakery"
    if a == "cafe":
        return "cafe"
    if a == "restaurant":
        return "restaurant"
    if a == "fast_food":
        return "fast_food"
    if a == "pub":
        return "pub"
    if a == "bar":
        return "bar"

    # ── ACCOMMODATION ─────────────────────────────────────────

    if t in ("camp_site", "caravan_site", "camp_pitch", "alpine_hut"):
        return "camp"
    if t == "motel":
        return "motel"
    if t == "hotel":
        return "hotel"
    if t == "hostel":
        return "hostel"

    # ── NATURE & OUTDOORS (before generic attraction) ────────

    if w == "waterfall":
        return "waterfall"
    if n == "hot_spring" or le == "hot_spring" or tags.get("bath:type") == "hot_spring":
        return "hot_spring"
    if le in ("swimming_area", "bathing_place") or (tags.get("sport") == "swimming" and n) or (n == "spring" and tags.get("bathing") == "yes"):
        return "swimming_hole"
    if n == "beach" or le == "beach_resort":
        return "beach"
    if b == "national_park" or le == "nature_reserve":
        return "national_park"
    if t == "viewpoint":
        return "viewpoint"
    if t == "picnic_site" or le == "picnic_table" or a == "bbq":
        return "picnic"
    if (
        tags.get("route") in ("hiking", "foot")
        or (hw == "path" and tags.get("sac_scale"))
        or (hw == "path" and tags.get("foot") == "designated")
        or info == "guidepost"
        or (t == "information" and info == "route_marker")
        or t == "wilderness_hut"
    ):
        return "hiking"
    if n == "cave_entrance":
        return "cave"
    if le == "fishing" or tags.get("sport") == "fishing":
        return "fishing"
    if le == "slipway":
        return "fishing"  # boat ramps → fishing category
    if tags.get("sport") == "surfing" or le == "surfing":
        return "surf"

    # ── FAMILY & RECREATION ──────────────────────────────────

    if le == "playground":
        return "playground"
    if le in ("swimming_pool", "water_park") or a == "public_bath":
        return "pool"
    if t == "zoo" or tags.get("attraction") == "animal" or tags.get("zoo") == "petting_zoo" or tags.get("attraction") == "maze":
        return "zoo"
    if t == "theme_park" or le in ("amusement_arcade", "miniature_golf", "trampoline_park") or tags.get("sport") == "karting":
        return "theme_park"
    if le == "dog_park":
        return "dog_park"
    if le == "golf_course":
        return "golf"
    if a == "cinema":
        return "cinema"

    # ── CULTURE & SIGHTSEEING ────────────────────────────────

    if t == "information" and info in ("office", "visitor_centre"):
        return "visitor_info"
    if cr == "winery" or t == "wine_cellar" or s == "wine":
        return "winery"
    if cr in ("brewery", "distillery", "cider") or tags.get("microbrewery") == "yes":
        return "brewery"
    if t == "museum":
        return "museum"
    if t == "gallery":
        return "gallery"
    if tags.get("heritage") or hi in ("monument", "memorial", "ruins", "castle", "fort", "archaeological_site", "wreck", "mine", "mine_shaft", "bridge"):
        return "heritage"
    if a == "marketplace" or s in ("farm", "deli", "greengrocer"):
        return "market"
    if le in ("park", "garden"):
        return "park"
    if t in ("attraction", "artwork"):
        return "attraction"
    if t == "aquarium":
        return "zoo"
    if a == "library":
        return "library"
    if le in ("showground", "horse_racing"):
        return "showground"

    # ── ANCHOR POINTS ────────────────────────────────────────

    if p in ("city", "town", "village", "hamlet"):
        return "town"

    return "town"


# ──────────────────────────────────────────────────────────────
# Synthetic name generation for nameless OSM features
# ──────────────────────────────────────────────────────────────

_CATEGORY_LABELS: Dict[str, str] = {
    "fuel": "Fuel Station",
    "ev_charging": "EV Charger",
    "rest_area": "Rest Area",
    "toilet": "Public Toilet",
    "water": "Drinking Water",
    "dump_point": "Dump Point",
    "mechanic": "Mechanic",
    "hospital": "Hospital",
    "pharmacy": "Pharmacy",
    "grocery": "Grocery",
    "town": "Town",
    "atm": "ATM",
    "laundromat": "Laundromat",
    "bakery": "Bakery",
    "cafe": "Café",
    "restaurant": "Restaurant",
    "fast_food": "Fast Food",
    "pub": "Pub",
    "bar": "Bar",
    "camp": "Campground",
    "hotel": "Hotel",
    "motel": "Motel",
    "hostel": "Hostel",
    "viewpoint": "Viewpoint",
    "waterfall": "Waterfall",
    "swimming_hole": "Swimming Hole",
    "beach": "Beach",
    "national_park": "National Park",
    "hiking": "Walking Track",
    "picnic": "Picnic Area",
    "hot_spring": "Hot Spring",
    "playground": "Playground",
    "pool": "Swimming Pool",
    "zoo": "Zoo",
    "theme_park": "Theme Park",
    "visitor_info": "Visitor Info",
    "museum": "Museum",
    "gallery": "Gallery",
    "heritage": "Heritage Site",
    "winery": "Winery",
    "brewery": "Brewery",
    "attraction": "Attraction",
    "market": "Market",
    "park": "Park",
    "cave": "Cave",
    "fishing": "Fishing Spot",
    "surf": "Surf Spot",
    "dog_park": "Dog Park",
    "golf": "Golf Course",
    "cinema": "Cinema",
    "library": "Library",
    "showground": "Showground",
}


def _synthetic_name(
    category: str,
    tags: Dict[str, Any],
    lat: float,
    lon: float,
) -> str:
    base = _CATEGORY_LABELS.get(category, category.replace("_", " ").title())

    locality = (
        tags.get("addr:suburb")
        or tags.get("addr:city")
        or tags.get("addr:state")
    )
    street = tags.get("addr:street")

    if category == "picnic":
        if tags.get("amenity") == "bbq":
            base = "BBQ"
        elif tags.get("leisure") == "picnic_table":
            base = "Picnic Table"
    elif category == "water":
        if tags.get("man_made") == "water_well":
            base = "Water Well"
        elif tags.get("man_made") == "water_tap":
            base = "Water Tap"
    elif category == "camp":
        if tags.get("tourism") == "caravan_site":
            base = "Caravan Park"
        elif tags.get("tourism") == "camp_pitch":
            base = "Camp Pitch"
        elif tags.get("tourism") == "alpine_hut":
            base = "Alpine Hut"
        if tags.get("fee") == "no":
            base = f"Free {base}"
    elif category == "hiking":
        if tags.get("route") in ("hiking", "foot"):
            base = "Walking Trail"
        elif tags.get("information") == "guidepost":
            base = "Trail Marker"
        elif tags.get("tourism") == "wilderness_hut":
            base = "Bush Hut"
    elif category == "heritage":
        ht = tags.get("historic", "")
        if ht == "monument":
            base = "Monument"
        elif ht == "memorial":
            base = "Memorial"
        elif ht == "ruins":
            base = "Ruins"
        elif ht == "mine" or ht == "mine_shaft":
            base = "Historic Mine"
        elif ht == "wreck":
            base = "Shipwreck"
        elif ht == "fort":
            base = "Fort"
        elif ht == "archaeological_site":
            base = "Archaeological Site"
        elif ht == "bridge":
            base = "Historic Bridge"
    elif category == "fishing":
        if tags.get("leisure") == "slipway":
            base = "Boat Ramp"
    elif category == "toilet":
        if tags.get("access") == "customers":
            base = "Customer Toilet"
        if tags.get("wheelchair") == "yes":
            base = f"{base} (Accessible)"

    if locality:
        return f"{base} - {locality}"
    elif street:
        return f"{base} - {street}"
    else:
        return base


def _element_to_item(el: Dict[str, Any]) -> Optional[PlaceItem]:
    tags = el.get("tags") or {}

    lat = el.get("lat")
    lon = el.get("lon")
    if lat is None or lon is None:
        center = el.get("center")
        if not center:
            return None
        lat = center.get("lat")
        lon = center.get("lon")
        if lat is None or lon is None:
            return None

    osm_type = el.get("type", "node")
    osm_id = el.get("id")
    if osm_id is None:
        return None

    category = _infer_category(tags)

    name = (
        tags.get("name")
        or tags.get("brand")
        or tags.get("operator")
        or tags.get("description")
        or tags.get("loc_name")
        or tags.get("alt_name")
        or tags.get("ref")
    )

    if not name:
        name = _synthetic_name(category, tags, lat, lon)

    extra: Dict[str, Any] = {"osm_type": osm_type, "osm_id": osm_id}

    for k in ("phone", "contact:phone", "website", "contact:website",
              "opening_hours", "fee", "access", "capacity",
              "brand", "operator", "description"):
        v = tags.get(k)
        if v:
            clean_k = k.replace("contact:", "")
            extra[clean_k] = str(v)[:200]

    addr_parts = []
    for ak in ("addr:housenumber", "addr:street", "addr:suburb",
                "addr:city", "addr:state", "addr:postcode"):
        av = tags.get(ak)
        if av:
            addr_parts.append(str(av))
    if addr_parts:
        extra["address"] = ", ".join(addr_parts)

    fuel_types = []
    for fk in ("fuel:diesel", "fuel:octane_91", "fuel:octane_95",
                "fuel:octane_98", "fuel:lpg", "fuel:adblue"):
        if tags.get(fk) == "yes":
            fuel_types.append(fk.replace("fuel:", ""))
    if fuel_types:
        extra["fuel_types"] = fuel_types
        # Convenience booleans so the client doesn't have to parse the array.
        # Only set False when we have explicit fuel_types data and the type is absent.
        # Leave unset (undefined on client) if no fuel_types data at all, so the
        # client doesn't incorrectly exclude stations with missing OSM tags.
        extra["has_diesel"] = "diesel" in fuel_types
        extra["has_unleaded"] = any(
            t in fuel_types for t in ("octane_91", "octane_95", "octane_98")
        )
        extra["has_lpg"] = "lpg" in fuel_types

    socket_types = []
    for sk in ("socket:type2", "socket:type2_combo", "socket:chademo",
                "socket:tesla_supercharger", "socket:type1"):
        if tags.get(sk):
            socket_types.append(sk.replace("socket:", ""))
    if socket_types:
        extra["socket_types"] = socket_types

    if tags.get("fee") == "no":
        extra["free"] = True
    if tags.get("power_supply") == "yes":
        extra["powered_sites"] = True
    if tags.get("drinking_water") == "yes":
        extra["has_water"] = True
    if tags.get("toilets") == "yes" or tags.get("amenity") == "toilets":
        extra["has_toilets"] = True

    if not (tags.get("name") or tags.get("brand") or tags.get("operator")):
        extra["synthetic_name"] = True

    # ── Enrichment: wikidata, wikipedia, thumbnail ────────────
    wd = tags.get("wikidata")
    if wd and isinstance(wd, str):
        extra["wikidata"] = str(wd)[:20]
    wp = tags.get("wikipedia")
    if wp and isinstance(wp, str):
        extra["wikipedia"] = str(wp)[:200]

    # Resolve a thumbnail URL from wikimedia_commons / image / wikidata
    thumb = _resolve_thumbnail(tags)
    if thumb:
        extra["thumbnail_url"] = thumb

    # Wheelchair accessibility
    wc = tags.get("wheelchair")
    if wc in ("yes", "limited"):
        extra["wheelchair"] = wc

    # Stars / rating (some OSM entries have tourism:stars)
    stars = tags.get("stars") or tags.get("tourism:stars")
    if stars:
        try:
            extra["stars"] = int(stars)
        except (ValueError, TypeError):
            pass

    return PlaceItem(
        id=f"osm:{osm_type}:{osm_id}",
        name=str(name),
        lat=float(lat),
        lng=float(lon),
        category=category,
        extra=extra,
    )


# ──────────────────────────────────────────────────────────────
# Overpass querying
# ──────────────────────────────────────────────────────────────

def _overpass_bbox_str(b: BBox4) -> str:
    return f"({b.minLat},{b.minLng},{b.maxLat},{b.maxLng})"


def _build_overpass_ql(*, bbox: BBox4, filters: List[str], name_clause: str) -> str:
    bbox_str = _overpass_bbox_str(bbox)

    parts: List[str] = []
    if not filters:
        parts.append(f'node{name_clause}{bbox_str};')
        parts.append(f'way{name_clause}{bbox_str};')
        parts.append(f'relation{name_clause}{bbox_str};')
    else:
        for f in filters:
            parts.append(f'node{name_clause}{f}{bbox_str};')
            parts.append(f'way{name_clause}{f}{bbox_str};')
            parts.append(f'relation{name_clause}{f}{bbox_str};')

    timeout_s = int(getattr(settings, "overpass_timeout_s", 90))
    return (
        f'[out:json][timeout:{timeout_s}];'
        f'('
        f'{"".join(parts)}'
        f');'
        f'out center;'
    )


def _build_overpass_around_ql(
    *,
    coords: List[Tuple[float, float]],
    radius_m: float,
    filters: List[str],
    name_clause: str,
    max_coords: int = 120,
) -> str:
    if len(coords) > max_coords:
        step = max(1, len(coords) // max_coords)
        coords = coords[::step]
        if coords[-1] != coords[-1]:
            coords.append(coords[-1])

    coord_csv = ",".join(f"{lat:.5f},{lng:.5f}" for lat, lng in coords)
    around = f"(around:{radius_m:.0f},{coord_csv})"

    parts: List[str] = []
    if not filters:
        parts.append(f"node{name_clause}{around};")
        parts.append(f"way{name_clause}{around};")
    else:
        for f in filters:
            parts.append(f"node{name_clause}{f}{around};")
            parts.append(f"way{name_clause}{f}{around};")

    timeout_s = int(getattr(settings, "overpass_timeout_s", 90))
    return (
        f"[out:json][timeout:{timeout_s}];"
        f"("
        f"{''.join(parts)}"
        f");"
        f"out center;"
    )


def _is_retryable_status(code: int) -> bool:
    return code in (429, 502, 503, 504)


def _fetch_overpass_with_retries(*, client: httpx.Client, ql: str) -> Dict[str, Any]:
    attempts = int(getattr(settings, "overpass_retries", 4))
    base_sleep = float(getattr(settings, "overpass_retry_base_s", 0.75))

    last_exc: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = client.post(settings.overpass_url, content=ql.encode("utf-8"))
            if _is_retryable_status(r.status_code):
                time.sleep(base_sleep * (2 ** i) + random.random() * 0.25)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(base_sleep * (2 ** i) + random.random() * 0.25)

    if last_exc:
        raise last_exc
    raise RuntimeError("overpass_fetch_failed")


def _safe_overpass_name_regex(q: str) -> str:
    q = q.strip()
    if not q:
        return ""
    q = q.replace('"', "")
    q = re.escape(q)
    return q[:80]


# ──────────────────────────────────────────────────────────────
# Corridor polyline key helper
# ──────────────────────────────────────────────────────────────

def _corridor_places_key(
    polyline6: str,
    buffer_km: float,
    categories: List[str],
    limit: int,
    algo_version: str,
) -> str:
    cats_str = ",".join(sorted(categories))
    raw = (
        f"CorridorPlaces/v1|"
        f"poly_sha={hashlib.sha256(polyline6.encode()).hexdigest()}|"
        f"buf={buffer_km}|cats={cats_str}|lim={limit}|"
        f"algo={algo_version}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────
# Bundle-specific helpers: tiers, budget, cluster cap
# ──────────────────────────────────────────────────────────────

# Critical infrastructure — fuel and EV charging get a DEDICATED Overpass query
# with a much higher coord limit (only 2 OSM filters, so query stays small even
# with 300+ sample points).  This prevents them from being squeezed out by the
# larger tier-1 query timing out on long routes.
_CRITICAL_INFRA_CATS: List[PlaceCategory] = ["fuel", "ev_charging"]
_CRITICAL_INFRA_BUFFER_KM = 30.0   # same wide radius as tier 1
_CRITICAL_INFRA_MAX_COORDS = 300   # can afford many coords with only 2 filters

# Tier 1 — things you *need* on a road trip (fuel, safety, sleep, food supply).
# Wide search radius: towns can be 100 km off-route in the outback and still
# be the only option.  No cluster cap — you want every servo, every hospital.
# NOTE: fuel + ev_charging are excluded here — they have their own dedicated query.
_BUNDLE_TIER1_CATS: List[PlaceCategory] = [
    "water", "toilet", "rest_area",
    "mechanic", "hospital", "pharmacy", "dump_point",
    "grocery", "town",
    "camp", "hotel", "motel", "hostel",
    "fast_food",                    # only reliable food option on remote highways
]
_BUNDLE_TIER1_BUFFER_KM = 30.0     # towns/fuel up to 30 km either side
_BUNDLE_TIER1_FRACTION  = 0.65     # 65 % of total budget reserved for tier 1

# Tier 2 — things that enrich a trip but aren't survival-critical.
# Tight corridor only; cluster-capped so one city doesn't eat all slots.
_BUNDLE_TIER2_CATS: List[PlaceCategory] = [
    "cafe", "restaurant", "pub", "bar", "bakery",
    "viewpoint", "beach", "waterfall", "swimming_hole",
    "hot_spring", "national_park", "hiking", "picnic",
    "cave", "fishing", "surf",
    "attraction", "heritage", "museum", "gallery",
    "winery", "brewery", "visitor_info",
    "park", "market", "library", "showground",
    "atm", "laundromat",
    "playground", "pool", "zoo", "theme_park",
    "dog_park", "golf", "cinema",
]

# High-value categories that deserve a wider search radius than generic tier-2.
# A cracking waterfall or gorge 12 km off the highway is absolutely worth showing;
# a laundromat 12 km away is not.
_BUNDLE_TIER2_HIGH_VALUE_CATS: Set[str] = {
    "viewpoint", "beach", "waterfall", "swimming_hole", "hot_spring",
    "national_park", "hiking", "cave", "fishing", "surf",
    "attraction", "heritage", "museum", "zoo", "theme_park",
    "winery", "brewery", "showground",
}
_BUNDLE_TIER2_HIGH_VALUE_BUFFER_KM = 15.0  # wider net for destination-worthy spots
_BUNDLE_TIER2_BUFFER_KM = 5.0              # tight for generic amenities
_BUNDLE_TIER2_CLUSTER_KM = 10.0    # one segment = 10 km
_BUNDLE_TIER2_PER_CLUSTER = 6      # max tier-2 items per 10 km segment


def _bundle_places_budget(route_km: float) -> int:
    """
    Dynamic offline bundle size.

    Scales with route length so a city hop doesn't pull too many places and an
    outback crossing doesn't run dry.  Caps at 5000 to keep download lean.

      50 km  →  ~350   (city loop)
     200 km  → ~1200
     500 km  → ~3000
     834 km  → ~5000   (cap)
    """
    raw = max(50.0, route_km) * 6.0
    return int(max(350, min(5000, raw)))


# ──────────────────────────────────────────────────────────────
# Relevance scoring — ranks places within each tier so the
# most useful/notable items are accepted first.
# ──────────────────────────────────────────────────────────────

# Category-intrinsic importance weights.  Higher = more likely to be a
# meaningful stop vs an anonymous node.
_CATEGORY_IMPORTANCE: Dict[str, float] = {
    # Nature highlights — these are *why* people take road trips
    "national_park": 5.0, "waterfall": 4.5, "hot_spring": 4.5,
    "cave": 4.5, "surf": 3.5,
    "swimming_hole": 4.0, "viewpoint": 4.0, "beach": 3.5, "hiking": 3.0,
    "fishing": 3.0,
    # Culture — destination-worthy attractions
    "museum": 4.5, "heritage": 4.0, "gallery": 3.5, "attraction": 3.5,
    "zoo": 3.5, "theme_park": 3.5, "winery": 3.0, "brewery": 3.0,
    "showground": 2.0,
    # Towns are anchor points
    "town": 3.0, "visitor_info": 2.5,
    # Essential infrastructure — always valuable but not "destination"
    "fuel": 2.0, "ev_charging": 2.0, "hospital": 2.0, "mechanic": 1.5,
    "grocery": 1.5, "pharmacy": 1.5, "camp": 2.5, "hotel": 2.0,
    "motel": 2.0, "hostel": 2.0,
    # Nice-to-have
    "restaurant": 1.5, "cafe": 1.5, "pub": 1.5, "bakery": 1.5,
    "fast_food": 1.0, "bar": 1.0, "rest_area": 1.0, "toilet": 0.5,
    "water": 1.0, "dump_point": 0.5, "atm": 0.5, "laundromat": 0.5,
    "picnic": 1.0, "park": 1.0, "market": 1.5, "pool": 1.5,
    "playground": 1.0, "dog_park": 1.5, "golf": 1.5, "cinema": 1.5,
    "library": 1.0,
}


def _score_place(
    item: PlaceItem,
    landmark_names: Set[str] | None = None,
) -> float:
    """
    Score a PlaceItem for bundle relevance.  Higher = more worth including.

    Signals:
      - Category importance (some categories are inherently more notable)
      - Data richness (named, has website/phone/hours → real establishment)
      - Landmark match (name appears in regional knowledge → known highlight)
      - Wikidata presence (notable enough to be in Wikipedia/Wikidata)
    """
    score = 0.0
    extra = item.extra or {}

    # 1. Category base weight
    score += _CATEGORY_IMPORTANCE.get(str(item.category), 1.0)

    # 2. Data richness signals — real, well-documented places
    if not extra.get("synthetic_name"):
        score += 2.0  # has a real name
    if extra.get("website"):
        score += 1.5
    if extra.get("phone"):
        score += 1.0
    if extra.get("opening_hours"):
        score += 1.0
    if extra.get("description"):
        score += 0.5
    if extra.get("brand"):
        score += 0.5  # known chain = reliable
    if extra.get("address"):
        score += 0.3

    # 3. Wikidata / Wikipedia — strong notability signal
    if extra.get("wikidata"):
        score += 3.0
    if extra.get("wikipedia"):
        score += 2.0

    # 4. Landmark boost — match against known regional highlights
    if landmark_names and item.name:
        name_lower = item.name.lower()
        for landmark in landmark_names:
            if landmark in name_lower or name_lower in landmark:
                score += 5.0
                break

    # 5. Amenity richness for camps/rest areas
    if item.category in ("camp", "rest_area"):
        if extra.get("has_water"):
            score += 0.5
        if extra.get("has_toilets"):
            score += 0.5
        if extra.get("powered_sites"):
            score += 0.5
        if extra.get("free"):
            score += 0.5

    # 6. Fuel completeness
    if item.category == "fuel":
        fuel_types = extra.get("fuel_types") or []
        if len(fuel_types) >= 3:
            score += 1.0
        elif len(fuel_types) >= 1:
            score += 0.5

    return score


# ──────────────────────────────────────────────────────────────
# Landmark extraction from regional knowledge
# ──────────────────────────────────────────────────────────────

# Regex to extract proper-noun landmark names from the region knowledge text.
# Matches capitalized multi-word names (2-6 words) that aren't sentence starters.
_LANDMARK_PATTERN = re.compile(
    r"(?<=[:\.\—–,])\s*"              # preceded by punctuation
    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,5})"  # 1-6 capitalized words
    r"(?:\s*(?:NP|National Park|Museum|Gallery|Falls|Gorge|Beach|"
    r"Lookout|Walk|Trail|Cave|Pool|Springs?|Range|Island|"
    r"Reef|Rocks?|Bridge|Market|Sanctuary|Reserve|Conservation Park))?"
)


@functools.lru_cache(maxsize=1)
def _load_all_landmark_names() -> Dict[str, Set[str]]:
    """
    Extract notable place names from each region's knowledge text.
    Returns {region_id: {lowercase landmark name, ...}}.
    """
    try:
        from app.services.guide_data import get_regions
        regions = get_regions()
    except Exception:
        return {}

    result: Dict[str, Set[str]] = {}
    for r in regions:
        rid = r.get("id", "")
        text = r.get("knowledge", "")
        names: Set[str] = set()

        # Extract from explicit mentions (words before parenthetical descriptions,
        # named attractions after colons/dashes)
        for m in _LANDMARK_PATTERN.finditer(text):
            candidate = m.group(0).strip()
            # Skip very short or generic terms
            if len(candidate) >= 5 and candidate.lower() not in {
                "the", "this", "that", "near", "from", "with", "most",
                "best", "book", "carry", "check", "drive", "allow",
                "watch", "avoid", "summer", "winter", "spring", "autumn",
                "excellent", "spectacular", "stunning", "extraordinary",
                "beautiful", "brilliant", "genuine", "deeply", "dramatically",
                "close", "closed", "contact", "distances", "guided",
                "confronting", "cultural", "artesian", "dozens", "great",
                "hinterland", "even", "every", "never", "serious",
                "including", "between", "about", "after", "before",
                "above", "below", "where", "which", "worth", "along",
                "around", "across", "through", "their", "these",
                "those", "other", "caves", "cellar", "gives", "catch",
                "fuel", "bore", "bill", "cash", "devil", "base",
            }:
                names.add(candidate.lower())

        # Also grab text between bold markers or inside quotes if present
        # and specifically named places (Cape X, Mt X, Lake X, etc.)
        for pattern in [
            r"((?:Cape|Mt|Mount|Lake|Port|Point)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            r"((?:Twelve|Three)\s+[A-Z][a-z]+)",
            r"([A-Z][a-z]+\s+(?:Falls|Gorge|Beach|Bay|Creek|River|Island|Ranges?|Pool|Head|Gap|Rock|Rocks|Springs?))",
        ]:
            for m2 in re.finditer(pattern, text):
                name = m2.group(1).strip()
                if len(name) >= 4:
                    names.add(name.lower())

        if names:
            result[rid] = names

    return result


def _landmarks_for_route(
    samples: List[Tuple[float, float]],
) -> Set[str]:
    """
    Determine which regions a route passes through, then return the union
    of all landmark names for those regions.
    """
    all_landmarks = _load_all_landmark_names()
    if not all_landmarks:
        return set()

    try:
        from app.services.guide_data import get_regions
        regions = get_regions()
    except Exception:
        return set()

    # Find which regions the route samples intersect
    matched_names: Set[str] = set()
    for r in regions:
        bbox = r.get("bbox", {})
        s, n = bbox.get("s", -90), bbox.get("n", 90)
        w, e = bbox.get("w", -180), bbox.get("e", 180)
        rid = r.get("id", "")
        if rid not in all_landmarks:
            continue
        for lat, lng in samples:
            if s <= lat <= n and w <= lng <= e:
                matched_names.update(all_landmarks[rid])
                break  # this region matched, move to next

    return matched_names


# ──────────────────────────────────────────────────────────────
# Wikimedia Commons thumbnail resolution
# ──────────────────────────────────────────────────────────────

_WIKI_THUMB_WIDTH = 400  # px — small enough for bundles, big enough to look good

_COMMONS_URL_TEMPLATE = (
    "https://commons.wikimedia.org/w/thumb.php?f={filename}&w={width}"
)


def _wikimedia_thumb_url(filename: str, width: int = _WIKI_THUMB_WIDTH) -> str:
    """
    Build a Wikimedia Commons thumbnail URL from a filename.
    OSM `image` or `wikimedia_commons` tags often contain
    "File:Example.jpg" or just "Example.jpg".
    """
    filename = filename.strip()
    if filename.startswith("File:"):
        filename = filename[5:]
    # URL-encode spaces
    filename = filename.replace(" ", "%20")
    return _COMMONS_URL_TEMPLATE.format(filename=filename, width=width)


def _resolve_thumbnail(tags: Dict[str, Any]) -> Optional[str]:
    """
    Try to derive a small thumbnail URL from OSM tags.
    Priority: wikimedia_commons > image > wikidata (via thumb API).
    Returns a URL string or None.  Never makes network calls — uses
    deterministic URL construction only.
    """
    # 1. Direct Wikimedia Commons file reference
    wmc = tags.get("wikimedia_commons") or tags.get("image")
    if wmc and isinstance(wmc, str):
        wmc = wmc.strip()
        # Only resolve Wikimedia/Commons references, not arbitrary URLs
        if wmc.startswith("File:") or wmc.startswith("Category:"):
            if wmc.startswith("File:"):
                return _wikimedia_thumb_url(wmc)
        elif not wmc.startswith("http"):
            # Bare filename — assume Commons
            return _wikimedia_thumb_url(wmc)
        elif "wikimedia.org" in wmc or "wikipedia.org" in wmc:
            # Already a URL — pass through (client will fetch directly)
            return wmc[:500]

    # 2. Wikidata entity → use Special:FilePath (auto-resolves to main image)
    wd = tags.get("wikidata")
    if wd and isinstance(wd, str) and wd.startswith("Q"):
        return (
            f"https://commons.wikimedia.org/wiki/Special:FilePath/"
            f"?width={_WIKI_THUMB_WIDTH}&wptype=entity&wpvalue={wd}"
        )

    return None


def _route_km_from_polyline(poly6: str) -> float:
    """Approximate route length in km by summing decoded segment distances."""
    pts = decode_polyline6(poly6)
    if not pts or len(pts) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(pts)):
        total += _haversine_m(
            (float(pts[i - 1][0]), float(pts[i - 1][1])),
            (float(pts[i][0]), float(pts[i][1])),
        )
    return total / 1000.0


def _cluster_cap_tier2(
    items: List[PlaceItem],
    samples: List[Tuple[float, float]],
    segment_km: float,
    per_segment: int,
    landmark_names: Set[str] | None = None,
) -> List[PlaceItem]:
    """
    Prevent a dense city from eating all tier-2 slots.

    Divides the route into `segment_km`-length buckets (keyed by the index of
    the nearest sample point).  Within each bucket:
      1. Items are scored by relevance (_score_place).
      2. Category diversity is enforced: no single category gets more than
         half the slots (rounded up), ensuring a mix of dining/nature/culture.
      3. Highest-scoring items are picked first within those constraints.
    """
    if not samples or segment_km <= 0 or per_segment <= 0:
        return items

    # Assign each item to its nearest sample bucket
    buckets: Dict[int, List[Tuple[float, PlaceItem]]] = collections.defaultdict(list)

    for it in items:
        best_idx = 0
        best_d = float("inf")
        for idx, s in enumerate(samples):
            d = _haversine_m((it.lat, it.lng), s)
            if d < best_d:
                best_d = d
                best_idx = idx
        score = _score_place(it, landmark_names)
        buckets[best_idx].append((score, it))

    # Within each bucket: sort by score descending, then pick with diversity
    max_per_cat = max(1, (per_segment + 1) // 2)  # no category > half the slots
    result: List[PlaceItem] = []

    for bucket_items in buckets.values():
        bucket_items.sort(key=lambda x: x[0], reverse=True)
        cat_counts: Dict[str, int] = {}
        picked: List[PlaceItem] = []
        # First pass: pick diverse high-scorers
        for score, it in bucket_items:
            if len(picked) >= per_segment:
                break
            cat = str(it.category)
            if cat_counts.get(cat, 0) < max_per_cat:
                picked.append(it)
                cat_counts[cat] = cat_counts.get(cat, 0) + 1
        # Second pass: fill remaining slots from unpicked items by score
        if len(picked) < per_segment:
            picked_ids = {it.id for it in picked}
            for score, it in bucket_items:
                if len(picked) >= per_segment:
                    break
                if it.id not in picked_ids:
                    picked.append(it)
        result.extend(picked)

    return result


# ──────────────────────────────────────────────────────────────
# Service
# ──────────────────────────────────────────────────────────────

class Places:
    """
    Places service - OVERPASS-FIRST corridor search.

    For corridor searches (search_corridor_polyline), the read order is:
      1) Overpass around query (distributed along the actual route)
      2) Local store supplement (fill gaps)
      3) Supa supplement (fill gaps)

    This ensures the full route gets coverage from start to end,
    instead of being dominated by destination-area items that were
    cached by previous bbox-based searches.

    For regular bbox searches (.search()), the order remains:
      1) Local store
      2) Supa
      3) Overpass tile top-up
    """

    def __init__(
        self,
        *,
        cache_conn,
        # ── BUMPED from places.v2.expanded to v3 ──────────────
        # This invalidates all stale corridor packs that were built
        # with the old local-first ordering (destination-biased).
        algo_version: str = "places.v3.overpass_first",
        store: PlacesStore | None = None,
    ):
        self.cache_conn = cache_conn
        self.algo_version = algo_version

        self.store = store or PlacesStore(cache_conn)
        self.store.ensure_schema()

        self.supa: SupaPlacesRepo | None
        if bool(getattr(settings, "supa_enabled", False)):
            self.supa = SupaPlacesRepo()
        else:
            self.supa = None

    # ──────────────────────────────────────────────────────────
    # Supa helpers
    # ──────────────────────────────────────────────────────────

    def _supa_upsert_best_effort(self, items: List[PlaceItem], *, source: str) -> int:
        if self.supa is None or not items:
            return 0
        try:
            logger.debug("supa upsert attempt: n=%d source=%s", len(items), source)
            n = self.supa.upsert_items(items, source=source)
            logger.debug("supa upsert ok: n=%d", n)
            return int(n)
        except Exception as e:
            logger.warning("supa upsert FAILED: %r", e)
            return 0

    def _supa_ingest_best_effort(self, items: List[PlaceItem]) -> None:
        if not items:
            return
        try:
            self.store.upsert_items(items)
        except Exception as e:
            logger.warning("places_store ingest from supa FAILED: %r", e)

    def _finalize_and_cache_pack(self, pack: PlacesPack, *, publish_to_supa: bool) -> PlacesPack:
        if publish_to_supa and self.supa is not None and pack.items:
            cap = int(getattr(settings, "supa_places_publish_cap", 4000))
            cap = max(0, cap)
            subset = pack.items[:cap] if cap else pack.items
            self._supa_upsert_best_effort(subset, source="pack")

        put_places_pack(
            self.cache_conn,
            places_key=pack.places_key,
            created_at=pack.created_at,
            algo_version=self.algo_version,
            pack=pack.model_dump(),
        )
        return pack

    # ──────────────────────────────────────────────────────────
    # Bundle search — two-tier, dynamic budget
    # ──────────────────────────────────────────────────────────
    #
    # Tier 1 (essentials): wide radius (100 km), no cluster cap.
    # Tier 2 (leisure):    tight radius (5 km), cluster-capped so
    #                      one city can't eat all remaining slots.
    #
    # Total budget scales with route length (≈ 3 places/km, 350–2500).
    # ──────────────────────────────────────────────────────────

    def search_bundle(
        self,
        *,
        polyline6: str,
        categories: List[PlaceCategory] | None = None,
    ) -> PlacesPack:
        """
        Offline-bundle-optimised place search.

        Returns a dynamically-sized, relevance-structured PlacesPack ready
        for ZIP bundling.  Pass `categories` to override the default tier
        split (useful for testing); omit for production use.
        """
        route_km = _route_km_from_polyline(polyline6)
        if route_km < 1.0:
            # Degenerate route — fall back gracefully
            route_km = 50.0

        total_budget = _bundle_places_budget(route_km)
        t1_budget = int(total_budget * _BUNDLE_TIER1_FRACTION)
        t2_budget = total_budget - t1_budget

        # Allow caller to override categories (e.g. user-selected interests)
        t1_cats: List[PlaceCategory] = [c for c in (categories or []) if c in _BUNDLE_TIER1_CATS] or _BUNDLE_TIER1_CATS
        t2_cats: List[PlaceCategory] = [c for c in (categories or []) if c in _BUNDLE_TIER2_CATS] or _BUNDLE_TIER2_CATS

        cats_str_t1 = [str(c) for c in t1_cats]
        cats_str_t2 = [str(c) for c in t2_cats]
        cats_str_ci = [str(c) for c in _CRITICAL_INFRA_CATS]
        all_cats_str = sorted(set(cats_str_ci + cats_str_t1 + cats_str_t2))

        logger.info(
            "search_bundle: route_km=%.1f budget=%d (t1=%d t2=%d) "
            "t1_cats=%d t2_cats=%d t1_buf_km=%.0f t2_buf_km=%.0f t2_hv_buf_km=%.0f",
            route_km, total_budget, t1_budget, t2_budget,
            len(t1_cats), len(t2_cats),
            _BUNDLE_TIER1_BUFFER_KM, _BUNDLE_TIER2_BUFFER_KM,
            _BUNDLE_TIER2_HIGH_VALUE_BUFFER_KM,
        )

        pkey = _corridor_places_key(
            polyline6,
            _BUNDLE_TIER1_BUFFER_KM,  # use the wider radius as the cache key discriminator
            all_cats_str,
            total_budget,
            self.algo_version + ".bundle_v2_scored",
        )

        cached = get_places_pack(self.cache_conn, pkey)
        if cached:
            pack = PlacesPack.model_validate(cached)
            logger.info(
                "search_bundle cache HIT: key=%s items=%d", pkey[:16], len(pack.items)
            )
            return pack

        logger.info("search_bundle cache MISS — running two-tier pipeline")

        # ── Sample route at 8 km intervals ───────────────────
        samples = _sample_polyline(polyline6, 8.0, include_endpoints=True)
        if not samples:
            logger.warning("search_bundle: no samples from polyline")
            empty_req = PlacesRequest(
                bbox=BBox4(minLng=0, minLat=0, maxLng=0, maxLat=0),
                categories=t1_cats + t2_cats,
                limit=total_budget,
            )
            return self._finalize_and_cache_pack(
                PlacesPack(
                    places_key=pkey,
                    req=empty_req,
                    items=[],
                    provider="bundle_empty",
                    created_at=utc_now_iso(),
                    algo_version=self.algo_version,
                ),
                publish_to_supa=False,
            )

        t1_buffer_m = _BUNDLE_TIER1_BUFFER_KM * 1000.0
        t2_buffer_m = _BUNDLE_TIER2_BUFFER_KM * 1000.0
        t2_hv_buffer_m = _BUNDLE_TIER2_HIGH_VALUE_BUFFER_KM * 1000.0
        wide_bbox   = _bbox_around_points(samples, _BUNDLE_TIER1_BUFFER_KM)
        # Use the wider high-value buffer for the tier-2 bbox so we can catch
        # destination-worthy spots further off the road
        tight_bbox  = _bbox_around_points(samples, _BUNDLE_TIER2_HIGH_VALUE_BUFFER_KM)

        seen_ids: set[str] = set()
        t1_items: List[PlaceItem] = []
        t2_items: List[PlaceItem] = []

        timeout_s = float(getattr(settings, "overpass_timeout_s", 90))
        timeout   = httpx.Timeout(timeout_s, connect=15.0)

        def _within(it: PlaceItem, buf_m: float) -> bool:
            return (
                it.id not in seen_ids
                and _min_distance_to_samples_m(it.lat, it.lng, samples) <= buf_m
            )

        def _accept(lst: List[PlaceItem], it: PlaceItem) -> None:
            seen_ids.add(it.id)
            lst.append(it)

        # ═══════════════════════════════════════════════════════
        # OVERPASS — BOTH TIERS IN PARALLEL
        # Tier 1 (essentials, wide) and Tier 2 (leisure, tight)
        # are independent queries — fire them concurrently so the
        # total wait is max(t1, t2) instead of t1 + t2.
        # ═══════════════════════════════════════════════════════

        def _overpass_fetch_tier(
            filters: List[str],
            radius_m: float,
            label: str,
        ) -> List[PlaceItem]:
            if not filters:
                return []
            ql = _build_overpass_around_ql(
                coords=samples,
                radius_m=radius_m,
                filters=filters,
                name_clause="",
            )
            logger.info(
                "search_bundle %s Overpass: samples=%d radius_m=%.0f filters=%d ql_len=%d",
                label, len(samples), radius_m, len(filters), len(ql),
            )
            try:
                with httpx.Client(timeout=timeout) as client:
                    data = _fetch_overpass_with_retries(client=client, ql=ql)
                items: List[PlaceItem] = []
                for el in data.get("elements") or []:
                    it = _element_to_item(el)
                    if it is not None:
                        items.append(it)
                logger.info("search_bundle %s Overpass raw=%d", label, len(items))
                return items
            except Exception as e:
                logger.warning("search_bundle %s Overpass FAILED: %r", label, e)
                return []

        t1_filters = _overpass_filters_for_categories(t1_cats)
        t2_filters = _overpass_filters_for_categories(t2_cats)

        # Critical infrastructure (fuel + EV) get their own dedicated query with
        # a higher coord limit.  Their 2 OSM filters keep the query small even at
        # 300 sample points, so they are never crowded out by the larger tier-1
        # query timing out on long routes.
        ci_cats: List[PlaceCategory] = list(_CRITICAL_INFRA_CATS)
        ci_filters = _overpass_filters_for_categories(ci_cats)
        ci_buffer_m = _CRITICAL_INFRA_BUFFER_KM * 1000.0

        def _overpass_fetch_critical() -> List[PlaceItem]:
            if not ci_filters:
                return []
            ql = _build_overpass_around_ql(
                coords=samples,
                radius_m=ci_buffer_m,
                filters=ci_filters,
                name_clause="",
                max_coords=_CRITICAL_INFRA_MAX_COORDS,
            )
            logger.info(
                "search_bundle critical Overpass: samples=%d radius_m=%.0f filters=%d ql_len=%d",
                len(samples), ci_buffer_m, len(ci_filters), len(ql),
            )
            try:
                with httpx.Client(timeout=timeout) as client:
                    data = _fetch_overpass_with_retries(client=client, ql=ql)
                items: List[PlaceItem] = []
                for el in data.get("elements") or []:
                    it = _element_to_item(el)
                    if it is not None:
                        items.append(it)
                logger.info("search_bundle critical Overpass raw=%d", len(items))
                return items
            except Exception as e:
                logger.warning("search_bundle critical Overpass FAILED: %r", e)
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            f1 = pool.submit(_overpass_fetch_tier, t1_filters, t1_buffer_m, "tier1")
            # Fetch tier 2 with the wider high-value buffer — the per-item
            # acceptance filter below applies the tight buffer to generic
            # categories while letting high-value destinations through at 15 km.
            f2 = pool.submit(_overpass_fetch_tier, t2_filters, t2_hv_buffer_m, "tier2")
            f_ci = pool.submit(_overpass_fetch_critical)
            t1_fetched = f1.result()
            t2_fetched = f2.result()
            ci_fetched = f_ci.result()

        # Persist all sets to local store + Supa (best-effort, non-blocking order)
        all_fetched = ci_fetched + t1_fetched + t2_fetched
        if all_fetched:
            try:
                self.store.upsert_items(all_fetched)
            except Exception as e:
                logger.warning("search_bundle store upsert FAILED: %r", e)
            self._supa_upsert_best_effort(all_fetched, source="bundle_overpass")

        # ── Resolve regional landmarks for scoring ────────────
        landmark_names = _landmarks_for_route(samples)
        if landmark_names:
            logger.info(
                "search_bundle: %d landmark names from %d route regions",
                len(landmark_names),
                len(landmark_names),  # approximate; exact region count not needed
            )

        # ═══════════════════════════════════════════════════════
        # TIER 1 — collect all candidates, score, sort, accept
        # Critical infra (fuel/EV) are collected alongside other
        # tier-1 items, then the combined pool is ranked by score.
        # ═══════════════════════════════════════════════════════

        ci_cats_set = set(ci_cats)
        t1_cats_set = set(t1_cats)
        t1_all_cats = list(ci_cats) + list(t1_cats)
        t1_all_cats_set = set(t1_all_cats)

        # Collect all tier-1 candidates (deduped, within buffer)
        t1_candidates: List[PlaceItem] = []
        t1_seen: set[str] = set()

        def _collect_t1(it: PlaceItem, buf_m: float) -> None:
            if it.id not in t1_seen and _min_distance_to_samples_m(it.lat, it.lng, samples) <= buf_m:
                if it.category in t1_all_cats_set:
                    t1_candidates.append(it)
                    t1_seen.add(it.id)

        for it in ci_fetched:
            _collect_t1(it, ci_buffer_m)
        for it in t1_fetched:
            _collect_t1(it, t1_buffer_m)

        # Supplement from local store if Overpass was thin
        if len(t1_candidates) < t1_budget:
            try:
                local = self.store.query_bbox(
                    bbox=wide_bbox, categories=t1_all_cats, limit=t1_budget * 2,
                )
                for it in local:
                    _collect_t1(it, t1_buffer_m)
            except Exception as e:
                logger.warning("search_bundle tier1 local FAILED: %r", e)

        if self.supa is not None and len(t1_candidates) < t1_budget:
            try:
                supa = self.supa.query_bbox(
                    bbox=wide_bbox, categories=t1_all_cats, limit=t1_budget * 2,
                )
                if supa:
                    self._supa_ingest_best_effort(supa)
                    for it in supa:
                        _collect_t1(it, t1_buffer_m)
            except Exception as e:
                logger.warning("search_bundle tier1 supa FAILED: %r", e)

        # Sort by relevance score descending, then accept top budget items
        t1_candidates.sort(
            key=lambda it: _score_place(it, landmark_names), reverse=True,
        )
        t1_items = t1_candidates[:t1_budget]
        seen_ids.update(it.id for it in t1_items)

        logger.info("search_bundle tier1 DONE: candidates=%d accepted=%d / budget=%d",
                     len(t1_candidates), len(t1_items), t1_budget)

        # ═══════════════════════════════════════════════════════
        # TIER 2 — collect all candidates, then diversity+score
        # cluster cap handles both scoring and category diversity.
        # ═══════════════════════════════════════════════════════

        t2_cats_set = set(t2_cats)
        t2_raw: List[PlaceItem] = []

        def _t2_buffer_for(cat: str) -> float:
            """High-value categories get the wider search radius."""
            return t2_hv_buffer_m if cat in _BUNDLE_TIER2_HIGH_VALUE_CATS else t2_buffer_m

        for it in t2_fetched:
            buf = _t2_buffer_for(str(it.category))
            if _within(it, buf) and it.category in t2_cats_set:
                t2_raw.append(it)
                seen_ids.add(it.id)

        if len(t2_raw) < t2_budget * 3:
            try:
                local = self.store.query_bbox(
                    bbox=tight_bbox, categories=t2_cats, limit=t2_budget * 3,
                )
                for it in local:
                    buf = _t2_buffer_for(str(it.category))
                    if it.id not in seen_ids and _within(it, buf):
                        t2_raw.append(it)
                        seen_ids.add(it.id)
            except Exception as e:
                logger.warning("search_bundle tier2 local FAILED: %r", e)

        if self.supa is not None and len(t2_raw) < t2_budget * 3:
            try:
                supa = self.supa.query_bbox(
                    bbox=tight_bbox, categories=t2_cats, limit=t2_budget * 3,
                )
                if supa:
                    self._supa_ingest_best_effort(supa)
                    for it in supa:
                        buf = _t2_buffer_for(str(it.category))
                        if it.id not in seen_ids and _within(it, buf):
                            t2_raw.append(it)
                            seen_ids.add(it.id)
            except Exception as e:
                logger.warning("search_bundle tier2 supa FAILED: %r", e)

        # Apply diversity-aware, score-ranked cluster cap then slice to budget
        t2_capped = _cluster_cap_tier2(
            t2_raw, samples,
            segment_km=_BUNDLE_TIER2_CLUSTER_KM,
            per_segment=_BUNDLE_TIER2_PER_CLUSTER,
            landmark_names=landmark_names,
        )
        t2_items = t2_capped[:t2_budget]

        logger.info(
            "search_bundle tier2 DONE: raw=%d after_cap=%d accepted=%d / budget=%d",
            len(t2_raw), len(t2_capped), len(t2_items), t2_budget,
        )

        # ═══════════════════════════════════════════════════════
        # MERGE & FINALISE
        # ═══════════════════════════════════════════════════════

        all_items = t1_items + t2_items

        logger.info(
            "search_bundle FINAL: route_km=%.1f budget=%d t1=%d t2=%d total=%d",
            route_km, total_budget, len(t1_items), len(t2_items), len(all_items),
        )

        bundle_req = PlacesRequest(
            bbox=wide_bbox,
            categories=list(ci_cats) + list(t1_cats) + list(t2_cats),
            limit=total_budget,
        )
        pack = PlacesPack(
            places_key=pkey,
            req=bundle_req,
            items=all_items,
            provider="bundle_v2_scored",
            created_at=utc_now_iso(),
            algo_version=self.algo_version,
        )
        return self._finalize_and_cache_pack(pack, publish_to_supa=True)

    # ──────────────────────────────────────────────────────────
    # Corridor-aware route search - OVERPASS FIRST
    # ──────────────────────────────────────────────────────────
    #
    # The critical change from v2: Overpass runs FIRST, then
    # local/supa supplement.  Previously local ran first and if it
    # had enough destination-area items (>70% of limit), Overpass
    # was skipped - meaning start and mid-route got nothing.
    # ──────────────────────────────────────────────────────────

    def search_corridor_polyline(
        self,
        *,
        polyline6: str,
        buffer_km: float = 15.0,
        categories: List[PlaceCategory],
        limit: int = 8000,
        sample_interval_km: float = 8.0,
    ) -> PlacesPack:
        cats_str = [str(c) for c in categories]

        logger.info(
            "search_corridor_polyline: polyline_len=%d buffer_km=%s cats=%d limit=%d interval_km=%s",
            len(polyline6), buffer_km, len(categories), limit, sample_interval_km,
        )

        pkey = _corridor_places_key(
            polyline6, buffer_km, cats_str, limit, self.algo_version,
        )

        # ── 0) Pack cache ────────────────────────────────────
        cached = get_places_pack(self.cache_conn, pkey)
        if cached:
            pack = PlacesPack.model_validate(cached)
            logger.info("corridor cache HIT: key=%s items=%d provider=%s", pkey[:16], len(pack.items), pack.provider)
            # Migrate to supa if needed (best-effort)
            migrate_cached = bool(getattr(settings, "supa_places_publish_cached_packs", True))
            if (
                migrate_cached
                and self.supa is not None
                and pack.items
                and ("supa" not in (pack.provider or ""))
            ):
                cap = int(getattr(settings, "supa_places_publish_cap", 4000))
                subset = pack.items[:cap] if cap else pack.items
                self._supa_upsert_best_effort(subset, source="cached_pack")
            return pack

        logger.info("corridor cache MISS - running full pipeline")

        # ── 1) Sample route ──────────────────────────────────
        samples = _sample_polyline(
            polyline6, sample_interval_km, include_endpoints=True,
        )
        if not samples:
            logger.warning("corridor: no samples from polyline - returning empty")
            empty_req = PlacesRequest(
                bbox=BBox4(minLng=0, minLat=0, maxLng=0, maxLat=0),
                categories=categories,
                limit=limit,
            )
            empty_pack = PlacesPack(
                places_key=pkey,
                req=empty_req,
                items=[],
                provider="corridor_empty",
                created_at=utc_now_iso(),
                algo_version=self.algo_version,
            )
            return self._finalize_and_cache_pack(empty_pack, publish_to_supa=False)

        buffer_m = buffer_km * 1000.0
        corridor_bbox = _bbox_around_points(samples, buffer_km)

        items: List[PlaceItem] = []
        seen_ids: set[str] = set()

        def _accept(it: PlaceItem) -> bool:
            if it.id in seen_ids:
                return False
            d = _min_distance_to_samples_m(it.lat, it.lng, samples)
            return d <= buffer_m

        # ──────────────────────────────────────────────────────
        # STEP 2: OVERPASS AROUND QUERY - RUNS FIRST
        # ──────────────────────────────────────────────────────
        # This is the whole point of corridor search: query a true
        # buffer along the actual road, from start to end.  Running
        # this first ensures every stretch of the route gets coverage
        # regardless of what's in the local store.
        # ──────────────────────────────────────────────────────

        provider_used = "corridor"
        overpass_items_total = 0
        used_overpass = False

        timeout_s = float(getattr(settings, "overpass_timeout_s", 90))
        timeout = httpx.Timeout(timeout_s, connect=15.0)

        # Split critical infra (fuel/EV) into a dedicated query with a higher
        # coord limit so they are never squeezed out by the main all-category
        # query timing out on long routes.
        ci_cats_in_req = [c for c in _CRITICAL_INFRA_CATS if c in set(categories)]
        non_ci_cats = [c for c in categories if c not in set(_CRITICAL_INFRA_CATS)]

        def _corridor_overpass_fetch(cats: List[PlaceCategory], max_coords: int, label: str) -> List[PlaceItem]:
            f = _overpass_filters_for_categories(cats)
            if not f and not cats:
                return []
            ql = _build_overpass_around_ql(
                coords=samples,
                radius_m=buffer_m,
                filters=f,
                name_clause="",
                max_coords=max_coords,
            )
            logger.info(
                "corridor Overpass %s: samples=%d radius_m=%s filters=%d ql_len=%d",
                label, len(samples), buffer_m, len(f), len(ql),
            )
            try:
                with httpx.Client(timeout=timeout) as client:
                    data = _fetch_overpass_with_retries(client=client, ql=ql)
                result: List[PlaceItem] = []
                for el in data.get("elements") or []:
                    it = _element_to_item(el)
                    if it is not None:
                        result.append(it)
                logger.info("corridor Overpass %s raw=%d", label, len(result))
                return result
            except Exception as e:
                logger.warning("corridor Overpass %s FAILED: %r", label, e)
                return []

        # Run critical infra + main categories in parallel
        ci_fetched_corr: List[PlaceItem] = []
        main_fetched: List[PlaceItem] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = {}
            if ci_cats_in_req:
                futures["ci"] = pool.submit(
                    _corridor_overpass_fetch, ci_cats_in_req, _CRITICAL_INFRA_MAX_COORDS, "critical"
                )
            if non_ci_cats:
                futures["main"] = pool.submit(
                    _corridor_overpass_fetch, non_ci_cats, 120, "main"
                )
            if "ci" in futures:
                ci_fetched_corr = futures["ci"].result()
            if "main" in futures:
                main_fetched = futures["main"].result()

        fetched = ci_fetched_corr + main_fetched
        if fetched:
            used_overpass = True
            overpass_items_total = len(fetched)

            # Persist to local store for future queries
            try:
                self.store.upsert_items(fetched)
            except Exception as e:
                logger.warning("corridor places_store upsert FAILED: %r", e)

            # Publish to supa (best-effort)
            self._supa_upsert_best_effort(fetched, source="overpass_corridor")

            # Accept items within the corridor buffer
            for it in fetched:
                if _accept(it):
                    seen_ids.add(it.id)
                    items.append(it)
                    if len(items) >= limit:
                        break

            logger.info(
                "corridor after Overpass: accepted=%d critical=%d main=%d",
                len(items), len(ci_fetched_corr), len(main_fetched),
            )

        # ──────────────────────────────────────────────────────
        # STEP 3: LOCAL STORE SUPPLEMENT
        # ──────────────────────────────────────────────────────
        # Now fill gaps with whatever's already in the local store.
        # Items already seen from Overpass are deduplicated.
        # ──────────────────────────────────────────────────────

        local_count = 0
        if len(items) < limit:
            try:
                local_items = self.store.query_bbox(
                    bbox=corridor_bbox, categories=categories, limit=limit * 2,
                )
            except Exception as e:
                logger.warning("corridor places_store query_bbox FAILED: %r", e)
                local_items = []

            for it in local_items:
                if _accept(it):
                    seen_ids.add(it.id)
                    items.append(it)
                    local_count += 1
                    if len(items) >= limit:
                        break

            logger.debug("corridor after local supplement: +%d → total=%d", local_count, len(items))

        # ──────────────────────────────────────────────────────
        # STEP 4: SUPA SUPPLEMENT
        # ──────────────────────────────────────────────────────

        supa_hit = 0
        if self.supa is not None and len(items) < limit:
            try:
                supa_items = self.supa.query_bbox(
                    bbox=corridor_bbox, categories=categories, limit=limit * 2,
                )
                if supa_items:
                    supa_hit = len(supa_items)
                    self._supa_ingest_best_effort(supa_items)
                    for it in supa_items:
                        if _accept(it):
                            seen_ids.add(it.id)
                            items.append(it)
                            if len(items) >= limit:
                                break
                    provider_used = "corridor+supa"
            except Exception as e:
                logger.warning("supa corridor query_bbox FAILED: %r", e)

        # ── 5) Finalize ──────────────────────────────────────
        if used_overpass:
            provider_used = (
                f"{provider_used}+overpass"
                if "overpass" not in provider_used
                else provider_used
            )

        logger.info(
            "corridor_polyline FINAL: provider=%s samples=%d overpass_raw=%d local_supplement=%d supa_supplement=%d total=%d",
            provider_used,
            len(samples),
            overpass_items_total,
            local_count,
            supa_hit,
            len(items),
        )

        corridor_req = PlacesRequest(
            bbox=corridor_bbox,
            categories=categories,
            limit=limit,
        )

        pack = PlacesPack(
            places_key=pkey,
            req=corridor_req,
            items=items[:limit],
            provider=provider_used,
            created_at=utc_now_iso(),
            algo_version=self.algo_version,
        )

        return self._finalize_and_cache_pack(pack, publish_to_supa=True)

    # ──────────────────────────────────────────────────────────
    # Original bbox-based search (unchanged)
    # ──────────────────────────────────────────────────────────

    def search(self, req: PlacesRequest) -> PlacesPack:
        pkey = places_key(req.model_dump(), self.algo_version)

        cached = get_places_pack(self.cache_conn, pkey)
        if cached:
            pack = PlacesPack.model_validate(cached)

            migrate_cached = bool(getattr(settings, "supa_places_publish_cached_packs", True))
            if migrate_cached and self.supa is not None and pack.items and ("supa" not in (pack.provider or "")):
                cap = int(getattr(settings, "supa_places_publish_cap", 4000))
                subset = pack.items[:cap] if cap else pack.items
                self._supa_upsert_best_effort(subset, source="cached_pack")

            return pack

        bbox = _bbox_from_req(req)
        if not bbox:
            pack = PlacesPack(
                places_key=pkey,
                req=req,
                items=[],
                provider="local",
                created_at=utc_now_iso(),
                algo_version=self.algo_version,
            )
            return self._finalize_and_cache_pack(pack, publish_to_supa=False)

        limit = int(req.limit or 50)
        limit = max(1, min(limit, int(getattr(settings, "places_hard_cap", 12000))))
        cats = req.categories or []

        min_ratio = float(getattr(settings, "places_local_satisfy_ratio", 0.70))
        need_count = max(1, int(limit * min_ratio))

        items: List[PlaceItem] = []
        seen_ids: set[str] = set()

        # 1) local store
        try:
            local_items = self.store.query_bbox(bbox=bbox, categories=cats, limit=limit)
        except Exception as e:
            logger.warning("PlacesStore.query_bbox FAILED: %r", e)
            local_items = []

        for it in local_items:
            if it.id in seen_ids:
                continue
            seen_ids.add(it.id)
            items.append(it)
            if len(items) >= limit:
                break

        if len(items) >= need_count:
            pack = PlacesPack(
                places_key=pkey,
                req=req,
                items=items[:limit],
                provider="local",
                created_at=utc_now_iso(),
                algo_version=self.algo_version,
            )
            return self._finalize_and_cache_pack(pack, publish_to_supa=True)

        # 2) Supabase read-through
        provider_used = "local"
        supa_hit = 0
        if self.supa is not None:
            try:
                supa_items = self.supa.query_bbox(bbox=bbox, categories=cats, limit=limit)
                if supa_items:
                    supa_hit = len(supa_items)
                    self._supa_ingest_best_effort(supa_items)

                    for it in supa_items:
                        if it.id in seen_ids:
                            continue
                        seen_ids.add(it.id)
                        items.append(it)
                        if len(items) >= limit:
                            break

                    provider_used = "local+supa"
            except Exception as e:
                logger.warning("SupaPlacesRepo.query_bbox FAILED: %r", e)

        if len(items) >= need_count:
            pack = PlacesPack(
                places_key=pkey,
                req=req,
                items=items[:limit],
                provider=provider_used,
                created_at=utc_now_iso(),
                algo_version=self.algo_version,
            )
            return self._finalize_and_cache_pack(pack, publish_to_supa=("supa" not in provider_used))

        # 3) Overpass top-up
        filters = _overpass_filters_for_categories(cats)
        name_clause = ""
        if req.query:
            safe = _safe_overpass_name_regex(req.query)
            if safe:
                name_clause = f'["name"~"{safe}",i]'

        if not filters and not name_clause:
            pack = PlacesPack(
                places_key=pkey,
                req=req,
                items=items[:limit],
                provider=provider_used,
                created_at=utc_now_iso(),
                algo_version=self.algo_version,
            )
            return self._finalize_and_cache_pack(pack, publish_to_supa=("supa" not in provider_used))

        tile_step = float(getattr(settings, "places_tile_step_deg", 0.15))
        max_tiles = int(getattr(settings, "places_max_tiles", 64))
        throttle_s = float(getattr(settings, "overpass_throttle_s", 0.20))
        ttl_s = int(getattr(settings, "places_tile_ttl_s", 60 * 60 * 24 * 14))
        time_budget_s = float(getattr(settings, "places_time_budget_s", 10.0))
        max_overpass_tiles = int(getattr(settings, "places_max_overpass_tiles_per_req", 12))

        tiles = self.store.tiles_for_bbox(bbox=bbox, step_deg=tile_step, max_tiles=max_tiles)

        timeout_s = float(getattr(settings, "overpass_timeout_s", 90))
        timeout = httpx.Timeout(timeout_s, connect=10.0)

        started = time.time()
        tiles_fetched = 0
        used_overpass = False
        total_overpass_items = 0
        total_supa_published = 0

        try:
            with httpx.Client(timeout=timeout) as client:
                for (tile_key, tb) in tiles:
                    if len(items) >= limit:
                        break

                    if (time.time() - started) >= time_budget_s and tiles_fetched > 0:
                        break

                    if self.store.tile_is_fresh(tile_key=tile_key, ttl_s=ttl_s):
                        continue

                    ql = _build_overpass_ql(bbox=tb, filters=filters, name_clause=name_clause)
                    data = _fetch_overpass_with_retries(client=client, ql=ql)

                    fetched_items: List[PlaceItem] = []
                    got = 0
                    for el in (data.get("elements") or []):
                        it = _element_to_item(el)
                        if not it:
                            continue
                        fetched_items.append(it)
                        got += 1

                    if fetched_items:
                        used_overpass = True
                        total_overpass_items += len(fetched_items)

                        try:
                            self.store.upsert_items(fetched_items)
                        except Exception as e:
                            logger.warning("PlacesStore.upsert_items FAILED: %r", e)

                        total_supa_published += self._supa_upsert_best_effort(
                            fetched_items,
                            source="overpass",
                        )

                    try:
                        self.store.mark_tile_fetched(
                            tile_key=tile_key,
                            bbox=tb,
                            categories=cats,
                            item_count=int(got),
                        )
                    except Exception as e:
                        logger.warning("PlacesStore.mark_tile_fetched FAILED: %r", e)

                    for it in fetched_items:
                        if it.id in seen_ids:
                            continue
                        seen_ids.add(it.id)
                        items.append(it)
                        if len(items) >= limit:
                            break

                    tiles_fetched += 1

                    if tiles_fetched >= max_overpass_tiles:
                        break
                    if throttle_s > 0:
                        time.sleep(throttle_s)

        except Exception as e:
            logger.warning("overpass loop FAILED: %r", e)

        if used_overpass:
            if provider_used == "local+supa":
                final_provider = "local+supa+overpass"
            else:
                final_provider = "local+overpass"
        else:
            final_provider = provider_used

        if used_overpass and self.supa is not None and total_supa_published > 0:
            if "supa" not in final_provider:
                final_provider = final_provider.replace("local", "local+supa")

        if used_overpass or supa_hit > 0:
            logger.info(
                "search summary: provider=%s local=%d supa_hit=%d overpass_tiles=%d overpass_items=%d supa_published=%d",
                final_provider,
                len(local_items),
                supa_hit,
                tiles_fetched,
                total_overpass_items,
                total_supa_published,
            )

        pack = PlacesPack(
            places_key=pkey,
            req=req,
            items=items[:limit],
            provider=final_provider,
            created_at=utc_now_iso(),
            algo_version=self.algo_version,
        )

        return self._finalize_and_cache_pack(pack, publish_to_supa=True)

    # ──────────────────────────────────────────────────────────
    # Suggest along route
    # ──────────────────────────────────────────────────────────

    def suggest_along_route(
        self,
        *,
        polyline6: str,
        interval_km: int,
        radius_m: int,
        categories: List[PlaceCategory],
        limit_per_sample: int,
    ) -> List[dict]:
        samples = _sample_route_points(polyline6, interval_km)
        out: List[dict] = []

        for (idx, lat, lng, km) in samples:
            preq = PlacesRequest(
                center={"lat": lat, "lng": lng},
                radius_m=int(radius_m),
                categories=categories,
                limit=int(limit_per_sample),
            )
            pack = self.search(preq)
            out.append(
                {
                    "idx": idx,
                    "lat": lat,
                    "lng": lng,
                    "km_from_start": km,
                    "places": pack,
                }
            )

        return out
