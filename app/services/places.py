from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import hashlib
import math
import time
import random
import re

import httpx

from app.core.contracts import PlaceItem, PlacesPack, PlacesRequest, BBox4, PlaceCategory
from app.core.keying import places_key
from app.core.time import utc_now_iso
from app.core.storage import get_places_pack, put_places_pack
from app.core.settings import settings
from app.core.polyline6 import decode_polyline6

from app.services.places_store import PlacesStore
from app.services.places_supa import SupaPlacesRepo

print("[PLACES MODULE LOADED]", __file__)


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

    print(
        f"[_sample_polyline] polyline_chars={len(poly6)}  "
        f"decoded_points={len(pts)}  interval_km={interval_km}"
    )

    if not pts or len(pts) < 2:
        print("[_sample_polyline] ABORT: fewer than 2 decoded points")
        return []

    first_pt = pts[0]
    last_pt = pts[-1]
    print(
        f"[_sample_polyline] first=({first_pt[0]:.6f},{first_pt[1]:.6f})  "
        f"last=({last_pt[0]:.6f},{last_pt[1]:.6f})"
    )

    straight_m = _haversine_m(
        (float(first_pt[0]), float(first_pt[1])),
        (float(last_pt[0]), float(last_pt[1])),
    )
    print(f"[_sample_polyline] straight_line_km={straight_m / 1000:.2f}")

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

    print(
        f"[_sample_polyline] RESULT: samples={len(samples)}  "
        f"total_dist_km={dist_acc / 1000:.2f}  "
        f"zero_segs={zero_segs}  nan_segs={nan_segs}"
    )

    expected_min = max(2, int(dist_acc / 1000.0 / interval_km) - 1)
    if len(samples) < expected_min and dist_acc > interval_m * 2:
        print(
            f"[_sample_polyline] WARNING: expected ~{expected_min} samples "
            f"but got {len(samples)} — falling back to uniform point pick"
        )
        n_want = max(2, int(dist_acc / 1000.0 / interval_km) + 2)
        step = max(1, len(pts) // n_want)
        samples = [(float(pts[j][0]), float(pts[j][1])) for j in range(0, len(pts), step)]
        last = (float(pts[-1][0]), float(pts[-1][1]))
        if _haversine_m(samples[-1], last) > 500.0:
            samples.append(last)
        print(f"[_sample_polyline] fallback produced {len(samples)} samples")

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
    ],
    "beach": [
        '["natural"="beach"]',
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
    ],
    "theme_park": [
        '["tourism"="theme_park"]',
        '["leisure"="amusement_arcade"]',
        '["leisure"="miniature_golf"]',
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
    ],
    "market": [
        '["amenity"="marketplace"]',
        '["shop"="farm"]',
    ],
    "park": [
        '["leisure"="park"]',
        '["leisure"="garden"]',
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

    if t in ("camp_site", "caravan_site", "camp_pitch"):
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
    if le == "swimming_area" or (tags.get("sport") == "swimming" and n) or (n == "spring" and tags.get("bathing") == "yes"):
        return "swimming_hole"
    if n == "beach":
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
    ):
        return "hiking"

    # ── FAMILY & RECREATION ──────────────────────────────────

    if le == "playground":
        return "playground"
    if le in ("swimming_pool", "water_park") or a == "public_bath":
        return "pool"
    if t == "zoo" or tags.get("attraction") == "animal":
        return "zoo"
    if t == "theme_park" or le in ("amusement_arcade", "miniature_golf"):
        return "theme_park"

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
    if tags.get("heritage") or hi in ("monument", "memorial", "ruins"):
        return "heritage"
    if a == "marketplace" or s == "farm":
        return "market"
    if le in ("park", "garden"):
        return "park"
    if t == "attraction":
        return "attraction"

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
        if tags.get("fee") == "no":
            base = f"Free {base}"
    elif category == "hiking":
        if tags.get("route") in ("hiking", "foot"):
            base = "Walking Trail"
        elif tags.get("information") == "guidepost":
            base = "Trail Marker"
    elif category == "heritage":
        ht = tags.get("historic", "")
        if ht == "monument":
            base = "Monument"
        elif ht == "memorial":
            base = "Memorial"
        elif ht == "ruins":
            base = "Ruins"
    elif category == "toilet":
        if tags.get("access") == "customers":
            base = "Customer Toilet"
        if tags.get("wheelchair") == "yes":
            base = f"{base} (Accessible)"

    if locality:
        return f"{base} — {locality}"
    elif street:
        return f"{base} — {street}"
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
# Service
# ──────────────────────────────────────────────────────────────

class Places:
    """
    Places service — OVERPASS-FIRST corridor search.

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
            print(f"[SupaPlacesRepo] upsert attempt: n={len(items)} source={source}")
            n = self.supa.upsert_items(items, source=source)
            print(f"[SupaPlacesRepo] upsert ok: n={n}")
            return int(n)
        except Exception as e:
            print(f"[SupaPlacesRepo] upsert FAILED: {repr(e)}")
            return 0

    def _supa_ingest_best_effort(self, items: List[PlaceItem]) -> None:
        if not items:
            return
        try:
            self.store.upsert_items(items)
        except Exception as e:
            print(f"[PlacesStore] ingest from supa FAILED: {repr(e)}")

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
    # Corridor-aware route search — OVERPASS FIRST
    # ──────────────────────────────────────────────────────────
    #
    # The critical change from v2: Overpass runs FIRST, then
    # local/supa supplement.  Previously local ran first and if it
    # had enough destination-area items (>70% of limit), Overpass
    # was skipped — meaning start and mid-route got nothing.
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

        print(
            f"[Places] search_corridor_polyline ENTRY: "
            f"polyline_len={len(polyline6)}  "
            f"buffer_km={buffer_km}  "
            f"cats={len(categories)}  "
            f"limit={limit}  "
            f"interval_km={sample_interval_km}  "
            f"poly_head={polyline6[:40]}...  "
            f"poly_tail=...{polyline6[-40:]}"
        )

        pkey = _corridor_places_key(
            polyline6, buffer_km, cats_str, limit, self.algo_version,
        )

        # ── 0) Pack cache ────────────────────────────────────
        cached = get_places_pack(self.cache_conn, pkey)
        if cached:
            pack = PlacesPack.model_validate(cached)
            print(
                f"[Places] corridor cache HIT: key={pkey[:16]}… "
                f"items={len(pack.items)} provider={pack.provider}"
            )
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

        print(f"[Places] corridor cache MISS — running full pipeline")

        # ── 1) Sample route ──────────────────────────────────
        samples = _sample_polyline(
            polyline6, sample_interval_km, include_endpoints=True,
        )
        if not samples:
            print("[Places] corridor: no samples from polyline — returning empty")
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
        # STEP 2: OVERPASS AROUND QUERY — RUNS FIRST
        # ──────────────────────────────────────────────────────
        # This is the whole point of corridor search: query a true
        # buffer along the actual road, from start to end.  Running
        # this first ensures every stretch of the route gets coverage
        # regardless of what's in the local store.
        # ──────────────────────────────────────────────────────

        provider_used = "corridor"
        overpass_items_total = 0
        used_overpass = False

        filters = _overpass_filters_for_categories(categories)
        if filters or categories:
            ql = _build_overpass_around_ql(
                coords=samples,
                radius_m=buffer_m,
                filters=filters,
                name_clause="",
            )

            print(
                f"[Places] corridor Overpass around query: "
                f"samples={len(samples)}  radius_m={buffer_m}  "
                f"filters={len(filters)}  ql_len={len(ql)}"
            )

            timeout_s = float(getattr(settings, "overpass_timeout_s", 90))
            timeout = httpx.Timeout(timeout_s, connect=15.0)

            try:
                with httpx.Client(timeout=timeout) as client:
                    data = _fetch_overpass_with_retries(client=client, ql=ql)

                fetched: List[PlaceItem] = []
                for el in data.get("elements") or []:
                    it = _element_to_item(el)
                    if it is not None:
                        fetched.append(it)

                overpass_items_total = len(fetched)
                print(
                    f"[Places] corridor Overpass returned: "
                    f"raw_elements={len(data.get('elements') or [])}  "
                    f"parsed_items={overpass_items_total}"
                )

                if fetched:
                    used_overpass = True

                    # Persist to local store for future queries
                    try:
                        self.store.upsert_items(fetched)
                    except Exception as e:
                        print(f"[PlacesStore] corridor upsert FAILED: {repr(e)}")

                    # Publish to supa (best-effort)
                    self._supa_upsert_best_effort(fetched, source="overpass_corridor")

                    # Accept items within the corridor buffer
                    for it in fetched:
                        if _accept(it):
                            seen_ids.add(it.id)
                            items.append(it)
                            if len(items) >= limit:
                                break

                    print(
                        f"[Places] corridor after Overpass: "
                        f"accepted={len(items)}/{overpass_items_total}"
                    )

            except Exception as e:
                print(f"[Places] corridor Overpass FAILED: {repr(e)}")

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
                print(f"[PlacesStore] corridor query_bbox FAILED: {repr(e)}")
                local_items = []

            for it in local_items:
                if _accept(it):
                    seen_ids.add(it.id)
                    items.append(it)
                    local_count += 1
                    if len(items) >= limit:
                        break

            print(f"[Places] corridor after local supplement: +{local_count} → total={len(items)}")

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
                print(f"[SupaPlacesRepo] corridor query_bbox FAILED: {repr(e)}")

        # ── 5) Finalize ──────────────────────────────────────
        if used_overpass:
            provider_used = (
                f"{provider_used}+overpass"
                if "overpass" not in provider_used
                else provider_used
            )

        print(
            "[Places] corridor_polyline FINAL summary:",
            f"provider={provider_used}",
            f"samples={len(samples)}",
            f"overpass_raw={overpass_items_total}",
            f"local_supplement={local_count}",
            f"supa_supplement={supa_hit}",
            f"total={len(items)}",
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
            print(f"[PlacesStore] query_bbox FAILED: {repr(e)}")
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
                print(f"[SupaPlacesRepo] query_bbox FAILED: {repr(e)}")

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
                            print(f"[PlacesStore] upsert_items FAILED: {repr(e)}")

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
                        print(f"[PlacesStore] mark_tile_fetched FAILED: {repr(e)}")

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
            print(f"[Places] overpass loop FAILED: {repr(e)}")

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
            print(
                "[Places] summary:",
                f"provider={final_provider}",
                f"local={len(local_items)}",
                f"supa_hit={supa_hit}",
                f"overpass_tiles={tiles_fetched}",
                f"overpass_items={total_overpass_items}",
                f"supa_published={total_supa_published}",
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