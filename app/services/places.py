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
    """
    Approximate minimum distance (metres) from a point to a set of route
    sample points.  For densely-sampled polylines (every 5-10 km) this is
    a good-enough proxy for distance-to-route without segment projection.
    """
    best = float("inf")
    for s in samples:
        d = _haversine_m((lat, lng), s)
        if d < best:
            best = d
            if d < 500.0:          # close enough, skip rest
                break
    return best


# ──────────────────────────────────────────────────────────────
# Route sampling (shared between corridor + suggest)
# ──────────────────────────────────────────────────────────────

def _sample_polyline(
    poly6: str, interval_km: float, *, include_endpoints: bool = True
) -> List[Tuple[float, float]]:
    """
    Walk along a Polyline6-encoded route and emit (lat, lng) every
    `interval_km` kilometres.  Always includes first and last point
    when `include_endpoints` is True.
    """
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

    # Straight-line distance first→last as a sanity check
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

        # Catch NaN — would silently break all subsequent comparisons
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

    # Safety: if we have a long route but suspiciously few samples,
    # fall back to evenly spaced decoded points
    expected_min = max(2, int(dist_acc / 1000.0 / interval_km) - 1)
    if len(samples) < expected_min and dist_acc > interval_m * 2:
        print(
            f"[_sample_polyline] WARNING: expected ~{expected_min} samples "
            f"but got {len(samples)} — falling back to uniform point pick"
        )
        n_want = max(2, int(dist_acc / 1000.0 / interval_km) + 2)
        step = max(1, len(pts) // n_want)
        samples = [(float(pts[j][0]), float(pts[j][1])) for j in range(0, len(pts), step)]
        # Always include last point
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
    # Essentials
    "fuel":       ['["amenity"="fuel"]'],
    "toilet":     ['["amenity"="toilets"]'],
    "water":      ['["amenity"="drinking_water"]', '["man_made"="water_well"]'],
    "camp":       ['["tourism"="camp_site"]', '["tourism"="caravan_site"]'],
    "town":       ['["place"~"^(town|village|city|hamlet)$"]'],
    "grocery":    ['["shop"="supermarket"]', '["shop"="convenience"]'],
    "mechanic":   ['["shop"="car_repair"]', '["amenity"="car_repair"]'],
    "hospital":   ['["amenity"="hospital"]'],
    "pharmacy":   ['["amenity"="pharmacy"]'],

    # Food & drink
    "cafe":       ['["amenity"="cafe"]'],
    "restaurant": ['["amenity"="restaurant"]'],
    "fast_food":  ['["amenity"="fast_food"]'],
    "pub":        ['["amenity"="pub"]'],
    "bar":        ['["amenity"="bar"]'],

    # Accommodation
    "hotel":      ['["tourism"="hotel"]'],
    "motel":      ['["tourism"="motel"]'],
    "hostel":     ['["tourism"="hostel"]'],

    # Nature & outdoors
    "viewpoint":      ['["tourism"="viewpoint"]'],
    "waterfall":      ['["waterway"="waterfall"]'],
    "swimming_hole":  ['["leisure"="swimming_area"]', '["sport"="swimming"]["natural"]'],
    "national_park":  ['["boundary"="national_park"]', '["leisure"="nature_reserve"]'],
    "picnic":         ['["tourism"="picnic_site"]', '["leisure"="picnic_table"]'],
    "hiking":         ['["highway"="path"]["sac_scale"]', '["route"="hiking"]'],

    # Sightseeing
    "attraction": ['["tourism"="attraction"]'],
    "park":       ['["leisure"="park"]'],
    "beach":      ['["natural"="beach"]'],
    "museum":     ['["tourism"="museum"]'],
    "gallery":    ['["tourism"="gallery"]'],
    "zoo":        ['["tourism"="zoo"]'],
    "theme_park": ['["tourism"="theme_park"]'],
    "heritage":   ['["heritage"]'],
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


def _infer_category(tags: Dict[str, Any]) -> PlaceCategory:
    a = tags.get("amenity")
    t = tags.get("tourism")
    p = tags.get("place")
    s = tags.get("shop")
    mm = tags.get("man_made")
    le = tags.get("leisure")
    n = tags.get("natural")
    w = tags.get("waterway")
    b = tags.get("boundary")

    # Essentials
    if a == "fuel":
        return "fuel"
    if a == "toilets":
        return "toilet"
    if a == "drinking_water" or mm == "water_well":
        return "water"
    if t in ("camp_site", "caravan_site"):
        return "camp"
    if p in ("city", "town", "village", "hamlet"):
        return "town"
    if s in ("supermarket", "convenience"):
        return "grocery"
    if s == "car_repair" or a == "car_repair":
        return "mechanic"
    if a == "hospital":
        return "hospital"
    if a == "pharmacy":
        return "pharmacy"

    # Nature & outdoors (BEFORE generic attraction)
    if w == "waterfall":
        return "waterfall"  # type: ignore
    if le == "swimming_area" or (tags.get("sport") == "swimming" and n):
        return "swimming_hole"  # type: ignore
    if b == "national_park" or le == "nature_reserve":
        return "national_park"  # type: ignore
    if t == "picnic_site" or le == "picnic_table":
        return "picnic"  # type: ignore
    if tags.get("route") == "hiking" or (tags.get("highway") == "path" and tags.get("sac_scale")):
        return "hiking"  # type: ignore
    if t == "viewpoint":
        return "viewpoint"
    if n == "beach":
        return "beach"  # type: ignore
    if le == "park":
        return "park"  # type: ignore

    # Food & drink
    if a == "cafe":
        return "cafe"  # type: ignore
    if a == "restaurant":
        return "restaurant"  # type: ignore
    if a == "fast_food":
        return "fast_food"  # type: ignore
    if a == "pub":
        return "pub"  # type: ignore
    if a == "bar":
        return "bar"  # type: ignore

    # Accommodation
    if t == "hotel":
        return "hotel"  # type: ignore
    if t == "motel":
        return "motel"  # type: ignore
    if t == "hostel":
        return "hostel"  # type: ignore

    # Sightseeing (last — broadest bucket)
    if t == "museum":
        return "museum"  # type: ignore
    if t == "gallery":
        return "gallery"  # type: ignore
    if t == "zoo":
        return "zoo"  # type: ignore
    if t == "theme_park":
        return "theme_park"  # type: ignore
    if tags.get("heritage"):
        return "heritage"  # type: ignore
    if t == "attraction":
        return "attraction"  # type: ignore

    return "town"


def _element_to_item(el: Dict[str, Any]) -> Optional[PlaceItem]:
    tags = el.get("tags") or {}
    name = tags.get("name") or tags.get("brand") or tags.get("operator")
    if not name:
        return None

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

    extra = dict(tags)
    extra["osm_type"] = osm_type
    extra["osm_id"] = osm_id

    return PlaceItem(
        id=f"osm:{osm_type}:{osm_id}",
        name=str(name),
        lat=float(lat),
        lng=float(lon),
        category=_infer_category(tags),
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
    """
    Build an Overpass QL query using the ``around`` filter so results
    follow the actual route polyline instead of a rectangular bbox.

    ``coords`` are (lat, lng) sample points along the route.
    ``radius_m`` is the corridor buffer in metres.
    ``max_coords`` caps coordinate count to avoid query-length issues.
    """
    if len(coords) > max_coords:
        step = max(1, len(coords) // max_coords)
        coords = coords[::step]
        # always include last point
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
    """
    Deterministic cache key for a corridor-polyline places query.
    Uses a SHA-256 of the polyline + params so the same route always
    resolves to the same cache entry.
    """
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
    Places service (GLOBAL POOLING + LOCAL HOT CACHE):

    Read order:
      0) Deterministic PlacesPack cache (places_packs)
      1) Local canonical store (PlacesStore)
      2) Supabase canonical store (roam_places_items)
      3) Overpass top-up (tile-based or around-based, time-budgeted)

    Write-behind:
      - Overpass discoveries are inserted into local store and published to Supabase (best-effort).
      - Supabase hits are ingested into local store (hot cache).
      - Pack-level backfill: if pack served from local/cache, still publish (capped) so global pool converges.
    """

    def __init__(
        self,
        *,
        cache_conn,
        algo_version: str = "places.v1.overpass.tiled",
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
        """
        Single exit point:
          - optional: publish pack items to Supa (capped)
          - always: write deterministic pack cache
        """
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
    # Corridor-aware route search (NEW)
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
        """
        Query places inside a true corridor buffer around the route
        polyline — not a start-to-end rectangle.

        Strategy:
        1. Sample (lat,lng) every ``sample_interval_km`` along the polyline.
        2. Build an enclosing bbox (for local-store / Supa rect queries),
           then post-filter results by distance-to-route.
        3. If local+supa don't satisfy the limit, issue a SINGLE Overpass
           ``around`` query using the sampled coordinates + ``buffer_km``
           radius.  This tells Overpass to search along the actual road
           shape instead of a giant rectangle.
        4. Deduplicate, cache, return.
        """
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
            # Best-effort Supa migration for old cached packs
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

        # ── 1) Sample route ──────────────────────────────────
        samples = _sample_polyline(
            polyline6, sample_interval_km, include_endpoints=True,
        )
        if not samples:
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
            """Return True if item is within buffer of the route."""
            if it.id in seen_ids:
                return False
            d = _min_distance_to_samples_m(it.lat, it.lng, samples)
            return d <= buffer_m

        # ── 2) Local store ───────────────────────────────────
        provider_used = "corridor"
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
                if len(items) >= limit:
                    break

        local_count = len(items)

        # ── 3) Supa read-through ─────────────────────────────
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

        # ── 4) Overpass around query ─────────────────────────
        min_ratio = float(getattr(settings, "places_local_satisfy_ratio", 0.70))
        need_count = max(1, int(limit * min_ratio))

        used_overpass = False
        overpass_items_total = 0

        if len(items) < need_count:
            filters = _overpass_filters_for_categories(categories)
            if filters or categories:
                # Build a single corridor-aware Overpass query
                ql = _build_overpass_around_ql(
                    coords=samples,
                    radius_m=buffer_m,
                    filters=filters,
                    name_clause="",
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

                    if fetched:
                        used_overpass = True
                        overpass_items_total = len(fetched)

                        # Persist into local store
                        try:
                            self.store.upsert_items(fetched)
                        except Exception as e:
                            print(f"[PlacesStore] corridor upsert FAILED: {repr(e)}")

                        # Publish to Supa
                        self._supa_upsert_best_effort(fetched, source="overpass_corridor")

                        # Merge into result set (already distance-filtered by Overpass around)
                        for it in fetched:
                            if it.id not in seen_ids:
                                seen_ids.add(it.id)
                                items.append(it)
                                if len(items) >= limit:
                                    break

                except Exception as e:
                    print(f"[Places] corridor overpass FAILED: {repr(e)}")

        # ── 5) Finalize ──────────────────────────────────────
        if used_overpass:
            provider_used = (
                f"{provider_used}+overpass"
                if "overpass" not in provider_used
                else provider_used
            )

        print(
            "[Places] corridor_polyline summary:",
            f"provider={provider_used}",
            f"samples={len(samples)}",
            f"local={local_count}",
            f"supa_hit={supa_hit}",
            f"overpass_items={overpass_items_total}",
            f"total={len(items)}",
        )

        # Build a synthetic PlacesRequest for pack serialization
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

        # 0) deterministic pack cache
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

        # 2) Supabase read-through (shared)
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

        # 3) Overpass top-up (tile-based for bbox queries)
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
    # Suggest along route (uses corridor polyline search now)
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