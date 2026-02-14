from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
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



# ──────────────────────────────────────────────────────────────
# Overpass mapping + classification
# ──────────────────────────────────────────────────────────────

_FALLBACK_FILTERS: Dict[str, List[str]] = {
    "fuel": ['["amenity"="fuel"]'],
    "toilet": ['["amenity"="toilets"]'],
    "water": ['["amenity"="drinking_water"]', '["man_made"="water_well"]'],
    "camp": ['["tourism"="camp_site"]', '["tourism"="caravan_site"]'],
    "town": ['["place"~"^(town|village|city|hamlet)$"]'],
    "grocery": ['["shop"="supermarket"]', '["shop"="convenience"]'],
    "mechanic": ['["shop"="car_repair"]', '["amenity"="car_repair"]'],
    "hospital": ['["amenity"="hospital"]'],
    "pharmacy": ['["amenity"="pharmacy"]'],
    "viewpoint": ['["tourism"="viewpoint"]'],

    "cafe": ['["amenity"="cafe"]'],
    "restaurant": ['["amenity"="restaurant"]'],
    "fast_food": ['["amenity"="fast_food"]'],
    "pub": ['["amenity"="pub"]'],
    "bar": ['["amenity"="bar"]'],

    "hotel": ['["tourism"="hotel"]'],
    "motel": ['["tourism"="motel"]'],
    "hostel": ['["tourism"="hostel"]'],

    "attraction": ['["tourism"="attraction"]'],
    "park": ['["leisure"="park"]'],
    "beach": ['["natural"="beach"]'],
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
    l = tags.get("leisure")
    n = tags.get("natural")

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
    if t == "viewpoint":
        return "viewpoint"

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

    if t == "hotel":
        return "hotel"  # type: ignore
    if t == "motel":
        return "motel"  # type: ignore
    if t == "hostel":
        return "hostel"  # type: ignore

    if t == "attraction":
        return "attraction"  # type: ignore
    if l == "park":
        return "park"  # type: ignore
    if n == "beach":
        return "beach"  # type: ignore

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
# Route sampling for /places/suggest
# ──────────────────────────────────────────────────────────────

def _haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    R = 6371000.0
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(a[0] if False else b[0]), math.radians(b[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(x))


def _sample_route_points(poly6: str, interval_km: int) -> List[Tuple[int, float, float, float]]:
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
# Service
# ──────────────────────────────────────────────────────────────

class Places:
    """
    Places service (GLOBAL POOLING + LOCAL HOT CACHE):

    Read order:
      0) Deterministic PlacesPack cache (places_packs)
      1) Local canonical store (PlacesStore)
      2) Supabase canonical store (roam_places_items)
      3) Overpass top-up (tile-based, time-budgeted)

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

    def search(self, req: PlacesRequest) -> PlacesPack:
        pkey = places_key(req.model_dump(), self.algo_version)

        # 0) deterministic pack cache
        cached = get_places_pack(self.cache_conn, pkey)
        if cached:
            pack = PlacesPack.model_validate(cached)

            # Optional migration behavior:
            # If old cached pack doesn’t mention supa, publish it once (capped) to seed global pool.
            # This is how you turn “already cached local packs” into Supa rows without deleting cache.
            migrate_cached = bool(getattr(settings, "supa_places_publish_cached_packs", True))
            if migrate_cached and self.supa is not None and pack.items and ("supa" not in (pack.provider or "")):
                # Don’t re-cache; just best-effort publish.
                cap = int(getattr(settings, "supa_places_publish_cap", 4000))
                subset = pack.items[:cap] if cap else pack.items
                self._supa_upsert_best_effort(subset, source="cached_pack")
                # NOTE: we don’t mutate provider here; cached pack should remain deterministic.

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
        seen_ids = set()

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
            # Backfill publish: yes (this is the core fix)
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
            # If provider already includes supa, no need to publish again.
            return self._finalize_and_cache_pack(pack, publish_to_supa=("supa" not in provider_used))

        # 3) Overpass top-up (only if we have filters or query)
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

        # Overpass policy knobs (UX sanity)
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

        # Backfill publish (yes). This is the key behavior you want.
        return self._finalize_and_cache_pack(pack, publish_to_supa=True)

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
