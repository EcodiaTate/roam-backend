# app/services/traffic.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import base64
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import httpx

from app.core.contracts import BBox4, TrafficEvent, TrafficOverlay
from app.core.settings import settings
from app.core.storage import get_traffic_pack, put_traffic_pack
from app.core.time import utc_now_iso


def _stable_key(prefix: str, obj: dict) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    h = hashlib.sha256((prefix + "::" + raw).encode("utf-8")).digest()
    return base64.urlsafe_b64encode(h).decode("utf-8").rstrip("=")


def _stable_id(parts: List[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()[:24]


def _bbox_intersects(a: List[float], b: BBox4) -> bool:
    # a = [minLng,minLat,maxLng,maxLat]
    return not (a[2] < b.minLng or a[0] > b.maxLng or a[3] < b.minLat or a[1] > b.maxLat)


def _bbox_from_geom(geom: Optional[Dict[str, Any]]) -> Optional[List[float]]:
    if not geom:
        return None

    coords: List[List[float]] = []

    def walk(x: Any) -> None:
        if isinstance(x, list):
            if len(x) == 2 and all(isinstance(v, (int, float)) for v in x):
                coords.append([float(x[0]), float(x[1])])
            else:
                for v in x:
                    walk(v)

    walk(geom.get("coordinates"))
    if not coords:
        return None

    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    return [min(xs), min(ys), max(xs), max(ys)]


def _parse_iso_to_epoch(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        t = str(s).strip()
        if not t:
            return None
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _is_fresh(created_at: Optional[str], *, max_age_s: int) -> bool:
    if max_age_s <= 0:
        return False
    ts = _parse_iso_to_epoch(created_at)
    if ts is None:
        return False
    return (time.time() - ts) <= float(max_age_s)


def _classify(headline: str, desc: str) -> Tuple[str, str]:
    hay = f"{headline} {desc}".lower()
    if "road closed" in hay or "closure" in hay or "closed" in hay:
        return "closure", "major"
    if "roadworks" in hay or "works" in hay:
        return "roadworks", "moderate"
    if "congestion" in hay or "heavy traffic" in hay:
        return "congestion", "minor"
    if "flood" in hay or "flooding" in hay:
        return "flooding", "major"
    if "crash" in hay or "incident" in hay or "collision" in hay:
        return "incident", "moderate"
    return "hazard", "info"


def _env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else None


def _append_query_params(url: str, params: Dict[str, str]) -> str:
    if not params:
        return url
    if "?" in url:
        return url + "&" + "&".join([f"{k}={v}" for k, v in params.items()])
    return url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])


def _event_is_too_old(props: Dict[str, Any], include_past_hours: int) -> bool:
    """
    Drop events that ended more than N hours ago (0 disables).
    We look for common "end-ish" fields and parse ISO timestamps when possible.
    """
    if include_past_hours <= 0:
        return False

    endish = (
        props.get("end")
        or props.get("end_time")
        or props.get("endTime")
        or props.get("expires")
        or props.get("expiry")
        or props.get("to")
        or props.get("valid_to")
    )
    ts = _parse_iso_to_epoch(str(endish)) if endish else None
    if ts is None:
        return False
    return (time.time() - ts) > float(include_past_hours * 3600)


class _QldTrafficCache:
    """
    In-process merge cache for the official QLD Traffic events endpoint.
    We still persist the final overlay pack to SQLite keyed by traffic_key.
    This cache just reduces API churn during bursty calls.
    """

    def __init__(self) -> None:
        self.full_at: float = 0.0
        self.delta_at: float = 0.0
        self.features_by_id: Dict[str, Dict[str, Any]] = {}

    def is_full_stale(self, full_refresh_s: int) -> bool:
        if not self.features_by_id:
            return True
        return (time.time() - self.full_at) > float(max(1, full_refresh_s))

    def can_use_cached(self, ttl_s: int) -> bool:
        if not self.features_by_id:
            return False
        return (time.time() - max(self.full_at, self.delta_at)) <= float(max(1, ttl_s))


_QCACHE = _QldTrafficCache()


class Traffic:
    def __init__(self, *, conn):
        self.conn = conn

    # ──────────────────────────────────────────────────────────
    # Source selection
    # ──────────────────────────────────────────────────────────

    def _qld_events_urls(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Preferred: official QLD Traffic v2 API.
        Uses Settings fields (which alias env vars), but keeps env fallback for safety.

        Env names supported:
          QLDTRAFFIC_EVENTS_URL
          QLDTRAFFIC_EVENTS_DELTA_URL
          QLDTRAFFIC_API_KEY
        """
        events_url = getattr(settings, "qldtraffic_events_url", None) or _env("QLDTRAFFIC_EVENTS_URL")
        delta_url = getattr(settings, "qldtraffic_events_delta_url", None) or _env("QLDTRAFFIC_EVENTS_DELTA_URL")
        api_key = getattr(settings, "qldtraffic_api_key", None) or _env("QLDTRAFFIC_API_KEY")

        # Treat empty string as missing
        if isinstance(api_key, str) and not api_key.strip():
            api_key = None

        return (
            str(events_url).strip() if events_url else None,
            str(delta_url).strip() if delta_url else None,
            str(api_key).strip() if api_key else None,
        )

    def _feeds_geojson(self) -> List[Tuple[str, str]]:
        """
        Fallback: per-feed GeoJSON URLs (optional/back-compat).
        """
        out: List[Tuple[str, str]] = []
        m = [
            ("incidents", settings.qldtraffic_incidents_url),
            ("roadworks", settings.qldtraffic_roadworks_url),
            ("closures", settings.qldtraffic_closures_url),
            ("flooding", settings.qldtraffic_flooding_url),
        ]
        for name, url in m:
            if url:
                out.append((name, str(url)))
        return out

    # ──────────────────────────────────────────────────────────
    # Official QLD Traffic API
    # ──────────────────────────────────────────────────────────

    async def _fetch_json(self, client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
        r = await client.get(url, headers={"User-Agent": "roam/traffic"})
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, dict) else {}

    def _feature_source_id(self, feature: Dict[str, Any]) -> str:
        props = feature.get("properties") or {}
        if not isinstance(props, dict):
            props = {}
        # common id fields
        fid = feature.get("id") or props.get("id") or props.get("event_id") or props.get("eventId")
        return str(fid).strip() if fid is not None else ""

    def _feature_id_for_cache(self, feature: Dict[str, Any], *, source: str) -> str:
        """
        Internal cache key for feature merge store (_QCACHE.features_by_id).
        Prefer upstream id; fallback to geometry signature.
        """
        sid = self._feature_source_id(feature)
        if sid:
            return _stable_id([source, sid])

        geom = feature.get("geometry") or {}
        props = feature.get("properties") or {}
        if not isinstance(props, dict):
            props = {}
        return _stable_id(
            [
                source,
                str(geom.get("type")),
                json.dumps(geom.get("coordinates"))[:240],
                json.dumps(props, sort_keys=True, separators=(",", ":"))[:240],
            ]
        )

    def _status_allows(self, feature: Dict[str, Any]) -> bool:
        props = feature.get("properties") or {}
        status = str(props.get("status") or "").strip().lower() if isinstance(props, dict) else ""
        if not status:
            return True
        # keep only published/reopened; drop closed/withdrawn etc.
        return status in ("published", "reopened")

    async def _qld_fetch_full(self, *, client: httpx.AsyncClient, url: str) -> None:
        data = await self._fetch_json(client, url)
        feats = data.get("features") or []
        by_id: Dict[str, Dict[str, Any]] = {}
        for f in feats:
            if not isinstance(f, dict):
                continue
            if not self._status_allows(f):
                continue
            fid = self._feature_id_for_cache(f, source="qldtraffic")
            by_id[fid] = f
        _QCACHE.features_by_id = by_id
        _QCACHE.full_at = time.time()

    async def _qld_fetch_delta_merge(self, *, client: httpx.AsyncClient, url: str) -> None:
        data = await self._fetch_json(client, url)
        feats = data.get("features") or []
        for f in feats:
            if not isinstance(f, dict):
                continue
            fid = self._feature_id_for_cache(f, source="qldtraffic")
            if not self._status_allows(f):
                _QCACHE.features_by_id.pop(fid, None)
            else:
                _QCACHE.features_by_id[fid] = f
        _QCACHE.delta_at = time.time()

    def _feature_to_event(self, feature: Dict[str, Any], *, feed: str) -> Optional[TrafficEvent]:
        if not isinstance(feature, dict):
            return None

        props = feature.get("properties") or {}
        if not isinstance(props, dict):
            props = {}

        # Optional pruning: drop events ended too long ago
        include_past_hours = int(getattr(settings, "traffic_include_past_hours", 0) or 0)
        if _event_is_too_old(props, include_past_hours):
            return None

        geom = feature.get("geometry") or None
        bb = _bbox_from_geom(geom) if isinstance(geom, dict) else None

        headline = str(
            props.get("headline")
            or props.get("title")
            or props.get("event_type")
            or props.get("type")
            or props.get("description")
            or f"{feed} event"
        ).strip()

        desc = str(props.get("description") or props.get("information") or props.get("advice") or "").strip()
        url2 = props.get("url") or props.get("link") or None
        last_updated = props.get("last_updated") or props.get("lastUpdated") or props.get("updated") or None

        typ, sev = _classify(headline, desc)

        # Stable event identity:
        # Prefer upstream id (do NOT include bbox/headline which can change).
        sid = self._feature_source_id(feature)
        if sid:
            ev_id = _stable_id(["qldtraffic", feed, sid])
        else:
            # fallback: geometry signature (best-effort)
            ev_id = _stable_id(
                [
                    "qldtraffic",
                    feed,
                    headline[:160],
                    json.dumps(geom, sort_keys=True, separators=(",", ":"))[:600] if isinstance(geom, dict) else "",
                ]
            )

        return TrafficEvent(
            id=ev_id,
            source="qldtraffic",
            feed=feed,
            type=typ,  # type: ignore
            severity=sev,  # type: ignore
            headline=headline or f"{feed} event",
            description=(desc or None),
            url=(str(url2) if url2 else None),
            last_updated=(str(last_updated) if last_updated else None),
            geometry=(geom if isinstance(geom, dict) else None),
            bbox=bb,
            raw=props,
        )

    # ──────────────────────────────────────────────────────────
    # Feed fallback
    # ──────────────────────────────────────────────────────────

    async def _poll_geojson_feeds(
        self,
        *,
        client: httpx.AsyncClient,
        bbox: BBox4,
        feeds_geojson: List[Tuple[str, str]],
        warnings: List[str],
    ) -> List[TrafficEvent]:
        items: List[TrafficEvent] = []

        for feed_name, url in feeds_geojson:
            try:
                r = await client.get(url, headers={"User-Agent": "roam/traffic"})
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                warnings.append(f"traffic:{feed_name} fetch failed: {e}")
                continue

            feats = data.get("features") or []
            for f in feats:
                if not isinstance(f, dict):
                    continue
                ev = self._feature_to_event(f, feed=feed_name)
                if not ev:
                    continue
                if ev.bbox and not _bbox_intersects(ev.bbox, bbox):
                    continue
                items.append(ev)

        return items

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    async def poll(
        self,
        *,
        bbox: BBox4,
        cache_seconds: int | None = None,
        timeout_s: float | None = None,
    ) -> TrafficOverlay:
        algo_version = settings.traffic_algo_version
        max_age = int(cache_seconds or settings.overlays_cache_seconds)
        timeout = float(timeout_s or settings.overlays_timeout_s)

        events_url, delta_url, api_key = self._qld_events_urls()
        feeds_geojson = self._feeds_geojson()

        # Stable pack identity (mode chosen is reflected in traffic_key).
        # NOTE: We still allow fallback-on-failure at runtime; the cache key
        # stays stable for the chosen preference, which is what you want for bundles.
        prefer_v2 = True if events_url else False

        traffic_key = _stable_key(
            "traffic",
            {
                "bbox": bbox.model_dump(),
                "algo_version": algo_version,
                "mode": "qld_v2" if prefer_v2 else "geojson_feeds",
                "feeds": (["events", "delta"] if prefer_v2 else [n for (n, _) in feeds_geojson]),
            },
        )

        # SQLite cache hit (respect TTL via created_at)
        cached = get_traffic_pack(self.conn, traffic_key)
        if cached:
            try:
                pack = TrafficOverlay.model_validate(cached)
                if _is_fresh(pack.created_at, max_age_s=max_age):
                    return pack
            except Exception:
                pass

        warnings: List[str] = []
        items: List[TrafficEvent] = []

        transport = httpx.AsyncHTTPTransport(retries=1)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, transport=transport) as client:
            v2_ok = False

            # Preferred: official events (+ optional delta merge)
            if events_url:
                try:
                    url_full = events_url
                    if api_key:
                        url_full = _append_query_params(url_full, {"apikey": api_key})

                    full_refresh_s = int(getattr(settings, "qldtraffic_full_refresh_seconds", 900) or 900)
                    ttl_s = int(getattr(settings, "qldtraffic_cache_seconds", 60) or 60)

                    # Ensure ttl_s is at least 1 and at most something sane
                    ttl_s = max(1, ttl_s)
                    full_refresh_s = max(1, full_refresh_s)

                    if _QCACHE.is_full_stale(full_refresh_s):
                        await self._qld_fetch_full(client=client, url=url_full)
                    else:
                        # Delta only when beyond ttl_s
                        if delta_url and not _QCACHE.can_use_cached(ttl_s):
                            url_delta = delta_url
                            if api_key:
                                url_delta = _append_query_params(url_delta, {"apikey": api_key})
                            await self._qld_fetch_delta_merge(client=client, url=url_delta)

                    for f in _QCACHE.features_by_id.values():
                        if not isinstance(f, dict):
                            continue
                        ev = self._feature_to_event(f, feed="events")
                        if not ev:
                            continue
                        if ev.bbox and not _bbox_intersects(ev.bbox, bbox):
                            continue
                        items.append(ev)

                    v2_ok = True
                except Exception as e:
                    warnings.append(f"traffic:qld_v2 failed: {e}")

            # Fallback: per-feed GeoJSON FeatureCollections if v2 unavailable or failed
            if (not v2_ok) and feeds_geojson:
                try:
                    items.extend(
                        await self._poll_geojson_feeds(
                            client=client,
                            bbox=bbox,
                            feeds_geojson=feeds_geojson,
                            warnings=warnings,
                        )
                    )
                except Exception as e:
                    warnings.append(f"traffic:feeds failed: {e}")

            # If neither is configured, return disabled pack
            if (not events_url) and (not feeds_geojson):
                pack = TrafficOverlay(
                    traffic_key=traffic_key,
                    bbox=bbox,
                    provider="disabled",
                    algo_version=algo_version,
                    created_at=utc_now_iso(),
                    items=[],
                    warnings=["No QLD Traffic sources configured (no v2 URL, no feed URLs)."],
                )
                put_traffic_pack(
                    self.conn,
                    traffic_key=traffic_key,
                    created_at=pack.created_at,
                    algo_version=algo_version,
                    pack=pack.model_dump(),
                )
                return pack

        # Dedup by id (stable IDs are critical here)
        dedup: Dict[str, TrafficEvent] = {}
        for it in items:
            dedup[it.id] = it

        # Provider string should reflect what was *attempted* and what actually produced items
        attempted = []
        if events_url:
            attempted.append("qldtraffic:v2")
        if feeds_geojson:
            attempted.append("qldtraffic:feeds")

        provider = "+".join(attempted) if attempted else "unknown"
        if not dedup:
            provider = provider + ":empty"

        pack = TrafficOverlay(
            traffic_key=traffic_key,
            bbox=bbox,
            provider=provider,
            algo_version=algo_version,
            created_at=utc_now_iso(),
            items=list(dedup.values()),
            warnings=warnings,
        )

        put_traffic_pack(
            self.conn,
            traffic_key=traffic_key,
            created_at=pack.created_at,
            algo_version=algo_version,
            pack=pack.model_dump(),
        )
        return pack
