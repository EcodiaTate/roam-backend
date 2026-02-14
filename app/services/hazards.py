from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import base64
import hashlib
import json
import math
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import httpx

from app.core.contracts import BBox4, HazardEvent, HazardOverlay
from app.core.settings import settings
from app.core.storage import get_hazards_pack, put_hazards_pack
from app.core.time import utc_now_iso


BBox = Tuple[float, float, float, float]  # minLng,minLat,maxLng,maxLat


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


def _bbox_intersects(a: BBox, b: BBox4) -> bool:
    return not (a[2] < b.minLng or a[0] > b.maxLng or a[3] < b.minLat or a[1] > b.maxLat)


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


def _safe_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        if math.isfinite(f):
            return f
    except Exception:
        return None
    return None


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_first_text(el: ET.Element, name: str) -> Optional[str]:
    for child in el.iter():
        if _localname(child.tag) == name:
            txt = (child.text or "").strip()
            return txt if txt else None
    return None


def _severity_from_cap(sev: Optional[str]) -> str:
    s = (sev or "").strip().lower()
    if s in ("extreme", "severe"):
        return "high"
    if s == "moderate":
        return "medium"
    if s == "minor":
        return "low"
    return "unknown"


def _severity_from_text(title: str, desc: str) -> str:
    hay = f"{title} {desc}".lower()
    if "emergency warning" in hay or "evacuate" in hay or "dangerous" in hay:
        return "high"
    if "warning" in hay:
        return "medium"
    if "watch" in hay or "advice" in hay:
        return "low"
    return "unknown"


def _kind_from_text(title: str, event: Optional[str] = None) -> str:
    t = (event or title or "").lower()
    if "flood" in t:
        return "flood"
    if "cyclone" in t:
        return "cyclone"
    if "storm" in t or "thunder" in t:
        return "storm"
    if "fire" in t or "bushfire" in t:
        return "fire"
    if "wind" in t or "gale" in t:
        return "wind"
    if "heat" in t:
        return "heat"
    if "marine" in t:
        return "marine"
    return "weather_warning"


def _bbox_of_coords(coords: List[Tuple[float, float]]) -> Optional[BBox]:
    if not coords:
        return None
    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return (min(lngs), min(lats), max(lngs), max(lats))


def _geom_bbox(geom: Dict[str, Any]) -> Optional[BBox]:
    def collect_points(g: Dict[str, Any], out: List[Tuple[float, float]]) -> None:
        t = g.get("type")
        coords = g.get("coordinates")

        if t == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
            out.append((float(coords[0]), float(coords[1])))
            return

        if t in ("LineString", "MultiPoint") and isinstance(coords, list):
            for p in coords:
                if isinstance(p, (list, tuple)) and len(p) >= 2:
                    out.append((float(p[0]), float(p[1])))
            return

        if t in ("MultiLineString", "Polygon") and isinstance(coords, list):
            for ring in coords:
                if not isinstance(ring, list):
                    continue
                for p in ring:
                    if isinstance(p, (list, tuple)) and len(p) >= 2:
                        out.append((float(p[0]), float(p[1])))
            return

        if t == "MultiPolygon" and isinstance(coords, list):
            for poly in coords:
                if not isinstance(poly, list):
                    continue
                for ring in poly:
                    if not isinstance(ring, list):
                        continue
                    for p in ring:
                        if isinstance(p, (list, tuple)) and len(p) >= 2:
                            out.append((float(p[0]), float(p[1])))
            return

        if t == "GeometryCollection":
            geoms = g.get("geometries")
            if isinstance(geoms, list):
                for sg in geoms:
                    if isinstance(sg, dict):
                        collect_points(sg, out)

    pts: List[Tuple[float, float]] = []
    try:
        collect_points(geom, pts)
    except Exception:
        return None
    return _bbox_of_coords(pts)


def _parse_cap_polygon(poly_text: str) -> Optional[List[List[float]]]:
    # CAP-AU polygon is "lat,lon lat,lon ..." -> GeoJSON ring [[lon,lat],...]
    pts: List[List[float]] = []
    raw = (poly_text or "").strip()
    if not raw:
        return None

    parts = [p for p in raw.replace("\n", " ").split(" ") if p.strip()]
    for part in parts:
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        lat = _safe_float(a)
        lon = _safe_float(b)
        if lat is None or lon is None:
            continue
        pts.append([float(lon), float(lat)])

    if len(pts) < 3:
        return None
    if pts[0] != pts[-1]:
        pts.append(pts[0])
    return pts


def _cap_info_to_geometry(info: ET.Element) -> Optional[Dict[str, Any]]:
    polygons: List[List[List[float]]] = []
    points: List[List[float]] = []

    for area in info.findall(".//*"):
        if _localname(area.tag) != "area":
            continue

        for poly in area.findall(".//*"):
            if _localname(poly.tag) == "polygon":
                ring = _parse_cap_polygon((poly.text or "").strip())
                if ring:
                    polygons.append(ring)

        for cir in area.findall(".//*"):
            if _localname(cir.tag) == "circle":
                txt = (cir.text or "").strip()
                if not txt:
                    continue
                bits = [b for b in txt.replace(",", " ").split() if b.strip()]
                if len(bits) >= 2:
                    lat = _safe_float(bits[0])
                    lon = _safe_float(bits[1])
                    if lat is not None and lon is not None:
                        points.append([float(lon), float(lat)])

    if polygons:
        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": [polygons[0]]}
        # multi-polygon: each ring becomes its own polygon shell
        return {"type": "MultiPolygon", "coordinates": [[[ring]] for ring in polygons]}

    if points:
        return {"type": "Point", "coordinates": points[0]}

    return None


def _parse_rss(xml_text: str, source: str) -> List[HazardEvent]:
    out: List[HazardEvent] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return out

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip() or None
        desc = (item.findtext("description") or "").strip() or None
        pub = (item.findtext("pubDate") or "").strip() or None

        if not title and not desc:
            continue

        sev = _severity_from_text(title, desc or "")
        kind = _kind_from_text(title)
        hid = _stable_id([source, title[:160], (link or "")[:160], (pub or "")[:80]])

        out.append(
            HazardEvent(
                id=hid,
                source=source,
                kind=kind,       # type: ignore
                severity=sev,    # type: ignore
                title=title or "Warning",
                description=desc,
                url=link,
                issued_at=pub,
                start_at=None,
                end_at=None,
                geometry=None,
                bbox=None,
                raw={},
            )
        )
    return out


def _parse_cap(xml_text: str, source: str) -> List[HazardEvent]:
    out: List[HazardEvent] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return out

    alert_identifier = _find_first_text(root, "identifier") or ""
    alert_sent = _find_first_text(root, "sent") or None

    for info in [el for el in root.iter() if _localname(el.tag) == "info"]:
        event_name = _find_first_text(info, "event") or None
        headline = _find_first_text(info, "headline") or None
        description = _find_first_text(info, "description") or None
        instruction = _find_first_text(info, "instruction") or None
        severity_cap = _find_first_text(info, "severity") or None

        onset = _find_first_text(info, "onset") or None
        effective = _find_first_text(info, "effective") or None
        expires = _find_first_text(info, "expires") or None

        web = None
        for ch in info.iter():
            if _localname(ch.tag) == "web":
                t = (ch.text or "").strip()
                if t:
                    web = t
                    break

        title = (headline or event_name or "Warning").strip()
        desc = description or instruction

        sev = _severity_from_cap(severity_cap)
        kind = _kind_from_text(title, event=event_name)

        geom = _cap_info_to_geometry(info)
        bb = _geom_bbox(geom) if geom else None
        bbox_out = [bb[0], bb[1], bb[2], bb[3]] if bb else None

        hid = _stable_id([source, alert_identifier, title[:160], (effective or onset or "")[:80], (expires or "")[:80]])

        out.append(
            HazardEvent(
                id=hid,
                source=source,
                kind=kind,       # type: ignore
                severity=sev,    # type: ignore
                title=title,
                description=desc,
                url=web,
                issued_at=alert_sent,
                start_at=effective or onset or alert_sent,
                end_at=expires,
                geometry=geom,
                bbox=bbox_out,
                raw={},
            )
        )

    return out


class Hazards:
    def __init__(self, *, conn):
        self.conn = conn

    def _sources(self) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        if settings.hazards_enable_bom_rss and settings.bom_rss_qld_url:
            out.append(("bom_rss_qld", settings.bom_rss_qld_url))
        if settings.qld_disaster_cap_url:
            out.append(("qld_disaster_cap", settings.qld_disaster_cap_url))
        if settings.qld_emergency_alerts_url:
            out.append(("qld_emergency_alerts", settings.qld_emergency_alerts_url))
        return out

    async def poll(
        self,
        *,
        bbox: BBox4,
        sources: Optional[List[str]] = None,
        cache_seconds: int | None = None,
        timeout_s: float | None = None,
    ) -> HazardOverlay:
        all_sources = self._sources()
        if sources:
            want = set(sources)
            use = [(n, u) for (n, u) in all_sources if n in want]
        else:
            use = all_sources

        algo_version = settings.hazards_algo_version
        max_age = int(cache_seconds or settings.overlays_cache_seconds)

        hazards_key = _stable_key(
            "hazards",
            {
                "bbox": bbox.model_dump(),
                "sources": [n for (n, _) in use],
                "algo_version": algo_version,
            },
        )

        cached = get_hazards_pack(self.conn, hazards_key)
        if cached:
            try:
                pack = HazardOverlay.model_validate(cached)
                if _is_fresh(pack.created_at, max_age_s=max_age):
                    return pack
            except Exception:
                pass

        warnings: List[str] = []
        items: List[HazardEvent] = []

        if not use:
            pack = HazardOverlay(
                hazards_key=hazards_key,
                bbox=bbox,
                provider="disabled",
                algo_version=algo_version,
                created_at=utc_now_iso(),
                items=[],
                warnings=["No hazard sources configured/enabled."],
            )
            put_hazards_pack(
                self.conn,
                hazards_key=hazards_key,
                created_at=pack.created_at,
                algo_version=algo_version,
                pack=pack.model_dump(),
            )
            return pack

        timeout = float(timeout_s or settings.overlays_timeout_s)
        transport = httpx.AsyncHTTPTransport(retries=1)

        # Heuristic: allow non-spatial warnings only if bbox is "big enough"
        dx = bbox.maxLng - bbox.minLng
        dy = bbox.maxLat - bbox.minLat
        bbox_diag = (dx * dx + dy * dy) ** 0.5

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, transport=transport) as client:
            for name, url in use:
                try:
                    r = await client.get(url, headers={"User-Agent": "roam/hazards"})
                    r.raise_for_status()
                    text = r.text
                except Exception as e:
                    warnings.append(f"hazards:{name} fetch failed: {e}")
                    continue

                try:
                    if name == "bom_rss_qld":
                        src_events = _parse_rss(text, name)
                    else:
                        src_events = _parse_cap(text, name)
                        # some feeds may be rss-ish; fallback attempt
                        if not src_events and name == "qld_emergency_alerts":
                            src_events = _parse_rss(text, name)
                except Exception as e:
                    warnings.append(f"hazards:{name} parse failed: {e}")
                    continue

                for ev in src_events:
                    if ev.bbox and len(ev.bbox) == 4:
                        bb: BBox = (float(ev.bbox[0]), float(ev.bbox[1]), float(ev.bbox[2]), float(ev.bbox[3]))
                        if _bbox_intersects(bb, bbox):
                            items.append(ev)
                    else:
                        # non-spatial -> only include if bbox is big enough
                        if bbox_diag >= 0.35:
                            items.append(ev)

        dedup: Dict[str, HazardEvent] = {}
        for it in items:
            dedup[it.id] = it

        provider = ";".join([n for (n, _) in use]) if dedup else "empty"

        pack = HazardOverlay(
            hazards_key=hazards_key,
            bbox=bbox,
            provider=provider,
            algo_version=algo_version,
            created_at=utc_now_iso(),
            items=list(dedup.values()),
            warnings=warnings,
        )

        put_hazards_pack(
            self.conn,
            hazards_key=hazards_key,
            created_at=pack.created_at,
            algo_version=algo_version,
            pack=pack.model_dump(),
        )
        return pack
