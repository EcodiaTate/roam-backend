from __future__ import annotations

import base64
import hashlib
from typing import Any, Dict

import orjson


def _orjson_dumps(obj: Any) -> bytes:
    return orjson.dumps(
        obj,
        option=orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS,
    )


def sha256_b32(data: bytes) -> str:
    h = hashlib.sha256(data).digest()
    # URL-safe base32-ish: we use base64 urlsafe with no padding for brevity
    return base64.urlsafe_b64encode(h).decode("ascii").rstrip("=")


def normalize_nav_request(req: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonicalize a NavRequest-like dict:
    - ensures numeric lat/lng
    - strips unknown fields
    - orders stops/waypoints as provided (semantic)
    - ensures profile/prefs fields exist
    """
    profile = str(req.get("profile") or "drive")
    prefs = req.get("prefs") or {}
    stops_in = req.get("stops") or []
    if not isinstance(stops_in, list) or len(stops_in) < 2:
        raise ValueError("NavRequest.stops must be an array with at least 2 stops")

    stops = []
    for s in stops_in:
        lat = float(s["lat"])
        lng = float(s["lng"])
        sid = s.get("id")
        stype = s.get("type") or "poi"
        stops.append(
            {
                "id": sid,
                "type": stype,
                "lat": round(lat, 6),
                "lng": round(lng, 6),
                "name": s.get("name"),
            }
        )

    return {
        "profile": profile,
        "prefs": prefs,
        "stops": stops,
        "avoid": req.get("avoid") or [],
        "depart_at": req.get("depart_at"),  # may be None
    }


def route_key_from_request(req: Dict[str, Any], algo_version: str) -> str:
    norm = normalize_nav_request(req)
    payload = {"algo_version": algo_version, "req": norm}
    blob = _orjson_dumps(payload)
    return sha256_b32(blob)


def corridor_key(route_key: str, buffer_m: int, max_edges: int, profile: str, algo_version: str) -> str:
    payload = {
        "algo_version": algo_version,
        "route_key": route_key,
        "buffer_m": int(buffer_m),
        "max_edges": int(max_edges),
        "profile": str(profile),
    }
    blob = _orjson_dumps(payload)
    return sha256_b32(blob)


def places_key(req: Dict[str, Any], algo_version: str) -> str:
    payload = {"algo_version": algo_version, "req": req}
    blob = _orjson_dumps(payload)
    return sha256_b32(blob)
