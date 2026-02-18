from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.core.contracts import (
    BBox4,
    NavLeg,
    NavManeuver,
    NavPack,
    NavRequest,
    NavRoute,
    NavStep,
)
from app.core.errors import bad_request, service_unavailable
from app.core.keying import route_key_from_request
from app.core.polyline6 import decode_polyline6, encode_polyline6
from app.core.time import utc_now_iso


# ──────────────────────────────────────────────────────────────
# Geometry helpers
# ──────────────────────────────────────────────────────────────

def _bbox_from_coords(coords: List[Tuple[float, float]]) -> BBox4:
    """Compute bounding box from [(lat, lng), ...] pairs."""
    if not coords:
        return BBox4(minLng=0, minLat=0, maxLng=0, maxLat=0)
    lats = [c[0] for c in coords]
    lngs = [c[1] for c in coords]
    return BBox4(minLng=min(lngs), minLat=min(lats), maxLng=max(lngs), maxLat=max(lats))


def _concat_step_geometries(steps: List[NavStep]) -> str:
    """
    Build a single polyline6 from an ordered list of NavSteps.

    Each step's geometry starts where the previous one ended (shared junction
    point).  We decode each, drop the duplicate first point from steps 2+,
    then re-encode the full sequence.
    """
    if not steps:
        return ""

    all_pts: List[Tuple[float, float]] = []
    for i, step in enumerate(steps):
        if not step.geometry:
            continue
        pts = decode_polyline6(step.geometry)
        if i == 0:
            all_pts.extend(pts)
        else:
            # Skip first point — it's the same as the last point of the
            # previous step (the shared junction).
            if pts:
                all_pts.extend(pts[1:])

    return encode_polyline6(all_pts) if all_pts else ""


# ──────────────────────────────────────────────────────────────
# OSRM → NavStep parsing
# ──────────────────────────────────────────────────────────────

# Valid OSRM maneuver types we map 1:1.  Anything unknown falls back to "turn".
_KNOWN_MANEUVER_TYPES = frozenset({
    "turn", "depart", "arrive",
    "merge", "fork", "on ramp", "off ramp",
    "roundabout", "rotary", "exit roundabout",
    "new name", "continue", "end of road",
    "notification",
})

# Valid OSRM modifiers.  Anything unknown is dropped (None).
_KNOWN_MODIFIERS = frozenset({
    "left", "right",
    "slight left", "slight right",
    "sharp left", "sharp right",
    "straight", "uturn",
})


def _parse_maneuver(raw: Dict[str, Any]) -> NavManeuver:
    """Parse an OSRM maneuver dict into a NavManeuver."""
    raw_type = raw.get("type", "turn")
    raw_modifier = raw.get("modifier")
    loc = raw.get("location", [0, 0])  # OSRM gives [lng, lat]

    return NavManeuver(
        type=raw_type if raw_type in _KNOWN_MANEUVER_TYPES else "turn",
        modifier=raw_modifier if raw_modifier in _KNOWN_MODIFIERS else None,
        location=[float(loc[0]), float(loc[1])] if len(loc) >= 2 else [0.0, 0.0],
        bearing_before=int(raw.get("bearing_before", 0)),
        bearing_after=int(raw.get("bearing_after", 0)),
        exit=raw.get("exit"),
    )


def _parse_step(osrm_step: Dict[str, Any]) -> NavStep:
    """Parse a single OSRM step dict into a NavStep."""
    m = osrm_step.get("maneuver", {})
    return NavStep(
        maneuver=_parse_maneuver(m),
        name=osrm_step.get("name", ""),
        ref=osrm_step.get("ref") or None,
        distance_m=float(osrm_step.get("distance", 0)),
        duration_s=float(osrm_step.get("duration", 0)),
        geometry=osrm_step.get("geometry", ""),  # already polyline6
        mode=osrm_step.get("mode", "driving"),
        pronunciation=osrm_step.get("pronunciation") or None,
    )


def _parse_osrm_leg(
    osrm_leg: Dict[str, Any],
    idx: int,
    from_stop_id: Optional[str],
    to_stop_id: Optional[str],
) -> NavLeg:
    """
    Parse an OSRM leg into a NavLeg with full step data.

    The leg geometry is built by concatenating step geometries (which is more
    accurate than the overview geometry for multi-leg routes).
    """
    steps = [_parse_step(s) for s in osrm_leg.get("steps", [])]

    # Build per-leg geometry from step segments
    leg_geometry = _concat_step_geometries(steps)

    return NavLeg(
        idx=idx,
        from_stop_id=from_stop_id,
        to_stop_id=to_stop_id,
        distance_m=int(round(float(osrm_leg.get("distance", 0)))),
        duration_s=int(round(float(osrm_leg.get("duration", 0)))),
        geometry=leg_geometry,
        steps=steps,
    )


# ──────────────────────────────────────────────────────────────
# Routing service
# ──────────────────────────────────────────────────────────────

class Routing:
    def __init__(self, *, osrm_base_url: str, osrm_profile: str, algo_version: str):
        self.osrm_base_url = osrm_base_url.rstrip("/")
        self.osrm_profile = osrm_profile
        self.algo_version = algo_version
        self.client = httpx.Client(timeout=30.0)

    def route(self, req: NavRequest) -> NavPack:
        if len(req.stops) < 2:
            bad_request("bad_nav_request", "stops must contain at least 2 points")

        # OSRM expects lng,lat
        coords = ";".join([f"{s.lng},{s.lat}" for s in req.stops])

        url = f"{self.osrm_base_url}/route/v1/{self.osrm_profile}/{coords}"
        params = {
            "overview": "full",
            "geometries": "polyline6",      # ← native polyline6, no GeoJSON conversion
            "steps": "true",
            "annotations": "distance,duration,speed",
            "alternatives": "false",
        }

        try:
            r = self.client.get(url, params=params)
        except Exception as e:
            service_unavailable("osrm_unreachable", f"OSRM request failed: {e}")

        if r.status_code != 200:
            service_unavailable(
                "osrm_error",
                f"OSRM returned {r.status_code}: {r.text[:500]}",
            )

        data = r.json()
        routes = data.get("routes") or []
        if not routes:
            service_unavailable("osrm_no_routes", "OSRM returned no routes")

        best = routes[0]

        # Overview geometry — already polyline6 from OSRM
        overview_poly6: str = best.get("geometry", "")
        if not overview_poly6:
            service_unavailable("osrm_bad_geometry", "OSRM returned empty geometry")

        # Decode overview for bbox computation
        overview_pts = decode_polyline6(overview_poly6)
        bbox = _bbox_from_coords(overview_pts)

        dist_m = int(round(float(best.get("distance") or 0)))
        dur_s = int(round(float(best.get("duration") or 0)))

        # Parse legs with full step data
        osrm_legs = best.get("legs") or []
        legs_out: List[NavLeg] = []
        for i, osrm_leg in enumerate(osrm_legs):
            from_id = req.stops[i].id if i < len(req.stops) else None
            to_id = req.stops[i + 1].id if i + 1 < len(req.stops) else None
            legs_out.append(_parse_osrm_leg(osrm_leg, i, from_id, to_id))

        # Route key from deterministic request hash
        req_dict: Dict[str, Any] = req.model_dump()
        rkey = route_key_from_request(req_dict, self.algo_version)

        primary = NavRoute(
            route_key=rkey,
            profile=req.profile,
            distance_m=dist_m,
            duration_s=dur_s,
            geometry=overview_poly6,
            bbox=bbox,
            legs=legs_out,
            provider="osrm",
            created_at=utc_now_iso(),
            algo_version=self.algo_version,
        )

        return NavPack(req=req, primary=primary)