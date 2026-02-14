from __future__ import annotations

from typing import Any, Dict, List, Tuple

import httpx

from app.core.contracts import BBox4, NavLeg, NavPack, NavRequest, NavRoute
from app.core.errors import bad_request, service_unavailable
from app.core.keying import route_key_from_request
from app.core.polyline6 import encode_polyline6
from app.core.time import utc_now_iso


def _bbox_from_coords(coords: List[Tuple[float, float]]) -> BBox4:
    if not coords:
        return BBox4(minLng=0, minLat=0, maxLng=0, maxLat=0)
    lats = [c[0] for c in coords]
    lngs = [c[1] for c in coords]
    return BBox4(minLng=min(lngs), minLat=min(lats), maxLng=max(lngs), maxLat=max(lats))


class Routing:
    def __init__(self, *, osrm_base_url: str, osrm_profile: str, algo_version: str):
        self.osrm_base_url = osrm_base_url.rstrip("/")
        self.osrm_profile = osrm_profile
        self.algo_version = algo_version
        self.client = httpx.Client(timeout=30.0)

    def route(self, req: NavRequest) -> NavPack:
        if len(req.stops) < 2:
            bad_request("bad_nav_request", "stops must contain at least 2 points")

        # OSRM expects lon,lat; we keep lat,lng in our contract.
        coords = ";".join([f"{s.lng},{s.lat}" for s in req.stops])

        url = f"{self.osrm_base_url}/route/v1/{self.osrm_profile}/{coords}"
        params = {
            "overview": "full",
            "geometries": "geojson",
            "steps": "true",
            "alternatives": "false",
            "annotations": "false",
        }

        try:
            r = self.client.get(url, params=params)
        except Exception as e:
            service_unavailable("osrm_unreachable", f"OSRM request failed: {e}")

        if r.status_code != 200:
            service_unavailable("osrm_error", f"OSRM returned {r.status_code}: {r.text[:500]}")

        data = r.json()
        routes = data.get("routes") or []
        if not routes:
            service_unavailable("osrm_no_routes", "OSRM returned no routes")

        best = routes[0]
        geom = best.get("geometry")
        if not geom or geom.get("type") != "LineString":
            service_unavailable("osrm_bad_geometry", "OSRM returned unexpected geometry format")

        # OSRM geojson geometry coords are [lon, lat]
        pts_latlng: List[Tuple[float, float]] = [(float(y), float(x)) for x, y in geom["coordinates"]]
        poly6 = encode_polyline6(pts_latlng)
        bbox = _bbox_from_coords(pts_latlng)

        dist_m = int(round(float(best.get("distance") or 0)))
        dur_s = int(round(float(best.get("duration") or 0)))

        # Legs: OSRM returns legs by waypoint-to-waypoint
        legs_out: List[NavLeg] = []
        legs = best.get("legs") or []
        for i, leg in enumerate(legs):
            ldist = int(round(float(leg.get("distance") or 0)))
            ldur = int(round(float(leg.get("duration") or 0)))
            # We approximate leg geometry as the whole polyline for v1
            # (we can split by OSRM step geometries later if desired)
            legs_out.append(
                NavLeg(
                    idx=i,
                    from_stop_id=req.stops[i].id if i < len(req.stops) else None,
                    to_stop_id=req.stops[i + 1].id if i + 1 < len(req.stops) else None,
                    distance_m=ldist,
                    duration_s=ldur,
                    geometry=poly6,
                )
            )

        req_dict: Dict[str, Any] = req.model_dump()
        rkey = route_key_from_request(req_dict, self.algo_version)

        primary = NavRoute(
            route_key=rkey,
            profile=req.profile,
            distance_m=dist_m,
            duration_s=dur_s,
            geometry=poly6,
            bbox=bbox,
            legs=legs_out,
            provider="osrm",
            created_at=utc_now_iso(),
            algo_version=self.algo_version,
        )

        return NavPack(req=req, primary=primary)
