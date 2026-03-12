from __future__ import annotations

import io

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.contracts import OfflineBundleManifest, PlacesRequest, PlaceCategory
from app.core.errors import bad_request, not_found
from app.core.storage import get_manifest
from app.services.bundle import Bundle
from app.services.corridor import Corridor
from app.services.places import Places
from app.services.traffic import Traffic
from app.services.hazards import Hazards

router = APIRouter(prefix="/bundle")


def get_bundle_service() -> Bundle:
    raise RuntimeError("Bundle must be provided by app dependency override")


def get_corridor_service() -> Corridor:
    raise RuntimeError("Corridor must be provided by app dependency override")


def get_places_service() -> Places:
    raise RuntimeError("Places must be provided by app dependency override")


def get_cache_conn():
    raise RuntimeError("cache conn must be provided by app dependency override")


def get_traffic_service(cache_conn=Depends(get_cache_conn)) -> Traffic:
    return Traffic(conn=cache_conn)


def get_hazards_service(cache_conn=Depends(get_cache_conn)) -> Hazards:
    return Hazards(conn=cache_conn)


class BundleBuildRequest(BaseModel):
    plan_id: str
    route_key: str
    geometry: str  # polyline6
    profile: str = "drive"
    buffer_m: int | None = None
    max_edges: int | None = None
    styles: list[str] = []


def _default_bundle_categories() -> list[PlaceCategory]:
    return [
        "fuel", "toilet", "water", "camp", "town",
        "grocery", "mechanic", "hospital", "pharmacy",
        "cafe", "restaurant", "fast_food", "pub", "bar",
        "hotel", "motel", "hostel",
        "viewpoint", "attraction", "park", "beach",
    ]


@router.post("/build", response_model=OfflineBundleManifest)
async def build_bundle(
    req: BundleBuildRequest,
    bundle: Bundle = Depends(get_bundle_service),
    corridor: Corridor = Depends(get_corridor_service),
    places: Places = Depends(get_places_service),
    traffic: Traffic = Depends(get_traffic_service),
    hazards: Hazards = Depends(get_hazards_service),
) -> OfflineBundleManifest:
    if not req.plan_id:
        bad_request("bad_bundle_request", "plan_id required")
    if not req.route_key:
        bad_request("bad_bundle_request", "route_key required")
    if not req.geometry:
        bad_request("bad_bundle_request", "geometry required")

    profile = req.profile or "drive"
    buffer_m = int(req.buffer_m or 15000)
    max_edges = int(req.max_edges or 350000)

    # 1) Ensure corridor exists
    cmeta = corridor.ensure(
        route_key=req.route_key,
        route_polyline6=req.geometry,
        profile=profile,
        buffer_m=buffer_m,
        max_edges=max_edges,
    ).meta

    cpack = corridor.get(cmeta.corridor_key)
    if not cpack:
        not_found("corridor_missing", f"no corridor pack found for {cmeta.corridor_key}")

    cats = _default_bundle_categories()

    # 2) Deterministic places pack for offline
    preq = PlacesRequest(
        bbox=cpack.bbox,
        categories=cats,
        limit=8000,
    )
    ppack = places.search(preq)

    # 3) Overlay packs (cached in sqlite)
    tpack = await traffic.poll(bbox=cpack.bbox)
    hpack = await hazards.poll(bbox=cpack.bbox)

    # 4) Manifest
    return bundle.build_manifest(
        plan_id=req.plan_id,
        route_key=req.route_key,
        styles=req.styles,
        navpack_ready=True,
        corridor_key=cmeta.corridor_key,
        corridor_ready=True,
        places_key=ppack.places_key,
        places_ready=True,
        traffic_key=tpack.traffic_key,
        traffic_ready=True,
        hazards_key=hpack.hazards_key,
        hazards_ready=True,
    )


@router.get("/{plan_id}", response_model=OfflineBundleManifest)
def get_bundle(plan_id: str, cache_conn=Depends(get_cache_conn)) -> OfflineBundleManifest:
    row = get_manifest(cache_conn, plan_id)
    if not row:
        not_found("bundle_missing", f"no manifest for plan_id {plan_id}")
    return OfflineBundleManifest.model_validate(row)


@router.get("/{plan_id}/download")
def download_bundle(
    plan_id: str,
    bundle: Bundle = Depends(get_bundle_service),
) -> StreamingResponse:
    z = bundle.build_zip(plan_id=plan_id)
    return StreamingResponse(
        io.BytesIO(z.zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="roam_bundle_{plan_id}.zip"'},
    )
