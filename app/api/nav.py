from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.core.contracts import (
    BBox4,
    CorridorGraphMeta,
    CorridorGraphPack,
    NavPack,
    NavRequest,
    TrafficOverlay,
    HazardOverlay,
)
from app.core.errors import bad_request, not_found
from app.services.routing import Routing
from app.services.corridor import Corridor
from app.services.traffic import Traffic
from app.services.hazards import Hazards
from app.core.storage import put_nav_pack
from app.core.settings import settings

router = APIRouter(prefix="/nav")


def get_routing_service() -> Routing:
    return Routing(
        osrm_base_url=settings.osrm_base_url,
        osrm_profile=settings.osrm_profile,
        algo_version=settings.algo_version,
    )


def get_corridor_service() -> Corridor:
    raise RuntimeError("Corridor must be provided by app dependency override")


def get_cache_conn():
    raise RuntimeError("cache conn must be provided by app dependency override")


def get_traffic_service(cache_conn=Depends(get_cache_conn)) -> Traffic:
    return Traffic(conn=cache_conn)


def get_hazards_service(cache_conn=Depends(get_cache_conn)) -> Hazards:
    return Hazards(conn=cache_conn)


class CorridorEnsureRequest(BaseModel):
    route_key: str
    geometry: str  # polyline6
    profile: str = "drive"
    buffer_m: int | None = None
    max_edges: int | None = None


@router.post("/route", response_model=NavPack)
def nav_route(
    req: NavRequest,
    svc: Routing = Depends(get_routing_service),
    cache_conn=Depends(get_cache_conn),
) -> NavPack:
    pack = svc.route(req)

    put_nav_pack(
        cache_conn,
        route_key=pack.primary.route_key,
        created_at=pack.primary.created_at,
        algo_version=pack.primary.algo_version,
        pack=pack.model_dump(),
    )
    return pack


@router.post("/corridor/ensure", response_model=CorridorGraphMeta)
def corridor_ensure(
    req: CorridorEnsureRequest,
    corridor: Corridor = Depends(get_corridor_service),
) -> CorridorGraphMeta:
    if not req.route_key:
        bad_request("bad_corridor_request", "route_key is required")
    if not req.geometry:
        bad_request("bad_corridor_request", "geometry (polyline6) is required")

    buffer_m = int(req.buffer_m or settings.corridor_buffer_m_default)
    max_edges = int(req.max_edges or settings.corridor_max_edges_default)

    result = corridor.ensure(
        route_key=req.route_key,
        route_polyline6=req.geometry,
        profile=req.profile or "drive",
        buffer_m=buffer_m,
        max_edges=max_edges,
    )
    return result.meta


@router.get("/corridor/{corridor_key}", response_model=CorridorGraphPack)
def corridor_get(
    corridor_key: str,
    corridor: Corridor = Depends(get_corridor_service),
) -> CorridorGraphPack:
    pack = corridor.get(corridor_key)
    if not pack:
        not_found("corridor_missing", f"no corridor pack found for {corridor_key}")
    return pack


# ──────────────────────────────────────────────────────────────
# Overlays (traffic + hazards)
# ──────────────────────────────────────────────────────────────

class OverlayPollRequest(BaseModel):
    bbox: BBox4
    cache_seconds: int | None = None
    timeout_s: float | None = None


@router.post("/traffic/poll", response_model=TrafficOverlay)
async def traffic_poll(
    req: OverlayPollRequest,
    traffic: Traffic = Depends(get_traffic_service),
) -> TrafficOverlay:
    if not req.bbox:
        bad_request("bad_overlay_request", "bbox required")
    return await traffic.poll(bbox=req.bbox, cache_seconds=req.cache_seconds, timeout_s=req.timeout_s)


class HazardsPollRequest(BaseModel):
    bbox: BBox4
    sources: list[str] = Field(default_factory=list)
    cache_seconds: int | None = None
    timeout_s: float | None = None


@router.post("/hazards/poll", response_model=HazardOverlay)
async def hazards_poll(
    req: HazardsPollRequest,
    hazards: Hazards = Depends(get_hazards_service),
) -> HazardOverlay:
    if not req.bbox:
        bad_request("bad_overlay_request", "bbox required")
    return await hazards.poll(
        bbox=req.bbox,
        sources=(req.sources or None),
        cache_seconds=req.cache_seconds,
        timeout_s=req.timeout_s,
    )
