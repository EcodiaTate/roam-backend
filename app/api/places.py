from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.core.contracts import PlacesRequest, PlacesPack, PlaceCategory, BBox4
from app.core.errors import bad_request, not_found
from app.services.places import Places
from app.services.corridor import Corridor

router = APIRouter(prefix="/places")


def get_places_service() -> Places:
    raise RuntimeError("Places must be provided by app dependency override")


def get_corridor_service() -> Corridor:
    raise RuntimeError("Corridor must be provided by app dependency override")


@router.post("/search", response_model=PlacesPack)
def places_search(req: PlacesRequest, places: Places = Depends(get_places_service)) -> PlacesPack:
    # validation: must have bbox or (center+radius) or query (we allow query-only but it yields empty unless you mapped it)
    if not req.bbox and not (req.center and req.radius_m) and not req.query:
        bad_request("bad_places_request", "Provide bbox or center+radius_m or query")
    return places.search(req)


class CorridorPlacesRequest(BaseModel):
    corridor_key: str
    categories: list[PlaceCategory] = Field(default_factory=list)
    limit: int = 8000


@router.post("/corridor", response_model=PlacesPack)
def places_corridor(
    req: CorridorPlacesRequest,
    corridor: Corridor = Depends(get_corridor_service),
    places: Places = Depends(get_places_service),
) -> PlacesPack:
    cpack = corridor.get(req.corridor_key)
    if not cpack:
        not_found("corridor_missing", f"no corridor pack found for {req.corridor_key}")

    cats = req.categories or [
        "fuel", "toilet", "water", "camp", "town",
        "grocery", "mechanic", "hospital", "pharmacy",
        "cafe", "restaurant", "fast_food", "pub", "bar",
        "hotel", "motel", "hostel",
        "viewpoint", "attraction", "park", "beach",
    ]

    preq = PlacesRequest(
        bbox=cpack.bbox,
        categories=cats,
        limit=int(req.limit or 8000),
    )
    return places.search(preq)


class PlacesSuggestRequest(BaseModel):
    geometry: str  # polyline6
    interval_km: int = 50
    radius_m: int = 15000
    categories: list[PlaceCategory] = Field(default_factory=list)
    limit_per_sample: int = 150


class PlacesSuggestionCluster(BaseModel):
    idx: int
    lat: float
    lng: float
    km_from_start: float
    places: PlacesPack


class PlacesSuggestResponse(BaseModel):
    # v1 response: list of packs keyed by sample point
    clusters: list[PlacesSuggestionCluster]


@router.post("/suggest", response_model=PlacesSuggestResponse)
def places_suggest(
    req: PlacesSuggestRequest,
    places: Places = Depends(get_places_service),
) -> PlacesSuggestResponse:
    cats = req.categories or [
        "fuel", "toilet", "town",
        "cafe", "restaurant", "fast_food",
        "viewpoint", "attraction",
        "camp", "water",
        "hotel", "motel",
    ]
    clusters = places.suggest_along_route(
        polyline6=req.geometry,
        interval_km=int(req.interval_km or 50),
        radius_m=int(req.radius_m or 15000),
        categories=cats,
        limit_per_sample=int(req.limit_per_sample or 150),
    )
    return PlacesSuggestResponse(clusters=clusters)
