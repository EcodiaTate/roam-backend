from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from app.core.contracts import (
    PlacesRequest,
    PlacesPack,
    PlaceCategory,
    CorridorPlacesRequest,
    PlacesSuggestRequest,
    PlacesSuggestResponse,
)
from app.core.errors import bad_request, not_found
from app.services.places import Places
from app.services.corridor import Corridor
from app.services.mapbox_geocoding import MapboxGeocoding

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/places")

# ── Lazy singleton for MapboxGeocoding ──────────────────────────────────
# Created on first use so the app still boots even if the token is missing
# (corridor/suggest endpoints don't need it).
_mapbox: MapboxGeocoding | None = None


def _get_mapbox() -> MapboxGeocoding:
    global _mapbox
    if _mapbox is None:
        _mapbox = MapboxGeocoding()
    return _mapbox


def get_places_service() -> Places:
    raise RuntimeError("Places must be provided by app dependency override")


def get_corridor_service() -> Corridor:
    raise RuntimeError("Corridor must be provided by app dependency override")


@router.post("/search", response_model=PlacesPack)
def places_search(req: PlacesRequest, places: Places = Depends(get_places_service)) -> PlacesPack:
    """
    Dual-mode search:
      • If `query` is present → Mapbox forward geocoding (autocomplete).
      • Otherwise (bbox / center+radius) → Overpass tile engine (POI discovery).
    """
    # ── Mapbox path: text query autocomplete ────────────────────────────
    if req.query and req.query.strip():
        proximity: tuple[float, float] | None = None
        if req.center:
            proximity = (req.center.lat, req.center.lng)

        bbox_tuple: tuple[float, float, float, float] | None = None
        if req.bbox:
            bbox_tuple = (req.bbox.minLng, req.bbox.minLat, req.bbox.maxLng, req.bbox.maxLat)

        limit = min(req.limit or 10, 10)  # Mapbox caps at 10

        try:
            mapbox = _get_mapbox()
            return mapbox.search(
                query=req.query.strip(),
                proximity=proximity,
                limit=limit,
                bbox=bbox_tuple,
            )
        except RuntimeError as exc:
            logger.error("mapbox_search_failed: %s — falling back to overpass", exc)
            # Fall through to Overpass if Mapbox is down / unconfigured

    # ── Overpass path: bbox / center+radius POI tile search ─────────────
    if not req.bbox and not (req.center and req.radius_m) and not req.query:
        bad_request("bad_places_request", "Provide bbox or center+radius_m or query")

    return places.search(req)


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
