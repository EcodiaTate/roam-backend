# app/core/geo_registry.py
"""
Australian state/territory detection from bounding boxes.

Used by traffic + hazards services to determine which state feeds to query.
Bounding boxes are intentionally generous (overlapping at borders) —
it's better to query both NSW and QLD for a border route than miss one.
"""
from __future__ import annotations

from typing import List, Tuple

from app.core.contracts import BBox4


# Approximate state bounding boxes: (minLng, minLat, maxLng, maxLat)
# Overlap at borders is intentional — we'd rather fetch both states.
_STATE_BOUNDS: dict[str, Tuple[float, float, float, float]] = {
    "qld": (137.5, -29.5, 154.5, -9.5),
    "nsw": (140.5, -37.6, 154.0, -27.5),
    "vic": (140.5, -39.3, 150.5, -33.5),
    "sa":  (128.5, -38.2, 141.5, -25.5),
    "wa":  (112.5, -35.2, 129.5, -13.5),
    "nt":  (128.5, -26.5, 138.5, -10.5),
    "tas": (143.5, -43.8, 149.0, -39.3),
    "act": (148.5, -36.0, 149.5, -35.0),
}


def _bbox_overlaps(a: Tuple[float, float, float, float], b: BBox4) -> bool:
    """Check if bbox `a` (minLng,minLat,maxLng,maxLat) overlaps with BBox4 `b`."""
    return not (
        a[2] < b.minLng or a[0] > b.maxLng or
        a[3] < b.minLat or a[1] > b.maxLat
    )


def states_for_bbox(bbox: BBox4) -> List[str]:
    """
    Return sorted list of Australian state/territory codes whose bounds
    overlap the given bbox.

    >>> states_for_bbox(BBox4(minLng=150.0, minLat=-34.0, maxLng=154.0, maxLat=-27.0))
    ['nsw', 'qld']
    """
    return sorted(
        code for code, bounds in _STATE_BOUNDS.items()
        if _bbox_overlaps(bounds, bbox)
    )


def bbox_covers_australia(bbox: BBox4) -> bool:
    """True if the bbox spans most of Australia (national-scale query)."""
    lng_span = bbox.maxLng - bbox.minLng
    lat_span = bbox.maxLat - bbox.minLat
    return lng_span > 15 and lat_span > 10


def state_label(code: str) -> str:
    """Human-readable state name."""
    return {
        "qld": "Queensland",
        "nsw": "New South Wales",
        "vic": "Victoria",
        "sa":  "South Australia",
        "wa":  "Western Australia",
        "nt":  "Northern Territory",
        "tas": "Tasmania",
        "act": "Australian Capital Territory",
    }.get(code, code.upper())