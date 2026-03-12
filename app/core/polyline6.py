from __future__ import annotations

from typing import List, Tuple


def _encode_value(v: int) -> str:
    v = ~(v << 1) if v < 0 else (v << 1)
    chunks = []
    while v >= 0x20:
        chunks.append(chr((0x20 | (v & 0x1F)) + 63))
        v >>= 5
    chunks.append(chr(v + 63))
    return "".join(chunks)


def encode_polyline6(coords: List[Tuple[float, float]]) -> str:
    """
    Encode [(lat, lng), ...] into Google polyline with 1e6 precision (Polyline6).
    """
    last_lat = 0
    last_lng = 0
    out = []
    for lat, lng in coords:
        ilat = int(round(lat * 1_000_000))
        ilng = int(round(lng * 1_000_000))
        dlat = ilat - last_lat
        dlng = ilng - last_lng
        out.append(_encode_value(dlat))
        out.append(_encode_value(dlng))
        last_lat = ilat
        last_lng = ilng
    return "".join(out)


def _decode_value(s: str, idx: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = ord(s[idx]) - 63
        idx += 1
        result |= (b & 0x1F) << shift
        shift += 5
        if b < 0x20:
            break
    d = ~(result >> 1) if (result & 1) else (result >> 1)
    return d, idx


def decode_polyline6(poly: str) -> List[Tuple[float, float]]:
    """
    Decode Polyline6 into [(lat, lng), ...]
    """
    idx = 0
    lat = 0
    lng = 0
    coords: List[Tuple[float, float]] = []
    n = len(poly)
    while idx < n:
        dlat, idx = _decode_value(poly, idx)
        dlng, idx = _decode_value(poly, idx)
        lat += dlat
        lng += dlng
        coords.append((lat / 1_000_000, lng / 1_000_000))
    return coords
