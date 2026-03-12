from __future__ import annotations

from typing import Any, Iterable

import httpx

from app.core.contracts import BBox4, PlaceCategory, PlaceItem
from app.core.settings import settings


def _norm_categories(categories: list[PlaceCategory] | None) -> list[str]:
    if not categories:
        return []
    out = [str(c).strip() for c in categories if c]
    out = [c for c in out if c]
    out.sort()
    return out


def _chunked(lst: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    if chunk_size <= 0:
        return [lst]
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


class SupaPlacesRepo:
    """
    Minimal Supabase REST repo:
      - upsert POIs by (osm_type, osm_id)
      - query bbox (+ optional category filter)

    Uses service role key (bypasses RLS).
    """

    def __init__(self) -> None:
        if not settings.supa_url or not settings.supa_service_role_key:
            raise RuntimeError("Supabase not configured (SUPA_URL / SUPA_SERVICE_ROLE_KEY)")
        self.base = settings.supa_url.rstrip("/")
        self.key = settings.supa_service_role_key

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }

    def upsert_items(self, items: Iterable[PlaceItem], *, source: str = "overpass") -> int:
        rows: list[dict[str, Any]] = []
        for it in items:
            extra = dict(it.extra or {})
            osm_type = str(extra.get("osm_type") or "")
            osm_id = extra.get("osm_id")

            if not osm_type or osm_id is None:
                continue

            try:
                osm_id_int = int(osm_id)
            except Exception:
                continue

            rows.append(
                {
                    "osm_type": osm_type,
                    "osm_id": osm_id_int,
                    "lat": float(it.lat),
                    "lng": float(it.lng),
                    "name": str(it.name) if it.name else None,
                    "category": str(it.category) if it.category else None,
                    "tags": extra,
                    "source": source,
                }
            )

        if not rows:
            return 0

        url = f"{self.base}/rest/v1/roam_places_items?on_conflict=osm_type,osm_id"
        headers = self._headers()
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

        # Chunking prevents oversized payload / timeouts on big corridor pulls.
        chunk_size = int(getattr(settings, "supa_places_upsert_chunk", 500))
        chunks = _chunked(rows, chunk_size)

        wrote = 0
        with httpx.Client(timeout=30.0) as client:
            for idx, chunk in enumerate(chunks):
                try:
                    resp = client.post(url, headers=headers, json=chunk)
                    resp.raise_for_status()
                    wrote += len(chunk)
                except httpx.HTTPStatusError as e:
                    # Print body (truncated) so you can see PostgREST complaint.
                    body = ""
                    try:
                        body = (e.response.text or "")[:800]
                    except Exception:
                        body = ""
                    raise RuntimeError(
                        f"supa_upsert_failed status={e.response.status_code} chunk={idx+1}/{len(chunks)} body={body}"
                    ) from e
                except Exception as e:
                    raise RuntimeError(f"supa_upsert_failed chunk={idx+1}/{len(chunks)} err={repr(e)}") from e

        return wrote

    def query_bbox(
        self,
        *,
        bbox: BBox4,
        categories: list[PlaceCategory] | None,
        limit: int,
    ) -> list[PlaceItem]:
        cats = _norm_categories(categories)
        limit = max(1, int(limit))

        params: list[tuple[str, str]] = [
            ("select", "osm_type,osm_id,lat,lng,name,category,tags"),
            ("lat", f"gte.{bbox.minLat}"),
            ("lat", f"lte.{bbox.maxLat}"),
            ("lng", f"gte.{bbox.minLng}"),
            ("lng", f"lte.{bbox.maxLng}"),
            ("limit", str(limit)),
        ]

        if cats:
            joined = ",".join(cats)
            params.append(("category", f"in.({joined})"))

        url = f"{self.base}/rest/v1/roam_places_items"
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url, headers=self._headers(), params=params)
            resp.raise_for_status()
            rows = resp.json()

        out: list[PlaceItem] = []
        for r in rows:
            osm_type = r.get("osm_type")
            osm_id = r.get("osm_id")
            lat = r.get("lat")
            lng = r.get("lng")
            if osm_type is None or osm_id is None or lat is None or lng is None:
                continue
            tags = r.get("tags") or {}
            tags["osm_type"] = osm_type
            tags["osm_id"] = osm_id

            out.append(
                PlaceItem(
                    id=f"osm:{osm_type}:{int(osm_id)}",
                    name=r.get("name") or "",
                    lat=float(lat),
                    lng=float(lng),
                    category=r.get("category") or "town",
                    extra=tags,
                )
            )

        return out
