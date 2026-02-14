from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple

import math
import time

import orjson

from app.core.contracts import BBox4, PlaceCategory, PlaceItem
from app.core.time import utc_now_iso


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS places_items (
  osm_type TEXT NOT NULL,         -- 'node'|'way'|'relation'
  osm_id   INTEGER NOT NULL,
  lat      REAL NOT NULL,
  lng      REAL NOT NULL,
  name     TEXT,
  category TEXT,                  -- inferred primary category
  tags_json BLOB NOT NULL,        -- orjson dump of tags/extra
  first_seen TEXT NOT NULL,
  last_seen  TEXT NOT NULL,
  PRIMARY KEY (osm_type, osm_id)
);

CREATE INDEX IF NOT EXISTS idx_places_items_lat ON places_items(lat);
CREATE INDEX IF NOT EXISTS idx_places_items_lng ON places_items(lng);
CREATE INDEX IF NOT EXISTS idx_places_items_cat ON places_items(category);

CREATE TABLE IF NOT EXISTS places_tile_state (
  tile_key TEXT PRIMARY KEY,
  minLat REAL NOT NULL,
  minLng REAL NOT NULL,
  maxLat REAL NOT NULL,
  maxLng REAL NOT NULL,
  categories_json BLOB NOT NULL,  -- sorted list of categories requested
  item_count INTEGER NOT NULL,
  last_fetched TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_places_tiles_last_fetched ON places_tile_state(last_fetched);
"""


def _now_iso() -> str:
    return utc_now_iso()


def _norm_categories(categories: Sequence[PlaceCategory] | None) -> list[str]:
    if not categories:
        return []
    out = [str(c) for c in categories if c]
    out = [c.strip() for c in out if c.strip()]
    out.sort()
    return out


def _parse_osm_identity(place_id: str, extra: dict[str, Any] | None) -> tuple[str, int]:
    """
    Canonical identity is (osm_type, osm_id).
    Preferred sources:
      1) extra['osm_type'], extra['osm_id']
      2) parse id like "osm:node:123"
    """
    if extra:
        ot = extra.get("osm_type")
        oi = extra.get("osm_id")
        if isinstance(ot, str) and (isinstance(oi, int) or (isinstance(oi, str) and str(oi).isdigit())):
            return ot, int(oi)

    if isinstance(place_id, str) and place_id.startswith("osm:"):
        parts = place_id.split(":")
        if len(parts) == 3 and parts[2].isdigit():
            return parts[1], int(parts[2])

    return "node", 0


def _bbox_for_radius(lat: float, lng: float, radius_m: float) -> BBox4:
    dlat = radius_m / 111_320.0
    cosv = max(0.2, math.cos(math.radians(lat)))
    dlng = radius_m / (111_320.0 * cosv)
    return BBox4(
        minLng=lng - dlng,
        minLat=lat - dlat,
        maxLng=lng + dlng,
        maxLat=lat + dlat,
    )


def _haversine_m(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
    R = 6371000.0
    lat1, lon1 = math.radians(a_lat), math.radians(a_lng)
    lat2, lon2 = math.radians(b_lat), math.radians(b_lng)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    x = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(x))


def _tile_key(step_deg: float, b: BBox4) -> str:
    # Deterministic by bbox corner; we call this only on a generated tile bbox.
    return f"tile:{step_deg}:{b.minLat},{b.minLng},{b.maxLat},{b.maxLng}"


def _iter_tiles(b: BBox4, *, step_deg: float, max_tiles: int) -> list[BBox4]:
    tiles: list[BBox4] = []
    lat = b.minLat
    while lat < b.maxLat:
        lat2 = min(b.maxLat, lat + step_deg)
        lng = b.minLng
        while lng < b.maxLng:
            lng2 = min(b.maxLng, lng + step_deg)
            tiles.append(BBox4(minLng=lng, minLat=lat, maxLng=lng2, maxLat=lat2))
            if len(tiles) >= max_tiles:
                return tiles
            lng = lng2
        lat = lat2
    return tiles


@dataclass(frozen=True)
class TileState:
    tile_key: str
    bbox: BBox4
    categories: list[str]
    item_count: int
    last_fetched: str


class PlacesStore:
    """
    Canonical local POI store (SQLite) that supports:
      - upserting Overpass discoveries
      - fast bbox/radius queries
      - tile freshness tracking (avoid repeated Overpass hits)

    Long-term plan:
      - Keep this as the local cache + bundle builder source of truth
      - Add a Supabase publisher later (write-behind), but keep reads local
    """

    def __init__(self, conn):
        self.conn = conn

    def ensure_schema(self) -> None:
        self.conn.executescript(_SCHEMA_SQL)
        self.conn.commit()

    # ──────────────────────────────────────────────────────────────
    # Upsert
    # ──────────────────────────────────────────────────────────────

    def upsert_items(self, items: Sequence[PlaceItem]) -> int:
        if not items:
            return 0

        now = _now_iso()
        rows: list[tuple] = []

        for it in items:
            extra = dict(it.extra or {})
            osm_type, osm_id = _parse_osm_identity(it.id, extra)
            if osm_id == 0:
                continue

            tags_json = orjson.dumps(extra)
            rows.append(
                (
                    osm_type,
                    osm_id,
                    float(it.lat),
                    float(it.lng),
                    (str(it.name) if it.name else None),
                    (str(it.category) if it.category else None),
                    tags_json,
                    now,  # first_seen for inserts
                    now,  # last_seen
                )
            )

        if not rows:
            return 0

        sql = """
        INSERT INTO places_items (osm_type, osm_id, lat, lng, name, category, tags_json, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(osm_type, osm_id) DO UPDATE SET
          lat=excluded.lat,
          lng=excluded.lng,
          name=COALESCE(excluded.name, places_items.name),
          category=COALESCE(excluded.category, places_items.category),
          tags_json=excluded.tags_json,
          last_seen=excluded.last_seen
        """

        cur = self.conn.cursor()
        cur.executemany(sql, rows)
        self.conn.commit()
        return cur.rowcount if cur.rowcount is not None else len(rows)

    # ──────────────────────────────────────────────────────────────
    # Query
    # ──────────────────────────────────────────────────────────────

    def query_bbox(
        self,
        *,
        bbox: BBox4,
        categories: Sequence[PlaceCategory] | None,
        limit: int,
    ) -> list[PlaceItem]:
        limit = max(1, int(limit))
        cats = _norm_categories(categories)

        params: list[Any] = [bbox.minLat, bbox.maxLat, bbox.minLng, bbox.maxLng]
        where = "lat >= ? AND lat <= ? AND lng >= ? AND lng <= ?"

        if cats:
            placeholders = ",".join("?" for _ in cats)
            where += f" AND category IN ({placeholders})"
            params.extend(cats)

        sql = f"""
        SELECT osm_type, osm_id, lat, lng, name, category, tags_json
        FROM places_items
        WHERE {where}
        LIMIT {limit}
        """

        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        out: list[PlaceItem] = []
        for (osm_type, osm_id, lat, lng, name, category, tags_json) in rows:
            try:
                extra = orjson.loads(tags_json) if tags_json else {}
            except Exception:
                extra = {}

            extra["osm_type"] = osm_type
            extra["osm_id"] = osm_id

            out.append(
                PlaceItem(
                    id=f"osm:{osm_type}:{osm_id}",
                    name=name or "",
                    lat=float(lat),
                    lng=float(lng),
                    category=(category or "town"),
                    extra=extra,
                )
            )

        return out

    def query_radius(
        self,
        *,
        center_lat: float,
        center_lng: float,
        radius_m: float,
        categories: Sequence[PlaceCategory] | None,
        limit: int,
    ) -> list[PlaceItem]:
        # bbox prefilter then haversine filter
        b = _bbox_for_radius(center_lat, center_lng, radius_m)
        pre = self.query_bbox(bbox=b, categories=categories, limit=max(limit * 3, 500))

        out: list[PlaceItem] = []
        for it in pre:
            if _haversine_m(center_lat, center_lng, float(it.lat), float(it.lng)) <= radius_m:
                out.append(it)
                if len(out) >= limit:
                    break
        return out

    # ──────────────────────────────────────────────────────────────
    # Tile freshness
    # ──────────────────────────────────────────────────────────────

    def tile_is_fresh(self, *, tile_key: str, ttl_s: int) -> bool:
        if ttl_s <= 0:
            return False

        cur = self.conn.cursor()
        cur.execute("SELECT last_fetched FROM places_tile_state WHERE tile_key=?", (tile_key,))
        row = cur.fetchone()
        if not row:
            return False

        last_fetched = row[0]
        try:
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(last_fetched.replace("Z", "+00:00"))
            age = time.time() - dt.timestamp()
            return age <= float(ttl_s)
        except Exception:
            return False

    def mark_tile_fetched(
        self,
        *,
        tile_key: str,
        bbox: BBox4,
        categories: Sequence[PlaceCategory] | None,
        item_count: int,
        fetched_at: Optional[str] = None,
    ) -> None:
        cats = _norm_categories(categories)
        cats_json = orjson.dumps(cats)
        ts = fetched_at or _now_iso()

        sql = """
        INSERT INTO places_tile_state (tile_key, minLat, minLng, maxLat, maxLng, categories_json, item_count, last_fetched)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tile_key) DO UPDATE SET
          categories_json=excluded.categories_json,
          item_count=excluded.item_count,
          last_fetched=excluded.last_fetched
        """

        self.conn.execute(
            sql,
            (
                tile_key,
                float(bbox.minLat),
                float(bbox.minLng),
                float(bbox.maxLat),
                float(bbox.maxLng),
                cats_json,
                int(item_count),
                ts,
            ),
        )
        self.conn.commit()

    def tiles_for_bbox(
        self,
        *,
        bbox: BBox4,
        step_deg: float,
        max_tiles: int,
    ) -> list[tuple[str, BBox4]]:
        tiles = _iter_tiles(bbox, step_deg=step_deg, max_tiles=max_tiles)
        return [(_tile_key(step_deg, t), t) for t in tiles]
