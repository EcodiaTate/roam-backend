"""
app/core/edges_db.py

Unified interface for querying the road-network edges database.

Two backends:
  - EdgesDBSqlite   — local dev (R-Tree spatial index)
  - EdgesDBPostgres — production on Fly.io (PostGIS GIST index)

Factory function `create_edges_db()` auto-selects based on config.

EdgeRow fields match what corridor.py expects:
  row.from_id, row.to_id, row.from_lat, row.from_lng,
  row.to_lat, row.to_lng, row.dist_m, row.cost_s,
  row.toll, row.ferry, row.unsealed
"""

from __future__ import annotations

import os
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional


# ── Data ─────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class EdgeRow:
    """Single road-network edge returned by a spatial query."""
    id: int
    from_id: int
    to_id: int
    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    dist_m: float
    cost_s: float
    toll: int          # 0 or 1
    ferry: int         # 0 or 1
    unsealed: int      # 0 or 1
    highway: Optional[str] = None
    name: Optional[str] = None
    osm_way_id: Optional[int] = None


# ── Abstract interface ───────────────────────────────────────────────

class EdgesDB(ABC):
    """Read-only spatial query interface for road edges."""

    @abstractmethod
    def query_bbox(
        self,
        min_lng: float,
        max_lng: float,
        min_lat: float,
        max_lat: float,
        max_edges: int = 500_000,
    ) -> List[EdgeRow]:
        ...

    @abstractmethod
    def count(self) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...


# ── SQLite backend (local dev) ───────────────────────────────────────

class EdgesDBSqlite(EdgesDB):
    """
    Queries a local SQLite DB with an R-Tree spatial index.
    Expects tables: `edges` + `edges_rtree` (R-Tree virtual table).
    Falls back to range scan if no R-Tree.
    """

    def __init__(self, db_path: str):
        self._path = db_path
        self._conn = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._has_rtree = self._check_rtree()
        n = self.count()
        print(f"[edges] SQLite opened: {db_path} ({n:,} rows, rtree={self._has_rtree})")

    def _check_rtree(self) -> bool:
        try:
            self._conn.execute("SELECT * FROM edges_rtree LIMIT 1")
            return True
        except sqlite3.OperationalError:
            return False

    def query_bbox(
        self,
        min_lng: float,
        max_lng: float,
        min_lat: float,
        max_lat: float,
        max_edges: int = 500_000,
    ) -> List[EdgeRow]:
        if self._has_rtree:
            sql = """
                SELECT e.rowid AS _rowid, e.*
                FROM edges e
                JOIN edges_rtree r ON e.rowid = r.id
                WHERE r.min_lng <= ? AND r.max_lng >= ?
                  AND r.min_lat <= ? AND r.max_lat >= ?
                LIMIT ?
            """
            params = (max_lng, min_lng, max_lat, min_lat, max_edges)
        else:
            sql = """
                SELECT rowid AS _rowid, *
                FROM edges
                WHERE (from_lng BETWEEN ? AND ? AND from_lat BETWEEN ? AND ?)
                   OR (to_lng   BETWEEN ? AND ? AND to_lat   BETWEEN ? AND ?)
                LIMIT ?
            """
            params = (
                min_lng, max_lng, min_lat, max_lat,
                min_lng, max_lng, min_lat, max_lat,
                max_edges,
            )

        cur = self._conn.execute(sql, params)
        rows = cur.fetchall()
        return [self._row_to_edge(r) for r in rows]

    def count(self) -> int:
        cur = self._conn.execute("SELECT COUNT(*) FROM edges")
        return cur.fetchone()[0]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _row_to_edge(row: sqlite3.Row) -> EdgeRow:
        keys = row.keys()

        def g(col, default=None):
            return row[col] if col in keys else default

        return EdgeRow(
            id=g("_rowid") or g("rowid") or 0,
            from_id=g("from_id", 0),
            to_id=g("to_id", 0),
            from_lat=float(g("from_lat", 0.0)),
            from_lng=float(g("from_lng", 0.0)),
            to_lat=float(g("to_lat", 0.0)),
            to_lng=float(g("to_lng", 0.0)),
            dist_m=float(g("dist_m", 0.0)),
            cost_s=float(g("cost_s", 0.0)),
            toll=int(g("toll", 0) or 0),
            ferry=int(g("ferry", 0) or 0),
            unsealed=int(g("unsealed", 0) or 0),
            highway=g("highway"),
            name=g("name"),
            osm_way_id=g("osm_way_id"),
        )


# ── Postgres + PostGIS backend (production) ──────────────────────────

class EdgesDBPostgres(EdgesDB):
    """
    Queries a Postgres+PostGIS database with a GIST spatial index.
    Uses a connection pool for concurrent requests.
    """

    # Column order must match _tuple_to_edge indices
    _SELECT_COLS = """
        id, from_id, to_id,
        from_lat, from_lng, to_lat, to_lng,
        dist_m, cost_s,
        toll, ferry, unsealed,
        highway, name, osm_way_id
    """

    def __init__(self, database_url: str, min_conn: int = 1, max_conn: int = 5):
        try:
            import psycopg2
            import psycopg2.pool
        except ImportError:
            raise RuntimeError(
                "psycopg2-binary is required for Postgres edges DB. "
                "Install: pip install psycopg2-binary"
            )

        self._database_url = database_url
        print(f"[edges] Connecting to Postgres (pool {min_conn}-{max_conn})...")

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn, max_conn, database_url
        )

        # Verify connection + PostGIS
        conn = self._pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT PostGIS_Version()")
            postgis_ver = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM edges")
            n = cur.fetchone()[0]
            cur.close()
            print(f"[edges] Postgres connected. PostGIS {postgis_ver}")
            print(f"[edges] Edges table has {n:,} rows")
        finally:
            self._pool.putconn(conn)

    def query_bbox(
        self,
        min_lng: float,
        max_lng: float,
        min_lat: float,
        max_lat: float,
        max_edges: int = 500_000,
    ) -> List[EdgeRow]:
        sql = f"""
            SELECT {self._SELECT_COLS}
            FROM edges
            WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            LIMIT %s
        """
        # ST_MakeEnvelope(xmin, ymin, xmax, ymax) = (min_lng, min_lat, max_lng, max_lat)
        params = (min_lng, min_lat, max_lng, max_lat, max_edges)

        conn = self._pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.close()
            return [self._tuple_to_edge(r) for r in rows]
        finally:
            self._pool.putconn(conn)

    def count(self) -> int:
        conn = self._pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM edges")
            n = cur.fetchone()[0]
            cur.close()
            return n
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.closeall()

    @staticmethod
    def _tuple_to_edge(row: tuple) -> EdgeRow:
        return EdgeRow(
            id=row[0],
            from_id=row[1],
            to_id=row[2],
            from_lat=float(row[3] or 0),
            from_lng=float(row[4] or 0),
            to_lat=float(row[5] or 0),
            to_lng=float(row[6] or 0),
            dist_m=float(row[7] or 0),
            cost_s=float(row[8] or 0),
            toll=int(row[9] or 0),
            ferry=int(row[10] or 0),
            unsealed=int(row[11] or 0),
            highway=row[12],
            name=row[13],
            osm_way_id=row[14],
        )


# ── Factory ──────────────────────────────────────────────────────────

def create_edges_db(
    *,
    database_url: str | None = None,
    sqlite_path: str | None = None,
) -> EdgesDB:
    """
    Auto-select edges database backend.

    Priority:
      1. database_url → Postgres+PostGIS
      2. sqlite_path  → local SQLite
      3. Fallback paths for legacy setups
    """
    if database_url:
        print("[edges] Using Postgres backend")
        return EdgesDBPostgres(database_url)

    if sqlite_path and os.path.isfile(sqlite_path):
        print(f"[edges] Using SQLite backend: {sqlite_path}")
        return EdgesDBSqlite(sqlite_path)

    # Legacy fallback paths
    fallback_paths = [
        os.path.join(os.path.dirname(__file__), "..", "data", "edges_queensland.db"),
        "/cache/edges_queensland.db",
        "/tmp/edges_queensland.db",
    ]
    for path in fallback_paths:
        resolved = os.path.abspath(path)
        if os.path.isfile(resolved):
            print(f"[edges] Using SQLite backend (fallback): {resolved}")
            return EdgesDBSqlite(resolved)

    raise FileNotFoundError(
        "[edges] No edges database found. "
        "Set EDGES_DATABASE_URL for Postgres or "
        "EDGES_DB_PATH for local SQLite."
    )