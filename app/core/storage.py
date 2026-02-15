from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import orjson

from app.services.places_store import PlacesStore


# ──────────────────────────────────────────────────────────────
# Connections
# ──────────────────────────────────────────────────────────────

def connect_sqlite(path: str) -> sqlite3.Connection:
    """
    Open a RW SQLite connection with sane pragmas.

    IMPORTANT:
    - SQLite will NOT create parent directories.
    - WAL mode requires the directory to be writable (creates -wal/-shm).
    """
    if path != ":memory:":
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def connect_sqlite_ro(path: str) -> sqlite3.Connection:
    """
    Open a read-only SQLite connection.

    Note:
    - This requires the DB file to already exist.
    - We intentionally do NOT mkdir here.
    """
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.execute("PRAGMA query_only=ON;")
    return conn


# ──────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    # Core cache tables
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS corridor_packs (
            corridor_key TEXT PRIMARY KEY,
            route_key TEXT NOT NULL,
            profile TEXT NOT NULL,
            buffer_m INTEGER NOT NULL,
            max_edges INTEGER NOT NULL,
            algo_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            pack_json BLOB NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS places_packs (
            places_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            algo_version TEXT NOT NULL,
            pack_json BLOB NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nav_packs (
            route_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            algo_version TEXT NOT NULL,
            pack_json BLOB NOT NULL
        );
        """
    )

    # Overlays (traffic + hazards)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic_packs (
            traffic_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            algo_version TEXT NOT NULL,
            pack_json BLOB NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hazard_packs (
            hazards_key TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            algo_version TEXT NOT NULL,
            pack_json BLOB NOT NULL
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manifests (
            plan_id TEXT PRIMARY KEY,
            route_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            manifest_json BLOB NOT NULL
        );
        """
    )

    # Canonical POI store (local-first)
    PlacesStore(conn).ensure_schema()

    conn.commit()


# ──────────────────────────────────────────────────────────────
# Bytes helpers (used by Bundle)
# ──────────────────────────────────────────────────────────────

def _len_or_zero(row) -> int:
    return int(row[0]) if row and row[0] is not None else 0


def get_nav_pack_bytes(conn: sqlite3.Connection, route_key: str) -> int:
    cur = conn.execute("SELECT length(pack_json) FROM nav_packs WHERE route_key=?;", (route_key,))
    return _len_or_zero(cur.fetchone())


def get_corridor_pack_bytes(conn: sqlite3.Connection, corridor_key: str | None) -> int:
    if not corridor_key:
        return 0
    cur = conn.execute("SELECT length(pack_json) FROM corridor_packs WHERE corridor_key=?;", (corridor_key,))
    return _len_or_zero(cur.fetchone())


def get_places_pack_bytes(conn: sqlite3.Connection, places_key: str | None) -> int:
    if not places_key:
        return 0
    cur = conn.execute("SELECT length(pack_json) FROM places_packs WHERE places_key=?;", (places_key,))
    return _len_or_zero(cur.fetchone())


def get_traffic_pack_bytes(conn: sqlite3.Connection, traffic_key: str | None) -> int:
    if not traffic_key:
        return 0
    cur = conn.execute("SELECT length(pack_json) FROM traffic_packs WHERE traffic_key=?;", (traffic_key,))
    return _len_or_zero(cur.fetchone())


def get_hazards_pack_bytes(conn: sqlite3.Connection, hazards_key: str | None) -> int:
    if not hazards_key:
        return 0
    cur = conn.execute("SELECT length(pack_json) FROM hazard_packs WHERE hazards_key=?;", (hazards_key,))
    return _len_or_zero(cur.fetchone())


# ──────────────────────────────────────────────────────────────
# Corridor packs
# ──────────────────────────────────────────────────────────────

def put_corridor_pack(
    conn: sqlite3.Connection,
    *,
    corridor_key: str,
    route_key: str,
    profile: str,
    buffer_m: int,
    max_edges: int,
    algo_version: str,
    created_at: str,
    pack: dict,
) -> int:
    blob = orjson.dumps(pack)
    conn.execute(
        """
        INSERT OR REPLACE INTO corridor_packs
          (corridor_key, route_key, profile, buffer_m, max_edges, algo_version, created_at, pack_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (corridor_key, route_key, profile, int(buffer_m), int(max_edges), algo_version, created_at, blob),
    )
    conn.commit()
    return len(blob)


def get_corridor_pack(conn: sqlite3.Connection, corridor_key: str) -> Optional[dict]:
    cur = conn.execute("SELECT pack_json FROM corridor_packs WHERE corridor_key=?;", (corridor_key,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


# ──────────────────────────────────────────────────────────────
# Places packs
# ──────────────────────────────────────────────────────────────

def put_places_pack(
    conn: sqlite3.Connection,
    *,
    places_key: str,
    created_at: str,
    algo_version: str,
    pack: dict,
) -> int:
    blob = orjson.dumps(pack)
    conn.execute(
        """
        INSERT OR REPLACE INTO places_packs (places_key, created_at, algo_version, pack_json)
        VALUES (?, ?, ?, ?);
        """,
        (places_key, created_at, algo_version, blob),
    )
    conn.commit()
    return len(blob)


def get_places_pack(conn: sqlite3.Connection, places_key: str) -> Optional[dict]:
    cur = conn.execute("SELECT pack_json FROM places_packs WHERE places_key=?;", (places_key,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


# ──────────────────────────────────────────────────────────────
# Nav packs
# ──────────────────────────────────────────────────────────────

def put_nav_pack(
    conn: sqlite3.Connection,
    *,
    route_key: str,
    created_at: str,
    algo_version: str,
    pack: dict,
) -> int:
    blob = orjson.dumps(pack)
    conn.execute(
        """
        INSERT OR REPLACE INTO nav_packs (route_key, created_at, algo_version, pack_json)
        VALUES (?, ?, ?, ?);
        """,
        (route_key, created_at, algo_version, blob),
    )
    conn.commit()
    return len(blob)


def get_nav_pack(conn: sqlite3.Connection, route_key: str) -> Optional[dict]:
    cur = conn.execute("SELECT pack_json FROM nav_packs WHERE route_key=?;", (route_key,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


# ──────────────────────────────────────────────────────────────
# Traffic packs
# ──────────────────────────────────────────────────────────────

def put_traffic_pack(
    conn: sqlite3.Connection,
    *,
    traffic_key: str,
    created_at: str,
    algo_version: str,
    pack: dict,
) -> int:
    blob = orjson.dumps(pack)
    conn.execute(
        """
        INSERT OR REPLACE INTO traffic_packs (traffic_key, created_at, algo_version, pack_json)
        VALUES (?, ?, ?, ?);
        """,
        (traffic_key, created_at, algo_version, blob),
    )
    conn.commit()
    return len(blob)


def get_traffic_pack(conn: sqlite3.Connection, traffic_key: str) -> Optional[dict]:
    cur = conn.execute("SELECT pack_json FROM traffic_packs WHERE traffic_key=?;", (traffic_key,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


# ──────────────────────────────────────────────────────────────
# Hazards packs
# ──────────────────────────────────────────────────────────────

def put_hazards_pack(
    conn: sqlite3.Connection,
    *,
    hazards_key: str,
    created_at: str,
    algo_version: str,
    pack: dict,
) -> int:
    blob = orjson.dumps(pack)
    conn.execute(
        """
        INSERT OR REPLACE INTO hazard_packs (hazards_key, created_at, algo_version, pack_json)
        VALUES (?, ?, ?, ?);
        """,
        (hazards_key, created_at, algo_version, blob),
    )
    conn.commit()
    return len(blob)


def get_hazards_pack(conn: sqlite3.Connection, hazards_key: str) -> Optional[dict]:
    cur = conn.execute("SELECT pack_json FROM hazard_packs WHERE hazards_key=?;", (hazards_key,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


# ──────────────────────────────────────────────────────────────
# Manifests
# ──────────────────────────────────────────────────────────────

def put_manifest(
    conn: sqlite3.Connection,
    *,
    plan_id: str,
    route_key: str,
    created_at: str,
    manifest: dict,
) -> None:
    blob = orjson.dumps(manifest)
    conn.execute(
        """
        INSERT OR REPLACE INTO manifests (plan_id, route_key, created_at, manifest_json)
        VALUES (?, ?, ?, ?);
        """,
        (plan_id, route_key, created_at, blob),
    )
    conn.commit()


def get_manifest(conn: sqlite3.Connection, plan_id: str) -> Optional[dict]:
    cur = conn.execute("SELECT manifest_json FROM manifests WHERE plan_id=?;", (plan_id,))
    row = cur.fetchone()
    if not row:
        return None
    return orjson.loads(row[0])


def get_manifest_meta(conn: sqlite3.Connection, plan_id: str) -> Optional[Tuple[str, str]]:
    cur = conn.execute("SELECT route_key, created_at FROM manifests WHERE plan_id=?;", (plan_id,))
    row = cur.fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1])
