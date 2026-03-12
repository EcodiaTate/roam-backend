#!/usr/bin/env python3
"""
scripts/migrate_edges_to_postgres.py

Bulk-load road network edges from local SQLite → Postgres + PostGIS.

Resume behavior:
  - Detects MAX(edge_id) already in Postgres
  - Reads SQLite rows WHERE edge_id > max_edge_id ORDER BY edge_id
  - Inserts with ON CONFLICT (edge_id) DO NOTHING (extra safety)

Speed-focused features (no data changes):
  - Fast-load session settings (synchronous_commit=off, etc.)
  - Larger batches + fewer commits
  - Inserts raw columns only (NO geom per-row)
  - Computes geom AFTER load in chunks
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from typing import Iterable

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: psycopg2-binary not installed.")
    print("  pip install psycopg2-binary")
    sys.exit(1)


DEFAULT_BATCH_SIZE = 200_000
DEFAULT_PAGE_SIZE = 20_000
DEFAULT_GEOM_CHUNK_SIZE = 500_000

SQLITE_COLS = [
    "edge_id",
    "from_id",
    "to_id",
    "from_lat",
    "from_lng",
    "to_lat",
    "to_lng",
    "dist_m",
    "cost_s",
    "toll",
    "ferry",
    "unsealed",
    "highway",
    "name",
    "way_id",  # mapped → osm_way_id in Postgres
]

COL_REMAP = {"way_id": "osm_way_id", "edge_id": "edge_id"}

# NOTE: geom intentionally NOT inserted here (computed after load)
PG_INSERT_SQL = """
INSERT INTO edges (
    edge_id,
    from_id, to_id,
    from_lat, from_lng, to_lat, to_lng,
    dist_m, cost_s,
    toll, ferry, unsealed,
    highway, name, osm_way_id
) VALUES %s
ON CONFLICT (edge_id) DO NOTHING
"""

PG_TEMPLATE = (
    "(%(edge_id)s, "
    "%(from_id)s, %(to_id)s, "
    "%(from_lat)s, %(from_lng)s, %(to_lat)s, %(to_lng)s, "
    "%(dist_m)s, %(cost_s)s, "
    "%(toll)s, %(ferry)s, %(unsealed)s, "
    "%(highway)s, %(name)s, %(osm_way_id)s)"
)


def count_sqlite_rows(sqlite_path: str) -> int:
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    n = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    conn.close()
    return int(n)


def detect_sqlite_columns(sqlite_path: str) -> set[str]:
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(edges)").fetchall()}
    conn.close()
    return cols


def count_pg_rows(pg_conn) -> int:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM edges")
        return int(cur.fetchone()[0])


def get_pg_max_edge_id(pg_conn) -> int:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(edge_id), 0) FROM edges")
        return int(cur.fetchone()[0] or 0)


def apply_fastload_settings(pg) -> None:
    with pg.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET client_min_messages = WARNING;")
        cur.execute("SET work_mem = '64MB';")
        cur.execute("SET temp_buffers = '64MB';")
    pg.commit()


def read_sqlite_batches(
    sqlite_path: str,
    available_cols: set[str],
    batch_size: int,
    min_edge_id_exclusive: int,
) -> Iterable[list[dict]]:
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    if "edge_id" not in available_cols:
        print("ERROR: SQLite edges table has no edge_id column; cannot resume by edge_id.")
        sys.exit(1)

    select_cols = [c for c in SQLITE_COLS if c in available_cols]
    if not select_cols:
        print(f"ERROR: No matching columns. Available: {available_cols}")
        sys.exit(1)

    missing = set(SQLITE_COLS) - available_cols
    if missing:
        print(f"WARNING: Missing columns (will be NULL/0): {missing}")

    sql = f"""
    SELECT {', '.join(select_cols)}
    FROM edges
    WHERE edge_id > ?
    ORDER BY edge_id
    """
    cur = conn.execute(sql, (min_edge_id_exclusive,))

    batch: list[dict] = []
    for row in cur:
        d = dict(row)

        if "way_id" in d:
            d["osm_way_id"] = d.pop("way_id")

        # Fill missing columns with defaults
        for col in SQLITE_COLS:
            pg_name = COL_REMAP.get(col, col)
            if pg_name not in d and col not in d:
                if pg_name in ("dist_m", "cost_s", "from_lat", "from_lng", "to_lat", "to_lng"):
                    d[pg_name] = 0.0
                elif pg_name in ("toll", "ferry", "unsealed", "from_id", "to_id", "edge_id"):
                    d[pg_name] = 0
                else:
                    d[pg_name] = None

        # Ensure numeric types
        d["edge_id"] = int(d["edge_id"] or 0)
        d["from_lat"] = float(d["from_lat"] or 0)
        d["from_lng"] = float(d["from_lng"] or 0)
        d["to_lat"] = float(d["to_lat"] or 0)
        d["to_lng"] = float(d["to_lng"] or 0)
        d["dist_m"] = float(d["dist_m"] or 0)
        d["cost_s"] = float(d["cost_s"] or 0)
        d["toll"] = int(d["toll"] or 0)
        d["ferry"] = int(d["ferry"] or 0)
        d["unsealed"] = int(d["unsealed"] or 0)
        d["from_id"] = int(d["from_id"] or 0)
        d["to_id"] = int(d["to_id"] or 0)
        if d.get("osm_way_id") is not None:
            d["osm_way_id"] = int(d["osm_way_id"])

        batch.append(d)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

    conn.close()


def ensure_geom(pg, chunk_size: int) -> None:
    with pg.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM edges WHERE geom IS NULL;")
        remaining = int(cur.fetchone()[0])

    if remaining == 0:
        print("[geom] geom already populated. ✅")
        return

    print(f"[geom] Populating geom for {remaining:,} rows (chunk={chunk_size:,}) ...")

    done = 0
    t0 = time.monotonic()

    while True:
        with pg.cursor() as cur:
            cur.execute(
                """
                WITH todo AS (
                  SELECT ctid
                  FROM edges
                  WHERE geom IS NULL
                  LIMIT %s
                )
                UPDATE edges e
                SET geom = ST_SetSRID(
                  ST_MakeLine(
                    ST_MakePoint(e.from_lng, e.from_lat),
                    ST_MakePoint(e.to_lng, e.to_lat)
                  ),
                  4326
                )
                FROM todo
                WHERE e.ctid = todo.ctid
                """,
                (chunk_size,),
            )
            updated = cur.rowcount

        pg.commit()

        if updated <= 0:
            break

        done += updated
        elapsed = time.monotonic() - t0
        rate = done / elapsed if elapsed > 0 else 0
        print(f"[geom] +{updated:,} (total {done:,}/{remaining:,}) | {rate:.0f} rows/s")

    print("[geom] ✅ geom population complete.")


def migrate(sqlite_path: str, database_url: str, batch_size: int, page_size: int, geom_chunk_size: int):
    available = detect_sqlite_columns(sqlite_path)
    print(f"[migrate] SQLite columns: {sorted(available)}")

    total_src = count_sqlite_rows(sqlite_path)
    print(f"[migrate] Source SQLite: {total_src:,} rows")

    if total_src == 0:
        print("[migrate] Nothing to migrate.")
        return

    pg = psycopg2.connect(database_url)
    pg.autocommit = False

    apply_fastload_settings(pg)

    existing = count_pg_rows(pg)
    max_edge_id = get_pg_max_edge_id(pg)

    print(f"[migrate] Postgres currently has {existing:,} rows; max(edge_id)={max_edge_id:,}")

    if existing >= total_src:
        print("[migrate] Already fully migrated. ✅")
        ensure_geom(pg, geom_chunk_size)
        pg.close()
        return

    # Remaining rows to add (approx, assumes edge_id mostly contiguous)
    remaining_est = max(0, total_src - existing)
    print(f"[migrate] Resuming: SQLite WHERE edge_id > {max_edge_id:,} (≈{remaining_est:,} rows left)")

    inserted = 0
    batch_num = 0
    t0 = time.monotonic()

    for batch in read_sqlite_batches(sqlite_path, available, batch_size, max_edge_id):
        batch_num += 1
        with pg.cursor() as cur:
            try:
                execute_values(
                    cur,
                    PG_INSERT_SQL,
                    batch,
                    template=PG_TEMPLATE,
                    page_size=page_size,
                )
                pg.commit()
            except Exception as e:
                pg.rollback()
                print(f"  batch {batch_num} FAILED: {e}")
                raise

        inserted += len(batch)

        elapsed = time.monotonic() - t0
        rate = inserted / elapsed if elapsed > 0 else 0
        eta_s = (remaining_est - inserted) / rate if rate > 0 and remaining_est > 0 else 0

        print(
            f"  batch {batch_num}: +{len(batch):,} | "
            f"added {inserted:,} | "
            f"{rate:.0f} rows/s | "
            f"ETA {eta_s/60:.1f}min"
        )

    ensure_geom(pg, geom_chunk_size)

    final = count_pg_rows(pg)
    print(f"[migrate] ✅ Done. Postgres final: {final:,} rows")

    pg.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate edges from SQLite to Postgres+PostGIS (resume by edge_id)")
    parser.add_argument("--sqlite", required=True, help="Path to SQLite edges database")
    parser.add_argument("--database-url", required=True, help="Postgres connection string")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--geom-chunk-size", type=int, default=DEFAULT_GEOM_CHUNK_SIZE)
    args = parser.parse_args()

    migrate(
        sqlite_path=args.sqlite,
        database_url=args.database_url,
        batch_size=args.batch_size,
        page_size=args.page_size,
        geom_chunk_size=args.geom_chunk_size,
    )


if __name__ == "__main__":
    main()
