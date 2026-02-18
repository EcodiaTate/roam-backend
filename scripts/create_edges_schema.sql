-- scripts/create_edges_schema.sql
--
-- Postgres + PostGIS schema for Roam road-network edges.
-- Column names match the SQLite edges table that corridor.py expects.
--
-- Run once after enabling PostGIS:
--   CREATE EXTENSION IF NOT EXISTS postgis;

-- ── Edges table ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS edges (
    id          BIGSERIAL PRIMARY KEY,

    -- Graph topology
    from_id     BIGINT      NOT NULL,
    to_id       BIGINT      NOT NULL,

    -- Node coordinates
    from_lat    DOUBLE PRECISION NOT NULL,
    from_lng    DOUBLE PRECISION NOT NULL,
    to_lat      DOUBLE PRECISION NOT NULL,
    to_lng      DOUBLE PRECISION NOT NULL,

    -- Edge costs
    dist_m      DOUBLE PRECISION NOT NULL DEFAULT 0,
    cost_s      DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Edge flags (0 or 1)
    toll        SMALLINT    NOT NULL DEFAULT 0,
    ferry       SMALLINT    NOT NULL DEFAULT 0,
    unsealed    SMALLINT    NOT NULL DEFAULT 0,

    -- Road metadata
    highway     TEXT,
    name        TEXT,
    osm_way_id  BIGINT,

    -- PostGIS geometry for spatial indexing
    geom        GEOMETRY(LINESTRING, 4326)
);

-- Spatial GIST index — this is what makes corridor bbox queries fast
CREATE INDEX IF NOT EXISTS idx_edges_geom
    ON edges USING GIST (geom);

-- Graph lookups
CREATE INDEX IF NOT EXISTS idx_edges_from_id
    ON edges (from_id);

CREATE INDEX IF NOT EXISTS idx_edges_to_id
    ON edges (to_id);

-- OSM way ID for dedup/updates
CREATE INDEX IF NOT EXISTS idx_edges_osm_way_id
    ON edges (osm_way_id);

-- ── Corridor packs cache ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS corridor_packs_cache (
    corridor_key    TEXT PRIMARY KEY,
    created_at      TEXT NOT NULL,
    algo_version    TEXT NOT NULL,
    pack            JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_corridor_cache_algo
    ON corridor_packs_cache (algo_version);