# app/main.py
from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

# Load /backend/.env (main.py is /backend/app/main.py)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

from app.core.settings import settings
from app.core.storage import connect_sqlite, ensure_schema
from app.core.edges_db import create_edges_db
from app.api import api_router

from app.services.corridor import Corridor
from app.services.bundle import Bundle
from app.services.places import Places
from app.services.places_store import PlacesStore

logger = logging.getLogger(__name__)

app = FastAPI(title="Roam Backend", version="1.0.0")

# ── Compression (must be added before CORS) ──
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        # Capacitor / iOS
        "capacitor://localhost",
        "ionic://localhost",

        # Local web dev
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "https://roam.ecodia.au",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)

# ──────────────────────────────────────────────────────────────
# DB connections
# ──────────────────────────────────────────────────────────────

# Cache DB (rw) — SQLite, local to the instance
_cache_conn = connect_sqlite(settings.cache_db_path)
ensure_schema(_cache_conn)

# Edges DB — Postgres+PostGIS in production, SQLite for local dev
# Priority: EDGES_DATABASE_URL → EDGES_DB_PATH → EDGES_DB_DIR fallback
_edges_db = create_edges_db(
    database_url=settings.edges_database_url,
    sqlite_path=settings.edges_db_path if not settings.edges_database_url else None,
)

# Canonical POI store (in the cache DB)
_places_store = PlacesStore(_cache_conn)

# ──────────────────────────────────────────────────────────────
# Dependency providers
# ──────────────────────────────────────────────────────────────

def provide_cache_conn():
    return _cache_conn


def provide_corridor_service() -> Corridor:
    return Corridor(
        cache_conn=_cache_conn,
        edges_db=_edges_db,
        algo_version=settings.corridor_algo_version,
    )


def provide_bundle_service() -> Bundle:
    return Bundle(conn=_cache_conn)


def provide_places_service() -> Places:
    return Places(
        cache_conn=_cache_conn,
        algo_version=settings.places_algo_version,
        store=_places_store,
    )


# ──────────────────────────────────────────────────────────────
# Dependency overrides
# ──────────────────────────────────────────────────────────────

from app.api import nav as nav_api
from app.api import bundle as bundle_api
from app.api import places as places_api

# Corridor
app.dependency_overrides[nav_api.get_corridor_service] = provide_corridor_service
app.dependency_overrides[bundle_api.get_corridor_service] = provide_corridor_service

# Bundle
app.dependency_overrides[bundle_api.get_bundle_service] = provide_bundle_service

# Cache conn
app.dependency_overrides[nav_api.get_cache_conn] = provide_cache_conn
app.dependency_overrides[bundle_api.get_cache_conn] = provide_cache_conn

# Places
app.dependency_overrides[places_api.get_places_service] = provide_places_service
app.dependency_overrides[places_api.get_corridor_service] = provide_corridor_service
app.dependency_overrides[bundle_api.get_places_service] = provide_places_service

# Routes
app.include_router(api_router)

# ──────────────────────────────────────────────────────────────
# Shutdown
# ──────────────────────────────────────────────────────────────

@app.on_event("shutdown")
def shutdown():
    logger.info("[app] Shutting down — closing connections")
    try:
        _edges_db.close()
    except Exception as e:
        logger.warning(f"[app] Error closing edges DB: {e}")
    try:
        _cache_conn.close()
    except Exception:
        pass