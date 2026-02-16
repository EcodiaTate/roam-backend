from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load /backend/.env (main.py is /backend/app/main.py)
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

from app.core.settings import settings
from app.core.storage import connect_sqlite, connect_sqlite_ro, ensure_schema
from app.api import api_router

from app.services.corridor import Corridor
from app.services.bundle import Bundle
from app.services.places import Places
from app.services.places_store import PlacesStore

app = FastAPI(title="Roam Backend", version="1.0.0")


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
        # If you run the frontend on another port, add it here.
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],  # includes Range
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)

# ──────────────────────────────────────────────────────────────
# DB connections
# ──────────────────────────────────────────────────────────────

# Cache DB (rw)
_cache_conn = connect_sqlite(settings.cache_db_path)
ensure_schema(_cache_conn)

# Edges DB (ro)
_edges_conn = connect_sqlite_ro(settings.edges_db_path)

# Canonical POI store (in the cache DB)
_places_store = PlacesStore(_cache_conn)  # ensure_schema already handled by ensure_schema(_cache_conn)

# ──────────────────────────────────────────────────────────────
# Dependency providers
# ──────────────────────────────────────────────────────────────

def provide_cache_conn():
    return _cache_conn


def provide_corridor_service() -> Corridor:
    return Corridor(
        cache_conn=_cache_conn,
        edges_conn=_edges_conn,
        algo_version=settings.corridor_algo_version,
    )


def provide_bundle_service() -> Bundle:
    # Bundle reads cached packs and assembles zips.
    return Bundle(conn=_cache_conn)


def provide_places_service() -> Places:
    # Local-first places with canonical store + Overpass top-up.
    return Places(
        cache_conn=_cache_conn,
        algo_version=settings.places_algo_version,
        store=_places_store,
    )


# ──────────────────────────────────────────────────────────────
# Dependency overrides (IMPORTANT: import the same modules your routers use)
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

# Places
app.dependency_overrides[places_api.get_places_service] = provide_places_service
app.dependency_overrides[places_api.get_corridor_service] = provide_corridor_service  #  ADD THIS
app.dependency_overrides[bundle_api.get_places_service] = provide_places_service

# Routes
app.include_router(api_router)

