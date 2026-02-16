from __future__ import annotations

from fastapi import APIRouter

from .health import router as health_router
from .tiles import router as tiles_router
from .nav import router as nav_router
from .places import router as places_router
from .bundle import router as bundle_router
from .sync import router as sync_router
from .guide import router as guide_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(tiles_router)
api_router.include_router(guide_router)
api_router.include_router(nav_router)
api_router.include_router(places_router)
api_router.include_router(bundle_router)
api_router.include_router(sync_router)
