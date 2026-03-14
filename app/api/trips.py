# app/api/trips.py
#
# Trip counter endpoints. Migrated from frontend Next.js API routes.
# Uses SUPABASE_SERVICE_ROLE_KEY to write user_trip_counts (the user's
# anon key cannot write this table directly).

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.core.auth import AuthUser, get_current_user
from app.core.supabase_admin import get_supabase_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trips", tags=["trips"])


class MergeRequest(BaseModel):
    local_count: int = 0


# ── POST /trips/increment ────────────────────────────────────────


@router.post("/increment")
async def increment_trip_count(user: AuthUser = Depends(get_current_user)):
    supa = get_supabase_admin()

    result = supa.rpc("increment_trip_count", {"p_user_id": user.id}).execute()

    if hasattr(result, "error") and result.error:
        logger.error("[trips/increment] %s", result.error)
        return JSONResponse({"error": "Failed to increment."}, status_code=500)

    return {"trips_used": result.data}


# ── POST /trips/merge ────────────────────────────────────────────


@router.post("/merge")
async def merge_trip_count(
    body: MergeRequest,
    user: AuthUser = Depends(get_current_user),
):
    supa = get_supabase_admin()

    # Clamp to sane range
    local_count = max(0, min(body.local_count, 100))

    # Get current server count
    existing = (
        supa.table("user_trip_counts")
        .select("trips_used")
        .eq("user_id", user.id)
        .maybe_single()
        .execute()
    )

    server_count = (existing.data or {}).get("trips_used", 0) if existing.data else 0
    merged = max(server_count, local_count)

    if merged > server_count:
        supa.table("user_trip_counts").upsert(
            {"user_id": user.id, "trips_used": merged},
            on_conflict="user_id",
        ).execute()

    return {"trips_used": merged}
