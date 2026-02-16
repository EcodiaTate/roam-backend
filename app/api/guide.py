# app/api/guide.py (or wherever your guide router lives)
from __future__ import annotations

from fastapi import APIRouter
from app.core.errors import bad_request
from app.services.guide import GuideService, GuideTurnRequest, GuideTurnResponse

router = APIRouter(prefix="/guide")


@router.post("/turn", response_model=GuideTurnResponse)
async def guide_turn(req: GuideTurnRequest) -> GuideTurnResponse:
    svc = GuideService()
    try:
        return await svc.turn(req)
    except RuntimeError as e:
        bad_request("guide_error", str(e))