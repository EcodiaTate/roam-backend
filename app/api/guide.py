# app/api/guide.py
from __future__ import annotations

from fastapi import APIRouter
from app.core.contracts import GuideTurnRequest, GuideTurnResponse
from app.core.errors import bad_request
from app.services.guide import GuideService

router = APIRouter(prefix="/guide")

# Shared instance so the underlying httpx client is reused across requests
_guide_svc: GuideService | None = None


def _get_guide_svc() -> GuideService:
    global _guide_svc
    if _guide_svc is None:
        _guide_svc = GuideService()
    return _guide_svc


@router.post("/turn", response_model=GuideTurnResponse)
async def guide_turn(req: GuideTurnRequest) -> GuideTurnResponse:
    svc = _get_guide_svc()
    try:
        return await svc.turn(req)
    except RuntimeError as e:
        bad_request("guide_error", str(e))
        raise  # unreachable - bad_request raises HTTPException
