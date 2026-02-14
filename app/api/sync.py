from __future__ import annotations

from fastapi import APIRouter

from app.core.contracts import SyncOpsRequest, SyncOpsResponse

router = APIRouter(prefix="/sync")


@router.post("/ops", response_model=SyncOpsResponse)
def sync_ops(req: SyncOpsRequest) -> SyncOpsResponse:
    # v1 placeholder: accept and discard
    return SyncOpsResponse(accepted=len(req.ops))
