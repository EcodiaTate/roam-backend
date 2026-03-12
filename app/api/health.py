from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
def health(response: Response) -> Dict[str, Any]:
    """
    Liveness probe - always 200 while the process is up.
    Fly.io and load balancers use this to decide whether to route traffic.
    """
    return {"ok": True}


@router.get("/ready")
def ready(response: Response) -> Dict[str, Any]:
    """
    Readiness probe - checks that the cache DB is queryable.
    Returns 503 if the DB is not ready so Fly.io stops routing traffic.
    """
    from app.main import _cache_conn_ref  # populated by lifespan startup

    checks: Dict[str, str] = {}
    ok = True

    conn = _cache_conn_ref()
    if conn is None:
        checks["cache_db"] = "not_initialised"
        ok = False
    else:
        try:
            conn.execute("SELECT 1")
            checks["cache_db"] = "ok"
        except Exception as exc:
            logger.warning("readiness check: cache_db error: %s", exc)
            checks["cache_db"] = f"error: {exc}"
            ok = False

    if not ok:
        response.status_code = 503

    return {"ok": ok, "checks": checks}
