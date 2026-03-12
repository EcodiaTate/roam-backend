from __future__ import annotations

from fastapi import HTTPException


def bad_request(code: str, message: str):
    raise HTTPException(status_code=400, detail={"code": code, "message": message})


def not_found(code: str, message: str):
    raise HTTPException(status_code=404, detail={"code": code, "message": message})


def service_unavailable(code: str, message: str):
    raise HTTPException(status_code=503, detail={"code": code, "message": message})
