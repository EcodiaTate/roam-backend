from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.core.settings import settings
from app.core.errors import not_found

router = APIRouter()


def _default_data_dir() -> Path:
    # This file lives at: backend/app/<something>/...
    # We want:           backend/app/data
    # If this file is at backend/app/<pkg>/routes/assets.py for example,
    # parents[2] should land on backend/app
    here = Path(__file__).resolve()
    app_dir = here.parents[2]  # backend/app
    return app_dir / "data"


def _data_dir() -> Path:
    # If settings.data_dir is set, trust it; otherwise default to backend/app/data
    if getattr(settings, "data_dir", None):
        return Path(settings.data_dir).resolve()
    return _default_data_dir()


@router.get("/tiles/{tile_id}.pmtiles")
def get_pmtiles(tile_id: str):
    if tile_id != "australia":
        not_found("tile_not_found", f"unknown tile_id: {tile_id}")

    base = _data_dir()
    path = (base / "tiles" / f"{tile_id}.pmtiles").resolve()

    if not path.exists():
        not_found("tile_missing", f"missing pmtiles at {path}")

    return FileResponse(str(path), media_type="application/octet-stream")


@router.get("/styles/{style_id}.style")
def get_style(style_id: str):
    base = _data_dir()
    styles_dir = (base / "styles").resolve()

    # Try a few sane filename patterns:
    candidates = [
        styles_dir / f"{style_id}.style",       # current contract
        styles_dir / f"{style_id}.style.json",  # common
        styles_dir / f"{style_id}.json",        # you said they may be json files
    ]

    for path in candidates:
        if path.exists():
            return FileResponse(str(path), media_type="application/json")

    not_found(
        "style_missing",
        f"missing style '{style_id}' in {styles_dir} (tried: {', '.join(p.name for p in candidates)})",
    )
