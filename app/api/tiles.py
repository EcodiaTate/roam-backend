from __future__ import annotations

import re
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.core.settings import settings
from app.core.errors import not_found

router = APIRouter()

# Only allow safe alphanumeric identifiers (no path separators, no dots)
_SAFE_ID = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def _default_data_dir() -> Path:
    here = Path(__file__).resolve()
    app_dir = here.parents[2]  # backend/app
    return app_dir / "data"


def _data_dir() -> Path:
    if getattr(settings, "data_dir", None):
        return Path(settings.data_dir).resolve()
    return _default_data_dir()


def _safe_path(base: Path, *parts: str) -> Path:
    """Resolve a path and verify it stays within base (prevents traversal)."""
    candidate = base.joinpath(*parts).resolve()
    if not str(candidate).startswith(str(base.resolve())):
        raise ValueError("path traversal detected")
    return candidate


@router.get("/tiles/{tile_id}.pmtiles")
def get_pmtiles(tile_id: str):
    if not _SAFE_ID.match(tile_id) or tile_id != "australia":
        not_found("tile_not_found", f"unknown tile_id: {tile_id}")

    base = _data_dir()
    try:
        path = _safe_path(base / "tiles", f"{tile_id}.pmtiles")
    except ValueError:
        not_found("tile_not_found", "invalid tile_id")

    if not path.exists():
        not_found("tile_missing", f"missing pmtiles at {path}")

    return FileResponse(str(path), media_type="application/octet-stream")


@router.get("/styles/{style_id}.style")
def get_style(style_id: str):
    if not _SAFE_ID.match(style_id):
        not_found("style_not_found", "invalid style_id")

    base = _data_dir()
    styles_dir = (base / "styles").resolve()

    extensions = [".style", ".style.json", ".json"]
    for ext in extensions:
        try:
            path = _safe_path(styles_dir, f"{style_id}{ext}")
        except ValueError:
            not_found("style_not_found", "invalid style_id")
        if path.exists():
            return FileResponse(str(path), media_type="application/json")

    not_found(
        "style_missing",
        f"missing style '{style_id}' in {styles_dir}",
    )
