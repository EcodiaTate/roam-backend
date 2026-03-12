from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from typing import Optional

import orjson

from app.core.contracts import (
    OfflineBundleManifest,
    NavPack,
    CorridorGraphPack,
    PlacesPack,
    TrafficOverlay,
    HazardOverlay,
)
from app.core.errors import not_found
from app.core.storage import (
    put_manifest,
    get_manifest,
    get_nav_pack,
    get_corridor_pack,
    get_places_pack,
    get_traffic_pack,
    get_hazards_pack,
    get_nav_pack_bytes,
    get_corridor_pack_bytes,
    get_places_pack_bytes,
    get_traffic_pack_bytes,
    get_hazards_pack_bytes,
)
from app.core.time import utc_now_iso


@dataclass(frozen=True)
class BundleZipResult:
    plan_id: str
    zip_bytes: bytes
    bytes_zip: int
    bytes_manifest: int
    bytes_navpack: int
    bytes_corridor: int
    bytes_places: int
    bytes_traffic: int
    bytes_hazards: int


class Bundle:
    def __init__(self, *, conn):
        self.conn = conn

    def build_manifest(
        self,
        *,
        plan_id: str,
        route_key: str,
        styles: list[str],
        navpack_ready: bool,
        corridor_key: str | None,
        corridor_ready: bool,
        places_key: str | None,
        places_ready: bool,
        traffic_key: str | None,
        traffic_ready: bool,
        hazards_key: str | None,
        hazards_ready: bool,
    ) -> OfflineBundleManifest:
        bytes_nav = get_nav_pack_bytes(self.conn, route_key) if navpack_ready else 0
        bytes_corr = get_corridor_pack_bytes(self.conn, corridor_key) if (corridor_ready and corridor_key) else 0
        bytes_places = get_places_pack_bytes(self.conn, places_key) if (places_ready and places_key) else 0
        bytes_traffic = get_traffic_pack_bytes(self.conn, traffic_key) if (traffic_ready and traffic_key) else 0
        bytes_hazards = get_hazards_pack_bytes(self.conn, hazards_key) if (hazards_ready and hazards_key) else 0
        bytes_total = int(bytes_nav + bytes_corr + bytes_places + bytes_traffic + bytes_hazards)

        m = OfflineBundleManifest(
            plan_id=plan_id,
            route_key=route_key,
            styles=styles,
            navpack_status="ready" if navpack_ready else "missing",
            corridor_status="ready" if corridor_ready else "missing",
            places_status="ready" if places_ready else "missing",
            traffic_status="ready" if traffic_ready else "missing",
            hazards_status="ready" if hazards_ready else "missing",
            corridor_key=corridor_key,
            places_key=places_key,
            traffic_key=traffic_key,
            hazards_key=hazards_key,
            bytes_total=bytes_total,
            created_at=utc_now_iso(),
        )

        put_manifest(
            self.conn,
            plan_id=plan_id,
            route_key=route_key,
            created_at=m.created_at,
            manifest=m.model_dump(),
        )
        return m

    def build_zip(self, *, plan_id: str) -> BundleZipResult:
        manifest_row = get_manifest(self.conn, plan_id)
        if not manifest_row:
            not_found("bundle_missing", f"no manifest for plan_id {plan_id}")
        manifest = OfflineBundleManifest.model_validate(manifest_row)

        nav_row = get_nav_pack(self.conn, manifest.route_key)
        if not nav_row:
            not_found("navpack_missing", f"no navpack cached for route_key {manifest.route_key}")
        navpack = NavPack.model_validate(nav_row)

        if not manifest.corridor_key:
            not_found("corridor_missing", "manifest has no corridor_key")
        corr_row = get_corridor_pack(self.conn, manifest.corridor_key)
        if not corr_row:
            not_found("corridor_missing", f"no corridor cached for corridor_key {manifest.corridor_key}")
        corridor = CorridorGraphPack.model_validate(corr_row)

        places: Optional[PlacesPack] = None
        if manifest.places_key:
            places_row = get_places_pack(self.conn, manifest.places_key)
            if not places_row:
                not_found("places_missing", f"no places cached for places_key {manifest.places_key}")
            places = PlacesPack.model_validate(places_row)

        traffic: Optional[TrafficOverlay] = None
        if manifest.traffic_key:
            traffic_row = get_traffic_pack(self.conn, manifest.traffic_key)
            if not traffic_row:
                not_found("traffic_missing", f"no traffic cached for traffic_key {manifest.traffic_key}")
            traffic = TrafficOverlay.model_validate(traffic_row)

        hazards: Optional[HazardOverlay] = None
        if manifest.hazards_key:
            hazards_row = get_hazards_pack(self.conn, manifest.hazards_key)
            if not hazards_row:
                not_found("hazards_missing", f"no hazards cached for hazards_key {manifest.hazards_key}")
            hazards = HazardOverlay.model_validate(hazards_row)

        b_manifest = orjson.dumps(manifest.model_dump())
        b_nav = orjson.dumps(navpack.model_dump())
        b_corr = orjson.dumps(corridor.model_dump())
        b_places = orjson.dumps(places.model_dump()) if places else b""
        b_traffic = orjson.dumps(traffic.model_dump()) if traffic else b""
        b_hazards = orjson.dumps(hazards.model_dump()) if hazards else b""

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("manifest.json", b_manifest)
            z.writestr("navpack.json", b_nav)
            z.writestr("corridor.json", b_corr)
            if places:
                z.writestr("places.json", b_places)
            if traffic:
                z.writestr("traffic.json", b_traffic)
            if hazards:
                z.writestr("hazards.json", b_hazards)

        zip_bytes = buf.getvalue()

        return BundleZipResult(
            plan_id=plan_id,
            zip_bytes=zip_bytes,
            bytes_zip=len(zip_bytes),
            bytes_manifest=len(b_manifest),
            bytes_navpack=len(b_nav),
            bytes_corridor=len(b_corr),
            bytes_places=(len(b_places) if places else 0),
            bytes_traffic=(len(b_traffic) if traffic else 0),
            bytes_hazards=(len(b_hazards) if hazards else 0),
        )
