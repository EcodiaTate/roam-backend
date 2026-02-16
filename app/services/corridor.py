from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Tuple

from app.core.contracts import (
    BBox4,
    CorridorGraphMeta,
    CorridorGraphPack,
    CorridorNode,
    CorridorEdge,
)
from app.core.keying import corridor_key
from app.core.polyline6 import decode_polyline6
from app.core.storage import get_corridor_pack, put_corridor_pack
from app.core.time import utc_now_iso


def _bbox_expand(b: BBox4, buffer_m: int) -> BBox4:
    """
    Rough meter -> degree expansion.
    Good enough for corridor extraction (bbox + rtree).
    """
    # 1 deg lat ~ 111,320m
    dlat = buffer_m / 111_320.0

    # 1 deg lon scales by cos(lat); approximate using bbox mid-lat
    import math
    mid_lat = (b.minLat + b.maxLat) / 2.0
    cosv = max(0.2, math.cos(math.radians(mid_lat)))
    dlng = buffer_m / (111_320.0 * cosv)

    return BBox4(
        minLng=b.minLng - dlng,
        minLat=b.minLat - dlat,
        maxLng=b.maxLng + dlng,
        maxLat=b.maxLat + dlat,
    )


def _bbox_from_poly6(poly6: str) -> BBox4:
    pts = decode_polyline6(poly6)
    if not pts:
        return BBox4(minLng=0, minLat=0, maxLng=0, maxLat=0)
    lats = [p[0] for p in pts]
    lngs = [p[1] for p in pts]
    return BBox4(minLng=min(lngs), minLat=min(lats), maxLng=max(lngs), maxLat=max(lats))


@dataclass
class CorridorEnsureResult:
    meta: CorridorGraphMeta
    pack: Optional[CorridorGraphPack] = None


class Corridor:
    """
    Builds a corridor graph pack from edges_queensland.db

    Uses edges_rtree to fetch all edges intersecting the corridor bbox.

    Node IDs:
      - we use edges.from_id / edges.to_id as stable node IDs

    Flags bitmask:
      1 = toll
      2 = ferry
      4 = unsealed
    """


    def __init__(self, *, cache_conn, edges_conn, algo_version: str):
        self.cache_conn = cache_conn
        self.edges_conn = edges_conn
        self.algo_version = algo_version

    def ensure(
        self,
        *,
        route_key: str,
        route_polyline6: str,
        profile: str,
        buffer_m: int,
        max_edges: int,
    ) -> CorridorEnsureResult:
        ckey = corridor_key(route_key, buffer_m, max_edges, profile, self.algo_version)

        existing = get_corridor_pack(self.cache_conn, ckey)
        if existing:
            pack = CorridorGraphPack.model_validate(existing)
            meta = CorridorGraphMeta(
                corridor_key=ckey,
                route_key=route_key,
                profile=profile,
                buffer_m=buffer_m,
                max_edges=max_edges,
                algo_version=self.algo_version,
                created_at=existing.get("created_at") or utc_now_iso(),
                bytes=len(existing.get("nodes", [])) + len(existing.get("edges", [])),
            )
            return CorridorEnsureResult(meta=meta, pack=pack)

        # Build corridor bbox from route geometry
        base_bbox = _bbox_from_poly6(route_polyline6)
        corridor_bbox = _bbox_expand(base_bbox, buffer_m)

        # Query edges via RTree intersection test.
        # RTree columns: min_lng, max_lng, min_lat, max_lat
        min_lng = float(corridor_bbox.minLng)
        max_lng = float(corridor_bbox.maxLng)
        min_lat = float(corridor_bbox.minLat)
        max_lat = float(corridor_bbox.maxLat)

        sql = """
        SELECT
          e.edge_id,
          e.from_id, e.to_id,
          e.from_lng, e.from_lat,
          e.to_lng,   e.to_lat,
          e.dist_m, e.cost_s,
          COALESCE(e.toll,0) AS toll,
          COALESCE(e.ferry,0) AS ferry,
          COALESCE(e.unsealed,0) AS unsealed
        FROM edges_rtree r
        JOIN edges e ON e.edge_id = r.edge_id
        WHERE r.max_lng >= ?
          AND r.min_lng <= ?
          AND r.max_lat >= ?
          AND r.min_lat <= ?
        LIMIT ?;
        """

        cur = self.edges_conn.execute(
            sql,
            (min_lng, max_lng, min_lat, max_lat, int(max_edges)),
        )

        # Build nodes + edges
        node_coords: Dict[int, Tuple[float, float]] = {}
        edges_out: list[CorridorEdge] = []

        count = 0
        for row in cur.fetchall():
            (
                edge_id,
                from_id, to_id,
                from_lng, from_lat,
                to_lng, to_lat,
                dist_m, cost_s,
                toll, ferry, unsealed
            ) = row

            from_id_i = int(from_id)
            to_id_i = int(to_id)

            # Record node coords (lat,lng)
            if from_id_i not in node_coords:
                node_coords[from_id_i] = (float(from_lat), float(from_lng))
            if to_id_i not in node_coords:
                node_coords[to_id_i] = (float(to_lat), float(to_lng))

            flags = 0
            if int(toll) == 1:
                flags |= 1
            if int(ferry) == 1:
                flags |= 2
            if int(unsealed) == 1:
                flags |= 4

            edges_out.append(
                CorridorEdge(
                    a=from_id_i,
                    b=to_id_i,
                    distance_m=int(round(float(dist_m))),
                    duration_s=int(round(float(cost_s))),
                    flags=flags,
                )
            )
            count += 1

        nodes_out = [
            CorridorNode(id=nid, lat=lat, lng=lng)
            for nid, (lat, lng) in node_coords.items()
        ]

        pack = CorridorGraphPack(
            corridor_key=ckey,
            route_key=route_key,
            profile=profile,
            algo_version=self.algo_version,
            bbox=corridor_bbox,
            nodes=nodes_out,
            edges=edges_out,
        )

        created_at = utc_now_iso()
        bytes_written = put_corridor_pack(
            self.cache_conn,
            corridor_key=ckey,
            route_key=route_key,
            profile=profile,
            buffer_m=buffer_m,
            max_edges=max_edges,
            algo_version=self.algo_version,
            created_at=created_at,
            pack=pack.model_dump(),
        )

        meta = CorridorGraphMeta(
            corridor_key=ckey,
            route_key=route_key,
            profile=profile,
            buffer_m=buffer_m,
            max_edges=max_edges,
            algo_version=self.algo_version,
            created_at=created_at,
            bytes=bytes_written,
        )
        return CorridorEnsureResult(meta=meta, pack=pack)

  #  API-facing method name expected by nav.py
    def get(self, corridor_key: str) -> Optional[CorridorGraphPack]:
        return self.get_corridor_pack(corridor_key)
    
    def get_corridor_pack(self, corridor_key_str: str) -> Optional[CorridorGraphPack]:
        row = get_corridor_pack(self.cache_conn, corridor_key_str)
        if not row:
            return None
        return CorridorGraphPack.model_validate(row)
