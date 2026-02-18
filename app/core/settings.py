from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # Paths
    data_dir: str = Field(default="app/data", alias="DATA_DIR")
    cache_db_path: str = Field(default="app/data/roam_cache.db", alias="CACHE_DB_PATH")

    # Edges DB — Postgres+PostGIS (production). Takes priority over edges_db_path.
    edges_database_url: str | None = Field(default=None, alias="EDGES_DATABASE_URL")

    # Edges DB — SQLite fallback (local dev)
    edges_db_path: str = Field(default="app/data/edges_queensland.db", alias="EDGES_DB_PATH")

    # OSRM
    osrm_base_url: str = Field(default="http://127.0.0.1:5000", alias="OSRM_BASE_URL")
    osrm_profile: str = Field(default="driving", alias="OSRM_PROFILE")
    mapbox_token: str = Field(default="", alias="ROAM_MAPBOX_TOKEN")
    mapbox_country: str = Field(default="au", alias="ROAM_MAPBOX_COUNTRY")

    # Versioning
    algo_version: str = Field(default="navpack.v1.osrm.mld", alias="ALGO_VERSION")
    corridor_algo_version: str = Field(default="corridor.v1.edgesqlite", alias="CORRIDOR_ALGO_VERSION")
    places_algo_version: str = Field(default="places.v1.overpass.tiled", alias="PLACES_ALGO_VERSION")

    # Corridor defaults
    corridor_buffer_m_default: int = Field(default=15000, alias="CORRIDOR_BUFFER_M_DEFAULT")
    corridor_max_edges_default: int = Field(default=350000, alias="CORRIDOR_MAX_EDGES_DEFAULT")

    # Places (Overpass)
    overpass_url: str = Field(default="https://overpass-api.de/api/interpreter", alias="OVERPASS_URL")
    overpass_timeout_s: int = Field(default=90, alias="OVERPASS_TIMEOUT_S")
    overpass_throttle_s: float = Field(default=0.2, alias="OVERPASS_THROTTLE_S")
    overpass_retries: int = Field(default=4, alias="OVERPASS_RETRIES")
    overpass_retry_base_s: float = Field(default=0.75, alias="OVERPASS_RETRY_BASE_S")

    # Places engine controls
    places_tile_step_deg: float = Field(default=0.15, alias="PLACES_TILE_STEP_DEG")
    places_max_tiles: int = Field(default=64, alias="PLACES_MAX_TILES")
    places_hard_cap: int = Field(default=12000, alias="PLACES_HARD_CAP")
    places_local_satisfy_ratio: float = Field(default=0.70, alias="PLACES_LOCAL_SATISFY_RATIO")
    places_tile_ttl_s: int = Field(default=60 * 60 * 24 * 14, alias="PLACES_TILE_TTL_S")  # 14d
    places_time_budget_s: float = Field(default=10.0, alias="PLACES_TIME_BUDGET_S")
    places_max_overpass_tiles_per_req: int = Field(default=12, alias="PLACES_MAX_OVERPASS_TILES_PER_REQ")

    # Supabase
    supa_url: str | None = Field(default=None, alias="SUPA_URL")
    supa_service_role_key: str | None = Field(default=None, alias="SUPA_SERVICE_ROLE_KEY")
    supa_bucket: str = Field(default="roam-bundles", alias="SUPA_BUCKET")
    supa_enabled: bool = Field(default=False, alias="SUPA_ENABLED")

    # ──────────────────────────────────────────────────────────────
    # Overlays: Traffic + Hazards — shared config
    # ──────────────────────────────────────────────────────────────

    traffic_algo_version: str = Field(
        default="traffic.v4.multistate",
        alias="TRAFFIC_ALGO_VERSION",
    )
    hazards_algo_version: str = Field(
        default="hazards.v3.multistate.cap",
        alias="HAZARDS_ALGO_VERSION",
    )

    overlays_cache_seconds: int = Field(default=120, alias="OVERLAYS_CACHE_SECONDS")
    overlays_timeout_s: float = Field(default=15.0, alias="OVERLAYS_TIMEOUT_S")

    # ──────────────────────────────────────────────────────────────
    # QLD Traffic (official v2 events + delta merge)
    # ──────────────────────────────────────────────────────────────

    qldtraffic_api_key: str = Field(default="", alias="QLDTRAFFIC_API_KEY")

    qldtraffic_events_url: str = Field(
        default="https://api.qldtraffic.qld.gov.au/v2/events",
        alias="QLDTRAFFIC_EVENTS_URL",
    )
    qldtraffic_events_delta_url: str = Field(
        default="https://api.qldtraffic.qld.gov.au/v2/events/past-one-hour",
        alias="QLDTRAFFIC_EVENTS_DELTA_URL",
    )

    qldtraffic_cache_seconds: int = Field(default=60, alias="QLDTRAFFIC_CACHE_SECONDS")
    qldtraffic_full_refresh_seconds: int = Field(default=900, alias="QLDTRAFFIC_FULL_REFRESH_SECONDS")

    traffic_include_past_hours: int = Field(default=6, alias="NAV_TRAFFIC_INCLUDE_PAST_HOURS")

    # Back-compat (optional QLD GeoJSON feed URLs)
    qldtraffic_incidents_url: str | None = Field(default=None, alias="QLDTRAFFIC_INCIDENTS_URL")
    qldtraffic_roadworks_url: str | None = Field(default=None, alias="QLDTRAFFIC_ROADWORKS_URL")
    qldtraffic_closures_url: str | None = Field(default=None, alias="QLDTRAFFIC_CLOSURES_URL")
    qldtraffic_flooding_url: str | None = Field(default=None, alias="QLDTRAFFIC_FLOODING_URL")

    # ──────────────────────────────────────────────────────────────
    # NSW Traffic — Live Traffic NSW (TfNSW Open Data)
    # GeoJSON feeds at api.transport.nsw.gov.au/v1/live/hazards/{type}
    # Types: incidents, fires, floods, alpine, roadworks, majorevent, planned
    # Auth: Authorization: apikey {key}
    # ──────────────────────────────────────────────────────────────

    nsw_traffic_enabled: bool = Field(default=True, alias="NSW_TRAFFIC_ENABLED")
    nsw_traffic_api_key: str = Field(default="", alias="NSW_TRAFFIC_API_KEY")
    nsw_traffic_base_url: str = Field(
        default="https://api.transport.nsw.gov.au/v1/live/hazards",
        alias="NSW_TRAFFIC_BASE_URL",
    )
    # Which hazard feeds to query (all 7 types available)
    nsw_traffic_feeds: str = Field(
        default="incidents,fires,floods,alpine,roadworks,majorevent,planned",
        alias="NSW_TRAFFIC_FEEDS",
    )

    # ──────────────────────────────────────────────────────────────
    # VIC Traffic — VicRoads Data Exchange
    # JSON API at data-exchange.vicroads.vic.gov.au
    # Auth: KeyID header
    # ──────────────────────────────────────────────────────────────

    vic_traffic_enabled: bool = Field(default=True, alias="VIC_TRAFFIC_ENABLED")
    vic_traffic_api_key: str = Field(default="", alias="VIC_TRAFFIC_API_KEY")
    vic_traffic_unplanned_url: str = Field(
        default="https://data-exchange.vicroads.vic.gov.au/opendata/v2/unplanneddisruptions",
        alias="VIC_TRAFFIC_UNPLANNED_URL",
    )
    vic_traffic_planned_url: str = Field(
        default="https://data-exchange.vicroads.vic.gov.au/opendata/v1/planneddisruptions",
        alias="VIC_TRAFFIC_PLANNED_URL",
    )
    vic_traffic_closures_url: str = Field(
        default="https://data-exchange.vicroads.vic.gov.au/opendata/v1/emergencyroadclosures",
        alias="VIC_TRAFFIC_CLOSURES_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # SA Traffic — Traffic SA + DIT outback road conditions
    # NOTE: data.sa.gov.au GeoJSON feed is 404/dead as of Feb 2026.
    # Disabled by default until a replacement feed is found.
    # ──────────────────────────────────────────────────────────────

    sa_traffic_enabled: bool = Field(default=False, alias="SA_TRAFFIC_ENABLED")
    sa_traffic_events_url: str = Field(
        default="https://data.sa.gov.au/data/dataset/traffic-sa-road-events/resource/road-events.geojson",
        alias="SA_TRAFFIC_EVENTS_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # WA Traffic — Main Roads WA ArcGIS GeoJSON (CC-BY 4.0)
    # Road incidents via ArcGIS FeatureServer query endpoint.
    # No auth required. Returns GeoJSON FeatureCollection.
    # ──────────────────────────────────────────────────────────────

    wa_traffic_enabled: bool = Field(default=True, alias="WA_TRAFFIC_ENABLED")
    wa_traffic_arcgis_url: str = Field(
        default=(
            "https://services2.arcgis.com/cHGEnmsJ165IBJRM/arcgis/rest/services/"
            "WebEoc_RoadIncidents/FeatureServer/1/query"
            "?where=1%3D1&outFields=*&f=geojson"
        ),
        alias="WA_TRAFFIC_ARCGIS_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # NT Traffic — NT Road Report (roadreport.nt.gov.au)
    # JSON array of obstructions with start/end coordinates.
    # No auth required. Also doubles as outback road conditions overlay.
    # ──────────────────────────────────────────────────────────────

    nt_traffic_enabled: bool = Field(default=True, alias="NT_TRAFFIC_ENABLED")
    nt_road_report_url: str = Field(
        default="https://roadreport.nt.gov.au/api/Obstruction/GetAll",
        alias="NT_ROAD_REPORT_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # Hazards feeds — BOM per-state RSS warnings (national coverage)
    # These are XML RSS feeds from the Bureau of Meteorology.
    # No auth required. Updated every few minutes.
    # ──────────────────────────────────────────────────────────────

    hazards_enable_bom_rss: bool = Field(default=True, alias="HAZARDS_ENABLE_BOM_RSS")

    bom_rss_qld_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml",
        alias="BOM_RSS_QLD_URL",
    )
    bom_rss_nsw_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00054.warnings_nsw.xml",
        alias="BOM_RSS_NSW_URL",
    )
    bom_rss_vic_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00059.warnings_vic.xml",
        alias="BOM_RSS_VIC_URL",
    )
    bom_rss_sa_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00057.warnings_sa.xml",
        alias="BOM_RSS_SA_URL",
    )
    bom_rss_wa_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00058.warnings_wa.xml",
        alias="BOM_RSS_WA_URL",
    )
    bom_rss_nt_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00055.warnings_nt.xml",
        alias="BOM_RSS_NT_URL",
    )
    bom_rss_tas_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00060.warnings_tas.xml",
        alias="BOM_RSS_TAS_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # CAP feeds — per-state emergency alerting (CAP-AU format)
    # ──────────────────────────────────────────────────────────────

    # QLD CAP feeds (existing)
    qld_disaster_cap_url: str = Field(
        default="https://publiccontent-qld-alerts.s3.ap-southeast-2.amazonaws.com/content/Feeds/StormFloodCycloneWarnings/StormWarnings_capau.xml",
        alias="QLD_DISASTER_CAP_URL",
    )
    qld_emergency_alerts_url: str = Field(
        default="https://publiccontent-qld-alerts.s3.ap-southeast-2.amazonaws.com/content/Feeds/QLDEmergencyAlerts/QLDEmergencyAlerts.xml",
        alias="QLD_EMERGENCY_ALERTS_URL",
    )

    # NSW emergency feeds
    # NOTE: NSW SES warnings XML confirmed 404/dead — removed.
    nsw_rfs_fires_url: str = Field(
        default="https://www.rfs.nsw.gov.au/feeds/majorIncidents.json",
        alias="NSW_RFS_FIRES_URL",
    )

    # VIC emergency feeds
    vic_emergency_url: str = Field(
        default="https://data.emergency.vic.gov.au/Show?pageId=getIncidentJSON",
        alias="VIC_EMERGENCY_URL",
    )

    # SA emergency feeds
    sa_cfs_url: str = Field(
        default="https://data.eso.sa.gov.au/prod/cfs/criimson/cfs_current_incidents.json",
        alias="SA_CFS_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # WA DFES emergency feeds — api.emergency.wa.gov.au/v1/
    # Confirmed working: incidents + warnings endpoints.
    # No auth required.
    # ──────────────────────────────────────────────────────────────

    wa_dfes_enabled: bool = Field(default=True, alias="WA_DFES_ENABLED")
    wa_dfes_base_url: str = Field(
        default="https://api.emergency.wa.gov.au/v1",
        alias="WA_DFES_BASE_URL",
    )
    wa_dfes_feeds: str = Field(
        default="incidents,warnings",
        alias="WA_DFES_FEEDS",
    )

    # ──────────────────────────────────────────────────────────────
    # National DEA Fire Hotspots — satellite detection (CC-BY 4.0)
    # Geoscience Australia Digital Earth Australia.
    # GeoJSON FeatureCollection of all recent satellite-detected hotspots.
    # Covers ALL Australian states via MODIS, HIMAWARI-9, VIIRS, AQUA.
    # No auth required.
    # ──────────────────────────────────────────────────────────────

    dea_hotspots_enabled: bool = Field(default=True, alias="DEA_HOTSPOTS_ENABLED")
    dea_hotspots_url: str = Field(
        default="https://hotspots.dea.ga.gov.au/data/recent-hotspots.json",
        alias="DEA_HOTSPOTS_URL",
    )
    dea_hotspots_min_confidence: int = Field(
        default=50,
        alias="DEA_HOTSPOTS_MIN_CONFIDENCE",
    )
    dea_hotspots_max_hours: int = Field(
        default=72,
        alias="DEA_HOTSPOTS_MAX_HOURS",
    )

    # ──────────────────────────────────────────────────────────────
    # TAS Hazards — TheList ArcGIS (public, no auth)
    # Emergency Management layer from services.thelist.tas.gov.au.
    # ArcGIS JSON format (NOT GeoJSON — uses {"x": lng, "y": lat}).
    # ──────────────────────────────────────────────────────────────

    tas_hazards_enabled: bool = Field(default=True, alias="TAS_HAZARDS_ENABLED")
    tas_thelist_url: str = Field(
        default=(
            "https://services.thelist.tas.gov.au/arcgis/rest/services/Public/"
            "EmergencyManagementPublic/MapServer/72/query"
            "?where=1%3D1&outFields=*&f=json"
        ),
        alias="TAS_THELIST_URL",
    )

    # ──────────────────────────────────────────────────────────────
    # TAS Direct Alert Feed — TasALERT (pending email permission)
    # Richer data at alert.tas.gov.au but requires emailing
    # info@alert.tas.gov.au for API access.
    # Uncomment when permission is granted.
    # ──────────────────────────────────────────────────────────────

    # tas_alert_direct_enabled: bool = Field(default=False, alias="TAS_ALERT_DIRECT_ENABLED")
    # tas_alert_direct_url: str = Field(
    #     default="https://alert.tas.gov.au/feeds/alerts.json",
    #     alias="TAS_ALERT_DIRECT_URL",
    # )

    # ──────────────────────────────────────────────────────────────
    # Guide (LLM)
    # ──────────────────────────────────────────────────────────────
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")
    openai_base_url: str = Field(default="https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    guide_max_steps: int = Field(default=4, alias="GUIDE_MAX_STEPS")
    guide_timeout_s: float = Field(default=25.0, alias="GUIDE_TIMEOUT_S")


settings = Settings()