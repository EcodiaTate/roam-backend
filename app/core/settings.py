from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # Paths
    data_dir: str = Field(default="app/data", alias="DATA_DIR")
    cache_db_path: str = Field(default="app/data/roam_cache.db", alias="CACHE_DB_PATH")

    # Edges DB (corridor graph source)
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

    # ──────────────────────────────────────────────────────────
    # Overlays: Traffic + Hazards
    # ──────────────────────────────────────────────────────────

    traffic_algo_version: str = Field(default="traffic.v2.qldtraffic.events", alias="TRAFFIC_ALGO_VERSION")
    hazards_algo_version: str = Field(default="hazards.v1.qld.cap", alias="HAZARDS_ALGO_VERSION")

    overlays_cache_seconds: int = Field(default=120, alias="OVERLAYS_CACHE_SECONDS")
    overlays_timeout_s: float = Field(default=15.0, alias="OVERLAYS_TIMEOUT_S")

    # ──────────────────────────────────────────────────────────
    # QLD Traffic (official v2 events + delta merge)
    # ──────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # Hazards feeds
    # ──────────────────────────────────────────────────────────

    hazards_enable_bom_rss: bool = Field(default=True, alias="HAZARDS_ENABLE_BOM_RSS")
    bom_rss_qld_url: str = Field(
        default="https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml",
        alias="BOM_RSS_QLD_URL",
    )

    qld_disaster_cap_url: str = Field(
        default="https://publiccontent-qld-alerts.s3.ap-southeast-2.amazonaws.com/content/Feeds/StormFloodCycloneWarnings/StormWarnings_capau.xml",
        alias="QLD_DISASTER_CAP_URL",
    )
    qld_emergency_alerts_url: str = Field(
        default="https://publiccontent-qld-alerts.s3.ap-southeast-2.amazonaws.com/content/Feeds/QLDEmergencyAlerts/QLDEmergencyAlerts.xml",
        alias="QLD_EMERGENCY_ALERTS_URL",
    )

    # Back-compat (optional)
    qldtraffic_incidents_url: str | None = Field(default=None, alias="QLDTRAFFIC_INCIDENTS_URL")
    qldtraffic_roadworks_url: str | None = Field(default=None, alias="QLDTRAFFIC_ROADWORKS_URL")
    qldtraffic_closures_url: str | None = Field(default=None, alias="QLDTRAFFIC_CLOSURES_URL")
    qldtraffic_flooding_url: str | None = Field(default=None, alias="QLDTRAFFIC_FLOODING_URL")

    # ──────────────────────────────────────────────────────────
    # Guide (LLM)
    # ──────────────────────────────────────────────────────────
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o-mini", alias="OPENAI_MODEL")
    openai_base_url: str = Field(default="https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    guide_max_steps: int = Field(default=4, alias="GUIDE_MAX_STEPS")
    guide_timeout_s: float = Field(default=25.0, alias="GUIDE_TIMEOUT_S")


settings = Settings()
