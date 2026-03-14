# app/core/supabase_admin.py
#
# Singleton Supabase admin client (service-role key).
# Used by stripe/webhook and trips routes to write entitlements and trip counts.

from __future__ import annotations

from functools import lru_cache

from supabase import create_client, Client

from app.core.settings import settings


@lru_cache(maxsize=1)
def get_supabase_admin() -> Client:
    url = settings.supa_url
    key = settings.supa_service_role_key
    if not url or not key:
        raise RuntimeError("SUPA_URL and SUPA_SERVICE_ROLE_KEY must be set")
    return create_client(url, key)
