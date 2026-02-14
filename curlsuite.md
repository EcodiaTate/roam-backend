set -euo pipefail

API="http://127.0.0.1:8000"

echo "== 0) Docs reachable =="
curl -sS "$API/docs" >/dev/null
echo "OK"
echo

PLAN_ID="plan_$(date +%Y%m%d_%H%M%S)"
echo "PLAN_ID=$PLAN_ID"
echo

# ------------------------------
# 1) Route
# ------------------------------
echo "=============================="
echo "1) POST /nav/route"
echo "=============================="
RESP=$(curl -sS -X POST "$API/nav/route" \
  -H "Content-Type: application/json" \
  -d '{
    "profile": "drive",
    "prefs": {},
    "stops": [
      { "type": "start", "lat": -27.4705, "lng": 153.0260 },
      { "type": "end",   "lat": -28.0167, "lng": 153.4000 }
    ]
  }')

echo "$RESP" | jq '{route_key:(.primary.route_key), provider:(.primary.provider), algo:(.primary.algo_version), polyline_len:(.primary.geometry|length)}'

ROUTE_KEY=$(echo "$RESP" | jq -r '.primary.route_key')
POLY6=$(echo "$RESP" | jq -r '.primary.geometry')
echo "ROUTE_KEY=$ROUTE_KEY"
echo "POLY6_LEN=${#POLY6}"
echo

# ------------------------------
# 2) Corridor ensure
# ------------------------------
echo "=============================="
echo "2) POST /nav/corridor/ensure"
echo "=============================="
PAYLOAD=$(jq -n \
  --arg route_key "$ROUTE_KEY" \
  --arg geometry "$POLY6" \
  --arg profile "drive" \
  --argjson buffer_m 15000 \
  --argjson max_edges 350000 \
  '{route_key:$route_key, geometry:$geometry, profile:$profile, buffer_m:$buffer_m, max_edges:$max_edges}')

CRES=$(curl -sS -X POST "$API/nav/corridor/ensure" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

echo "$CRES" | jq
CORRIDOR_KEY=$(echo "$CRES" | jq -r '.corridor_key')
echo "CORRIDOR_KEY=$CORRIDOR_KEY"
echo

# ------------------------------
# 3) Corridor get (counts)
# ------------------------------
echo "=============================="
echo "3) GET /nav/corridor/{key}"
echo "=============================="
CPACK=$(curl -sS "$API/nav/corridor/$CORRIDOR_KEY")
echo "$CPACK" | jq '{nodes:(.nodes|length), edges:(.edges|length), bbox:.bbox, route_key:.route_key, algo:.algo_version}'

NODES=$(echo "$CPACK" | jq '.nodes|length')
EDGES=$(echo "$CPACK" | jq '.edges|length')
if [ "$NODES" -le 0 ] || [ "$EDGES" -le 0 ]; then
  echo "ERROR: corridor pack empty"
  exit 1
fi
echo "OK: corridor non-empty"
echo

# ------------------------------
# 4) Places search (run twice to prove caching/collection)
# ------------------------------
echo "=============================="
echo "4) POST /places/search (twice: seed then fast)"
echo "=============================="

BBOX=$(echo "$CPACK" | jq '.bbox')
PLACES_REQ=$(jq -n \
  --argjson bbox "$BBOX" \
  --argjson limit 8000 \
  --argjson categories '["fuel","toilet","water","camp","town","grocery","mechanic","hospital","pharmacy","cafe","restaurant","fast_food","pub","bar","hotel","motel","hostel","viewpoint","attraction","park","beach"]' \
  '{bbox:$bbox, categories:$categories, limit:$limit}')

echo "-- First call (may seed via Overpass + store) --"
P1=$(curl -sS -w "\n__HTTP__:%{http_code}\n__TIME__:%{time_total}\n" \
  -X POST "$API/places/search" -H "Content-Type: application/json" -d "$PLACES_REQ")
HTTP1=$(echo "$P1" | sed -n 's/^__HTTP__://p' | tail -n 1)
T1=$(echo "$P1" | sed -n 's/^__TIME__://p' | tail -n 1)
BODY1=$(echo "$P1" | sed '/^__HTTP__:/,$d')
echo "$BODY1" | jq '{places_key:.places_key, provider:.provider, items:(.items|length), algo:.algo_version}'
echo "HTTP=$HTTP1 time_total=$T1"
echo

echo "-- Second call (should be faster: local-first / cached tiles / pack cache) --"
P2=$(curl -sS -w "\n__HTTP__:%{http_code}\n__TIME__:%{time_total}\n" \
  -X POST "$API/places/search" -H "Content-Type: application/json" -d "$PLACES_REQ")
HTTP2=$(echo "$P2" | sed -n 's/^__HTTP__://p' | tail -n 1)
T2=$(echo "$P2" | sed -n 's/^__TIME__://p' | tail -n 1)
BODY2=$(echo "$P2" | sed '/^__HTTP__:/,$d')
echo "$BODY2" | jq '{places_key:.places_key, provider:.provider, items:(.items|length), algo:.algo_version}'
echo "HTTP=$HTTP2 time_total=$T2"
echo

PKEY=$(echo "$BODY2" | jq -r '.places_key')
PITEMS=$(echo "$BODY2" | jq '.items|length')
if [ "$PITEMS" -le 0 ]; then
  echo "WARN: places returned 0 items. Overpass may be throttling or coverage is low."
else
  echo "OK: places non-empty"
fi
echo

# ------------------------------
# 5) Bundle build
# ------------------------------
echo "=============================="
echo "5) POST /bundle/build"
echo "=============================="
BRES=$(curl -sS -X POST "$API/bundle/build" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --arg plan_id "$PLAN_ID" \
    --arg route_key "$ROUTE_KEY" \
    --arg geometry "$POLY6" \
    --arg profile "drive" \
    --argjson buffer_m 15000 \
    --argjson max_edges 350000 \
    '{plan_id:$plan_id, route_key:$route_key, geometry:$geometry, profile:$profile, buffer_m:$buffer_m, max_edges:$max_edges, styles:[] }')")

echo "$BRES" | jq
echo

# ------------------------------
# 6) Bundle download zip
# ------------------------------
echo "=============================="
echo "6) GET /bundle/{plan_id}/download"
echo "=============================="
OUT="roam_bundle_${PLAN_ID}.zip"
curl -sS -o "$OUT" "$API/bundle/$PLAN_ID/download"
echo "Wrote $OUT"
echo

echo "=============================="
echo "7) Inspect zip contents"
echo "=============================="
unzip -l "$OUT" | head -n 80
echo

echo "=============================="
echo "8) Validate zip payloads"
echo "=============================="
unzip -p "$OUT" manifest.json | jq '{plan_id, route_key, navpack_status, corridor_status, places_status, bytes_total, corridor_key, places_key, created_at}'
unzip -p "$OUT" navpack.json | jq '{route_key:.primary.route_key, profile:.primary.profile, algo:.primary.algo_version}'
unzip -p "$OUT" corridor.json | jq '{corridor_key, route_key, profile, algo_version, nodes:(.nodes|length), edges:(.edges|length)}'
if unzip -p "$OUT" places.json >/dev/null 2>&1; then
  unzip -p "$OUT" places.json | jq '{places_key, provider, algo_version, items:(.items|length)}'
else
  echo "NOTE: places.json not present (places_key missing in manifest)"
fi
echo

echo "✅ ALL CORE ENDPOINTS OK"
