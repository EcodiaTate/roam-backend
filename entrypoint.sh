#!/bin/sh
set -e

# ─── Copy edges DBs from GCSFuse mount to local disk ─────────────────
# GCSFuse translates every SQLite page read into an HTTP round-trip to GCS.
# Copying to /tmp (tmpfs or local SSD on Cloud Run) gives native disk speed.
#
# This runs once at cold start. Cloud Run instances are ephemeral anyway,
# so /tmp is fine — it's backed by the instance's memory/disk allocation.

EDGES_MOUNT="/mnt/roam-cache"   # GCSFuse mount point (configured in Cloud Run service YAML)
EDGES_LOCAL="/tmp/edges"         # Local fast-access copy

if [ -d "$EDGES_MOUNT" ]; then
  echo "[entrypoint] Copying edges DBs from GCSFuse to local disk..."
  mkdir -p "$EDGES_LOCAL"

  # Copy all .db files (edges_queensland.db, edges_nsw.db, etc.)
  # Skip -wal and -shm files — SQLite will recreate them locally
  for db in "$EDGES_MOUNT"/edges_*.db; do
    if [ -f "$db" ]; then
      basename=$(basename "$db")
      echo "[entrypoint]   Copying $basename..."
      t0=$(date +%s)
      cp "$db" "$EDGES_LOCAL/$basename"
      t1=$(date +%s)
      size=$(du -h "$EDGES_LOCAL/$basename" | cut -f1)
      echo "[entrypoint]   Done: $basename ($size) in $((t1 - t0))s"
    fi
  done

  # Tell the app where to find the local copies
  export EDGES_DB_DIR="$EDGES_LOCAL"
  echo "[entrypoint] EDGES_DB_DIR=$EDGES_DB_DIR"
else
  echo "[entrypoint] No GCSFuse mount at $EDGES_MOUNT, using default paths"
fi

# ─── Start the app ───────────────────────────────────────────────────
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8080}"