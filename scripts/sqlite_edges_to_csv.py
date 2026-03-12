import sqlite3
import csv

SQLITE_PATH = "app/data/edges_queensland.db"
CSV_PATH = "edges.csv"
BATCH = 100_000

conn = sqlite3.connect(SQLITE_PATH)
cur = conn.cursor()

cur.execute("""
SELECT
  edge_id,
  from_id, to_id,
  from_lat, from_lng,
  to_lat, to_lng,
  dist_m, cost_s,
  toll, ferry, unsealed,
  highway, name,
  way_id
FROM edges
""")

with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    while True:
        rows = cur.fetchmany(BATCH)
        if not rows:
            break
        writer.writerows(rows)
        print(f"wrote {len(rows)} rows")

conn.close()
print("done")
