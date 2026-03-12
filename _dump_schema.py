import sqlite3

db = r"D:\.WEB-DEV-PROJECTS\ROAM\BACKEND\DATA\edges_queensland.db"
con = sqlite3.connect(db)
cur = con.cursor()

print("TABLES:")
for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(name)

print("\nSCHEMA:")
for (sql,) in cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY name"):
    print(sql)
    print()
