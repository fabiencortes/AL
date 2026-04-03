import sqlite3

DB_PATH = "airportslines.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("🧱 Structure de planning_cache :\n")

for row in cur.execute("PRAGMA table_info(planning_cache)"):
    # cid, name, type, notnull, dflt_value, pk
    print(row)

conn.close()
