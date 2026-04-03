import sqlite3

DB_PATH = "airportslines.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("📋 Tables dans airportslines.db :\n")

for row in cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
):
    print("-", row[0])

conn.close()
