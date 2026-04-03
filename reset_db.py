import sqlite3
import os

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "airportslines.db"
)

print("🧨 DB utilisée :", DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 🔥 SUPPRESSION FORCÉE
cur.execute("DROP TABLE IF EXISTS planning_cache")
conn.commit()

print("✅ Table planning_cache SUPPRIMÉE")

# Vérification
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("📋 Tables restantes :", cur.fetchall())

conn.close()
