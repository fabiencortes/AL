import sqlite3
import os

DB_PATH = "airportslines.db"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE planning_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    DATE TEXT,
    HEURE TEXT,
    CH TEXT,

    NOM TEXT,
    ADRESSE TEXT,
    CP TEXT,
    LOCALITE TEXT,

    DESIGNATION TEXT,
    GO TEXT,

    GROUPAGE TEXT,
    PARTAGE TEXT,

    VOL TEXT,
    PAX TEXT,
    PAIEMENT TEXT,
    REMARQUE TEXT
)
""")

conn.commit()
conn.close()

print("✅ DB recréée avec planning_cache")
