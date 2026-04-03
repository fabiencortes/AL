import sqlite3
from datetime import date

DB = "airportslines.db"
today = date.today().strftime("%Y-%m-%d")

conn = sqlite3.connect(DB)
cur = conn.cursor()

print("🧨 Purge réelle à partir d’aujourd’hui…")

cur.execute("""
DELETE FROM planning
WHERE
    date(
        CASE
            WHEN LENGTH(DATE)=10 AND substr(DATE,3,1)='/' THEN
                substr(DATE,7,4)||'-'||substr(DATE,4,2)||'-'||substr(DATE,1,2)
            ELSE DATE
        END
    ) >= date(?)
""", (today,))

print(f"✅ {conn.total_changes} lignes supprimées")

conn.commit()
conn.close()
