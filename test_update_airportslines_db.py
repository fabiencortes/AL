# test_update_airportslines_db.py
"""
TEST D'ÉCRITURE DANS airportslines.db
- PAS de Streamlit
- PAS de Dropbox
- Juste ouverture + écriture + commit
"""

import sqlite3
from datetime import datetime

DB_PATH = "airportslines.db"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def test_update_db():
    log("START test_update_db")

    # 1️⃣ Connexion
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cur = conn.cursor()
    log("DB ouverte")

    # 2️⃣ Table de test (inoffensive)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_test_airports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            info TEXT,
            created_at TEXT
        )
    """)
    log("Table sync_test_airports OK")

    # 3️⃣ Insertion
    cur.execute(
        "INSERT INTO sync_test_airports (info, created_at) VALUES (?, ?)",
        ("test écriture airportslines.db", datetime.now().isoformat())
    )
    log("INSERT OK")

    # 4️⃣ Commit + fermeture
    conn.commit()
    log("COMMIT OK")

    conn.close()
    log("DB fermée")
    log("END test_update_db")


if __name__ == "__main__":
    test_update_db()

