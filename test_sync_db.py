# test_sync_db.py
"""
TEST ISOLÉ DE SYNCHRO DB
- PAS de Streamlit
- PAS de Dropbox
- SIMULE sync_planning_from_today()
"""

import sqlite3
import time
from datetime import datetime

DB_PATH = "transfers.db"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fake_sync_planning_from_today():
    """
    Simulation d'une synchro :
    - ouvre DB
    - écrit une ligne
    - commit
    - referme
    """

    log("START fake_sync_planning_from_today")

    conn = sqlite3.connect(DB_PATH, timeout=5)
    cur = conn.cursor()

    # Table test (inoffensive)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT
        )
    """)

    log("DB ouverte")

    # Écriture simulée
    cur.execute(
        "INSERT INTO sync_test (ts) VALUES (?)",
        (datetime.now().isoformat(),)
    )

    log("INSERT OK")

    conn.commit()
    log("COMMIT OK")

    conn.close()
    log("DB fermée")

    log("END fake_sync_planning_from_today")


if __name__ == "__main__":
    for i in range(3):
        fake_sync_planning_from_today()
        time.sleep(1)
