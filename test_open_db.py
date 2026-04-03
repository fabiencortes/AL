# test_open_db.py
"""
TEST ISOLÉ OUVERTURE / FERMETURE SQLITE
- PAS de Streamlit
- PAS d'écriture
- PAS de synchro
"""

import sqlite3
import time
from datetime import datetime

DB_PATH = "transfers.db"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def test_open_close_db():
    log("START test_open_close_db")

    conn = sqlite3.connect(DB_PATH, timeout=5)
    log("DB ouverte")

    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    log(f"{len(tables)} tables détectées")

    conn.close()
    log("DB fermée")

    log("END test_open_close_db")


if __name__ == "__main__":
    for i in range(5):
        test_open_close_db()
        time.sleep(1)
