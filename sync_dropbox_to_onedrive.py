import requests
import os
import time
import hashlib
from datetime import datetime

# =========================
# CONFIG
# =========================
INTERVAL = 30
STABILITY_DELAY = 5

# Dropbox **lien de téléchargement direct**
DROPBOX_LINK = "https://www.dropbox.com/scl/fi/lymuumy8en46l7p0uwjj3/Planning-2026.xlsx?dl=1"

# Chemins locaux
LOCAL_PATH = r"C:\Users\admin\Dropbox\Goldenlines\Planning 2026.xlsx"
ONE_DRIVE_PATH = r"C:\Users\admin\OneDrive\Goldenlines\Planning 2026.xlsx"

# =========================
# Fonctions
# =========================
def download_dropbox_file(url, dest):
    r = requests.get(url)
    r.raise_for_status()
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    with open(dest, "wb") as f:
        f.write(r.content)
    print(f"[{datetime.now()}] 📥 Téléchargé depuis Dropbox")

def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def file_signature(path):
    stat = os.stat(path)
    return {
        "mtime": stat.st_mtime,
        "size": stat.st_size,
        "hash": file_hash(path),
    }

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# =========================
# MAIN LOOP
# =========================
last_sig = None
log("🔄 Synchronisation Dropbox → OneDrive automatique active")

while True:
    try:
        # Download
        download_dropbox_file(DROPBOX_LINK, LOCAL_PATH)

        # Wait for stability
        sig1 = file_signature(LOCAL_PATH)
        time.sleep(STABILITY_DELAY)
        sig2 = file_signature(LOCAL_PATH)

        if sig1 != sig2:
            log("⏳ Fichier en cours d’écriture (attente)")
            time.sleep(INTERVAL)
            continue

        if last_sig is None:
            last_sig = sig2
            log("📌 Signature initiale enregistrée")
            time.sleep(INTERVAL)
            continue

        if sig2["hash"] != last_sig["hash"]:
            log("🔁 Changements détectés → sync")
            sync_file()
            last_sig = sig2
            log("✅ Synchronisation OneDrive terminée")
        else:
            log("⏭️ Aucun changement détecté")

    except Exception as e:
        log(f"❌ Erreur : {e}")

    time.sleep(INTERVAL)
