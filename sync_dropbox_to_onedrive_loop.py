import time
from sync_dropbox_to_onedrive import sync_file

INTERVAL = 300  # 5 minutes

print("🟢 Synchronisation active (Ctrl+C pour arrêter)")

while True:
    try:
        sync_file()
    except Exception as e:
        print(f"❌ Erreur : {e}")

    time.sleep(INTERVAL)
