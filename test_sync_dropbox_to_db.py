from sync_core import sync_planning_cache_from_dropbox

if __name__ == "__main__":
    print("🚀 LANCEMENT TEST SYNCHRO planning_cache")
    n = sync_planning_cache_from_dropbox()
    print(f"🎯 TERMINÉ — {n} lignes importées")
