AIRPORTS-LINES — Versions sans boucle (17/02/2026)

Fichiers fournis :
- app_ULTRA.py        : 🔒 ULTRA-STABLE production (aucun auto-sync, aucun rerun implicite)
- app_FAST.py         : ⚡ Version rapide (cache Streamlit conservé, mais toujours sans auto-sync / sans boucle)
- database_ULTRA.py   : 🔒 Connexions SQLite stables (journal_mode=DELETE, busy_timeout)
- utils.py            : inchangé (Dropbox + couleurs Excel)

IMPORTANT (évite 99% des soucis)
1) Arrête Streamlit complètement
2) Supprime si présents :
   - airportslines.db-wal
   - airportslines.db-shm
3) Redémarre Streamlit

Règles de fonctionnement (dans les 2 versions)
- La synchro Excel -> DB doit être lancée uniquement via tes boutons existants (manuels).
- Plus aucun auto-sync en arrière-plan.
- request_soft_refresh / consume_soft_refresh ne déclenchent plus st.rerun() => fini les boucles.

Installation / remplacement
- Remplace ton app.py actuel par app_ULTRA.py (ou renomme app_ULTRA.py en app.py)
- Remplace ton database.py actuel par database_ULTRA.py (ou renomme database_ULTRA.py en database.py)
- Pour la version rapide, utilise app_FAST.py à la place.

