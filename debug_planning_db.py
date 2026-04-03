# ============================================
# 🔎 DEBUG BASE SQLITE - AIRPORTSLINES
# ============================================
# Ce script :
# 1) Affiche la structure de la table planning
# 2) Affiche des lignes existantes
# 3) Affiche des exemples par Num BDC (JC / KI / FNH / BT)
#
# ⚠️ AUCUNE MODIFICATION DE LA DB
# ============================================

import sqlite3
import os

# 🔧 NOM EXACT DE TA BASE
DB_FILE = "airportslines.db"

if not os.path.exists(DB_FILE):
    print(f"❌ Fichier DB introuvable : {DB_FILE}")
    print("➡️ Mets ce script dans le même dossier que airportslines.db")
    input("Appuie sur ENTER pour quitter")
    exit(1)

print(f"✅ DB trouvée : {DB_FILE}")

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# ============================================
# 1️⃣ STRUCTURE TABLE planning
# ============================================
print("\n" + "=" * 60)
print("📋 STRUCTURE DE LA TABLE planning")
print("=" * 60)

try:
    cur.execute("PRAGMA table_info(planning);")
    cols = cur.fetchall()
    for c in cols:
        # (index, nom, type, notnull, default, pk)
        print(c)
except Exception as e:
    print("❌ Erreur lecture structure :", e)

# ============================================
# 2️⃣ EXEMPLES DE LIGNES (10 premières)
# ============================================
print("\n" + "=" * 60)
print("📄 EXEMPLES DE LIGNES (10 premières)")
print("=" * 60)

try:
    cur.execute("""
        SELECT
            DATE,
            HEURE,
            ADRESSE,
            CP,
            Localité,
            "Unnamed: 8",
            DESIGNATION,
            "Num BDC",
            KM,
            "H TVA",
            TTC,
            "Type Nav",
            PAIEMENT
        FROM planning
        LIMIT 10;
    """)
    rows = cur.fetchall()
    for r in rows:
        print(r)
except Exception as e:
    print("❌ Erreur lecture lignes :", e)

# ============================================
# 3️⃣ TEST PAR PRÉFIXE NUM BDC
# ============================================
def show_by_prefix(prefix):
    print("\n" + "-" * 60)
    print(f"🔎 EXEMPLES Num BDC commençant par '{prefix}'")
    print("-" * 60)
    try:
        cur.execute(f"""
            SELECT
                DATE,
                HEURE,
                ADRESSE,
                CP,
                Localité,
                "Unnamed: 8",
                DESIGNATION,
                "Num BDC",
                KM,
                "H TVA",
                TTC,
                "Type Nav",
                PAIEMENT
            FROM planning
            WHERE "Num BDC" LIKE '{prefix}%'
            LIMIT 5;
        """)
        rows = cur.fetchall()
        if not rows:
            print("⚠️ Aucune ligne trouvée")
        for r in rows:
            print(r)
    except Exception as e:
        print("❌ Erreur requête :", e)

for pref in ["JC", "KI", "FNH", "BT"]:
    show_by_prefix(pref)

conn.close()

print("\n✅ FIN DU DEBUG")
input("Appuie sur ENTER pour fermer")