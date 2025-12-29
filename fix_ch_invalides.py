# fix_ch_invalides.py
import pandas as pd
from database import get_connection, get_chauffeurs

MODE = "DRY_RUN"  
# "DRY_RUN"  -> affiche seulement
# "CLEAN"    -> remplace CH invalide par ""

def fix_ch_invalides():
    with get_connection() as conn:
        df = pd.read_sql_query("SELECT id, CH FROM planning", conn)

    if df.empty or "CH" not in df.columns:
        print("Aucune donnée ou colonne CH absente.")
        return

    chauffeurs_racine = get_chauffeurs()
    if not chauffeurs_racine:
        print("⚠️ Aucun chauffeur officiel.")
        return

    def is_valid_ch(ch_value: str) -> bool:
        if not ch_value:
            return True
        v = str(ch_value).upper().replace(" ", "").strip()
        return any(v.startswith(ch) for ch in chauffeurs_racine)

    invalid_ids = df.loc[~df["CH"].apply(is_valid_ch), "id"].tolist()

    if not invalid_ids:
        print("✅ Rien à corriger.")
        return

    print(f"⚠️ {len(invalid_ids)} ligne(s) à corriger.")

    if MODE == "DRY_RUN":
        print("Mode DRY_RUN activé → aucune modification.")
        print("IDs concernés :", invalid_ids)
        return

    with get_connection() as conn:
        cur = conn.cursor()
        for rid in invalid_ids:
            cur.execute(
                "UPDATE planning SET CH = '' WHERE id = ?",
                (rid,),
            )
        conn.commit()

    print("🧹 Nettoyage terminé : CH invalides neutralisés.")

if __name__ == "__main__":
    fix_ch_invalides()
