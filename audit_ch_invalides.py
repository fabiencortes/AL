# audit_ch_invalides.py
import pandas as pd
from database import get_connection, get_chauffeurs

def audit_ch_invalides():
    with get_connection() as conn:
        df = pd.read_sql_query("SELECT id, DATE, HEURE, NOM, CH FROM planning", conn)

    if df.empty or "CH" not in df.columns:
        print("Aucune donnée ou colonne CH absente.")
        return

    chauffeurs_racine = get_chauffeurs()
    if not chauffeurs_racine:
        print("⚠️ Aucun chauffeur officiel (Feuil2).")
        return

    def is_valid_ch(ch_value: str) -> bool:
        if not ch_value:
            return True  # vide = acceptable
        v = str(ch_value).upper().replace(" ", "").strip()
        # Valide si commence par un chauffeur officiel
        return any(v.startswith(ch) for ch in chauffeurs_racine)

    invalides = df[~df["CH"].apply(is_valid_ch)].copy()

    if invalides.empty:
        print("✅ Aucun CH invalide détecté.")
        return

    print(f"❌ {len(invalides)} ligne(s) avec CH invalide détectée(s) :\n")
    for _, row in invalides.iterrows():
        print(
            f"- ID {row['id']} | {row.get('DATE','')} {row.get('HEURE','')} | "
            f"CH='{row.get('CH')}' | Client='{row.get('NOM','')}'"
        )

if __name__ == "__main__":
    audit_ch_invalides()
