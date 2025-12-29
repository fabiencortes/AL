import sqlite3
import pandas as pd
from datetime import date, timedelta
import datetime

# =========================
# CONFIG
# =========================
DB_PATH = "airportslines.db"
EXCEL_PATH = "Planning 2025.xlsx"
EXCEL_SHEET = "Feuil1"

# =========================
# OUTILS
# =========================
def normalize_date(val):
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val, dayfirst=True).date()
    except Exception:
        return None


def sqlite_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit toutes les valeurs non supportées par SQLite
    en TEXT (ou None).
    """
    df = df.copy()

    for col in df.columns:
        def _safe(val):
            if pd.isna(val):
                return None

            # pandas / python timedelta
            if isinstance(val, (pd.Timedelta, datetime.timedelta)):
                return int(val.total_seconds() // 60)

            # datetime.time
            if isinstance(val, datetime.time):
                return f"{val.hour:02d}:{val.minute:02d}"

            # datetime / date
            if isinstance(val, (datetime.datetime, datetime.date)):
                return val.strftime("%d/%m/%Y")

            return str(val)

        df[col] = df[col].apply(_safe)

    return df


def ensure_tables_exist(conn, columns):
    """
    Crée planning_full, planning_7j, planning_day
    si absentes, avec colonnes Excel en TEXT.
    """
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    cur = conn.cursor()

    for table in ["planning_full", "planning_7j", "planning_day"]:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {cols_sql}
            )
        """)

    conn.commit()


# =========================
# SYNCHRO PRINCIPALE
# =========================
def sync_from_excel():

    print("📥 Lecture du fichier Excel...")
    df = pd.read_excel(EXCEL_PATH, sheet_name=EXCEL_SHEET, engine="openpyxl")

    if df.empty:
        print("❌ Excel vide")
        return

    # Normalisation DATE
    if "DATE" not in df.columns:
        raise ValueError("❌ Colonne DATE absente du fichier Excel")

    df["DATE"] = df["DATE"].apply(normalize_date)
    df = df[df["DATE"].notna()].copy()

    today = date.today()
    max_7j = today + timedelta(days=7)

    # =========================
    # Connexion DB
    # =========================
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # =========================
    # Création tables
    # =========================
    ensure_tables_exist(conn, df.columns)

    print("🧹 Nettoyage des tables...")
    cur.execute("DELETE FROM planning_full")
    cur.execute("DELETE FROM planning_7j")
    cur.execute("DELETE FROM planning_day")
    conn.commit()

    # =========================
    # planning_full (TOUT)
    # =========================
    print("📦 Insertion planning_full...")
    df_full = sqlite_safe_df(df)

    df_full.to_sql(
        "planning_full",
        conn,
        if_exists="append",
        index=False,
    )

    # =========================
    # planning_7j (J → J+7)
    # =========================
    print("📅 Insertion planning_7j...")
    df_7j = df[
        (df["DATE"] >= today) &
        (df["DATE"] <= max_7j)
    ].copy()

    if not df_7j.empty:
        df_7j = sqlite_safe_df(df_7j)

        df_7j.to_sql(
            "planning_7j",
            conn,
            if_exists="append",
            index=False,
        )

    # =========================
    # planning_day (AUJOURD'HUI)
    # =========================
    print("📆 Insertion planning_day...")
    df_day = df[df["DATE"] == today].copy()

    if not df_day.empty:
        df_day = sqlite_safe_df(df_day)

        df_day.to_sql(
            "planning_day",
            conn,
            if_exists="append",
            index=False,
        )

    conn.commit()
    conn.close()

    print("✅ Synchronisation terminée avec succès")


# =========================
# LANCEMENT MANUEL
# =========================
if __name__ == "__main__":
    sync_from_excel()
