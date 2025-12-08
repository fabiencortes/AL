import io
import sqlite3
from datetime import datetime

import pandas as pd
from ftplib import FTP

# =========================
#   CONFIG FTP
# =========================
FTP_HOST = "ftp.airports-linescom.webhosting.be"
FTP_USER = "info@airports-linescom"
FTP_PASSWORD = "A1rp0rts-L1nes"
FTP_FILE_PATH = "/www/wp-content/uploads/2025/11/Planning-2025.xlsx"

# =========================
#   CONFIG BASE DE DONNÉES
# =========================
DB_PATH = "airportslines.db"


def download_excel_from_ftp() -> bytes:
    """Télécharge le fichier Excel depuis le FTP et renvoie les bytes."""
    print("Connexion FTP...")
    bio = io.BytesIO()
    with FTP(FTP_HOST, timeout=20) as ftp:
        ftp.login(FTP_USER, FTP_PASSWORD)
        print("Téléchargement du fichier Excel...")
        ftp.retrbinary(f"RETR {FTP_FILE_PATH}", bio.write)
    print("Téléchargement terminé.")
    return bio.getvalue()


def normalize_feuil1(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie Feuil1 (DATE, GROUPAGE/PARTAGE, etc.) pour la base de données."""
    df = df.copy()

    # Normalisation DATE -> texte dd/mm/yyyy
    if "DATE" in df.columns:
        try:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.strftime("%d/%m/%Y")
        except Exception:
            df["DATE"] = df["DATE"].astype(str)

    # Normalisation HEURE : 08:15, 09:05...
    if "HEURE" in df.columns:
        def norm_time(v):
            if pd.isna(v) or v is None:
                return ""
            s = str(v).strip()
            if not s:
                return ""
            # si 8:15 -> 08:15
            if len(s) == 4 and s[1] == ":":
                s = "0" + s
            if ":" in s:
                return s[:5]
            return s

        df["HEURE"] = df["HEURE"].apply(norm_time)

    # GROUPAGE / PARTAGE -> "0" / "1"
    def flag_to_str(x):
        if pd.isna(x):
            return "0"
        s = str(x).strip().lower()
        if s in ["1", "true", "x", "oui", "yes"]:
            return "1"
        return "0"

    for col in ["GROUPAGE", "PARTAGE"]:
        if col in df.columns:
            df[col] = df[col].apply(flag_to_str)

    # Remplacer NaN par None pour insertion SQLite
    df = df.where(pd.notna(df), None)
    return df


def normalize_generic(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage générique pour Feuil2 / Feuil3."""
    df = df.copy()
    # tout en str ou None
    df = df.where(pd.notna(df), None)
    return df


def create_table_from_df(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame):
    """Crée une table SQLite à partir d'un DataFrame (une colonne par champ)."""
    cur = conn.cursor()

    # Drop table si elle existe déjà
    cur.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()

    if df.empty:
        print(f"⚠️  Feuille vide, table {table_name} non créée.")
        return

    # Construction de la requête CREATE TABLE
    cols_sql_parts = []
    for col in df.columns:
        col_escaped = f'"{col}"'  # garde le nom exact, même avec espaces
        cols_sql_parts.append(f"{col_escaped} TEXT")

    cols_sql = ", ".join(cols_sql_parts)
    sql_create = f'CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {cols_sql})'
    cur.execute(sql_create)
    conn.commit()

    # Préparation de l'INSERT
    col_names_escaped = [f'"{c}"' for c in df.columns]
    placeholders = ", ".join(["?"] * len(df.columns))
    sql_insert = f'INSERT INTO {table_name} ({", ".join(col_names_escaped)}) VALUES ({placeholders})'

    # Insertion ligne par ligne
    print(f"Insertion des données dans {table_name}...")
    for _, row in df.iterrows():
        values = [None if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v) for v in row]
        cur.execute(sql_insert, values)

    conn.commit()
    print(f"✅ Table {table_name} créée avec {len(df)} lignes.")


def main():
    # 1. Télécharger Excel depuis le FTP
    xlsx_bytes = download_excel_from_ftp()

    # 2. Charger les feuilles
    print("Lecture des feuilles Excel...")
    bio = io.BytesIO(xlsx_bytes)

    try:
        df1 = pd.read_excel(bio, sheet_name="Feuil1", engine="openpyxl")
    except Exception as e:
        print("❌ Erreur lecture Feuil1 :", e)
        return

    # pour Feuil2 / Feuil3 on relit le flux
    bio2 = io.BytesIO(xlsx_bytes)
    try:
        df2 = pd.read_excel(bio2, sheet_name="Feuil2", engine="openpyxl")
    except Exception:
        df2 = pd.DataFrame()

    bio3 = io.BytesIO(xlsx_bytes)
    try:
        df3 = pd.read_excel(bio3, sheet_name="Feuil3", engine="openpyxl")
    except Exception:
        df3 = pd.DataFrame()

    print("Feuil1 :", df1.shape, "lignes")
    print("Feuil2 :", df2.shape, "lignes")
    print("Feuil3 :", df3.shape, "lignes")

    # 3. Normaliser
    df1_norm = normalize_feuil1(df1)
    df2_norm = normalize_generic(df2) if not df2.empty else df2
    df3_norm = normalize_generic(df3) if not df3.empty else df3

    # 4. Créer la base SQLite
    print(f"Création / ouverture de la base {DB_PATH} ...")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Table planning (Feuil1)
        create_table_from_df(conn, "planning", df1_norm)

        # Table chauffeurs (Feuil2)
        if not df2_norm.empty:
            create_table_from_df(conn, "chauffeurs", df2_norm)

        # Table annexe (Feuil3)
        if not df3_norm.empty:
            create_table_from_df(conn, "annexe", df3_norm)

    finally:
        conn.close()

    print("🎉 Base de données créée avec succès :", DB_PATH)


if __name__ == "__main__":
    main()
