import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
import streamlit as st



import os
import requests
from io import BytesIO
from openpyxl import load_workbook

def get_dropbox_access_token():
    r = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["DROPBOX_REFRESH_TOKEN"],
            "client_id": os.environ["DROPBOX_APP_KEY"],
            "client_secret": os.environ["DROPBOX_APP_SECRET"],
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def download_dropbox_excel_bytes(path="/Goldenlines/Planning 2026.xlsx"):
    token = get_dropbox_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Dropbox-API-Arg": f'{{"path": "{path}"}}',
        "Content-Type": "application/octet-stream",
    }
    r = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return r.content

@st.cache_data(ttl=300)
def get_dropbox_excel_cached():
    return download_dropbox_excel_bytes()

def _cell_is_yellow(cell) -> bool:
    """
    Détecte le jaune Excel (fill, theme, indexed).
    Compatible Excel réel.
    """
    try:
        fill = cell.fill
        if fill is None or fill.patternType is None:
            return False

        fg = fill.fgColor
        if fg is None:
            return False

        # RGB direct
        if fg.type == "rgb" and fg.rgb:
            rgb = fg.rgb.upper()
            return rgb.endswith("FFFF00") or rgb in {"FFFFFF00", "00FFFF00"}

        # Indexed color (Excel ancien)
        if fg.type == "indexed":
            return fg.indexed in {5, 6}

        # Theme color (Excel moderne)
        if fg.type == "theme":
            return True

        return False
    except Exception:
        return False


# ======================================================
# 📘 FLAGS COULEURS EXCEL (DROPBOX)
# ======================================================

def add_excel_color_flags_from_dropbox(
    df: pd.DataFrame,
    sheet_name: str = "Feuil1"
) -> pd.DataFrame:
    """
    Ajoute les flags Excel depuis le fichier Dropbox :
    - IS_GROUPAGE
    - IS_PARTAGE
    - IS_ATTENTE

    ⚠️ LOGIQUE STRICTEMENT IDENTIQUE À TON ANCIEN CODE
    """

    df = df.copy().reset_index(drop=True)

    try:
        # 🔐 Télécharger le fichier Excel via la fonction fournie
        content = get_dropbox_excel_cached()
        if not content:
            raise RuntimeError("Fichier Dropbox inaccessible")

        wb = load_workbook(BytesIO(content), data_only=True)
        ws = wb[sheet_name]

        # ⚠️ Header Excel en ligne 2
        headers = [str(c.value).strip() if c.value else "" for c in ws[2]]

        def col_idx(name: str):
            name = name.strip().upper()
            for i, h in enumerate(headers):
                if h.upper() == name:
                    return i + 1
            return None

        col_date = col_idx("DATE")
        col_heure = col_idx("HEURE")

        is_groupage = []
        is_partage = []

        # ======================================================
        # 🎨 GROUPAGE / PARTAGE
        # ======================================================
        for excel_row in range(3, 3 + len(df)):
            c_date = ws.cell(excel_row, col_date) if col_date else None
            c_heure = ws.cell(excel_row, col_heure) if col_heure else None

            date_y = _cell_is_yellow(c_date) if c_date else False
            heure_y = _cell_is_yellow(c_heure) if c_heure else False

            is_groupage.append(1 if date_y and heure_y else 0)
            is_partage.append(1 if (not date_y) and heure_y else 0)

        df["IS_GROUPAGE"] = is_groupage
        df["IS_PARTAGE"] = is_partage

        # ======================================================
        # ⭐ ATTENTE (étoile chauffeur)
        # ======================================================
        ch_col = None
        for c in df.columns:
            if str(c).strip().upper() in ("CH", "CHAUFFEUR"):
                ch_col = c
                break

        if ch_col:
            df["IS_ATTENTE"] = (
                df[ch_col]
                .astype(str)
                .str.contains(r"\*", na=False)
                .astype(int)
            )

            if "CH" not in df.columns:
                df["CH"] = df[ch_col]
        else:
            df["IS_ATTENTE"] = 0

        return df

    except Exception as e:
        df["IS_GROUPAGE"] = 0
        df["IS_PARTAGE"] = 0
        df["IS_ATTENTE"] = 0
        st.error(f"❌ Couleurs Excel non lues : {e}")
        return df
