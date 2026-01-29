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

@st.cache_data
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
GREEN_RGBS  = {"FF00B050", "FF92D050"}
ORANGE_RGBS = {"FFFFC000", "FFF4B084"}

def _cell_is_green(cell) -> bool:
    try:
        if not cell or not cell.fill or not cell.fill.fgColor:
            return False
        fg = cell.fill.fgColor
        if fg.type == "rgb" and fg.rgb:
            return fg.rgb.upper() in GREEN_RGBS
        if fg.type == "indexed":
            return fg.indexed in {17}
        if fg.type == "theme":
            return True
        return False
    except Exception:
        return False

def _cell_is_orange(cell) -> bool:
    try:
        if not cell or not cell.fill or not cell.fill.fgColor:
            return False
        fg = cell.fill.fgColor
        if fg.type == "rgb" and fg.rgb:
            return fg.rgb.upper() in ORANGE_RGBS
        if fg.type == "indexed":
            return fg.indexed in {45}
        return False
    except Exception:
        return False
import re

def parse_mail_to_navette(text: str) -> dict:
    """
    Transforme un mail brut en données navette (heuristique simple).
    """
    if not text:
        return {}

    t = text.lower()
    data = {}

    # 📆 Date
    m = re.search(r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})", text)
    if m:
        data["DATE"] = m.group(1)

    # ⏱ Heure
    m = re.search(r"(\d{1,2}[:h]\d{2})", text)
    if m:
        data["HEURE"] = m.group(1).replace("h", ":")

    # 👥 Pax
    m = re.search(r"(\d+)\s*(pax|personne|personnes)", t)
    if m:
        data["PAX"] = int(m.group(1))

    # ✈️ Vol
    m = re.search(r"\b([A-Z]{2}\s?\d{2,4})\b", text)
    if m:
        data["VOL"] = m.group(1).replace(" ", "")

    # 📍 Adresse simple
    m = re.search(r"\b\d{4}\s+[a-zà-ÿ\- ]+", t)
    if m:
        data["ADRESSE"] = m.group(0).title()

    # 🎯 Destination (règles simples)
    if "zaventem" in t or "bruxelles" in t:
        data["DESTINATION"] = "BRU"
    elif "charleroi" in t:
        data["DESTINATION"] = "CRL"
    elif "luxembourg" in t:
        data["DESTINATION"] = "LUX"

    data["RAW"] = text
    return data

# ======================================================
# 📘 FLAGS COULEURS EXCEL (DROPBOX)
# ======================================================

def add_excel_color_flags_from_dropbox(
    df: pd.DataFrame,
    sheet_name: str = "Feuil1"
) -> pd.DataFrame:

    df = df.copy().reset_index(drop=True)

    try:
        content = get_dropbox_excel_cached()
        if not content:
            raise RuntimeError("Fichier Dropbox inaccessible")

        wb = load_workbook(BytesIO(content), data_only=True)
        ws = wb[sheet_name]

        # Header Excel en ligne 2
        headers = [str(c.value).strip() if c.value else "" for c in ws[2]]

        def col_idx(name: str):
            name = name.strip().upper()
            for i, h in enumerate(headers):
                if h.upper() == name:
                    return i + 1
            return None

        col_date   = col_idx("DATE")
        col_heure  = col_idx("HEURE")
        col_ch     = col_idx("CH") or col_idx("CHAUFFEUR")
        col_caisse = col_idx("CAISSE") or col_idx("Caisse") or col_idx("PAIEMENT")

        is_groupage = []
        is_partage  = []
        is_paye     = []
        ack_excel   = []
        is_modif    = []

        # ======================================================
        # 🎨 LECTURE LIGNE PAR LIGNE
        # ======================================================
        for excel_row in range(3, 3 + len(df)):

            c_date   = ws.cell(excel_row, col_date)   if col_date else None
            c_heure  = ws.cell(excel_row, col_heure)  if col_heure else None
            c_ch     = ws.cell(excel_row, col_ch)     if col_ch else None
            c_caisse = ws.cell(excel_row, col_caisse) if col_caisse else None

            # 🟡 GROUPAGE / PARTAGE
            date_y  = _cell_is_yellow(c_date)  if c_date else False
            heure_y = _cell_is_yellow(c_heure) if c_heure else False

            is_groupage.append(1 if date_y and heure_y else 0)
            is_partage.append(1 if (not date_y) and heure_y else 0)

            # 💰 PAIEMENT
            is_paye.append(1 if c_caisse and _cell_is_green(c_caisse) else 0)

            # 👨‍✈️ CHAUFFEUR (Excel)
            if c_ch and _cell_is_green(c_ch):
                ack_excel.append(1)
                is_modif.append(0)
            elif c_ch and _cell_is_orange(c_ch):
                ack_excel.append(0)
                is_modif.append(1)
            else:
                ack_excel.append(0)
                is_modif.append(0)

        df["IS_GROUPAGE"] = is_groupage
        df["IS_PARTAGE"]  = is_partage
        df["IS_PAYE"]     = is_paye
        df["ACK_EXCEL"]   = ack_excel
        df["IS_MODIF"]    = is_modif

        # ⭐ ATTENTE (étoile chauffeur)
        if "CH" in df.columns:
            df["IS_ATTENTE"] = (
                df["CH"]
                .astype(str)
                .str.contains(r"\*", na=False)
                .astype(int)
            )
        else:
            df["IS_ATTENTE"] = 0

        return df

    except Exception as e:
        for col in ["IS_GROUPAGE", "IS_PARTAGE", "IS_ATTENTE", "IS_PAYE", "ACK_EXCEL", "IS_MODIF"]:
            df[col] = 0
        st.error(f"❌ Couleurs Excel non lues : {e}")
        return df


# ======================================================
# 🧾 LOGS (mémoire session) — visible dans l'UI
# ======================================================

import datetime as _dt


def log_event(message: str, level: str = "INFO"):
    """Ajoute une ligne de log en mémoire (st.session_state)."""
    try:
        ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level.upper()}] {message}"
        if "logs" not in st.session_state:
            st.session_state["logs"] = []
        st.session_state["logs"].append(line)
        # limiter taille
        if len(st.session_state["logs"]) > 800:
            st.session_state["logs"] = st.session_state["logs"][-800:]
    except Exception:
        pass


def clear_logs():
    try:
        st.session_state["logs"] = []
    except Exception:
        pass


def render_logs_ui(title: str = "🧾 Logs", height: int = 260):
    """Affiche les logs dans Streamlit (safe)."""
    try:
        st.markdown(f"#### {title}")
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("🧹 Vider les logs", key="btn_clear_logs"):
                clear_logs()
        with col2:
            if st.button("🔄 Rafraîchir", key="btn_refresh_logs"):
                st.rerun()
        logs = st.session_state.get("logs") or []
        st.code("\n".join(logs), language="text")
    except Exception:
        pass
