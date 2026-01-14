# ============================================================
#   AIRPORTS LINES – APP.PLANNING – VERSION OPTIMISÉE 2025
#   BLOC 1/7 : IMPORTS, CONFIG, HELPERS, SESSION
# ============================================================
DEBUG_SAFE_MODE = True
AUTO_REFRESH_MINUTES = 5  # 🔁 auto-refresh toutes les X minutes
DROPBOX_CHECK_EVERY_SEC = 45          # check mtime toutes les 45s (léger)
AUTO_SYNC_COOLDOWN_SEC = 60           # évite 2 sync collées
META_DBX_MTIME_KEY = "dropbox_mtime"
META_SYNC_LOCK_KEY = "sync_lock"

import os
import io
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
from database import init_time_rules_table
from database import init_actions_table
from database import mark_navette_confirmed
from database import ensure_ack_columns
from pathlib import Path
from streamlit_autorefresh import st_autorefresh

import math
import smtplib
from email.mime.text import MIMEText
import pandas as pd
import requests
from openpyxl import load_workbook
from io import BytesIO
import streamlit as st
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm

from database import (
    get_planning,
    get_chauffeurs,
    get_chauffeur_planning,
    search_client,
    get_row_by_id,
    insert_planning_row,
    update_planning_row,
    delete_planning_row,
    get_planning_columns,
    get_connection,
    confirm_navette_row,
    is_row_confirmed,
    init_indispo_table,
    create_indispo_request,
    get_indispo_requests,
    set_indispo_status,
    ensure_planning_updated_at_column,
    ensure_km_time_columns,
    init_chauffeur_ack_table,
    get_chauffeur_last_ack,
    set_chauffeur_last_ack,
    init_flight_alerts_table,
    ensure_flight_alerts_time_columns,
    should_notify_flight_change,
    upsert_flight_alert,
    sqlite_safe,
    get_last_sync_time,
    set_last_sync_time,
    ensure_meta_table,
    get_meta,
    set_meta,
    get_chauffeur_phone,
    init_chauffeur_ack_rows_table,

)
# ============================================================
#   SESSION STATE
# ============================================================

def init_session_state():
    defaults = {
        # 🔐 Auth
        "logged_in": False,
        "username": None,
        "role": None,
        "chauffeur_code": None,

        # 📅 UI planning
        "planning_start": date.today(),
        "planning_end": date.today() + timedelta(days=6),
        "planning_sort_choice": "Date + heure",

        # 🔄 Sync & refresh
        "sync_running": False,
        "last_auto_sync": 0,

        # 🧭 Rafraîchissement par onglet
        "tab_refresh": {},   # ex: {"admin": 123456789}
        
        # 🔄 Dropbox mtime cache (session)
        "last_dropbox_check": 0,
        "last_seen_dropbox_mtime": None,

    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
# ============================================================
#   CONFIG UTILISATEURS
#   (admins, restreints, chauffeurs GSM)
# ============================================================

USERS = {
    "fab":  {"password": "AL2025",  "role": "admin"},
    "oli":  {"password": "AL2025",  "role": "admin"},
    "leon": {"password": "GL2025", "role": "restricted"},

    # Comptes chauffeurs pour GSM
    "gg": {"password": "gg", "role": "driver", "chauffeur_code": "GG"},
    "fa": {"password": "fa", "role": "driver", "chauffeur_code": "FA"},
    "np": {"password": "np", "role": "driver", "chauffeur_code": "NP"},
    "do": {"password": "do", "role": "driver", "chauffeur_code": "DO"},
    "ma": {"password": "ma", "role": "driver", "chauffeur_code": "MA"},
    "po": {"password": "po", "role": "driver", "chauffeur_code": "PO"},
    "gd": {"password": "gd", "role": "driver", "chauffeur_code": "GD"},
    "om": {"password": "om", "role": "driver", "chauffeur_code": "OM"},
    "ad": {"password": "ad", "role": "driver", "chauffeur_code": "AD"},
}

# Fallback si Feuil2 ne contient rien
CH_CODES = [
    "AU", "FA", "GD", "GG", "LL", "MA", "O", "RK", "RO", "SW", "NP", "DO",
    "OM", "AD", "CB", "CF", "CM", "EM", "GE", "HM", "JF", "KM", "LILLO",
    "MF", "WS", "FA1"
]

# ============================================================
#   LOGIN SCREEN
# ============================================================

def login_screen():
    st.title("🚐 Airports-Lines — Planning chauffeurs (DB)")
    st.subheader("Connexion")

    col1, col2 = st.columns(2)

    with col1:
        login = st.text_input("Login", key="login_name")
    with col2:
        pwd = st.text_input("Mot de passe", type="password", key="login_pass")

    if st.button("Se connecter"):
        user = USERS.get(login)
        if user and user["password"] == pwd:
            st.session_state.logged_in = True
            st.session_state.username = login
            st.session_state.role = user["role"]
            st.session_state.chauffeur_code = user.get("chauffeur_code")
            st.success(f"Connecté en tant que **{login}** – rôle : {user['role']}")
            st.rerun()
        else:
            st.error("Identifiants incorrects.")

    st.caption(
        "Admins : fab/fab, oli/oli — "
        "Utilisateur restreint : leon/leon — "
        "Chauffeur (GSM) : gg/gg, fa/fa, np/np"
    )
FLIGHT_ALERT_DELAY_MIN = 30  # seuil d’alerte retard (modifiable)

def extract_positive_int(val):
    """
    Retourne un entier > 0 si val contient un chiffre valide,
    sinon retourne None.
    """
    if val is None:
        return None

    s = str(val).strip()

    if not s:
        return None

    # On garde uniquement les chiffres
    if s.isdigit():
        n = int(s)
        return n if n > 0 else None

    return None

# ============================================================
#   COULEURS EXCEL -> FLAGS DB (GROUPAGE / PARTAGE / ATTENTE)
# ============================================================

YELLOW_RGBS = {"FFFFFF00", "FFFF00", "00FFFF00"}

def _cell_is_yellow(cell) -> bool:
    """
    Détecte le jaune Excel (fill, theme, indexed).
    Compatible Excel réel (pas théorique).
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
            return fg.indexed in {5, 6}  # jaunes courants Excel

        # Theme color (Excel moderne)
        if fg.type == "theme":
            return True  # on considère thème = volontaire

        return False
    except Exception:
        return False


import urllib.parse

def normalize_address_for_gps(addr: str) -> str:
    """
    Nettoie et normalise une adresse pour Waze / Google Maps.
    """
    if not addr:
        return ""

    a = str(addr)

    # Supprimer retours ligne et espaces multiples
    a = a.replace("\n", " ").replace("\r", " ")
    a = " ".join(a.split())

    # Forcer le pays pour éviter les homonymes
    if "belgique" not in a.lower() and "belgium" not in a.lower():
        a = f"{a}, Belgique"

    return a


def build_waze_link(addr: str) -> str:
    """
    Génère un lien Waze fiable à partir d'une adresse texte.
    """
    addr = normalize_address_for_gps(addr)
    if not addr:
        return ""
    return "https://waze.com/ul?q=" + urllib.parse.quote(addr)


def build_google_maps_link(addr: str) -> str:
    """
    Génère un lien Google Maps fiable à partir d'une adresse texte.
    """
    addr = normalize_address_for_gps(addr)
    if not addr:
        return ""
    return (
        "https://www.google.com/maps/search/?api=1&query="
        + urllib.parse.quote(addr)
    )


def add_excel_color_flags_from_dropbox(
    df: pd.DataFrame,
    sheet_name: str = "Feuil1"
) -> pd.DataFrame:
    df = df.copy().reset_index(drop=True)

    try:
        # 🔐 Télécharger le fichier Excel via l’API Dropbox (UNE seule source)
        content = download_dropbox_excel_bytes()
        if not content:
            raise RuntimeError("Fichier Dropbox inaccessible")

        wb = load_workbook(BytesIO(content), data_only=True)
        ws = wb[sheet_name]

        headers = [str(c.value).strip() if c.value else "" for c in ws[1]]

        def col_idx(name: str):
            name = name.strip().upper()
            for i, h in enumerate(headers):
                if h.upper() == name:
                    return i + 1
            return None

        col_date = col_idx("DATE")
        col_heure = col_idx("HEURE")

        is_groupage: list[int] = []
        is_partage: list[int] = []

        for excel_row in range(2, 2 + len(df)):
            c_date = ws.cell(excel_row, col_date) if col_date else None
            c_heure = ws.cell(excel_row, col_heure) if col_heure else None

            date_y = _cell_is_yellow(c_date) if c_date else False
            heure_y = _cell_is_yellow(c_heure) if c_heure else False

            is_groupage.append(1 if date_y and heure_y else 0)
            is_partage.append(1 if (not date_y) and heure_y else 0)

        df["IS_GROUPAGE"] = is_groupage
        df["IS_PARTAGE"] = is_partage
        df["IS_ATTENTE"] = (
            df["CH"]
            .astype(str)
            .str.contains(r"\*", na=False)
            .astype(int)
        )

        return df

    except Exception as e:
        # 🛡️ Fallback sûr (pas de crash)
        df["IS_GROUPAGE"] = 0
        df["IS_PARTAGE"] = 0
        df["IS_ATTENTE"] = (
            df["CH"]
            .astype(str)
            .str.contains(r"\*", na=False)
            .astype(int)
        )
        st.error(f"❌ Couleurs Excel non lues : {e}")
        return df


# ============================================================
#   BADGES VISUELS NAVETTES
# ============================================================

def navette_badges(row) -> str:
    badges = []

    if int(row.get("IS_GROUPAGE", 0)) == 1:
        badges.append("🟡 Groupée")

    if int(row.get("IS_PARTAGE", 0)) == 1:
        badges.append("🟡 Partagée")

    if int(row.get("IS_ATTENTE", 0)) == 1:
        badges.append("⭐ Attente")

    return " ".join(badges)


# ============================================================
# 🔁 SYNCHRONISATION AUTOMATIQUE INVISIBLE (PLANNING FUTUR)
# ============================================================

import time

def auto_sync_planning_if_needed(silent: bool = True) -> bool:

    now_ts = int(datetime.now().timestamp())
    last_check = int(st.session_state.get("last_dropbox_check") or 0)
    if (now_ts - last_check) < DROPBOX_CHECK_EVERY_SEC:
        return False

    st.session_state["last_dropbox_check"] = now_ts

    mtime = get_dropbox_file_last_modified_safe()
    if not mtime:
        return False

    ensure_meta_table()
    last_global_mtime = get_meta("dropbox_mtime") or ""

    if last_global_mtime == mtime:
        return False

    if not _try_acquire_sync_lock(ttl_seconds=240):
        return False

    try:
        sync_planning_from_today()   # ⚠️ ÉCRASEMENT ABSOLU
        set_meta("dropbox_mtime", mtime)
        set_last_sync_time(datetime.now())

        if not silent:
            st.success("Synchro effectuée")
        return True

    finally:
        _release_sync_lock()


import os, json
from io import BytesIO
import pandas as pd
import requests
import streamlit as st

DROPBOX_FILE_PATH = "/Goldenlines/Planning 2026.xlsx"

import os
import requests

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

def _try_acquire_sync_lock(ttl_seconds: int = 180) -> bool:
    ensure_meta_table()

    now = datetime.now()
    lock_val = get_meta("sync_lock")

    if lock_val:
        try:
            lock_dt = datetime.fromisoformat(lock_val)
            if (now - lock_dt).total_seconds() < ttl_seconds:
                return False
        except Exception:
            pass

    set_meta("sync_lock", now.isoformat(timespec="seconds"))
    return True

def _release_sync_lock():
    try:
        set_meta("sync_lock", "")
    except Exception:
        pass

def get_dropbox_file_last_modified_safe() -> str | None:
    try:
        return get_dropbox_file_last_modified()  # ta fonction existante
    except Exception:
        return None



def load_planning_from_dropbox(sheet_name: str | None = None) -> pd.DataFrame:
    content = download_dropbox_excel_bytes()
    if not content:
        return pd.DataFrame()

    try:
        bio = BytesIO(content)

        # 📌 Règle des en-têtes selon la feuille
        if sheet_name == "Feuil1":
            header_row = 1   # en-tête ligne 2
        else:
            header_row = 0   # Feuil2, Feuil3 → ligne 1

        df = pd.read_excel(
            bio,
            sheet_name=sheet_name,
            engine="openpyxl",
            header=header_row
        )

        return df.fillna("")

    except Exception as e:
        st.error(f"❌ Erreur lecture Excel ({sheet_name}) : {e}")
        return pd.DataFrame()



def get_dropbox_file_last_modified() -> datetime | None:
    try:
        token = os.environ.get("DROPBOX_TOKEN")
        if not token:
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        data = {
            "path": "/Goldenlines/Planning 2026.xlsx"
        }

        r = requests.post(
            "https://api.dropboxapi.com/2/files/get_metadata",
            headers=headers,
            json=data,
            timeout=20,
        )
        r.raise_for_status()

        info = r.json()
        return datetime.fromisoformat(
            info["server_modified"].replace("Z", "+00:00")
        )

    except Exception:
        return None

# ============================================================
#   DB — COLONNES FLAGS COULEURS (AUTO)
# ============================================================

def ensure_planning_color_columns():
    """
    Ajoute dans la table planning les colonnes de flags si elles n'existent pas.
    Compatible avec une DB déjà existante.
    """
    wanted = ["IS_GROUPAGE", "IS_PARTAGE", "IS_ATTENTE"]

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        existing = {row[1] for row in cur.fetchall()}  # row[1] = nom colonne

        for col in wanted:
            if col not in existing:
                conn.execute(f'ALTER TABLE planning ADD COLUMN "{col}" TEXT')
        conn.commit()

# ============================================================
# NORMALISATION DES CODES CHAUFFEURS (FA, FA*, FADO, NPFA...)
# ============================================================

def normalize_ch_code(ch: str) -> str:
    if not ch:
        return ""

    ch = str(ch).upper().replace("*", "").strip()

    # Cas composés → chauffeur principal
    if ch.startswith("FADO"):
        return "FA"
    if ch.startswith("NPFA"):
        return "FA"

    # Cas simples
    if ch.startswith("FA"):
        return "FA"
    if ch.startswith("DO"):
        return "DO"
    if ch.startswith("NP"):
        return "NP"
    if ch.startswith("GD"):
        return "GD"

    # Fallback
    return ch


from datetime import datetime
def render_last_sync_info():
    ts = st.session_state.get("last_auto_sync", 0)
    if not ts:
        return

    txt = datetime.fromtimestamp(ts).strftime("%H:%M")
    st.caption(f"🕒 Dernière synchro : {txt}")

def rebuild_db_fast(status):
    import os
    import shutil
    from datetime import datetime
    from database import ensure_indexes

    NEW_DB = "airportslines_NEW.db"
    MAIN_DB = "airportslines.db"
    BACKUP_DIR = "db_backups"

    status.update(label="📦 Bascule vers la nouvelle base…")

    os.makedirs(BACKUP_DIR, exist_ok=True)

    if os.path.exists(MAIN_DB):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.move(
            MAIN_DB,
            os.path.join(BACKUP_DIR, f"airportslines_{ts}.db")
        )

    os.rename(NEW_DB, MAIN_DB)

    ensure_indexes()

    status.update(label="🎉 Base active remplacée", state="complete")
def format_navette_full_details(row, chauffeur_code: str) -> str:
    """
    Mail ADMIN – détail complet navette
    - IMMAT affichée si non vide
    - REH / SIÈGE affichés uniquement si chiffre > 0
    - Pas de Waze / Google Maps
    """
    from datetime import datetime, date
    import pandas as pd

    # =========================
    # DATE
    # =========================
    dv = row.get("DATE")
    if isinstance(dv, (datetime, date)):
        date_txt = dv.strftime("%d/%m/%Y")
    else:
        dtmp = pd.to_datetime(dv, dayfirst=True, errors="coerce")
        date_txt = dtmp.strftime("%d/%m/%Y") if not pd.isna(dtmp) else ""

    # =========================
    # HEURE
    # =========================
    heure_txt = normalize_time_string(row.get("HEURE")) or "??:??"

    # =========================
    # CLIENT / TRAJET
    # =========================
    nom = str(row.get("NOM", "") or "").strip()
    adr_full = build_full_address_from_row(
        pd.Series(row) if not isinstance(row, pd.Series) else row
    )
    tel_client = get_client_phone_from_row(
        pd.Series(row) if not isinstance(row, pd.Series) else row
    )

    # =========================
    # INFOS NAVETTE
    # =========================
    def g(*cols):
        for c in cols:
            v = row.get(c, "")
            if v is None:
                continue
            s = str(v).strip()
            if s and s.lower() != "nan":
                return s
        return ""

    route = g("DE/VERS", "Unnamed: 8", "DESTINATION", "ROUTE")
    vol = extract_vol_val(row, list(row.keys())) if hasattr(row, "keys") else ""
    pax = g("PAX")
    num_bdc = g("NUM_BDC", "NUM BDC", "BDC")
    paiement = g("PAIEMENT", "Paiement")
    caisse = g("CAISSE", "Caisse", "MONTANT", "Montant")

    # =========================
    # CHAUFFEUR
    # =========================
    ch_raw = str(row.get("CH", "") or "").strip()
    ch_norm = normalize_ch_for_phone(ch_raw)

    # =========================
    # VÉHICULE (RÈGLES STRICTES)
    # =========================
    immat = g("IMMAT", "PLAQUE", "IMMATRICULATION")
    reh_n = extract_positive_int(row.get("REH"))
    siege_n = extract_positive_int(row.get("SIEGE", "SIÈGE"))

    # =========================
    # CONSTRUCTION MAIL
    # =========================
    lines = []
    lines.append("📌 NAVETTE — DÉTAIL ADMIN")
    lines.append(f"📆 Date : {date_txt}")
    lines.append(f"⏱ Heure : {heure_txt}")

    if route:
        lines.append(f"🧭 Trajet : {route}")
    if vol:
        lines.append(f"✈️ Vol : {vol}")
    if pax:
        lines.append(f"👥 PAX : {pax}")
    if num_bdc:
        lines.append(f"🧾 BDC : {num_bdc}")

    lines.append("")
    lines.append(f"👨‍✈️ Chauffeur : {ch_raw}")

    if immat or reh_n or siege_n:
        lines.append("")
        lines.append("🚘 Véhicule :")
        if immat:
            lines.append(f"- Plaque : {immat}")
        if siege_n:
            lines.append(f"- Siège enfant : {siege_n}")
        if reh_n:
            lines.append(f"- REH : {reh_n}")

    lines.append("")
    lines.append(f"🧑 Client : {nom or '—'}")
    lines.append(f"📍 Adresse : {adr_full or '—'}")
    lines.append(f"📞 Client : {tel_client or '—'}")

    if paiement or caisse:
        lines.append("")
        lines.append("💳 Paiement :")
        if paiement:
            lines.append(f"- Type : {paiement}")
        if caisse:
            lines.append(f"- Montant caisse : {caisse}")

    return "\n".join(lines).strip()


def format_navette_ack(row, ch_selected, trajet, probleme):
    from datetime import datetime, date
    import pandas as pd

    # =========================
    # DATE
    # =========================
    dv = row.get("DATE")
    if isinstance(dv, (datetime, date)):
        date_txt = dv.strftime("%d/%m/%Y")
    else:
        dtmp = pd.to_datetime(dv, dayfirst=True, errors="coerce")
        date_txt = dtmp.strftime("%d/%m/%Y") if not pd.isna(dtmp) else ""

    # =========================
    # HEURE
    # =========================
    heure_txt = normalize_time_string(row.get("HEURE")) or "??:??"

    # =========================
    # SENS + LIEU
    # =========================
    sens = str(row.get("Unnamed: 8", "") or "").strip().upper()
    if sens not in ("DE", "VERS"):
        sens = "DE"

    lieu = str(row.get("DESIGNATION", "") or "").strip()
    lieu = resolve_client_alias(lieu)

    sens_txt = f"{sens} ({lieu})" if lieu else sens

    # =========================
    # CLIENT
    # =========================
    nom = str(row.get("NOM", "") or "").strip()
    adr_full = build_full_address_from_row(row)
    tel_client = get_client_phone_from_row(row)

    # =========================
    # VÉHICULE (RÈGLES STRICTES)
    # =========================
    immat = str(row.get("IMMAT", "") or "").strip()
    reh_n = extract_positive_int(row.get("REH"))
    siege_n = extract_positive_int(row.get("SIEGE", "SIÈGE"))

    vehicule_lines = []
    if immat:
        vehicule_lines.append(f"Plaque : {immat}")
    if siege_n:
        vehicule_lines.append(f"Siège enfant : {siege_n}")
    if reh_n:
        vehicule_lines.append(f"REH : {reh_n}")

    vehicule_block = ""
    if vehicule_lines:
        vehicule_block = "\n🚘 Véhicule :\n" + "\n".join(vehicule_lines)

    # =========================
    # MAIL FINAL (SANS WAZE / MAPS)
    # =========================
    return f"""📆 {date_txt} | ⏱ {heure_txt}
👨‍✈️ Chauffeur : {ch_selected}
🚗 Sens : {sens_txt}

🧑 Client : {nom}
📍 Adresse : {adr_full}
📞 Client : {tel_client or "—"}{vehicule_block}

📝 Infos chauffeur :
Trajet : {trajet or "—"}
Problème : {probleme or "—"}
"""


def send_planning_confirmation_email(chauffeur: str, row, trajet: str, commentaire: str):
    """
    Mail admin = DÉTAIL COMPLET navette + en dessous la réponse du chauffeur.
    """
    from datetime import datetime

    subject = f"[CONFIRMATION PLANNING] {chauffeur}"

    navette_full = format_navette_full_details(row, chauffeur)

    # Réponse chauffeur (en dessous)
    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    reponse = f"""✅ RÉPONSE DU CHAUFFEUR
Horodatage : {ts}
Chauffeur : {chauffeur}

Trajet compris : {trajet or "—"}
Commentaire / problème : {commentaire or "—"}
"""

    body = navette_full + "\n\n" + reponse + "\nMessage envoyé depuis l’application Airports Lines."

    send_mail_admin(subject, body)


def is_navette_confirmed(row):
    """
    Une navette est confirmée si ACK_AT est renseigné en DB
    """
    return bool(row.get("ACK_AT"))

def rebuild_planning_views():
    """
    🔁 Recrée toutes les vues SQL planning
    → indispensable quand on ajoute des colonnes (IMMAT, REH, SIEGE, etc.)
    """
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("DROP VIEW IF EXISTS planning_day")
        cur.execute("DROP VIEW IF EXISTS planning_7j")
        cur.execute("DROP VIEW IF EXISTS planning_full")

        cur.execute("""
            CREATE VIEW planning_full AS
            SELECT * FROM planning
        """)

        cur.execute("""
            CREATE VIEW planning_7j AS
            SELECT *
            FROM planning
            WHERE DATE_ISO BETWEEN date('now') AND date('now','+6 day')
        """)

        cur.execute("""
            CREATE VIEW planning_day AS
            SELECT *
            FROM planning
            WHERE DATE_ISO = date('now')
        """)

        conn.commit()


def sync_planning_from_today():
    from datetime import datetime

    today_iso = date.today().strftime("%Y-%m-%d")

    # 1️⃣ Charger Excel Dropbox (Feuil1)
    df_excel = load_planning_from_dropbox("Feuil1")
    if df_excel.empty:
        st.warning("Planning Dropbox vide.")
        return 0

    # 2️⃣ Flags couleurs Excel
    df_excel = add_excel_color_flags_from_dropbox(df_excel, "Feuil1")
    ensure_planning_color_columns()

    # 3️⃣ Normalisation DATE
    df_excel["DATE_ISO"] = pd.to_datetime(
        df_excel["DATE"],
        dayfirst=True,
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")

    # 🔥 garder uniquement aujourd’hui + futur
    df_excel = df_excel[df_excel["DATE_ISO"] >= today_iso].copy()

    if df_excel.empty:
        st.info("Aucune donnée à synchroniser.")
        return 0

    # 4️⃣ Suppression DB ciblée
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM planning WHERE DATE_ISO >= ?",
            (today_iso,),
        )
        conn.commit()

    # 5️⃣ Réinsertion propre (Feuil1)
    inserts = 0

    for _, row in df_excel.iterrows():
        heure_norm = normalize_time_string(row.get("HEURE"))

        data = {
            col: sqlite_safe(row[col])
            for col in df_excel.columns
            if col not in ("id", "HEURE")
        }

        data["HEURE"] = heure_norm
        data["DATE_ISO"] = row["DATE_ISO"]

        insert_planning_row(data)
        inserts += 1

    # 6️⃣ Recréer les vues SQL
    rebuild_planning_views()

    # ======================================================
    # 7️⃣ Import Feuil2 → table chauffeurs
    # ======================================================
    df_ch = load_planning_from_dropbox("Feuil2")
    if not df_ch.empty:
        with get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS chauffeurs")
            conn.commit()

        cols = [c for c in df_ch.columns if c]
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        cols_sql = ",".join(f'"{c}"' for c in cols)
        placeholders = ",".join("?" for _ in cols)

        with get_connection() as conn:
            conn.execute(f'CREATE TABLE chauffeurs ({col_defs})')
            conn.commit()

        for _, r in df_ch.iterrows():
            values = [sqlite_safe(r.get(c)) for c in cols]
            with get_connection() as conn:
                conn.execute(
                    f'INSERT INTO chauffeurs ({cols_sql}) VALUES ({placeholders})',
                    values,
                )
                conn.commit()

    # ======================================================
    # 8️⃣ Import Feuil3 → table feuil3
    # ======================================================
    df_f3 = load_planning_from_dropbox("Feuil3")
    if not df_f3.empty:
        with get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS feuil3")
            conn.commit()

        cols3 = [c for c in df_f3.columns if c]
        col_defs3 = ", ".join(f'"{c}" TEXT' for c in cols3)
        cols_sql3 = ",".join(f'"{c}"' for c in cols3)
        placeholders3 = ",".join("?" for _ in cols3)

        with get_connection() as conn:
            conn.execute(f'CREATE TABLE feuil3 ({col_defs3})')
            conn.commit()

        for _, r in df_f3.iterrows():
            values = [sqlite_safe(r.get(c)) for c in cols3]
            with get_connection() as conn:
                conn.execute(
                    f'INSERT INTO feuil3 ({cols_sql3}) VALUES ({placeholders3})',
                    values,
                )
                conn.commit()

    # ======================================================
    # 9️⃣ Mémoriser heure de dernière synchro (UI)
    # ======================================================
    st.session_state["last_sync_time"] = datetime.now().strftime("%H:%M")

    # ======================================================
    # 🔥 Rafraîchir toutes les vues Streamlit
    # ======================================================
    st.cache_data.clear()
    st.rerun()

    return inserts

def sync_planning_from_uploaded_file(uploaded_file):
    """
    Synchronisation DB depuis un fichier Excel uploadé manuellement
    (mode secours si Dropbox indisponible)
    """
    try:
        # 🔹 Lire le fichier uploadé en mémoire
        content = uploaded_file.getbuffer()

        # 🔹 Monkey-patch temporaire : on remplace le downloader Dropbox
        def _mock_download_dropbox_excel_bytes(path=None):
            return content

        # Sauvegarde de la fonction originale
        original_download = download_dropbox_excel_bytes

        # Remplacement temporaire
        globals()["download_dropbox_excel_bytes"] = _mock_download_dropbox_excel_bytes

        # 🔁 Réutilise EXACTEMENT la même logique que Dropbox
        inserted = sync_planning_from_today()

        # 🔙 Restauration fonction originale
        globals()["download_dropbox_excel_bytes"] = original_download

        return inserted

    except Exception as e:
        st.error(f"❌ Erreur synchronisation fichier manuel : {e}")
        return 0




from database import make_row_key_from_row, get_latest_ch_overrides_map

def apply_actions_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()

    # calc row_key
    keys = []
    row_keys = []
    for _, r in df.iterrows():
        rk = make_row_key_from_row(r.to_dict())
        row_keys.append(rk)
        keys.append(rk)

    df["row_key"] = row_keys

    # overrides
    mp = get_latest_ch_overrides_map(keys)
    if mp:
        df["_CH_ORIG"] = df.get("CH", "")
        df["CH"] = df.apply(lambda x: mp.get(x["row_key"], x.get("CH", "")), axis=1)
        df["_needs_excel_update"] = df["row_key"].apply(lambda k: 1 if k in mp else 0)
    else:
        df["_needs_excel_update"] = 0

    return df

import requests
def flight_badge(status: str, delay_min: int = 0) -> str:
    status = (status or "").upper()
    delay_min = int(delay_min or 0)

    if status == "ON_TIME":
        return "🟢 À l’heure"
    if status == "DELAYED":
        if delay_min >= 30:
            return f"🔴 Retard {delay_min} min"
        return f"🟠 Retard {delay_min} min"
    if status == "CANCELLED":
        return "🔴 Annulé"
    if status == "LANDED":
        return "✅ Atterri"
    return "⚪ Statut inconnu"
def extract_vol_val(row, columns):
    """
    Extrait le numéro de vol depuis une ligne,
    robuste aux variantes de nom de colonne.
    """
    for col in ["N° Vol", "N° Vol ", "Num Vol", "VOL", "Vol"]:
        if col in columns:
            v = str(row.get(col, "") or "").strip()
            if v:
                return v
    return ""
AVIATIONSTACK_KEY = "e5cb6733f9d69693e880c982795ba27d"
import requests
import streamlit as st

@st.cache_data(ttl=900)
def get_flight_status_cached(flight_number: str):
    """
    Retourne TOUJOURS un tuple :
    (status, delay_min, sched_dt, est_dt)
    sched_dt / est_dt = datetime pandas (ou None)
    """
    if not flight_number:
        return "", 0, None, None

    try:
        r = requests.get(
            "http://api.aviationstack.com/v1/flights",
            params={"access_key": AVIATIONSTACK_KEY, "flight_iata": flight_number},
            timeout=5,
        )
        data = r.json()

        if not data.get("data"):
            return "", 0, None, None

        f = data["data"][0]
        status_raw = (f.get("flight_status") or "").lower()

        # mapping statut
        if status_raw in ("scheduled", "active"):
            status = "ON_TIME"
        elif status_raw == "delayed":
            status = "DELAYED"
        elif status_raw == "cancelled":
            status = "CANCELLED"
        elif status_raw == "landed":
            status = "LANDED"
        else:
            status = ""

        # ⚠️ on prend ici ARRIVAL (arrivée) : scheduled / estimated
        sched = f.get("arrival", {}).get("scheduled")
        est = f.get("arrival", {}).get("estimated")

        sched_dt = pd.to_datetime(sched) if sched else None
        est_dt = pd.to_datetime(est) if est else None

        delay_min = 0
        if sched_dt is not None and est_dt is not None:
            delay_min = int((est_dt - sched_dt).total_seconds() / 60)

        return status, delay_min, sched_dt, est_dt

    except Exception:
        return "", 0, None, None

# ============================================================
#   MAPPING ABRÉVIATIONS CLIENTS / SITES
# ============================================================

CLIENT_ALIASES = {
    "KI HQ": {
        "name": "Knauf Insulation",
        "site": "Headquarters",
        "city": "Visé",
    },
    "JCO": {
        "name": "John Cockerill",
        "site": "Site industriel",
        "city": "Seraing",
    },
    "JCC": {
        "name": "John Cockerill",
        "site": "Site château",
        "city": "Seraing",
    },
}


# ==========================
#  KM / TEMPS (OpenRouteService)
# ==========================
ORS_API_KEY = "5b3ce3597851110001cf62480ac03479d6074e1ebda549044ad14608"

AIRPORT_ALIASES = {
    "CRL": "Brussels South Charleroi Airport, Belgium",
    "CHARLEROI": "Brussels South Charleroi Airport, Belgium",
    "BRU": "Brussels Airport, Zaventem, Belgium",
    "BRUXELLES": "Brussels Airport, Zaventem, Belgium",
    "ZAVENTEM": "Brussels Airport, Zaventem, Belgium",
    "LUX": "Luxembourg Airport, Luxembourg",
    "LUXEMBOURG": "Luxembourg Airport, Luxembourg",
}

def _pick_first(row, candidates):
    for c in candidates:
        if c in row.index:
            v = str(row.get(c, "") or "").strip()
            if v and v.lower() != "nan":
                return v
    return ""

def build_full_address_from_row(row: pd.Series) -> str:
    # Essaye de reconstruire "Adresse + CP + Ville"
    adr = _pick_first(row, ["ADRESSE", "Adresse", "ADRESSE RDV", "Adresse RDV", "RUE", "Rue"])
    cp  = _pick_first(row, ["CP", "Code postal", "CODE POSTAL", "Postal", "ZIP"])
    vil = _pick_first(row, ["Localité", "LOCALITE", "Ville", "VILLE", "COMMUNE"])
    parts = [p for p in [adr, cp, vil] if p]
    return " ".join(parts).strip()

def resolve_destination_text(row: pd.Series) -> str:
    # Colonne destination/route dans ton fichier : tu utilises déjà "DE/VERS" et parfois "Unnamed: 8"
    dest = _pick_first(row, ["DE/VERS", "DESTINATION", "Destination", "Unnamed: 8", "ROUTE"])
    if not dest:
        return ""
    key = dest.strip().upper()
    for k, full in AIRPORT_ALIASES.items():
        if k in key:
            return full
    return dest

@st.cache_data(ttl=24*3600)
def ors_route_km_min(origin_text: str, dest_text: str):
    """
    Retourne (km, minutes) via ORS directions.
    Cache 24h pour éviter de brûler la clé.
    """
    if not ORS_API_KEY:
        return None, None
    if not origin_text or not dest_text:
        return None, None

    # ORS: on passe par géocodage Nominatim-like ? => ORS a aussi /geocode/search.
    # Pour rester simple et robuste: ORS Geocode puis Directions.
    try:
        # 1) Geocode origin
        r1 = requests.get(
            "https://api.openrouteservice.org/geocode/search",
            params={"api_key": ORS_API_KEY, "text": origin_text},
            timeout=8
        ).json()
        if not r1.get("features"):
            return None, None
        o_lon, o_lat = r1["features"][0]["geometry"]["coordinates"]

        # 2) Geocode dest
        r2 = requests.get(
            "https://api.openrouteservice.org/geocode/search",
            params={"api_key": ORS_API_KEY, "text": dest_text},
            timeout=8
        ).json()
        if not r2.get("features"):
            return None, None
        d_lon, d_lat = r2["features"][0]["geometry"]["coordinates"]

        # 3) Directions driving-car
        r3 = requests.post(
            "https://api.openrouteservice.org/v2/directions/driving-car",
            headers={"Authorization": ORS_API_KEY, "Content-Type": "application/json"},
            json={"coordinates": [[o_lon, o_lat], [d_lon, d_lat]]},
            timeout=10
        ).json()

        feat = (r3.get("features") or [None])[0]
        if not feat:
            return None, None

        seg = feat["properties"]["segments"][0]
        dist_m = float(seg.get("distance", 0.0))
        dur_s  = float(seg.get("duration", 0.0))

        km = round(dist_m / 1000.0, 1)
        minutes = int(round(dur_s / 60.0))
        return km, minutes
    except Exception:
        return None, None

# ============================================================
#   CONFIG STREAMLIT
# ============================================================

st.set_page_config(
    page_title="Airports-Lines – Planning chauffeurs",
    layout="wide",
)

# ============================================================
# 🔐 INITIALISATION SESSION
# ============================================================
init_session_state()

# 🧾 TABLE ACK CHAUFFEUR (PAR LIGNE)
init_chauffeur_ack_rows_table()

# ============================================================
# 🔁 SYNCHRONISATION SILENCIEUSE (AVANT LOGIN)
# 👉 fonctionne même sur l'écran login
# ============================================================
try:
    auto_sync_planning_if_needed()
except Exception:
    pass

# ============================================================
# 🔐 LOGIN (PERSISTANT)
# ============================================================
if st.session_state.get("logged_in") is not True:
    login_screen()
    st.stop()


def get_chauffeurs_for_ui() -> List[str]:
    """
    Liste des codes CH pour les listes déroulantes :

    - on part des chauffeurs officiels (Feuil2 → get_chauffeurs())
    - on ajoute tous les codes distincts trouvés dans la colonne CH
      du planning (FA*, FANP, FADO, …)
    - on ne modifie rien dans la DB, ni dans le XLSX
    - les codes sont affichés exactement comme dans le planning
      (on enlève juste les espaces autour)
    """
    # Base : chauffeurs officiels (Feuil2 / table chauffeurs)
    try:
        base = get_chauffeurs()  # ex: FA, FA1, DO, NP, ...
    except Exception:
        base = []

    # Valeurs réelles présentes dans la colonne CH du planning
    extra: List[str] = []
    try:
        df_all = get_planning(
            start_date=None,
            end_date=None,
            chauffeur=None,
            type_filter=None,
            search=None,
            max_rows=None,  # pas de limite
        )
        if not df_all.empty and "CH" in df_all.columns:
            extra = (
                df_all["CH"]
                .astype(str)
                .map(lambda x: x.strip() if x is not None else "")
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
    except Exception:
        df_all = None

    # Union des deux listes, sans doublons, sans changer la casse
    all_codes = []
    seen = set()
    for code in (base + extra):
        if code is None:
            continue
        c = str(code).strip()
        if not c:
            continue
        if c not in seen:
            seen.add(c)
            all_codes.append(c)

    # Tri alphabétique simple
    all_codes = sorted(all_codes, key=lambda x: x.upper())
    return all_codes

# ===========================
#  CONFIG NOTIFICATIONS EMAIL
# ===========================

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "airportslinesbureau@gmail.com"
SMTP_PASSWORD = "xnib fwba oisn aadk"

ADMIN_NOTIFICATION_EMAIL = "airportslinesbureau@gmail.com"
FROM_EMAIL = SMTP_USER
# ============================================================
#   HELPERS — NORMALISATION DES HEURES
# ============================================================

def normalize_time_string(val):
    """
    Nettoie et convertit une heure vers HH:MM:SS pour la DB.
    Retourne None si invalide / vide.
    """
    if val is None:
        return None

    s = str(val).strip()
    if not s or s == "0":
        return None

    # Remplacer H / h par :
    s = s.replace("H", ":").replace("h", ":").strip()

    # Format HHMM → HH:MM
    if s.isdigit():
        try:
            if len(s) <= 2:
                h = int(s)
                m = 0
            else:
                h = int(s[:-2])
                m = int(s[-2:])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f"{h:02d}:{m:02d}:00"
            return None
        except Exception:
            return None

    # Format H:M, HH:M, H:MM, HH:MM
    if ":" in s:
        try:
            h, m = s.split(":")[:2]
            h = int(h)
            m = int(m)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f"{h:02d}:{m:02d}:00"
            return None
        except Exception:
            return None

    return None

def format_sens_ar(val: str) -> str:
    """
    Normalise la colonne Unnamed: 8 :
    - DE / VERS
    - + A/R si présent
    """
    if not val:
        return ""

    txt = str(val).upper().strip()

    has_ar = "A/R" in txt or txt.replace("/", "") == "AR"

    if "DE" in txt:
        sens = "DE"
    elif "VERS" in txt:
        sens = "VERS"
    else:
        sens = ""

    if has_ar and sens:
        return f"{sens} – A/R"
    if has_ar:
        return "A/R"
    return sens


def resolve_client_alias(text: str) -> str:
    """
    Remplace une abréviation connue par sa description complète.
    (Pour affichage : vue chauffeur, vue mobile, PDF, WhatsApp, etc.)
    """
    if not text:
        return ""

    raw = str(text).strip()
    key = raw.upper()

    info = CLIENT_ALIASES.get(key)
    if not info:
        return raw

    parts = [info.get("name", "").strip()]
    if info.get("site"):
        parts.append(str(info["site"]).strip())
    if info.get("city"):
        parts.append(str(info["city"]).strip())

    parts = [p for p in parts if p]
    return " – ".join(parts) if parts else raw

# ============================================================
#   HELPERS – BOOL FLAG
# ============================================================

def bool_from_flag(x) -> bool:
    """Convertit 1, TRUE, x, oui, Yes, etc. en bool."""
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "x", "oui", "yes"]
# ============================================================
#   📊 HISTORIQUE DES ENVOIS — DB
# ============================================================

def ensure_send_log_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS send_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                chauffeur TEXT,
                canal TEXT,
                periode TEXT,
                statut TEXT,
                message TEXT
            )
        """)
        conn.commit()


def log_send(chauffeur, canal, periode, statut, message):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO send_log (chauffeur, canal, periode, statut, message)
            VALUES (?, ?, ?, ?, ?)
            """,
            (chauffeur, canal, periode, statut, message),
        )
        conn.commit()



# ============================================================
#   DÉTECTION INDISPO CHAUFFEUR
#   (Feuil1 = NP 12:00 … etc.)
# ============================================================

def is_indispo_row(row, cols) -> bool:
    """
    Une ligne est une indispo si :
    - colonne ²²²² contient une HEURE DE FIN
    - et aucune info client (NOM / DESIGNATION / ADRESSE ...)
    """
    if "²²²²" not in cols:
        return False

    end_raw = row.get("²²²²", "")
    end_indispo = normalize_time_string(end_raw)

    if not end_indispo:
        return False

    # Vérifie que ce n’est pas une vraie navette
    nom = str(row.get("NOM", "") or "").strip()
    designation = str(row.get("DESIGNATION", "") or "").strip()
    route = str(row.get("Unnamed: 8", "") or "").strip()

    if nom == "" and designation == "" and route == "":
        return True

    return False


# ============================================================
#   HELPERS — PHONE / WHATSAPP / MAIL
# ============================================================

def clean_phone(phone: str) -> str:
    if phone is None:
        return ""
    return "".join(ch for ch in str(phone) if ch.isdigit())


def phone_to_whatsapp_number(phone: str) -> str:
    digits = clean_phone(phone)
    if not digits:
        return ""
    if digits.startswith("0"):
        return "32" + digits[1:]
    return digits


def build_whatsapp_link(phone: str, message: str) -> str:
    import urllib.parse
    num = phone_to_whatsapp_number(phone)
    if not num:
        return "#"
    return f"https://wa.me/{num}?text={urllib.parse.quote(message)}"

def build_waze_link(address: str) -> str:
    """Construit un lien Waze vers une adresse texte."""
    import urllib.parse

    addr = (address or "").strip()
    if not addr:
        return "#"

    query = urllib.parse.quote(addr)
    # Sur GSM, ce lien ouvre directement l'appli Waze si elle est installée
    return f"https://waze.com/ul?q={query}&navigate=yes"

def build_google_maps_link(address: str) -> str:
    import urllib.parse
    if not address:
        return "#"
    return (
        "https://www.google.com/maps/search/?api=1&query="
        + urllib.parse.quote(address)
    )


def build_mailto_link(to_email: str, subject: str, body: str) -> str:
    import urllib.parse
    if not to_email:
        return "#"
    return (
        "mailto:"
        + to_email
        + "?subject="
        + urllib.parse.quote(subject)
        + "&body="
        + urllib.parse.quote(body)
    )
def send_mail_admin(subject: str, body: str):
    """Envoie un mail texte simple à l'admin."""
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = ADMIN_NOTIFICATION_EMAIL

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
    except Exception as e:
        print("Erreur envoi mail:", e)
def build_planning_mail_body(
    df_ch: pd.DataFrame,
    ch: str,
    from_date: date,
    to_date: date | None,
):
    cols = df_ch.columns.tolist()
    lines: list[str] = []

    # =============================
    # EN-TÊTE
    # =============================
    periode = (
        from_date.strftime("%d/%m/%Y")
        if not to_date or from_date == to_date
        else f"{from_date.strftime('%d/%m/%Y')} → {to_date.strftime('%d/%m/%Y')}"
    )

    lines.append(f"🚖 Planning — Chauffeur : {ch}")
    lines.append(f"📆 Période : {periode}")
    lines.append("")

    # =============================
    # BOUCLE NAVETTES
    # =============================
    for _, row in df_ch.iterrows():

        # ===================================================
        # 🚖 NAVETTE — BLOC COMPLET (MAIL)
        # ===================================================

        # ------------------
        # Flags groupage / partage / attente
        # ------------------
        is_groupage = int(row.get("IS_GROUPAGE", 0) or 0) == 1
        is_partage = int(row.get("IS_PARTAGE", 0) or 0) == 1
        is_attente = int(row.get("IS_ATTENTE", 0) or 0) == 1

        prefix = ""
        if is_groupage:
            prefix += "[GROUPÉE] "
        elif is_partage:
            prefix += "[PARTAGÉE] "
        if is_attente:
            prefix += "⭐ "

        # ------------------
        # Chauffeur
        # ------------------
        ch_code = str(row.get("CH", "") or ch).strip()
        lines.append(f"👨‍✈️ {ch_code}")

        # ------------------
        # Confirmation (par row_key)
        # ------------------
        row_key = row.get("ROW_KEY") or row.get("row_key")
        if row_key and is_row_confirmed(ch, row_key):
            lines.append("✅ Navette confirmée")
        else:
            lines.append("🕒 À confirmer")

        # ------------------
        # Date / Heure
        # ------------------
        dv = row.get("DATE")
        if isinstance(dv, date):
            date_txt = dv.strftime("%d/%m/%Y")
        else:
            dtmp = pd.to_datetime(dv, dayfirst=True, errors="coerce")
            date_txt = dtmp.strftime("%d/%m/%Y") if not pd.isna(dtmp) else ""

        heure_txt = normalize_time_string(row.get("HEURE")) or "??:??"
        lines.append(f"{prefix}📆 {date_txt} | ⏱ {heure_txt}")

        # ------------------
        # Sens / Destination
        # ------------------
        sens_txt = format_sens_ar(row.get("Unnamed: 8"))
        dest = resolve_client_alias(str(row.get("DESIGNATION", "") or "").strip())
        if sens_txt or dest:
            lines.append(f"➡ {sens_txt} ({dest})".strip())

        # ------------------
        # Client
        # ------------------
        nom = str(row.get("NOM", "") or "").strip()
        if nom:
            lines.append(f"🧑 {nom}")

        # ------------------
        # 👥 PAX
        # ------------------
        pax = row.get("PAX")
        if pax not in ("", None, 0, "0"):
            try:
                pax_i = int(pax)
                if pax_i > 0:
                    lines.append(f"👥 {pax_i} pax")
            except Exception:
                lines.append(f"👥 {pax} pax")

        # ------------------
        # 🚘 Véhicule
        # ------------------
        if row.get("IMMAT"):
            lines.append(f"🚘 Plaque : {row.get('IMMAT')}")

        siege_bebe = extract_positive_int(row.get("SIEGE", row.get("SIÈGE")))
        if siege_bebe:
            lines.append(f"🍼 Siège bébé : {siege_bebe}")

        reh_n = extract_positive_int(row.get("REH"))
        if reh_n:
            lines.append(f"🪑 Rehausseur : {reh_n}")

        # ------------------
        # Adresse / Tel
        # ------------------
        adr = build_full_address_from_row(row)
        if adr:
            lines.append(f"📍 {adr}")

        tel = get_client_phone_from_row(row)
        if tel:
            lines.append(f"📞 {tel}")

        # ------------------
        # Paiement
        # ------------------
        paiement = str(row.get("PAIEMENT", "") or "").lower().strip()
        caisse = row.get("Caisse")

        if paiement == "facture":
            lines.append("🧾 FACTURE")
        elif paiement == "caisse" and caisse:
            lines.append(f"💶 {caisse} € (CASH)")
        elif paiement == "bancontact" and caisse:
            lines.append(f"💳 {caisse} € (BANCONTACT)")

        # ------------------
        # Vol + statut
        # ------------------
        vol = extract_vol_val(row, cols)
        if vol:
            lines.append(f"✈️ Vol {vol}")
            status, delay_min, *_ = get_flight_status_cached(vol)
            badge = flight_badge(status, delay_min)
            if badge:
                lines.append(f"📡 {badge}")

        # ------------------
        # GO
        # ------------------
        go_val = str(row.get("GO", "") or "").strip()
        if go_val:
            lines.append(f"🟢 {go_val}")

        # ------------------
        # 🧾 BDC
        # ------------------
        for cand in ["NUM BDC", "Num BDC", "NUM_BDC", "BDC"]:
            if cand in cols and row.get(cand):
                lines.append(f"🧾 BDC : {row.get(cand)}")
                break

        # ------------------
        # Séparation navettes
        # ------------------
        lines.append("")

    return "\n".join(lines).strip()



def get_client_phone_from_row(row: pd.Series) -> str:
    """
    Récupère le numéro GSM du client.
    Ta colonne dans l'Excel s'appelle 'Tél'.
    On ajoute aussi des variantes au cas où.
    """
    candidate_cols = [
        "Tél",          # ta colonne principale
        "TEL",          # variantes possibles
        "Tel",
        "Téléphone",
        "GSM",
        "N° GSM",
        "N°GSM",
        "TEL CLIENT",
        "TEL_CLIENT",
        "PHONE",
    ]

    for col in candidate_cols:
        if col in row.index:
            val = row.get(col)
            if val is not None and str(val).strip():
                return str(val).strip()

    return ""
def normalize_ch_for_phone(ch_code: str) -> str:
    """
    Normalise le code chauffeur pour retrouver son GSM / MAIL dans Feuil2.

    Règles métier finales :
      - 'DO*'   -> 'DO'
      - 'DOFA'  -> 'DO'
      - 'FADO'  -> 'DO'
      - 'FA*'   -> 'FA'
      - 'FA1*'  -> 'FA1'
      - 'AD*'   -> 'AD'
      - 'NP*'   -> 'NP'
    """
    if not ch_code:
        return ""

    code = str(ch_code).strip().upper()

    # Supprimer les étoiles
    code = code.replace("*", "")

    # 🔥 PRIORITÉ ABSOLUE À DO
    if "DO" in code:
        return "DO"

    # Liste des chauffeurs connus (Feuil2)
    try:
        known = [c.strip().upper() for c in get_chauffeurs()]
    except Exception:
        known = []

    # Code exact connu
    if code in known:
        return code

    # Préfixe connu (FA*, NPX → FA / NP)
    if not code[-1].isdigit():
        for k in known:
            if code.startswith(k):
                return k

    return code

def build_client_sms(row: pd.Series, tel_chauffeur: str) -> str:
    """
    Construit le message SMS/WhatsApp envoyé au client
    pour confirmer son transfert.
    """
    # DATE
    d_val = row.get("DATE", "")
    if isinstance(d_val, date):
        d_txt = d_val.strftime("%d/%m/%Y")
    else:
        try:
            d_txt = pd.to_datetime(d_val, dayfirst=True, errors="coerce").strftime("%d/%m/%Y")
        except Exception:
            d_txt = str(d_val or "").strip()

    # HEURE
    heure = normalize_time_string(row.get("HEURE", "")) or "??:??"

    # NOM client (si dispo)
    nom_client = str(row.get("NOM", "") or "").strip()
    if nom_client:
        bonjour = f"Bonjour {nom_client}, c'est Airports-Lines."
    else:
        bonjour = "Bonjour, c'est Airports-Lines."

    # Code chauffeur (CH)
    ch_code = str(row.get("CH", "") or "").strip()

    return (
        f"{bonjour}\n"
        f"Votre transfert du {d_txt} à {heure} est confirmé.\n"
        f"Votre chauffeur sera {ch_code} (GSM {tel_chauffeur}).\n"
        f"Merci pour votre confiance."
    )
def build_client_sms_from_driver(
    row: pd.Series,
    ch_code: str,
    tel_chauffeur: str,
) -> str:
    """
    Message WhatsApp envoyé par le chauffeur au client.
    """

    # DATE
    d_val = row.get("DATE", "")
    if isinstance(d_val, date):
        d_txt = d_val.strftime("%d/%m/%Y")
    else:
        try:
            d_txt = pd.to_datetime(
                d_val, dayfirst=True, errors="coerce"
            ).strftime("%d/%m/%Y")
        except Exception:
            d_txt = str(d_val or "").strip()

    # HEURE
    heure = normalize_time_string(row.get("HEURE", "")) or "??:??"

    # NOM CLIENT
    nom_client = str(row.get("NOM", "") or "").strip()
    if nom_client:
        bonjour = (
            f"Bonjour Mr/Mme {nom_client}, "
            f"c'est votre chauffeur {ch_code} pour Airports-Lines."
        )
    else:
        bonjour = (
            f"Bonjour, c'est votre chauffeur {ch_code} pour Airports-Lines."
        )

    lignes = [
        bonjour,
        f"Je serai bien à l'heure prévue le {d_txt} à {heure}.",
    ]

    if tel_chauffeur:
        lignes.append(f"Voici mon numéro : {tel_chauffeur}.")

    lignes.append("En cas de problème, n’hésitez pas à me prévenir.")

    return "\n".join(lignes)



def show_client_messages_for_period(df_base: pd.DataFrame, start: date, nb_days: int):
    """
    Prépare et affiche la liste des messages clients (WhatsApp/SMS)
    pour une période donnée à partir du planning, avec diagnostics.
    """
    end = start + timedelta(days=nb_days - 1)

    df = df_base.copy()
    if "DATE" not in df.columns:
        st.warning("La colonne DATE est manquante dans le planning, impossible de filtrer.")
        return

    # Normalisation des dates en objets date
    try:
        df["DATE_TMP"] = pd.to_datetime(df["DATE"], dayfirst=True, errors="coerce").dt.date
    except Exception:
        df["DATE_TMP"] = pd.NaT

    mask = df["DATE_TMP"].notna() & (df["DATE_TMP"] >= start) & (df["DATE_TMP"] <= end)
    df = df[mask].copy()
    df.drop(columns=["DATE_TMP"], inplace=True, errors="ignore")

    if df.empty:
        st.info("Aucune navette client sur cette période (planning vide).")
        return

    st.markdown(
        f"#### Messages clients pour la période du "
        f"{start.strftime('%d/%m/%Y')} au {end.strftime('%d/%m/%Y')}"
    )

    st.caption(f"{len(df)} ligne(s) dans le planning sur cette période (avant filtrage).")

    cols = df.columns.tolist()
    lignes_indispo = 0
    lignes_sans_tel = 0
    lignes_sans_ch_phone = 0
    lignes_affichees = 0

    for _, row in df.iterrows():
        # 1) On ignore les lignes d'indisponibilité
        if is_indispo_row(row, cols):
            lignes_indispo += 1
            continue

        # 2) Numéro client
        client_phone = get_client_phone_from_row(row)
        if not client_phone:
            lignes_sans_tel += 1
            continue

        # 3) GSM chauffeur (si absent, on affiche quand même mais sans lien WhatsApp fonctionnel)
        raw_ch_code = str(row.get("CH", "") or "").strip()

        # On normalise le code pour retrouver le bon chauffeur dans Feuil2
        norm_ch_code = normalize_ch_for_phone(raw_ch_code)
        tel_ch, _mail_ch = get_chauffeur_contact(norm_ch_code) if norm_ch_code else ("", "")
        if not tel_ch:
            lignes_sans_ch_phone += 1

        # Construire le texte du message
        msg = build_client_sms(row, tel_ch or "??")
        wa_url = build_whatsapp_link(client_phone, msg) if tel_ch else None

        # Affichage : date / heure / nom client
        date_val = row.get("DATE", "")
        if isinstance(date_val, date):
            d_txt = date_val.strftime("%d/%m/%Y")
        else:
            try:
                d_txt = pd.to_datetime(date_val, dayfirst=True, errors="coerce").strftime("%d/%m/%Y")
            except Exception:
                d_txt = str(date_val or "").strip()

        heure = normalize_time_string(row.get("HEURE", "")) or "??:??"
        nom_client = str(row.get("NOM", "") or "").strip()
        label_client = nom_client if nom_client else "(client sans nom)"

        if wa_url:
            st.markdown(
                f"- **{d_txt} {heure}** – {label_client} – CH {raw_ch_code} → "
                f"[Envoyer WhatsApp au client]({wa_url})"
            )
        else:
            st.markdown(
                f"- **{d_txt} {heure}** – {label_client} – CH {raw_ch_code} "
                f"⚠ pas de GSM chauffeur configuré (Feuil2)."
            )

        lignes_affichees += 1

    # Résumé des filtres
    st.markdown("---")
    st.caption(
        f"Résumé : {lignes_affichees} navette(s) affichée(s) • "
        f"{lignes_indispo} indispo(s) ignorée(s) • "
        f"{lignes_sans_tel} sans numéro client ('Tél') • "
        f"{lignes_sans_ch_phone} sans GSM chauffeur."
    )
    st.caption(
        "⚠ Les messages ne partent pas automatiquement : "
        "clique sur chaque lien WhatsApp pour les envoyer."
    )

import time

def silent_tab_refresh(tab_key: str, interval_sec: int = 60):
    """
    Rafraîchissement silencieux par onglet.
    Ne touche PAS à la session login.
    """
    now = time.time()

    last = st.session_state["tab_refresh"].get(tab_key, 0)

    if now - last >= interval_sec:
        st.session_state["tab_refresh"][tab_key] = now
        return True  # on recharge les données

    return False

# ============================================================
#   HELPERS — ENVOI SMTP
# ============================================================

def send_email_smtp(to_email: str, subject: str, body: str) -> bool:
    """Envoie un e-mail texte simple via SMTP. Retourne True si OK."""
    if not to_email:
        return False

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = FROM_EMAIL
        msg["To"] = to_email

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        return True

    except Exception as e:
        st.error(f"Erreur en envoyant le mail à {to_email} : {e}")
        return False


import urllib.parse

def build_outlook_mailto(to, subject, body):
    subject = urllib.parse.quote(subject, safe="")
    body = urllib.parse.quote(body, safe="")
    return f"mailto:{to}?subject={subject}&body={body}"



# ============================================================
#   RÔLES — RESTRICTION GO/GL (Pour LEON)
# ============================================================

def role_allows_go_gl_only() -> bool:
    return st.session_state.get("role") == "restricted"


def leon_allowed_for_row(go_val: str) -> bool:
    """Leon ne peut agir QUE sur GO / GL."""
    if not role_allows_go_gl_only():
        return True
    val = (go_val or "").upper().strip()
    return val in ["GO", "GL"]


# ============================================================
#   LOGOUT (DÉCONNEXION PROPRE ET SÉCURISÉE)
# ============================================================

def logout():
    """
    Déconnexion volontaire uniquement.
    Ne casse pas la session Streamlit interne.
    """
    for k in (
        "logged_in",
        "username",
        "role",
        "chauffeur_code",
    ):
        st.session_state.pop(k, None)

    st.cache_data.clear()
    st.rerun()

# ============================================================
#   TOP BAR (INFORMATIONS UTILISATEUR + DECONNEXION)
# ============================================================

def render_top_bar():
    col1, col2, col3 = st.columns([4, 3, 1])

    with col1:
        st.markdown("### 🚐 Airports-Lines — Gestion du planning")

    with col2:
        user = st.session_state.username
        role = st.session_state.role

        if user:
            if role == "admin":
                label = "Admin (accès complet)"
            elif role == "restricted":
                label = "Restreint (GO/GL uniquement)"
            elif role == "driver":
                ch = st.session_state.get("chauffeur_code")
                label = f"Chauffeur {ch}"
            else:
                label = role

            st.info(f"Connecté : **{user}** — *{label}*")

    with col3:
        if st.button("🔓 Déconnexion"):
            logout()


# ============================================================
#   STYLE PLANNING — TOUTES LES COULEURS (FINAL)
# ============================================================

def style_groupage_partage(df: pd.DataFrame):

    def style_row(row):
        styles = [""] * len(row)

        # -------------------------
        # 🔴 INDISPONIBILITÉ
        # -------------------------
        if is_indispo_row(row, df.columns.tolist()):
            styles = ["background-color: #f8d7da"] * len(row)
            return styles   # priorité absolue

        # -------------------------
        # 🟡 GROUPAGE (ligne entière)
        # -------------------------
        if int(row.get("IS_GROUPAGE", 0)) == 1:
            styles = ["background-color: #fff3cd"] * len(row)
            return styles

        # -------------------------
        # 🟡 PARTAGE (heure seule)
        # -------------------------
        if int(row.get("IS_PARTAGE", 0)) == 1 and "HEURE" in df.columns:
            idx = df.columns.get_loc("HEURE")
            styles[idx] = "background-color: #fff3cd"

        # -------------------------
        # 🟢 GO / 🔵 GL (colonne GO)
        # -------------------------
        if "GO" in df.columns:
            go_val = str(row.get("GO", "")).upper().strip()
            idx_go = df.columns.get_loc("GO")

            if go_val == "GO":
                styles[idx_go] += "; background-color: #d1e7dd; font-weight: bold"
            elif go_val == "GL":
                styles[idx_go] += "; background-color: #cfe2ff; font-weight: bold"

        # -------------------------
        # ⭐ ATTENTE (chauffeur *)
        # -------------------------
        if int(row.get("IS_ATTENTE", 0)) == 1 and "CH" in df.columns:
            idx = df.columns.get_loc("CH")
            styles[idx] += "; font-weight: bold"

        return styles

    return df.style.apply(style_row, axis=1)




# ============================================================
#   PDF CHAUFFEUR – FEUILLE DE ROUTE
# ============================================================

def create_chauffeur_pdf(df_ch: pd.DataFrame, ch_selected: str, day_label: str) -> bytes:
    """
    Génère une feuille PDF claire pour le chauffeur.
    """
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 2 * cm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(2 * cm, y, f"Feuille chauffeur — {ch_selected} — {day_label}")
    y -= 1 * cm
    c.setFont("Helvetica", 10)

    cols = df_ch.columns.tolist()

    for _, row in df_ch.iterrows():

        if y < 3 * cm:
            c.showPage()
            y = height - 2 * cm
            c.setFont("Helvetica-Bold", 14)
            c.drawString(2 * cm, y, f"Feuille chauffeur — {ch_selected} — {day_label}")
            y -= 1 * cm
            c.setFont("Helvetica", 10)

        # Indisponibilité
        if is_indispo_row(row, cols):
            heure = normalize_time_string(row.get("HEURE", ""))
            fin = normalize_time_string(row.get("²²²²", ""))
            c.drawString(2 * cm, y, f"{heure or '??:??'} → {fin or '??:??'} — 🚫 Indisponible")
            y -= 1 * cm
            continue

        # Heure
        heure = normalize_time_string(row.get("HEURE", "")) or "??:??"

        # Destination
        designation = str(row.get("DESIGNATION", "") or "").strip()
        route = ""
        for cnd in ["Unnamed: 8", "DESIGNATION"]:
            if cnd in cols and row.get(cnd):
                route = str(row[cnd]).strip()
                break

        if route and designation and designation not in route:
            dest = f"{route} ({designation})"
        else:
            dest = route or designation or "Navette"

        # Groupage / Partage
        g = bool_from_flag(row.get("GROUPAGE", "0"))
        p = bool_from_flag(row.get("PARTAGE", "0"))
        prefix = "[GRP] " if g else "[PARTAGE] " if p else ""

        # Ligne principale
        ligne1 = f"{prefix}{heure} – {dest}"

        # Nom client
        nom = str(row.get("NOM", "") or "")
        if nom:
            ligne1 += f" – {nom}"

        c.drawString(2 * cm, y, ligne1)
        y -= 0.5 * cm

        # Adresse
        adresse = str(row.get("ADRESSE", "") or "").strip()
        cp = str(row.get("CP", "") or "").strip()
        loc = str(row.get("Localité", "") or row.get("LOCALITE", "") or "").strip()
        adr_full = " ".join(x for x in [adresse, cp, loc] if x)

        if adr_full:
            c.drawString(2 * cm, y, adr_full)
            y -= 0.5 * cm

        # Vol
        infos_vol = []
        if row.get("N° Vol"): infos_vol.append(f"Vol {row.get('N° Vol')}")
        if row.get("Origine"): infos_vol.append(f"Origine {row.get('Origine')}")
        if row.get("Décollage"): infos_vol.append(f"Décollage {row.get('Décollage')}")
        if row.get("H South"): infos_vol.append(f"H SO {row.get('H South')}")
        if infos_vol:
            c.drawString(2 * cm, y, " | ".join(infos_vol))
            y -= 0.5 * cm
        # ✈️ Numéro de vol (PDF)
        vol_val = ""
        for col in ["N° Vol", "N° Vol ", "Num Vol", "VOL", "Vol"]:
            if col in df_ch.columns:
                v = str(row.get(col, "") or "").strip()
                if v:
                    vol_val = v
                    break
        
        if vol_val:
            status, delay_min, sched_dt, est_dt = get_flight_status_cached(vol_val)
            badge = flight_badge(status, delay_min)



        # Paiement / caisse
        infos_pay = []
        if row.get("PAX"): infos_pay.append(f"PAX {row.get('PAX')}")
        if row.get("PAIEMENT"): infos_pay.append(f"Paiement {row.get('PAIEMENT')}")
        if row.get("Caisse"): infos_pay.append(f"Caisse : {row.get('Caisse')} €")
        if infos_pay:
            c.drawString(2 * cm, y, " | ".join(infos_pay))

        y -= 1 * cm

    c.save()
    pdf = buffer.getvalue()
    buffer.close()
    return pdf


# ============================================================
#   MESSAGES POUR WHATSAPP / MAIL — VUE CHAUFFEUR
# ============================================================

def build_chauffeur_day_message(df_ch: pd.DataFrame, ch_selected: str, day_label: str) -> str:
    cols = df_ch.columns.tolist()
    lines = []
    lines.append(f"🚖 Planning du {day_label} — Chauffeur : {ch_selected}")
    lines.append("")

    for _, row in df_ch.iterrows():

        if is_indispo_row(row, cols):
            h1 = normalize_time_string(row.get("HEURE", ""))
            h2 = normalize_time_string(row.get("²²²²", ""))
            lines.append(f"⏱ {h1} → {h2} — 🚫 Indisponible")
            lines.append("")
            continue

        heure = normalize_time_string(row.get("HEURE", "")) or "??:??"

        designation = str(row.get("DESIGNATION", "") or "").strip()
        route = ""
        for cnd in ["Unnamed: 8", "DESIGNATION"]:
            if cnd in cols and row.get(cnd):
                route = str(row[cnd]).strip()
                break

        if route and designation and designation not in route:
            dest = f"{route} ({designation})"
        else:
            dest = route or designation or "Navette"

        dest = resolve_client_alias(dest)

        nom = str(row.get("NOM", "") or "")

        # Groupage
        g = bool_from_flag(row.get("GROUPAGE", "0"))
        p = bool_from_flag(row.get("PARTAGE", "0"))
        prefix = "[GRP] " if g else "[PARTAGE] " if p else ""

        line = f"{prefix}➡ {heure} — {dest}"
        if nom:
            line += f" — {nom}"
        lines.append(line)

        # Adresse
        adr = " ".join(
            x for x in [
                str(row.get("ADRESSE", "") or "").strip(),
                str(row.get("CP", "") or "").strip(),
                str(row.get("Localité", "") or row.get("LOCALITE", "") or "").strip(),
            ] if x
        )
        if adr:
            lines.append(f"📍 {adr}")

        # Extras
        extra = []
        if row.get("PAX"): extra.append(f"{row.get('PAX')} pax")
        if row.get("PAIEMENT"): extra.append(f"Paiement {row.get('PAIEMENT')}")
        if row.get("Caisse"): extra.append(f"Caisse {row.get('Caisse')} €")
        if extra:
            lines.append(" | ".join(extra))

        if g: lines.append("🔶 Groupage")
        if p: lines.append("🟨 Navette partagée")

        lines.append("")

    return "\n".join(lines).strip()
# ============================================================
#   ONGLET 📅 PLANNING — VUE RAPIDE AVEC COULEURS
# ============================================================

def render_tab_planning():
    st.subheader("📅 Planning — vue rapide")

    # 🔄 Rafraîchissement silencieux de l’onglet
    refresh = silent_tab_refresh("planning_rapide", interval_sec=60)
    if refresh:
        st.cache_data.clear()

    today = date.today()

    # ----------------- Raccourcis de dates -----------------
    colb1, colb2, colb3, colb4 = st.columns(4)

    with colb1:
        if st.button("📆 Aujourd’hui"):
            st.session_state.planning_start = today
            st.session_state.planning_end = today

    with colb2:
        if st.button("📆 Demain"):
            d = today + timedelta(days=1)
            st.session_state.planning_start = d
            st.session_state.planning_end = d

    with colb3:
        if st.button("📆 Cette semaine"):
            lundi = today - timedelta(days=today.weekday())
            dimanche = lundi + timedelta(days=6)
            st.session_state.planning_start = lundi
            st.session_state.planning_end = dimanche

    with colb4:
        if st.button("📆 Semaine prochaine"):
            lundi_next = today - timedelta(days=today.weekday()) + timedelta(days=7)
            dimanche_next = lundi_next + timedelta(days=6)
            st.session_state.planning_start = lundi_next
            st.session_state.planning_end = dimanche_next

    # ----------------- Sélection période -----------------
    colf1, colf2 = st.columns(2)

    with colf1:
        start_date = st.date_input(
            "Date de début",
            value=st.session_state.planning_start,
        )

    with colf2:
        end_date = st.date_input(
            "Date de fin",
            value=st.session_state.planning_end,
        )

    st.session_state.planning_start = start_date
    st.session_state.planning_end = end_date

    # ----------------- Chauffeur / type / recherche -----------------
    chs = get_chauffeurs_for_ui()

    colf3, colf4 = st.columns([1, 2])

    with colf3:
        ch_value = st.selectbox("Chauffeur (CH)", ["(Tous)"] + chs)
        if ch_value == "(Tous)":
            ch_value = None

    with colf4:
        type_choice = st.selectbox(
            "Type de transferts",
            ["Tous", "AL (hors GO/GL)", "GO / GL"],
        )

    if type_choice == "Tous":
        type_filter = None
    elif type_choice.startswith("AL"):
        type_filter = "AL"
    else:
        type_filter = "GO_GL"

    colf5, colf6 = st.columns([3, 1])

    with colf5:
        search = st.text_input(
            "Recherche (client, désignation, vol, remarque…)",
            ""
        )

    with colf6:
        sort_choice = st.selectbox(
            "Tri",
            ["Date + heure", "Chauffeur + date + heure", "Aucun"],
        )

    # ----------------- Lecture DB -----------------
    df = get_planning(
        start_date=start_date,
        end_date=end_date,
        chauffeur=ch_value,
        type_filter=type_filter,
        search=search,
        max_rows=2000,
        source="7j",
    )

    if df.empty:
        st.warning("Aucune navette pour ces paramètres.")
        return

    # ----------------- Tri -----------------
    sort_cols = []

    if sort_choice == "Date + heure":
        sort_cols = [c for c in ["DATE", "HEURE"] if c in df.columns]

    elif sort_choice == "Chauffeur + date + heure":
        sort_cols = [c for c in ["CH", "DATE", "HEURE"] if c in df.columns]

    if sort_cols:
        df = df.sort_values(sort_cols)

    # ----------------- Stats -----------------
    colm1, colm2 = st.columns(2)

    colm1.metric("🚐 Navettes", len(df))

    if "GO" in df.columns:
        nb_go_gl = df["GO"].astype(str).str.upper().isin(["GO", "GL"]).sum()
        colm2.metric("🎯 GO / GL", int(nb_go_gl))

    # ----------------- Légende couleurs -----------------
    with st.expander("ℹ️ Légende des couleurs", expanded=False):
        st.markdown("""
        🟡 **Ligne complète jaune** : navette **groupée**  
        🟡 **Heure jaune uniquement** : navette **partagée**  
        ⭐ **Chauffeur avec \\*** : aller + attente + reprise client  
        """)
    # ----------------- Préparation affichage -----------------
    df_display = df.copy()

    # retirer id de l'affichage
    if "id" in df_display.columns:
        df_display = df_display.drop(columns=["id"])

    # 🔁 mettre GO avant Num BDC
    if "GO" in df_display.columns and "Num BDC" in df_display.columns:
        cols = list(df_display.columns)
        cols.remove("GO")
        idx = cols.index("Num BDC")
        cols.insert(idx, "GO")
        df_display = df_display[cols]

    # ----------------- Style AVANT suppression des flags -----------------
    try:
        styled = style_groupage_partage(df_display)
    except Exception:
        styled = df_display

    # ----------------- Masquer colonnes techniques APRÈS style -----------------
    try:
        # pandas récents
        styled = styled.hide(
            columns=[c for c in ["IS_GROUPAGE", "IS_PARTAGE", "IS_ATTENTE"] if c in df_display.columns]
        )
    except TypeError:
        # pandas plus anciens
        styled = styled.hide(
            subset=[c for c in ["IS_GROUPAGE", "IS_PARTAGE", "IS_ATTENTE"] if c in df_display.columns],
            axis="columns"
        )

    # ----------------- Affichage tableau -----------------
    st.dataframe(styled, use_container_width=True, height=520)



def render_tab_quick_day_mobile():
    """Vue jour admin : toutes les navettes du jour (tous chauffeurs) + changement chauffeur + WhatsApp."""
    st.subheader("⚡ Vue jour (mobile) — Tous chauffeurs")

    today = date.today()
    sel_date = st.date_input(
        "Jour à afficher :",
        value=today,
        key="quick_day_date",
    )

    # 1) Charger TOUTE la journée (tous chauffeurs)
    df = get_planning(
        start_date=sel_date,
        end_date=sel_date,
        chauffeur=None,
        type_filter=None,
        search="",
        max_rows=300,
        source="day",
    )

    if df.empty:
        st.info("Aucune navette pour cette journée.")
        return
    
    df = apply_actions_overrides(df)

    df = df.copy()
    cols = df.columns.tolist()

    # 2) Liste chauffeurs pour remplacement
    chs_ui = get_chauffeurs_for_ui()
    if not chs_ui:
        chs_ui = get_chauffeurs() or CH_CODES

    # 3) Tri par heure 
    def _key_time(v):
        txt = normalize_time_string(v)  # renvoie HH:MM:SS
        if not txt:
            return datetime.max.time()
        try:
            return datetime.strptime(txt, "%H:%M:%S").time()
        except Exception:
            try:
                return datetime.strptime(txt, "%H:%M").time()
            except Exception:
                return datetime.max.time()

    if "HEURE" in df.columns:
        df["_sort_time"] = df["HEURE"].apply(_key_time)
        df = df.sort_values("_sort_time", ascending=True)

    st.markdown("### 📋 Détail des navettes (texte compact)")
    st.caption("Vue admin : toutes les navettes du jour.")

    for _, row in df.iterrows():

        # Ignorer les indispos
        if is_indispo_row(row, cols):
            continue

        # ID
        try:
            row_id = int(row.get("id"))
        except Exception:
            continue

        # Date
        date_val = row.get("DATE")
        if isinstance(date_val, (datetime, date)):
            date_txt = date_val.strftime("%d/%m/%Y")
        else:
            dtmp = pd.to_datetime(date_val, dayfirst=True, errors="coerce")
            date_txt = dtmp.strftime("%d/%m/%Y") if not pd.isna(dtmp) else ""

        # Heure
        heure_txt = normalize_time_string(row.get("HEURE", "")) or "??:??"

        # Chauffeur
        ch_current = str(row.get("CH", "") or "").strip()

        # Destination
        designation = str(row.get("DESIGNATION", "") or "").strip()
        route_txt = str(row.get("Unnamed: 8", "") or "").strip()
        dest = f"{route_txt} ({designation})" if route_txt and designation else route_txt or designation or "Navette"

        # Client
        nom = str(row.get("NOM", "") or "").strip()

        # Adresse
        adresse = str(row.get("ADRESSE", "") or "").strip()
        cp = str(row.get("CP", "") or "").strip()
        loc = str(row.get("Localité", "") or row.get("LOCALITE", "") or "").strip()
        adr_full = " ".join(x for x in [adresse, cp, loc] if x)

        # Extras
        pax = str(row.get("PAX", "") or "").strip()
        paiement = str(row.get("PAIEMENT", "") or "").strip()
        bdc = str(row.get("Num BDC", "") or "").strip()

        # ============================
        # ✈️ ALERTE VOL (ADMIN)
        # ============================
        vol = extract_vol_val(row, cols)
        badge = ""

        if vol:
            status, delay_min, sched_dt, est_dt = get_flight_status_cached(vol)
            badge = flight_badge(status, delay_min)

            if sched_dt is not None:
                sched_dt = sched_dt.replace(second=0, microsecond=0)
            if est_dt is not None:
                est_dt = est_dt.replace(second=0, microsecond=0)

            sched_txt = sched_dt.strftime("%H:%M") if sched_dt else ""
            est_txt = est_dt.strftime("%H:%M") if est_dt else ""

            ch_txt = ch_current

            if should_notify_flight_change(
                date_txt,
                ch_txt,
                vol,
                sched_txt,
                est_txt,
            ):
                msg = (
                    f"✈️ ALERTE VOL\n\n"
                    f"Vol : {vol}\n"
                    f"Date : {date_txt}\n"
                    f"Chauffeur : {ch_txt}\n\n"
                    f"Statut : {status}\n"
                    f"Heure prévue : {sched_txt or '??:??'}\n"
                    f"Heure estimée : {est_txt or '??:??'}\n"
                    f"Variation : {delay_min:+} min\n"
                )

                send_mail_admin(
                    subject=f"✈️ Changement vol {vol}",
                    body=msg,
                )

                upsert_flight_alert(
                    date_txt,
                    ch_txt,
                    vol,
                    sched_txt,
                    est_txt,
                )

        # ============================
        # AFFICHAGE LIGNE
        # ============================
        line = f"📆 {date_txt} | ⏱ {heure_txt} | 👤 {ch_current} → {dest}"
        if nom:
            line += f" | 🙂 {nom}"
        if adr_full:
            line += f" | 📍 {adr_full}"
        if vol:
            line += f" | ✈️ {vol} {badge}"
        if paiement:
            line += f" | 💳 {paiement}"
        if bdc:
            line += f" | 📄 BDC: {bdc}"
        if pax:
            line += f" | 👥 {pax} pax"

        with st.container(border=True):
            st.markdown(line)

            colA, colB, colC = st.columns([2, 1, 1])

            # Remplacement chauffeur
            with colA:
                new_ch = st.selectbox(
                    "Remplacer chauffeur",
                    chs_ui,
                    index=chs_ui.index(ch_current) if ch_current in chs_ui else 0,
                    key=f"qd_newch_{row_id}",
                )

            # Sauvegarde (journal d’actions, PAS écriture DB planning)
            with colB:
                if new_ch != ch_current:
                    if st.button("💾 Appliquer", key=f"qd_save_{row_id}"):

                        from database import log_ch_change, make_row_key_from_row

                        # clé stable basée sur la ligne Excel
                        row_key = make_row_key_from_row(row.to_dict())

                        old_ch = ch_current
                        user = (
                            st.session_state.get("username")
                            or st.session_state.get("user")
                            or ""
                        )

                        # écrire dans la DB actions (persistante)
                        log_ch_change(
                            row_key=row_key,
                            old_ch=old_ch,
                            new_ch=new_ch,
                            user=user,
                        )

                        st.warning(
                            "⚠️ Chauffeur modifié côté application.\n"
                            "📄 À reporter dans le planning Excel (Feuil1)."
                        )
                        st.rerun()
                else:
                    st.caption("")


            # WhatsApp
            with colC:
                norm_ch = normalize_ch_for_phone(new_ch or ch_current)
                tel_ch, _ = get_chauffeur_contact(norm_ch) if norm_ch else ("", "")
                if tel_ch:
                    msg = (
                        f"Bonjour {new_ch or ch_current},\n"
                        f"Navette du {date_txt} à {heure_txt}\n"
                        f"Destination : {dest}\n"
                        + (f"Client : {nom}\n" if nom else "")
                        + (f"Adresse : {adr_full}\n" if adr_full else "")
                        + (f"PAX : {pax}\n" if pax else "")
                        + (f"BDC : {bdc}\n" if bdc else "")
                        + "Merci de confirmer si problème 🙏"
                    )
                    wa = build_whatsapp_link(tel_ch, msg)
                    st.markdown(f"[💬 WhatsApp]({wa})")
                else:
                    st.caption("No GSM")





# ============================================================
#   ONGLET 📊 TABLEAU / ÉDITION — EXCEL ONLINE → DB
# ============================================================
def render_tab_table():
    st.subheader("📊 Planning — Édition Excel Online")

    st.markdown(
        "Le planning s’édite dans **Excel Online**. "
        "La base locale est synchronisée **uniquement à partir d’aujourd’hui**."
    )

    EXCEL_ONLINE_URL = (
        "https://www.dropbox.com/scl/fi/lymuumy8en46l7p0uwjj3/"
        "Planning-2026.xlsx"
        "?rlkey=sgvr0a58ekpr471p5aguqk3k8&dl=0"
    )

    # 🌐 Ouvrir Excel Online
    st.markdown(
        f"""
        <a href="{EXCEL_ONLINE_URL}" target="_blank">
            <button style="
                padding:10px 16px;
                font-size:16px;
                background-color:#0f6cbd;
                color:white;
                border:none;
                border-radius:6px;
                cursor:pointer;
            ">
                🌐 Ouvrir le planning Excel Online
            </button>
        </a>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")


# ============================================================
#   ONGLET 🔍 CLIENTS — HISTORIQUE & CRÉATION RAPIDE
# ============================================================

def render_tab_clients():
    st.subheader("🔍 Clients — Historique & création rapide")

    query = st.text_input(
        "Nom du client (ou partie du nom)",
        "",
        key="client_search",
    )

    if not query.strip():
        st.info("Tape un nom de client pour afficher son historique.")
        return

    df = search_client(query, max_rows=500)
    if df.empty:
        st.warning("Aucune navette trouvée pour ce client.")
        return

    if "id" not in df.columns:
        st.error("La table `planning` doit contenir une colonne `id`.")
        return

    # max 40 colonnes
    if df.shape[1] > 40:
        df = df.iloc[:, :40]

    st.markdown(f"#### {len(df)} navette(s) trouvée(s)")

    df_display = df.copy()
    df_display = df_display.drop(columns=["id"])
    st.dataframe(df_display, use_container_width=True, height=400)

    # Sélection d’une navette modèle
    ids = df["id"].tolist()
    df_view = df.drop(columns=["id"]).copy().reset_index(drop=True)
    df_view.insert(0, "_SELECT", False)
    if "KM_EST" in df.columns:
        df_view["_KM_EST"] = df["KM_EST"].fillna("").astype(str)
    if "TEMPS_EST" in df.columns:
        df_view["_TEMPS_EST"] = df["TEMPS_EST"].fillna("").astype(str)
    # --- Affichage KM / TEMPS depuis la DB ---
    if "KM_EST" in df.columns:
        df_view["_KM_EST"] = df["KM_EST"].fillna("").astype(str)
    else:
        df_view["_KM_EST"] = ""

    if "TEMPS_EST" in df.columns:
        df_view["_TEMPS_EST"] = df["TEMPS_EST"].fillna("").astype(str)
    else:
        df_view["_TEMPS_EST"] = ""

    # Injecter KM / MIN si on a déjà calculé
    km_map = st.session_state.get("km_time_by_id", {}) or {}
    km_col = []
    min_col = []
    for rid in ids:
        km, mn = km_map.get(int(rid), (None, None))
        km_col.append("" if km is None else f"{km} km")
        min_col.append("" if mn is None else f"{mn} min")

    # Colonnes d'affichage (préfixe "_" pour éviter confusion avec colonnes Excel)
    df_view["_KM_EST"] = km_col
    df_view["_TEMPS_EST"] = min_col

    st.markdown("#### Sélectionne une navette modèle")
    edited = st.data_editor(
        df_view,
        use_container_width=True,
        height=300,
        num_rows="fixed",
        key="client_editor",
    )
    # ==================================================
    # D) Exécuter le calcul KM / TEMPS (à la demande)
    # ==================================================
    if st.session_state.get("km_time_run"):
        selected_indices = edited.index[edited["_SELECT"] == True].tolist()
        selected_ids = [int(ids[i]) for i in selected_indices]

        mode = st.session_state.get("km_time_last_mode", "✅ Lignes cochées (_SELECT)")
        targets = selected_ids if mode.startswith("✅") else [int(x) for x in ids]

        for rid in targets:
            row = df[df["id"] == rid].iloc[0]

            if row.get("KM_EST") and row.get("TEMPS_EST"):
                continue

            origin = (
                build_full_address_from_row(row)
                or st.session_state.get("km_base_address", "Liège, Belgique")
            )
            dest = resolve_destination_text(row)

            km, mn = ors_route_km_min(origin, dest)
            if km is not None and mn is not None:
                update_planning_row(
                    rid,
                    {
                        "KM_EST": str(km),
                        "TEMPS_EST": str(mn),
                    }
                )

        # ✅ CES LIGNES DOIVENT ÊTRE ICI
        st.session_state["km_time_run"] = False
        st.success("KM et temps calculés et sauvegardés ✅")
        st.rerun()

  
        # 🔒 IMPORTANT : couper le flag AVANT rerun
        st.session_state["km_time_run"] = False
        st.session_state["km_time_last_mode"] = None

        st.success("KM et temps calculés et sauvegardés ✅")

        # rerun propre (une seule fois)
        st.experimental_rerun()




    selected_indices = edited.index[edited["_SELECT"] == True].tolist()
    if selected_indices:
        selected_idx = selected_indices[-1]
    else:
        selected_idx = 0

    selected_id = int(ids[selected_idx])
    base_row = get_row_by_id(selected_id)
    if base_row is None:
        st.error("Navette modèle introuvable.")
        return

    st.markdown("### 📝 Créer / modifier à partir du modèle")

    cols_names = get_planning_columns()
    cols_names = cols_names[:40]

    new_values: Dict[str, Any] = {}
    cL, cR = st.columns(2)
    today = date.today()

    for i, col_name in enumerate(cols_names):
        cont = cL if i % 2 == 0 else cR
        val = base_row.get(col_name)

        # DATE
        if col_name == "DATE":
            default_date = today
            if isinstance(val, str) and val:
                try:
                    default_date = datetime.strptime(val, "%d/%m/%Y").date()
                except Exception:
                    pass
            new_d = cont.date_input(
                "DATE",
                value=default_date,
                key=f"client_DATE_{selected_id}",
            )
            new_values[col_name] = new_d.strftime("%d/%m/%Y")
            continue

        # GROUPAGE / PARTAGE
        if col_name in ["GROUPAGE", "PARTAGE"]:
            b = cont.checkbox(
                "Groupage" if col_name == "GROUPAGE" else "Navette partagée",
                value=bool_from_flag(val),
                key=f"client_{col_name}_{selected_id}",
            )
            new_values[col_name] = "1" if b else "0"
            continue

        # GO
        if col_name == "GO":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "GO (AL / GO / GL)",
                value=txt,
                key=f"client_GO_{selected_id}",
            )
            new_values[col_name] = t2.strip().upper()
            continue

        # HEURE
        if col_name == "HEURE":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "HEURE",
                value=txt,
                key=f"client_HEURE_{selected_id}",
            )
            new_values[col_name] = normalize_time_string(t2)
            continue

        # HEURE FIN (²²²²)
        if col_name == "²²²²":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "Heure fin (²²²²)",
                value=txt,
                key=f"client_2222_{selected_id}",
            )
            new_values[col_name] = normalize_time_string(t2)
            continue

        txt = "" if val is None or str(val).lower() == "nan" else str(val)
        t2 = cont.text_input(col_name, value=txt, key=f"client_{col_name}_{selected_id}")
        new_values[col_name] = t2

    role = st.session_state.role

    c1, c2 = st.columns(2)

    with c1:
        if st.button("➕ Créer une nouvelle navette pour ce client"):
            if role_allows_go_gl_only() and not leon_allowed_for_row(new_values.get("GO")):
                st.error("Utilisateur 'leon' : création autorisée uniquement pour GO / GL.")
            else:
                insert_planning_row(new_values)
                st.success("Nouvelle navette créée.")
                st.rerun()

    with c2:
        if st.button("✅ Mettre à jour la navette existante"):
            if role_allows_go_gl_only() and not leon_allowed_for_row(base_row.get("GO")):
                st.error("Utilisateur 'leon' : modification autorisée uniquement pour GO / GL.")
            else:
                update_planning_row(selected_id, new_values)
                st.success("Navette mise à jour.")
                st.rerun()

    st.markdown("---")
    st.markdown("### 🔁 Créer un RETOUR à partir de ce modèle")

    retour_data = new_values.copy()
    colR1, colR2 = st.columns(2)
    with colR1:
        retour_date = st.date_input(
            "Date du RETOUR",
            value=today,
            key=f"client_retour_DATE_{selected_id}",
        )
    with colR2:
        retour_heure = st.text_input(
            "Heure du RETOUR",
            value="",
            key=f"client_retour_HEURE_{selected_id}",
        )

    retour_data["DATE"] = retour_date.strftime("%d/%m/%Y")
    if "HEURE" in retour_data:
        retour_data["HEURE"] = normalize_time_string(retour_heure)

    if st.button("📋 Créer un RETOUR (copie modifiable)"):
        if role_allows_go_gl_only() and not leon_allowed_for_row(retour_data.get("GO")):
            st.error("Utilisateur 'leon' : création autorisée uniquement pour GO / GL.")
        else:
            insert_planning_row(retour_data)
            st.success("Navette RETOUR créée.")
            st.rerun()

# ============================================================
#   OUTILS CHAUFFEURS — CONTACTS, STATS, TRI
# ============================================================

def get_chauffeur_contact(ch: str):
    """Récupère téléphone + mail du chauffeur via table `chauffeurs` (Feuil2)."""
    tel = ""
    mail = ""
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM chauffeurs WHERE TRIM(INITIALE) = ? LIMIT 1", (ch,))
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                data = {cols[i]: row[i] for i in range(len(cols))}
                tel = (
                    data.get("TEL_CH")
                    or data.get("TEL")
                    or data.get("Tél")
                    or data.get("PHONE")
                    or ""
                )
                mail = data.get("MAIL") or data.get("Email") or ""
    except Exception:
        pass
    return str(tel or ""), str(mail or "")


def render_chauffeur_stats(df_ch: pd.DataFrame):
    """Affiche navettes / PAX / caisse pour un chauffeur."""
    if df_ch is None or df_ch.empty:
        return

    cols = df_ch.columns
    mask_course = ~df_ch.apply(lambda r: is_indispo_row(r, cols), axis=1)
    df_course = df_ch[mask_course].copy()

    nb_nav = len(df_course)
    pax_total = pd.to_numeric(df_course.get("PAX", 0), errors="coerce").fillna(0).sum()
    caisse_total = pd.to_numeric(df_course.get("Caisse", 0), errors="coerce").fillna(0).sum()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🚐 Navettes (hors indispo)", int(nb_nav))
    with c2:
        st.metric("👥 PAX total", int(pax_total))
    with c3:
        st.metric("💶 Caisse totale", float(caisse_total))

# ============================================================
#   ENVOI PLANNING AUX CHAUFFEURS (MAIL + WHATSAPP)
# ============================================================

def send_planning_to_chauffeurs(
    chauffeurs: list[str],
    from_date: date,
    to_date: date | None = None,
    message_type: str = "planning",
):
    """
    Envoie à chaque chauffeur un mail avec SON planning individuel
    et prépare les liens WhatsApp.
    """

    if not chauffeurs:
        st.warning("Aucun chauffeur sélectionné.")
        return

    df_all = get_planning(
        start_date=from_date,
        end_date=to_date,
        max_rows=5000,
        source="7j",
    )

    if df_all.empty:
        st.warning("Aucune navette sur la période sélectionnée.")
        return

    sent = 0
    no_email: list[str] = []
    wa_links: list[dict] = []

    for ch in chauffeurs:

        tel, mail = get_chauffeur_contact(ch)
        ch_norm = normalize_ch_code(ch)

        df_ch = df_all[
            df_all["CH"].astype(str).apply(normalize_ch_code) == ch_norm
        ]

        if df_ch.empty:
            continue

        # ---------------- MAIL ----------------
        if message_type == "planning":
            subject = f"🚖 Planning — {ch} ({from_date.strftime('%d/%m/%Y')})"
            msg_txt = build_planning_mail_body(
                df_ch=df_ch,
                ch=ch,
                from_date=from_date,
                to_date=to_date,
            )
        else:
            subject = f"📢 Modification planning — {ch}"
            msg_txt = (
                "Bonjour,\n\n"
                "📢 Une modification de planning a été effectuée aujourd’hui.\n"
                "Merci de consulter l’application Airports Lines "
                "et de confirmer la réception.\n\n"
                "— Airports Lines"
            )

        if mail:
            if send_email_smtp(mail, subject, msg_txt):
                sent += 1
        else:
            no_email.append(ch)

        # ---------------- WHATSAPP ----------------
        if tel:
            wa_msg = build_chauffeur_new_planning_message(ch, from_date)
            wa_url = build_whatsapp_link(tel, wa_msg)
            wa_links.append({
                "ch": ch,
                "tel": tel,
                "url": wa_url,
            })

    st.success(f"📧 Emails envoyés pour {sent} chauffeur(s).")

    if no_email:
        st.info(
            "📭 Pas d'adresse email configurée pour : "
            + ", ".join(sorted(no_email))
        )

    if wa_links:
        st.markdown("### 💬 Prévenir les chauffeurs par WhatsApp")
        st.caption("Clique sur un lien pour ouvrir WhatsApp avec le message pré-rempli.")

        for item in wa_links:
            st.markdown(
                f"- {item['ch']} ({item['tel']}) → "
                f"[Envoyer WhatsApp]({item['url']})"
            )


    # ===================================================
    # RETOUR UI
    # ===================================================
    st.success(f"📧 Emails envoyés pour {sent} chauffeur(s).")

    if no_email:
        st.info(
            "📭 Pas d'adresse email configurée pour : "
            + ", ".join(sorted(no_email))
        )

    if wa_links:
        st.markdown("### 💬 Prévenir les chauffeurs par WhatsApp")
        st.caption(
            "Clique sur un lien pour ouvrir WhatsApp avec le message pré-rempli."
        )

        for item in wa_links:
            st.markdown(
                f"- {item['ch']} ({item['tel']}) → "
                f"[Envoyer WhatsApp]({item['url']})"
            )




def _sort_df_by_date_heure(df: pd.DataFrame) -> pd.DataFrame:
    """Tri par DATE + HEURE (normalisée)."""
    df = df.copy()

    if "DATE" in df.columns:
        try:
            df["DATE_SORT"] = pd.to_datetime(df["DATE"], errors="coerce")
        except Exception:
            df["DATE_SORT"] = pd.NaT
    else:
        df["DATE_SORT"] = pd.NaT

    if "HEURE" in df.columns:
        def _hs(h):
            h = normalize_time_string(h)
            if not h:
                return (99, 99)
            try:
                parts = h.split(":")
                if len(parts) != 2:
                    return (99, 99)
                return (int(parts[0]), int(parts[1]))
            except Exception:
                return (99, 99)
        df["HEURE_SORT"] = df["HEURE"].apply(_hs)
    else:
        df["HEURE_SORT"] = (99, 99)

    df = df.sort_values(["DATE_SORT", "HEURE_SORT"]).drop(
        columns=["DATE_SORT", "HEURE_SORT"],
        errors="ignore",
    )
    return df

def make_row_key_from_row(row: dict) -> str:
    """
    Génère une clé stable pour une navette,
    indépendante de l'ID DB et résistante aux sync.
    """
    parts = [
        str(row.get("DATE", "")).strip(),
        (normalize_time_string(row.get("HEURE", "")) or "").strip(),
        str(row.get("CH", "")).strip(),
        str(row.get("DESIGNATION", "")).strip(),
        str(row.get("NOM", "")).strip(),
    ]
    return "|".join(parts)


def build_chauffeur_future_message(df: pd.DataFrame, ch_selected: str, from_date: date) -> str:
    lines: List[str] = []
    lines.append(f"🚖 Planning à partir du {from_date.strftime('%d/%m/%Y')} — Chauffeur : {ch_selected}")
    lines.append("")

    df = df.copy()
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
        df = df[df["DATE"].notna() & (df["DATE"] >= from_date)]

    if df.empty:
        lines.append("Aucune navette planifiée.")
        return "\n".join(lines)

    df = df[df["CH"].astype(str).str.upper() == ch_selected.upper()]
    if df.empty:
        lines.append("Aucune navette pour ce chauffeur.")
        return "\n".join(lines)

    df = _sort_df_by_date_heure(df)
    cols = df.columns.tolist()

    for d, sub in df.groupby("DATE"):
        lines.append(f"📆 {d.strftime('%d/%m/%Y')}")

        for _, row in sub.iterrows():

            if is_indispo_row(row, cols):
                h1 = normalize_time_string(row.get("HEURE"))
                h2 = normalize_time_string(row.get("²²²²"))
                lines.append(f"  ⏱ {h1 or '??:??'} → {h2 or '??:??'} — 🚫 Indisponible")
                lines.append("")
                continue

            heure = normalize_time_string(row.get("HEURE")) or "??:??"

            sens_txt = format_sens_ar(row.get("Unnamed: 8"))
            dest = resolve_client_alias(str(row.get("DESIGNATION", "") or "").strip())
            sens_dest = f"{sens_txt} ({dest})" if sens_txt and dest else dest or sens_txt or "Navette"

            nom = str(row.get("NOM", "") or "").strip()

            lines.append(f"  ➡ {heure} — {sens_dest} — {nom}")

            adr = build_full_address_from_row(row)
            if adr:
                lines.append(f"     📍 {adr}")

            extras = []
            if row.get("PAX"):
                extras.append(f"{row.get('PAX')} pax")

            paiement = str(row.get("PAIEMENT", "") or "").lower()
            caisse = row.get("Caisse")
            if paiement == "facture":
                extras.append("Facture")
            elif paiement in ("caisse", "bancontact"):
                extras.append(f"{paiement} {caisse}€" if caisse else paiement)

            if extras:
                lines.append("     " + " — ".join(extras))

            lines.append("")
        lines.append("")

    return "\n".join(lines).strip()


def build_chauffeur_new_planning_message(ch: str, from_date: date) -> str:
    """
    Petit message WhatsApp pour dire au chauffeur qu'il a un nouveau planning.
    """
    d_txt = from_date.strftime("%d/%m/%Y")
    return (
        f"Bonjour {ch}, c'est Airports-Lines.\n"
        f"Ton planning a été mis à jour à partir du {d_txt}.\n"
        f"Les courses modifiées sont indiquées dans ta vue chauffeur.\n\n"
        f"Merci de te connecter à l'application et de cliquer sur "
        f"« J'ai bien reçu mon planning » pour confirmer. 👍"
    )

def build_chauffeur_day_message(df_ch: pd.DataFrame, ch_selected: str, day_label: str) -> str:
    cols = df_ch.columns.tolist()
    lines = []

    lines.append(f"🚖 Planning à partir du {day_label} — Chauffeur : {ch_selected}")
    lines.append("")

    for _, row in df_ch.iterrows():

        if is_indispo_row(row, cols):
            h1 = normalize_time_string(row.get("HEURE")) or "??:??"
            h2 = normalize_time_string(row.get("²²²²")) or "??:??"
            lines.append(f"⏱ {h1} → {h2} — 🚫 Indisponible")
            lines.append("")
            continue

        heure = normalize_time_string(row.get("HEURE")) or "??:??"

        sens_txt = format_sens_ar(row.get("Unnamed: 8"))
        dest = resolve_client_alias(resolve_destination_text(row))
        sens_dest = f"{sens_txt} ({dest})" if sens_txt and dest else dest or sens_txt or "Navette"

        nom = str(row.get("NOM", "") or "").strip()
        lines.append(f"  ➡ {heure} — {sens_dest} — {nom}")

        adr = build_full_address_from_row(row)
        if adr:
            lines.append(f"     📍 {adr}")

        tel = get_client_phone_from_row(row)
        if tel:
            lines.append(f"     📞 Client : {tel}")

        vol = extract_vol_val(row, cols)
        if vol:
            lines.append(f"     ✈️ Vol : {vol}")

        extras = []
        if row.get("PAX"):
            extras.append(f"{row.get('PAX')} pax")

        paiement = str(row.get("PAIEMENT", "") or "").lower()
        caisse = row.get("Caisse")
        if paiement == "facture":
            extras.append("Facture")
        elif paiement in ("caisse", "bancontact"):
            extras.append(f"{paiement} {caisse}€" if caisse else paiement)

        if extras:
            lines.append("     " + " — ".join(extras))

        lines.append("")

    return "\n".join(lines).strip()



# ============================================================
#   ONGLET 🚖 VUE CHAUFFEUR (PC + GSM)
# ============================================================

def render_tab_vue_chauffeur(forced_ch=None):
    from streamlit_autorefresh import st_autorefresh

    # 🔁 Rafraîchissement automatique (relance la vue)
    AUTO_REFRESH_MINUTES = 5
    st_autorefresh(
        interval=AUTO_REFRESH_MINUTES * 60 * 1000,
        key="auto_refresh_vue_chauffeur",
    )

    # 🔍 Auto-sync si le fichier Dropbox a changé
    last_dbx_mtime = get_dropbox_file_last_modified()
    last_known = st.session_state.get("last_dropbox_mtime")

    if last_dbx_mtime and last_dbx_mtime != last_known:
        with st.spinner("🔁 Planning mis à jour — actualisation automatique…"):
            sync_planning_from_today()
        st.session_state["last_dropbox_mtime"] = last_dbx_mtime

    st.subheader("🚖 Vue Chauffeur (texte compact)")

    chs = get_chauffeurs_for_ui()

    # ============================
    #   CHOIX DU CHAUFFEUR
    # ============================
    if forced_ch:
        ch_selected = forced_ch
        st.markdown(f"Chauffeur connecté : **{ch_selected}**")
    else:
        ch_selected = st.selectbox(
            "Choisir un chauffeur (CH) (laisser vide pour tous les chauffeurs)",
            [""] + chs,
            key="vue_chauffeur_ch",
        )

    today = date.today()

    # ============================
    #   MODE TOUS LES CHAUFFEURS
    # ============================
    mode_all = False

    if not ch_selected and not forced_ch:
        if st.session_state.get("role") == "admin":
            mode_all = True
            st.info("Mode tous les chauffeurs")
        else:
            st.info("Sélectionne un chauffeur")
            return

    # ============================
    #   CHARGEMENT DU PLANNING
    # ============================
    if mode_all:
        # ----------------------------
        # ADMIN : TOUS LES CHAUFFEURS
        # ----------------------------
        df_ch = get_planning(
            start_date=today,
            end_date=today + timedelta(days=6),
            chauffeur=None,
            type_filter=None,
            search="",
            max_rows=5000,
            source="7j",
        )

        tel_ch = None
        mail_ch = None
        last_ack = None

    else:
        # ----------------------------
        # MODE CHAUFFEUR UNIQUE
        # ----------------------------
        tel_ch, mail_ch = get_chauffeur_contact(ch_selected)
        last_ack = get_chauffeur_last_ack(ch_selected)

        df_ch = get_chauffeur_planning(
            ch_selected,
            from_date=today,
            to_date=today + timedelta(days=6),
        )

    if df_ch is None or df_ch.empty:
        st.warning("Aucune navette.")
        return
    # =======================================================
    #   📢 ENVOI DU PLANNING (ADMIN)
    # =======================================================
    if st.session_state.get("role") == "admin":
        st.markdown("---")
        st.markdown("### 📢 Envoi du planning")

        ensure_send_log_table()

        # ---------------------------
        # Choix période
        # ---------------------------
        periode = st.radio(
            "📅 Quelle période envoyer ?",
            ["Aujourd’hui", "Demain + 2 jours"],
            horizontal=True,
            key="send_planning_periode",
        )

        if periode == "Aujourd’hui":
            d_start = today
            d_end = today
            periode_label = "du jour"
        else:
            d_start = today + timedelta(days=1)
            d_end = today + timedelta(days=3)
            periode_label = "de demain à J+3"

        # ---------------------------
        # Choix destinataire
        # ---------------------------
        ch_choice = st.radio(
            "🚖 Destinataire",
            ["Tous les chauffeurs", "Un chauffeur"],
            horizontal=True,
            key="send_planning_target",
        )

        if ch_choice == "Un chauffeur":
            ch_target = st.selectbox(
                "Sélectionner le chauffeur",
                sorted(df_ch["CH"].dropna().unique().tolist()),
                key="send_planning_one_ch",
            )

            base = ch_target.strip().upper()
            chauffeurs = [
                ch for ch in
                df_ch["CH"].dropna().astype(str).str.upper().unique()
                if base in ch
            ]
        else:
            chauffeurs = sorted(
                df_ch["CH"].dropna().astype(str).str.upper().unique()
            )

        col_mail, col_wa = st.columns(2)

        # =========
        # 📧 MAIL AUTO
        # =========
        with col_mail:
            if st.button("📧 Envoyer le planning", use_container_width=True):

                errors = []
                sent_once = set()

                if not chauffeurs:
                    st.warning("Aucun chauffeur à notifier.")
                else:
                    for ch_raw in chauffeurs:
                        ch = normalize_ch_code(ch_raw)

                        if not ch:
                            continue

                        if ch in sent_once:
                            continue

                        sent_once.add(ch)

                        try:
                            tel, mail = get_chauffeur_contact(ch)

                            if not mail:
                                raise ValueError("Email manquant")

                            send_planning_to_chauffeurs(
                                chauffeurs=[ch],
                                from_date=d_start,
                                to_date=d_end,
                                message_type="planning",
                            )

                            log_send(ch, "MAIL", periode_label, "OK", "Envoyé")

                        except Exception as e:
                            msg = str(e)
                            log_send(ch, "MAIL", periode_label, "ERREUR", msg)
                            errors.append((ch, msg))

                if errors:
                    st.error("❌ Certains envois ont échoué")
                    for ch, msg in errors:
                        st.write(f"- {ch} : {msg}")
                else:
                    st.success(f"✅ Planning {periode_label} envoyé")


        # ---------------------------
        # 📨 ENVOI MANUEL VIA OUTLOOK
        # ---------------------------
        if chauffeurs:
            st.markdown("")
            if st.button("📨 Envoyer manuellement via Outlook", use_container_width=True):

                st.markdown("### 📨 Envoi manuel via Outlook")

                for ch in chauffeurs:
                    tel, mail = get_chauffeur_contact(ch)

                    # ⛔ Sécurité : pas d'email → pas de bouton
                    if not mail:
                        st.warning(
                            f"⚠️ {ch} : email manquant → impossible d’ouvrir Outlook"
                        )
                        continue

                    body = build_planning_mail_body(
                        df_ch=df_ch,
                        ch=ch,
                        from_date=d_start,
                        to_date=d_end,
                    )

                    mailto = build_outlook_mailto(
                        to=mail,
                        subject=f"Planning {periode_label}",
                        body=body,
                    )

                    col1, col2 = st.columns([1, 3])
                    with col1:
                        st.write(f"👉 **{ch}**")
                    with col2:
                        st.link_button(
                            "📨 Ouvrir Outlook",
                            mailto,
                        )



        # =========
        # 💬 WHATSAPP
        # =========
        with col_wa:
            if st.button("💬 Envoyer par WhatsApp", use_container_width=True):

                if not chauffeurs:
                    st.warning("Aucun chauffeur à notifier.")
                else:
                    wa_links = []

                    df_all = get_planning(
                        start_date=d_start,
                        end_date=d_end,
                        max_rows=5000,
                        source="7j",
                    )

                    for ch in chauffeurs:
                        tel, _ = get_chauffeur_contact(ch)
                        if not tel:
                            continue

                        df_ch_wa = df_all[
                            df_all["CH"]
                            .astype(str)
                            .str.upper()
                            .str.contains(ch, na=False)
                        ]

                        if df_ch_wa.empty:
                            continue

                        wa_text = build_planning_mail_body(
                            df_ch=df_ch_wa,
                            ch=ch,
                            from_date=d_start,
                            to_date=d_end,
                        )

                        wa_url = build_whatsapp_link(tel, wa_text)

                        wa_links.append({
                            "ch": ch,
                            "tel": tel,
                            "url": wa_url,
                        })

                    if not wa_links:
                        st.warning("Aucun numéro WhatsApp disponible.")
                    else:
                        st.markdown("### 💬 Envoi WhatsApp")
                        st.caption(
                            "Clique sur un lien pour ouvrir WhatsApp avec le message prêt à envoyer."
                        )

                        for item in wa_links:
                            st.markdown(
                                f"- **{item['ch']}** ({item['tel']}) → "
                                f"[📲 Ouvrir WhatsApp]({item['url']})"
                            )

        # ===================================================
        #   📊 HISTORIQUE DES ENVOIS
        # ===================================================
        st.markdown("---")
        st.markdown("### 📊 Historique des envois")

        with st.expander("🧹 Gestion de l’historique"):
            st.warning("Cette action supprime définitivement l’historique.")
            if st.button("🗑️ Vider l’historique des envois"):
                with get_connection() as conn:
                    conn.execute("DELETE FROM send_log")
                    conn.commit()
                st.success("✅ Historique supprimé.")
                st.rerun()

        with get_connection() as conn:
            df_log = pd.read_sql_query(
                """
                SELECT ts, chauffeur, canal, periode, statut, message
                FROM send_log
                ORDER BY ts DESC
                LIMIT 100
                """,
                conn,
            )

        st.dataframe(df_log, use_container_width=True)






    # =======================================================
    #   📊 STATUT CONFIRMATION PAR CHAUFFEUR (ADMIN)
    # =======================================================
    if mode_all and st.session_state.get("role") == "admin":
        st.markdown("---")
        st.markdown("### 📊 Statut des chauffeurs")

        chauffeurs = sorted(df_ch["CH"].dropna().unique().tolist())

        status_rows = []

        for ch in chauffeurs:
            last_ack = get_chauffeur_last_ack(ch)

            status_rows.append({
                "Chauffeur": ch,
                "Statut": "🟢 Confirmé" if last_ack else "🔴 Non confirmé",
                "Dernière confirmation": (
                    last_ack.strftime("%d/%m/%Y %H:%M")
                    if last_ack else "—"
                ),
            })

        st.dataframe(
            pd.DataFrame(status_rows),
            use_container_width=True,
            hide_index=True,
        )

        # ===================================================
        #   ⏰ RAPPEL AUX CHAUFFEURS NON CONFIRMÉS
        # ===================================================
        if st.button("⏰ Rappel aux chauffeurs non confirmés"):

            chauffeurs = sorted(
                df_ch["CH"].dropna().unique().tolist()
            )

            non_confirmes = [
                ch for ch in chauffeurs
                if not get_chauffeur_last_ack(ch)
            ]

            if not non_confirmes:
                st.success("✅ Tous les chauffeurs ont confirmé leur planning.")
            else:
                send_planning_to_chauffeurs(
                    chauffeurs=non_confirmes,
                    from_date=today,
                    to_date=None,
                    message_type="modification",
                )

                st.success(
                    f"⏰ Rappel envoyé à {len(non_confirmes)} chauffeur(s) non confirmé(s)."
                )
                st.rerun()



    # =======================================================
    #   CHOIX DE LA PÉRIODE (CLAIR POUR LE CHAUFFEUR)
    # =======================================================
    scope = st.radio(
        "📅 Quelles navettes veux-tu voir ?",
        ["Navettes du jour", "Navettes à partir de demain"],
        index=0,
        horizontal=True,
        key="vue_chauffeur_scope",
    )

    if scope == "Navettes du jour":
        sel_date = today
        scope_label = sel_date.strftime("%d/%m/%Y")

        df_ch = get_chauffeur_planning(
            ch_selected,
            from_date=sel_date,
            to_date=sel_date,
        )

    else:
        sel_date = today + timedelta(days=1)
        scope_label = f"à partir du {sel_date.strftime('%d/%m/%Y')}"

        df_ch = get_chauffeur_planning(
            ch_selected,
            from_date=sel_date,
            to_date=None,
        )

    if df_ch.empty:
        st.warning(f"Aucune navette {scope_label}.")
        return

    df_ch = _sort_df_by_date_heure(df_ch)
    render_chauffeur_stats(df_ch)

    # =======================================================
    #   CONFIRMATION GLOBALE DU CHAUFFEUR
    #   (envoi de TOUT ce qui a été encodé)
    # =======================================================
    st.markdown("---")
    st.markdown("### ✅ Envoyer ma confirmation au bureau")

    missing = []
    recap_lines = []

    for _, row in df_ch.iterrows():
        nav_id = row.get("id")

        trajet = st.session_state.get(f"trajet_nav_{nav_id}", "").strip()
        probleme = st.session_state.get(f"prob_nav_{nav_id}", "").strip()

        if not trajet:
            missing.append(nav_id)

        recap_lines.append(
            f"Navette ID {nav_id}\n"
            f"Chauffeur : {ch_selected}\n"
            f"Trajet : {trajet or '❌ NON RENSEIGNÉ'}\n"
            f"Problème : {probleme or '—'}\n"
            "-----------------------------"
        )

    if missing:
        st.error(
            f"❌ {len(missing)} navette(s) sans trajet renseigné. "
            "Merci de compléter toutes les lignes avant l’envoi."
        )

    if st.button(
        "📤 Envoyer ma confirmation et mes remarques",
        disabled=bool(missing),
        key=f"confirm_all_{ch_selected}",
    ):
        try:
            send_mail_admin(
                subject=f"[CONFIRMATION CHAUFFEUR] {ch_selected}",
                body=(
                    f"Confirmation du chauffeur {ch_selected}\n\n"
                    + "\n".join(recap_lines)
                ),
            )

            # Marquer comme confirmé
            set_chauffeur_last_ack(ch_selected)

            st.success("✅ Confirmation envoyée au bureau. Merci 👍")
            st.rerun()

        except Exception as e:
            st.error(f"Erreur lors de l’envoi : {e}")


    # =======================================================
    #   DÉTAIL DES NAVETTES (TEXTE COMPACT)
    # =======================================================
    if df_ch is None or df_ch.empty:
        st.info("Aucune navette pour cette période.")

    else:
        st.markdown("---")
        st.markdown("### 📋 Détail des navettes (texte compact)")
        st.caption(
            "Les lignes marquées 🆕 sont celles modifiées depuis ta dernière confirmation."
        )

        cols = df_ch.columns.tolist()

        for _, row in df_ch.iterrows():

            # ===================================================
            # INITIALISATION (OBLIGATOIRE)
            # ===================================================
            bloc_lines = []

            nav_id = row.get("id")
            is_new = bool(row.get("IS_NEW", False))
            heure_txt = normalize_time_string(row.get("HEURE", "")) or "??:??"

            # ===================================================
            # Groupage / Partage / Attente
            # ===================================================
            is_groupage = int(row.get("IS_GROUPAGE", 0) or 0) == 1
            is_partage = int(row.get("IS_PARTAGE", 0) or 0) == 1
            is_attente = int(row.get("IS_ATTENTE", 0) or 0) == 1

            prefix = ""
            if is_groupage:
                prefix = "🟡 [GROUPÉE] "
            elif is_partage:
                prefix = "🟡 [PARTAGÉE] "
            if is_attente:
                prefix += "⭐ "

            # ===================================================
            # Date
            # ===================================================
            date_val = row.get("DATE", "")
            if isinstance(date_val, (datetime, date)):
                date_obj = date_val
            else:
                date_obj = pd.to_datetime(
                    date_val, dayfirst=True, errors="coerce"
                )

            date_txt = (
                date_obj.strftime("%d/%m/%Y")
                if not pd.isna(date_obj)
                else ""
            )

            # ===================================================
            # Indisponibilité
            # ===================================================
            if is_indispo_row(row, cols):
                end_indispo = (
                    normalize_time_string(row.get("²²²²", "")) or "??:??"
                )
                bloc_lines.append(
                    f"📆 {date_txt} | ⏱ {heure_txt} → {end_indispo} | 🚫 Indisponible"
                )
                bloc_lines.append(
                    f"👨‍✈️ {row.get('CH', ch_selected)}"
                )
                st.markdown("<br>".join(bloc_lines), unsafe_allow_html=True)
                st.markdown("---")
                continue

            # ===================================================
            # HEADER
            # ===================================================
            header = ""
            if is_new:
                header += "🆕 "
            header += prefix
            header += f"📆 {date_txt} | ⏱ {heure_txt}"
            bloc_lines.append(header)

            # ===================================================
            # Chauffeur
            # ===================================================
            bloc_lines.append(
                f"👨‍✈️ {row.get('CH', ch_selected)}"
            )

            # ===================================================
            # Sens / Destination (DE / VERS + BRU / CRL / etc.)
            # ===================================================
            sens_txt = format_sens_ar(row.get("Unnamed: 8"))

            dest_raw = ""
            for cand in ["DESIGNATION", "DESTINATION", "DE/VERS"]:
                if cand in cols and row.get(cand):
                    dest_raw = str(row.get(cand)).strip()
                    if dest_raw:
                        break

            dest = resolve_client_alias(dest_raw)

            if sens_txt and dest:
                bloc_lines.append(f"➡ {sens_txt} ({dest})")
            elif sens_txt:
                bloc_lines.append(f"➡ {sens_txt}")
            elif dest:
                bloc_lines.append(f"➡ {dest}")

            # ===================================================
            # Client
            # ===================================================
            nom = str(row.get("NOM", "") or "").strip()
            if nom:
                bloc_lines.append(f"🧑 {nom}")

            # ===================================================
            # BDC
            # ===================================================
            for cand in ["NUM BDC", "Num BDC", "NUM_BDC", "BDC"]:
                if cand in cols and row.get(cand):
                    bloc_lines.append(
                        f"🧾 BDC : {str(row.get(cand)).strip()}"
                    )
                    break

            # ===================================================
            # Véhicule
            # ===================================================
            immat = str(row.get("IMMAT", "") or "").strip()
            if immat:
                bloc_lines.append(f"🚘 Plaque : {immat}")

            siege_bebe = extract_positive_int(row.get("SIEGE", row.get("SIÈGE")))
            if siege_bebe:
                bloc_lines.append(f"🍼 Siège bébé : {siege_bebe}")

            reh_n = extract_positive_int(row.get("REH"))
            if reh_n:
                bloc_lines.append(f"🪑 Rehausseur : {reh_n}")

            # ===================================================
            # Adresse / Téléphone
            # ===================================================
            adr_full = build_full_address_from_row(row)
            if adr_full:
                bloc_lines.append(f"📍 {adr_full}")

            client_phone = get_client_phone_from_row(row)
            tel_clean = clean_phone(client_phone) if client_phone else ""

            if client_phone:
                bloc_lines.append(
                    f"📞 Client : [{client_phone}](tel:{tel_clean})"
                )

            # ===================================================
            # Paiement / PAX
            # ===================================================
            pay_lines = []

            if row.get("PAX"):
                pay_lines.append(f"👥 {row.get('PAX')} pax")

            paiement = str(row.get("PAIEMENT", "") or "").lower().strip()
            caisse = row.get("Caisse")

            if paiement == "facture":
                pay_lines.append("🧾 **FACTURE**")
            elif paiement == "caisse" and caisse:
                pay_lines.append(
                    "<span style='color:#d32f2f;font-weight:800;'>"
                    f"💶 {caisse} € (CASH)</span>"
                )
            elif paiement == "bancontact" and caisse:
                pay_lines.append(
                    "<span style='color:#1976d2;font-weight:800;'>"
                    f"💳 {caisse} € (BANCONTACT)</span>"
                )

            if pay_lines:
                bloc_lines.append(" | ".join(pay_lines))

            # ===================================================
            # GO
            # ===================================================
            go_val = str(row.get("GO", "") or "").strip()
            if go_val:
                bloc_lines.append(f"🟢 {go_val}")

            # ===================================================
            # Confirmation
            # ===================================================
            row_key = make_row_key_from_row(row.to_dict())

            if is_row_confirmed(ch_selected, row_key):
                bloc_lines.append("✅ **Navette confirmée**")
            else:
                bloc_lines.append("🕒 **À confirmer**")

            # ===================================================
            # ✈️ Vol – statut (UNIQUEMENT AUJOURD'HUI)
            # ===================================================
            vol = extract_vol_val(row, cols)
            if (
                vol
                and isinstance(date_obj, (datetime, date))
                and date_obj == today
            ):
                bloc_lines.append(f"✈️ Vol {vol}")

                status, delay_min, *_ = get_flight_status_cached(vol)
                badge = flight_badge(status, delay_min)

                if badge:
                    bloc_lines.append(f"📡 {badge}")

                if (
                    delay_min is not None
                    and delay_min >= FLIGHT_ALERT_DELAY_MIN
                ):
                    bloc_lines.append(
                        f"🚨 **ATTENTION : retard {delay_min} min**"
                    )

            # ===================================================
            # AFFICHAGE FINAL
            # ===================================================
            st.markdown(
                "<br>".join(bloc_lines),
                unsafe_allow_html=True,
            )
            # ===================================================
            # Saisie chauffeur
            # ===================================================
            trajet_key = f"trajet_{row_key}"
            prob_key = f"prob_{row_key}"

            st.session_state.setdefault(trajet_key, "")
            st.session_state.setdefault(prob_key, "")

            st.text_input(
                "Trajet compris (ex : Liège → Zaventem)",
                key=trajet_key,
            )

            with st.expander("🚨 Signaler un problème (optionnel)"):
                st.text_area(
                    "Décris le problème pour cette navette",
                    key=prob_key,
                    placeholder=(
                        "Ex : heure impossible, adresse incorrecte, "
                        "client injoignable…"
                    ),
                )

        st.markdown("---")
        st.markdown("### 📄 Mon planning")

        if st.button("📄 Télécharger mon planning en PDF"):
            pdf_buffer = export_chauffeur_planning_pdf(
                df_ch, ch_selected
            )
            st.download_button(
                label="⬇️ Télécharger le PDF",
                data=pdf_buffer,
                file_name=f"planning_{ch_selected}.pdf",
                mime="application/pdf",
            )

def export_chauffeur_planning_pdf(df_ch: pd.DataFrame, ch: str):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    margin_x = 2 * cm
    y = height - 2 * cm
    line_h = 0.55 * cm

    def new_page():
        nonlocal y
        c.showPage()
        y = height - 2 * cm
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin_x, y, f"Planning chauffeur — {ch}")
        y -= 0.9 * cm
        c.setFont("Helvetica", 10)

    # En-tête page 1
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin_x, y, f"Planning chauffeur — {ch}")
    y -= 0.9 * cm
    c.setFont("Helvetica", 10)

    cols = df_ch.columns.tolist()

    def write_line(txt: str, indent: float = 0.0, bold: bool = False):
        nonlocal y
        if y < 2 * cm:
            new_page()

        c.setFont("Helvetica-Bold" if bold else "Helvetica", 10)
        c.drawString(margin_x + indent, y, txt[:120])
        y -= line_h

    for _, row in df_ch.iterrows():

        # --- Date ---
        dv = row.get("DATE")
        if isinstance(dv, date):
            date_txt = dv.strftime("%d/%m/%Y")
        else:
            try:
                dt = pd.to_datetime(dv, dayfirst=True, errors="coerce")
                date_txt = dt.strftime("%d/%m/%Y") if not pd.isna(dt) else "??/??/????"
            except Exception:
                date_txt = "??/??/????"

        # --- Heure ---
        heure = normalize_time_string(row.get("HEURE")) or "??:??"

        # --- Sens + destination ---
        sens_txt = format_sens_ar(row.get("Unnamed: 8"))
        lieu = resolve_client_alias(str(row.get("DESIGNATION", "") or "").strip())
        sens_dest = f"{sens_txt} ({lieu})" if sens_txt and lieu else (lieu or sens_txt or "Navette")

        # --- Client / tel / adresse ---
        nom = str(row.get("NOM", "") or "").strip()
        tel_client = get_client_phone_from_row(row)
        adr_full = build_full_address_from_row(row)

        # --- NUMÉRO DE BDC (ROBUSTE) ---
        num_bdc = ""
        for cand in ["NUM BDC", "Num BDC", "NUM_BDC", "BDC"]:
            if cand in cols and row.get(cand):
                num_bdc = str(row.get(cand)).strip()
                break

        # --- Véhicule (SIÈGE BÉBÉ / RÉHAUSSEUR) ---
        immat = str(row.get("IMMAT", "") or "").strip()

        # 🍼 Siège bébé (SIEGE / SIÈGE)
        siege_bebe = extract_positive_int(row.get("SIEGE", row.get("SIÈGE")))

        # 🪑 Rehausseur
        reh_n = extract_positive_int(row.get("REH"))


        # --- Paiement / caisse / pax ---
        pax = row.get("PAX")
        paiement = str(row.get("PAIEMENT", "") or "").lower()
        caisse = row.get("Caisse")

        # --- Vol ---
        vol = extract_vol_val(row, cols)

        # --- GO ---
        go_val = str(row.get("GO", "") or "").strip()

        # =======================
        # Impression bloc navette
        # =======================
        write_line(f"📆 {date_txt} | ⏱ {heure} — {sens_dest}", bold=True)

        if nom:
            write_line(f"👤 Client : {nom}", indent=10)

        if num_bdc:
            write_line(f"🧾 BDC : {num_bdc}", indent=10)

        if tel_client:
            write_line(f"📞 Client : {tel_client}", indent=10)

        if adr_full:
            write_line(f"📍 Adresse : {adr_full}", indent=10)

        veh_infos = []

        if immat:
            veh_infos.append(f"Plaque {immat}")

        if siege_bebe:
            veh_infos.append(f"🍼 Siège bébé {siege_bebe}")

        if reh_n:
            veh_infos.append(f"🪑 Rehausseur {reh_n}")

        if veh_infos:
            write_line("🚘 " + " | ".join(veh_infos), indent=10)


        extra = []
        if vol:
            extra.append(f"✈️ {vol}")
        if pax:
            extra.append(f"👥 {pax} pax")

        if paiement == "facture":
            extra.append("🧾 Facture")
        elif paiement in ("caisse", "bancontact"):
            if caisse not in ("", None):
                extra.append(f"💶 {caisse} € ({paiement})")
            else:
                extra.append(f"💶 {paiement}")

        if extra:
            write_line(" — ".join(extra), indent=10)

        if go_val:
            write_line(f"🟢 GO : {go_val}", indent=10)

        write_line("")

    c.save()
    buffer.seek(0)
    return buffer



    # =======================================================
    #   ENVOI DE CONFIRMATION (NAVETTES REMPLIES UNIQUEMENT)
    # =======================================================
    st.markdown("### ✅ Envoyer mes informations au bureau")

    recap_lines = []
    nb_remplies = 0

    for _, row in df_ch.iterrows():
        nav_id = row.get("id")

        trajet = st.session_state.get(f"trajet_nav_{nav_id}", "").strip()
        probleme = st.session_state.get(f"prob_nav_{nav_id}", "").strip()

        # on ignore totalement les navettes vides
        if not trajet and not probleme:
            continue

        nb_remplies += 1

        recap_lines.append(
            format_navette_ack(
                row=row,
                ch_selected=ch_selected,
                trajet=trajet,
                probleme=probleme,
            )
        )

    if nb_remplies == 0:
        st.warning(
            "ℹ️ Aucune information encodée. "
            "Merci de compléter au moins une navette avant l’envoi."
        )

    if st.button(
        "📤 Envoyer mes informations",
        disabled=(nb_remplies == 0),
        key=f"confirm_all_{ch_selected}_{scope}_{sel_date}",
    ):
        send_mail_admin(
            subject=f"[INFOS CHAUFFEUR] {ch_selected}",
            body="\n".join(recap_lines),
        )

        # marquer comme envoyées UNIQUEMENT les navettes remplies
        for _, row in df_ch.iterrows():
            nav_id = row.get("id")

            trajet = st.session_state.get(f"trajet_nav_{nav_id}", "").strip()
            probleme = st.session_state.get(f"prob_nav_{nav_id}", "").strip()

            if trajet or probleme:
                st.session_state[f"sent_nav_{nav_id}"] = True

        set_chauffeur_last_ack(ch_selected)

        st.success(f"✅ {nb_remplies} navette(s) envoyée(s) au bureau.")
        st.rerun()

def mark_new_rows_for_chauffeur(
    df: pd.DataFrame,
    ch_code: str,
) -> pd.DataFrame:
    """
    Marque les navettes 🆕 si elles ont été modifiées
    après la dernière confirmation du chauffeur.
    """

    last_ack = get_chauffeur_last_ack(ch_code)

    # Chauffeur n'a jamais confirmé → tout est nouveau
    if not last_ack:
        df["IS_NEW"] = True
        return df

    def _is_new(row):
        upd = row.get("UPDATED_AT")
        if not upd:
            return False

        try:
            upd_dt = pd.to_datetime(upd)
        except Exception:
            return False

        return upd_dt > last_ack

    df["IS_NEW"] = df.apply(_is_new, axis=1)
    return df

# ============================================================
#   🚖 ONGLET CHAUFFEUR — MON PLANNING COMPLET
# ============================================================

def render_tab_chauffeur_driver():
    ch_selected = st.session_state.get("chauffeur_code")
    if not ch_selected:
        st.error("Chauffeur non identifié.")
        return

    st.subheader(f"🚖 Mon planning — {ch_selected}")

    today = date.today()

    # ===================================================
    # 📅 CHOIX DE LA PÉRIODE (CHAUFFEUR)
    # ===================================================
    scope = st.radio(
        "📅 Quelles navettes veux-tu voir ?",
        [
            "📍 Aujourd’hui",
            "➡️ À partir de demain",
            "📆 Tout mon planning",
        ],
        index=0,
        horizontal=True,
        key="vue_chauffeur_scope",
    )

    if scope == "📍 Aujourd’hui":
        from_date = today
        to_date = today
        scope_label = "du jour"

    elif scope == "➡️ À partir de demain":
        from_date = today + timedelta(days=1)
        to_date = None
        scope_label = "à partir de demain"

    else:  # 📆 Tout
        from_date = None
        to_date = None
        scope_label = "complet"

    # ===================================================
    # 🔄 AUTO-SYNC SILENCIEUSE (DROPBOX → DB)
    # ===================================================
    auto_sync_planning_if_needed(silent=True)

    # ===================================================
    # 🔄 CHARGEMENT DU PLANNING
    # ===================================================
    df_ch = get_chauffeur_planning(
        ch_selected,
        from_date=from_date,
        to_date=to_date,
    )

    if df_ch is None or df_ch.empty:
        st.info(f"Aucune navette {scope_label}.")
        return

    df_ch = _sort_df_by_date_heure(df_ch)
    # ===================================================
    # 🔑 CALCUL DU ROW_KEY (OBLIGATOIRE POUR CONFIRMATION)
    # ===================================================
    if "ROW_KEY" not in df_ch.columns and "row_key" not in df_ch.columns:
        df_ch = df_ch.copy()
        df_ch["ROW_KEY"] = df_ch.apply(
            lambda r: make_row_key_from_row(r.to_dict()),
            axis=1,
        )

    # ===================================================
    # 🆕 MARQUAGE DES LIGNES NOUVELLES / MODIFIÉES
    # ===================================================
    df_ch = mark_new_rows_for_chauffeur(df_ch, ch_selected)

    cols = df_ch.columns.tolist()

    # ===================================================
    # 📄 PDF
    # ===================================================
    st.markdown("### 📄 Mon planning")

    if st.button(
        "📄 Télécharger mon planning en PDF",
        key=f"pdf_chauffeur_{ch_selected}_{scope}",
    ):
        pdf = export_chauffeur_planning_pdf(df_ch, ch_selected)
        st.download_button(
            label="⬇️ Télécharger le PDF",
            data=pdf,
            file_name=f"planning_{ch_selected}.pdf",
            mime="application/pdf",
        )

    st.markdown("---")

    # ===================================================
    # 🚖 NAVETTES
    # ===================================================
    for _, row in df_ch.iterrows():

        nav_id = row.get("id")
        bloc = []
        actions = []

        # ------------------
        # Flags groupage / partage / attente
        # ------------------
        is_groupage = int(row.get("IS_GROUPAGE", 0) or 0) == 1
        is_partage = int(row.get("IS_PARTAGE", 0) or 0) == 1
        is_attente = int(row.get("IS_ATTENTE", 0) or 0) == 1

        prefix = ""
        if is_groupage:
            prefix += "🟡 [GROUPÉE] "
        elif is_partage:
            prefix += "🟡 [PARTAGÉE] "
        if is_attente:
            prefix += "⭐ "

        # ------------------
        # Chauffeur
        # ------------------
        ch_code = str(row.get("CH", "") or ch_selected).strip()
        bloc.append(f"👨‍✈️ **{ch_code}**")

        # ------------------
        # Confirmation
        # ------------------
        row_key = make_row_key_from_row(row.to_dict())

        if is_row_confirmed(ch_selected, row_key):
            bloc.append("✅ **Navette confirmée**")
        else:
            bloc.append("🕒 **À confirmer**")


        # ------------------
        # Date / Heure
        # ------------------
        dv = row.get("DATE")
        if isinstance(dv, (datetime, date)):
            date_obj = dv if isinstance(dv, date) else dv.date()
            date_txt = date_obj.strftime("%d/%m/%Y")
        else:
            dtmp = pd.to_datetime(dv, dayfirst=True, errors="coerce")
            date_obj = dtmp.date() if not pd.isna(dtmp) else None
            date_txt = date_obj.strftime("%d/%m/%Y") if date_obj else ""

        heure_txt = normalize_time_string(row.get("HEURE")) or "??:??"
        bloc.append(f"{prefix}📆 {date_txt} | ⏱ {heure_txt}")
        # ------------------
        # Adresse de départ
        # ------------------
        adresse_depart = build_full_address_from_row(row)
        if adresse_depart:
            bloc.append(f"📍  {adresse_depart}")


        # ------------------
        # Sens / Destination
        # ------------------
        sens_txt = format_sens_ar(row.get("Unnamed: 8"))

        dest_raw = ""
        for cand in ["DESIGNATION", "DESTINATION", "DE/VERS"]:
            if cand in cols and row.get(cand):
                dest_raw = str(row.get(cand)).strip()
                if dest_raw:
                    break

        dest = resolve_client_alias(dest_raw)

        if sens_txt and dest:
            bloc.append(f"➡ {sens_txt} ({dest})")
        elif sens_txt:
            bloc.append(f"➡ {sens_txt}")
        elif dest:
            bloc.append(f"➡ {dest}")

        # ------------------
        # Client
        # ------------------
        nom = str(row.get("NOM", "") or "").strip()
        if nom:
            bloc.append(f"🧑 {nom}")

        # ------------------
        # 👥 PAX
        # ------------------
        pax = row.get("PAX")
        if pax not in ("", None, 0, "0"):
            try:
                pax_i = int(pax)
                if pax_i > 0:
                    bloc.append(f"👥 **{pax_i} pax**")
            except Exception:
                bloc.append(f"👥 **{pax} pax**")

        # ------------------
        # 🚘 Véhicule (SIÈGE BÉBÉ / RÉHAUSSEUR)
        # ------------------
        immat = str(row.get("IMMAT", "") or "").strip()
        if immat:
            bloc.append(f"🚘 Plaque : {immat}")

        siege_bebe = extract_positive_int(row.get("SIEGE", row.get("SIÈGE")))
        if siege_bebe:
            bloc.append(f"🍼 Siège bébé : {siege_bebe}")

        reh_n = extract_positive_int(row.get("REH"))
        if reh_n:
            bloc.append(f"🪑 Rehausseur : {reh_n}")

        # ------------------
        # Adresse / GPS
        # ------------------
        adr = build_full_address_from_row(row)
        adr_gps = normalize_address_for_gps(adr)

        if adr_gps:
            actions.append(
                f"[🧭 Waze]({build_waze_link(adr_gps)})"
            )
            actions.append(
                f"[🗺 Google Maps]({build_google_maps_link(adr_gps)})"
            )

        tel = get_client_phone_from_row(row)
        if tel:
            bloc.append(f"📞 {tel}")

        # ------------------
        # Paiement
        # ------------------
        paiement = str(row.get("PAIEMENT", "") or "").lower().strip()
        caisse = row.get("Caisse")

        if paiement == "facture":
            bloc.append("🧾 **FACTURE**")
        elif paiement == "caisse" and caisse:
            bloc.append(
                "<span style='color:#d32f2f;font-weight:800;'>"
                f"💶 {caisse} € (CASH)</span>"
            )
        elif paiement == "bancontact" and caisse:
            bloc.append(
                "<span style='color:#1976d2;font-weight:800;'>"
                f"💳 {caisse} € (BANCONTACT)</span>"
            )

        # ===================================================
        # ✈️ Vol – statut (UNIQUEMENT AUJOURD'HUI)
        # ===================================================
        vol = extract_vol_val(row, cols)
        if vol and date_obj == today:

            bloc.append(f"✈️ Vol {vol}")

            status, delay_min, *_ = get_flight_status_cached(vol)
            badge = flight_badge(status, delay_min)

            if badge:
                bloc.append(f"📡 {badge}")

            if delay_min is not None and delay_min >= FLIGHT_ALERT_DELAY_MIN:
                bloc.append(
                    f"🚨 **ATTENTION : retard {delay_min} min**"
                )

        # ------------------
        # GO
        # ------------------
        go_val = str(row.get("GO", "") or "").strip()
        if go_val:
            bloc.append(f"🟢 {go_val}")

        # ------------------
        # 🧾 BDC (juste après GO)
        # ------------------
        for cand in ["NUM BDC", "Num BDC", "NUM_BDC", "BDC"]:
            if cand in cols and row.get(cand):
                bloc.append(f"🧾 **BDC : {row.get(cand)}**")
                break

        # 📞 Numéro CLIENT
        tel_client = get_client_phone_from_row(row)

        # 📱 Numéro CHAUFFEUR (Feuil2 → PHONE)
        tel_chauffeur = get_chauffeur_phone(ch_selected)

        if tel_client:
            actions.append(f"[📞 Appeler](tel:{clean_phone(tel_client)})")

        # 💬 WhatsApp : vers le CLIENT, avec le numéro du CHAUFFEUR dans le message
        if tel_client and tel_chauffeur:
            msg = build_client_sms_from_driver(
                row=row,
                ch_code=ch_selected,
                tel_chauffeur=tel_chauffeur,
            )

            actions.append(
                f"[💬 WhatsApp]({build_whatsapp_link(tel_client, msg)})"
            )

        if actions:
            bloc.append(" | ".join(actions))


        # ------------------
        # Affichage
        # ------------------
        st.markdown("<br>".join(bloc), unsafe_allow_html=True)

        # =========================
        # 🔑 ROW_KEY
        # =========================
        row_key = (
            row.get("ROW_KEY")
            or row.get("row_key")
            or row.get("id")
            or nav_id
            or ""
        )
        row_key = str(row_key).strip()

        # =========================
        # 📝 SAISIE CHAUFFEUR (PAS D’ENVOI ICI)
        # =========================
        trajet_key = f"trajet_{row_key}"
        prob_key = f"prob_{row_key}"

        st.text_input(
            "Trajet compris",
            key=trajet_key,
        )

        with st.expander("🚨 Signaler un problème"):
            st.text_area(
                "Décrire le problème",
                key=prob_key,
            )

        st.markdown("---")
    # ===================================================
    # 📤 ENVOI GLOBAL DES INFORMATIONS CHAUFFEUR
    # ===================================================
    st.markdown("### 📤 Envoi des informations chauffeur")

    if st.button("📤 Envoyer toutes mes informations"):
        recap = []

        for _, row in df_ch.iterrows():

            nav_id = row.get("id")

            row_key = (
                row.get("ROW_KEY")
                or row.get("row_key")
                or row.get("id")
                or nav_id
                or ""
            )
            row_key = str(row_key).strip()

            trajet = st.session_state.get(f"trajet_{row_key}", "") or ""
            prob = st.session_state.get(f"prob_{row_key}", "") or ""

            if not trajet and not prob:
                continue

            recap.append(
                format_navette_ack(
                    row=row,
                    ch_selected=ch_selected,
                    trajet=trajet,
                    probleme=prob,
                )
            )

            if row_key:
                confirm_navette_row(ch_selected, row_key)

        if not recap:
            st.warning("Aucune information encodée.")
        else:
            send_mail_admin(
                subject=f"[INFOS CHAUFFEUR] {ch_selected}",
                body="\n\n".join(recap),
            )

            set_chauffeur_last_ack(ch_selected)
            st.success("✅ Toutes les informations ont été envoyées.")
            st.rerun()






# ======================================================================
#  ONGLET — Demandes d’indispo côté chauffeur
# ======================================================================

def render_tab_indispo_driver(ch_code: str):
    st.subheader("🚫 Mes indisponibilités")

    today = date.today()

    with st.form("form_indispo"):
        d = st.date_input("Date", value=today)
        col1, col2 = st.columns(2)
        with col1:
            h_debut = st.text_input("Heure début (ex: 08:00)")
        with col2:
            h_fin = st.text_input("Heure fin (ex: 12:00)")
        commentaire = st.text_input("Commentaire (optionnel)")
        submit = st.form_submit_button("📩 Envoyer la demande")

    if submit:
        req_id = create_indispo_request(ch_code, d, h_debut, h_fin, commentaire)

        # mail automatique
        send_mail_admin(
            f"Nouvelle indispo chauffeur {ch_code}",
            f"Chauffeur : {ch_code}\n"
            f"Date : {d.strftime('%d/%m/%Y')}\n"
            f"De {h_debut} à {h_fin}\n"
            f"Commentaire : {commentaire}\n"
            f"ID demande : {req_id}"
        )

        st.success("Demande envoyée à l’admin")
        st.rerun()

    st.markdown("### Mes demandes")
    df = get_indispo_requests(chauffeur=ch_code)

    st.dataframe(df, use_container_width=True, height=300)

# ============================================================
#   ONGLET 👨‍✈️ FEUIL2 / CHAUFFEURS
# ============================================================

def render_tab_chauffeurs():
    st.subheader("👨‍✈️ Chauffeurs (Feuil2)")

    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                'SELECT * FROM "chauffeurs" ORDER BY INITIALE',
                conn,
            )
    except Exception as e:
        st.error(f"Erreur en lisant la table `chauffeurs` : {e}")
        return

    # 🔒 Sécurité Streamlit : aucune colonne dupliquée
    df = df.loc[:, ~df.columns.duplicated()]

    st.markdown("#### Table chauffeurs (éditable)")
    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        key="chauffeurs_editor",
    )

    if st.button("💾 Enregistrer les modifications (chauffeurs)"):
        try:
            with get_connection() as conn:
                cur = conn.cursor()

                # On repart de zéro pour éviter doublons / lignes fantômes
                cur.execute('DELETE FROM "chauffeurs"')

                cols = [c for c in edited.columns if c != "id"]
                col_list_sql = ",".join(f'"{c}"' for c in cols)
                placeholders = ",".join("?" for _ in cols)

                for _, row in edited.iterrows():
                    values = [
                        row[c] if pd.notna(row[c]) else None
                        for c in cols
                    ]
                    cur.execute(
                        f'INSERT INTO "chauffeurs" ({col_list_sql}) VALUES ({placeholders})',
                        values,
                    )

                conn.commit()

            st.success("Table chauffeurs mise à jour ✅")
            st.rerun()

        except Exception as e:
            st.error(f"Erreur lors de la sauvegarde des chauffeurs : {e}")



# ============================================================
#   ONGLET 📄 FEUIL3 (INFOS DIVERSES)
# ============================================================

def render_tab_feuil3():
    st.subheader("📄 Feuil3 (infos diverses / logins, etc.)")

    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                "SELECT rowid AS id, * FROM feuil3",
                conn,
            )
    except Exception as e:
        st.warning(f"Table `feuil3` introuvable ou erreur : {e}")
        st.info("Si tu veux l'utiliser, ajoute la feuille Feuil3 dans l'Excel et relance l'import.")
        return

    st.markdown("#### Table Feuil3 (éditable)")
    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        key="feuil3_editor",
    )

    if st.button("💾 Enregistrer les modifications (Feuil3)"):
        try:
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM feuil3")

                cols = [c for c in edited.columns if c != "id"]
                col_list_sql = ",".join(f'"{c}"' for c in cols)
                placeholders = ",".join("?" for _ in cols)

                for _, row in edited.iterrows():
                    values = [row[c] if pd.notna(row[c]) else None for c in cols]
                    cur.execute(
                        f"INSERT INTO feuil3 ({col_list_sql}) VALUES ({placeholders})",
                        values,
                    )
                conn.commit()
            st.success("Table Feuil3 mise à jour ✅")
            st.rerun()
        except Exception as e:
            st.error(f"Erreur lors de la sauvegarde de Feuil3 : {e}")


# ============================================================
#   ONGLET 📂 EXCEL ↔ DB (Dropbox)
# ============================================================

def render_tab_excel_sync():

    from streamlit_autorefresh import st_autorefresh

    # ===================================================
    # 🔐 SÉCURITÉ — ADMIN UNIQUEMENT
    # ===================================================
    if st.session_state.get("role") != "admin":
        st.warning("🔒 Seuls les administrateurs peuvent synchroniser la base.")
        return

    # ===================================================
    # 🔁 RAFRAÎCHISSEMENT AUTOMATIQUE
    # ===================================================
    AUTO_REFRESH_MINUTES = 5  # ⬅️ modifiable si besoin
    st_autorefresh(
        interval=AUTO_REFRESH_MINUTES * 60 * 1000,
        key="auto_refresh_excel_sync",
    )

    # ===================================================
    # 🔍 VÉRIFICATION AUTO DROPBOX
    # ===================================================
    try:
        last_dbx_mtime = get_dropbox_file_last_modified()
    except Exception as e:
        last_dbx_mtime = None
        st.warning(f"⚠️ Dropbox indisponible : {e}")

    last_known = st.session_state.get("last_dropbox_mtime")

    if last_dbx_mtime and last_dbx_mtime != last_known:
        with st.spinner("🔁 Dropbox modifié — mise à jour automatique…"):
            inserted = sync_planning_from_today()

        st.session_state["last_dropbox_mtime"] = last_dbx_mtime
        st.session_state["last_sync_time"] = datetime.now().strftime("%d/%m/%Y %H:%M")

        if inserted > 0:
            st.toast("Planning mis à jour automatiquement depuis Dropbox 🚐", icon="📂")

    # ===================================================
    # 📂 TITRE
    # ===================================================
    st.subheader("📂 Synchronisation Excel → Base de données")

    # ===================================================
    # 🟢 DERNIÈRE SYNCHRO
    # ===================================================
    last_sync = st.session_state.get("last_sync_time")
    if last_sync:
        st.success(f"🟢 Dernière mise à jour : {last_sync}")
    else:
        st.info("🔴 Aucune synchronisation effectuée dans cette session")

    # ===================================================
    # ℹ️ INFO WORKFLOW
    # ===================================================
    st.markdown(
        """
        **Source principale du planning : Dropbox (Excel unique)**

        ---
        🔧 **Workflow normal :**

        1. Ouvre le fichier **Planning 2026.xlsx** dans **Dropbox**
        2. Modifie :
           - *Feuil1* → planning
           - *Feuil2* → chauffeurs
           - *Feuil3* → données annexes
        3. Enregistre le fichier
        4. La synchronisation se fait automatiquement
        """
    )

    st.markdown("---")

    # ===================================================
    # 🆘 MODE SECOURS — UPLOAD MANUEL
    # ===================================================
    st.subheader("🆘 Mode secours — Charger un fichier Excel manuellement")

    st.warning(
        "À utiliser uniquement en cas de problème avec Dropbox "
        "(token expiré, réseau indisponible, erreur API…)."
    )

    uploaded_file = st.file_uploader(
        "📤 Charger un fichier Planning Excel (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=False,
        help="Le fichier doit avoir exactement la même structure que Planning 2026.xlsx",
    )

    if uploaded_file:
        st.info(
            f"📄 Fichier chargé : {uploaded_file.name}\n\n"
            "⚠️ Cette action remplacera les données à partir d’aujourd’hui dans la base."
        )

        confirm_upload = st.checkbox(
            "Je confirme vouloir synchroniser la base depuis ce fichier",
            key="confirm_manual_excel_upload",
        )

        if st.button(
            "🆘 SYNCHRONISER DEPUIS LE FICHIER MANUEL",
            type="secondary",
            disabled=not confirm_upload,
        ):
            with st.spinner("🔄 Synchronisation depuis fichier manuel…"):
                inserted = sync_planning_from_uploaded_file(uploaded_file)

            st.session_state["last_sync_time"] = datetime.now().strftime("%d/%m/%Y %H:%M")

            if inserted > 0:
                st.success(f"✅ DB mise à jour ({inserted} lignes importées)")
                st.toast("Planning synchronisé depuis fichier manuel 📄", icon="🆘")
            else:
                st.warning("Aucune donnée n’a été modifiée.")

    st.markdown("---")

    # ===================================================
    # 🔄 SYNCHRO MANUELLE DROPBOX
    # ===================================================
    confirm = st.checkbox(
        "Je confirme vouloir forcer la mise à jour de la base depuis Dropbox",
        key="confirm_force_sync_dropbox",
    )

    col1, col2 = st.columns([2, 3])

    with col1:
        btn_force = st.button(
            "🔄 FORCER MAJ DROPBOX → DB",
            type="primary",
            disabled=not confirm,
        )

    with col2:
        st.caption(
            "⚠️ Cette action remplace toutes les navettes "
            "à partir d’aujourd’hui dans la base."
        )

    if btn_force:
        with st.spinner("🔄 Synchronisation en cours depuis Dropbox…"):
            inserted = sync_planning_from_today()

        st.session_state["last_sync_time"] = datetime.now().strftime("%d/%m/%Y %H:%M")

        if inserted > 0:
            st.success(f"✅ DB mise à jour depuis aujourd’hui ({inserted} lignes)")
            st.toast("Planning mis à jour depuis Dropbox 🚐", icon="📂")
        else:
            st.warning("Aucune donnée n’a été modifiée.")

    st.markdown("---")

    # ===================================================
    # ℹ️ INFO FINALE
    # ===================================================
    st.info(
        "💡 **Dropbox est la source principale du planning.**\n\n"
        "- Synchronisation automatique quand Dropbox est disponible\n"
        "- Mode secours possible via upload manuel\n"
        "- Aucun SharePoint / OneDrive\n"
        "- Base toujours alignée sur un Excel de référence"
    )

# ============================================================
#   ONGLET 📦 ADMIN TRANSFERTS (LISTE GLOBALE)
# ============================================================

def render_tab_admin_transferts():
    st.subheader("📦 Tous les transferts — vue admin")

    # Sous-onglets Admin transferts
    tab_transferts, tab_excel, tab_heures = st.tabs([
        "📋 Transferts / SMS",
        "🟡 À reporter dans Excel",
        "⏱️ Calcul d’heures",
    ])

    with tab_excel:
        st.subheader("🟡 Modifications à reporter dans Excel (Feuil1)")

        from database import list_pending_actions
        import pandas as pd

        actions = list_pending_actions(limit=300)

        if not actions:
            st.success("✅ Aucune modification en attente. Excel et l’application sont alignés.")
        else:
            rows = []

            for (
                action_id,
                row_key,
                action_type,
                old_value,
                new_value,
                user,
                created_at,
            ) in actions:
                rows.append({
                    "Type": action_type,
                    "Avant": old_value,
                    "Après": new_value,
                    "Modifié par": user,
                    "Date / heure": created_at,
                })

            df_actions = pd.DataFrame(rows)

            st.info(
                "Ces modifications ont été faites dans l’application "
                "mais ne sont pas encore reportées dans Excel (Feuil1)."
            )

            st.dataframe(
                df_actions,
                use_container_width=True,
                hide_index=True,
            )

    # ======================================================
    # 📋 ONGLET TRANSFERTS / SMS  (TON CODE ACTUEL)
    # ======================================================
    with tab_transferts:

        today = date.today()
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Date de début",
                value=today.replace(day=1),
                key="admin_start_date",
            )
        with col2:
            end_date = st.date_input(
                "Date de fin",
                value=today,
                key="admin_end_date",
            )

        df = get_planning(
            start_date=start_date,
            end_date=end_date,
            chauffeur=None,
            type_filter=None,
            search="",
            max_rows=5000,
        )

        if df.empty:
            st.warning("Aucun transfert pour cette période.")
            return

        # 🔽 Filtres avancés
        col3, col4, col5 = st.columns(3)
        with col3:
            bdc_prefix = st.text_input(
                "Filtrer par Num BDC (préfixe, ex : JC → JCS, JCH…)",
                "",
                key="admin_bdc_prefix",
            )
        with col4:
            paiement_filter = st.text_input(
                "Filtrer par mode de paiement (contient, ex : CASH, VISA…)",
                "",
                key="admin_paiement_filter",
            )
        with col5:
            ch_filter = st.text_input(
                "Filtrer par chauffeur (CH, ex : GG, FA, NP…)",
                "",
                key="admin_ch_filter",
            )

        if bdc_prefix.strip() and "Num BDC" in df.columns:
            p = bdc_prefix.strip().upper()
            df = df[df["Num BDC"].astype(str).str.upper().str.startswith(p)]

        if paiement_filter.strip() and "PAIEMENT" in df.columns:
            p = paiement_filter.strip().upper()
            df = df[df["PAIEMENT"].astype(str).str.upper().str.contains(p)]

        if ch_filter.strip() and "CH" in df.columns:
            p = ch_filter.strip().upper()
            df = df[df["CH"].astype(str).str.upper() == p]

        if df.empty:
            st.warning("Aucun transfert après application des filtres.")
            return

        # Tri
        sort_mode = st.radio(
            "Tri",
            ["DATE + HEURE", "CH + DATE + HEURE"],
            horizontal=True,
            key="admin_sort_mode",
        )

        sort_cols = []
        if sort_mode == "CH + DATE + HEURE" and "CH" in df.columns:
            sort_cols.append("CH")
        if "DATE" in df.columns:
            sort_cols.append("DATE")
        if "HEURE" in df.columns:
            sort_cols.append("HEURE")

        if sort_cols:
            df = df.sort_values(sort_cols)

        df_display = df.copy()
        if "id" in df_display.columns:
            df_display = df_display.drop(columns=["id"])

        st.markdown(f"#### {len(df_display)} transfert(s) sur la période sélectionnée")
        st.dataframe(df_display, use_container_width=True, height=500)

        # ======================================================
        #   SMS / WHATSAPP CLIENTS
        # ======================================================
        st.markdown("---")
        st.markdown("### 📱 Messages clients (WhatsApp / SMS)")

        col_sms1, col_sms2 = st.columns(2)

        with col_sms1:
            if st.button("📅 Préparer SMS/WhatsApp pour demain", key="sms_clients_demain"):
                target = today + timedelta(days=1)
                show_client_messages_for_period(df, target, nb_days=1)

        with col_sms2:
            if st.button("📅 Préparer SMS/WhatsApp pour les 3 prochains jours", key="sms_clients_3j"):
                target = today + timedelta(days=1)
                show_client_messages_for_period(df, target, nb_days=3)

    # ======================================================
    # ⏱️ ONGLET CALCUL D’HEURES
    # ======================================================
    with tab_heures:
        render_tab_calcul_heures()

def render_tab_calcul_heures():
    st.subheader("⏱️ Calcul d’heures")

    from database import (
        get_time_rules_df,
        save_time_rules_df,
        get_rule_minutes,
        _detect_sens_dest_from_row,
        _minutes_to_hhmm,
    )

    tab_calc, tab_rules = st.tabs(["📊 Calcul", "⚙️ Règles"])

    # =========================
    # ⚙️ ONGLET RÈGLES
    # =========================
    with tab_rules:
        st.markdown("### ⚙️ Règles de calcul")
        st.caption("Chauffeur (NP, NP*, *), Sens (VERS/DE), Destination (BRU/AMS/…/AUTRE), Heures (ex: 2h30)")

        df_rules = get_time_rules_df()
        if df_rules.empty:
            df_rules = pd.DataFrame(columns=["id", "ch", "sens", "dest", "heures"])

        df_rules = df_rules.loc[:, ~df_rules.columns.duplicated()]

        edited = st.data_editor(
            df_rules,
            use_container_width=True,
            num_rows="dynamic",
            key="time_rules_editor",
        )

        if st.button("💾 Enregistrer les règles"):
            try:
                if "id" in edited.columns:
                    edited = edited.drop(columns=["id"], errors="ignore")
                save_time_rules_df(edited)
                st.success("Règles enregistrées ✅")
                st.rerun()
            except Exception as e:
                st.error(f"Erreur sauvegarde règles : {e}")

    # =========================
    # 📊 ONGLET CALCUL
    # =========================
    with tab_calc:
        col1, col2, col3 = st.columns(3)

        today = date.today()
        with col1:
            d1 = st.date_input("Date début", value=today, key="hrs_d1")
        with col2:
            d2 = st.date_input("Date fin", value=today, key="hrs_d2")
        with col3:
            ch_filter = st.selectbox("Chauffeur", ["Tous", "NP", "NP*"], key="hrs_ch")

        df = get_planning(
            start_date=d1,
            end_date=d2,
            chauffeur=None if ch_filter == "Tous" else ch_filter,
            type_filter=None,
            search="",
            max_rows=5000,
        )

        if df.empty:
            st.info("Aucune navette sur cette période.")
            return

        rows = []
        total_minutes = 0
        to_check = 0

        for _, r in df.iterrows():
            if is_indispo_row(r, df.columns.tolist()):
                continue

            ch = str(r.get("CH", "") or "").strip()
            if ch_filter != "Tous" and ch != ch_filter:
                continue

            dv = r.get("DATE")
            if isinstance(dv, (datetime, date)):
                date_txt = dv.strftime("%d/%m/%Y")
            else:
                dtmp = pd.to_datetime(dv, dayfirst=True, errors="coerce")
                date_txt = dtmp.strftime("%d/%m/%Y") if not pd.isna(dtmp) else ""

            sens, dest = _detect_sens_dest_from_row(r.to_dict())
            minutes = get_rule_minutes(ch, sens, dest)

            note = ""
            if minutes <= 0:
                note = "⚠️ Heure estimée à vérifier / modifier"
                to_check += 1
            else:
                total_minutes += minutes

            rows.append({
                "Date": date_txt,
                "CH": ch,
                "Sens": sens,
                "Dest": dest,
                "Heures": _minutes_to_hhmm(minutes) if minutes else "",
                "Note": note,
            })

        out = pd.DataFrame(rows)
        st.dataframe(out, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.metric("Total heures", _minutes_to_hhmm(total_minutes))
        st.metric("Lignes à vérifier", to_check)




# ==========================================================================
#  ONGLET Admin — Validation des indispos
# ==========================================================================

def render_tab_indispo_admin():
    st.subheader("🚫 Indisponibilités chauffeurs")

    # Toutes les demandes
    df = get_indispo_requests()

    if df.empty:
        st.info("Aucune demande d'indisponibilité.")
        return

    st.markdown("### 🔍 Toutes les demandes")
    st.dataframe(df, use_container_width=True, height=250)

    # Demandes en attente
    if "STATUT" not in df.columns:
        st.error("Colonne STATUT manquante dans la table chauffeur_indispo.")
        return

    df_pending = df[df["STATUT"] == "EN_ATTENTE"].copy()

    if df_pending.empty:
        st.info("Aucune demande en attente.")
        return

    st.warning(f"🔔 {len(df_pending)} demande(s) en attente")
    st.markdown("### 📝 Traiter une demande")

    # Sélecteur avec un joli label
    def _format_option(row):
        ch = str(row.get("CH", "") or "")
        d = str(row.get("DATE", "") or "")
        h1 = str(row.get("HEURE_DEBUT", "") or "")
        h2 = str(row.get("HEURE_FIN", "") or "")
        com = str(row.get("COMMENTAIRE", "") or "")

        label = f"#{row['id']} — {ch} {d} {h1}→{h2}"
        if com:
            label += f" — {com[:40]}"
        return label

    options = [int(v) for v in df_pending["id"].tolist()]
    labels_map = {int(row["id"]): _format_option(row) for _, row in df_pending.iterrows()}

    selected_id = st.selectbox(
        "Sélectionne une demande",
        options=options,
        format_func=lambda x: labels_map.get(int(x), f"#{x}"),
    )

    row = df_pending[df_pending["id"] == selected_id].iloc[0]

    colA, colB = st.columns(2)

    with colA:
        if st.button("✅ Accepter"):
            # Création d'une ligne INDISPO dans le planning
            data_planning = {
                "DATE": row.get("DATE", ""),
                "HEURE": row.get("HEURE_DEBUT", ""),
                "²²²²": row.get("HEURE_FIN", ""),
                "CH": row.get("CH", ""),
                "REMARQUE": f"INDISPO {row.get('CH','')} - {row.get('COMMENTAIRE','')}",
            }
            planning_id = insert_planning_row(data_planning)

            # MAJ statut + lien vers la ligne planning
            set_indispo_status(int(row["id"]), "ACCEPTEE", planning_id=planning_id)

            st.success("Indisponibilité acceptée et ajoutée au planning.")
            st.rerun()

    with colB:
        if st.button("❌ Refuser"):
            set_indispo_status(int(row["id"]), "REFUSEE")
            st.error("La demande a été refusée.")
            st.rerun()


# ============================================================
#   MAIN — ROUTAGE PAR RÔLE (admin / restricted / driver)
# ============================================================

def main():
    auto_sync_planning_if_needed()
    # ======================================
    # 1️⃣ INITIALISATION SESSION (OBLIGATOIRE)
    # ======================================
    init_session_state()

    # ======================================
    # 2️⃣ INITIALISATIONS DB SAFE
    #    (ne plantent pas si DB vide)
    # ======================================
    init_indispo_table()
    init_chauffeur_ack_table()
    init_flight_alerts_table()
    init_time_rules_table()
    init_actions_table() 

    # Ces fonctions DOIVENT être safe
    ensure_planning_updated_at_column()
    ensure_km_time_columns()
    ensure_flight_alerts_time_columns()
    ensure_ack_columns()

    # ======================================
    # 3️⃣ LOGIN
    # ======================================
    if not st.session_state.logged_in:
        login_screen()
        st.stop()

    # ======================================
    # 4️⃣ UI PRINCIPALE
    # ======================================
    render_top_bar()

    role = st.session_state.role

    # 👉 ensuite ton routing normal :
    # if role == "admin":
    #     ...
    # elif role == "driver":
    #     ...


    # ====================== ADMIN ===========================
    # ====================== ADMIN ===========================
    if role == "admin":
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(
            [
                "📅 Planning",
                "⚡ Vue jour (mobile)",
                "📊 Tableau / Édition",
                "🔍 Clients / Historique",
                "🚖 Vue Chauffeur",
                "👨‍✈️ Feuil2 / Chauffeurs",
                "📄 Feuil3",
                "📦 Admin transferts",
                "📂 Excel ↔ DB",
                "🚫 Indispos chauffeurs",
            ]
        )
        with tab1:
            render_tab_planning()

        with tab2:
            render_tab_quick_day_mobile()

        with tab3:
            render_tab_table()

        with tab4:
            render_tab_clients()

        with tab5:
            render_tab_vue_chauffeur()

        with tab6:
            render_tab_chauffeurs()

        with tab7:
            render_tab_feuil3()

        with tab8:
            render_tab_admin_transferts()

        with tab9:
            render_tab_excel_sync()

        with tab10:
            render_tab_indispo_admin()



    # ==================== RESTRICTED (LEON) =================
    elif role == "restricted":
        # leon (role = restricted) n'a PAS accès à l’onglet Admin ni Excel↔DB
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
            [
                "📅 Planning",
                "📊 Tableau / Édition",
                "🔍 Clients / Historique",
                "🚖 Vue Chauffeur",
                "👨‍✈️ Feuil2 / Chauffeurs",
                "📄 Feuil3",
            ]
        )

        with tab1:
            render_tab_planning()

        with tab2:
            render_tab_table()

        with tab3:
            render_tab_clients()

        with tab4:
            render_tab_vue_chauffeur()

        with tab5:
            render_tab_chauffeurs()

        with tab6:
            render_tab_feuil3()

    # ==================== DRIVER (CHAUFFEUR) = GG, FA,... ===
    elif role == "driver":
        ch_code = st.session_state.get("chauffeur_code")
        if not ch_code:
            st.error("Aucun code chauffeur configuré pour cet utilisateur.")
            return

        tab1, tab2 = st.tabs(
            ["🚖 Mon planning", "🚫 Mes indispos"]
        )

        with tab1:
            render_tab_chauffeur_driver()

        with tab2:
            render_tab_indispo_driver(ch_code)


    # ==================== AUTRE RÔLE INCONNU = ERREUR ======
    else:
        st.error(f"Rôle inconnu : {role}")


if __name__ == "__main__":


    main()

