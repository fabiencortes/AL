# ============================================================
#   AIRPORTS LINES – APP.PLANNING – VERSION OPTIMISÉE 2025
#   BLOC 1/7 : IMPORTS, CONFIG, HELPERS, SESSION
# ============================================================

import os
import io
from datetime import datetime, date, timedelta
from typing import Dict, Any, List

import math
import smtplib
from email.mime.text import MIMEText
import pandas as pd
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
    init_indispo_table,
    create_indispo_request,
    get_indispo_requests,
    set_indispo_status,
    ensure_planning_updated_at_column,
    init_chauffeur_ack_table,
    get_chauffeur_last_ack,
    set_chauffeur_last_ack,
)

from import_excel_to_db import EXCEL_FILE, import_planning_from_feuil1

# ============================================================
#   CONFIG STREAMLIT
# ============================================================

st.set_page_config(
    page_title="Airports-Lines – Planning chauffeurs",
    layout="wide",
)

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
    "fa1": {"password": "fa1", "role": "driver", "chauffeur_code": "FA1"},
    "gd": {"password": "gd", "role": "driver", "chauffeur_code": "GD"},
    "om": {"password": "om", "role": "driver", "chauffeur_code": "OM"},
}

# Fallback si Feuil2 ne contient rien
CH_CODES = [
    "AU", "FA", "GD", "GG", "LL", "MA", "O", "RK", "RO", "SW", "NP", "DO",
    "OM", "AD", "CB", "CF", "CM", "EM", "GE", "HM", "JF", "KM", "LILLO",
    "MF", "WS", "FA1"
]

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

SMTP_HOST = "smtp.office365.com"      # 📌 ou smtp.gmail.com selon ton adresse
SMTP_PORT = 587
SMTP_USER = "info@airports-lines.com"    # 📌 ton mail
SMTP_PASSWORD = "TON_MOT_DE_PASSE"       # 📌 ton mot de passe approuvé SMTP

ADMIN_NOTIFICATION_EMAIL = "info@airports-lines.com"
# ============================================================
#   HELPERS — NORMALISATION DES HEURES
# ============================================================

def normalize_time_string(val) -> str:
    """
    Nettoie et convertit : 8, 815, 8h15, 08H15, " 8:5 "...
    Retourne toujours HH:MM ou "".
    """
    if val is None:
        return ""

    s = str(val).strip()
    if not s:
        return ""

    # Remplacer H / h par :
    s = s.replace("H", ":").replace("h", ":").strip()

    # Format HHMM → HH:MM
    if s.isdigit():
        if len(s) <= 2:
            try:
                h = int(s)
                return f"{h:02d}:00"
            except:
                return s
        else:
            try:
                h = int(s[:-2])
                m = int(s[-2:])
                return f"{h:02d}:{m:02d}"
            except:
                return s

    # Format H:M, HH:M, H:MM, etc.
    if ":" in s:
        try:
            h, m = s.split(":")
            h = int(h)
            m = int(m)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f"{h:02d}:{m:02d}"
        except:
            return ""

    return s


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
    Normalise le code chauffeur pour retrouver son GSM dans Feuil2
    sans confondre FA et FA1.

    Règles :
      - 'FA*'  -> 'FA'
      - 'FA1*' -> 'FA1'
      - 'DO*'  -> 'DO'
      - 'AD*'  -> 'AD'
      - 'NP*'  -> 'NP'
      - 'FADO' ou 'FADO*' -> 'FA'  (on prend FA comme chauffeur principal)
    """
    if not ch_code:
        return ""

    code = str(ch_code).strip().upper()

    # On enlève les étoiles (FA* -> FA, FA1* -> FA1, FADO* -> FADO)
    code = code.replace("*", "")

    # Mapping explicite pour les combinaisons
    combo_map = {
        "FADO": "FA",
    }
    if code in combo_map:
        return combo_map[code]

    # Liste des initiales connues dans Feuil2
    try:
        known = [c.strip().upper() for c in get_chauffeurs()]
    except Exception:
        known = []

    # Si le code exact existe dans la liste (FA, FA1, DO, DO1, NP, NP1, etc.)
    if code in known:
        return code

    # Si jamais (par erreur) il y a un code plus long qu'une initiale connue,
    # on teste uniquement comme "préfixe" MAIS on NE touche PAS aux codes
    # qui se terminent par un chiffre (FA1 doit rester FA1, pas FA).
    if not code[-1].isdigit():
        # Ex: FADO -> FA si FA est connu (mais FAD1 ne sera pas tronqué)
        for k in known:
            if code.startswith(k):
                return k

    # Sinon, on renvoie le code nettoyé tel quel
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
def build_client_sms_from_driver(row: pd.Series, ch_code: str, tel_chauffeur: str) -> str:
    """
    Message WhatsApp envoyé par le chauffeur au client,
    SANS mentionner l'adresse du point de rendez-vous.
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

    # Nom du client
    nom_client = str(row.get("NOM", "") or "").strip()
    if nom_client:
        bonjour = f"Bonjour {nom_client}, c'est votre chauffeur {ch_code} pour Airports-Lines."
    else:
        bonjour = f"Bonjour, c'est votre chauffeur {ch_code} pour Airports-Lines."

    # Message SANS adresse
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
#   SESSION STATE
# ============================================================

def init_session_state():
    """
    Initialise proprement toutes les variables session
    pour éviter les plantages Streamlit.
    """
    defaults = {
        "logged_in": False,
        "username": None,
        "role": None,
        "chauffeur_code": None,
        "planning_start": date.today(),
        "planning_end": date.today() + timedelta(days=6),
        "planning_sort_choice": "Date + heure",
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


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
        if st.button("Déconnexion"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()


# ============================================================
#   STYLE COULEURS — EXACTEMENT COMME TES XLSX
# ============================================================

def style_groupage_partage(df: pd.DataFrame):
    """
    Applique les couleurs :
    - GROUPAGE = ligne jaune (#fff9c4 comme Excel)
    - PARTAGE = colonne HEURE jaune
    - INDISPO = ligne grise
    - GO/GL/AL = couleurs sur colonne GO
    """
    columns = list(df.columns)

    idx_heure = columns.index("HEURE") if "HEURE" in columns else None
    idx_go = columns.index("GO") if "GO" in columns else None

    def apply_style(row):
        style_row = [""] * len(columns)

        # Indisponibilité
        if is_indispo_row(row, columns):
            return ['background-color: #ff8a80; color: #000;'] * len(columns)

        # Groupage
        if bool_from_flag(row.get("GROUPAGE", "0")):
            return ['background-color: #fff9c4;'] * len(columns)

        # Partagée : uniquement la colonne HEURE
        if bool_from_flag(row.get("PARTAGE", "0")) and idx_heure is not None:
            style_row[idx_heure] = 'background-color: #fff9c4; font-weight: bold;'

        # GO / GL / AL
        if idx_go is not None:
            go_val = str(row.get("GO", "")).upper().strip()
            if go_val == "GO":
                style_row[idx_go] = 'background-color: #c8e6c9; font-weight: bold;'  # vert clair
            elif go_val == "GL":
                style_row[idx_go] = 'background-color: #ffcdd2; font-weight: bold;'  # rouge clair
            elif go_val == "AL":
                style_row[idx_go] = 'background-color: #bbdefb; font-weight: bold;'  # bleu clair

        return style_row

    return df.style.apply(apply_style, axis=1)


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

    today = date.today()

    # ----------------- Raccourcis de dates -----------------
    colb1, colb2, colb3, colb4 = st.columns(4)
    with colb1:
        if st.button("📆 Aujourd’hui"):
            st.session_state.planning_start = today
            st.session_state.planning_end = today
            st.rerun()
    with colb2:
        if st.button("📆 Demain"):
            d = today + timedelta(days=1)
            st.session_state.planning_start = d
            st.session_state.planning_end = d
            st.rerun()
    with colb3:
        if st.button("📆 Cette semaine"):
            lundi = today - timedelta(days=today.weekday())
            dimanche = lundi + timedelta(days=6)
            st.session_state.planning_start = lundi
            st.session_state.planning_end = dimanche
            st.rerun()
    with colb4:
        if st.button("📆 Semaine prochaine"):
            lundi_next = today - timedelta(days=today.weekday()) + timedelta(days=7)
            dimanche_next = lundi_next + timedelta(days=6)
            st.session_state.planning_start = lundi_next
            st.session_state.planning_end = dimanche_next
            st.rerun()

    # ----------------- Sélection de période -----------------
    colf1, colf2 = st.columns(2)
    with colf1:
        start_date = st.date_input(
            "Date de début",
            value=st.session_state.planning_start,
            key="planning_start_widget",
        )
    with colf2:
        end_date = st.date_input(
            "Date de fin",
            value=st.session_state.planning_end,
            key="planning_end_widget",
        )

    st.session_state.planning_start = start_date
    st.session_state.planning_end = end_date

    # ----------------- Chauffeur / type / recherche ---------
    chs = get_chauffeurs_for_ui()


    colf3, colf4 = st.columns([1, 2])
    with colf3:
        ch_value = st.selectbox(
            "Chauffeur (CH)",
            ["(Tous)"] + chs,
            key="planning_ch",
        )
        if ch_value == "(Tous)":
            ch_value = None
    with colf4:
        type_choice = st.selectbox(
            "Type de transferts",
            ["Tous", "AL (hors GO/GL)", "GO / GL"],
            key="planning_type",
        )

    if type_choice == "Tous":
        type_filter = None
    elif type_choice.startswith("AL"):
        type_filter = "AL"      # GO/GL exclus
    else:
        type_filter = "GO_GL"   # uniquement GO/GL

    colf5, colf6 = st.columns([3, 1])
    with colf5:
        search = st.text_input(
            "Recherche (client, désignation, vol, remarque…)",
            "",
            key="planning_search",
        )
    with colf6:
        sort_choice = st.selectbox(
            "Tri",
            ["Date + heure", "Chauffeur + date + heure", "Aucun"],
            key="planning_sort_choice_select",
        )
        st.session_state.planning_sort_choice = sort_choice

    # ----------------- Lecture DB planning ------------------
    df = get_planning(
        start_date=start_date,
        end_date=end_date,
        chauffeur=ch_value,
        type_filter=type_filter,
        search=search,
        max_rows=2000,
    )

    if df.empty:
        st.warning("Aucune navette pour ces paramètres.")
        return

    # Sécurité : max 40 colonnes, MAIS on garde les colonnes importantes
    if df.shape[1] > 40:
        priority = ["id", "DATE", "HEURE", "CH", "GO", "GROUPAGE", "PARTAGE"]
        core_cols = [c for c in priority if c in df.columns]
        other_cols = [c for c in df.columns if c not in core_cols]
        max_cols = 40
        keep_cols = core_cols + other_cols[: max_cols - len(core_cols)]
        df = df[keep_cols]


    # ----------------- Tri ------------------
    sort_cols = []
    if sort_choice == "Date + heure":
        if "DATE" in df.columns:
            sort_cols.append("DATE")
        if "HEURE" in df.columns:
            sort_cols.append("HEURE")
    elif sort_choice == "Chauffeur + date + heure":
        if "CH" in df.columns:
            sort_cols.append("CH")
        if "DATE" in df.columns:
            sort_cols.append("DATE")
        if "HEURE" in df.columns:
            sort_cols.append("HEURE")

    if sort_cols:
        df = df.sort_values(sort_cols)

    # ----------------- Stats rapides ------------------
    nb_navettes = len(df)
    nb_go_gl = 0
    if "GO" in df.columns:
        go_series = df["GO"].astype(str).str.upper().str.strip()
        nb_go_gl = go_series.isin(["GO", "GL"]).sum()

    colm1, colm2 = st.columns(2)
    with colm1:
        st.metric("🚐 Navettes", int(nb_navettes))
    with colm2:
        st.metric("🎯 GO / GL", int(nb_go_gl))

    # ----------------- Tableau avec couleurs ------------------
    df_display = df.copy()
    if "id" in df_display.columns:
        df_display = df_display.drop(columns=["id"])

    try:
        styled = style_groupage_partage(df_display)
        st.dataframe(styled, use_container_width=True, height=500)
    except Exception:
        st.dataframe(df_display, use_container_width=True, height=500)

    st.markdown("---")
    st.markdown("### 🔁 Actions de groupe (dupliquer / supprimer les navettes sélectionnées)")

    if "id" not in df.columns:
        st.info("La colonne `id` est nécessaire pour les actions (dupliquer/supprimer).")
        return

    # ----------------- Sélection multiple ------------------
    labels_by_id: Dict[int, str] = {}
    for _, row in df.iterrows():
        rid = int(row["id"])
        d_txt = str(row.get("DATE", "") or "")
        h_txt = normalize_time_string(row.get("HEURE", ""))
        nom = str(row.get("NOM", "") or "")
        ch_txt = str(row.get("CH", "") or "")
        bdc = str(row.get("Num BDC", "") or "")

        label = f"{d_txt} {h_txt} | {nom} ({ch_txt})"
        if bdc:
            label += f" | BDC: {bdc}"

        labels_by_id[rid] = label

    selected_ids = st.multiselect(
        "Sélectionne une ou plusieurs navettes",
        options=list(labels_by_id.keys()),
        format_func=lambda x: labels_by_id.get(x, str(x)),
        key="planning_group_ids",
    )

    colg1, colg2 = st.columns(2)

    # ----------------- Dupliquer ------------------
    with colg1:
        if st.button("📋 Dupliquer les navettes sélectionnées"):
            if not selected_ids:
                st.warning("Aucune navette sélectionnée.")
            else:
                nb_done = 0
                for rid in selected_ids:
                    row_g = get_row_by_id(int(rid))
                    if not row_g:
                        continue
                    if role_allows_go_gl_only() and not leon_allowed_for_row(row_g.get("GO")):
                        continue
                    clone = {k: v for k, v in row_g.items() if k != "id"}
                    insert_planning_row(clone)
                    nb_done += 1
                st.success(f"{nb_done} navette(s) dupliquée(s).")
                st.rerun()

    # ----------------- Supprimer ------------------
    with colg2:
        if st.button("🗑️ Supprimer les navettes sélectionnées"):
            if not selected_ids:
                st.warning("Aucune navette sélectionnée.")
            else:
                nb_done = 0
                for rid in selected_ids:
                    row_g = get_row_by_id(int(rid))
                    if not row_g:
                        continue
                    if role_allows_go_gl_only() and not leon_allowed_for_row(row_g.get("GO")):
                        continue
                    delete_planning_row(int(rid))
                    nb_done += 1
                st.success(f"{nb_done} navette(s) supprimée(s).")
                st.rerun()
# ============================================================
#   ONGLET 📊 TABLEAU / ÉDITION — SÉLECTION + FICHE DÉTAILLÉE
# ============================================================

def render_tab_table():
    st.subheader("📊 Tableau planning — sélection, édition, groupage")

    today = date.today()
    start_date = st.date_input(
        "Afficher les navettes à partir de :",
        value=today,
        key="table_start",
    )

    df = get_planning(start_date=start_date, end_date=None, max_rows=2000)

    # On mémorise le tableau original pour pouvoir détecter les modifications
    if (
        "table_original_df" not in st.session_state
        or st.session_state.get("table_original_start") != start_date
    ):
        st.session_state["table_original_df"] = df.copy()
        st.session_state["table_original_start"] = start_date

    if df.empty:
        st.warning("Aucune navette à partir de cette date.")
        return

    if "id" not in df.columns:
        st.error("La table `planning` doit contenir une colonne `id` (clé primaire).")
        return

    # Limiter à 40 colonnes max pour garder une vue lisible,
    # mais en gardant les colonnes importantes visibles
    if df.shape[1] > 40:
        priority = ["id", "DATE", "HEURE", "²²²²", "CH", "GO", "GROUPAGE", "PARTAGE"]
        core_cols = [c for c in priority if c in df.columns]
        other_cols = [c for c in df.columns if c not in core_cols]
        max_cols = 40
        keep_cols = core_cols + other_cols[: max_cols - len(core_cols)]
        df = df[keep_cols]


    # On garde les id à part
    ids = df["id"].tolist()
    df_view = df.drop(columns=["id"]).copy().reset_index(drop=True)

    # Colonne de sélection
    df_view.insert(0, "_SELECT", False)

    st.markdown("#### Aperçu (coche une ligne pour l’éditer en bas)")
    edited = st.data_editor(
        df_view,
        use_container_width=True,
        height=400,
        num_rows="fixed",
        key="table_editor",
    )

    # ========= MISE À JOUR DIRECTE DEPUIS LE TABLEAU =========

    # On reconstruit un DataFrame complet avec la colonne id
    df_edited_full = edited.drop(columns=["_SELECT"]).copy()
    df_edited_full.insert(0, "id", ids)

    if st.button("💾 Mettre à jour les modifications du tableau"):
        original = st.session_state.get("table_original_df")
        if original is None or len(original) != len(df_edited_full):
            st.error("Impossible de comparer les modifications (recharge la page ou rechoisis la date).")
        else:
            # On compare ligne par ligne en texte pour voir ce qui a changé
            orig_str = original.set_index("id").astype(str)
            edit_str = df_edited_full.set_index("id").astype(str)

            nb_done = 0
            for rid in ids:
                o = orig_str.loc[rid]
                n = edit_str.loc[rid]
                if not o.equals(n):
                    # Cette ligne a été modifiée dans le tableau
                    row_new = df_edited_full[df_edited_full["id"] == rid].iloc[0].to_dict()
                    row_new.pop("id", None)

                    # Nettoyage des NaN
                    clean: Dict[str, Any] = {}
                    for k, v in row_new.items():
                        if isinstance(v, float) and math.isnan(v):
                            clean[k] = ""
                        else:
                            clean[k] = v

                    update_planning_row(int(rid), clean)
                    nb_done += 1

            if nb_done:
                st.success(f"{nb_done} navette(s) mise(s) à jour depuis le tableau.")
                st.rerun()
            else:
                st.info("Aucun changement détecté dans le tableau.")

    # ========= SÉLECTION POUR LA FICHE DÉTAILLÉE =========

    # Indices cochés
    selected_indices = edited.index[edited["_SELECT"] == True].tolist()


    # Indices cochés
    selected_indices = edited.index[edited["_SELECT"] == True].tolist()
    if selected_indices:
        selected_idx = selected_indices[-1]  # dernière ligne cochée
    else:
        selected_idx = 0  # par défaut première ligne

    selected_ids_for_group = [int(ids[i]) for i in selected_indices] if selected_indices else []
    selected_id = int(ids[selected_idx])
    row_data = get_row_by_id(selected_id)

    # Résumé rapide
    resume_date = row_data.get("DATE", "")
    resume_heure = row_data.get("HEURE", "")
    resume_nom = row_data.get("NOM", "")
    st.markdown(
        f"**Navette sélectionnée :** id `{selected_id}` — "
        f"{resume_date} {resume_heure} — {resume_nom}"
    )

    st.markdown("### 📝 Fiche détaillée")

    cols_names = get_planning_columns()

    priority = ["DATE", "HEURE", "²²²²", "CH", "GO", "GROUPAGE", "PARTAGE"]
    ordered = []

    for c in priority:
        if c in cols_names and c not in ordered:
            ordered.append(c)

    for c in cols_names:
        if c not in ordered:
            ordered.append(c)

    cols_names = ordered[:40]  # on garde 40 max, mais avec GROUPAGE/PARTAGE dedans

    new_values: Dict[str, Any] = {}
    cL, cR = st.columns(2)

    for i, col_name in enumerate(cols_names):
        cont = cL if i % 2 == 0 else cR
        val = row_data.get(col_name)

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
                key=f"edit_DATE_{selected_id}",
            )
            new_values[col_name] = new_d.strftime("%d/%m/%Y")
            continue

        # GROUPAGE / PARTAGE
        if col_name in ["GROUPAGE", "PARTAGE"]:
            b = cont.checkbox(
                "Groupage" if col_name == "GROUPAGE" else "Navette partagée",
                value=bool_from_flag(val),
                key=f"edit_{col_name}_{selected_id}",
            )
            new_values[col_name] = "1" if b else "0"
            continue

        # GO
        if col_name == "GO":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "GO (vide / AL / GO / GL)",
                value=txt,
                key=f"edit_GO_{selected_id}",
            )
            new_values[col_name] = t2.strip().upper()
            continue

        # HEURE
        if col_name == "HEURE":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "HEURE",
                value=txt,
                key=f"edit_HEURE_{selected_id}",
            )
            new_values[col_name] = normalize_time_string(t2)
            continue

        # HEURE DE FIN (²²²²) → on la normalise aussi
        if col_name == "²²²²":
            txt = "" if val is None else str(val)
            t2 = cont.text_input(
                "Heure fin (²²²²)",
                value=txt,
                key=f"edit_2222_{selected_id}",
            )
            new_values[col_name] = normalize_time_string(t2)
            continue

        # Tous les autres champs en simple texte
        txt = "" if val is None or str(val).lower() == "nan" else str(val)
        t2 = cont.text_input(col_name, value=txt, key=f"edit_{col_name}_{selected_id}")
        new_values[col_name] = t2

    st.markdown("#### 🧾 Bloc note")
    st.text_area(
        "Texte libre (non enregistré, juste pour copier/coller)",
        value="",
        key=f"edit_notepad_{selected_id}",
        height=100,
    )

    role = st.session_state.role

    colA, colB, colC, colD, colE = st.columns(5)

    # ---------- Mettre à jour ----------
    with colA:
        if st.button("✅ Mettre à jour"):
            if role_allows_go_gl_only() and not leon_allowed_for_row(row_data.get("GO")):
                st.error("Utilisateur 'leon' : modification autorisée uniquement pour GO / GL.")
            else:
                update_planning_row(selected_id, new_values)
                st.success("Navette mise à jour.")
                st.rerun()

    # ---------- Dupliquer même date ----------
    with colB:
        if st.button("📋 Dupliquer (même date)"):
            if role_allows_go_gl_only() and not leon_allowed_for_row(new_values.get("GO")):
                st.error("Utilisateur 'leon' : création autorisée uniquement pour GO / GL.")
            else:
                clone = new_values.copy()
                insert_planning_row(clone)
                st.success("Navette dupliquée.")
                st.rerun()

    # ---------- Dupliquer pour demain ----------
    with colC:
        if st.button("📆 Dupliquer pour demain"):
            clone = new_values.copy()
            d_txt = clone.get("DATE")
            try:
                d = datetime.strptime(d_txt, "%d/%m/%Y").date()
                d2 = d + timedelta(days=1)
                clone["DATE"] = d2.strftime("%d/%m/%Y")
            except Exception:
                pass

            if role_allows_go_gl_only() and not leon_allowed_for_row(clone.get("GO")):
                st.error("Utilisateur 'leon' : création autorisée uniquement pour GO / GL.")
            else:
                insert_planning_row(clone)
                st.success("Navette dupliquée pour le lendemain.")
                st.rerun()

    # ---------- Dupliquer sur date choisie ----------
    with colD:
        dup_date = st.date_input(
            "Date pour duplication personnalisée",
            value=today,
            key=f"dup_custom_{selected_id}",
        )
        if st.button("📆 Dupliquer sur cette date"):
            clone = new_values.copy()
            clone["DATE"] = dup_date.strftime("%d/%m/%Y")

            if role_allows_go_gl_only() and not leon_allowed_for_row(clone.get("GO")):
                st.error("Utilisateur 'leon' : création autorisée uniquement pour GO / GL.")
            else:
                insert_planning_row(clone)
                st.success(f"Navette dupliquée sur le {clone['DATE']}.")
                st.rerun()

    # ---------- Supprimer ----------
    with colE:
        if st.button("🗑️ Supprimer"):
            if role_allows_go_gl_only() and not leon_allowed_for_row(row_data.get("GO")):
                st.error("Utilisateur 'leon' : suppression autorisée uniquement pour GO / GL.")
            else:
                delete_planning_row(selected_id)
                st.success("Navette supprimée.")
                st.rerun()

    st.markdown("---")
    st.markdown("### 👥 Groupage / Navette partagée (via les coches du tableau)")

    st.caption("Coche les lignes dans la colonne `_SELECT`, puis utilise les boutons ci-dessous.")

    colG1, colG2, colG3 = st.columns(3)

    # ---------- Marquer Groupage ----------
    with colG1:
        if st.button("🔶 Marquer comme Groupage"):
            if not selected_ids_for_group:
                st.warning("Aucune navette cochée.")
            else:
                for rid in selected_ids_for_group:
                    row_g = get_row_by_id(int(rid))
                    if not row_g:
                        continue
                    if role_allows_go_gl_only() and not leon_allowed_for_row(row_g.get("GO")):
                        continue
                    update_planning_row(int(rid), {"GROUPAGE": "1", "PARTAGE": "0"})
                st.success("Navettes marquées comme Groupage.")
                st.rerun()

    # ---------- Marquer Navette partagée ----------
    with colG2:
        if st.button("🟨 Marquer comme Navette partagée"):
            if not selected_ids_for_group:
                st.warning("Aucune navette cochée.")
            else:
                for rid in selected_ids_for_group:
                    row_g = get_row_by_id(int(rid))
                    if not row_g:
                        continue
                    if role_allows_go_gl_only() and not leon_allowed_for_row(row_g.get("GO")):
                        continue
                    update_planning_row(int(rid), {"PARTAGE": "1", "GROUPAGE": "0"})
                st.success("Navettes marquées comme Navette partagée.")
                st.rerun()

    # ---------- Effacer les deux ----------
    with colG3:
        if st.button("⬜ Effacer Groupage / Partagée"):
            if not selected_ids_for_group:
                st.warning("Aucune navette cochée.")
            else:
                for rid in selected_ids_for_group:
                    row_g = get_row_by_id(int(rid))
                    if not row_g:
                        continue
                    if role_allows_go_gl_only() and not leon_allowed_for_row(row_g.get("GO")):
                        continue
                    update_planning_row(int(rid), {"GROUPAGE": "0", "PARTAGE": "0"})
                st.success("Groupage / Partagée effacés pour ces navettes.")
                st.rerun()
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

    st.markdown("#### Sélectionne une navette modèle")
    edited = st.data_editor(
        df_view,
        use_container_width=True,
        height=300,
        num_rows="fixed",
        key="client_editor",
    )

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
#   ENVOI PLANNING À TOUS LES CHAUFFEURS
# ============================================================

def send_planning_to_all_chauffeurs(from_date: date):
    """
    Envoie à chaque chauffeur un mail avec SON planning individuel
    à partir de from_date, et prépare les liens WhatsApp pour
    prévenir qu'un nouveau planning est disponible.
    """
    chs = get_chauffeurs()
    if not chs:
        st.warning("Aucun chauffeur configuré dans Feuil2.")
        return

    # Charger une seule fois tout le planning
    df_all = get_planning(start_date=from_date, end_date=None, max_rows=5000)
    if df_all.empty:
        st.warning(f"Aucune navette à partir du {from_date.strftime('%d/%m/%Y')}.")
        return

    sent = 0
    no_email = []
    wa_links: List[Dict[str, str]] = []

    # --------------- BOUCLE POUR CHAQUE CHAUFFEUR ---------------
    for ch in chs:

        # Téléphone + mail du chauffeur
        tel, mail = get_chauffeur_contact(ch)

        # Filtrer SON planning (on ne fait rien si aucune navette)
        df_ch = df_all[df_all["CH"].astype(str).str.strip().str.upper() == ch.upper()]
        if df_ch.empty:
            continue

        # ------------ MAIL ------------
        msg_txt = build_chauffeur_future_message(df_all, ch, from_date)
        subject = f"Planning à partir du {from_date.strftime('%d/%m/%Y')} — {ch}"

        if mail:
            if send_email_smtp(mail, subject, msg_txt):
                sent += 1
        else:
            no_email.append(ch)

        # ------------ WHATSAPP ------------
        if tel:
            wa_msg = build_chauffeur_new_planning_message(ch, from_date)
            wa_url = build_whatsapp_link(tel, wa_msg)
            wa_links.append({"ch": ch, "tel": tel, "url": wa_url})

    # Résultats MAIL
    st.success(f"Emails envoyés pour {sent} chauffeur(s).")

    if no_email:
        st.info("Pas d'adresse email configurée pour : " + ", ".join(no_email))

    # Résultats WHATSAPP
    if wa_links:
        st.markdown("### 💬 Prévenir les chauffeurs par WhatsApp")
        st.caption(
            "Clique sur chaque lien pour ouvrir WhatsApp avec le message "
            "pré-rempli. Seuls les chauffeurs qui ont des navettes à partir "
            "de cette date et un numéro de GSM apparaissent ici."
        )
        for item in wa_links:
            ch = item["ch"]
            tel = item["tel"]
            url = item["url"]
            st.markdown(f"- {ch} ({tel}) → [Envoyer WhatsApp]({url})")



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


def build_chauffeur_future_message(df: pd.DataFrame, ch_selected: str, from_date: date) -> str:
    """
    Message multi-jours pour un chauffeur à partir d'une date donnée.
    """
    lines: List[str] = []
    lines.append(f"🚖 Planning à partir du {from_date.strftime('%d/%m/%Y')} — Chauffeur : {ch_selected}")
    lines.append("")

    df = df.copy()
    if "DATE" in df.columns:
        try:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
        except Exception:
            pass
        df = df[df["DATE"].notna() & (df["DATE"] >= from_date)]

    if df.empty:
        lines.append("Aucune navette planifiée.")
        return "\n".join(lines)

    if "CH" in df.columns:
        df = df[df["CH"].astype(str).str.strip().str.upper() == ch_selected.upper()]

    if df.empty:
        lines.append("Aucune navette pour ce chauffeur.")
        return "\n".join(lines)

    # Tri
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

    sort_cols = []
    if "DATE" in df.columns:
        sort_cols.append("DATE")
    sort_cols.append("HEURE_SORT")
    df = df.sort_values(sort_cols).drop(columns=["HEURE_SORT"])

    cols = df.columns.tolist()
    grouped = df.groupby("DATE") if "DATE" in df.columns else [(None, df)]

    for d, sub in grouped:
        if isinstance(d, date):
            lines.append(f"📆 {d.strftime('%d/%m/%Y')}")
        else:
            lines.append("📆 Date non définie")

        for _, row in sub.iterrows():
            if is_indispo_row(row, cols):
                h1 = normalize_time_string(row.get("HEURE", ""))
                h2 = normalize_time_string(row.get("²²²²", ""))
                lines.append(f"  ⏱ {h1 or '??:??'} → {h2 or '??:??'} — 🚫 Indisponible")
                lines.append("")
                continue

            heure = normalize_time_string(row.get("HEURE", "")) or "??:??"
            nom = str(row.get("NOM", "") or "").strip()
            designation = str(row.get("DESIGNATION", "") or "").strip()

            route_txt = ""
            for cand in ["Unnamed: 8", "DESIGNATION"]:
                if cand in cols and row.get(cand):
                    route_txt = str(row[cand]).strip()
                    break

            if route_txt and designation and designation not in route_txt:
                dest = f"{route_txt} ({designation})"
            else:
                dest = route_txt or designation or "Navette"

            adresse = str(row.get("ADRESSE", "") or "").strip()
            cp = str(row.get("CP", "") or "").strip()
            loc = str(row.get("Localité", "") or row.get("LOCALITE", "") or "").strip()
            adr_full = " ".join(x for x in [adresse, cp, loc] if x)

            pax = str(row.get("PAX", "") or "").strip()
            paiement = str(row.get("PAIEMENT", "") or "").strip()
            caisse = str(row.get("Caisse", "") or "").strip()

            groupage_flag = bool_from_flag(row.get("GROUPAGE", "0")) if "GROUPAGE" in cols else False
            partage_flag = bool_from_flag(row.get("PARTAGE", "0")) if "PARTAGE" in cols else False

            prefix = ""
            if groupage_flag:
                prefix = "[GRP] "
            elif partage_flag:
                prefix = "[PARTAGE] "

            line1 = f"  {prefix}➡ {heure} — {dest}"
            if nom:
                line1 += f" — {nom}"
            lines.append(line1)

            if adr_full:
                lines.append(f"     📍 {adr_full}")

            extra = []
            if pax:
                extra.append(f"{pax} pax")
            if paiement:
                extra.append(f"Paiement: {paiement}")
            if caisse:
                extra.append(f"Caisse: {caisse} €")
            if extra:
                lines.append("     " + " — ".join(extra))

            if groupage_flag:
                lines.append("     🔶 Groupage")
            elif partage_flag:
                lines.append("     🟨 Navette partagée")

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

# ============================================================
#   ONGLET 🚖 VUE CHAUFFEUR (PC + GSM)
# ============================================================

def render_tab_vue_chauffeur(forced_ch=None):
    st.subheader("🚖 Vue Chauffeur (texte compact)")

    chs = get_chauffeurs_for_ui()


    # ============================
    #   CHOIX DU CHAUFFEUR
    # ============================
    if forced_ch:
        # Chauffeur connecté via GSM
        ch_selected = forced_ch
        st.markdown(f"Chauffeur connecté : **{ch_selected}**")
    else:
        # Admin / bureau : possibilité de laisser vide = tous les chauffeurs
        ch_selected = st.selectbox(
            "Choisir un chauffeur (CH) (laisser vide pour tous les chauffeurs)",
            [""] + chs,
            key="vue_chauffeur_ch",
        )

    today = date.today()

    # ============================
    #   MODE "TOUS LES CHAUFFEURS"
    #   (aucun chauffeur sélectionné)
    # ============================
    if not ch_selected and not forced_ch:
        # Ici on est côté admin/bureau
        if st.session_state.get("role") == "admin":
            st.info(
                "Aucun chauffeur sélectionné : tu es en mode "
                "'tous les chauffeurs' (envoi groupé)."
            )
            st.markdown("---")
            st.markdown("### 📧 Envoi groupé à tous les chauffeurs")

            from_date_all = st.date_input(
                "Envoyer le planning à partir de cette date pour TOUS les chauffeurs :",
                value=today + timedelta(days=1),
                key="vue_chauffeur_all_from",
            )

            if st.button(
                "📤 Envoyer mails + préparer WhatsApp pour tous les chauffeurs",
                key="vue_chauffeur_send_all",
            ):
                send_planning_to_all_chauffeurs(from_date_all)
        else:
            # Cas très théorique (un chauffeur qui arriverait ici sans forced_ch)
            st.info("Sélectionne un chauffeur pour voir tes navettes.")
        return  # on s'arrête ici en mode "tous"

    # ============================
    #   MODE CHAUFFEUR INDIVIDUEL
    # ============================
    tel_ch, mail_ch = get_chauffeur_contact(ch_selected)
    last_ack = get_chauffeur_last_ack(ch_selected)

    scope = st.radio(
        "Période",
        ["Uniquement une date", "À partir de demain"],
        index=0,
        horizontal=True,
        key="vue_chauffeur_scope",
    )

    if scope == "Uniquement une date":
        day_selected = st.date_input(
            "Date",
            value=today,
            key="vue_chauffeur_date",
        )
        df_ch = get_chauffeur_planning(
            ch_selected,
            from_date=day_selected,
            to_date=day_selected,
        )

        if df_ch.empty:
            st.warning(f"Aucune navette pour {ch_selected} le {day_selected.strftime('%d/%m/%Y')}.")
            return

        df_ch = _sort_df_by_date_heure(df_ch)
        render_chauffeur_stats(df_ch)
        day_label = day_selected.strftime("%d/%m/%Y")

        pdf_bytes = create_chauffeur_pdf(df_ch, ch_selected, day_label)
        message_txt = build_chauffeur_day_message(df_ch, ch_selected, day_label)
        mail_subject = f"Planning {day_label} — {ch_selected}"
        mail_body = message_txt
        notif_from_date = day_selected

    else:
        from_date = today + timedelta(days=1)
        df_ch = get_chauffeur_planning(ch_selected, from_date=from_date, to_date=None)

        if df_ch.empty:
            st.warning(f"Aucune navette pour {ch_selected} à partir du {from_date.strftime('%d/%m/%Y')}.")
            return

        df_ch = _sort_df_by_date_heure(df_ch)
        render_chauffeur_stats(df_ch)

        from_label = from_date.strftime("%d/%m/%Y")
        pdf_bytes = create_chauffeur_pdf(df_ch, ch_selected, from_label)

        df_all = get_planning(start_date=from_date, end_date=None, max_rows=5000)
        message_txt = build_chauffeur_future_message(df_all, ch_selected, from_date)
        mail_subject = f"Planning à partir du {from_label} — {ch_selected}"
        mail_body = message_txt
        notif_from_date = from_date

    # Marquer les lignes nouvelles / modifiées pour ce chauffeur
    if "updated_at" in df_ch.columns:
        def _is_new_row(val):
            if last_ack is None:
                # s'il n'a jamais confirmé, on considère tout comme "nouveau"
                return True
            if val is None or val == "":
                return False
            try:
                dt = pd.to_datetime(val, errors="coerce")
            except Exception:
                return False
            if pd.isna(dt):
                return False
            try:
                dt_py = dt.to_pydatetime()
            except AttributeError:
                dt_py = dt
            return dt_py > last_ack

        df_ch["IS_NEW"] = df_ch["updated_at"].apply(_is_new_row)
    else:
        df_ch["IS_NEW"] = False

    # Boutons PDF / WhatsApp / Mail
    col_pdf, col_whats, col_mail = st.columns(3)

    with col_pdf:
        st.download_button(
            "📄 Télécharger la feuille chauffeur (PDF)",
            data=pdf_bytes,
            file_name=f"AirportsLines_{ch_selected}.pdf",
            mime="application/pdf",
        )

    with col_whats:
        if tel_ch:
            # petit message "nouveau planning" + demande de confirmation
            wa_msg = build_chauffeur_new_planning_message(ch_selected, notif_from_date)
            wa_link = build_whatsapp_link(tel_ch, wa_msg)
            st.markdown(f"[💬 Prévenir le chauffeur par WhatsApp]({wa_link})")
        else:
            st.caption("Pas de numéro trouvé pour ce chauffeur (table `chauffeurs`).")

    with col_mail:
        email_key = f"vue_chauffeur_email_{ch_selected}"
        email_default = mail_ch or ""
        email = st.text_input(
            "Adresse e-mail du chauffeur",
            value=email_default,
            key=email_key,
        )

        if email:
            mailto_link = build_mailto_link(email, mail_subject, mail_body)
            st.markdown(f"[📧 Ouvrir un mail avec ce planning]({mailto_link})")

            if email != mail_ch and st.button("💾 Enregistrer cet e-mail pour ce chauffeur"):
                try:
                    with get_connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE chauffeurs SET MAIL = ? WHERE TRIM(INITIALE) = ?",
                            (email, ch_selected),
                        )
                        conn.commit()
                    st.success("E-mail mis à jour pour ce chauffeur.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erreur lors de la mise à jour de l’e-mail : {e}")
        else:
            st.caption("Renseigne un e-mail pour activer le lien mail.")



    # =======================================================
    #   CONFIRMATION DU CHAUFFEUR : PLANNING REÇU
    # =======================================================
    st.markdown("---")
    st.markdown("### ✅ Confirmation de réception du planning")

    if last_ack is None:
        st.info("Tu n'as pas encore confirmé la réception de ton planning.")
    else:
        try:
            ack_txt = last_ack.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            ack_txt = str(last_ack)
        st.caption(f"Dernière confirmation : {ack_txt}")

    if st.button("👍 J'ai bien reçu mon planning et les modifications", key=f"ack_{ch_selected}"):
        set_chauffeur_last_ack(ch_selected)
        st.success("Merci, ta confirmation a bien été enregistrée.")
        st.rerun()

    # =======================================================
    #   DÉTAIL DES NAVETTES (TEXTE COMPACT)
    # =======================================================
    st.markdown("---")
    st.markdown("### 📋 Détail des navettes (texte compact)")

    cols = df_ch.columns.tolist()
    st.caption("Les lignes marquées 🆕 sont celles modifiées depuis ta dernière confirmation.")

    for _, row in df_ch.iterrows():
        bloc_lines: List[str] = []
        is_new = bool(row.get("IS_NEW", False))

        heure_txt = normalize_time_string(row.get("HEURE", ""))

        # Date
        date_val = row.get("DATE", "")
        if isinstance(date_val, (datetime, date)):
            date_txt = date_val.strftime("%d/%m/%Y")
        else:
            date_txt = str(date_val or "").strip()

        # Indispo
        if is_indispo_row(row, cols):
            end_indispo = normalize_time_string(row.get("²²²²", ""))
            header = ""
            if date_txt:
                header += f"📆 {date_txt} | "
            header += f"⏱ {heure_txt or '??:??'} → {end_indispo or '??:??'} | 🚫 Indisponible"
            bloc_lines.append(header)
            ch_txt = str(row.get("CH", "") or ch_selected)
            bloc_lines.append(f"👨‍✈️ {ch_txt}")
            st.markdown("\n".join(bloc_lines))
            st.markdown("---")
            continue

        designation = str(row.get("DESIGNATION", "") or "").strip()
        route_text = ""
        for cand in ["Unnamed: 8", "DESIGNATION"]:
            if cand in cols and row.get(cand):
                route_text = str(row[cand]).strip()
                break

        if route_text and designation and designation not in route_text:
            dest_full = f"{route_text} ({designation})"
        else:
            dest_full = route_text or designation or ""

        groupage_flag = bool_from_flag(row.get("GROUPAGE", "0")) if "GROUPAGE" in cols else False
        partage_flag = bool_from_flag(row.get("PARTAGE", "0")) if "PARTAGE" in cols else False

        prefix = ""
        if groupage_flag:
            prefix = "[GRP] "
        elif partage_flag:
            prefix = "[PARTAGE] "

        header = ""
        if is_new:
            header += "🆕 "

        header += prefix
        if date_txt:
            header += f"📆 {date_txt} | "
        header += f"⏱ {heure_txt or '??:??'}"

        bloc_lines.append(header)

        # si ligne "nouvelle", on met le header en gras
        if is_new:
            bloc_lines[0] = f"**{bloc_lines[0]}**"

        ch_txt = str(row.get("CH", "") or ch_selected)
        bloc_lines.append(f"👨‍✈️ {ch_txt}")

        if dest_full:
            bloc_lines.append(f"➡ {dest_full}")

        nom = str(row.get("NOM", "") or "").strip()
        if nom:
            bloc_lines.append(f"🧑 {nom}")

        adresse = str(row.get("ADRESSE", "") or "").strip()
        cp = str(row.get("CP", "") or "").strip()
        loc = str(row.get("Localité", "") or row.get("LOCALITE", "") or "").strip()
        adr_full = " ".join(x for x in [adresse, cp, loc] if x)
        if adr_full:
            bloc_lines.append(f"📍 {adr_full}")

        pax = str(row.get("PAX", "") or "").strip()
        paiement = str(row.get("PAIEMENT", "") or "").strip()
        caisse = str(row.get("Caisse", "") or row.get("CAISSE", "") or "").strip()
        num_bdc = str(row.get("Num BDC", "") or "").strip()
        go = str(row.get("GO", "") or "").strip()

        pay_parts = []
        if paiement:
            pay_parts.append(f"Paiement : {paiement}")
        if caisse:
            pay_parts.append(f"Caisse : {caisse}")
        if num_bdc:
            pay_parts.append(f"BDC : {num_bdc}")
        if go:
            pay_parts.append(f"GO : {go}")
        if pax:
            pay_parts.append(f"{pax} pax")
        if pay_parts:
            bloc_lines.append("💶 " + " | ".join(pay_parts))

        remarque = str(row.get("REMARQUE", "") or "").strip()
        if remarque:
            bloc_lines.append(f"💬 {remarque}")

        # ================================
        #   Actions GSM : Waze + WhatsApp
        # ================================
        actions = []

        # 1) Lien Waze sur l'adresse
        if adr_full:
            waze_url = build_waze_link(adr_full)
            if waze_url and waze_url != "#":
                actions.append(f"[🧭 Ouvrir Waze]({waze_url})")

        # 2) WhatsApp vers le client (si on a son numéro + GSM chauffeur)
        client_phone = get_client_phone_from_row(row)
        if client_phone and tel_ch:
            msg_client = build_client_sms_from_driver(row, ch_selected, tel_ch)
            wa_client_url = build_whatsapp_link(client_phone, msg_client)
            actions.append(f"[💬 WhatsApp client]({wa_client_url})")

        if actions:
            bloc_lines.append(" | ".join(actions))

        # Affichage final du bloc
        st.markdown("\n".join(bloc_lines))
        st.markdown("---")

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
                "SELECT rowid AS id, * FROM chauffeurs ORDER BY INITIALE",
                conn,
            )
    except Exception as e:
        st.error(f"Erreur en lisant la table `chauffeurs` : {e}")
        return

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
                # On repart de zéro pour éviter les doublons / lignes fantômes
                cur.execute("DELETE FROM chauffeurs")

                cols = [c for c in edited.columns if c != "id"]
                col_list_sql = ",".join(f'"{c}"' for c in cols)
                placeholders = ",".join("?" for _ in cols)

                for _, row in edited.iterrows():
                    values = [row[c] if pd.notna(row[c]) else None for c in cols]
                    cur.execute(
                        f"INSERT INTO chauffeurs ({col_list_sql}) VALUES ({placeholders})",
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
#   ONGLET 📂 EXCEL ↔ DB (FEUIL1)
# ============================================================

def render_tab_excel_sync():
    st.subheader("📂 Synchronisation Excel → Base de données (Feuil1)")

    if import_planning_from_feuil1 is None:
        st.error(
            "La fonction `import_planning_from_feuil1()` n'est pas disponible.\n\n"
            "Vérifie que dans `import_excel_to_db.py` tu as bien une fonction "
            "`import_planning_from_feuil1()` et que tu fais :\n"
            "`from import_excel_to_db import EXCEL_FILE, import_planning_from_feuil1`."
        )
        return

    st.markdown(
        f"""
        **Fichier Excel utilisé :**  
        `{EXCEL_FILE}`

        ---
        🔧 **Workflow conseillé :**

        1. Clique sur **📂 Ouvrir dans Excel**  
           → Tu modifies *Feuil1* comme d'habitude (groupage, couleurs, etc.).  
        2. Tu **enregistres** le fichier Excel.  
        3. Tu reviens ici et cliques sur **🔁 Mettre à jour la base**  
           → La table `planning` est recréée à partir de Feuil1.

        ⚠️ Les couleurs Excel (groupage / partagée / indispo) sont traduites en colonnes
        `GROUPAGE`, `PARTAGE`, `²²²²`… puis réutilisées dans l’app pour les styles.
        """
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("📂 Ouvrir dans Excel"):
            try:
                abs_path = os.path.abspath(EXCEL_FILE)
                os.startfile(abs_path)
                st.success(f"Fichier ouvert dans Excel : {abs_path}")
            except Exception as e:
                st.error(f"Impossible d'ouvrir Excel automatiquement : {e}")
                st.info("Ouvre le fichier manuellement dans l'Explorateur si besoin.")

    with col2:
        if st.button("🔁 Mettre à jour la base depuis Feuil1"):
            try:
                import_planning_from_feuil1()
                st.success("Base de données mise à jour depuis Feuil1 ✅")
                st.toast("Planning synchronisé avec l'Excel.", icon="🚐")
                st.rerun()
            except Exception as e:
                st.error(f"Erreur pendant l'import : {e}")

    st.markdown("---")
    st.info("💡 Tu peux faire toutes tes modifs dans Excel, sauvegarder, puis revenir ici pour recharger la base.")


# ============================================================
#   ONGLET 📦 ADMIN TRANSFERTS (LISTE GLOBALE)
# ============================================================

def render_tab_admin_transferts():
    st.subheader("📦 Tous les transferts — vue admin")

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

    # Tri : par défaut Date + Heure, sinon Chauffeur + Date + Heure
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

    today = date.today()
    col_sms1, col_sms2 = st.columns(2)

    # Bouton : demain
    with col_sms1:
        if st.button("📅 Préparer SMS/WhatsApp pour demain", key="sms_clients_demain"):
            target = today + timedelta(days=1)
            # ⚠️ IMPORTANT : df = planning filtré COMPLET (pas df_display)
            show_client_messages_for_period(df, target, nb_days=1)

    # Bouton : 3 prochains jours
    with col_sms2:
        if st.button("📅 Préparer SMS/WhatsApp pour les 3 prochains jours", key="sms_clients_3j"):
            target = today + timedelta(days=1)
            show_client_messages_for_period(df, target, nb_days=3)

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
    # s'assurer que la colonne updated_at existe et que la table d'ack est prête
    ensure_planning_updated_at_column()
    init_indispo_table()
    init_chauffeur_ack_table()

    # init session
    init_session_state()

    # Si pas connecté → écran de login
    if not st.session_state.logged_in:
        login_screen()
        return

    # Barre du haut (titre + info utilisateur + bouton déconnexion)
    render_top_bar()

    role = st.session_state.role

    # ====================== ADMIN ===========================
    # ====================== ADMIN ===========================
    if role == "admin":
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(
            [
                "📅 Planning",
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
            render_tab_table()

        with tab3:
            render_tab_clients()

        with tab4:
            render_tab_vue_chauffeur()

        with tab5:
            render_tab_chauffeurs()

        with tab6:
            render_tab_feuil3()

        with tab7:
            render_tab_admin_transferts()

        with tab8:
            render_tab_excel_sync()

        with tab9:
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
        # Chauffeur : uniquement la vue chauffeur, filtrée sur son code
        ch_code = st.session_state.get("chauffeur_code")
        if not ch_code:
            st.error("Aucun code chauffeur configuré pour cet utilisateur.")
            return

        tab1, tab2 = st.tabs(["🚖 Mes navettes", "🚫 Mes indispos"])
        with tab1:
            render_tab_vue_chauffeur(forced_ch=ch_code)
        with tab2:
            render_tab_indispo_driver(ch_code)

    # ==================== AUTRE RÔLE INCONNU = ERREUR ======
    else:
        st.error(f"Rôle inconnu : {role}")


if __name__ == "__main__":
    main()
