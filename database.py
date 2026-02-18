import sqlite3
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
import hashlib


import os
import sys
from utils import debug_print, debug_enabled

debug_print("DATABASE LOADED:", __file__)
from utils import log_event


def sqlite_safe(value):
    """
    Convertit une valeur en type compatible SQLite
    SANS JAMAIS MODIFIER LES DATES
    """
    if value is None:
        return None

    # pandas NaN
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # datetime / date → string ISO ou string Excel
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")

    return str(value)


# =========================
#   NORMALISATION PAIEMENT / CAISSE (compat)
# =========================
def normalize_payment_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise les champs liés au paiement / caisse.

    Compatibilité avec l'historique :
    - CAISSE_OK (si présent) -> CAISSE_PAYEE
    - CAISSE_MONTANT (si présent) -> "Caisse" si absent
    - accepte des valeurs texte : 'payé', 'ok', 'x', '1', etc.
    """
    if not isinstance(data, dict):
        return data

    # Alias colonnes
    if "CAISSE_OK" in data and "CAISSE_PAYEE" not in data:
        data["CAISSE_PAYEE"] = data.get("CAISSE_OK")

    if "CAISSE_MONTANT" in data and "Caisse" not in data and "CAISSE" not in data:
        data["Caisse"] = data.get("CAISSE_MONTANT")

    # Normalise IS_PAYE si fourni en texte
    if "IS_PAYE" in data:
        v = data.get("IS_PAYE")
        if isinstance(v, str):
            s = v.strip().lower()
            data["IS_PAYE"] = 1 if s in ("1", "true", "yes", "ok", "payé", "paye", "x") else 0

    # Normalise CAISSE_PAYEE si fourni en texte
    if "CAISSE_PAYEE" in data:
        v = data.get("CAISSE_PAYEE")
        if isinstance(v, str):
            s = v.strip().lower()
            data["CAISSE_PAYEE"] = 1 if s in ("1", "true", "yes", "ok", "payé", "paye", "x") else 0

    return data


def ensure_payment_columns():
    """Ajoute les colonnes paiement/caisse modernes si absentes (sans casser l'existant)."""
    with get_connection() as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(planning)").fetchall()}

        if "IS_PAYE" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "IS_PAYE" INTEGER DEFAULT 0')

        # colonne historique caisse déjà utilisée dans l'app
        if "CAISSE_PAYEE" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "CAISSE_PAYEE" INTEGER DEFAULT 0')
        if "CAISSE_PAYEE_AT" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "CAISSE_PAYEE_AT" TEXT')
        if "CAISSE_COMMENT" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "CAISSE_COMMENT" TEXT')

        if "LOCKED_BY_APP" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "LOCKED_BY_APP" INTEGER DEFAULT 0')

        if "IS_NEW" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "IS_NEW" INTEGER DEFAULT 0')

        conn.commit()


def apply_row_update(
    row_id: int,
    patch: Dict[str, Any],
    *,
    lock_row: bool = False,
    touch_is_new: bool = False,
) -> None:
    """Bloc unique : applique une mise à jour DB de façon cohérente et future-proof.

    - Normalise paiement/caisse
    - Pose updated_at
    - Option : LOCKED_BY_APP=1 (modif via app / chauffeur)
    - Option : IS_NEW=1 (modif récente à afficher)
    """
    if not patch:
        return

    ensure_planning_updated_at_column()
    ensure_payment_columns()

    patch = normalize_payment_fields(dict(patch))

    if "DATE" in patch:
        patch["DATE"] = _normalize_date_str(patch["DATE"])

    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    patch["updated_at"] = now_iso

    if lock_row:
        patch["LOCKED_BY_APP"] = 1

    if touch_is_new:
        patch["IS_NEW"] = 1

    set_parts = []
    values: List[Any] = []
    for col, val in patch.items():
        set_parts.append(f'"{col}" = ?')
        values.append(sqlite_safe(val))

    values.append(int(row_id))
    set_clause = ", ".join(set_parts)

    with get_connection() as conn:
        conn.execute(f"UPDATE planning SET {set_clause} WHERE id = ?", values)
        conn.commit()
# =========================
#   CONFIG BASE DE DONNÉES
# =========================
DB_PATH = "airportslines.db"
ACTIONS_DB_PATH = "planning_actions.db"


# =========================
#   OUTILS INTERNES
# =========================
def get_connection() -> sqlite3.Connection:
    """Connexion d'écriture SQLite (sans WAL pour éviter .db-wal/.db-shm)."""

    conn = sqlite3.connect(
        DB_PATH,
        timeout=60,
        check_same_thread=False,
    )

    # 🐞 Trace SQL (très verbeux) si AL_DEBUG=1
    try:
        if debug_enabled():
            conn.set_trace_callback(lambda x: debug_print("SQL:", x))
    except Exception:
        pass

    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    return conn

def _normalize_date_str(d: Any) -> str:
    """
    Utilitaire : convertit une date (datetime.date ou str)
    vers le format dd/mm/YYYY, sans warning pandas.
    """
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return ""

    if isinstance(d, date):
        return d.strftime("%d/%m/%Y")

    s = str(d).strip()
    if not s:
        return ""

    try:
        # Cas ISO déjà normalisé : YYYY-MM-DD
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            v = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        else:
            # Cas Excel / européen : DD/MM/YYYY
            v = pd.to_datetime(s, dayfirst=True, errors="coerce")

        if pd.isna(v):
            return s

        return v.strftime("%d/%m/%Y")

    except Exception:
        return s


def _to_date_any(val: Any) -> date | None:
    """Parse robuste -> date.

    Accepte:
    - datetime/date
    - 'YYYY-MM-DD'
    - 'YYYY/MM/DD'
    - 'DD/MM/YYYY' (dayfirst)
    - autres formats reconnus par pandas
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val

    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None

    # Normalise séparateurs fréquents
    s2 = s.replace(".", "/").replace("-", "-")

    # 1) Essai direct (ISO / pandas)
    try:
        dt = pd.to_datetime(s2, errors="coerce")
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass

    # 2) Essai européen (DD/MM)
    try:
        dt = pd.to_datetime(s2, dayfirst=True, errors="coerce")
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass

    # 3) Essai yearfirst (YYYY/MM)
    try:
        dt = pd.to_datetime(s2, yearfirst=True, errors="coerce")
        if not pd.isna(dt):
            return dt.date()
    except Exception:
        pass

    return None


def ensure_date_iso_populated() -> int:
    """Ajoute/backfill la colonne DATE_ISO (YYYY-MM-DD).

    Retourne le nombre de lignes mises à jour.
    Objectif: éviter les vues vides / filtres cassés quand DATE contient
    des formats hétérogènes (DD/MM/YYYY, YYYY/MM/DD, etc.).
    """
    updated = 0
    with get_connection() as conn:
        cur = conn.cursor()

        # Colonne existante ?
        cur.execute("PRAGMA table_info(planning)")
        cols = {row[1] for row in cur.fetchall()}
        if "DATE_ISO" not in cols:
            cur.execute('ALTER TABLE planning ADD COLUMN "DATE_ISO" TEXT')
            conn.commit()

        # Backfill uniquement si vide
        cur.execute(
            """
            SELECT id, DATE, DATE_ISO
            FROM planning
            WHERE COALESCE(DATE_ISO,'') = ''
               OR DATE_ISO IS NULL
            """
        )
        rows = cur.fetchall()

        for rid, date_txt, date_iso in rows:
            d = _to_date_any(date_txt)
            if d is None:
                continue
            iso = d.strftime("%Y-%m-%d")
            cur.execute(
                "UPDATE planning SET DATE_ISO = ? WHERE id = ?",
                (iso, int(rid)),
            )
            updated += 1

        conn.commit()

    return updated

def ensure_admin_reply_columns():
    with get_connection() as conn:
        cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(planning)"
            ).fetchall()
        }

        if "ADMIN_REPLY" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ADMIN_REPLY" TEXT'
            )

        if "ADMIN_REPLY_AT" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ADMIN_REPLY_AT" TEXT'
            )

        if "ADMIN_REPLY_READ" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ADMIN_REPLY_READ" INTEGER DEFAULT 0'
            )

        conn.commit()
def ensure_ch_manual_column():
    """
    Ajoute la colonne CH_MANUAL si absente.
    Utilisée quand un chauffeur modifie manuellement son code.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        cols = {row[1] for row in cur.fetchall()}

        if "CH_MANUAL" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CH_MANUAL" TEXT'
            )

        conn.commit()


def _normalize_heure_str(h: Any) -> str:
    """
    Utilitaire pour trier les heures : retourne HH:MM propre quand possible.
    """
    if h is None:
        return ""
    s = str(h).strip().replace("H", "h").replace("h", ":")
    if not s:
        return ""
    if ":" in s:
        parts = s.split(":")
        if len(parts) >= 2:
            try:
                hh = int(parts[0])
                mm = int(parts[1])
                if 0 <= hh <= 23 and 0 <= mm <= 59:
                    return f"{hh:02d}:{mm:02d}"
            except Exception:
                return s
        return s
    if s.isdigit():
        if len(s) <= 2:
            try:
                hh = int(s)
                mm = 0
            except Exception:
                return s
        else:
            try:
                hh = int(s[:-2])
                mm = int(s[-2:])
            except Exception:
                return s
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
        return s
    return s

def get_read_connection():
    """Connexion lecture (mêmes PRAGMA que écriture)."""
    conn = sqlite3.connect(
        DB_PATH,
        timeout=60,
        check_same_thread=False,
    )
    try:
        conn.execute("PRAGMA journal_mode=DELETE;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return conn

# =========================
#   CHARGEMENT GLOBAL PLANNING
# =========================

def _load_planning_df() -> pd.DataFrame:
    """
    Charge la table 'planning' complète en DataFrame.
    """
    with get_connection() as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM planning", conn)
        except Exception:
            return pd.DataFrame()

    if df.empty:
        return df

    # Colonnes de compatibilité
    if "GROUPAGE" not in df.columns:
        df["GROUPAGE"] = "0"
    if "PARTAGE" not in df.columns:
        df["PARTAGE"] = "0"
    if "IS_INDISPO" not in df.columns:
        df["IS_INDISPO"] = 0

    return df


@st.cache_data
def get_planning(
    start_date=None,
    end_date=None,
    chauffeur=None,
    type_filter=None,
    search="",
    max_rows=2000,
    source="7j",   # "day" | "7j" | "full"
) -> pd.DataFrame:
    """
    Retourne un DataFrame filtré depuis la DB.

    RÈGLE MÉTIER MAÎTRE :
        IS_INDISPO = 1  ➜  ligne TOUJOURS visible

    RÈGLE CRITIQUE :
        source="full" ➜ AUCUN filtre date
    """

    # =========================
    # Choix de la table
    # =========================
    if source == "day":
        table = "planning_day"
    elif source == "full":
        table = "planning_full"
    else:
        table = "planning_7j"

    # =========================
    # Lecture DB
    # =========================
    try:
        with get_read_connection() as conn:
            try:
                df = pd.read_sql_query(
                    f"""
                    SELECT *
                    FROM {table}
                    ORDER BY DATE_ISO, HEURE
                    LIMIT ?
                    """,
                    conn,
                    params=(max_rows,),
                )
            except Exception:
                df = pd.read_sql_query(
                    f"""
                    SELECT *
                    FROM {table}
                    ORDER BY DATE, HEURE
                    LIMIT ?
                    """,
                    conn,
                    params=(max_rows,),
                )
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    # =========================
    # Colonnes garanties
    # =========================
    if "IS_INDISPO" not in df.columns:
        df["IS_INDISPO"] = 0

    # =========================
    # Masquage superseded
    # =========================
    if "IS_SUPERSEDED" in df.columns:
        df = df[df["IS_SUPERSEDED"] != 1].copy()

    # =====================================================
    # 🔒 NOTE: en source='full', on garde le full scan,
    # mais si start_date/end_date sont fournis on filtre aussi.
    # =====================================================

    # =========================
    # Conversion DATE propre
    # =========================
    if "DATE_ISO" in df.columns and df["DATE_ISO"].notna().any():
        dt = pd.to_datetime(df["DATE_ISO"], errors="coerce")
    elif "DATE" in df.columns:
        dt = pd.to_datetime(df["DATE"], dayfirst=True, errors="coerce")
    else:
        dt = pd.to_datetime(pd.Series([None] * len(df)), errors="coerce")

    df["DATE"] = dt.dt.date

    # =========================
    # Filtre date (ROBUSTE)
    # ⚠️ Les indispos passent TOUJOURS
    # =========================
    if start_date or end_date:

        start_d = _to_date_any(start_date)
        end_d = _to_date_any(end_date)

        def _keep_date(iso_val, is_indispo):
            if int(is_indispo or 0) == 1:
                return True

            d = _to_date_any(iso_val)
            if d is None:
                return False

            if start_d and d < start_d:
                return False
            if end_d and d > end_d:
                return False

            return True

        df = df[
            df.apply(
                lambda r: _keep_date(
                    r.get("DATE_ISO"),
                    r.get("IS_INDISPO", 0),
                ),
                axis=1,
            )
        ].copy()

    # =========================
    # Filtre chauffeur
    # =========================
    if chauffeur and "CH" in df.columns:
        ch = str(chauffeur).strip().upper()
        ch_series = df["CH"].fillna("").astype(str).str.strip().str.upper()

        mask = (
            ch_series.eq(ch)
            | ch_series.eq(f"{ch}*")
            | ch_series.str.contains(ch, regex=False)
        )

        df = df[mask | (df["IS_INDISPO"] == 1)].copy()

    # =========================
    # Filtre type AL / GO_GL
    # =========================
    if "GO" in df.columns and type_filter:
        go_series = df["GO"].fillna("").astype(str).str.strip().str.upper()

        if type_filter == "AL":
            df = df[(~go_series.isin(["GO", "GL"])) | (df["IS_INDISPO"] == 1)].copy()
        elif type_filter == "GO_GL":
            df = df[(go_series.isin(["GO", "GL"])) | (df["IS_INDISPO"] == 1)].copy()

    # =========================
    # Recherche texte
    # =========================
    if search and str(search).strip():
        s = str(search).lower().strip()

        def _row_match(row):
            for col in ["NOM", "ADRESSE", "REMARQUE", "VOL", "NUM_BDC", "Num BDC", "DESIGNATION"]:
                if col in row and s in str(row[col]).lower():
                    return True
            return False

        df = df[df.apply(_row_match, axis=1) | (df["IS_INDISPO"] == 1)].copy()

    # =========================
    # Tri DATE + HEURE
    # =========================
    def _heure_sort(h):
        h2 = _normalize_heure_str(h)
        if not h2 or ":" not in h2:
            return (99, 99)
        try:
            hh, mm = h2.split(":")
            return (int(hh), int(mm))
        except Exception:
            return (99, 99)

    if "HEURE" in df.columns:
        df["_HSORT"] = df["HEURE"].apply(_heure_sort)
    else:
        df["_HSORT"] = [(99, 99)] * len(df)

    sort_cols = []
    if "DATE" in df.columns:
        sort_cols.append("DATE")
    sort_cols.append("_HSORT")

    df = df.sort_values(sort_cols).drop(columns=["_HSORT"], errors="ignore")

    if max_rows and len(df) > max_rows:
        df = df.head(max_rows)

    return df


def get_planning_columns() -> List[str]:
    """
    Retourne la liste des colonnes de la table planning (dans l'ordre SQLite).
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        rows = cur.fetchall()
    cols = [r[1] for r in rows]  # r[1] = name
    return cols

# ============================================================
#   🧹 CLEANUP — NAVETTES FANTÔMES
# ============================================================

def cleanup_orphan_planning_rows(last_sync_ts: str, silent: bool = False):
    """
    Supprime les navettes absentes d’Excel,
    uniquement si aucune action humaine n’a eu lieu.
    """
    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM planning
            WHERE
                (EXCEL_SYNC_TS IS NULL OR EXCEL_SYNC_TS < ?)
                AND COALESCE(CONFIRMED, 0) = 0
                AND COALESCE(IS_PAYE, 0) = 0
                AND COALESCE(ACK_AT, '') = ''
                AND COALESCE(LOCKED_BY_APP,0) = 0
                AND DATE >= DATE('now', '-7 days')
            """,
            (last_sync_ts,),
        )
        conn.commit()

# =========================
#   CHAUFFEURS
# =========================
def get_chauffeurs() -> List[str]:
    """
    Retourne la liste des codes chauffeurs (CH) distincts, triés.

    On fusionne :
    - les INITIALE de la table 'chauffeurs' (Feuil2)
    - toutes les valeurs distinctes de la colonne CH du planning (Feuil1)

    On ne modifie rien dans la base : c'est uniquement pour l'interface.
    """
    base: List[str] = []
    extra: List[str] = []

    # 1) Codes de la table chauffeurs (Feuil2)
    try:
        with get_connection() as conn:
            df_ch = pd.read_sql_query("SELECT INITIALE FROM chauffeurs", conn)
        if not df_ch.empty and "INITIALE" in df_ch.columns:
            base = (
                df_ch["INITIALE"]
                .astype(str)
                .map(lambda x: x.strip() if x is not None else "")
                .replace("", pd.NA)
                .dropna()
                .tolist()
            )
    except Exception:
        base = []

    # 2) Codes réels présents dans la colonne CH du planning (Feuil1)
    try:
        df_pl = _load_planning_df()
        if not df_pl.empty and "CH" in df_pl.columns:
            extra = (
                df_pl["CH"]
                .astype(str)
                .map(lambda x: x.strip() if x is not None else "")
                .replace("", pd.NA)
                .dropna()
                .tolist()
            )
    except Exception:
        extra = []

    # 3) Fusion sans doublons, en gardant l'écriture exacte (AD, AD*, FA*, FA1, …)
    all_codes: List[str] = []
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

    # Tri alphabétique insensible à la casse (mais on garde la forme originale)
    all_codes = sorted(all_codes, key=lambda x: x.upper())
    return all_codes
# ============================================================
#   CACHE GLOBAL — CHAUFFEURS POUR split_chauffeurs
# ============================================================

_SPLIT_KNOWN_CHAUFFEURS = None


def _load_known_chauffeurs_once():
    global _SPLIT_KNOWN_CHAUFFEURS

    if _SPLIT_KNOWN_CHAUFFEURS is not None:
        return _SPLIT_KNOWN_CHAUFFEURS

    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                "SELECT INITIALE FROM chauffeurs",
                conn
            )

        _SPLIT_KNOWN_CHAUFFEURS = (
            df["INITIALE"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )

    except Exception:
        # fallback sécurité (ne bloque jamais l’app)
        _SPLIT_KNOWN_CHAUFFEURS = [
            "FA1", "FA", "DO", "NP", "AD", "GG", "MA", "OM"
        ]

    return _SPLIT_KNOWN_CHAUFFEURS


# ============================================================
#   SPLIT CHAUFFEURS — VERSION OPTIMISÉE
# ============================================================

def split_chauffeurs(ch_raw: str) -> list[str]:
    """
    Décompose un code chauffeur du planning en chauffeurs réels.
    Version sécurisée et performante.
    """

    if not ch_raw:
        return []

    # 🔒 NORMALISATION FORTE
    raw = (
        str(ch_raw)
        .upper()
        .replace("*", "")
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
        .replace("+", "")
        .replace(",", "")
        .strip()
    )

    if not raw:
        return []

    known = _load_known_chauffeurs_once()

    # ⚡ tri UNE SEULE FOIS
    if not hasattr(split_chauffeurs, "_known_sorted"):
        split_chauffeurs._known_sorted = sorted(
            set(known),
            key=len,
            reverse=True
        )

    known_sorted = split_chauffeurs._known_sorted

    found = []
    remaining = raw

    # ⛔ garde-fou anti-boucle lente
    max_iter = len(raw) * 2
    i = 0

    while remaining and i < max_iter:
        i += 1
        matched = False

        for ch in known_sorted:
            if remaining.startswith(ch):
                found.append(ch)
                remaining = remaining[len(ch):]
                matched = True
                break

        if not matched:
            remaining = remaining[1:]

    # suppression doublons en conservant l’ordre
    result = []
    seen = set()
    for ch in found:
        if ch not in seen:
            seen.add(ch)
            result.append(ch)

    return result




@st.cache_data
def get_chauffeur_planning(
    chauffeur: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Planning chauffeur — FILTRE STRICT
    - respecte les codes chauffeurs (FA, FA*, FADO, etc.)
    - masque les indispos / congés
    - applique strictement from_date / to_date
    """

    ch = (chauffeur or "").strip()
    if not ch:
        return pd.DataFrame()

    df = get_planning(
        start_date=from_date,
        end_date=to_date,
        chauffeur=ch,
        type_filter=None,
        search="",
        max_rows=5000,
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # 🔴 RÈGLE SPÉCIFIQUE VUE CHAUFFEUR
    if from_date:
        df = df[
            (df["DATE"] >= from_date)
            & (df["IS_INDISPO"] == 0)
        ].copy()

    return df



# =========================
#   RECHERCHE CLIENT
# =========================

def search_client(query, max_rows=500):
    """
    Recherche client dans TOUT l'historique (planning_full),
    colonnes dynamiques (robuste aux changements Excel),
    avec TRI DATE correct (passé + futur).
    """
    q = f"%{query.strip()}%"

    with get_connection() as conn:
        # 1) récupérer les colonnes réelles de la table
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning_full)")
        cols = [row[1] for row in cur.fetchall()]

        if not cols:
            return pd.DataFrame()

        # 2) colonnes candidates à la recherche (ordre logique)
        preferred = [
            "NOM",
            "ADRESSE",
            "REMARQUE",
            "NUM_BDC",
            "CLIENT",
            "CONTACT",
            "VILLE",
        ]

        search_cols = [c for c in preferred if c in cols]

        # fallback de sécurité
        if not search_cols:
            search_cols = cols[:3]

        # 3) construction dynamique du WHERE
        where_sql = " OR ".join([f'"{c}" LIKE ?' for c in search_cols])
        params = [q] * len(search_cols) + [max_rows]

        # 4) TRI DATE CORRECT (dd/mm/yyyy -> yyyy-mm-dd)
        sql = f"""
            SELECT *
            FROM planning_full
            WHERE {where_sql}
            ORDER BY
                substr(DATE, 7, 4) || '-' ||
                substr(DATE, 4, 2) || '-' ||
                substr(DATE, 1, 2) DESC
            LIMIT ?
        """

        df = pd.read_sql_query(sql, conn, params=params)

    return df





    # On ne peut pas filtrer directement sur des dates ici, on renvoie tout ce qui correspond au nom
    # Identification de la colonne NOM (il peut y avoir des variantes, mais chez toi c'est NOM)
    client_col = None
    for col in ["NOM", "Nom", "Client"]:
        if col in df.columns:
            client_col = col
            break

    if not client_col:
        return pd.DataFrame()

    mask = df[client_col].astype(str).str.contains(client_name_part.strip(), case=False, na=False)
    df_res = df[mask].copy()

    # Tri par DATE + HEURE si possible
    if "HEURE" in df_res.columns:
        def _heure_sort_tuple(h):
            h2 = _normalize_heure_str(h)
            if not h2 or ":" not in h2:
                return (99, 99)
            try:
                hh, mm = h2.split(":")
                return (int(hh), int(mm))
            except Exception:
                return (99, 99)

        df_res["_HSORT"] = df_res["HEURE"].apply(_heure_sort_tuple)
    else:
        df_res["_HSORT"] = (99, 99)

    sort_cols: List[str] = []
    if "DATE" in df_res.columns:
        sort_cols.append("DATE")
    sort_cols.append("_HSORT")
    df_res = df_res.sort_values(sort_cols).drop(columns=["_HSORT"], errors="ignore")

    if max_rows and len(df_res) > max_rows:
        df_res = df_res.head(max_rows)

    return df_res


# =========================
#   CRUD LIGNES PLANNING
# =========================

def get_row_by_id(row_id: int) -> Optional[Dict[str, Any]]:
    """
    Retourne une ligne planning sous forme de dict, ou None si introuvable.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM planning WHERE id = ?", (row_id,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        data = {cols[i]: row[i] for i in range(len(cols))}
    return data

def insert_planning_row(
    data: Dict[str, Any],
    ignore_conflict: bool = False,
) -> int:
    """
    Insère une nouvelle navette dans la table planning.

    - ignore_conflict=True  → INSERT OR IGNORE (anti-doublon)
    - utilise row_key comme clé logique unique
    - retourne :
        • id créé
        • -1 si insertion ignorée (doublon)
    """

    if not data:
        return -1

    # ✅ S'assurer que la colonne updated_at existe
    ensure_planning_updated_at_column()

    # S'assure que LOCKED_BY_APP existe
    with get_connection() as conn:
        cols = {r[1] for r in conn.execute('PRAGMA table_info(planning)').fetchall()}
        if 'LOCKED_BY_APP' not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "LOCKED_BY_APP" INTEGER DEFAULT 0')
        conn.commit()

    # ✅ Normaliser DATE en texte dd/mm/YYYY (logique existante)
    if "DATE" in data:
        data["DATE"] = _normalize_date_str(data["DATE"])

    # ⚠️ row_key STRICTEMENT obligatoire
    if not data.get("row_key"):
        # Interdiction ABSOLUE de recalculer le row_key ici
        return -1

    # Timestamp de mise à jour
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 🔒 Si une ligne est modifiée manuellement dans l’app (ex: changement chauffeur), on la verrouille
    # pour éviter qu’un sync Excel en arrière-plan n’écrase la modif.
    if int(data.get("CH_MANUAL", 0) or 0) == 1:
        try:
            data["LOCKED_BY_APP"] = 1
        except Exception:
            pass

    cols = list(data.keys())
    col_list_sql = ",".join(f'"{c}"' for c in cols)
    placeholders = ",".join("?" for _ in cols)
    values = [sqlite_safe(data[c]) for c in cols]

    insert_mode = "INSERT OR IGNORE" if ignore_conflict else "INSERT"

    sql = f"""
        {insert_mode}
        INTO planning ({col_list_sql})
        VALUES ({placeholders})
    """

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()

        # 🔒 Si OR IGNORE → aucun insert → lastrowid = 0
        if ignore_conflict and cur.rowcount == 0:
            return -1

        return cur.lastrowid

def is_new_ack(prev_ack_at, new_ack_at) -> bool:
    """
    Retourne True si une réponse chauffeur vient d’être reçue
    (ACK_AT passe de vide -> valeur)
    """
    return not prev_ack_at and bool(new_ack_at)


def update_planning_row(row_id: int, data: Dict[str, Any]) -> None:
    """
    Met à jour une navette existante (par id) avec les colonnes fournies.
    Déclenche une notification admin si un chauffeur répond.
    """
    if not data:
        return

    # --------------------------------------------------
    # 🔍 État AVANT modification (pour détecter ACK)
    # --------------------------------------------------
    prev_ack_at = None
    if "ACK_AT" in data:
        old_row = get_row_by_id(row_id)
        if old_row:
            prev_ack_at = old_row.get("ACK_AT")

    # --------------------------------------------------
    # ✅ Update cohérent (apply_row_update)
    # --------------------------------------------------
    apply_row_update(row_id, data)


    # --------------------------------------------------
    # 🔔 NOTIFICATION ADMIN : nouvelle réponse chauffeur
    # --------------------------------------------------
    if "ACK_AT" in data:
        new_ack_at = data.get("ACK_AT")
        if is_new_ack(prev_ack_at, new_ack_at):
            st.session_state["admin_notif"] = {
                "type": "ACK",
                "row_id": row_id,
            }

def get_planning_table_columns() -> set[str]:
    """
    Retourne l'ensemble des colonnes existantes
    dans la table planning (SQLite).
    """
    with get_connection() as conn:
        cur = conn.execute("PRAGMA table_info(planning)")
        return {row[1] for row in cur.fetchall()}
def ensure_planning_row_key_column():
    """
    S'assure que la colonne row_key existe dans la table planning.
    """
    with get_connection() as conn:
        cur = conn.execute("PRAGMA table_info(planning)")
        cols = {row[1] for row in cur.fetchall()}

        if "row_key" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "row_key" TEXT')
            conn.commit()
def ensure_planning_audit_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS planning_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                user TEXT,
                action TEXT,
                row_key TEXT,
                details TEXT
            )
        """)
        conn.commit()
def ensure_excel_sync_column():
    with get_connection() as conn:
        cols = [
            r[1]
            for r in conn.execute(
                "PRAGMA table_info(planning)"
            ).fetchall()
        ]

        if "EXCEL_SYNC_TS" not in cols:
            conn.execute(
                "ALTER TABLE planning ADD COLUMN EXCEL_SYNC_TS TEXT"
            )
            conn.commit()


def ensure_planning_row_key_index():
    with get_connection() as conn:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_planning_row_key "
            "ON planning(row_key)"
        )
        conn.commit()

def ensure_planning_confirmation_and_caisse_columns():
    """
    Colonnes métier stables (NE JAMAIS recalculer depuis Excel)
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        existing_cols = {row[1] for row in cur.fetchall()}

        if "CONFIRMED" not in existing_cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CONFIRMED" INTEGER DEFAULT 0'
            )

        if "CONFIRMED_AT" not in existing_cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CONFIRMED_AT" TEXT'
            )

        if "CAISSE_PAYEE" not in existing_cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CAISSE_PAYEE" INTEGER DEFAULT 0'
            )

        if "ACK_AT" not in existing_cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ACK_AT" TEXT'
            )

        if "ACK_TEXT" not in existing_cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ACK_TEXT" TEXT'
            )

        conn.commit()

def set_caisse_payee_for_ids(ids: list[int], payee: int = 1):
    if not ids:
        return 0

    with get_connection() as conn:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in ids)
        cur.execute(
            f'UPDATE planning SET "CAISSE_PAYEE" = ? WHERE id IN ({placeholders})',
            [int(payee)] + [int(x) for x in ids],
        )
        conn.commit()
        return cur.rowcount


def set_caisse_payee_for_chauffeur(ch: str, from_date: str, to_date: str, payee: int = 1):
    """
    from_date / to_date au format YYYY-MM-DD (DATE_ISO)
    """
    ch = str(ch or "").strip()
    if not ch:
        return 0

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE planning
            SET "CAISSE_PAYEE" = ?
            WHERE DATE_ISO >= ?
              AND DATE_ISO <= ?
              AND UPPER(TRIM(COALESCE(CH,''))) = UPPER(TRIM(?))
              AND LOWER(TRIM(COALESCE(PAIEMENT,''))) = 'caisse'
              AND CAST(COALESCE(Caisse, 0) AS REAL) > 0
            """,
            (int(payee), from_date, to_date, ch),
        )
        conn.commit()
        return cur.rowcount

def ensure_caisse_columns():
    """
    Ajoute les colonnes liées à la caisse si elles n'existent pas encore.
    Migration sûre (SQLite).
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        cols = {row[1] for row in cur.fetchall()}

        if "CAISSE_PAYEE" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CAISSE_PAYEE" INTEGER DEFAULT 0'
            )

        if "CAISSE_PAYEE_AT" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CAISSE_PAYEE_AT" TEXT'
            )

        if "CAISSE_COMMENT" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "CAISSE_COMMENT" TEXT'
            )

        conn.commit()

def ensure_urgence_columns():
    """Ajoute les colonnes urgences dans `planning` si manquantes."""
    with get_connection() as conn:
        cols = get_planning_table_columns()

        if "URGENCE" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "URGENCE" INTEGER DEFAULT 0')
        if "URGENCE_STATUS" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "URGENCE_STATUS" TEXT')
        if "URGENCE_CREATED_AT" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "URGENCE_CREATED_AT" TEXT')
        if "URGENCE_NOTIFIED_AT" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "URGENCE_NOTIFIED_AT" TEXT')
        if "URGENCE_CHANNEL" not in cols:
            conn.execute('ALTER TABLE planning ADD COLUMN "URGENCE_CHANNEL" TEXT')

        conn.commit()


def _time_to_minutes(hhmm: str) -> int | None:
    try:
        s = (hhmm or "").strip()
        if not s:
            return None
        s = s.replace("h", ":").replace("H", ":")
        parts = s.split(":")
        if len(parts) < 2:
            return None
        h = int(parts[0])
        m = int(parts[1])
        return h * 60 + m
    except Exception:
        return None


def find_time_conflicts(
    date_iso: str,
    heure: str,
    ch: str | None = None,
    immat: str | None = None,
    window_min: int = 90,
    exclude_id: int | None = None,
) -> pd.DataFrame:
    """Détecte des conflits horaires simples le même jour.

    - Si `ch` est fourni, cherche les navettes où ce chauffeur apparaît (gère FA*DO, etc.)
    - Si `immat` est fourni, cherche les navettes avec le même véhicule.
    - Conflit = |heure - autre_heure| <= window_min minutes
    """
    target_min = _time_to_minutes(heure)
    if target_min is None:
        return pd.DataFrame()

    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT id, DATE, DATE_ISO, HEURE, CH, IMMAT, NOM, ADRESSE, DESIGNATION, VOL, ADM, REMARQUE,
                   COALESCE(IS_SUPERSEDED, 0) AS IS_SUPERSEDED
            FROM planning
            WHERE DATE_ISO = ?
              AND COALESCE(IS_SUPERSEDED, 0) = 0
            """,
            conn,
            params=[date_iso],
        )

    if df is None or df.empty:
        return pd.DataFrame()

    if exclude_id is not None and "id" in df.columns:
        df = df[df["id"] != int(exclude_id)]

    # Filtre chauffeur
    if ch:
        ch = str(ch).strip()
        if ch:
            def _has_ch(x):
                try:
                    return ch in split_chauffeurs(str(x or ""))
                except Exception:
                    return False
            if "CH" in df.columns:
                df = df[df["CH"].apply(_has_ch)]

    # Filtre véhicule
    if immat:
        immat = str(immat).strip()
        if immat and "IMMAT" in df.columns:
            df = df[df["IMMAT"].fillna("").astype(str).str.strip().eq(immat)]

    # Filtre fenêtre horaire
    mins = df["HEURE"].fillna("").astype(str).map(_time_to_minutes)
    df = df.assign(_m=mins)
    df = df[df["_m"].notna()]
    df = df[(df["_m"] - target_min).abs() <= int(window_min)]

    return df.drop(columns=["_m"], errors="ignore")


def get_urgences(status: str | None = None, days_back: int = 14) -> pd.DataFrame:
    """Retourne les urgences récentes (planning.URGENCE=1)."""
    d0 = date.today() - pd.Timedelta(days=int(days_back))
    d0_iso = d0.strftime("%Y-%m-%d")

    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT *
            FROM planning
            WHERE COALESCE(URGENCE, 0) = 1
              AND COALESCE(IS_SUPERSEDED, 0) = 0
              AND COALESCE(DATE_ISO, '') >= ?
            ORDER BY DATE_ISO ASC, HEURE ASC
            """,
            conn,
            params=[d0_iso],
        )

    if df is None:
        return pd.DataFrame()

    if status:
        df = df[df.get("URGENCE_STATUS", "").fillna("").astype(str).str.upper().eq(status.upper())]

    return df


def set_urgence_status(row_id: int, status: str, notified_at: str | None = None, channel: str | None = None):
    updates: Dict[str, Any] = {"URGENCE_STATUS": status}
    if notified_at is not None:
        updates["URGENCE_NOTIFIED_AT"] = notified_at
    if channel is not None:
        updates["URGENCE_CHANNEL"] = channel
    update_planning_row(int(row_id), updates)

def rebuild_planning_db_from_two_excel_files(file_1, file_2) -> int:
    """
    🔥 Reconstruction complète de la DB (table planning)
    à partir de 2 fichiers Excel (ex: Planning 2025 + Planning 2026).

    - Supprime toute la table planning
    - Réimporte Feuil1 des 2 fichiers
    - Déduplique par row_key (zéro doublon)
    - Recrée les vues SQL
    - Réimporte Feuil2 et Feuil3 (depuis le 2e fichier si possible, sinon le 1er)
    """

    import pandas as pd
    from io import BytesIO
    from datetime import datetime

    def _read_excel_uploaded(uploaded_file, sheet_name: str) -> pd.DataFrame:
        """
        uploaded_file peut être un streamlit UploadedFile ou bytes-like.
        """
        if uploaded_file is None:
            return pd.DataFrame()

        # Streamlit UploadedFile -> .getvalue()
        try:
            content = uploaded_file.getvalue()
        except Exception:
            # fallback: si on reçoit déjà des bytes
            content = uploaded_file

        if not content:
            return pd.DataFrame()

        try:
            return pd.read_excel(BytesIO(content), sheet_name=sheet_name, engine="openpyxl")
        except Exception:
            return pd.DataFrame()

    def _prepare_feuil1_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # DATE -> date python (robuste)
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True, errors="coerce").dt.date
        df = df[df.get("DATE").notna()] if "DATE" in df.columns else df

        # HEURE normalisée si existe
        if "HEURE" in df.columns:
            df["HEURE"] = df["HEURE"].apply(_normalize_heure_str)

        # row_key (obligatoire)
        df["row_key"] = df.apply(lambda r: make_row_key_from_row(r.to_dict()), axis=1)

        # suppression doublons
        df = df.drop_duplicates(subset=["row_key"]).copy()

        return df

    # 🔑 Mise à niveau structure DB (OBLIGATOIRE)
    ensure_planning_row_key_column()
    ensure_planning_row_key_index()

    # ======================================================
    # 1️⃣ Lire Feuil1 des 2 fichiers
    # ======================================================
    df1 = _read_excel_uploaded(file_1, "Feuil1")
    df2 = _read_excel_uploaded(file_2, "Feuil1")

    df1 = _prepare_feuil1_df(df1)
    df2 = _prepare_feuil1_df(df2)

    df_all = pd.concat([df1, df2], ignore_index=True)
    if df_all.empty:
        return 0

    # sécurité finale anti-doublon
    df_all = df_all.drop_duplicates(subset=["row_key"]).copy()

    # ✅ DATE_ISO robuste (pour tri/filtre sans ambiguïté)
    if "DATE" in df_all.columns:
        dt = pd.to_datetime(df_all["DATE"], dayfirst=True, errors="coerce")
        df_all["DATE_ISO"] = dt.dt.strftime("%Y-%m-%d")
        # normalise aussi DATE en dd/mm/YYYY pour l'affichage
        df_all["DATE"] = dt.dt.strftime("%d/%m/%Y").fillna(df_all["DATE"].astype(str))

    # ======================================================
    # 2️⃣ Purge totale planning
    # ======================================================
    with get_connection() as conn:
        conn.execute("DELETE FROM planning")
        conn.commit()

    # ======================================================
    # 3️⃣ Insertion (OR IGNORE) — via insert_planning_row(ignore_conflict=True)
    # ======================================================

    inserts = 0
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 🔑 Colonnes réellement présentes dans la table planning
    planning_cols = get_planning_table_columns()

    for _, row in df_all.iterrows():

        data = {}

        # --------------------------------------------------
        # Copier UNIQUEMENT les colonnes Excel existantes en DB
        # --------------------------------------------------
        for c in df_all.columns:
            if c == "id":
                continue
            if c not in planning_cols:
                continue  # ⛔ colonne Excel absente de la DB → ignorée

            v = row.get(c)
            data[c] = sqlite_safe(v)

        # --------------------------------------------------
        # DATE (format attendu par la table planning)
        # --------------------------------------------------
        if isinstance(row.get("DATE"), (datetime,)):
            data["DATE"] = row["DATE"].strftime("%d/%m/%Y")
        else:
            try:
                data["DATE"] = row["DATE"].strftime("%d/%m/%Y")
            except Exception:
                data["DATE"] = sqlite_safe(row.get("DATE"))

        # --------------------------------------------------
        # Champs techniques obligatoires
        # --------------------------------------------------
        data["updated_at"] = now_ts
        data["row_key"] = row["row_key"]

        if "HEURE" in planning_cols:
            data["HEURE"] = row.get("HEURE", "")

        # --------------------------------------------------
        # Insertion sécurisée (anti-doublon)
        # --------------------------------------------------
        rid = insert_planning_row(
            data,
            ignore_conflict=True,
        )

        if rid != -1:
            inserts += 1

    # ======================================================
    # 4️⃣ Recréer les vues SQL
    # ======================================================
    rebuild_planning_views()

    # ======================================================
    # 5️⃣ Import Feuil2 → chauffeurs (depuis file_2 si possible)
    # ======================================================
    df_ch = _read_excel_uploaded(file_2, "Feuil2")
    if df_ch.empty:
        df_ch = _read_excel_uploaded(file_1, "Feuil2")

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
    # 6️⃣ Import Feuil3 → feuil3 (depuis file_2 si possible)
    # ======================================================
    df_f3 = _read_excel_uploaded(file_2, "Feuil3")
    if df_f3.empty:
        df_f3 = _read_excel_uploaded(file_1, "Feuil3")

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

    return inserts



def delete_planning_row(row_id: int) -> None:
    """
    Supprime une navette par son id.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM planning WHERE id = ?", (row_id,))
        conn.commit()
# =========================
#   INDISPONIBILITÉS CHAUFFEURS
# =========================

def init_indispo_table() -> None:
    """
    Crée la table des demandes d'indisponibilité si elle n'existe pas.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chauffeur_indispo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                CH TEXT NOT NULL,
                DATE TEXT NOT NULL,
                HEURE_DEBUT TEXT,
                HEURE_FIN TEXT,
                COMMENTAIRE TEXT,
                STATUT TEXT DEFAULT 'EN_ATTENTE',  -- EN_ATTENTE / ACCEPTEE / REFUSEE
                planning_id INTEGER,
                created_at TEXT
            )
            """
        )
        conn.commit()


def create_indispo_request(
    ch: str,
    date_val,
    heure_debut,
    heure_fin,
    commentaire: str = "",
) -> int:
    """
    Crée une demande d'indisponibilité pour un chauffeur.
    """
    date_txt = _normalize_date_str(date_val)
    h1 = _normalize_heure_str(heure_debut)
    h2 = _normalize_heure_str(heure_fin)

    created_at = datetime.now().isoformat(timespec="seconds")

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO chauffeur_indispo
                (CH, DATE, HEURE_DEBUT, HEURE_FIN, COMMENTAIRE, STATUT, planning_id, created_at)
            VALUES (?, ?, ?, ?, ?, 'EN_ATTENTE', NULL, ?)
            """,
            (ch, date_txt, h1, h2, commentaire or "", created_at),
        )
        conn.commit()
        return cur.lastrowid


def get_indispo_requests(
    chauffeur: Optional[str] = None,
    statut: Optional[str] = None,
) -> pd.DataFrame:
    """
    Récupère les demandes d'indispos.

    - chauffeur = code CH (ex: 'GG') ou None pour tous
    - statut = 'EN_ATTENTE' / 'ACCEPTEE' / 'REFUSEE' ou None pour tous
    """
    with get_connection() as conn:
        base_sql = "SELECT * FROM chauffeur_indispo"
        where = []
        params: List[Any] = []

        if chauffeur:
            where.append("TRIM(UPPER(CH)) = ?")
            params.append(chauffeur.strip().upper())

        if statut:
            where.append("STATUT = ?")
            params.append(statut)

        if where:
            base_sql += " WHERE " + " AND ".join(where)

        base_sql += " ORDER BY DATE, HEURE_DEBUT"

        try:
            df = pd.read_sql_query(base_sql, conn, params=params)
        except Exception:
            return pd.DataFrame()

    return df


def set_indispo_status(
    request_id: int,
    statut: str,
    planning_id: Optional[int] = None,
) -> None:
    """
    Met à jour le statut d'une demande (EN_ATTENTE / ACCEPTEE / REFUSEE)
    et éventuellement le lien vers la ligne planning créée.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE chauffeur_indispo
            SET STATUT = ?, planning_id = ?
            WHERE id = ?
            """,
            (statut, planning_id, request_id),
        )
        conn.commit()
# ============================================================
#   ACKNOWLEDGEMENT PLANNING CHAUFFEURS
#   (qui a confirmé avoir reçu son planning ?)
# ============================================================
from typing import Any  # normalement déjà importé en haut, sinon garde-le
from datetime import datetime
from typing import Optional


def ensure_planning_updated_at_column():
    """
    Ajoute la colonne updated_at UNIQUEMENT si :
    - la table planning existe
    - la colonne n'existe pas encore
    """
    with get_connection() as conn:
        cur = conn.cursor()

        # 1️⃣ Vérifier si la table planning existe
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='planning'
        """)
        if cur.fetchone() is None:
            # ❗ table absente → NE RIEN FAIRE
            return

        # 2️⃣ Récupérer les colonnes existantes
        cur.execute("PRAGMA table_info(planning)")
        existing_cols = [row[1] for row in cur.fetchall()]

        # 3️⃣ Ajouter la colonne uniquement si absente
        if "updated_at" not in existing_cols:
            cur.execute(
                "ALTER TABLE planning ADD COLUMN updated_at TEXT"
            )
            conn.commit()



def init_chauffeur_ack_table() -> None:
    """
    Table qui stocke la dernière fois qu'un chauffeur a cliqué
    sur 'J'ai bien reçu mon planning'.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS chauffeur_ack (
                chauffeur TEXT PRIMARY KEY,
                last_ack TEXT
            )
            """
        )
        conn.commit()

def init_chauffeur_ack_rows_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chauffeur_ack_rows (
                chauffeur TEXT NOT NULL,
                row_key TEXT NOT NULL,
                confirmed_at TIMESTAMP NOT NULL,
                PRIMARY KEY (chauffeur, row_key)
            )
        """)
        conn.commit()

def confirm_navette_row(chauffeur: str, row_key: str):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO chauffeur_ack_rows
            (chauffeur, row_key, confirmed_at)
            VALUES (?, ?, ?)
            """,
            (chauffeur, row_key, datetime.now()),
        )
        conn.commit()

def get_chauffeur_phone(ch_code: str) -> str:
    """
    Retourne le numéro GSM du chauffeur depuis la table 'chauffeurs' (Feuil2).

    - ch_code : FA, DO, NP, GD, FA1, etc.
    - correspond à la colonne INITIALE
    """
    if not ch_code:
        return ""

    ch = str(ch_code).strip().upper()

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT PHONE
                FROM chauffeurs
                WHERE UPPER(TRIM(INITIALE)) = ?
                LIMIT 1
                """,
                (ch,),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0]).strip()
    except Exception as e:
        print("❌ get_chauffeur_phone error:", e)

    return ""



def is_row_confirmed(chauffeur: str, row_key: str) -> bool:
    """
    Retourne True si le chauffeur a confirmé cette navette (row_key).
    """
    if not chauffeur or not row_key:
        return False

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM chauffeur_ack_rows
            WHERE chauffeur = ?
              AND row_key = ?
            LIMIT 1
            """,
            (chauffeur, row_key),
        )
        return cur.fetchone() is not None



def get_chauffeur_last_ack(chauffeur: str) -> Optional[datetime]:
    """
    Retourne la dernière confirmation de ce chauffeur (datetime) ou None.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_ack FROM chauffeur_ack WHERE chauffeur = ?",
            (chauffeur.upper(),),
        )
        row = cur.fetchone()

    if not row or row[0] is None or row[0] == "":
        return None

    txt = str(row[0]).strip()
    # format ISO ou "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S",):
        try:
            return datetime.strptime(txt, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(txt)
    except Exception:
        return None
def mark_navette_confirmed(nav_id: int, ch: str):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE planning
            SET ACK_BY = ?, ACK_AT = datetime('now')
            WHERE id = ?
            """,
            (ch, nav_id),
        )
        conn.commit()


def ensure_ack_columns():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        cols = [r[1] for r in cur.fetchall()]

        if "ACK_BY" not in cols:
            cur.execute("ALTER TABLE planning ADD COLUMN ACK_BY TEXT")

        if "ACK_AT" not in cols:
            cur.execute("ALTER TABLE planning ADD COLUMN ACK_AT TEXT")

        conn.commit()



def set_chauffeur_last_ack(chauffeur: str, dt: Optional[datetime] = None) -> None:
    """
    Enregistre / met à jour la date de confirmation pour ce chauffeur.
    """
    if dt is None:
        dt = datetime.now()
    value = dt.strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO chauffeur_ack (chauffeur, last_ack)
            VALUES (?, ?)
            ON CONFLICT(chauffeur) DO UPDATE SET last_ack = excluded.last_ack
            """,
            (chauffeur.upper(), value),
        )
        conn.commit()
def init_flight_alerts_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS flight_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_number TEXT NOT NULL,
            flight_date TEXT NOT NULL,
            chauffeur TEXT,
            alerted_at TEXT NOT NULL
        )
    """)
    conn.commit()
from datetime import datetime

def has_flight_alert_been_sent(conn, flight_number, flight_date, chauffeur):
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM flight_alerts
        WHERE flight_number = ?
          AND flight_date = ?
          AND chauffeur = ?
        LIMIT 1
    """, (flight_number, flight_date, chauffeur))
    return cur.fetchone() is not None


def record_flight_alert(conn, flight_number, flight_date, chauffeur):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO flight_alerts (flight_number, flight_date, chauffeur, alerted_at)
        VALUES (?, ?, ?, ?)
    """, (
        flight_number,
        flight_date,
        chauffeur,
        datetime.now().isoformat()
    ))
    conn.commit()
def init_flight_alerts_table():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flight_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_txt TEXT NOT NULL,
                ch TEXT NOT NULL,
                vol TEXT NOT NULL,
                last_status TEXT,
                last_delay_min INTEGER,
                notified_at TEXT,
                UNIQUE(date_txt, ch, vol)
            )
        """)
        conn.commit()

def ensure_flight_alerts_time_columns():
    """
    Ajoute les colonnes de temps dans flight_alerts
    UNIQUEMENT si :
    - la table flight_alerts existe
    - les colonnes n'existent pas encore
    """
    with get_connection() as conn:
        cur = conn.cursor()

        # 1️⃣ Vérifier si la table flight_alerts existe
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='flight_alerts'
        """)
        if cur.fetchone() is None:
            # ❗ table absente → on ne fait RIEN
            return

        # 2️⃣ Colonnes existantes
        cur.execute("PRAGMA table_info(flight_alerts)")
        cols = [row[1] for row in cur.fetchall()]

        # 3️⃣ Ajouts sécurisés
        if "sched_time" not in cols:
            cur.execute(
                'ALTER TABLE flight_alerts ADD COLUMN "sched_time" TEXT'
            )

        if "est_time" not in cols:
            cur.execute(
                'ALTER TABLE flight_alerts ADD COLUMN "est_time" TEXT'
            )

        conn.commit()


def should_notify_flight_change(date_txt, ch_txt, flight_num, sched_time, est_time):
    """
    Vérifie si un changement de vol doit être notifié.
    SAFE même si la DB vient d'être recréée.
    """
    with get_connection() as conn:
        cur = conn.cursor()

        # 1️⃣ Vérifier que la table flight_alerts existe
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='flight_alerts'
        """)
        if cur.fetchone() is None:
            return False

        # 2️⃣ Vérifier les colonnes existantes
        cur.execute("PRAGMA table_info(flight_alerts)")
        cols = [row[1] for row in cur.fetchall()]

        # Colonnes requises
        required = {"last_sched_time", "last_est_time"}
        if not required.issubset(set(cols)):
            # DB recréée mais colonnes absentes → pas de notif
            return False

        # 3️⃣ Lecture sécurisée
        cur.execute(
            """
            SELECT last_sched_time, last_est_time
            FROM flight_alerts
            WHERE date = ? AND ch = ? AND flight_num = ?
            """,
            (date_txt, ch_txt, flight_num),
        )

        row = cur.fetchone()
        if row is None:
            return True  # première fois → notifier

        last_sched, last_est = row

        # 4️⃣ Comparaison
        if last_sched != sched_time or last_est != est_time:
            return True

        return False


def flight_alert_exists(date_txt: str, ch: str, vol: str) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM flight_alerts WHERE date_txt=? AND ch=? AND vol=? LIMIT 1",
            (date_txt, ch, vol),
        )
        return cur.fetchone() is not None


def upsert_flight_alert(
    date_txt: str,
    ch_txt: str,
    flight_num: str,
    sched_time: str,
    est_time: str,
):
    """
    Insère ou met à jour une alerte de vol.
    SAFE si la table ou les colonnes n'existent pas encore.
    """

    with get_connection() as conn:
        cur = conn.cursor()

        # 1️⃣ Vérifier que la table flight_alerts existe
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='flight_alerts'
        """)
        if cur.fetchone() is None:
            # table absente → rien à faire
            return

        # 2️⃣ Vérifier / créer les colonnes nécessaires
        cur.execute("PRAGMA table_info(flight_alerts)")
        cols = [row[1] for row in cur.fetchall()]

        if "date" not in cols:
            cur.execute('ALTER TABLE flight_alerts ADD COLUMN "date" TEXT')
        if "ch" not in cols:
            cur.execute('ALTER TABLE flight_alerts ADD COLUMN "ch" TEXT')
        if "flight_num" not in cols:
            cur.execute('ALTER TABLE flight_alerts ADD COLUMN "flight_num" TEXT')

        if "last_sched_time" not in cols:
            cur.execute(
                'ALTER TABLE flight_alerts ADD COLUMN "last_sched_time" TEXT'
            )
        if "last_est_time" not in cols:
            cur.execute(
                'ALTER TABLE flight_alerts ADD COLUMN "last_est_time" TEXT'
            )

        conn.commit()

        # 3️⃣ Vérifier si une ligne existe déjà
        cur.execute(
            """
            SELECT id
            FROM flight_alerts
            WHERE date = ? AND ch = ? AND flight_num = ?
            """,
            (date_txt, ch_txt, flight_num),
        )
        row = cur.fetchone()

        if row is None:
            # 4️⃣ INSERT
            cur.execute(
                """
                INSERT INTO flight_alerts
                ("date", "ch", "flight_num", "last_sched_time", "last_est_time")
                VALUES (?, ?, ?, ?, ?)
                """,
                (date_txt, ch_txt, flight_num, sched_time, est_time),
            )
        else:
            alert_id = row[0]
            # 5️⃣ UPDATE
            cur.execute(
                """
                UPDATE flight_alerts
                SET last_sched_time = ?, last_est_time = ?
                WHERE id = ?
                """,
                (sched_time, est_time, alert_id),
            )

        conn.commit()


def ensure_km_time_columns():
    """
    Ajoute les colonnes KM_EST, TIME_EST uniquement si :
    - la table planning existe
    - les colonnes n'existent pas encore
    """
    with get_connection() as conn:
        cur = conn.cursor()

        # 1️⃣ Vérifier si la table planning existe
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='planning'
        """)
        if cur.fetchone() is None:
            # ❗ planning absente → on ne fait RIEN
            return

        # 2️⃣ Colonnes existantes
        cur.execute("PRAGMA table_info(planning)")
        cols = [row[1] for row in cur.fetchall()]

        # 3️⃣ Ajout sécurisé
        if "KM_EST" not in cols:
            cur.execute('ALTER TABLE planning ADD COLUMN "KM_EST" TEXT')

        if "TIME_EST" not in cols:
            cur.execute('ALTER TABLE planning ADD COLUMN "TIME_EST" TEXT')

        conn.commit()

def init_sync_meta_table():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()


def get_last_sync_time() -> datetime | None:
    init_sync_meta_table()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM sync_meta WHERE key = 'last_sync'")
        row = cur.fetchone()
        if row and row[0]:
            try:
                return datetime.fromisoformat(row[0])
            except Exception:
                return None
    return None


def set_last_sync_time(dt: datetime):
    init_sync_meta_table()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "REPLACE INTO sync_meta (key, value) VALUES ('last_sync', ?)",
            (dt.isoformat(timespec="seconds"),)
        )
        conn.commit()

def delete_planning_from_date(min_date: str):
    """
    Supprime toutes les lignes planning à partir d'une date (dd/mm/yyyy)
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM planning WHERE DATE >= ?",
            (min_date,)
        )
        conn.commit()


def ensure_indexes():
    """Crée les index utiles pour l'app (perf UI).
    Safe: ignore si certaines colonnes n'existent pas encore.
    """
    with get_connection() as conn:
        cur = conn.cursor()

        # Index historiques
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_date ON planning (DATE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_ch ON planning (CH)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_date_ch ON planning (DATE, CH)")

        # Index modernes (si colonnes présentes)
        cols = {row[1] for row in cur.execute("PRAGMA table_info(planning)").fetchall()}
        if "DATE_ISO" in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_date_iso ON planning (DATE_ISO)")
        if "row_key" in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_row_key ON planning (row_key)")
        if "updated_at" in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_updated_at ON planning (updated_at)")
        if "URGENCE" in cols and "URGENCE_STATUS" in cols:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_urgence ON planning (URGENCE, URGENCE_STATUS)")

        conn.commit()

def init_time_rules_table():
    """Table des règles d'heures (modifiable en admin)."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS time_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ch TEXT NOT NULL,            -- ex: 'NP', 'NP*', '*'
                sens TEXT NOT NULL,          -- 'VERS' ou 'DE'
                dest TEXT NOT NULL,          -- ex: 'BRU', 'AMS', 'AUTRE'
                minutes INTEGER NOT NULL     -- durée en minutes
            )
        """)
        conn.commit()

def init_time_rules_audit_table():
    """Historique des modifications des règles d'heures (audit)."""
    ensure_meta_table()
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS time_rules_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                user TEXT,
                action TEXT NOT NULL,
                details TEXT,
                rules_json TEXT
            )
            """
        )
        conn.commit()


def is_time_rules_locked() -> bool:
    """True si les règles d'heures sont verrouillées (meta.time_rules_locked == '1')."""
    try:
        ensure_meta_table()
        v = get_meta("time_rules_locked")
        return str(v or "").strip() == "1"
    except Exception:
        return False


def set_time_rules_locked(locked: bool, user: str = "", details: str = ""):
    """Verrouille / déverrouille les règles + trace audit."""
    ensure_meta_table()
    init_time_rules_audit_table()
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("time_rules_locked", "1" if locked else "0"),
        )
        conn.execute(
            """
            INSERT INTO time_rules_audit (ts, user, action, details, rules_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                user or "",
                "LOCK" if locked else "UNLOCK",
                details or "",
                None,
            ),
        )
        conn.commit()


def log_time_rules_audit(action: str, user: str = "", details: str = "", rules_df: 'pd.DataFrame | None' = None):
    """Enregistre un audit (snapshot JSON des règles)."""
    init_time_rules_audit_table()
    rules_json = None
    try:
        if rules_df is not None and not rules_df.empty:
            rules_json = rules_df.to_json(orient="records", force_ascii=False)
    except Exception:
        rules_json = None

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO time_rules_audit (ts, user, action, details, rules_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                user or "",
                action or "UPDATE",
                details or "",
                rules_json,
            ),
        )
        conn.commit()


def get_time_rules_audit_df(limit: int = 50) -> pd.DataFrame:
    init_time_rules_audit_table()
    with get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT id, ts, user, action, details
            FROM time_rules_audit
            ORDER BY id DESC
            LIMIT ?
            """,
            conn,
            params=[int(limit)],
        )


def init_time_adjustments_table():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS time_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chauffeur TEXT NOT NULL,
                date_from TEXT,     -- YYYY-MM-DD ou NULL
                date_to   TEXT,     -- YYYY-MM-DD ou NULL
                minutes INTEGER NOT NULL,
                reason TEXT,
                created_at TEXT
            )
            """
        )
        conn.commit()


def get_time_adjustments_df(date_from_iso: str, date_to_iso: str) -> pd.DataFrame:
    init_time_adjustments_table()
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT id, chauffeur, date_from, date_to, minutes, reason, created_at
            FROM time_adjustments
            WHERE
                (date_from IS NULL OR date_from <= ?)
                AND
                (date_to   IS NULL OR date_to   >= ?)
            ORDER BY chauffeur, created_at DESC
            """,
            conn,
            params=[date_to_iso, date_from_iso],
        )
    return df if df is not None else pd.DataFrame()


def insert_time_adjustment(chauffeur: str, date_from_iso: str | None, date_to_iso: str | None, minutes: int, reason: str):
    init_time_adjustments_table()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO time_adjustments (chauffeur, date_from, date_to, minutes, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(chauffeur).strip().upper(),
                date_from_iso,
                date_to_iso,
                int(minutes),
                reason or "",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        conn.commit()


def _hhmm_to_minutes(txt: str) -> int:
    """Accepte '2h30', '2:30', '150', '2h'."""
    if txt is None:
        return 0
    s = str(txt).strip().lower().replace(" ", "")
    if not s:
        return 0
    if s.isdigit():
        return int(s)
    s = s.replace("h", ":")
    if ":" in s:
        parts = s.split(":")
        try:
            hh = int(parts[0]) if parts[0] else 0
            mm = int(parts[1]) if len(parts) > 1 and parts[1] else 0
            return hh * 60 + mm
        except Exception:
            return 0
    return 0


def _minutes_to_hhmm(minutes: int) -> str:
    try:
        m = int(minutes)
    except Exception:
        return ""
    hh = m // 60
    mm = m % 60
    return f"{hh}h{mm:02d}" if hh else f"{mm}min"


def get_time_rules_df() -> pd.DataFrame:
    """
    Retourne les règles AU FORMAT attendu par app.py :
    - ch_base (ex: 'NP', 'ALL')
    - is_star (0/1)
    - sens ('VERS'/'DE')
    - dest_contains (ex: 'BRU')
    - minutes (int)
    """
    init_time_rules_table()
    # 🔒 Verrouillage (sécurité)
    if is_time_rules_locked():
        raise PermissionError("Règles d'heures verrouillées")
    with get_connection() as conn:
        df = pd.read_sql_query(
            "SELECT id, ch, sens, dest, minutes FROM time_rules ORDER BY ch, sens, dest",
            conn,
        )

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    df["ch"] = df["ch"].fillna("").astype(str).str.strip().str.upper()
    df["sens"] = df["sens"].fillna("").astype(str).str.strip().str.upper()
    df["dest"] = df["dest"].fillna("").astype(str).str.strip().str.upper()
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce").fillna(0).astype(int)

    # '*' ou 'ALL' = règle globale (tous les chauffeurs)
    def _to_ch_base(v: str) -> str:
        if v in ("*", "ALL"):
            return "ALL"
        return v.replace("*", "").strip()

    df["ch_base"] = df["ch"].apply(_to_ch_base)

    # is_star = 1 si la règle vise les codes avec *
    # ex: 'NP*' => star=1, base=NP
    # ex: '*' => star=0 (global)
    df["is_star"] = df["ch"].apply(lambda x: 1 if (x not in ("*", "ALL") and "*" in x) else 0).astype(int)

    # format attendu
    df.rename(columns={"dest": "dest_contains"}, inplace=True)

    # colonnes inutiles pour le moteur
    df.drop(columns=["ch"], inplace=True, errors="ignore")

    return df



def save_time_rules_df(edited: pd.DataFrame, user: str = ""):
    """
    Sauvegarde complète des règles depuis l'UI.

    Colonnes attendues depuis l'UI :
    - ch            (ex: *, NP, NP*)
    - sens          (VERS, DE, *)
    - dest          (texte exact : BRU, ZAVENTEM, CDG, *)
    - heures        (ex: 2.5, 2h30, 150)

    Colonnes stockées en DB :
    - ch
    - sens
    - dest_contains
    - minutes
    """
    init_time_rules_table()

    # Autoriser table vide
    if edited is None or edited.empty:
        with get_connection() as conn:
            conn.execute("DELETE FROM time_rules")
            conn.commit()
        return

    df = edited.copy()

    # Colonnes requises (UI)
    for col in ["ch", "sens", "dest", "heures"]:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante: {col}")

    # -----------------------------
    # Normalisation
    # -----------------------------
    df["ch"] = (
        df["ch"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["sens"] = (
        df["sens"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["dest_contains"] = (
        df["dest"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["minutes"] = df["heures"].apply(_hhmm_to_minutes)

    # -----------------------------
    # Filtrage règles valides
    # -----------------------------
    df = df[
        (df["ch"] != "")
        & (df["sens"].isin(["VERS", "DE", "*"]))
        & (df["dest_contains"] != "")
        & (df["minutes"] > 0)
    ].copy()

    # -----------------------------
    # Sauvegarde DB (RESET + INSERT)
    # -----------------------------
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM time_rules")

        for _, r in df.iterrows():
            cur.execute(
                """
                INSERT INTO time_rules (ch, sens, dest, minutes)
                VALUES (?, ?, ?, ?)
                """,
                (
                    r["ch"],
                    r["sens"],
                    r["dest_contains"],
                    int(r["minutes"]),
                ),
            )

        conn.commit()


def _detect_sens_dest_from_row(row: dict) -> tuple[str, str]:
    """
    Détecte sens (VERS/DE) et destination (code).
    Dest = mot-clé dans texte (BRU, GUIL, JCO, AMS, etc.)
    Si rien -> ('', '')
    """
    txt = " ".join([
        str(row.get("DESIGNATION", "") or ""),
        str(row.get("Unnamed: 8", "") or ""),
        str(row.get("ROUTE", "") or ""),
    ]).upper()

    sens = ""
    if "VERS" in txt:
        sens = "VERS"
    elif " DE " in f" {txt} " or txt.startswith("DE "):
        sens = "DE"

    # destination = le premier code connu trouvé, sinon AUTRE si on a un sens
    known = ["BRU", "GUIL", "JCO", "AMS"]
    dest = ""
    for k in known:
        if k in txt:
            dest = k
            break

    if sens and not dest:
        dest = "AUTRE"

    return sens, dest


def get_rule_minutes(ch: str, sens: str, dest: str) -> int:
    """
    Cherche la règle la plus prioritaire :
    1) ch exact + sens + dest
    2) '*' + sens + dest
    3) ch exact + sens + 'AUTRE'
    4) '*' + sens + 'AUTRE'
    Sinon 0
    """
    init_time_rules_table()
    ch = (ch or "").strip()
    sens = (sens or "").strip().upper()
    dest = (dest or "").strip().upper()

    priorities = [
        (ch, sens, dest),
        ("*", sens, dest),
        (ch, sens, "AUTRE"),
        ("*", sens, "AUTRE"),
    ]

    with get_connection() as conn:
        cur = conn.cursor()
        for c, s, d in priorities:
            cur.execute(
                "SELECT minutes FROM time_rules WHERE ch=? AND sens=? AND dest=? LIMIT 1",
                (c, s, d),
            )
            r = cur.fetchone()
            if r and r[0]:
                return int(r[0])
    return 0

def get_actions_connection():
    return sqlite3.connect(ACTIONS_DB_PATH, check_same_thread=False)

def init_actions_table():
    with get_actions_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                row_key TEXT NOT NULL,
                action_type TEXT NOT NULL,          -- 'CH_CHANGE'
                old_value TEXT,
                new_value TEXT,
                user TEXT,
                created_at TEXT NOT NULL,
                needs_excel_update INTEGER NOT NULL DEFAULT 1
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_row_key ON actions(row_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_needs_excel ON actions(needs_excel_update)")
        conn.commit()

def make_row_key_from_row(row: dict) -> str:
    """
    Clé stable basée sur des champs Feuil1.
    Plus ces champs sont stables, plus la clé survivra aux rebuilds.
    """
    parts = [
        str(row.get("DATE", "") or ""),
        str(row.get("HEURE", "") or ""),
        str(row.get("Num BDC", "") or row.get("NUM BDC", "") or ""),
        str(row.get("NOM", "") or ""),
        str(row.get("ADRESSE", "") or ""),
        str(row.get("CP", "") or ""),
        str(row.get("Localité", "") or row.get("LOCALITE", "") or ""),
        str(row.get("Unnamed: 8", "") or ""),   # route
        str(row.get("DESIGNATION", "") or ""),
        str(row.get("VOL", "") or row.get("Vol", "") or ""),
    ]
    raw = "|".join(p.strip().upper() for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def log_ch_change(row_key: str, old_ch: str, new_ch: str, user: str = ""):
    init_actions_table()
    with get_actions_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO actions (row_key, action_type, old_value, new_value, user, created_at, needs_excel_update)
            VALUES (?, 'CH_CHANGE', ?, ?, ?, ?, 1)
        """, (row_key, old_ch or "", new_ch or "", user or "", datetime.now().isoformat(timespec="seconds")))
        conn.commit()

def get_latest_ch_overrides_map(row_keys: list[str]) -> dict:
    """
    Retourne {row_key: new_ch} pour les dernières actions CH_CHANGE.
    """
    if not row_keys:
        return {}

    init_actions_table()
    q_marks = ",".join("?" for _ in row_keys)
    sql = f"""
        SELECT a.row_key, a.new_value
        FROM actions a
        JOIN (
            SELECT row_key, MAX(id) AS max_id
            FROM actions
            WHERE action_type='CH_CHANGE' AND row_key IN ({q_marks})
            GROUP BY row_key
        ) x ON x.row_key = a.row_key AND x.max_id = a.id
    """

    with get_actions_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, row_keys)
        rows = cur.fetchall()

    return {rk: (nv or "").strip() for rk, nv in rows}

def list_pending_actions(limit: int = 500):
    init_actions_table()
    with get_actions_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, row_key, action_type, old_value, new_value, user, created_at
            FROM actions
            WHERE needs_excel_update=1
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()

def mark_actions_done(action_ids: list[int]):
    if not action_ids:
        return
    init_actions_table()
    q = ",".join("?" for _ in action_ids)
    with get_actions_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE actions SET needs_excel_update=0 WHERE id IN ({q})", action_ids)
        conn.commit()

def mark_row_needs_excel_update(row_key: str):
    """
    Marque une ligne comme modifiée dans l'app
    et nécessitant une mise à jour Excel.
    """
    with sqlite3.connect(ACTIONS_DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO planning_actions (row_key, action, created_at)
            VALUES (?, 'EXCEL_UPDATE_NEEDED', datetime('now'))
        """, (row_key,))
        conn.commit()
# ============================================================
#   TABLE META (état de la synchro SharePoint)
# ============================================================

def ensure_meta_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()


def get_meta(key: str):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

def insert_planning_row_from_mail(
    DATE=None,
    HEURE=None,
    DESIGNATION="",
    ADRESSE="",
    NOM="",
    PAX=1,
    VOL="",
    REMARQUES="",
    SOURCE="MAIL",
):
    """Insert flexible depuis l'onglet 'Mail → Navette'.

    ✅ Compatible avec deux styles d'appel :
    - insert_planning_row_from_mail({...dict...})
    - insert_planning_row_from_mail(DATE="...", HEURE="...", ...)

    ✅ Supporte aussi des colonnes supplémentaires si présentes dans la table `planning`
    (ex: CH, IMMAT, PAIEMENT, TTC, H TVA, URGENCE, etc.)

    Retourne l'id inséré (int) ou None si échec.
    """

    data: Dict[str, Any] = {}

    # Support appel dict (app.py)
    if isinstance(DATE, dict) and HEURE is None:
        d = DATE
        data = dict(d)  # copie

        # Compat clés usuelles
        if "DESTINATION" in data and "DESIGNATION" not in data:
            data["DESIGNATION"] = data.get("DESTINATION")
        if "REMARQUES" in data and "REMARQUE" not in data:
            data["REMARQUE"] = data.get("REMARQUES")

        # Normalisation champs minimaux
        data.setdefault("DATE", d.get("DATE"))
        data.setdefault("HEURE", d.get("HEURE"))
        data.setdefault("DESIGNATION", d.get("DESIGNATION", ""))
        data.setdefault("ADRESSE", d.get("ADRESSE", ""))
        data.setdefault("NOM", d.get("NOM", ""))
        data.setdefault("PAX", d.get("PAX", 1) or 1)
        data.setdefault("VOL", d.get("VOL", ""))
        data.setdefault("SOURCE", d.get("SOURCE", SOURCE) or SOURCE)
    else:
        data = {
            "DATE": DATE,
            "HEURE": HEURE,
            "DESIGNATION": DESIGNATION,
            "ADRESSE": ADRESSE,
            "NOM": NOM,
            "PAX": int(PAX or 1),
            "VOL": VOL,
            "SOURCE": SOURCE,
        }
        if REMARQUES:
            data["REMARQUE"] = REMARQUES

    # Nettoyage basique
    if "PAX" in data:
        try:
            data["PAX"] = int(data.get("PAX") or 1)
        except Exception:
            data["PAX"] = 1

    with get_connection() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(planning)").fetchall()]
        cols_set = set(cols)

        # Gérer remarque si la colonne existe
        if "REMARQUE" not in cols_set and "REMARQUES" not in cols_set:
            # fallback : append dans adresse
            rem = data.pop("REMARQUE", data.pop("REMARQUES", "")) if isinstance(data, dict) else ""
            if rem:
                addr = str(data.get("ADRESSE") or "")
                data["ADRESSE"] = (addr + " | " + str(rem)).strip(" |")
        else:
            # Choisir le bon nom de colonne
            if "REMARQUE" in cols_set:
                if "REMARQUES" in data and "REMARQUE" not in data:
                    data["REMARQUE"] = data.pop("REMARQUES")
            else:
                if "REMARQUE" in data and "REMARQUES" not in data:
                    data["REMARQUES"] = data.pop("REMARQUE")

        # Garder uniquement les colonnes existantes (hors id)
        insert_cols = [c for c in data.keys() if c in cols_set and c != "id"]
        if not insert_cols:
            return None

        placeholders = ",".join(["?"] * len(insert_cols))
        sql = f"INSERT INTO planning ({', '.join(insert_cols)}) VALUES ({placeholders})"
        values = [data.get(c) for c in insert_cols]

        cur = conn.execute(sql, values)
        conn.commit()
        return int(cur.lastrowid)

def insert_planning_rows_from_table(
    rows: List[Dict[str, Any]],
    ignore_conflict: bool = True,
) -> Dict[str, int]:
    """Insère plusieurs lignes provenant d'un tableau copié-collé (Excel/TSV).

    - Calcule automatiquement row_key si absent.
    - Backfill DATE_ISO sur la ligne insérée (si la colonne existe).
    - Ne tente d'insérer que les colonnes existantes dans la table planning.
    - Retourne un résumé: {"inserted": n, "skipped": m}.
    """

    if not rows:
        return {"inserted": 0, "skipped": 0}

    # Colonnes nécessaires
    ensure_planning_row_key_column()
    ensure_planning_row_key_index()
    ensure_date_iso_populated()

    with get_connection() as conn:
        cols = {r[1] for r in conn.execute('PRAGMA table_info(planning)').fetchall()}

    inserted = 0
    skipped = 0

    for r in rows:
        if not isinstance(r, dict):
            skipped += 1
            continue

        # Normaliser clés -> exactes (strip)
        row = {str(k).strip(): ("" if v is None else v) for k, v in (r or {}).items()}

        # Lignes vides
        if not (str(row.get("DATE", "") or "").strip() or str(row.get("HEURE", "") or "").strip() or str(row.get("NOM", "") or "").strip()):
            skipped += 1
            continue

        # --- row_key (obligatoire) ---
        try:
            tmp_for_key = dict(row)
            # make_row_key_from_row attend VOL / Num BDC / etc.
            if "VOL" not in tmp_for_key:
                tmp_for_key["VOL"] = row.get("N° Vol") or row.get("Vol") or row.get("N°VOL") or ""
            if "Num BDC" not in tmp_for_key and "NUM BDC" in row:
                tmp_for_key["Num BDC"] = row.get("NUM BDC")

            rk = str(row.get("row_key") or "").strip()
            if not rk:
                rk = make_row_key_from_row(tmp_for_key)
            row["row_key"] = rk
        except Exception:
            skipped += 1
            continue

        # --- DATE normalisée + DATE_ISO ---
        try:
            if "DATE" in row:
                row["DATE"] = _normalize_date_str(row.get("DATE"))
            d = _to_date_any(row.get("DATE"))
            if d is not None and "DATE_ISO" in cols:
                row["DATE_ISO"] = d.strftime("%Y-%m-%d")
        except Exception:
            pass

        # --- Nettoyage colonnes ---
        data = {k: row.get(k) for k in row.keys() if k in cols}

        # row_key obligatoire pour insert_planning_row
        if not data.get("row_key"):
            skipped += 1
            continue

        try:
            rid = insert_planning_row(data, ignore_conflict=ignore_conflict)
            if rid == -1:
                skipped += 1
            else:
                inserted += 1
        except Exception:
            skipped += 1

    return {"inserted": int(inserted), "skipped": int(skipped)}


def set_meta(key: str, value: str):
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()


def unlock_rows_by_row_keys(row_keys: List[str]) -> None:
    """Déverrouille (LOCKED_BY_APP=0) une liste de lignes planning via row_key."""
    if not row_keys:
        return
    keys = [str(k) for k in row_keys if k]
    if not keys:
        return
    placeholders = ",".join("?" for _ in keys)
    with get_connection() as conn:
        conn.execute(
            f"UPDATE planning SET LOCKED_BY_APP=0 WHERE row_key IN ({placeholders})",
            keys,
        )
        conn.commit()

def get_last_caisse_paid_dates(chauffeur: Optional[str] = None) -> Dict[str, str]:
    """
    Dernière DATE_ISO (YYYY-MM-DD) où la caisse est marquée payée (vert) par chauffeur.
    """
    def _norm_ch(x: Any) -> str:
        return (
            str(x or "")
            .upper()
            .strip()
            .replace("*", "")
            .replace(" ", "")
        )

    params: List[Any] = []
    where_ch = ""
    ch_norm = _norm_ch(chauffeur) if chauffeur else ""
    if chauffeur:
        # ⚠️ On évite CH = ? (trop strict) car on a souvent FA / FA* / FA+... en DB
        # On filtre large, puis on agrège côté Python par chauffeur normalisé.
        where_ch = " AND UPPER(REPLACE(REPLACE(CH,'*',''),' ','')) LIKE ?"
        params.append(f"{ch_norm}%")

    sql = f"""
        SELECT CH, MAX(COALESCE(DATE_ISO, '')) as last_date
        FROM planning
        WHERE LOWER(COALESCE(PAIEMENT,'')) = 'caisse'
          AND COALESCE(CAISSE_PAYEE,0) = 1
          AND CAST(COALESCE(Caisse,0) AS REAL) > 0
          AND COALESCE(IS_INDISPO,0) = 0
          AND COALESCE(IS_SUPERSEDED,0) = 0
          {where_ch}
        GROUP BY CH
    """
    out: Dict[str, str] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        for ch, last_date in cur.fetchall():
            if not ch or not last_date:
                continue
            key = _norm_ch(ch)
            # garde la date max par chauffeur normalisé
            prev = out.get(key)
            if not prev or str(last_date) > prev:
                out[key] = str(last_date)
    return out


def update_chauffeur_planning(row_key: str, new_ch: str, user: str = ""):
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE planning
            SET CH = ?,
                CH_MANUAL = 1,
                CH_MANUAL_USER = ?,
                CH_MANUAL_AT = CURRENT_TIMESTAMP
            WHERE row_key = ?
            """,
            (new_ch, user, row_key),
        )

def update_chauffeur_by_row_key(row_key: str, new_ch: str):
    """
    Met à jour le chauffeur d'une ligne planning
    """
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE planning
            SET CH = ?
            WHERE row_key = ?
            """,
            (new_ch, row_key),
        )

def rebuild_planning_db_from_dropbox_full():
    """
    🔥 Reconstruction complète de planning_full depuis Dropbox
    (2025 + 2026) — SANS DOUBLONS
    """
    import pandas as pd
    from io import BytesIO
    from datetime import datetime, date

    content = download_dropbox_excel_bytes()
    if not content:
        return 0

    df = pd.read_excel(
        BytesIO(content),
        sheet_name="Feuil1",
        engine="openpyxl",
    )

    if df.empty:
        return 0

    # 🔧 Normalisation DATE
    df["DATE"] = pd.to_datetime(
        df["DATE"], dayfirst=True, errors="coerce"
    ).dt.date

    df = df[df["DATE"].notna()].copy()

    # 🔑 Génération row_key AVANT insertion
    df["row_key"] = df.apply(
        lambda r: make_row_key_from_row(r.to_dict()),
        axis=1,
    )

    # ❌ Suppression doublons Excel
    df = df.drop_duplicates(subset=["row_key"])

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 🔥 PURGE TOTALE
        cur.execute("DELETE FROM planning_full")
        conn.commit()

        inserted = 0
        for _, row in df.iterrows():
            try:
                insert_planning_row(
                    row.to_dict(),
                    table="planning_full",
                    ignore_conflict=True,  # sécurité
                )
                inserted += 1
            except Exception:
                pass

        conn.commit()

    return inserted
def get_chauffeur_phone(ch_code: str) -> str:
    """
    Retourne le GSM du chauffeur depuis la table chauffeurs (Feuil2)
    """
    if not ch_code:
        return ""

    with get_connection() as conn:
        try:
            df = pd.read_sql_query(
                """
                SELECT *
                FROM chauffeurs
                WHERE UPPER(INITIALE) = ?
                LIMIT 1
                """,
                conn,
                params=(ch_code.upper(),),
            )
        except Exception:
            return ""

    if df.empty:
        return ""

    # 🔁 variantes possibles du nom de colonne GSM
    for col in ["GSM", "TEL", "TELEPHONE", "PHONE", "NUMERO"]:
        if col in df.columns:
            val = str(df.iloc[0][col]).strip()
            if val and val.lower() != "nan":
                return val

    return ""
def get_chauffeurs_phones(ch_raw: str) -> list[str]:
    """
    Retourne la liste des numéros GSM des chauffeurs concernés
    (FA, FA*, FADO, NPFA, etc.)
    """
    from database import split_chauffeurs

    phones = []

    # 🔹 1) découper FA / DO / NP / ...
    chauffeurs = split_chauffeurs(ch_raw)

    if not chauffeurs:
        return phones

    with get_connection() as conn:
        df = pd.read_sql_query("SELECT * FROM chauffeurs", conn)

    if df.empty:
        return phones

    for ch in chauffeurs:
        df_ch = df[
            df["INITIALE"]
            .astype(str)
            .str.upper()
            .str.strip()
            == ch.upper()
        ]

        if df_ch.empty:
            continue

        # variantes possibles colonne GSM
        for col in ["GSM", "TEL", "TELEPHONE", "PHONE", "NUMERO"]:
            if col in df_ch.columns:
                val = str(df_ch.iloc[0][col]).strip()
                if val and val.lower() != "nan":
                    phones.append(val)
                break

    return phones

def ensure_chauffeur_messages_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chauffeur_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                chauffeur TEXT,
                canal TEXT,
                contenu TEXT,
                traite INTEGER DEFAULT 0
            )
        """)
        conn.commit()
def ensure_admin_reply_read_column():
    with get_connection() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(planning)")]
        if "ADMIN_REPLY_READ" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ADMIN_REPLY_READ" INTEGER DEFAULT 0'
            )
        conn.commit()

def ensure_superseded_column():
    with get_connection() as conn:
        cur = conn.execute("PRAGMA table_info(planning)")
        cols = {r[1] for r in cur.fetchall()}
        if "IS_SUPERSEDED" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "IS_SUPERSEDED" INTEGER DEFAULT 0'
            )
            conn.commit()

def ensure_connected_users_table():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS connected_users (
                username TEXT PRIMARY KEY,
                role TEXT,
                chauffeur_code TEXT,
                last_seen TEXT
            )
        """)
        conn.commit()


def ensure_admin_reply_read_column():
    with get_connection() as conn:
        cur = conn.execute("PRAGMA table_info(planning)")
        cols = {row[1] for row in cur.fetchall()}

        if "ADMIN_REPLY_READ" not in cols:
            conn.execute(
                'ALTER TABLE planning ADD COLUMN "ADMIN_REPLY_READ" INTEGER DEFAULT 0'
            )
            conn.commit()
# ======================================================
# ⏱️ TIME RULES — LOCK / AUDIT
# ======================================================

def is_time_rules_locked() -> bool:
    """
    Retourne True si les règles heures sont verrouillées.
    Stocké dans la table meta.
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT value FROM meta WHERE key = 'time_rules_locked'"
            )
            row = cur.fetchone()
            return bool(row and str(row[0]) == "1")
    except Exception:
        return False


def set_time_rules_locked(locked: bool):
    """
    Verrouille / déverrouille les règles heures.
    """
    val = "1" if locked else "0"
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO meta (key, value)
            VALUES ('time_rules_locked', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (val,),
        )
        conn.commit()


def get_time_rules_audit_df():
    """
    Retourne l'historique des modifications des règles heures.
    """
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                """
                SELECT ts, user, action, details
                FROM time_rules_audit
                ORDER BY ts DESC
                LIMIT 200
                """,
                conn,
            )
        return df
    except Exception:
        return pd.DataFrame(columns=["ts", "user", "action", "details"])



# ============================================================
#   🧠 MEMO PRIX / DEMANDEUR / ALIAS (MAIL → NAVETTE)
# ============================================================

def init_price_memory_table():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS price_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                dest_code TEXT,
                sens TEXT,
                nom_key TEXT,
                paiement TEXT,
                prix_ttc REAL,
                prix_htva REAL,
                updated_at TEXT
            )
            """
        )
        conn.commit()

def _price_key(dest_code: str, sens: str, nom_key: str = "", paiement: str = "") -> str:
    import hashlib
    raw = f"{(dest_code or '').upper().strip()}|{(sens or '').upper().strip()}|{(nom_key or '').upper().strip()}|{(paiement or '').upper().strip()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def get_price_suggestion(dest_code: str, sens: str = "", nom: str = "", paiement: str = "") -> dict:
    """
    Retourne {'ttc': float|None, 'htva': float|None} si on a déjà un prix appris.
    Stratégie : (dest+sens+nom+paiement) sinon (dest+sens) sinon (dest).
    """
    init_price_memory_table()
    nom_key = (nom or "").upper().strip()
    keys = [
        _price_key(dest_code, sens, nom_key, paiement),
        _price_key(dest_code, sens, "", paiement),
        _price_key(dest_code, "", "", paiement),
        _price_key(dest_code, sens, "", ""),
        _price_key(dest_code, "", "", ""),
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        q = ",".join("?" for _ in keys)
        cur.execute(
            f"SELECT prix_ttc, prix_htva FROM price_memory WHERE key IN ({q}) ORDER BY updated_at DESC LIMIT 1",
            keys,
        )
        row = cur.fetchone()
    if not row:
        return {"ttc": None, "htva": None}
    return {"ttc": row[0], "htva": row[1]}

def save_price_memory(dest_code: str, sens: str = "", nom: str = "", paiement: str = "", prix_ttc=None, prix_htva=None):
    init_price_memory_table()
    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nom_key = (nom or "").upper().strip()
    k = _price_key(dest_code, sens, nom_key, paiement)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO price_memory (key, dest_code, sens, nom_key, paiement, prix_ttc, prix_htva, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                prix_ttc=excluded.prix_ttc,
                prix_htva=excluded.prix_htva,
                updated_at=excluded.updated_at
            """,
            (k, dest_code, sens, nom_key, paiement, prix_ttc, prix_htva, now_iso),
        )
        conn.commit()

def init_requester_memory_table():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS requester_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demandeur TEXT UNIQUE,
                societe TEXT,
                tva TEXT,
                bdc TEXT,
                imputation TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()

def get_requester_defaults(demandeur: str) -> dict:
    init_requester_memory_table()
    d = (demandeur or "").strip()
    if not d:
        return {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT societe, tva, bdc, imputation FROM requester_memory WHERE demandeur=?",
            (d,),
        )
        row = cur.fetchone()
    if not row:
        return {}
    return {"societe": row[0] or "", "tva": row[1] or "", "bdc": row[2] or "", "imputation": row[3] or ""}

def save_requester_defaults(demandeur: str, societe: str = "", tva: str = "", bdc: str = "", imputation: str = ""):
    init_requester_memory_table()
    d = (demandeur or "").strip()
    if not d:
        return
    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO requester_memory (demandeur, societe, tva, bdc, imputation, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(demandeur) DO UPDATE SET
                societe=excluded.societe,
                tva=excluded.tva,
                bdc=excluded.bdc,
                imputation=excluded.imputation,
                updated_at=excluded.updated_at
            """,
            (d, societe, tva, bdc, imputation, now_iso),
        )
        conn.commit()

def init_location_aliases_table():
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS location_aliases (
                code TEXT PRIMARY KEY,
                label TEXT
            )
            """
        )
        conn.commit()

    defaults = {
        "JCO": "John Cockerill Orangerie",
        "JCC": "John Cockerill Chateau",
        "GUIL": "Guillemins",
        "BRU": "Zaventem",
        "CRL": "Charleroi",
        "LBE": "Leonardo Belgium",
        "FNH": "FN Herstal",
    }
    with get_connection() as conn:
        for code, label in defaults.items():
            conn.execute(
                "INSERT INTO location_aliases(code,label) VALUES(?,?) ON CONFLICT(code) DO NOTHING",
                (code, label),
            )
        conn.commit()

def get_location_aliases_df() -> pd.DataFrame:
    init_location_aliases_table()
    with get_connection() as conn:
        return pd.read_sql("SELECT code, label FROM location_aliases ORDER BY code", conn)

def save_location_aliases_df(df: pd.DataFrame):
    init_location_aliases_table()
    if df is None:
        return
    df2 = df.copy()
    if "code" not in df2.columns:
        return
    if "label" not in df2.columns:
        df2["label"] = ""
    df2["code"] = df2["code"].fillna("").astype(str).str.upper().str.strip()
    df2["label"] = df2["label"].fillna("").astype(str).str.strip()
    df2 = df2[df2["code"] != ""]
    with get_connection() as conn:
        conn.execute("DELETE FROM location_aliases")
        conn.executemany(
            "INSERT INTO location_aliases(code,label) VALUES(?,?)",
            list(df2[["code", "label"]].itertuples(index=False, name=None)),
        )
        conn.commit()

def find_chauffeur_conflicts(ch: str, date_txt: str, heure_txt: str, exclude_id: int = None, window_min: int = 45) -> list:
    """Retourne les navettes du même chauffeur proches dans le temps (même DATE)."""
    try:
        if not ch or not date_txt or not heure_txt:
            return []
        ch = str(ch).strip().upper()

        d = pd.to_datetime(date_txt, dayfirst=True, errors="coerce")
        if pd.isna(d):
            return []

        try:
            hh, mm = str(heure_txt).split(":")[:2]
            base_minutes = int(hh) * 60 + int(mm)
        except Exception:
            return []

        with get_connection() as conn:
            df = pd.read_sql(
                "SELECT id, row_key, DATE, HEURE, CH, NOM, DESIGNATION, [Unnamed: 8] as route FROM planning WHERE DATE=?",
                conn,
                params=(d.strftime("%d/%m/%Y"),),
            )

        if df.empty:
            return []

        df = df[df["CH"].fillna("").astype(str).str.upper().str.contains(ch)]
        if exclude_id is not None:
            df = df[df["id"] != exclude_id]

        out = []
        for _, r in df.iterrows():
            try:
                hh, mm = str(r.get("HEURE", "")).split(":")[:2]
                m2 = int(hh) * 60 + int(mm)
            except Exception:
                continue
            if abs(m2 - base_minutes) <= int(window_min):
                out.append(
                    {
                        "id": int(r["id"]),
                        "row_key": str(r.get("row_key") or ""),
                        "HEURE": str(r.get("HEURE") or ""),
                        "NOM": str(r.get("NOM") or ""),
                        "DEST": (str(r.get("route") or "") + " " + str(r.get("DESIGNATION") or "")).strip(),
                    }
                )
        return out
    except Exception:
        return []

def normalize_airport(x: str) -> str:
    x = str(x or "").upper()
    MAP = {
        "ZAV": "BRU",
        "ZAVENTEM": "BRU",
        "BRUX": "BRU",
        "BRUXELLES": "BRU",
        "BRU": "BRU",
        "CRL": "CRL",
        "CHARLEROI": "CRL",
        "LUX": "LUX",
        "LUXEMBOURG": "LUX",
    }
    for k, v in MAP.items():
        if k in x:
            return v
    return x


def guess_sens_from_text(text: str) -> str:
    t = (text or "").upper()
    if "EX " in t or "ARRIV" in t:
        return "DE"
    if "DEVRA ETRE A" in t or "POUR" in t:
        return "VERS"
    if "ALLER" in t or "RETOUR" in t or "A/R" in t:
        return "A/R"
    return ""


def find_similar_transfer_in_db(row: dict, lookback_days: int = 365) -> dict:
    """
    Cherche un ancien transfert similaire pour pré-remplir automatiquement.
    - Ne touche PAS à l'adresse
    - Sert uniquement à proposer/compléter KM, HTVA, TTC, Type Nav, Paiement, Demandeur, etc.
    """
    from datetime import date, timedelta
    import pandas as pd
    import re

    def _norm(s):
        return str(s or "").strip().upper()

    # ===============================
    # 🔑 Normalisation entrée (MAIL)
    # ===============================
    nom = _norm(row.get("NOM"))
    adresse = _norm(row.get("ADRESSE"))
    cp = _norm(row.get("CP"))
    loc = _norm(row.get("Localité") or row.get("LOCALITE"))

    tel = _norm(row.get("Tél") or row.get("TEL"))
    tel = re.sub(r"[^\d+]", "", tel)

    designation_raw = _norm(row.get("DESIGNATION"))
    dest_key = normalize_airport(designation_raw)

    sens_key = _norm(row.get("Unnamed: 8") or row.get("SENS") or row.get("Sens") or "")
    if sens_key not in ("DE", "VERS", "A/R"):
        sens_key = ""

    d_from = (date.today() - timedelta(days=int(lookback_days))).isoformat()

    # ===============================
    # 📦 Chargement DB
    # ===============================
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT
                DATE_ISO,
                HEURE,
                CH,
                NOM,
                ADRESSE,
                CP,
                "Localité" AS LOCALITE,
                DESIGNATION,
                "Unnamed: 8" AS SENS,
                PAIEMENT,
                "Type Nav" AS TypeNav,
                KM,
                "H TVA" AS HTVA,
                TTC,
                DEMANDEUR,
                IMPUTATION,
                updated_at,
                Tél
            FROM planning
            WHERE
                COALESCE(IS_INDISPO,0) = 0
                AND COALESCE(IS_SUPERSEDED,0) = 0
                AND DATE_ISO >= ?
            ORDER BY DATE_ISO DESC, HEURE DESC
            LIMIT 800
            """,
            conn,
            params=(d_from,),
        )

    if df is None or df.empty:
        return {}

    # ===============================
    # 🔄 Normalisation DB
    # ===============================
    df["NOM_N"] = df["NOM"].fillna("").astype(str).str.upper().str.strip()
    df["ADR_N"] = df["ADRESSE"].fillna("").astype(str).str.upper().str.strip()
    df["CP_N"] = df["CP"].fillna("").astype(str).str.upper().str.strip()
    df["LOC_N"] = df["LOCALITE"].fillna("").astype(str).str.upper().str.strip()

    df["TEL_N"] = (
        df.get("Tél", "")
        .fillna("")
        .astype(str)
        .str.replace(r"[^\d+]", "", regex=True)
        .str.upper()
    )

    df["DEST_N"] = df["DESIGNATION"].fillna("").astype(str).apply(normalize_airport)
    df["SENS_N"] = df["SENS"].fillna("").astype(str).str.upper().str.strip()

    # ===============================
    # 🎯 SCORE MÉTIER (ULTRA IMPORTANT)
    # ===============================
    def _score(r):
        s = 0

        # 📞 Téléphone = clé la plus forte
        if tel and tel == r["TEL_N"]:
            s += 6

        # 👤 Nom
        if nom and r["NOM_N"] == nom:
            s += 3

        # 🏠 Adresse
        if adresse and adresse in r["ADR_N"]:
            s += 3

        if cp and r["CP_N"] == cp:
            s += 2

        if loc and loc in r["LOC_N"]:
            s += 1

        # ✈️ Aéroport normalisé (ZAV = BRU)
        if dest_key and dest_key == r["DEST_N"]:
            s += 3

        # 🔁 Sens DE / VERS / A/R
        if sens_key and sens_key == r["SENS_N"]:
            s += 2

        return s

    df["SCORE"] = df.apply(_score, axis=1)
    df = df.sort_values(
        ["SCORE", "DATE_ISO", "HEURE"],
        ascending=[False, False, False]
    )

    best = df.iloc[0].to_dict()

    # 🔒 Seuil métier : en-dessous → pas assez fiable
    if int(best.get("SCORE") or 0) < 6:
        return {}

    # ===============================
    # 🔁 RETOUR CIBLÉ (PRÉ-REMPLISSAGE)
    # ===============================
    return {
        "PAIEMENT": best.get("PAIEMENT") or "",
        "Type Nav": best.get("TypeNav") or "",
        "KM": best.get("KM") or "",
        "H TVA": best.get("HTVA") or "",
        "TTC": best.get("TTC") or "",
        "DEMANDEUR": best.get("DEMANDEUR") or "",
        "IMPUTATION": best.get("IMPUTATION") or "",
        "_SIMILAR_INFO": (
            f"{best.get('DATE_ISO','')} "
            f"{best.get('HEURE','')} "
            f"(score={int(best.get('SCORE') or 0)})"
        ),
        "_SIMILAR_CH": best.get("CH") or "",
    }

def should_create_return(rows: list, raw_mail: str, similar_info: dict) -> bool:
    txt = (raw_mail or "").upper()

    if any(k in txt for k in ["RETOUR", "ALLER-RETOUR", "ALLER RETOUR", "A/R", "AR "]):
        return True

    if len(rows) >= 2:
        return True

    if similar_info.get("_HAS_RETURN_HISTORY"):
        return True

    return False
def invert_sens(s):
    s = (s or "").upper()
    if s == "DE":
        return "VERS"
    if s == "VERS":
        return "DE"
    return s
def build_return_row(base_row: dict) -> dict:
    r = base_row.copy()
    r["HEURE"] = ""
    r["DATE"] = ""
    r["Unnamed: 8"] = invert_sens(base_row.get("Unnamed: 8"))
    r["REMARQUE"] = (base_row.get("REMARQUE","") + " | RETOUR auto").strip()
    r["_AUTO_RETURN"] = True
    return r
def parse_sens_from_mail(text: str) -> str:
    t = (text or "").upper()

    if "EX " in t or "ARRIV" in t:
        return "DE"

    if "DEVRA ETRE A" in t or "POUR" in t or "DEPART" in t:
        return "VERS"

    if "RETOUR" in t or "ALLER-RETOUR" in t or "A/R" in t:
        return "A/R"

    return ""

