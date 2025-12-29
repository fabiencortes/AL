import sqlite3
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import streamlit as st
import pandas as pd
import hashlib

def sqlite_safe(val):
    if val is None:
        return None

    # datetime.time
    if hasattr(val, "hour") and hasattr(val, "minute"):
        return f"{val.hour:02d}:{val.minute:02d}"

    # datetime / date / pandas
    try:
        import pandas as pd
        from datetime import datetime, date
        if isinstance(val, (pd.Timestamp, datetime, date)):
            return val.strftime("%d/%m/%Y")
    except Exception:
        pass

    return str(val)


# =========================
#   CONFIG BASE DE DONNÉES
# =========================
DB_PATH = "airportslines.db"
ACTIONS_DB_PATH = "planning_actions.db"


# =========================
#   OUTILS INTERNES
# =========================

def get_connection() -> sqlite3.Connection:
    """
    Retourne une connexion SQLite.
    Utilise row_factory par défaut (dictionnaires faits à la main quand nécessaire).
    """
    conn = sqlite3.connect(DB_PATH)
    return conn


def _normalize_date_str(d: Any) -> str:
    """
    Utilitaire : convertit une date (datetime.date ou str) au format dd/mm/YYYY.
    """
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return ""
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y")
    s = str(d).strip()
    if not s:
        return ""
    try:
        v = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(v):
            return s
        return v.strftime("%d/%m/%Y")
    except Exception:
        return s


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


# =========================
#   CHARGEMENT GLOBAL PLANNING
# =========================

def _load_planning_df() -> pd.DataFrame:
    """
    Charge la table 'planning' complète en DataFrame.

    - DATE : convertie en datetime.date si possible
    - HEURE : laissée en texte
    - Garantit que les colonnes GROUPAGE / PARTAGE existent (remplies à '0' si absentes)
    """
    with get_connection() as conn:
        try:
            df = pd.read_sql_query("SELECT * FROM planning", conn)
        except Exception:
            return pd.DataFrame()

    if df.empty:
        return df

    # Normalisation DATE -> datetime.date (tout en gardant la valeur texte initiale dans la base)
    if "DATE" in df.columns:
        try:
            df["DATE"] = pd.to_datetime(
                df["DATE"], dayfirst=True, errors="coerce"
            ).dt.date
        except Exception:
            # on laisse tel quel si ça échoue
            pass

    # S'assurer que GROUPAGE / PARTAGE existent toujours (sinon colonnes 0)
    if "GROUPAGE" not in df.columns:
        df["GROUPAGE"] = "0"
    if "PARTAGE" not in df.columns:
        df["PARTAGE"] = "0"

    return df


# =========================
#   LECTURE PLANNING
# =========================

@st.cache_data(ttl=300)
# =========================
#   LECTURE PLANNING
# =========================

@st.cache_data(ttl=300)
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

    source :
        - "day"  -> planning_day   (vue jour mobile)
        - "7j"   -> planning_7j    (planning / édition / chauffeur)
        - "full" -> planning_full  (admin / clients / historique)
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
        with get_connection() as conn:
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
    except Exception as e:
        print(f"❌ Erreur lecture {table} :", e)
        return pd.DataFrame()

    if df.empty:
        return df

    # =========================
    # Conversion DATE propre
    # =========================
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(
            df["DATE"],
            dayfirst=True,
            errors="coerce"
        ).dt.date

    # =========================
    # Filtre date (si demandé)
    # =========================
    if start_date or end_date:
        if "DATE" in df.columns:
            def _keep_date(d):
                if pd.isna(d):
                    return False
                if not isinstance(d, date):
                    return False
                if start_date and d < start_date:
                    return False
                if end_date and d > end_date:
                    return False
                return True

            df = df[df["DATE"].apply(_keep_date)].copy()

    # =========================
    # Filtre chauffeur
    # =========================
    if chauffeur and "CH" in df.columns:
        ch = chauffeur.strip().upper()
        ch_series = df["CH"].astype(str).str.strip().str.upper()

        if ch.endswith("*"):
            df = df[ch_series == ch].copy()
        else:
            mask_exact = ch_series == ch
            starts_with = ch_series.str.startswith(ch)
            next_char = ch_series.str.slice(len(ch), len(ch) + 1)
            mask_non_digit_after = (next_char == "") | (~next_char.str.match(r"\d"))
            mask_prefix = starts_with & mask_non_digit_after

            df = df[mask_exact | mask_prefix].copy()

    # =========================
    # Filtre type AL / GO_GL
    # =========================
    if "GO" in df.columns and type_filter:
        go_series = df["GO"].astype(str).str.strip().str.upper()

        if type_filter == "AL":
            df = df[~go_series.isin(["GO", "GL"])].copy()
        elif type_filter == "GO_GL":
            df = df[go_series.isin(["GO", "GL"])].copy()

    # =========================
    # Recherche texte libre
    # =========================
    if search:
        s = search.lower()

        def _row_match(row):
            for col in ["NOM", "ADRESSE", "REMARQUE", "VOL", "NUM_BDC"]:
                if col in row and s in str(row[col]).lower():
                    return True
            return False

        df = df[df.apply(_row_match, axis=1)].copy()

    return df


    # Tri par DATE + HEURE
    if "HEURE" in df.columns:
        def _heure_sort_tuple(h):
            h2 = _normalize_heure_str(h)
            if not h2 or ":" not in h2:
                return (99, 99)
            try:
                hh, mm = h2.split(":")
                return (int(hh), int(mm))
            except Exception:
                return (99, 99)

        df["_HSORT"] = df["HEURE"].apply(_heure_sort_tuple)
    else:
        df["_HSORT"] = (99, 99)

    sort_cols: List[str] = []
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


def get_chauffeur_planning(
    chauffeur: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> pd.DataFrame:
    """Retourne le planning pour un chauffeur donné, en réutilisant
    exactement la même logique de filtre que get_planning (codes AD*, FADO, etc.)."""
    ch = (chauffeur or "").strip()
    if not ch:
        return pd.DataFrame()

    return get_planning(
        start_date=from_date,
        end_date=to_date,
        chauffeur=ch,
        type_filter=None,
        search="",
        max_rows=5000,
    )

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


def insert_planning_row(data: Dict[str, Any]) -> int:
    """
    Insère une nouvelle navette dans la table planning.
    Retourne l'id créé.
    """
    if not data:
        return -1

    # s'assurer que la colonne existe
    ensure_planning_updated_at_column()

    # Normaliser DATE en texte dd/mm/YYYY
    if "DATE" in data:
        data["DATE"] = _normalize_date_str(data["DATE"])

    # Timestamp de mise à jour
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cols = list(data.keys())
    col_list_sql = ",".join(f'"{c}"' for c in cols)
    placeholders = ",".join("?" for _ in cols)
    values = [sqlite_safe(data[c]) for c in cols]

    with get_connection() as conn:
        cur = conn.cursor()
        sql = f"INSERT INTO planning ({col_list_sql}) VALUES ({placeholders})"
        cur.execute(sql, values)
        conn.commit()
        return cur.lastrowid



def update_planning_row(row_id: int, data: Dict[str, Any]) -> None:
    """
    Met à jour une navette existante (par id) avec les colonnes fournies.
    """
    if not data:
        return

    # s'assurer que la colonne existe
    ensure_planning_updated_at_column()

    if "DATE" in data:
        data["DATE"] = _normalize_date_str(data["DATE"])

    # Timestamp de mise à jour
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    set_parts = []
    values: List[Any] = []
    for col, val in data.items():
        set_parts.append(f'"{col}" = ?')
        values.append(sqlite_safe(val))

    values.append(row_id)


    values.append(row_id)
    set_clause = ", ".join(set_parts)

    with get_connection() as conn:
        cur = conn.cursor()
        sql = f"UPDATE planning SET {set_clause} WHERE id = ?"
        cur.execute(sql, values)
        conn.commit()



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
    """
    Crée les index SQL essentiels pour les performances
    """
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_planning_date ON planning (DATE)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_planning_ch ON planning (CH)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_planning_date_ch ON planning (DATE, CH)"
        )

        conn.commit()

def ensure_indexes():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_date ON planning (DATE)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_ch ON planning (CH)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_planning_date_ch ON planning (DATE, CH)")
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
    """Retourne les règles en DataFrame (heures affichées en 2h30)."""
    init_time_rules_table()
    with get_connection() as conn:
        df = pd.read_sql_query('SELECT * FROM time_rules ORDER BY ch, sens, dest', conn)

    if df.empty:
        return df

    df = df.copy()
    df["heures"] = df["minutes"].apply(_minutes_to_hhmm)
    df.drop(columns=["minutes"], inplace=True, errors="ignore")
    return df


def save_time_rules_df(edited: pd.DataFrame):
    """Sauvegarde complète des règles depuis un DataFrame édité."""
    init_time_rules_table()
    if edited is None or edited.empty:
        # autoriser table vide
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM time_rules")
            conn.commit()
        return

    df = edited.copy()
    # colonnes attendues
    for col in ["ch", "sens", "dest", "heures"]:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante: {col}")

    # normalisation
    df["ch"] = df["ch"].astype(str).str.strip()
    df["sens"] = df["sens"].astype(str).str.strip().str.upper()
    df["dest"] = df["dest"].astype(str).str.strip().str.upper()
    df["heures"] = df["heures"].astype(str).str.strip()

    # calcul minutes
    df["minutes"] = df["heures"].apply(_hhmm_to_minutes)

    # filtrer lignes invalides
    df = df[(df["ch"] != "") & (df["sens"].isin(["VERS", "DE"])) & (df["dest"] != "") & (df["minutes"] > 0)].copy()

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM time_rules")

        for _, r in df.iterrows():
            cur.execute(
                "INSERT INTO time_rules (ch, sens, dest, minutes) VALUES (?, ?, ?, ?)",
                (r["ch"], r["sens"], r["dest"], int(r["minutes"])),
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

