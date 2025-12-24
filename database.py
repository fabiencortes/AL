import sqlite3
from datetime import date, datetime
from typing import Optional, Dict, Any, List

import pandas as pd

# =========================
#   CONFIG BASE DE DONNÉES
# =========================
DB_PATH = "airportslines.db"


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

def get_planning(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    chauffeur: Optional[str] = None,
    type_filter: Optional[str] = None,  # None, "AL", "GO_GL"
    search: str = "",
    max_rows: int = 2000,
) -> pd.DataFrame:
    """
    Retourne un DataFrame filtré de la table planning.

    - start_date / end_date : dates Python (inclusives)
    - chauffeur : code CH (FA, GG, NP, ...)
    - type_filter :
        None      -> tous
        "AL"      -> uniquement AL (ou vide)
        "GO_GL"   -> uniquement GO/GL
    - search : texte à chercher dans NOM / ADRESSE / REMARQUE / N° Vol / Num BDC
    """
    df = _load_planning_df()
    if df.empty:
        return df

    # Filtre date
    if start_date or end_date:
        if "DATE" in df.columns:
            def _keep_date(d):
                # 1) ignorer les valeurs vides / NaT
                if pd.isna(d):
                    return False

                # 2) si c'est un Timestamp pandas, on prend juste la date
                if isinstance(d, pd.Timestamp):
                    d2 = d.date()
                else:
                    d2 = d

                # 3) on ne garde que les vraies dates Python
                if not isinstance(d2, date):
                    return False

                if start_date and d2 < start_date:
                    return False
                if end_date and d2 > end_date:
                    return False
                return True

            mask = df["DATE"].apply(_keep_date)
            df = df[mask].copy()

    # Filtre chauffeur
    # Filtre chauffeur
    if chauffeur and "CH" in df.columns:
        ch = chauffeur.strip().upper()
        ch_series = df["CH"].astype(str).str.strip().str.upper()

        # Si on choisit un code avec étoile (AD*, FA*, ...), on filtre strictement sur ce code
        if ch.endswith("*"):
            df = ch_series[ch_series == ch].to_frame().join(df, how="right").dropna(subset=["CH"])
            df = df[df["CH"].astype(str).str.strip().str.upper() == ch].copy()
        else:
            # 1) égalité exacte
            mask_exact = ch_series == ch

            # 2) + toutes les lignes dont le CH commence par ce code,
            #    mais SANS inclure celles où le caractère suivant est un chiffre.
            #    Exemples :
            #      - chauffeur = "AD"  -> AD, AD*, ADNP, ADGO...
            #      - chauffeur = "FA"  -> FA, FA*, FADO..., mais PAS FA1, FA1*
            #      - chauffeur = "FA1" -> FA1, FA1*, FA1NP...
            starts_with = ch_series.str.startswith(ch)
            next_char = ch_series.str.slice(len(ch), len(ch) + 1)
            # next_char == ""  (code exactement égal) ou non chiffré
            mask_non_digit_after = (next_char == "") | (~next_char.str.match(r"\d"))

            mask_prefix = starts_with & mask_non_digit_after

            df = df[mask_exact | mask_prefix].copy()


    # Filtre type AL / GO_GL selon colonne GO
    if "GO" in df.columns and type_filter:
        go_series = df["GO"].astype(str).str.strip().str.upper()
        if type_filter == "AL":
            # tout ce qui n'est pas GO/GL (donc vide ou AL ou autre)
            df = df[~go_series.isin(["GO", "GL"])].copy()
        elif type_filter == "GO_GL":
            df = df[go_series.isin(["GO", "GL"])].copy()

    # Filtre texte
    if search:
        s_low = search.strip().lower()
        if s_low:
            mask = False
            candidates = ["NOM", "ADRESSE", "REMARQUE", "N° Vol", "Num BDC", "DESIGNATION"]
            for col in candidates:
                if col in df.columns:
                    mask = mask | df[col].astype(str).str.lower().str.contains(s_low, na=False)
            df = df[mask].copy()

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

def search_client(client_name_part: str, max_rows: int = 500) -> pd.DataFrame:
    """
    Recherche un client par nom (partiel) dans la table planning.
    """
    df = _load_planning_df()
    if df.empty:
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
    values = [data[c] for c in cols]

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
        values.append(val)

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


def ensure_planning_updated_at_column() -> None:
    """
    Ajoute la colonne updated_at à la table planning si elle n'existe pas encore.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        cols = [row[1].upper() for row in cur.fetchall()]
        if "UPDATED_AT" not in cols:
            cur.execute("ALTER TABLE planning ADD COLUMN updated_at TEXT")
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


def flight_alert_exists(date_txt: str, ch: str, vol: str) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM flight_alerts WHERE date_txt=? AND ch=? AND vol=? LIMIT 1",
            (date_txt, ch, vol),
        )
        return cur.fetchone() is not None


def upsert_flight_alert(date_txt: str, ch: str, vol: str, status: str, delay_min: int) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO flight_alerts(date_txt, ch, vol, last_status, last_delay_min, notified_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date_txt, ch, vol)
            DO UPDATE SET
                last_status=excluded.last_status,
                last_delay_min=excluded.last_delay_min,
                notified_at=excluded.notified_at
        """, (date_txt, ch, vol, status, int(delay_min or 0), now))
        conn.commit()
def ensure_km_time_columns():
    """
    Ajoute les colonnes KM_EST et TEMPS_EST à la table planning
    si elles n'existent pas encore.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(planning)")
        cols = [r[1] for r in cur.fetchall()]

        if "KM_EST" not in cols:
            cur.execute('ALTER TABLE planning ADD COLUMN "KM_EST" TEXT')

        if "TEMPS_EST" not in cols:
            cur.execute('ALTER TABLE planning ADD COLUMN "TEMPS_EST" TEXT')

        conn.commit()