from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

DB_NAME = "airportslines.db"
DEFAULT_BASE_DIESEL = 1.54
DEFAULT_CURRENT_DIESEL = 2.30
DEFAULT_CONSO_100 = 8.0
DEFAULT_RECOVERY = 100.0
DEFAULT_CLIENTS = [
    {"Client": "Tarif 0.55", "Prix_km": 0.55, "Recuperation_pct": 100.0, "Actif": True, "Notes": ""},
    {"Client": "Tarif 0.60", "Prix_km": 0.60, "Recuperation_pct": 100.0, "Actif": True, "Notes": ""},
    {"Client": "Tarif 0.70", "Prix_km": 0.70, "Recuperation_pct": 100.0, "Actif": True, "Notes": ""},
    {"Client": "Tarif 0.78", "Prix_km": 0.78, "Recuperation_pct": 100.0, "Actif": True, "Notes": ""},
]


def _get_db_path() -> Path:
    return Path.cwd() / DB_NAME


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_tables() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fuel_surcharge_clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT NOT NULL UNIQUE,
                price_per_km REAL NOT NULL DEFAULT 0,
                recovery_pct REAL NOT NULL DEFAULT 100,
                active INTEGER NOT NULL DEFAULT 1,
                notes TEXT DEFAULT '',
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fuel_surcharge_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT
            )
            """
        )
        conn.commit()

    if not _load_clients_from_db().shape[0]:
        _save_clients_to_db(pd.DataFrame(DEFAULT_CLIENTS))

    defaults = {
        "base_diesel": str(DEFAULT_BASE_DIESEL),
        "current_diesel": str(DEFAULT_CURRENT_DIESEL),
        "conso_100": str(DEFAULT_CONSO_100),
        "recovery_default": str(DEFAULT_RECOVERY),
    }
    current = _load_settings_from_db()
    for k, v in defaults.items():
        if k not in current:
            current[k] = v
    _save_settings_to_db(current)


def _load_settings_from_db() -> dict[str, str]:
    with _connect() as conn:
        rows = conn.execute("SELECT setting_key, setting_value FROM fuel_surcharge_settings").fetchall()
    return {str(r["setting_key"]): str(r["setting_value"] or "") for r in rows}


def _save_settings_to_db(settings: dict[str, str]) -> None:
    with _connect() as conn:
        for k, v in settings.items():
            conn.execute(
                """
                INSERT INTO fuel_surcharge_settings(setting_key, setting_value)
                VALUES(?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value
                """,
                (str(k), str(v)),
            )
        conn.commit()


def _load_clients_from_db() -> pd.DataFrame:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                client_name AS Client,
                price_per_km AS Prix_km,
                recovery_pct AS Recuperation_pct,
                CASE WHEN active = 1 THEN 1 ELSE 0 END AS Actif,
                COALESCE(notes, '') AS Notes
            FROM fuel_surcharge_clients
            ORDER BY client_name COLLATE NOCASE
            """
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["Client", "Prix_km", "Recuperation_pct", "Actif", "Notes"])
    df = pd.DataFrame([dict(r) for r in rows])
    df["Actif"] = df["Actif"].astype(bool)
    return df


def _save_clients_to_db(df: pd.DataFrame) -> None:
    clean = df.copy()
    if clean.empty:
        clean = pd.DataFrame(columns=["Client", "Prix_km", "Recuperation_pct", "Actif", "Notes"])

    clean = clean.fillna("")
    clean["Client"] = clean["Client"].astype(str).str.strip()
    clean = clean[clean["Client"] != ""]

    if not clean.empty:
        clean["Prix_km"] = pd.to_numeric(clean["Prix_km"], errors="coerce").fillna(0.0)
        clean["Recuperation_pct"] = pd.to_numeric(clean["Recuperation_pct"], errors="coerce").fillna(DEFAULT_RECOVERY).clip(lower=0, upper=200)
        clean["Actif"] = clean["Actif"].astype(bool)
        clean["Notes"] = clean["Notes"].astype(str)
        clean = clean.drop_duplicates(subset=["Client"], keep="first")

    now = datetime.now().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute("DELETE FROM fuel_surcharge_clients")
        for _, row in clean.iterrows():
            conn.execute(
                """
                INSERT INTO fuel_surcharge_clients(client_name, price_per_km, recovery_pct, active, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["Client"],
                    float(row["Prix_km"]),
                    float(row["Recuperation_pct"]),
                    1 if bool(row["Actif"]) else 0,
                    row["Notes"],
                    now,
                ),
            )
        conn.commit()


def _ensure_state() -> None:
    if "fuel_settings" not in st.session_state:
        st.session_state["fuel_settings"] = _load_settings_from_db()
    if "fuel_clients_df" not in st.session_state:
        st.session_state["fuel_clients_df"] = _load_clients_from_db()


def _reload_state() -> None:
    st.session_state["fuel_settings"] = _load_settings_from_db()
    st.session_state["fuel_clients_df"] = _load_clients_from_db()


def _compute_metrics(km: float, price_per_km: float, diesel_base: float, diesel_current: float, conso_100: float, recovery_pct: float) -> dict[str, float]:
    km = max(float(km or 0.0), 0.0)
    price_per_km = max(float(price_per_km or 0.0), 0.0)
    diesel_base = max(float(diesel_base or 0.0), 0.0)
    diesel_current = max(float(diesel_current or 0.0), 0.0)
    conso_100 = max(float(conso_100 or 0.0), 0.0)
    recovery_pct = max(float(recovery_pct or 0.0), 0.0)

    conso_l_km = conso_100 / 100.0
    cost_km_base = diesel_base * conso_l_km
    cost_km_current = diesel_current * conso_l_km
    raw_surcharge_km = max(cost_km_current - cost_km_base, 0.0)
    applied_surcharge_km = raw_surcharge_km * (recovery_pct / 100.0)
    fuel_total_current = km * cost_km_current
    surcharge_total = km * applied_surcharge_km
    base_trip_total = km * price_per_km
    price_share_pct = (cost_km_current / price_per_km * 100.0) if price_per_km > 0 else 0.0
    surcharge_pct = (applied_surcharge_km / price_per_km * 100.0) if price_per_km > 0 else 0.0
    return {
        "cost_km_base": cost_km_base,
        "cost_km_current": cost_km_current,
        "raw_surcharge_km": raw_surcharge_km,
        "applied_surcharge_km": applied_surcharge_km,
        "fuel_total_current": fuel_total_current,
        "surcharge_total": surcharge_total,
        "price_share_pct": price_share_pct,
        "surcharge_pct": surcharge_pct,
        "recommended_price_km": price_per_km + applied_surcharge_km,
        "trip_total_without_surcharge": base_trip_total,
        "trip_total_with_surcharge": base_trip_total + surcharge_total,
    }


def _fmt_eur(v: float) -> str:
    return f"{float(v):,.4f} €".replace(",", "X").replace(".", ",").replace("X", " ")


def _fmt_pct(v: float) -> str:
    return f"{float(v):,.2f} %".replace(",", "X").replace(".", ",").replace("X", " ")


def render_fuel_tab() -> None:
    _init_tables()
    _ensure_state()

    settings = st.session_state["fuel_settings"]
    df_clients = st.session_state["fuel_clients_df"].copy()

    st.subheader("⛽ Surcharge carburant")
    st.caption("Version allégée : moins de requêtes, moins de reruns, calcul plus rapide.")

    with st.expander("ℹ️ Base de calcul", expanded=False):
        st.markdown(
            """
- **Coût carburant au km** = prix diesel × (consommation / 100)
- **Surcoût carburant au km** = (diesel actuel - diesel de base) × (consommation / 100)
- **Surcharge appliquée** = surcoût × récupération
- Le **%** dépend du tarif client.
            """
        )

    with st.form("fuel_settings_form", border=True):
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            diesel_base = st.number_input("Diesel de base (€ / L)", min_value=0.0, value=float(settings.get("base_diesel", DEFAULT_BASE_DIESEL)), step=0.01)
        with p2:
            diesel_current = st.number_input("Diesel actuel (€ / L)", min_value=0.0, value=float(settings.get("current_diesel", DEFAULT_CURRENT_DIESEL)), step=0.01)
        with p3:
            conso_100 = st.number_input("Consommation (L / 100 km)", min_value=0.0, value=float(settings.get("conso_100", DEFAULT_CONSO_100)), step=0.1)
        with p4:
            recovery_default = st.number_input("Récupération défaut (%)", min_value=0.0, max_value=200.0, value=float(settings.get("recovery_default", DEFAULT_RECOVERY)), step=5.0)
        save_settings = st.form_submit_button("💾 Enregistrer les paramètres", use_container_width=True)

    if save_settings:
        new_settings = {
            "base_diesel": str(diesel_base),
            "current_diesel": str(diesel_current),
            "conso_100": str(conso_100),
            "recovery_default": str(recovery_default),
        }
        _save_settings_to_db(new_settings)
        st.session_state["fuel_settings"] = new_settings
        settings = new_settings
        st.success("Paramètres enregistrés.")

    diesel_base = float(settings.get("base_diesel", DEFAULT_BASE_DIESEL))
    diesel_current = float(settings.get("current_diesel", DEFAULT_CURRENT_DIESEL))
    conso_100 = float(settings.get("conso_100", DEFAULT_CONSO_100))
    recovery_default = float(settings.get("recovery_default", DEFAULT_RECOVERY))

    calc_base = _compute_metrics(1, max(diesel_current, 0.01), diesel_base, diesel_current, conso_100, 100.0)
    m1, m2, m3 = st.columns(3)
    m1.metric("Carburant actuel / km", _fmt_eur(calc_base["cost_km_current"]))
    m2.metric("Surcoût réel / km", _fmt_eur(calc_base["raw_surcharge_km"]))
    m3.metric("Carburant base / km", _fmt_eur(calc_base["cost_km_base"]))

    tab_normal, tab_pro = st.tabs(["Mode normal", "Mode pro"])

    with tab_normal:
        with st.form("fuel_normal_form", border=True):
            c1, c2, c3 = st.columns(3)
            with c1:
                km = st.number_input("Km du trajet", min_value=0.0, value=180.0, step=1.0)
            with c2:
                price_per_km = st.number_input("Tarif client (€ / km)", min_value=0.0, value=0.70, step=0.01)
            with c3:
                recovery_pct = st.number_input("Récupération appliquée (%)", min_value=0.0, max_value=200.0, value=recovery_default, step=5.0)
            run_normal = st.form_submit_button("Calculer", use_container_width=True)

        if run_normal or "fuel_normal_last" not in st.session_state:
            st.session_state["fuel_normal_last"] = {
                "km": km,
                "price_per_km": price_per_km,
                "recovery_pct": recovery_pct,
            }
        vals = st.session_state["fuel_normal_last"]
        res = _compute_metrics(vals["km"], vals["price_per_km"], diesel_base, diesel_current, conso_100, vals["recovery_pct"])

        a, b, c, d = st.columns(4)
        a.metric("Carburant réel / km", _fmt_eur(res["cost_km_current"]))
        b.metric("Carburant trajet", _fmt_eur(res["fuel_total_current"]))
        c.metric("Surcharge / km", _fmt_eur(res["applied_surcharge_km"]))
        d.metric("Surcharge trajet", _fmt_eur(res["surcharge_total"]))

        e, f, g, h = st.columns(4)
        e.metric("Part carburant du tarif", _fmt_pct(res["price_share_pct"]))
        f.metric("% surcharge sur tarif", _fmt_pct(res["surcharge_pct"]))
        g.metric("Prix conseillé / km", _fmt_eur(res["recommended_price_km"]))
        h.metric("Total trajet avec surcharge", _fmt_eur(res["trip_total_with_surcharge"]))

    with tab_pro:
        left, right = st.columns([1.05, 1.4])
        with left:
            st.markdown("### Clients")
            st.dataframe(df_clients, use_container_width=True, hide_index=True, height=260)

            names = df_clients["Client"].tolist() if not df_clients.empty else []
            mode = st.radio("Action", ["Ajouter", "Modifier / supprimer"], horizontal=True)
            selected_name = None
            selected_row = None
            if mode == "Modifier / supprimer" and names:
                selected_name = st.selectbox("Client", names)
                selected_row = df_clients[df_clients["Client"] == selected_name].iloc[0]

            with st.form("fuel_client_form", border=True):
                c1, c2 = st.columns(2)
                with c1:
                    client_name = st.text_input("Nom client", value="" if selected_row is None else str(selected_row["Client"]))
                    client_price = st.number_input("Prix / km", min_value=0.0, value=0.0 if selected_row is None else float(selected_row["Prix_km"]), step=0.01)
                with c2:
                    client_recovery = st.number_input("Récupération %", min_value=0.0, max_value=200.0, value=recovery_default if selected_row is None else float(selected_row["Recuperation_pct"]), step=5.0)
                    client_active = st.checkbox("Actif", value=True if selected_row is None else bool(selected_row["Actif"]))
                client_notes = st.text_input("Notes", value="" if selected_row is None else str(selected_row["Notes"]))

                save_client = st.form_submit_button("💾 Enregistrer le client", use_container_width=True)
                delete_client = st.form_submit_button("🗑️ Supprimer le client", use_container_width=True) if selected_row is not None else False

            if save_client:
                new_row = {
                    "Client": str(client_name).strip(),
                    "Prix_km": float(client_price),
                    "Recuperation_pct": float(client_recovery),
                    "Actif": bool(client_active),
                    "Notes": str(client_notes),
                }
                temp = df_clients.copy()
                temp = temp[temp["Client"] != (selected_name or "")]
                temp = pd.concat([temp, pd.DataFrame([new_row])], ignore_index=True)
                _save_clients_to_db(temp)
                _reload_state()
                df_clients = st.session_state["fuel_clients_df"].copy()
                st.success("Client enregistré.")

            if delete_client:
                temp = df_clients.copy()
                temp = temp[temp["Client"] != selected_name]
                _save_clients_to_db(temp)
                _reload_state()
                df_clients = st.session_state["fuel_clients_df"].copy()
                st.success("Client supprimé.")

        with right:
            st.markdown("### Calcul pro par client")
            df_active = df_clients[df_clients["Actif"] == True].copy()
            if df_active.empty:
                st.warning("Aucun client actif.")
                return

            names = df_active["Client"].tolist()
            with st.form("fuel_pro_form", border=True):
                p1, p2, p3 = st.columns(3)
                with p1:
                    selected = st.selectbox("Client", names)
                with p2:
                    km_pro = st.number_input("Km du trajet", min_value=0.0, value=180.0, step=1.0)
                with p3:
                    compare_all = st.checkbox("Comparer tous les clients", value=False)
                run_pro = st.form_submit_button("Calculer", use_container_width=True)

            if run_pro or "fuel_pro_last" not in st.session_state:
                st.session_state["fuel_pro_last"] = {"selected": selected, "km": km_pro, "compare_all": compare_all}

            provals = st.session_state["fuel_pro_last"]
            row = df_active[df_active["Client"] == provals["selected"]].iloc[0]
            metrics = _compute_metrics(
                provals["km"],
                float(row["Prix_km"]),
                diesel_base,
                diesel_current,
                conso_100,
                float(row.get("Recuperation_pct", recovery_default)),
            )

            x1, x2, x3, x4 = st.columns(4)
            x1.metric("Tarif client / km", _fmt_eur(float(row["Prix_km"])))
            x2.metric("Carburant réel / km", _fmt_eur(metrics["cost_km_current"]))
            x3.metric("Surcharge / km", _fmt_eur(metrics["applied_surcharge_km"]))
            x4.metric("Prix conseillé / km", _fmt_eur(metrics["recommended_price_km"]))

            y1, y2, y3, y4 = st.columns(4)
            y1.metric("Part carburant du tarif", _fmt_pct(metrics["price_share_pct"]))
            y2.metric("% surcharge sur tarif", _fmt_pct(metrics["surcharge_pct"]))
            y3.metric("Surcharge trajet", _fmt_eur(metrics["surcharge_total"]))
            y4.metric("Total trajet avec surcharge", _fmt_eur(metrics["trip_total_with_surcharge"]))

            if provals["compare_all"]:
                rows = []
                for _, client in df_active.iterrows():
                    cm = _compute_metrics(
                        provals["km"],
                        float(client["Prix_km"]),
                        diesel_base,
                        diesel_current,
                        conso_100,
                        float(client["Recuperation_pct"]),
                    )
                    rows.append({
                        "Client": client["Client"],
                        "Prix / km": round(float(client["Prix_km"]), 4),
                        "Récupération %": round(float(client["Recuperation_pct"]), 2),
                        "Surcharge / km": round(cm["applied_surcharge_km"], 4),
                        "% surcharge": round(cm["surcharge_pct"], 2),
                        f"Surcharge {int(provals['km'])} km": round(cm["surcharge_total"], 2),
                        f"Prix conseillé / km": round(cm["recommended_price_km"], 4),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=260)
