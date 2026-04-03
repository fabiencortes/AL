import re
from io import BytesIO

import pandas as pd
import requests
import streamlit as st

DIESEL_REFERENCE_DEFAULT = 1.54
DEFAULT_TABLE = [{"KM": 0.0, "Prix/km (€)": 1.80}]


@st.cache_data(ttl=3600)
def fetch_official_diesel_price() -> tuple[float | None, str]:
    urls = [
        "https://economie.fgov.be/fr/themes/energie/prix-de-lenergie/prix-maximum-des-produits/tarif-officiel-des-produits",
        "https://economie.fgov.be/nl/themes/energie/energieprijzen/maximumprijzen-van-de/officieel-tarief-der",
    ]
    patterns = [
        r"Diesel\s*B7[^\d]{0,80}(\d+[\.,]\d{3,4})",
        r"Gasolie\s*diesel\s*B7[^\d]{0,80}(\d+[\.,]\d{3,4})",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}

    for url in urls:
        try:
            r = requests.get(url, timeout=12, headers=headers)
            r.raise_for_status()
            text = r.text.replace("\xa0", " ")
            for pat in patterns:
                m = re.search(pat, text, flags=re.IGNORECASE | re.DOTALL)
                if m:
                    raw = m.group(1).replace(",", ".")
                    return float(raw), url
        except Exception:
            continue
    return None, ""


def _default_grid() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Hausse diesel min (%)": 0.0, "Surcharge appliquée (%)": 0.0},
            {"Hausse diesel min (%)": 5.0, "Surcharge appliquée (%)": 2.0},
            {"Hausse diesel min (%)": 10.0, "Surcharge appliquée (%)": 4.0},
            {"Hausse diesel min (%)": 20.0, "Surcharge appliquée (%)": 7.0},
            {"Hausse diesel min (%)": 30.0, "Surcharge appliquée (%)": 11.0},
            {"Hausse diesel min (%)": 40.0, "Surcharge appliquée (%)": 15.0},
        ]
    )


def _compute_auto_pct(reference: float, current: float, factor_pct: float) -> float:
    if reference <= 0 or current <= 0:
        return 0.0
    rise_pct = ((current - reference) / reference) * 100.0
    if rise_pct <= 0:
        return 0.0
    return round(rise_pct * (factor_pct / 100.0), 2)


def render_fuel_surcharge_quick_tab() -> None:
    st.subheader("⛽ Surcharge carburant")
    st.caption("Calcul ultra rapide : tu tapes les KM et le prix/km, le total se calcule directement selon ta grille.")

    if "fuel_quick_diesel_today" not in st.session_state:
        st.session_state["fuel_quick_diesel_today"] = 2.30
    if "fuel_quick_grid" not in st.session_state:
        st.session_state["fuel_quick_grid"] = _default_grid()
    if "fuel_quick_rows" not in st.session_state:
        st.session_state["fuel_quick_rows"] = pd.DataFrame(DEFAULT_TABLE)

    with st.sidebar:
        st.markdown("### ⛽ Réglages carburant")
        diesel_reference = st.number_input(
            "Base diesel (€ / L)", min_value=0.01, value=DIESEL_REFERENCE_DEFAULT, step=0.01,
            key="fuel_quick_reference"
        )

        col_a, col_b = st.columns([1, 1])
        with col_a:
            use_official = st.button("Prix officiel", key="fuel_quick_official")
        with col_b:
            reset_grid = st.button("Reset grille", key="fuel_quick_reset")

        if use_official:
            price, source = fetch_official_diesel_price()
            if price is not None:
                st.session_state["fuel_quick_diesel_today"] = price
                st.success(f"Prix chargé : {price:.3f} €/L")
                if source:
                    st.caption(source)
            else:
                st.warning("Prix officiel non récupéré. Tu peux le saisir à la main.")

        diesel_today = st.number_input(
            "Prix diesel du jour (€ / L)",
            min_value=0.01,
            value=float(st.session_state.get("fuel_quick_diesel_today", 2.30)),
            step=0.01,
            key="fuel_quick_diesel_today",
        )

        factor_pct = st.slider(
            "Part du carburant dans ton coût (%)",
            min_value=10,
            max_value=60,
            value=35,
            step=1,
            key="fuel_quick_factor",
        )

        if reset_grid:
            st.session_state["fuel_quick_grid"] = _default_grid()

        st.markdown("### Grille de surcharge")
        edited_grid = st.data_editor(
            st.session_state["fuel_quick_grid"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            key="fuel_quick_grid_editor",
        )
        st.session_state["fuel_quick_grid"] = edited_grid.copy()

    rise_pct = max(0.0, round(((diesel_today - diesel_reference) / diesel_reference) * 100.0, 2))
    auto_pct = _compute_auto_pct(diesel_reference, diesel_today, factor_pct)

    grid_df = st.session_state["fuel_quick_grid"].copy()
    for col in ["Hausse diesel min (%)", "Surcharge appliquée (%)"]:
        if col in grid_df.columns:
            grid_df[col] = pd.to_numeric(grid_df[col], errors="coerce").fillna(0.0)

    if not grid_df.empty:
        grid_df = grid_df.sort_values("Hausse diesel min (%)")
        applicable = grid_df[grid_df["Hausse diesel min (%)"] <= rise_pct]
        grid_pct = float(applicable.iloc[-1]["Surcharge appliquée (%)"]) if not applicable.empty else 0.0
    else:
        grid_pct = 0.0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Base", f"{diesel_reference:.2f} €")
    col2.metric("Aujourd'hui", f"{diesel_today:.2f} €")
    col3.metric("Hausse diesel", f"{rise_pct:.2f} %")
    col4.metric("Surcharge grille", f"{grid_pct:.2f} %")

    st.markdown("### Calcul ultra rapide")
    left, right = st.columns([1, 2])
    with left:
        km = st.number_input("Nombre de km", min_value=0.0, value=0.0, step=1.0, key="fuel_quick_km")
        price_per_km = st.number_input("Prix au km (€)", min_value=0.0, value=1.80, step=0.01, key="fuel_quick_ppk")
        surcharge_mode = st.radio("Méthode", ["Grille", "Calcul automatique"], horizontal=True, key="fuel_quick_mode")
        used_pct = grid_pct if surcharge_mode == "Grille" else auto_pct

        base_total = round(km * price_per_km, 2)
        surcharge_eur = round(base_total * used_pct / 100.0, 2)
        final_total = round(base_total + surcharge_eur, 2)

        st.success(
            f"Prix base : {base_total:.2f} €\n\n"
            f"Surcharge : {surcharge_eur:.2f} € ({used_pct:.2f} %)\n\n"
            f"Total : {final_total:.2f} €"
        )

    with right:
        st.markdown("### Tableau rapide")
        quick_df = st.data_editor(
            st.session_state["fuel_quick_rows"],
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            key="fuel_quick_rows_editor",
        )
        st.session_state["fuel_quick_rows"] = quick_df.copy()

        calc_df = quick_df.copy()
        if calc_df.empty:
            calc_df = pd.DataFrame(columns=["KM", "Prix/km (€)"])

        for col in ["KM", "Prix/km (€)"]:
            if col not in calc_df.columns:
                calc_df[col] = 0.0
            calc_df[col] = pd.to_numeric(calc_df[col], errors="coerce").fillna(0.0)

        used_pct_table = grid_pct if surcharge_mode == "Grille" else auto_pct
        calc_df["Prix base (€)"] = (calc_df["KM"] * calc_df["Prix/km (€)"]).round(2)
        calc_df["Surcharge %"] = used_pct_table
        calc_df["Surcharge (€)"] = (calc_df["Prix base (€)"] * used_pct_table / 100.0).round(2)
        calc_df["Total (€)"] = (calc_df["Prix base (€)"] + calc_df["Surcharge (€)"]).round(2)

        st.dataframe(calc_df, use_container_width=True, hide_index=True)

        total_base = round(calc_df["Prix base (€)"].sum(), 2) if not calc_df.empty else 0.0
        total_surcharge = round(calc_df["Surcharge (€)"].sum(), 2) if not calc_df.empty else 0.0
        total_final = round(calc_df["Total (€)"].sum(), 2) if not calc_df.empty else 0.0

        a, b, c = st.columns(3)
        a.metric("Base total", f"{total_base:.2f} €")
        b.metric("Surcharge totale", f"{total_surcharge:.2f} €")
        c.metric("Total général", f"{total_final:.2f} €")

        export_buffer = BytesIO()
        with pd.ExcelWriter(export_buffer, engine="openpyxl") as writer:
            calc_df.to_excel(writer, index=False, sheet_name="Calcul carburant")
        export_buffer.seek(0)

        st.download_button(
            "⬇️ Télécharger Excel",
            data=export_buffer.getvalue(),
            file_name="calcul_surcharge_carburant.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="fuel_quick_export",
        )


def _standalone_main() -> None:
    st.set_page_config(page_title="Surcharge carburant simple", layout="wide")
    st.title("⛽ Surcharge carburant — calcul rapide")
    render_fuel_surcharge_quick_tab()


if __name__ == "__main__":
    _standalone_main()
