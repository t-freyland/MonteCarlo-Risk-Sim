import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection

# --- 1. KONFIGURATION & VERSION ---
APP_VERSION = "1.6.6"
st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. LOGIN VIA SECRETS ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    with st.form("login_form"):
        user_input = st.text_input("Benutzername")
        pw_input = st.text_input("Passwort", type="password")
        if st.form_submit_button("Anmelden"):
            try:
                if user_input == st.secrets["credentials"]["username"] and \
                   pw_input == st.secrets["credentials"]["password"]:
                    st.session_state["auth_ok"] = True
                    st.rerun()
                else: st.error("Falsche Login-Daten.")
            except: st.error("Konfigurationsfehler: Secrets nicht gefunden.")
    st.stop()

# --- 3. VERBINDUNGEN & DATEN-BEREINIGUNG ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_df(df, type="task"):
    """Bereinigt Spaltennamen und stellt Datentypen sicher."""
    df.columns = [str(c).strip() for c in df.columns]
    num_cols = ["Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Duration (Days)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    
    if type == "risk" and "Maßnahme / Mitigation" not in df.columns:
        df["Maßnahme / Mitigation"] = ""
    return df

if "data_loaded" not in st.session_state:
    try:
        t_raw = conn.read(worksheet="Tasks", ttl=0)
        r_raw = conn.read(worksheet="Risks", ttl=0)
        st.session_state.tasks = clean_df(t_raw, "task")
        st.session_state.risks = clean_df(r_raw, "risk")
        st.session_state.data_loaded = True
    except:
        st.session_state.tasks = pd.DataFrame({"Task Name": ["Entwicklung", "QA"], "Duration (Days)": [20.0, 10.0]})
        st.session_state.risks = pd.DataFrame(columns=["Risk Name", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Maßnahme / Mitigation"])
        st.session_state.data_loaded = True

# --- 4. SIMULATIONS-KERN ---
def run_fast_simulation(tasks, risks, std_risks, n):
    base_sum = tasks["Duration (Days)"].sum()
    total_days = np.full(n, base_sum, dtype=float)
    all_r = []
    
    for _, r in risks.iterrows():
        try:
            p = float(r.get("Probability (0-1)", 0))
            if p > 0:
                impact_vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
                imin, ilikely, imax = impact_vals
                if imin == imax: imax += 0.001
                target = str(r.get("Target (Global/Task)", "Global")).strip()
                ref = base_sum
                if target.lower() != "global" and target != "":
                    matched = tasks[tasks["Task Name"].astype(str).str.strip() == target]
                    if not matched.empty: ref = matched["Duration (Days)"].values[0]
                all_r.append((p, imin, ilikely, imax, ref))
        except: continue

    for sr in std_risks:
        all_r.append((sr["prob"], sr["min"], sr["likely"], sr["max"], base_sum))

    for prob, imin, ilikely, imax, ref in all_r:
        hits = np.random.random(n) < prob
        impacts = np.random.triangular(imin, ilikely, max(ilikely + 0.001, imax), n)
        total_days += (hits * impacts * ref)
    return total_days.astype(int)

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("👤 Menü")
    if st.button("Abmelden"):
        st.session_state.clear()
        st.rerun()
    st.divider()
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.subheader("🏢 Globale Standards")
    STANDARD_RISKS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_RISKS if st.checkbox(sr["name"], value=True)]

# --- 6. INPUT BEREICH (FULL-WIDTH LAYOUT) ---
st.title(f"🎲 Risk Sim Pro v{APP_VERSION}")

with st.form("main_form"):
    st.subheader("📋 1. Projekt-Struktur (Aufgaben)")
    ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="t_edit")
    
    st.divider()
    
    st.subheader("⚠️ 2. Risiko-Inventar & Mitigations-Plan")
    t_opts = ["Global"]
    if ed_tasks is not None and not ed_tasks.empty and "Task Name" in ed_tasks.columns:
        valid_names = ed_tasks["Task Name"].dropna().unique().tolist()
        t_opts += [str(name).strip() for name in valid_names if str(name).strip() != ""]
    
    # Korrigierte Konfiguration ohne das fehlerhafte 'placeholder' Argument
    risk_cfg = {
        "Target (Global/Task)": st.column_config.SelectboxColumn("Geltungsbereich", options=t_opts, width="medium"),
        "Maßnahme / Mitigation": st.column_config.TextColumn("Geplante Maßnahme (Mitigation)", width="large")
    }
    ed_risks = st.data_editor(st.session_state.risks, use_container_width=True, num_rows="dynamic", key="r_edit", column_config=risk_cfg)
    
    # Submit-Button innerhalb des Formulars
    submitted = st.form_submit_button("💾 Strategie & Daten in Cloud speichern")
    if submitted:
        st.session_state.tasks = clean_df(ed_tasks, "task")
        st.session_state.risks = clean_df(ed_risks, "risk")
        try:
            conn.update(worksheet="Tasks", data=st.session_state.tasks)
            conn.update(worksheet="Risks", data=st.session_state.risks)
            st.success("✅ Strategie erfolgreich in Google Sheets gespeichert!")
            st.cache_data.clear()
        except Exception as e: st.error(f"Fehler: {e}")

# --- 7. AUSWERTUNG & STAKEHOLDER-VIEW ---
if st.button("🚀 Simulation & Analyse starten"):
    tasks_df = st.session_state.tasks
    risks_df = st.session_state.risks
    base_days = tasks_df["Duration (Days)"].sum()

    if base_days <= 0:
        st.warning("Bitte Aufgaben eingeben.")
    else:
        with st.spinner("Berechne Monte-Carlo Szenarien..."):
            durations = run_fast_simulation(tasks_df, risks_df, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            sim_mean = np.mean(durations)

            # --- GRAFIK 1: ABSCHLUSSWAHRSCHEINLICHKEIT ---
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Häufigkeit", marker_color="#1f77b4", opacity=0.7))
            s_dates = np.sort(end_dates)
            fig.add_trace(go.Scatter(x=s_dates, y=np.linspace(0, 100, n_sim), name="Sicherheit (%)", line=dict(color='orange', width=3), yaxis="y2"))
            fig.update_layout(yaxis2=dict(overlaying="y", side="right", range=[0, 100]), template="plotly_white", title="Wahrscheinlichkeitsverteilung des Projektendes")
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red")
            st.plotly_chart(fig, use_container_width=True)

            # --- GRAFIK 2: TREIBER-ANALYSE ---
            st.divider()
            st.subheader("🎯 Warum verzögert sich das Projekt?")
            contrib = []
            for _, r in risks_df.iterrows():
                try:
                    p = float(r.get("Probability (0-1)", 0))
                    if p > 0:
                        avg_i = (float(r.get("Impact Min", 0)) + float(r.get("Impact Likely", 0)) + float(r.get("Impact Max", 0))) / 3
                        target = str(r.get("Target (Global/Task)", "Global")).strip()
                        ref = base_days
                        if target.lower() != "global" and target != "":
                            match = tasks_df[tasks_df["Task Name"].astype(str).str.strip() == target]
                            if not match.empty: ref = match["Duration (Days)"].values[0]
                        val = p * avg_i * ref
                        if val > 0: contrib.append({"Quelle": r.get("Risk Name", "Unbenannt"), "Verzögerung": round(val, 1), "Art": "Projektspezifisch"})
                except: continue

            for sr in selected_std:
                val = sr["prob"] * ((sr["min"] + sr["likely"] + sr["max"]) / 3) * base_days
                contrib.append({"Quelle": sr["name"], "Verzögerung": round(val, 1), "Art": "Markt-Standard"})

            if contrib:
                df_c = pd.DataFrame(contrib).sort_values(by="Verzögerung", ascending=True)
                colors = ['#EF553B' if a == "Projektspezifisch" else '#636EFA' for a in df_c["Art"]]
                fig_c = go.Figure(go.Bar(x=df_c["Verzögerung"], y=df_c["Quelle"], orientation='h', marker_color=colors, text=df_c["Verzögerung"].astype(str) + " Tage"))
                fig_c.update_layout(template="plotly_white", height=max(200, len(contrib)*45), xaxis_title="Einfluss auf Verzögerung (Ø Tage)")
                st.plotly_chart(fig_c, use_container_width=True)

            # --- NEU: STAKEHOLDER AKTIONSPLAN ---
            st.divider()
            st.subheader("🛠️ Aktionsplan & Mitigations-Strategie")
            action_df = risks_df[risks_df["Probability (0-1)"] > 0].copy()
            if not action_df.empty:
                # Wir zeigen nur relevante Spalten für die Stakeholder
                view_df = action_df[["Risk Name", "Target (Global/Task)", "Probability (0-1)", "Maßnahme / Mitigation"]]
                view_df.columns = ["Risiko", "Fokus", "Wahrscheinlichkeit", "Geplante Maßnahme"]
                st.table(view_df)
            else:
                st.info("Keine spezifischen Maßnahmen für aktive Risiken definiert.")

            # --- FINALE METRIKEN ---
            st.divider()
            m1, m2, m3 = st.columns(3)
            m1.metric("📅 85% Sicherheit", commit_85.strftime('%d.%m.%Y'))
            m2.metric("⏱️ Ø Projektdauer", f"{int(sim_mean)} Tage")
            m3.metric("📉 Pessimistisch (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))