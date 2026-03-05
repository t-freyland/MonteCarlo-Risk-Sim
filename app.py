import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go

# --- 1. KONFIGURATION & VERSION ---
APP_VERSION = "1.3.3"
st.set_page_config(page_title=f"Risk Sim v{APP_VERSION}", layout="wide")

# --- 2. LOGIN VIA SECRETS ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    with st.form("login_form"):
        user_input = st.text_input("Benutzername")
        pw_input = st.text_input("Passwort", type="password")
        submit = st.form_submit_button("Anmelden")
        
        if submit:
            try:
                # Prüfung gegen .streamlit/secrets.toml
                correct_user = st.secrets["credentials"]["username"]
                correct_pw = st.secrets["credentials"]["password"]
                
                if user_input == correct_user and pw_input == correct_pw:
                    st.session_state["auth_ok"] = True
                    st.rerun()
                else:
                    st.error("Falsche Daten.")
            except Exception:
                st.error("Konfigurationsfehler: .streamlit/secrets.toml nicht gefunden oder falsch formatiert.")
    st.stop()

# --- 3. HAUPTLOGIK (Wird nur erreicht, wenn auth_ok == True) ---

# --- INTERNE FUNKTIONEN ---
def run_fast_simulation(tasks, risks, std_risks, n):
    base_sum = tasks["Duration (Days)"].sum()
    total_days = np.full(n, base_sum, dtype=float)
    
    all_r = []
    for _, r in risks.iterrows():
        if pd.notnull(r["Probability (0-1)"]):
            try:
                ref = base_sum if r["Target (Global/Task)"] == "Global" else tasks.loc[tasks["Task Name"] == r["Target (Global/Task)"], "Duration (Days)"].values[0]
                all_r.append((r["Probability (0-1)"], r["Impact Min"], r["Impact Likely"], r["Impact Max"], ref))
            except: continue
    
    for sr in std_risks:
        all_r.append((sr["prob"], sr["min"], sr["likely"], sr["max"], base_sum))

    for prob, imin, ilikely, imax, ref in all_r:
        hits = np.random.random(n) < prob
        imax_adj = max(ilikely, imax)
        impacts = np.random.triangular(imin, ilikely, imax_adj, n)
        total_days += (hits * impacts * ref)
        
    return total_days.astype(int)

# --- SIDEBAR ---
with st.sidebar:
    st.header("👤 Menü")
    if st.button("Abmelden"):
        st.session_state["auth_ok"] = False
        st.rerun()
        
    st.divider()
    st.subheader("⚙️ Projekt Setup")
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.divider()
    st.subheader("🏢 Globale Standards")
    STANDARD_RISKS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Feature Inflation (Scope Creep)", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_RISKS if st.checkbox(sr["name"])]

# --- HAUPTBEREICH ---
st.title(f"🎲 Project Risk Simulator Pro")
st.caption(f"Eingeloggt | Version {APP_VERSION}")

# Session State Init
if "tasks" not in st.session_state:
    st.session_state.tasks = pd.DataFrame({"Task Name": ["Entwicklung", "QA"], "Duration (Days)": [20, 10]})
if "risks" not in st.session_state:
    st.session_state.risks = pd.DataFrame(columns=["Risk Name", "Target (Global/Task)", "Risk Type", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max"])

# Eingabe-Formular
with st.form("input_data_form_final"):
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📋 Aufgaben")
        ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="task_edit_main")
    with col2:
        st.subheader("⚠️ Eigene Risiken")
        t_opts = ["Global"] + list(ed_tasks["Task Name"].unique())
        risk_cfg = {
            "Target (Global/Task)": st.column_config.SelectboxColumn("Geltungsbereich", options=t_opts, required=True),
            "Risk Type": st.column_config.SelectboxColumn("Typ", options=["Binary", "Continuous"], required=True),
        }
        ed_risks = st.data_editor(st.session_state.risks, column_config=risk_cfg, use_container_width=True, num_rows="dynamic", key="risk_edit_main")
    
    if st.form_submit_button("💾 Daten fixieren"):
        st.session_state.tasks, st.session_state.risks = ed_tasks, ed_risks
        st.success("Daten fixiert!")

# Simulation starten
if st.button("🚀 Simulation starten"):
    with st.spinner("Berechne..."):
        durations = run_fast_simulation(st.session_state.tasks, st.session_state.risks, selected_std, n_sim)
        start_np = np.datetime64(start_date)
        end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
        commit_85 = pd.Series(end_dates).quantile(0.85)

        fig = go.Figure()
        fig.add_trace(go.Histogram(x=end_dates, name="Verteilung", marker_color="#1f77b4", opacity=0.7))
        s_dates = np.sort(end_dates)
        fig.add_trace(go.Scatter(x=s_dates, y=np.linspace(0, 100, n_sim), name="Sicherheit (%)", line=dict(color='orange', width=3), yaxis="y2"))
        
        fig.update_layout(
            yaxis2=dict(overlaying="y", side="right", range=[0, 100]),
            template="plotly_white", 
            height=450, 
            margin=dict(l=20, r=20, t=40, b=20)
        )
        
        fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("85% Sicherheit", commit_85.strftime('%d.%m.%Y'))
        m2.metric("Ø Dauer", f"{int(np.mean(durations))} Tage")
        m3.metric("Pessimistisch (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))

# Export
st.divider()
csv = st.session_state.risks.to_csv(index=False).encode('utf-8')
st.download_button("📥 Risiko-Export (CSV)", data=csv, file_name="risk_data.csv", mime="text/csv", key="download_csv_main")