import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go

# --- SEITEN KONFIGURATION ---
st.set_page_config(page_title="Monte Carlo Risk Simulation Pro", layout="wide")

st.title("🎲 Project Risk Simulator Pro")
st.markdown("Professionelle Risiko-Analyse mit **globalen** und **lokalen** Faktoren.")

# --- INDUSTRIE STANDARDS ---
STANDARD_RISKS = [
    {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
    {"name": "Feature Inflation (Scope Creep)", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
    {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
    {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
]

# --- INITIALISIERUNG ---
if "tasks" not in st.session_state:
    st.session_state.tasks = pd.DataFrame({"Task Name": ["Backend Entwicklung", "Frontend Integration"], "Duration (Days)": [20, 15]})

if "risks" not in st.session_state:
    st.session_state.risks = pd.DataFrame(columns=[
        "Risk Name", "Target (Global/Task)", "Risk Type", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max"
    ])

# --- SIDEBAR SETUP ---
with st.sidebar:
    st.header("⚙️ Projekt Setup")
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.divider()
    st.header("🏢 Globale Standards")
    selected_std = [sr for sr in STANDARD_RISKS if st.checkbox(sr["name"])]

# --- EINGABE FORMULAR ---
with st.form("main_input_form"):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📋 Aufgaben")
        ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="tasks_editor")
    with c2:
        st.subheader("⚠️ Eigene Risiken")
        t_opts = ["Global"] + list(ed_tasks["Task Name"].unique())
        risk_cfg = {
            "Target (Global/Task)": st.column_config.SelectboxColumn("Geltungsbereich", options=t_opts, required=True),
            "Risk Type": st.column_config.SelectboxColumn("Typ", options=["Binary", "Continuous"], required=True),
            "Probability (0-1)": st.column_config.NumberColumn("Wahrsch.", min_value=0.0, max_value=1.0, format="%.2f"),
            "Impact Min": st.column_config.NumberColumn("Min (0-1)"),
            "Impact Likely": st.column_config.NumberColumn("Likely (0-1)"),
            "Impact Max": st.column_config.NumberColumn("Max (0-1)"),
        }
        ed_risks = st.data_editor(st.session_state.risks, column_config=risk_cfg, use_container_width=True, num_rows="dynamic", key="risks_editor")

    if st.form_submit_button("💾 Daten fixieren"):
        st.session_state.tasks, st.session_state.risks = ed_tasks, ed_risks
        st.success("Daten wurden übernommen!")

# --- VEKTORISIERTE SIMULATION (HIGH SPEED) ---
def run_fast_simulation(tasks, risks, std_risks, n):
    base_sum = tasks["Duration (Days)"].sum()
    total_days = np.full(n, base_sum, dtype=float)
    
    # Risiken kombinieren
    all_r = []
    for _, r in risks.iterrows():
        if pd.notnull(r["Probability (0-1)"]):
            ref = base_sum if r["Target (Global/Task)"] == "Global" else tasks.loc[tasks["Task Name"]==r["Target (Global/Task)"], "Duration (Days)"].values[0]
            all_r.append((r["Probability (0-1)"], r["Impact Min"], r["Impact Likely"], r["Impact Max"], ref))
    for sr in std_risks:
        all_r.append((sr["prob"], sr["min"], sr["likely"], sr["max"], base_sum))

    for prob, imin, ilikely, imax, ref in all_r:
        hits = np.random.random(n) < prob
        # Sicherheits-Check: Max muss >= Likely sein
        imax_adj = max(ilikely, imax)
        impacts = np.random.triangular(imin, ilikely, imax_adj, n)
        total_days += (hits * impacts * ref)
        
    return total_days.astype(int)

# --- VISUALISIERUNG ---
if st.button("🚀 Simulation starten"):
    with st.spinner("Simuliere Szenarien..."):
        durations = run_fast_simulation(st.session_state.tasks, st.session_state.risks, selected_std, n_sim)
        
        start_np = np.datetime64(start_date)
        end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
        
        # Statistiken
        commit_85 = pd.Series(end_dates).quantile(0.85)
        
        # Plotly Graph
        fig = go.Figure()
        
        # Histogramm
        fig.add_trace(go.Histogram(
            x=end_dates, 
            name="Simulation", 
            marker_color="#1f77b4", 
            opacity=0.7,
            nbinsx=40
        ))
        
        # S-Kurve (Sicherheit)
        s_dates = np.sort(end_dates)
        fig.add_trace(go.Scatter(
            x=s_dates, 
            y=np.linspace(0, 100, n_sim), 
            name="Sicherheit (%)", 
            line=dict(color='orange', width=3), 
            yaxis="y2"
        ))

        fig.update_layout(
            title="Wahrscheinlichkeitsverteilung des Release-Datums",
            xaxis_title="Datum",
            yaxis=dict(title="Anzahl Szenarien"),
            yaxis2=dict(title="Sicherheit (%)", overlaying="y", side="right", range=[0, 100]),
            template="plotly_white",
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            height=500
        )
        
        # Rote Commitment-Linie
        fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red", 
                     annotation_text=f"85% Commitment: {commit_85.strftime('%d.%m.')}")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Dashboard Metriken
        st.divider()
        m1, m2, m3 = st.columns(3)
        m1.metric("85% Sicherheit", commit_85.strftime('%d.%m.%Y'))
        m2.metric("Durchschn. Dauer", f"{int(np.mean(durations))} Werktage")
        m3.metric("Pessimistisch (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))