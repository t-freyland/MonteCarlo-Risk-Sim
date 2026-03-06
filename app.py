import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import plotly.graph_objects as go

# --- 1. SETUP ---
APP_VERSION = "2.0.5 (SQL Edition)"
PROJECTS = ["Projekt_Alpha", "Projekt_Beta", "Projekt_Gamma", "Test_Sandbox"]
DB_FILE = "risk_management.db"

st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. SESSION STATE ---
for key in ["snapshot_durations", "snapshot_date", "auth_ok", "last_durations", "last_commit_85"]:
    if key not in st.session_state:
        st.session_state[key] = None

# --- 3. DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, task_name TEXT, duration REAL, description TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS risks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, risk_name TEXT, risk_type TEXT, target TEXT, prob REAL, impact_min REAL, impact_likely REAL, impact_max REAL, mitigation TEXT)")
    conn.commit()
    conn.close()

init_db()

def load_tasks(project):
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT task_name as 'Task Name', duration as 'Duration (Days)', description as 'Beschreibung' FROM tasks WHERE project='{project}'", conn)
    conn.close()
    if df.empty: df = pd.DataFrame([{"Task Name": "Basis-Task", "Duration (Days)": 5.0, "Beschreibung": ""}])
    return df

def load_risks(project):
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT risk_name as 'Risk Name', risk_type as 'Risk Type', target as 'Target (Global/Task)', prob as 'Probability (0-1)', impact_min as 'Impact Min', impact_likely as 'Impact Likely', impact_max as 'Impact Max', mitigation as 'Maßnahme / Mitigation' FROM risks WHERE project='{project}'", conn)
    conn.close()
    if df.empty: df = pd.DataFrame(columns=["Risk Name", "Risk Type", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Maßnahme / Mitigation"])
    return df

# --- 4. LOGIN ---
if not st.session_state.auth_ok:
    st.title("🔐 Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Login"):
        if "credentials" in st.secrets and u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
            st.session_state.auth_ok = True
            st.rerun()
        else: st.error("Login falsch.")
    st.stop()

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("📂 Projekt")
    selected_proj = st.selectbox("Auswahl:", PROJECTS)
    start_date = st.date_input("Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000)
    
    st.divider()
    st.subheader("🏢 Globale Standards")
    STD_DEFS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [s for s in STD_DEFS if st.checkbox(s["name"], value=True)]
    
    st.divider()
    if st.button("📸 Stand einfrieren") and st.session_state.last_durations is not None:
        st.session_state.snapshot_durations = st.session_state.last_durations
        st.session_state.snapshot_date = st.session_state.last_commit_85
        st.success("Referenz gespeichert!")
    if st.button("🗑️ Reset Vergleich"):
        st.session_state.snapshot_durations = None
        st.session_state.snapshot_date = None
        st.rerun()

# --- 6. INPUT BEREICH ---
st.title(f"🎲 {selected_proj} | v{APP_VERSION}")

# TASKS
st.subheader("📋 1. Projektstruktur (Tasks)")
t_curr = load_tasks(selected_proj)
ed_t = st.data_editor(t_curr, use_container_width=True, num_rows="dynamic", key=f"t_ed_{selected_proj}")

if st.button("💾 Tasks speichern", key="save_t"):
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (selected_proj,))
    for _, row in ed_t.iterrows():
        if str(row.get("Task Name", "")).strip():
            conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                         (selected_proj, str(row["Task Name"]), float(row["Duration (Days)"]), str(row["Beschreibung"])))
    conn.commit()
    conn.close()
    st.success("Tasks gespeichert!")
    st.rerun()

st.divider()

# RISIKEN
st.subheader("⚠️ 2. Risiko-Register")
r_curr = load_risks(selected_proj)
t_opts = ["Global"] + t_curr["Task Name"].tolist()

risk_config = {
    "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], required=True),
    "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_opts, required=True)
}
ed_r = st.data_editor(r_curr, use_container_width=True, num_rows="dynamic", key=f"r_ed_{selected_proj}", column_config=risk_config)

if st.button("💾 Risiken speichern", key="save_r"):
    conn = get_db_connection()
    conn.execute("DELETE FROM risks WHERE project=?", (selected_proj,))
    for _, row in ed_r.iterrows():
        if str(row.get("Risk Name", "")).strip():
            conn.execute("INSERT INTO risks (project, risk_name, risk_type, target, prob, impact_min, impact_likely, impact_max, mitigation) VALUES (?,?,?,?,?,?,?,?,?)",
                         (selected_proj, str(row["Risk Name"]), str(row["Risk Type"]), str(row["Target (Global/Task)"]), 
                          float(row["Probability (0-1)"]), float(row["Impact Min"]), float(row["Impact Likely"]), 
                          float(row["Impact Max"]), str(row["Maßnahme / Mitigation"])))
    conn.commit()
    conn.close()
    st.success("Risiken gespeichert!")
    st.rerun()

# --- 7. SIMULATIONS-LOGIK MIT IMPACT-ANALYSE ---
def run_fast_simulation(tasks, risks, std_risks, n):
    # Basis
    task_durations = np.tile(tasks["Duration (Days)"].values, (n, 1)).astype(float)
    base_sum = tasks["Duration (Days)"].sum()
    impact_results = []
    
    # 1. Spezifische Risiken berechnen
    for _, r in risks.iterrows():
        p = float(r.get("Probability (0-1)", 0))
        if p <= 0: continue
        
        vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
        target, r_name = str(r.get("Target (Global/Task)", "Global")), str(r.get("Risk Name", "Unbekannt"))
        
        hits = np.random.random(n) < p
        impacts = np.random.triangular(vals[0], vals[1], max(vals[1]+0.001, vals[2]), n)
        
        delay_contribution = 0
        if target == "Global":
            delay_contribution = (hits * impacts * base_sum).mean()
            if str(r.get("Risk Type")) == "Kontinuierlich":
                task_durations *= (1 + (hits * impacts))
            else:
                task_durations += (hits * impacts * base_sum / len(tasks)).reshape(-1, 1)
        else:
            if target in tasks["Task Name"].values:
                idx = tasks.index[tasks["Task Name"] == target][0]
                t_dur = tasks.iloc[idx]["Duration (Days)"]
                delay_contribution = (hits * impacts * t_dur).mean()
                if str(r.get("Risk Type")) == "Kontinuierlich":
                    task_durations[:, idx] *= (1 + (hits * impacts))
                else:
                    task_durations[:, idx] += (hits * impacts * t_dur)
        
        impact_results.append({"Risiko": r_name, "Ø Verzögerung (Tage)": round(delay_contribution, 1)})

    total_days = task_durations.sum(axis=1)
    
    # 2. Standard-Risiken
    for sr in std_risks:
        hits = np.random.random(n) < sr["prob"]
        impacts = np.random.triangular(sr["min"], sr["likely"], sr["max"], n)
        delay_avg = (total_days * (hits * impacts)).mean()
        total_days = total_days * (1 + (hits * impacts))
        impact_results.append({"Risiko": f"STD: {sr['name']}", "Ø Verzögerung (Tage)": round(delay_avg, 1)})
        
    return total_days.astype(int), pd.DataFrame(impact_results)

st.divider()

if st.button("🚀 Simulation starten"):
    if t_curr["Duration (Days)"].sum() <= 0:
        st.warning("Keine Task-Dauer vorhanden.")
    else:
        with st.spinner("Monte-Carlo & Sensitivitäts-Analyse läuft..."):
            durations, impact_df = run_fast_simulation(t_curr, ed_r, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            
            st.session_state.last_durations = durations
            st.session_state.last_commit_85 = commit_85

            # ERGEBNISSE
            c1, c2 = st.columns([2, 1])
            with c1:
                fig = go.Figure()
                fig.add_trace(go.Histogram(x=end_dates, name="Aktuell", marker_color="#1f77b4", opacity=0.7))
                if st.session_state.snapshot_date:
                    ref_ends = pd.to_datetime(np.busday_offset(start_np, st.session_state.snapshot_durations, roll='forward'))
                    fig.add_trace(go.Histogram(x=ref_ends, name="Referenz", marker_color="#7f7f7f", opacity=0.3))
                fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red", annotation_text="85% Sicherheit")
                fig.update_layout(barmode='overlay', template="plotly_white", margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                st.metric("Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'))
                st.metric("Ø Dauer", f"{int(np.mean(durations))} Tage")
                
                st.subheader("🔥 Top Zeitfresser")
                if not impact_df.empty:
                    impact_df = impact_df.sort_values("Ø Verzögerung (Tage)", ascending=False)
                    st.dataframe(impact_df, use_container_width=True, hide_index=True, column_config={
                        "Ø Verzögerung (Tage)": st.column_config.ProgressColumn(format="%f d", min_value=0, max_value=float(impact_df["Ø Verzögerung (Tage)"].max() if not impact_df.empty else 100))
                    })