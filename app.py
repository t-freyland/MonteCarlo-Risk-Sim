import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime
import plotly.graph_objects as go

# --- 1. SETUP & CONFIG ---
APP_VERSION = "2.1.1 (Master Report Edition)"
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

def get_all_projects():
    conn = get_db_connection()
    df = pd.read_sql("SELECT DISTINCT project FROM tasks UNION SELECT DISTINCT project FROM risks", conn)
    conn.close()
    projs = df['project'].tolist()
    return projs if projs else ["Demo_Projekt"]

def load_tasks(project):
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT task_name as 'Task Name', duration as 'Duration (Days)', description as 'Beschreibung' FROM tasks WHERE project=?", conn, params=(project,))
    conn.close()
    if df.empty: df = pd.DataFrame([{"Task Name": "Basis-Task", "Duration (Days)": 10.0, "Beschreibung": ""}])
    return df

def load_risks(project):
    conn = get_db_connection()
    df = pd.read_sql(f"SELECT risk_name as 'Risk Name', risk_type as 'Risk Type', target as 'Target (Global/Task)', prob as 'Probability (0-1)', impact_min as 'Impact Min', impact_likely as 'Impact Likely', impact_max as 'Impact Max', mitigation as 'Maßnahme / Mitigation' FROM risks WHERE project=?", conn, params=(project,))
    conn.close()
    if df.empty: df = pd.DataFrame(columns=["Risk Name", "Risk Type", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Maßnahme / Mitigation"])
    return df

def save_data(project, df_tasks, df_risks):
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    for _, row in df_tasks.iterrows():
        if str(row.get("Task Name", "")).strip():
            conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                         (project, str(row["Task Name"]), float(row["Duration (Days)"]), str(row["Beschreibung"])))
    for _, row in df_risks.iterrows():
        if str(row.get("Risk Name", "")).strip():
            conn.execute("INSERT INTO risks (project, risk_name, risk_type, target, prob, impact_min, impact_likely, impact_max, mitigation) VALUES (?,?,?,?,?,?,?,?,?)",
                         (project, str(row["Risk Name"]), str(row["Risk Type"]), str(row["Target (Global/Task)"]), 
                          float(row["Probability (0-1)"]), float(row["Impact Min"]), float(row["Impact Likely"]), 
                          float(row["Impact Max"]), str(row["Maßnahme / Mitigation"])))
    conn.commit()
    conn.close()

# --- 4. LOGIN ---
if not st.session_state.auth_ok:
    st.title("🔐 Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Anmelden"):
        if "credentials" in st.secrets and u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
            st.session_state.auth_ok = True
            st.rerun()
        else: st.error("Login fehlgeschlagen.")
    st.stop()

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("📂 Projekt-Steuerung")
    all_projs = get_all_projects()
    selected_proj = st.selectbox("Aktives Projekt:", all_projs)
    
    with st.expander("🛠️ Admin, Export & Import"):
        new_p = st.text_input("Neues Projekt / Kopie:")
        if st.button("🚀 Erstellen"):
            save_data(new_p, load_tasks(selected_proj), load_risks(selected_proj))
            st.rerun()
        
        st.divider()
        t_exp = load_tasks(selected_proj)
        r_exp = load_risks(selected_proj)
        export_payload = json.dumps({"project": selected_proj, "tasks": t_exp.to_dict(orient="records"), "risks": r_exp.to_dict(orient="records")}, indent=2)
        st.download_button("📤 Projekt Export (JSON)", export_payload, f"{selected_proj}_export.json")
        
        imp_file = st.file_uploader("📥 Projekt Import", type=["json"])
        if imp_file:
            imp_data = json.load(imp_file)
            if st.button("✅ Import bestätigen"):
                save_data(imp_data["project"], pd.DataFrame(imp_data["tasks"]), pd.DataFrame(imp_data["risks"]))
                st.rerun()

        st.divider()
        if st.button(f"🗑️ {selected_proj} löschen"):
            conn = get_db_connection()
            conn.execute("DELETE FROM tasks WHERE project=?", (selected_proj,))
            conn.execute("DELETE FROM risks WHERE project=?", (selected_proj,))
            conn.commit(); conn.close(); st.rerun()

    st.divider()
    st.subheader("📊 Szenarien-Vergleich")
    if st.button("📸 Stand einfrieren"):
        if st.session_state.last_durations is not None:
            st.session_state.snapshot_durations = st.session_state.last_durations
            st.session_state.snapshot_date = st.session_state.last_commit_85
            st.success("Referenz gespeichert!")
    
    st.divider()
    st.subheader("🏢 Globale Standards")
    STANDARD_DEFS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_DEFS if st.checkbox(sr["name"], value=True)]
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000)
    start_date = st.date_input("Projekt-Start", datetime.now())

# --- 6. MAIN ---
st.title(f"🎲 {selected_proj} | v{APP_VERSION}")

t_curr = load_tasks(selected_proj)
st.subheader("📋 1. Projektstruktur")
ed_t = st.data_editor(t_curr, use_container_width=True, num_rows="dynamic", key=f"t_{selected_proj}")
if st.button("💾 Tasks speichern"):
    save_data(selected_proj, ed_t, load_risks(selected_proj)); st.rerun()

st.divider()

r_curr = load_risks(selected_proj)
st.subheader("⚠️ 2. Risiko-Register")
t_opts = ["Global"] + t_curr["Task Name"].tolist()
risk_config = {
    "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], required=True),
    "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_opts, required=True)
}
ed_r = st.data_editor(r_curr, use_container_width=True, num_rows="dynamic", key=f"r_{selected_proj}", column_config=risk_config)
if st.button("💾 Risiken speichern"):
    save_data(selected_proj, ed_t, ed_r); st.rerun()

# --- 7. SIMULATION & ANALYTICS ---
def run_fast_simulation(tasks, risks, std_risks, n):
    task_durations = np.tile(tasks["Duration (Days)"].values, (n, 1)).astype(float)
    base_sum = tasks["Duration (Days)"].sum()
    impact_results = []
    
    for _, r in risks.iterrows():
        p = float(r.get("Probability (0-1)", 0))
        if p <= 0: continue
        vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
        hits = np.random.random(n) < p
        impacts = np.random.triangular(vals[0], vals[1], max(vals[1]+0.001, vals[2]), n)
        
        target = str(r.get("Target (Global/Task)", "Global"))
        relevant_duration = base_sum if target == "Global" else tasks[tasks["Task Name"]==target]["Duration (Days)"].sum()
        
        if target == "Global":
            if str(r.get("Risk Type")) == "Kontinuierlich": task_durations *= (1 + (hits * impacts))
            else: task_durations += (hits * impacts * base_sum / len(tasks)).reshape(-1, 1)
        else:
            if target in tasks["Task Name"].values:
                idx = tasks.index[tasks["Task Name"] == target][0]
                if str(r.get("Risk Type")) == "Kontinuierlich": task_durations[:, idx] *= (1 + (hits * impacts))
                else: task_durations[:, idx] += (hits * impacts * tasks.iloc[idx]["Duration (Days)"])
        
        avg_delay = (hits * impacts * relevant_duration).mean()
        impact_results.append({"Quelle": str(r["Risk Name"]), "Verzögerung": avg_delay, "Typ": "Projekt"})

    total_days = task_durations.sum(axis=1)
    for sr in std_risks:
        hits = np.random.random(n) < sr["prob"]
        impacts = np.random.triangular(sr["min"], sr["likely"], sr["max"], n)
        avg_delay = (total_days * (hits * impacts)).mean()
        total_days = total_days * (1 + (hits * impacts))
        impact_results.append({"Quelle": sr["name"], "Verzögerung": avg_delay, "Typ": "Standard"})
        
    return total_days.astype(int), pd.DataFrame(impact_results)

st.divider()

if st.button("🚀 Simulation & Analyse starten"):
    with st.spinner("Monte-Carlo & Impact-Ranking..."):
        durations, impact_df = run_fast_simulation(t_curr, ed_r, selected_std, n_sim)
        start_np = np.datetime64(start_date)
        end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
        commit_85 = pd.Series(end_dates).quantile(0.85)
        st.session_state.last_durations, st.session_state.last_commit_85 = durations, commit_85

        # --- VISUALISIERUNG ---
        col_main, col_side = st.columns([2, 1])
        
        with col_main:
            fig = go.Figure()
            # Aktuelles Histogramm
            fig.add_trace(go.Histogram(x=end_dates, name="Aktuell", marker_color="#1f77b4", opacity=0.6))
            
            # Referenz Histogramm
            if st.session_state.snapshot_date:
                ref_ends = pd.to_datetime(np.busday_offset(start_np, st.session_state.snapshot_durations, roll='forward'))
                fig.add_trace(go.Histogram(x=ref_ends, name="Referenz", marker_color="#7f7f7f", opacity=0.3))
                # Referenz Linie gestrichelt
                fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, 
                             line_dash="dash", line_color="#7f7f7f", 
                             annotation_text=f"Ref: {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")

            # Aktuelle 85% Linie
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="solid", line_color="red", 
                         annotation_text=f"Ziel 85%: {commit_85.strftime('%d.%m.%Y')}", 
                         annotation_position="top left")
            
            fig.update_layout(template="plotly_white", barmode='overlay', margin=dict(l=0,r=0,t=30,b=0), height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col_side:
            st.metric("Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'))
            base_dur = t_curr['Duration (Days)'].sum()
            mean_dur = np.mean(durations)
            st.metric("Puffer benötigt", f"{int(mean_dur - base_dur)} Tage")
            if st.session_state.snapshot_date:
                delta = (commit_85 - st.session_state.snapshot_date).days
                st.info(f"Verschiebung zur Referenz: {delta} Tage")

        # --- TORNADO CHART ---
        st.subheader("🎯 Risiko Impact Overview (Tornado Chart)")
        
        if not impact_df.empty:
            impact_df = impact_df.sort_values("Verzögerung", ascending=True)
            fig_tornado = go.Figure(go.Bar(
                x=impact_df["Verzögerung"], y=impact_df["Quelle"], orientation='h',
                marker_color=['#EF553B' if t == "Projekt" else '#636EFA' for t in impact_df["Typ"]]
            ))
            fig_tornado.update_layout(template="plotly_white", xaxis_title="Ø Verzögerung in Arbeitstagen", height=max(300, len(impact_df)*35), margin=dict(l=200))
            st.plotly_chart(fig_tornado, use_container_width=True)
            
        # --- VOLLSTÄNDIGER REPORT EXPORT ---
        st.subheader("📜 Management Report")
        
        # Report generieren
        risk_ranking_str = ""
        if not impact_df.empty:
            top_risks = impact_df.sort_values("Verzögerung", ascending=False)
            for i, row in top_risks.iterrows():
                risk_ranking_str += f"- {row['Quelle']}: ~{row['Verzögerung']:.1f} Tage ({row['Typ']})\n"

        report_content = f"""MANAGEMENT RISK REPORT - {selected_proj}
Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M')}
---------------------------------------------------------
PROJEKT-STATUS:
- Geplanter Start: {start_date.strftime('%d.%m.%Y')}
- Basis-Dauer (Netto): {base_dur:.1f} Tage
- Aktueller Zieltermin (85% Konfidenz): {commit_85.strftime('%d.%m.%Y')}
- Durchschnittliche Projektdauer: {mean_dur:.1f} Tage
- Erforderlicher Risiko-Puffer: {int(mean_dur - base_dur)} Arbeitstage

SZENARIEN-VERGLEICH:
"""
        if st.session_state.snapshot_date:
            delta = (commit_85 - st.session_state.snapshot_date).days
            report_content += f"- Referenz-Termin (85%): {st.session_state.snapshot_date.strftime('%d.%m.%Y')}\n"
            report_content += f"- Verschiebung: {delta} Tage {'SPÄTER' if delta > 0 else 'FRÜHER'}\n"
        else:
            report_content += "- Keine Referenz/Snapshot vorhanden.\n"

        report_content += f"""
RISIKO-TREIBER (Ø Auswirkung auf Gesamtzeit):
{risk_ranking_str}
---------------------------------------------------------
Simulationseinstellungen: {n_sim} Durchläufe (Monte Carlo)
"""
        st.text_area("Report Vorschau", report_content, height=400)
        st.download_button("📥 Vollständigen Report herunterladen (.txt)", 
                          report_content, 
                          f"Risk_Report_{selected_proj}_{datetime.now().strftime('%Y%m%d')}.txt")