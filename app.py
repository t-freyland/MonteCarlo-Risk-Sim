import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
import traceback
import pickle
import base64
from datetime import datetime, timedelta
import plotly.graph_objects as go

# --- 1. SETUP & CONFIG ---
APP_VERSION = "2.3.0 (Usability Enhanced)"
DB_FILE = "risk_management.db"

st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# ENTFERNE: Den gesamten CSS-Block für das dunkle Design
# ---- custom theme / css ----
# st.markdown(
#     """
#     <style>
#     /* zusätzliche Farben aus Deiner Liste */
#     :root {
#         --accent1: #0C90A8;
#         --accent2: #0C90A8;
#         --accent3: #54909C;
#         --border: #606C6C;
#         --text-secondary: #909090;
#     }
# 
#     /* global */
#     .stApp, .css-1d391kg, .css-1v3fvcr, .css-18e3th9, .css-1avcm0n {
#         background-color: #48606C !important;
#         color: #D8D8D8 !important;
#     }
# 
#     /* sidebar */
#     .css-1d391kg, .css-1v3fvcr {
#         background-color: #183C48 !important;
#     }
# 
#     /* buttons */
#     button.stButton>button, .stDownloadButton>button {
#         background-color: var(--accent1) !important;
#         color: #48606C !important;
#         border: 1px solid var(--border) !important;
#     }
#     button.stButton>button:hover, .stDownloadButton>button:hover {
#         background-color: var(--accent2) !important;
#     }
# 
#     /* inputs */
#     input, .stTextInput>div>div>input,
#     .stNumberInput>div>div>input,
#     .stSelectbox>div>div>div>div {
#         background-color: #183C48 !important;
#         color: #D8D8D8 !important;
#         border: 1px solid var(--border) !important;
#     }
# 
#     /* data editor & dataframe */
#     .stDataFrame, .stDataEditor, .css-1lcbmhc {
#         background-color: #242424 !important;
#         color: #D8D8D8 !important;
#     }
# 
#     /* plots */
#     .js-plotly-plot .plotly .main-svg,
#     .js-plotly-plot .plotly .bg {
#         background-color: #48606C !important;
#     }
# 
#     /* metric / headers */
#     .stMetric, .css-1gkcyyc, h1, h2, h3, h4 {
#         color: #D8D8D8 !important;
#     }
#     </style>
#     """,
#     unsafe_allow_html=True,
# )

# --- 2. SESSION STATE ---
for key in ["snapshot_durations", "snapshot_date", "auth_ok", "last_durations", "last_commit_85", "demo_mode"]:
    if key not in st.session_state:
        st.session_state[key] = None

# --- 3. DATABASE ENGINE ---
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, task_name TEXT, duration REAL, description TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS risks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, risk_name TEXT, risk_type TEXT, target TEXT, prob REAL, impact_min REAL, impact_likely REAL, impact_max REAL, mitigation TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, timestamp TEXT, target_date TEXT, buffer REAL, top_risk TEXT)")
    conn.commit()
    conn.close()

init_db()

# --- 4. HILFSFUNKTIONEN (alle vor Verwendung) ---
def get_all_projects():
    conn = get_db_connection()
    df = pd.read_sql("SELECT DISTINCT project FROM tasks UNION SELECT DISTINCT project FROM risks", conn)
    conn.close()
    projs = df['project'].tolist()
    return projs if projs else ["Demo_Projekt"]

def load_tasks(project):
    conn = get_db_connection()
    df = pd.read_sql("SELECT task_name as 'Task Name', duration as 'Duration (Days)', description as 'Beschreibung' FROM tasks WHERE project=?", conn, params=(project,))
    conn.close()
    if df.empty: 
        df = pd.DataFrame([{"Task Name": "Basis-Task", "Duration (Days)": 10.0, "Beschreibung": ""}])
    return df.reset_index(drop=True)

def load_risks(project):
    conn = get_db_connection()
    df = pd.read_sql("SELECT risk_name as 'Risk Name', risk_type as 'Risk Type', target as 'Target (Global/Task)', prob as 'Probability (0-1)', impact_min as 'Impact Min', impact_likely as 'Impact Likely', impact_max as 'Impact Max', mitigation as 'Maßnahme / Mitigation' FROM risks WHERE project=?", conn, params=(project,))
    conn.close()
    return df.reset_index(drop=True)

def delete_project_complete(project):
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    conn.execute("DELETE FROM history WHERE project=?", (project,))
    conn.commit()
    conn.close()

def delete_history_only(project):
    conn = get_db_connection()
    conn.execute("DELETE FROM history WHERE project=?", (project,))
    conn.commit()
    conn.close()

def save_history(project, target_date, buffer, top_risk):
    conn = get_db_connection()
    conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                 (project, datetime.now().strftime('%Y-%m-%d %H:%M'), target_date, buffer, top_risk))
    conn.commit()
    conn.close()

def load_history(project):
    conn = get_db_connection()
    df = pd.read_sql("SELECT timestamp, target_date, buffer, top_risk FROM history WHERE project=? AND timestamp != 'SNAPSHOT' ORDER BY id ASC", conn, params=(project,))
    conn.close()
    return df

def save_data(project, df_tasks, df_risks):
    """Speichert Tasks und Risks mit Validierung und Column-Normalisierung"""
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    
    df_tasks_clean = df_tasks.copy()
    df_tasks_clean.columns = df_tasks_clean.columns.str.strip()
    
    df_risks_clean = df_risks.copy()
    df_risks_clean.columns = df_risks_clean.columns.str.strip()
    
    for _, row in df_tasks_clean.iterrows():
        task_name = str(row.get("Task Name", "")).strip()
        if task_name:
            try:
                duration = float(row["Duration (Days)"])
                if duration <= 0:
                    st.warning(f"⚠️ Task '{task_name}' hat ungültige Duration ({duration}), übersprungen.")
                    continue
                conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                             (project, task_name, duration, str(row.get("Beschreibung", ""))))
            except (ValueError, TypeError) as e:
                st.warning(f"⚠️ Task '{task_name}' konnte nicht gespeichert werden: {e}")
    
    for _, row in df_risks_clean.iterrows():
        risk_name = str(row.get("Risk Name", "")).strip()
        if risk_name:
            try:
                prob = float(row.get("Probability (0-1)", 0))
                if not (0 <= prob <= 1):
                    st.warning(f"⚠️ Risk '{risk_name}' hat ungültige Wahrscheinlichkeit ({prob}), übersprungen.")
                    continue
                
                conn.execute("INSERT INTO risks (project, risk_name, risk_type, target, prob, impact_min, impact_likely, impact_max, mitigation) VALUES (?,?,?,?,?,?,?,?,?)",
                             (project, risk_name, str(row.get("Risk Type", "Binär")), str(row.get("Target (Global/Task)", "Global")), 
                              prob, float(row.get("Impact Min", 0)), float(row.get("Impact Likely", 0)), 
                              float(row.get("Impact Max", 0)), str(row.get("Maßnahme / Mitigation", ""))))
            except (ValueError, TypeError) as e:
                st.warning(f"⚠️ Risk '{risk_name}' konnte nicht gespeichert werden: {e}")
    
    conn.commit()
    conn.close()

# --- 5. LOGIN (mit Demo-Mode) ---
if not st.session_state.auth_ok:
    st.title("🔐 Login")
    st.divider()
    
    col_login1, col_login2 = st.columns([1.5, 1])
    
    with col_login1:
        st.subheader("Anmeldung")
        u = st.text_input("👤 Username", placeholder="admin")
        p = st.text_input("🔑 Password", type="password", placeholder="••••••")
        
        if st.button("🔓 Anmelden", use_container_width=True):
            if "credentials" in st.secrets and u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
                st.session_state.auth_ok = True
                st.session_state.demo_mode = False
                st.rerun()
            else: 
                st.error("❌ **Login fehlgeschlagen**\n\nUngültige Anmeldedaten.")
    
    with col_login2:
        st.subheader("Demo-Modus")
        st.info("ℹ️ Testen Sie die App ohne Login mit vordefinierten Beispiel-Daten.")
        if st.button("🎮 Demo starten", use_container_width=True):
            st.session_state.auth_ok = True
            st.session_state.demo_mode = True
            st.rerun()
    
    st.stop()

# --- 6. SIDEBAR ---
with st.sidebar:
    st.header("📂 Projekt-Steuerung")
    
    if st.session_state.demo_mode:
        st.warning("🎮 **Demo-Modus aktiv**")
    
    all_projs = get_all_projects()
    selected_proj = st.selectbox("Aktives Projekt:", all_projs)
    
    # ENTFERNE: snapshot_durations, snapshot_date = load_snapshot(selected_proj)
    
    st.divider()
    st.subheader("📈 Tracking")
    do_history = st.checkbox("Messung in Zeitreihe ablegen", value=True)
    
    with st.expander("🛠️ Admin, Export & Import"):
        st.subheader("📋 Projekt-Verwaltung")
        new_p = st.text_input("Neues Projekt / Kopie:")
        if st.button("🚀 Erstellen", use_container_width=True):
            if new_p.strip():
                save_data(new_p, load_tasks(selected_proj), load_risks(selected_proj))
                st.success(f"✅ Projekt '{new_p}' erstellt!")
                st.rerun()
            else:
                st.warning("⚠️ Projekt-Name erforderlich!")
        
        st.divider()
        t_exp = load_tasks(selected_proj)
        r_exp = load_risks(selected_proj)
        export_payload = json.dumps({"project": selected_proj, "tasks": t_exp.to_dict(orient="records"), "risks": r_exp.to_dict(orient="records")}, indent=2)
        st.download_button("📤 Projekt exportieren (JSON)", export_payload, f"{selected_proj}_export.json", use_container_width=True)

        st.divider()
        st.subheader("🗑️ Daten bereinigen")
        if st.button("📊 Historie (Zeitreihe) löschen", use_container_width=True):
            delete_history_only(selected_proj)
            st.success("✅ Historie gelöscht.")
            st.rerun()

        st.divider()
        st.subheader("❗ Projekt löschen")
        st.warning("⚠️ Diese Aktion ist **nicht umkehrbar**!")
        confirm_del = st.checkbox(f"Ich bestätige das Löschen von '{selected_proj}'")
        if st.button(f"🗑️ {selected_proj} löschen", use_container_width=True, type="secondary"):
            if confirm_del:
                delete_project_complete(selected_proj)
                st.success(f"✅ Projekt '{selected_proj}' gelöscht.")
                st.rerun()
            else:
                st.warning("⚠️ Bestätigung erforderlich!")

    st.divider()
    st.subheader("📊 Szenarien-Vergleich")
    
    # ÄNDERE: Nutze session_state statt load_snapshot
    if st.session_state.snapshot_durations is not None and st.session_state.snapshot_date is not None:
        st.success(f"✅ **Snapshot aktiv**\n\n📅 {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")
        if st.button("🗑️ Snapshot löschen", use_container_width=True, key="delete_snapshot"):
            st.session_state.snapshot_durations = None
            st.session_state.snapshot_date = None
            st.success("✅ Snapshot gelöscht!")
    else:
        st.info("ℹ️ Kein Snapshot gespeichert.\n\nNach einer Simulation: Klick auf '📸 Stand einfrieren' im Überblick-Tab.")

    st.divider()
    st.subheader("🏢 Globale Standards")
    STANDARD_DEFS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_DEFS if st.checkbox(sr["name"], value=True)]
    
    st.divider()
    st.subheader("⚙️ Simulationsparameter")
    n_sim = st.number_input("Durchläufe", min_value=1000, max_value=50000, value=10000, step=1000,
                           help="Mehr Durchläufe = genauere Ergebnisse, aber länger Rechenzeit")
    start_date = st.date_input("Projekt-Start", datetime.now(),
                              help="Startdatum für Zieltermin-Berechnung")

# --- 7. MAIN ---
st.title(f"🎲 {selected_proj} | v{APP_VERSION}")

col_info, col_logo = st.columns([4, 1])
with col_info:
    st.info("📌 **Workflow:** 1️⃣ Tasks definieren & speichern → 2️⃣ Risiken konfigurieren → 3️⃣ Simulation starten")

# --- SECTION 1: TASKS ---
st.divider()
st.subheader("📋 1. Projektstruktur (Tasks)")
t_curr = load_tasks(selected_proj)

task_config = {
    "Duration (Days)": st.column_config.NumberColumn(
        "Dauer (Tage)",
        min_value=0.1,
        step=1.0,
        help="Muss > 0 sein"
    )
}

ed_t = st.data_editor(
    t_curr, 
    use_container_width=True, 
    num_rows="dynamic", 
    key=f"t_{selected_proj}",
    column_config=task_config
)

col_t1, col_t2, col_t3 = st.columns([1, 1, 2])
with col_t1:
    if st.button("💾 Tasks speichern", key="save_tasks_btn", use_container_width=True):
        try:
            save_data(selected_proj, ed_t, load_risks(selected_proj))
            st.success("✅ Tasks gespeichert! Jetzt können Risiken konfiguriert werden.")
            st.rerun()
        except Exception as e:
            st.error(f"❌ **Fehler beim Speichern:**\n{str(e)}")

with col_t3:
    st.metric("Gesamt-Dauer", f"{ed_t['Duration (Days)'].sum():.1f} Tage")

# --- SECTION 2: RISKS ---
st.divider()
st.subheader("⚠️ 2. Risiko-Register")

r_curr = load_risks(selected_proj)
t_opts = ["Global"] + t_curr["Task Name"].tolist()

risk_config = {
    "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], required=True),
    "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_opts, required=True),
    "Probability (0-1)": st.column_config.NumberColumn(
        "Wahrscheinlichkeit",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        help="Wert zwischen 0 und 1"
    )
}

ed_r = st.data_editor(
    r_curr, 
    use_container_width=True, 
    num_rows="dynamic", 
    key=f"r_{selected_proj}", 
    column_config=risk_config
)

col_r1, col_r2, col_r3 = st.columns([1, 1, 2])
with col_r1:
    if st.button("💾 Risiken speichern", key="save_risks_btn", use_container_width=True):
        try:
            save_data(selected_proj, ed_t, ed_r)
            st.success("✅ Risiken gespeichert! Bereit für Simulation.")
            st.rerun()
        except Exception as e:
            st.error(f"❌ **Fehler beim Speichern:**\n{str(e)}")

with col_r3:
    st.metric("Anzahl Risiken", len(ed_r))

# --- 8. SIMULATION & ANALYTICS ---
def run_fast_simulation(tasks, risks, std_risks, n):
    """Monte-Carlo Simulation mit korrekter Behandlung von Standard-Risiken."""
    if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
        raise ValueError("Keine gültigen Tasks definiert oder alle Dauern = 0")
    
    task_durations = np.tile(tasks["Duration (Days)"].values, (n, 1)).astype(float)
    base_sum = tasks["Duration (Days)"].sum()
    impact_results = []
    
    for _, r in risks.iterrows():
        p = float(r.get("Probability (0-1)", 0))
        if p <= 0:
            continue
        
        vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
        impacts = np.random.triangular(vals[0], vals[1], max(vals[1] + 0.001, vals[2]), n)
        hits = np.random.random(n) < p
        
        target = str(r.get("Target (Global/Task)", "Global")).strip()
        risk_type = str(r.get("Risk Type", "Binär")).strip()
        
        if target == "Global":
            relevant_duration = base_sum
        else:
            matching_tasks = tasks[tasks["Task Name"] == target]
            if matching_tasks.empty:
                relevant_duration = 0
            else:
                relevant_duration = matching_tasks["Duration (Days)"].sum()
        
        if relevant_duration <= 0:
            continue
        
        avg_delay = (hits * impacts * relevant_duration).mean()
        
        if target == "Global":
            if risk_type == "Kontinuierlich":
                task_durations *= (1 + (hits * impacts)).reshape(-1, 1)
            else:
                task_durations += ((hits * impacts * base_sum / len(tasks)).reshape(-1, 1))
        else:
            matching_indices = tasks.index[tasks["Task Name"] == target].tolist()
            if matching_indices:
                idx = matching_indices[0]
                if risk_type == "Kontinuierlich":
                    task_durations[:, idx] *= (1 + (hits * impacts))
                else:
                    task_durations[:, idx] += (hits * impacts * tasks.iloc[idx]["Duration (Days)"])
        
        impact_results.append({"Quelle": str(r["Risk Name"]), "Verzögerung": avg_delay, "Typ": "Projekt"})
    
    total_days = task_durations.sum(axis=1)
    
    for sr in std_risks:
        hits = np.random.random(n) < sr["prob"]
        impacts = np.random.triangular(sr["min"], sr["likely"], sr["max"], n)
        delay_contribution = (total_days * (hits * impacts)).mean()
        total_days = total_days * (1 + (hits * impacts))
        impact_results.append({"Quelle": f"STD: {sr['name']}", "Verzögerung": delay_contribution, "Typ": "Standard"})
    
    total_days = np.clip(total_days, 0, 10000)
    return total_days.astype(int), pd.DataFrame(impact_results)

st.divider()

if st.button("🚀 Simulation starten & Trend analysieren", use_container_width=True, type="primary"):
    try:
        if ed_t.empty or ed_t["Duration (Days)"].sum() <= 0:
            st.error("❌ **Fehler:** Keine gültigen Tasks definiert. Bitte mindestens eine Task mit Dauer > 0 hinzufügen.")
            st.stop()
        
        with st.spinner("⏳ Berechne Monte-Carlo Trends... (dies kann bei vielen Durchläufen kurz dauern)"):
            durations, impact_df = run_fast_simulation(ed_t, ed_r, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            
            offsets = durations.astype('timedelta64[D]')
            end_dates_np = start_np + offsets
            end_dates = pd.to_datetime(end_dates_np, errors='coerce')
            
            commit_85 = pd.Series(end_dates).quantile(0.85)
            st.session_state.last_durations = durations
            st.session_state.last_impact_df = impact_df
            st.session_state.last_commit_85 = commit_85
            st.session_state.last_end_dates = end_dates
            
            history_df = load_history(selected_proj)
            diff, warning_msg, rec = 0, "✅ Erste Messung", "Projekt befindet sich im Plan."
            
            if not history_df.empty:
                last_entry = history_df.iloc[-1]
                try:
                    last_date = pd.to_datetime(last_entry['target_date'])
                    diff = (commit_85 - last_date).days
                    if diff > 0:
                        warning_msg = f"⚠️ TERMINWARNUNG: Verzögerung um {diff} Tage!"
                        rec = "🚨 EMPFEHLUNG: Stakeholder informieren & Mitigation-Plan aktivieren."
                    elif diff < 0:
                        warning_msg = f"✨ POSITIVER TREND: Verbesserung um {abs(diff)} Tage."
                except Exception as e:
                    st.warning(f"⚠️ Fehler bei Datums-Parsing: {e}")

            top_r = impact_df.sort_values("Verzögerung", ascending=False).iloc[0]["Quelle"] if not impact_df.empty else "N/A"
            st.session_state.last_top_r = top_r
            st.session_state.last_diff = diff
            st.session_state.last_warning_msg = warning_msg
            st.session_state.last_rec = rec
            st.session_state.last_history_df = history_df
            
            if do_history:
                save_history(selected_proj, commit_85.strftime('%Y-%m-%d'), float(np.mean(durations)), top_r)
                history_df = load_history(selected_proj)
                st.session_state.last_history_df = history_df

            if "⚠️" in warning_msg:
                st.error(warning_msg)
            elif "✨" in warning_msg:
                st.success(warning_msg)
            else:
                st.success(warning_msg)
            st.info(f"**Anweisung:** {rec}")
    
    except ValueError as e:
        st.error(f"❌ **Simulationsfehler:**\n{str(e)}")
    except Exception as e:
        st.error(f"❌ **Unerwarteter Fehler:**\n{type(e).__name__}: {str(e)}")
        with st.expander("🔧 Debug-Info"):
            st.code(traceback.format_exc(), language="python")
        st.warning("📞 Bitte kontaktieren Sie den Support mit den obigen Informationen.")

# >>> ENDE des Simulation-Buttons

# ------------------------------------------------------------------------------------------------
# immer dann, wenn wir schon einmal simuliert haben, zeige die Auswertung
# ------------------------------------------------------------------------------------------------
if st.session_state.last_durations is not None:
    # die gespeicherten Werte übernehmen
    durations = st.session_state.last_durations
    impact_df = st.session_state.last_impact_df
    commit_85 = st.session_state.last_commit_85
    end_dates = st.session_state.last_end_dates
    top_r = st.session_state.last_top_r
    diff = st.session_state.last_diff
    warning_msg = st.session_state.last_warning_msg
    rec = st.session_state.last_rec
    history_df = st.session_state.last_history_df
    
    # start_np für Referenzkurve definieren
    start_np = np.datetime64(start_date)

    # TAB-BASIERTE ERGEBNISANZEIGE
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Überblick", "🔥 Risk-Analyse", "📈 Trends", "📜 Report"])
    
    # --- TAB 1: ÜBERBLICK ---
    with tab1:
        st.subheader("Kernkennzahlen & Verteilung")
        
        col_metric1, col_metric2, col_metric3 = st.columns(3)
        with col_metric1:
            st.metric(
                "🎯 Zieltermin (85%)", 
                commit_85.strftime('%d.%m.%Y'), 
                delta=f"{diff} Tage" if not history_df.empty else None, 
                delta_color="inverse",
                help="Das Datum, bis zu dem das Projekt zu 85% Wahrscheinlichkeit abgeschlossen ist"
            )
        with col_metric2:
            st.metric(
                "📦 Pufferbedarf (Ø)", 
                f"{int(np.mean(durations) - ed_t['Duration (Days)'].sum())} Tage",
                help="Durchschnittliche zusätzliche Zeit für Risiken"
            )
        with col_metric3:
            st.metric(
                "📊 Min - Max", 
                f"{int(np.min(durations))} - {int(np.max(durations))} Tage",
                help="Best-Case und Worst-Case Szenario"
            )
        
        st.divider()
        
        col_chart, col_top = st.columns([2, 1])
        
        with col_chart:
            st.subheader("Verteilung der Projektenddaten")
            fig = go.Figure()

            # aktuelle Simulation
            fig.add_trace(go.Histogram(
                x=end_dates, 
                name="Aktuell (neu)", 
                marker_color="#1f77b4",  # ← Zurück zu Standard-Blau
                opacity=0.7,
                nbinsx=40,
                showlegend=True
            ))
            
            # Referenzkurve aus session_state
            if st.session_state.snapshot_durations is not None:
                ref_offsets_td = st.session_state.snapshot_durations.astype('timedelta64[D]')
                ref_ends_np = start_np + ref_offsets_td
                ref_ends = pd.to_datetime(ref_ends_np, errors='coerce')
                
                fig.add_trace(go.Histogram(
                    x=ref_ends, 
                    name="Referenz (alt)", 
                    marker_color="#a0a0a0",  # ← Zurück zu Standard-Grau
                    opacity=0.4,
                    nbinsx=40,
                    showlegend=True
                ))
                
                fig.add_vline(
                    x=st.session_state.snapshot_date.timestamp()*1000, 
                    line_dash="dash", 
                    line_color="#707070",  # ← Zurück zu Standard-Grau
                    line_width=3,
                    annotation_text=f"Ref: {st.session_state.snapshot_date.strftime('%d.%m.%Y')}", 
                    annotation_position="top right"
                )
            
            fig.add_vline(
                x=commit_85.timestamp()*1000, 
                line_dash="solid", 
                line_color="#d62728",
                line_width=3,
                annotation_text=f"Ziel (neu): {commit_85.strftime('%d.%m.%Y')}", 
                annotation_position="top left"
            )
            
            fig.update_layout(
                template="plotly_white",  # ← Zurück zu Standard-Template
                barmode='overlay',
                height=450,
                hovermode='x unified',
                xaxis_title="Enddatum",
                yaxis_title="Häufigkeit (Anzahl Simulationen)",
                legend=dict(
                    x=0.02, 
                    y=0.98, 
                    bgcolor="rgba(255,255,255,0.8)",  # ← Zurück zu weißer Legende
                    bordercolor="black", 
                    borderwidth=1
                )
            )
            st.plotly_chart(fig, use_container_width=True)

            if st.session_state.snapshot_durations is not None:
                st.success(f"✅ **Vergleich aktiv:** Referenz vom {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")

        with col_top:
            st.subheader("🔥 Top Risiko-Treiber")
            
            if st.button("📸 Stand einfrieren", use_container_width=True, 
                         help="Aktuelle Ergebnisse als Referenz speichern", 
                         key="freeze_snapshot"):
                st.session_state.snapshot_durations = durations.copy()
                st.session_state.snapshot_date = commit_85
                st.success("✅ Snapshot gespeichert! Histogram zeigt jetzt beide Kurven.")
            
            st.divider()
            st.markdown(f"### {top_r}")
            if not impact_df.empty:
                top_delay = impact_df.sort_values("Verzögerung", ascending=False).iloc[0]["Verzögerung"]
                st.write(f"**Ø Verzögerung:** {top_delay:.1f} Tage")

    # --- TAB 2: RISIKO-ANALYSE ---
    with tab2:
        st.subheader("Risiko Impact Overview (Tornado Chart)")  # ← Fixed: subheading → subheader
        
        if not impact_df.empty:
            impact_df_sorted = impact_df.sort_values("Verzögerung", ascending=True)
            fig_tornado = go.Figure(go.Bar(
                x=impact_df_sorted["Verzögerung"], 
                y=impact_df_sorted["Quelle"], 
                orientation='h', 
                marker_color=['#EF553B' if t == "Projekt" else '#636EFA' for t in impact_df_sorted["Typ"]],
                text=impact_df_sorted["Verzögerung"].round(1),
                textposition='auto'
            ))
            fig_tornado.update_layout(
                template="plotly_white", 
                xaxis_title="Ø Verzögerung in Tagen", 
                height=max(400, len(impact_df_sorted)*40), 
                margin=dict(l=250),
                hovermode='y'
            )
            st.plotly_chart(fig_tornado, use_container_width=True)
            
            st.divider()
            st.subheader("Detaillierte Risiko-Rankings")  # ← Fixed: subheading → subheader
            impact_display = impact_df.sort_values("Verzögerung", ascending=False).copy()
            impact_display["Verzögerung"] = impact_display["Verzögerung"].round(2)
            st.dataframe(impact_display, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Keine Risiken definiert.")
    
    # --- TAB 3: TREND-ANALYSE ---
    with tab3:
        if not history_df.empty and len(history_df) > 1:
            st.subheader("📉 Fieberkurve (Trend des Zieltermins)")  # ← Fixed: subheading → subheader
            history_df_local = history_df.copy()
            history_df_local['target_date_dt'] = pd.to_datetime(history_df_local['target_date'])
            
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=history_df_local['timestamp'], 
                y=history_df_local['target_date_dt'], 
                mode='lines+markers', 
                line=dict(color='firebrick', width=3),
                fill='tozeroy',
                fillcolor='rgba(255,0,0,0.1)',
                marker=dict(size=8),
                hovertemplate='<b>%{x}</b><br>Zieltermin: %{y|%d.%m.%Y}<extra></extra>'
            ))
            fig_trend.update_layout(
                template="plotly_white", 
                yaxis_title="Prognostizierter Zieltermin", 
                xaxis_title="Messzeitpunkt",
                height=400,
                hovermode='x unified'
            )
            st.plotly_chart(fig_trend, use_container_width=True)
            
            st.divider()
            st.subheader("Mess-Historie")  # ← Fixed: subheading → subheader
            history_display = history_df_local.copy()
            history_display['target_date'] = pd.to_datetime(history_display['target_date']).dt.strftime('%d.%m.%Y')
            history_display['buffer'] = history_display['buffer'].round(1)
            st.dataframe(history_display, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Mindestens 2 Messungen nötig für Trend-Visualisierung. Führen Sie die Simulation mehrfach aus.")
    
    # --- TAB 4: MANAGEMENT REPORT ---
    with tab4:
        st.subheader("📜 Management Report")  # ← Fixed: subheading → subheader
        
        task_list_str = "".join([f"- {row['Task Name']}: {row['Duration (Days)']} Tage\n" for _, row in ed_t.iterrows()])
        risk_list_str = "".join([f"- {row['Risk Name']} ({row['Risk Type']}): {row['Probability (0-1)']} P | Ziel: {row['Target (Global/Task)']}\n" for _, row in ed_r.iterrows()])
        
        impact_ranking_str = ""
        if not impact_df.empty:
            ranked = impact_df.sort_values("Verzögerung", ascending=False)
            impact_ranking_str = "".join([f"- {row['Quelle']}: ~{row['Verzögerung']:.1f} Tage ({row['Typ']})\n" for _, row in ranked.iterrows()])

        hist_report_df = history_df.tail(10).copy()
        try:
            hist_report_df['timestamp'] = pd.to_datetime(hist_report_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
        except:
            pass

        report_content = f"""MANAGEMENT TREND & RISK REPORT - {selected_proj}
Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M')}
App Version: {APP_VERSION}
---------------------------------------------------------
1. STATUS & TREND:
Status: {warning_msg}
Anweisung: {rec}

2. ZIELE & KENNZAHLEN:
- Aktueller Zieltermin (85%-Perzentil): {commit_85.strftime('%d.%m.%Y')}
- Durchschnittliche Projektdauer: {np.mean(durations):.1f} Tage
- Notwendiger Risiko-Puffer: {int(np.mean(durations) - ed_t['Duration (Days)'].sum())} Tage
- Min / Max Szenarien: {int(np.min(durations))} / {int(np.max(durations))} Tage

3. RISIKO-TREIBER (Ø Auswirkung):
{impact_ranking_str if impact_ranking_str else "- Keine Risiken definiert"}

4. PROJEKTSTRUKTUR (TASKS):
{task_list_str if task_list_str else "- Keine Tasks definiert"}

5. RISIKO-REGISTER (DETAILS):
{risk_list_str if risk_list_str else "- Keine Risiken registriert"}

6. HISTORISCHER VERLAUF (Letzte 10 Messungen):
{hist_report_df.to_string(index=False) if not hist_report_df.empty else "- Keine historischen Daten"}

---------------------------------------------------------
Simulationsparameter:
- Anzahl Durchläufe: {n_sim}
- Standard-Risiken aktiviert: {len(selected_std)}/3
- Projekt-Startdatum: {start_date.strftime('%d.%m.%Y')}
"""
        st.text_area("Report Vorschau", report_content, height=400, disabled=True, key="report_preview")
        st.download_button(
            "📥 Vollständigen Report exportieren (.txt)", 
            report_content, 
            f"Report_{selected_proj}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
            use_container_width=True
        )