import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
import traceback
from datetime import datetime
import plotly.graph_objects as go
import matplotlib.pyplot as plt

# --- 1. SETUP & CONFIG ---
APP_VERSION = "2.5.0"  # Updated mit Backtesting-Funktionen
DB_FILE = "risk_management.db"

st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. SESSION STATE ---
for key in [
    "snapshot_durations", "snapshot_date", "auth_ok",
    "last_durations", "last_impact_df", "last_commit_85",
    "last_end_dates", "last_top_r", "last_diff",
    "last_warning_msg", "last_rec", "last_history_df",
    "demo_mode", "std_risk_df",
]:
    if key not in st.session_state:
        st.session_state[key] = None

# --- 3. DATABASE ---
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, task_name TEXT, duration REAL, description TEXT, team TEXT, sequence INTEGER)")
    conn.execute("CREATE TABLE IF NOT EXISTS risks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, risk_name TEXT, risk_type TEXT, target TEXT, prob REAL, impact_min REAL, impact_likely REAL, impact_max REAL, mitigation TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, timestamp TEXT, target_date TEXT, buffer REAL, top_risk TEXT)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS actual_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT,
            start_date TEXT,
            planned_end_date TEXT,
            actual_end_date TEXT,
            planned_duration REAL,
            actual_duration REAL,
            buffer_used REAL,
            top_risk TEXT,
            risks_occurred TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project TEXT,
            team_name TEXT,
            capacity REAL DEFAULT 1.0
        )
    """)
    conn.commit()
    conn.close()

init_db()

# --- 4. HILFSFUNKTIONEN ---

# ===== NORMALE HILFSFUNKTIONEN =====
def get_all_projects():
    conn = get_db_connection()
    df = pd.read_sql("SELECT DISTINCT project FROM tasks UNION SELECT DISTINCT project FROM risks", conn)
    conn.close()
    projs = df['project'].tolist()
    return projs if projs else ["Demo_Projekt"]

def load_tasks(project):
    """Lade alle Tasks für ein Projekt."""
    conn = get_db_connection()
    df = pd.read_sql("SELECT task_name as 'Task Name', duration as 'Duration (Days)', description as 'Beschreibung' FROM tasks WHERE project=?", conn, params=(project,))
    conn.close()
    if df.empty:
        df = pd.DataFrame([{"Task Name": "Basis-Task", "Duration (Days)": 10.0, "Beschreibung": ""}])
    return df.reset_index(drop=True)

def load_risks(project):
    """Lade alle Risiken für ein Projekt."""
    conn = get_db_connection()
    df = pd.read_sql("SELECT risk_name as 'Risk Name', risk_type as 'Risk Type', target as 'Target (Global/Task)', prob as 'Probability (0-1)', impact_min as 'Impact Min', impact_likely as 'Impact Likely', impact_max as 'Impact Max', mitigation as 'Maßnahme / Mitigation' FROM risks WHERE project=?", conn, params=(project,))
    conn.close()
    return df.reset_index(drop=True)

def save_data(project, df_tasks, df_risks):
    """Speichere Tasks und Risiken für ein Projekt."""
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    for _, row in df_tasks.iterrows():
        task_name = str(row.get("Task Name", "")).strip()
        if task_name:
            try:
                duration = float(row["Duration (Days)"])
                if duration > 0:
                    conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                                 (project, task_name, duration, str(row.get("Beschreibung", ""))))
            except (ValueError, TypeError):
                pass
    for _, row in df_risks.iterrows():
        risk_name = str(row.get("Risk Name", "")).strip()
        if risk_name:
            try:
                prob = float(row.get("Probability (0-1)", 0))
                if 0 <= prob <= 1:
                    conn.execute("INSERT INTO risks (project, risk_name, risk_type, target, prob, impact_min, impact_likely, impact_max, mitigation) VALUES (?,?,?,?,?,?,?,?,?)",
                                 (project, risk_name, str(row.get("Risk Type", "Binär")),
                                  str(row.get("Target (Global/Task)", "Global")),
                                  prob, float(row.get("Impact Min", 0)),
                                  float(row.get("Impact Likely", 0)),
                                  float(row.get("Impact Max", 0)),
                                  str(row.get("Maßnahme / Mitigation", ""))))
            except (ValueError, TypeError):
                pass
    conn.commit()
    conn.close()

def save_history(project, target_date, buffer, top_risk):
    """Speichere die Mess-Historie."""
    conn = get_db_connection()
    conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                 (project, datetime.now().strftime('%Y-%m-%d %H:%M'), target_date, buffer, top_risk))
    conn.commit()
    conn.close()

def delete_history_only(project):
    """Lösche die Mess-Historie für ein Projekt."""
    conn = get_db_connection()
    conn.execute("DELETE FROM history WHERE project=?", (project,))
    conn.commit()
    conn.close()

def load_history(project):
    """Lade die Mess-Historie für ein Projekt."""
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT timestamp, target_date, buffer, top_risk FROM history WHERE project=? ORDER BY timestamp ASC",
        conn,
        params=(project,)
    )
    conn.close()
    return df

def delete_project_complete(project):
    """Lösche ein Projekt komplett mit allen zugehörigen Daten."""
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    conn.execute("DELETE FROM history WHERE project=?", (project,))
    conn.execute("DELETE FROM actual_results WHERE project=?", (project,))
    conn.commit()
    conn.close()

def save_actual_result(project, start_date, planned_end, actual_end, top_risk, risks_occurred, notes=""):
    """Speichere tatsächliches Projektergebnis für Backtesting."""
    conn = get_db_connection()
    planned_days = (pd.to_datetime(planned_end) - pd.to_datetime(start_date)).days
    actual_days = (pd.to_datetime(actual_end) - pd.to_datetime(start_date)).days
    buffer_used = actual_days - planned_days
    
    conn.execute("""
        INSERT INTO actual_results 
        (project, start_date, planned_end_date, actual_end_date, 
         planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (project, start_date, planned_end, actual_end, planned_days, actual_days, 
          buffer_used, top_risk, json.dumps(risks_occurred), notes))
    conn.commit()
    conn.close()

def load_actual_results(project):
    """Lade tatsächliche Ergebnisse für Validierung."""
    conn = get_db_connection()
    df = pd.read_sql("""
        SELECT start_date, planned_end_date, actual_end_date, 
               planned_duration, actual_duration, buffer_used, 
               top_risk, risks_occurred, notes
        FROM actual_results 
        WHERE project = ? 
        ORDER BY actual_end_date ASC
    """, conn, params=(project,))
    conn.close()
    return df

def get_default_standard_risks_df():
    return pd.DataFrame([
        {"Aktiv": True,  "name": "Schätz-Ungenauigkeit",        "type": "Kontinuierlich", "prob": 1.00, "min": -0.03, "likely": 0.00, "max": 0.08},
        {"Aktiv": True,  "name": "Scope Creep",                  "type": "Binär",          "prob": 0.40, "min":  0.05, "likely": 0.15, "max": 0.30},
        {"Aktiv": True,  "name": "Technische Schulden",           "type": "Kontinuierlich", "prob": 1.00, "min":  0.03, "likely": 0.07, "max": 0.15},
        {"Aktiv": True,  "name": "Personalfluktuation",           "type": "Binär",          "prob": 0.20, "min":  0.05, "likely": 0.12, "max": 0.25},
        {"Aktiv": True,  "name": "Krankheit / Ausfall",           "type": "Binär",          "prob": 0.25, "min":  0.03, "likely": 0.08, "max": 0.15},
        {"Aktiv": True,  "name": "Integrationsprobleme",          "type": "Binär",          "prob": 0.35, "min":  0.04, "likely": 0.10, "max": 0.22},
        {"Aktiv": True,  "name": "Test- und QA-Nacharbeit",       "type": "Kontinuierlich", "prob": 1.00, "min":  0.02, "likely": 0.06, "max": 0.12},
        {"Aktiv": True,  "name": "Abhängigkeiten extern",         "type": "Binär",          "prob": 0.30, "min":  0.05, "likely": 0.12, "max": 0.25},
        {"Aktiv": True,  "name": "Deployment-/Release-Risiko",    "type": "Binär",          "prob": 0.22, "min":  0.03, "likely": 0.07, "max": 0.16},
        {"Aktiv": True,  "name": "Anforderungsunklarheit",        "type": "Kontinuierlich", "prob": 1.00, "min":  0.02, "likely": 0.05, "max": 0.11},
        {"Aktiv": False, "name": "Umgebungs-/Tooling-Probleme",   "type": "Binär",          "prob": 0.18, "min":  0.02, "likely": 0.06, "max": 0.12},
        {"Aktiv": False, "name": "Security-/Compliance-Auflagen", "type": "Binär",          "prob": 0.15, "min":  0.03, "likely": 0.09, "max": 0.20},
    ])

def load_teams(project):
    """Lade alle Teams für ein Projekt."""
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT team_name as 'Team', capacity as 'Capacity' FROM teams WHERE project=? ORDER BY team_name",
        conn, params=(project,)
    )
    conn.close()
    if df.empty:
        df = pd.DataFrame([{"Team": "Standard", "Capacity": 1.0}])
    return df.reset_index(drop=True)

def save_teams(project, df_teams):
    """Speichere Teams für ein Projekt."""
    conn = get_db_connection()
    conn.execute("DELETE FROM teams WHERE project=?", (project,))
    for _, row in df_teams.iterrows():
        team_name = str(row.get("Team", "")).strip()
        if team_name:
            try:
                capacity = float(row.get("Capacity", 1.0))
                if capacity > 0:
                    conn.execute("INSERT INTO teams (project, team_name, capacity) VALUES (?,?,?)",
                                 (project, team_name, capacity))
            except (ValueError, TypeError):
                pass
    conn.commit()
    conn.close()

def calculate_critical_path(tasks_df):
    """Berechne den kritischen Pfad (längste Sequenz pro Team)."""
    if tasks_df.empty or "Duration (Days)" not in tasks_df.columns:
        return 0.0
    
    if "team" not in tasks_df.columns:
        return float(tasks_df["Duration (Days)"].sum())
    
    # Gruppiere nach Team und summiere Dauern
    team_durations = tasks_df.groupby("team")["Duration (Days)"].sum()
    return float(team_durations.max()) if not team_durations.empty else 0.0

# ===== WERKTAGE-FUNKTIONEN =====
def add_business_days(start_date, num_days):
    """Addiere Werktage (Mo-Fr) zu einem Datum."""
    current = pd.to_datetime(start_date)
    days_added = 0
    
    while days_added < num_days:
        current += pd.Timedelta(days=1)
        # 0=Mo, 1=Di, ..., 4=Fr, 5=Sa, 6=So
        if current.weekday() < 5:  # Montag bis Freitag
            days_added += 1
    
    return current

# Deutsche Feiertage 2024-2026 (hardcoded)
GERMAN_HOLIDAYS = [
    "2024-01-01", "2024-03-29", "2024-03-30", "2024-04-01", "2024-05-01",
    "2024-05-09", "2024-05-19", "2024-05-30", "2024-10-03", "2024-12-25", "2024-12-26",
    "2025-01-01", "2025-04-18", "2025-04-19", "2025-04-21", "2025-05-01",
    "2025-05-08", "2025-05-29", "2025-06-09", "2025-10-03", "2025-12-25", "2025-12-26",
    "2026-01-01", "2026-04-10", "2026-04-11", "2026-04-13", "2026-05-01",
    "2026-05-21", "2026-05-31", "2026-06-11", "2026-10-03", "2026-12-25", "2026-12-26",
]

def add_business_days_with_holidays(start_date, num_days):
    """Addiere Werktage (Mo-Fr, ohne deutsche Feiertage)."""
    current = pd.to_datetime(start_date)
    days_added = 0
    holiday_set = set(pd.to_datetime(GERMAN_HOLIDAYS).strftime('%Y-%m-%d'))
    
    while days_added < num_days:
        current += pd.Timedelta(days=1)
        date_str = current.strftime('%Y-%m-%d')
        # Werktag UND kein Feiertag
        if current.weekday() < 5 and date_str not in holiday_set:
            days_added += 1
    
    return current

def convert_calendar_to_business_days(calendar_days):
    """Konvertiere Kalendertage zu Werktagen (grobe Schätzung)."""
    # Annahme: 5 Arbeitstage pro 7 Kalendertage
    return calendar_days * (5.0 / 7.0)

# --- 5. SIMULATION ---
def run_fast_simulation(tasks, risks, std_risks, teams, n):
    """
    Monte-Carlo Simulation mit Team-Parallelisierung.
    - Tasks innerhalb eines Teams: SEQUENZIELL
    - Verschiedene Teams: PARALLEL
    """
    if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
        raise ValueError("Keine gültigen Tasks definiert oder alle Dauern = 0")

    # Fülle fehlende Team-Spalte
    tasks_copy = tasks.copy()
    if "team" not in tasks_copy.columns:
        tasks_copy["team"] = "Sequenziell"
    
    # Berechne Basis-Dauer: kritischer Pfad (längste Team-Sequenz)
    team_durations = {}
    for team_name in tasks_copy["team"].unique():
        team_tasks = tasks_copy[tasks_copy["team"] == team_name]
        team_sum = team_tasks["Duration (Days)"].sum()
        
        # Wende Team-Capacity an
        if not teams.empty:
            team_row = teams[teams["Team"] == team_name]
            if not team_row.empty:
                capacity = float(team_row.iloc[0].get("Capacity", 1.0))
                team_sum = team_sum / capacity  # Höhere Capacity = schneller
        
        team_durations[team_name] = team_sum
    
    # Kritischer Pfad = längste Dauer aller Teams
    base_sum = max(team_durations.values()) if team_durations else tasks_copy["Duration (Days)"].sum()
    
    total_days = np.full(n, base_sum, dtype=float)
    impact_results = []

    # --- PROZESS-RISIKEN (wirken auf Tasks) ---
    for _, r in risks.iterrows():
        p = float(r.get("Probability (0-1)", 0))
        rtype = str(r.get("Risk Type", "Binär")).strip().lower()
        target = str(r.get("Target (Global/Task)", "Global")).strip()
        mins = float(r.get("Impact Min", 0))
        lik = float(r.get("Impact Likely", mins))
        maxv = float(r.get("Impact Max", lik))
        
        impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
        hits = np.ones(n, dtype=bool) if rtype == "kontinuierlich" else (np.random.random(n) < p)

        # Bestimme betroffene Dauer
        if target.lower() == "global":
            # Global = wirkt auf kritischen Pfad
            relevant_dur = base_sum
        else:
            # Task-spezifisch = wirkt nur auf diese Task
            match = tasks_copy[tasks_copy["Task Name"] == target]
            if not match.empty:
                relevant_dur = float(match["Duration (Days)"].iloc[0])
                # Wenn Task in schnellerem Team: Risiko-Dauer auch proportional schneller
                task_team = match.iloc[0].get("team", "Sequenziell")
                if not teams.empty:
                    team_row = teams[teams["Team"] == task_team]
                    if not team_row.empty:
                        capacity = float(team_row.iloc[0].get("Capacity", 1.0))
                        relevant_dur = relevant_dur / capacity
            else:
                relevant_dur = 0.0

        if relevant_dur <= 0:
            continue

        # Addiere Risiko-Verzögerung zum kritischen Pfad
        delay = impacts * relevant_dur
        total_days += delay if rtype == "kontinuierlich" else (hits * delay)
        
        avg_delay = float(delay.mean()) if rtype == "kontinuierlich" else float((hits * delay).mean())
        impact_results.append({
            "Quelle": str(r["Risk Name"]), 
            "Verzögerung": avg_delay, 
            "Typ": "Projekt",
            "Team": task_team if target.lower() != "global" else "Global"
        })

    # --- STANDARD-RISIKEN ---
    for sr in std_risks:
        rtype = str(sr.get("type", "Binär")).strip().lower()
        p = float(sr.get("prob", 0))
        mins = float(sr.get("min", 0))
        lik = float(sr.get("likely", mins))
        maxv = float(sr.get("max", lik))
        
        impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
        hits = np.ones(n, dtype=bool) if rtype == "kontinuierlich" else (np.random.random(n) < p)
        delay = impacts * base_sum
        total_days += delay if rtype == "kontinuierlich" else (hits * delay)
        
        avg_delay = float(delay.mean()) if rtype == "kontinuierlich" else float((hits * delay).mean())
        impact_results.append({
            "Quelle": f"STD: {sr['name']}", 
            "Verzögerung": avg_delay, 
            "Typ": "Standard",
            "Team": "Global"
        })

    total_days = np.clip(total_days, 0, 10000)
    return total_days.astype(int), pd.DataFrame(impact_results)

# --- 6. LOGIN ---
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
                st.error("❌ Ungültige Anmeldedaten.")
    with col_login2:
        st.subheader("Demo-Modus")
        st.info("ℹ️ Testen Sie die App ohne Login.")
        if st.button("🎮 Demo starten", use_container_width=True):
            st.session_state.auth_ok = True
            st.session_state.demo_mode = True
            st.rerun()
    st.stop()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header("📂 Projekt-Steuerung")
    if st.session_state.demo_mode:
        st.warning("🎮 **Demo-Modus aktiv**")

    all_projs = get_all_projects()
    selected_proj = st.selectbox("Aktives Projekt:", all_projs)

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
        if st.button("📊 Historie löschen", use_container_width=True):
            delete_history_only(selected_proj)
            st.success("✅ Historie gelöscht.")
            st.rerun()

        st.divider()
        st.subheader("❗ Projekt löschen")
        st.warning("⚠️ Nicht umkehrbar!")
        confirm_del = st.checkbox(f"Löschen von '{selected_proj}' bestätigen")
        if st.button(f"🗑️ {selected_proj} löschen", use_container_width=True, type="secondary"):
            if confirm_del:
                delete_project_complete(selected_proj)
                st.success(f"✅ Projekt '{selected_proj}' gelöscht.")
                st.rerun()
            else:
                st.warning("⚠️ Bestätigung erforderlich!")

    st.divider()
    st.subheader("📊 Szenarien-Vergleich")
    if st.session_state.snapshot_durations is not None and st.session_state.snapshot_date is not None:
        st.success(f"✅ **Snapshot aktiv**\n\n📅 {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")
        if st.button("🗑️ Snapshot löschen", use_container_width=True, key="delete_snapshot"):
            st.session_state.snapshot_durations = None
            st.session_state.snapshot_date = None
            st.rerun()
    else:
        st.info("ℹ️ Kein Snapshot gespeichert.\n\nNach Simulation: '📸 Stand einfrieren' klicken.")

    st.divider()
    st.subheader("🏢 Globale Standards")

    if st.session_state.std_risk_df is None:
        st.session_state.std_risk_df = get_default_standard_risks_df()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Alle an", use_container_width=True):
            df_tmp = st.session_state.std_risk_df.copy()
            df_tmp["Aktiv"] = True
            st.session_state.std_risk_df = df_tmp
            st.rerun()
    with c2:
        if st.button("Alle aus", use_container_width=True):
            df_tmp = st.session_state.std_risk_df.copy()
            df_tmp["Aktiv"] = False
            st.session_state.std_risk_df = df_tmp
            st.rerun()

    with st.expander("⚙️ Standardrisiken konfigurieren", expanded=False):
        edited_std = st.data_editor(
            st.session_state.std_risk_df,
            use_container_width=True,
            num_rows="dynamic",
            key="std_risk_editor",
            column_config={
                "Aktiv":   st.column_config.CheckboxColumn("Aktiv"),
                "name":    st.column_config.TextColumn("Risiko"),
                "type":    st.column_config.SelectboxColumn("Typ", options=["Binär", "Kontinuierlich"], required=True),
                "prob":    st.column_config.NumberColumn("Wahrsch.", min_value=0.0, max_value=1.0, step=0.01),
                "min":     st.column_config.NumberColumn("Impact Min",    step=0.01),
                "likely":  st.column_config.NumberColumn("Impact Likely", step=0.01),
                "max":     st.column_config.NumberColumn("Impact Max",    step=0.01),
            },
        )
        if st.button("💾 Übernehmen", use_container_width=True):
            df_tmp = edited_std.copy()
            df_tmp["prob"] = df_tmp["prob"].clip(0, 1)
            vals = np.sort(df_tmp[["min", "likely", "max"]].values, axis=1)
            df_tmp["min"], df_tmp["likely"], df_tmp["max"] = vals[:, 0], vals[:, 1], vals[:, 2]
            st.session_state.std_risk_df = df_tmp
            st.success("✅ Standardrisiken aktualisiert.")

    active_std_df = st.session_state.std_risk_df[st.session_state.std_risk_df["Aktiv"] == True].copy()
    selected_std = active_std_df[["name", "type", "prob", "min", "likely", "max"]].to_dict(orient="records")
    st.caption(f"Aktiv: {len(selected_std)} von {len(st.session_state.std_risk_df)}")

    st.divider()
    st.subheader("⚙️ Simulationsparameter")
    n_sim = st.number_input("Durchläufe", min_value=1000, max_value=50000, value=10000, step=1000)
    start_date = st.date_input("Projekt-Start", datetime.now())

    # NEUE OPTION:
    use_business_days = st.checkbox("📅 Werktage verwenden (Mo-Fr)", value=False, 
                                 help="True = nur Montag-Freitag zählen\nFalse = alle Kalendertage")

# --- 8. MAIN ---
st.title(f"🎲 {selected_proj} | v{APP_VERSION}")
st.info("📌 **Workflow:** 1️⃣ Tasks definieren & speichern → 2️⃣ Risiken konfigurieren → 3️⃣ Simulation starten")

# --- TEAMS (Vereinfacht) ---
st.divider()
st.subheader("👥 Team-Verwaltung")

col_tm1, col_tm2 = st.columns([2, 1])
with col_tm1:
    st.info("💡 **Wie Teams funktionieren:**\n"
            "- Tasks werden Teams zugewiesen\n"
            "- Teams arbeiten **parallel** zueinander\n"
            "- Capacity: 1.0 = normal, 2.0 = doppelte Geschwindigkeit\n"
            "- Projekt endet wenn **längster Team** fertig ist (nicht Summe!)")

tm_curr = load_teams(selected_proj)
with col_tm2:
    st.metric("📊 Teams aktiv", len(tm_curr))

# Teams mit besserer Visualisierung
st.subheader("Team-Struktur")
col_a, col_b, col_c = st.columns(3)

with col_a:
    st.write("**Team Name**")
    team_names = st.data_editor(
        tm_curr[["Team"]].reset_index(drop=True),
        use_container_width=True,
        num_rows="dynamic",
        key=f"team_names_{selected_proj}",
        hide_index=True
    )

with col_b:
    st.write("**Kapazität**")
    team_caps = st.data_editor(
        tm_curr[["Capacity"]].reset_index(drop=True),
        use_container_width=True,
        num_rows="dynamic",
        key=f"team_caps_{selected_proj}",
        column_config={"Capacity": st.column_config.NumberColumn(
            "Kapazität", min_value=0.1, max_value=5.0, step=0.1
        )},
        hide_index=True
    )

with col_c:
    st.write("**Beispiele**")
    st.markdown("""
    - 0.5 = **Doppelt so langsam**
    - 1.0 = **Normal**
    - 2.0 = **Doppelt so schnell**
    - 3.0 = **3x schneller**
    """)

# Merge & Save
if not team_names.empty and not team_caps.empty:
    ed_tm = pd.concat([
        team_names.reset_index(drop=True),
        team_caps.reset_index(drop=True)
    ], axis=1)
    
    if st.button("💾 Teams speichern", use_container_width=True, key="save_teams"):
        save_teams(selected_proj, ed_tm)
        st.success("✅ Teams aktualisiert!")
        st.rerun()
else:
    # Fallback wenn leer
    ed_tm = tm_curr.copy()

st.caption("💡 Capacity > 1.0 = schneller (z.B. 2.0 = doppeltes Tempo). Wird nicht verwendet, wenn leer.")

# --- TASKS ---
st.divider()
st.subheader("📋 1. Projektstruktur (Tasks)")
st.info("💡 **Parallel-Arbeit:** Ordne Tasks Teams zu. Tasks desselben Teams laufen sequenziell, Teams arbeiten parallel.")

t_curr = load_tasks(selected_proj)
if "team" not in t_curr.columns:
    t_curr.insert(len(t_curr.columns), "team", "Sequenziell")

# Visuelle Vorschau
st.write("**Aufgaben-Zuordnung**")
tab_edit, tab_preview = st.tabs(["Bearbeiten", "Vorschau"])

with tab_edit:
    team_options = ["Sequenziell"] + ed_tm["Team"].tolist() if not ed_tm.empty else ["Sequenziell"]
    
    ed_t = st.data_editor(
        t_curr,
        use_container_width=True,
        num_rows="dynamic",
        key=f"t_{selected_proj}",
        column_config={
            "Duration (Days)": st.column_config.NumberColumn(
                "Dauer (Tage)", min_value=0.1, step=1.0
            ),
            "team": st.column_config.SelectboxColumn(
                "Team zugewiesen", 
                options=team_options,
                help="Wähle Team für Parallelverarbeitung"
            )
        }
    )

with tab_preview:
    # Zeige kritischen Pfad Berechnung
    st.write("**Kritischer Pfad Berechnung**")
    
    if not ed_t.empty and "team" in ed_t.columns:
        team_breakdown = ed_t.groupby("team")["Duration (Days)"].sum().sort_values(ascending=False)
        
        for team, duration in team_breakdown.items():
            # Capacity anwenden
            capacity = 1.0
            if not ed_tm.empty:
                team_row = ed_tm[ed_tm["Team"] == team]
                if not team_row.empty:
                    capacity = float(team_row.iloc[0]["Capacity"])
            
            effective = duration / capacity
            
            col1, col2, col3, col4 = st.columns(4)
            col1.write(f"**{team}**")
            col2.metric("Summe", f"{duration:.0f}d")
            col3.metric("Kapazität", f"{capacity}x")
            col4.metric("Effektiv", f"{effective:.1f}d", delta="CP" if effective == team_breakdown.max() / capacity else None)
        
        critical_path_calc = max([
            (float(ed_t[ed_t["team"] == t]["Duration (Days)"].sum()) / 
             (float(ed_tm[ed_tm["Team"] == t]["Capacity"].iloc[0]) if not ed_tm[ed_tm["Team"] == t].empty else 1.0))
            for t in ed_t["team"].unique()
        ])
        
        st.success(f"✅ **Kritischer Pfad = {critical_path_calc:.1f} Tage** (nicht {ed_t['Duration (Days)'].sum():.0f}!)")

col_t1, col_t2 = st.columns([1, 1])
with col_t1:
    if st.button("💾 Tasks speichern", use_container_width=True):
        if "team" not in ed_t.columns:
            ed_t["team"] = "Sequenziell"
        ed_t["team"] = ed_t["team"].fillna("Sequenziell")
        save_data(selected_proj, ed_t, load_risks(selected_proj))
        st.success("✅ Tasks gespeichert!")
        st.rerun()

with col_t2:
    st.info(f"📊 {len(ed_t)} Tasks | CP: {critical_path_calc:.0f}d")

# --- RISKS ---
st.divider()
st.subheader("⚠️ 2. Risiko-Register")
r_curr = load_risks(selected_proj)
t_opts = ["Global"] + t_curr["Task Name"].tolist()
ed_r = st.data_editor(
    r_curr, use_container_width=True, num_rows="dynamic", key=f"r_{selected_proj}",
    column_config={
        "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], required=True),
        "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_opts, required=True),
        "Probability (0-1)": st.column_config.NumberColumn("Wahrscheinlichkeit", min_value=0.0, max_value=1.0, step=0.01),
    }
)
col_r1, _, col_r3 = st.columns([1, 1, 2])
with col_r1:
    if st.button("💾 Risiken speichern", use_container_width=True):
        save_data(selected_proj, ed_t, ed_r)
        st.success("✅ Risiken gespeichert!")
        st.rerun()
with col_r3:
    st.metric("Anzahl Risiken", len(ed_r))

# --- SIMULATION ---
st.divider()
if st.button("🚀 Simulation starten & Trend analysieren", use_container_width=True, type="primary"):
    try:
        if ed_t.empty or ed_t["Duration (Days)"].sum() <= 0:
            st.error("❌ Keine gültigen Tasks definiert.")
            st.stop()

        with st.spinner("⏳ Berechne Monte-Carlo Simulation..."):
            # WICHTIG: ed_tm (Teams) mitübergeben!
            durations, impact_df = run_fast_simulation(ed_t, ed_r, selected_std, ed_tm, n_sim)
            
            # --- WERKTAGE ODER KALENDERTAGE ---
            if use_business_days:
                # Mit Feiertagen
                end_dates = pd.Series([
                    add_business_days_with_holidays(start_date, int(d)) for d in durations
                ])
            else:
                # Standard: Kalendertage
                start_np = np.datetime64(start_date)
                end_dates = pd.to_datetime(start_np + durations.astype('timedelta64[D]'), errors='coerce')
            
            commit_85 = pd.Series(end_dates).quantile(0.85)

            st.session_state.last_durations   = durations
            st.session_state.last_impact_df   = impact_df
            st.session_state.last_commit_85   = commit_85
            st.session_state.last_end_dates   = end_dates

            history_df = load_history(selected_proj)
            diff, warning_msg, rec = 0, "✅ Erste Messung", "Projekt befindet sich im Plan."

            if not history_df.empty:
                try:
                    last_date = pd.to_datetime(history_df.iloc[-1]['target_date'])
                    diff = (commit_85 - last_date).days
                    if diff > 0:
                        warning_msg = f"⚠️ TERMINWARNUNG: Verzögerung um {diff} Tage!"
                        rec = "🚨 Stakeholder informieren & Mitigation-Plan aktivieren."
                    elif diff < 0:
                        warning_msg = f"✨ POSITIVER TREND: Verbesserung um {abs(diff)} Tage."
                except Exception:
                    pass

            top_r = impact_df.sort_values("Verzögerung", ascending=False).iloc[0]["Quelle"] if not impact_df.empty else "N/A"
            st.session_state.last_top_r       = top_r
            st.session_state.last_diff        = diff
            st.session_state.last_warning_msg = warning_msg
            st.session_state.last_rec         = rec
            st.session_state.last_history_df  = history_df

            if do_history:
                save_history(selected_proj, commit_85.strftime('%Y-%m-%d'), float(np.mean(durations)), top_r)
                st.session_state.last_history_df = load_history(selected_proj)

            if "⚠️" in warning_msg:
                st.error(warning_msg)
            elif "✨" in warning_msg:
                st.success(warning_msg)
            else:
                st.success(warning_msg)
            st.info(f"**Anweisung:** {rec}")

    except ValueError as e:
        st.error(f"❌ {str(e)}")
    except Exception as e:
        st.error(f"❌ {type(e).__name__}: {str(e)}")
        with st.expander("🔧 Debug"):
            st.code(traceback.format_exc())

# --- ERGEBNISANZEIGE ---
if st.session_state.last_durations is not None:
    durations   = st.session_state.last_durations
    impact_df   = st.session_state.last_impact_df
    commit_85   = st.session_state.last_commit_85
    end_dates   = st.session_state.last_end_dates
    top_r       = st.session_state.last_top_r
    diff        = st.session_state.last_diff
    warning_msg = st.session_state.last_warning_msg
    rec         = st.session_state.last_rec
    history_df  = st.session_state.last_history_df
    start_np    = np.datetime64(start_date)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Überblick", "🔥 Risk-Analyse", "📈 Trends", "📜 Report", "📊 Validierung"])

    with tab1:
        st.subheader("Kernkennzahlen & Verteilung")
        col_metric1, col_metric2, col_metric3 = st.columns(3)
        with col_metric1:
            st.metric("🎯 Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'),
                      delta=f"{diff} Tage" if not history_df.empty else None, delta_color="inverse")
        with col_metric2:
            st.metric("📦 Pufferbedarf (Ø)", f"{int(np.mean(durations) - ed_t['Duration (Days)'].sum())} Tage")
        with col_metric3:
            st.metric("📊 Min - Max", f"{int(np.min(durations))} - {int(np.max(durations))} Tage")

        st.divider()
        col_chart, col_top = st.columns([2, 1])

        with col_chart:
            st.subheader("Verteilung der Projektenddaten")
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Aktuell", marker_color="#1f77b4", opacity=0.7, nbinsx=40))

            if st.session_state.snapshot_durations is not None:
                ref_ends = pd.to_datetime(start_np + st.session_state.snapshot_durations.astype('timedelta64[D]'), errors='coerce')
                fig.add_trace(go.Histogram(x=ref_ends, name="Referenz (alt)", marker_color="#a0a0a0", opacity=0.5, nbinsx=40))
                fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, line_dash="dash", line_color="#707070",
                              annotation_text=f"Ref: {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")

            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="solid", line_color="#d62728",
                          annotation_text=f"Ziel: {commit_85.strftime('%d.%m.%Y')}")
            fig.update_layout(template="plotly_white", barmode='overlay', height=450,
                              xaxis_title="Enddatum", yaxis_title="Häufigkeit",
                              legend=dict(bgcolor="rgba(255,255,255,0.8)", bordercolor="black", borderwidth=1))
            st.plotly_chart(fig, use_container_width=True)

        with col_top:
            st.subheader("🔥 Top Risiko-Treiber")
            if st.button("📸 Stand einfrieren", use_container_width=True, key="freeze_snapshot"):
                st.session_state.snapshot_durations = durations.copy()
                st.session_state.snapshot_date = commit_85
                st.success("✅ Snapshot gespeichert!")
            st.divider()
            st.markdown(f"### {top_r}")
            if not impact_df.empty:
                top_delay = impact_df.sort_values("Verzögerung", ascending=False).iloc[0]["Verzögerung"]
                st.write(f"**Ø Verzögerung:** {top_delay:.1f} Tage")

    with tab2:
        st.subheader("Risiko Impact Overview (Tornado Chart)")
        if not impact_df.empty:
            impact_df_sorted = impact_df.sort_values("Verzögerung", ascending=True)
            fig_tornado = go.Figure(go.Bar(
                x=impact_df_sorted["Verzögerung"], y=impact_df_sorted["Quelle"], orientation='h',
                marker_color=['#EF553B' if t == "Projekt" else '#636EFA' for t in impact_df_sorted["Typ"]],
                text=impact_df_sorted["Verzögerung"].round(1), textposition='auto'
            ))
            fig_tornado.update_layout(template="plotly_white", xaxis_title="Ø Verzögerung in Tagen",
                                      height=max(400, len(impact_df_sorted)*40), margin=dict(l=250))
            st.plotly_chart(fig_tornado, use_container_width=True)
            st.divider()
            st.subheader("Detaillierte Risiko-Rankings")
            impact_display = impact_df.sort_values("Verzögerung", ascending=False).copy()
            impact_display["Verzögerung"] = impact_display["Verzögerung"].round(2)
            st.dataframe(impact_display, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Keine Risiken definiert.")

    with tab3:
        if not history_df.empty and len(history_df) > 1:
            st.subheader("📉 Fieberkurve")
            history_df_local = history_df.copy()
            history_df_local['target_date_dt'] = pd.to_datetime(history_df_local['target_date'])
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=history_df_local['timestamp'], y=history_df_local['target_date_dt'],
                mode='lines+markers', line=dict(color='firebrick', width=3),
                fill='tozeroy', fillcolor='rgba(255,0,0,0.1)', marker=dict(size=8)
            ))
            fig_trend.update_layout(template="plotly_white", yaxis_title="Prognostizierter Zieltermin",
                                    xaxis_title="Messzeitpunkt", height=400)
            st.plotly_chart(fig_trend, use_container_width=True)
            st.divider()
            st.subheader("Mess-Historie")
            history_display = history_df_local.copy()
            history_display['target_date'] = pd.to_datetime(history_display['target_date']).dt.strftime('%d.%m.%Y')
            history_display['buffer'] = history_display['buffer'].round(1)
            st.dataframe(history_display, use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ Mindestens 2 Messungen nötig für Trend-Visualisierung.")

    with tab4:
        st.subheader("📜 Management Report")
        task_list_str = "".join([f"- {row['Task Name']}: {row['Duration (Days)']} Tage\n" for _, row in ed_t.iterrows()])
        risk_list_str = "".join([f"- {row['Risk Name']} ({row['Risk Type']}): {row['Probability (0-1)']} P\n" for _, row in ed_r.iterrows()])
        impact_ranking_str = ""
        if not impact_df.empty:
            ranked = impact_df.sort_values("Verzögerung", ascending=False)
            impact_ranking_str = "".join([f"- {row['Quelle']}: ~{row['Verzögerung']:.1f} Tage\n" for _, row in ranked.iterrows()])

        hist_report_df = history_df.tail(10).copy()
        try:
            hist_report_df['timestamp'] = pd.to_datetime(hist_report_df['timestamp']).dt.strftime('%d.%m.%Y %H:%M')
        except Exception:
            pass

        report_content = f"""MANAGEMENT TREND & RISK REPORT - {selected_proj}
Erstellt am: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Version: {APP_VERSION}
---------------------------------------------------------
STATUS: {warning_msg}
EMPFEHLUNG: {rec}

KENNZAHLEN:
- Zieltermin (85%): {commit_85.strftime('%d.%m.%Y')}
- Ø Projektdauer: {np.mean(durations):.1f} Tage
- Risiko-Puffer: {int(np.mean(durations) - ed_t['Duration (Days)'].sum())} Tage
- Min / Max: {int(np.min(durations))} / {int(np.max(durations))} Tage

RISIKO-TREIBER:
{impact_ranking_str or "- Keine Risiken definiert"}

TASKS:
{task_list_str or "- Keine Tasks"}

RISIKEN:
{risk_list_str or "- Keine Risiken"}

HISTORIE (letzte 10):
{hist_report_df.to_string(index=False) if not hist_report_df.empty else "- Keine Daten"}

Simulationsparameter: {n_sim} Durchläufe | Start: {start_date.strftime('%d.%m.%Y')} | Std-Risiken aktiv: {len(selected_std)}/{len(st.session_state.std_risk_df)}
"""
        st.text_area("Report Vorschau", report_content, height=400, disabled=True, key="report_preview")
        st.download_button("📥 Report exportieren (.txt)", report_content,
                           f"Report_{selected_proj}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                           use_container_width=True)

    with tab5:
        st.subheader("📊 Risiko-Validierung (Backtesting)")
        
        actual_df = load_actual_results(selected_proj)
        
        if not actual_df.empty:
            actual_df["planned_days_int"] = actual_df["planned_duration"].astype(int)
            actual_df["actual_days_int"] = actual_df["actual_duration"].astype(int)
            actual_df["abweichung_prozent"] = ((actual_df["actual_duration"] - actual_df["planned_duration"]) / actual_df["planned_duration"] * 100).round(1)
            
            # 1. Genauigkeit der 85%-Prognose
            st.write("### 🎯 85%-Prognose-Genauigkeit")
            correct_predictions = 0
            total = len(actual_df)
            
            for _, row in actual_df.iterrows():
                if row["actual_duration"] <= row["planned_duration"] * 1.15:
                    correct_predictions += 1
            
            accuracy = (correct_predictions / total * 100) if total > 0 else 0
            col_acc1, col_acc2, col_acc3 = st.columns(3)
            with col_acc1:
                st.metric("✅ Vorhersage-Accuracy", f"{accuracy:.1f}%", 
                          delta="Gut" if accuracy > 75 else "Überprüfen", 
                          delta_color="normal" if accuracy > 75 else "bad")
            with col_acc2:
                st.metric("📊 Projekte erfasst", total)
            with col_acc3:
                st.metric("✓ Prognose korrekt", int(correct_predictions))
            
            # 2. Häufigste eingetretene Risiken
            st.divider()
            st.write("### 🔥 Häufigste eingetretene Risiken")
            
            all_risks = []
            for risks_json in actual_df["risks_occurred"]:
                if risks_json:
                    try:
                        all_risks.extend(json.loads(risks_json))
                    except json.JSONDecodeError:
                        pass
            
            if all_risks:
                risk_counts = pd.Series(all_risks).value_counts()
                fig_risk = go.Figure(go.Bar(
                    x=risk_counts.values, 
                    y=risk_counts.index, 
                    orientation='h',
                    marker_color='#EF553B',
                    text=risk_counts.values,
                    textposition='auto'
                ))
                fig_risk.update_layout(
                    template="plotly_white",
                    xaxis_title="Häufigkeit",
                    height=max(300, len(risk_counts)*40),
                    margin=dict(l=200)
                )
                st.plotly_chart(fig_risk, use_container_width=True)
            else:
                st.info("ℹ️ Noch keine Risiken dokumentiert.")
            
            # 3. Abweichungs-Trend
            st.divider()
            st.write("### 📈 Abweichungs-Trend")
            
            fig_deviation = go.Figure()
            fig_deviation.add_trace(go.Scatter(
                x=actual_df.index,
                y=actual_df["abweichung_prozent"],
                mode='lines+markers',
                fill='tozeroy',
                fillcolor='rgba(239, 85, 59, 0.2)',
                line=dict(color='#EF553B', width=3),
                marker=dict(size=8),
                name='Abweichung (%)'
            ))
            fig_deviation.add_hline(y=0, line_dash="dash", line_color="green", 
                                    annotation_text="Plan", annotation_position="right")
            fig_deviation.add_hline(y=15, line_dash="dot", line_color="orange",
                                    annotation_text="85% Ziel", annotation_position="right")
            fig_deviation.update_layout(
                template="plotly_white",
                yaxis_title="Abweichung (%)",
                xaxis_title="Projekt-Nummer",
                height=400
            )
            st.plotly_chart(fig_deviation, use_container_width=True)
            
            # 4. Kalibrierungs-Empfehlungen
            st.divider()
            st.write("### 🔧 Kalibrierungs-Empfehlungen")
            
            avg_deviation = actual_df["abweichung_prozent"].mean()
            std_deviation = actual_df["abweichung_prozent"].std()
            
            col_rec1, col_rec2 = st.columns(2)
            with col_rec1:
                st.metric("Ø Abweichung", f"{avg_deviation:+.1f}%")
            with col_rec2:
                st.metric("Std. Abweichung", f"{std_deviation:.1f}%")
            
            st.divider()
            
            if avg_deviation > 15:
                st.warning(
                    f"⚠️ **Zu optimistisch geplant** (Ø +{avg_deviation:.1f}%)\n\n"
                    "**Empfehlung:**\n"
                    "- Standard-Risiko-Parameter um ~10-15% erhöhen\n"
                    "- Insbesondere: Erhöhen Sie Impact-Spannen bei kontinuierlichen Risiken\n"
                    "- Überprüfen Sie, ob neue Standardrisiken fehlen"
                )
            elif avg_deviation < -10:
                st.info(
                    f"ℹ️ **Zu pessimistisch geplant** (Ø {avg_deviation:.1f}%)\n\n"
                    "**Empfehlung:**\n"
                    "- Standard-Risiko-Parameter um ~5-10% reduzieren\n"
                    "- Überprüfen Sie Wahrscheinlichkeiten (sind sie zu hoch?)\n"
                    "- Deaktivieren Sie möglicherweise nicht-relevante Risiken"
                )
            else:
                st.success(
                    f"✅ **Sehr gut geplant** (Ø {avg_deviation:+.1f}%)\n\n"
                    "Ihre Risiko-Parameter sind gut kalibriert. Weiterhin monatlich überprüfen."
                )
            
            # 5. Detaillierte Tabelle
            st.divider()
            st.write("### 📋 Alle erfassten Projektergebnisse")
            
            display_df = actual_df[[
                "start_date", "planned_end_date", "actual_end_date", 
                "planned_days_int", "actual_days_int", "abweichung_prozent", "notes"
            ]].copy()
            display_df.columns = [
                "Start", "Geplant", "Tatsächlich", 
                "Plan (Tage)", "Actual (Tage)", "Abw. (%)", "Notizen"
            ]
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
        else:
            st.info(
                "ℹ️ **Noch keine Projektergebnisse erfasst.**\n\n"
                "**So funktioniert Backtesting:**\n"
                "1. Nach Projekt-Abschluss das Formular unten ausfüllen\n"
                "2. Mindestens 3-5 Projekte sammeln\n"
                "3. Dieser Tab zeigt dann automatisch Validierungsergebnisse\n\n"
                "Mit echten Daten können Sie die Risiko-Parameter kontinuierlich verbessern!"
            )

    # --- ALTES FORMULAR KOMPLETT LÖSCHEN ---
    # Das folgende expander-Block ENTFERNEN:
    # with st.expander("✅ Projekt-Abschluss dokumentieren", expanded=False):
    #     ...

    # --- NUR DIESES FORMULAR BEHALTEN ---
    st.divider()
    with st.expander("✅ Projektverlauf dokumentieren", expanded=False):
        
        erfassungs_modus = st.radio(
            "Erfassungs-Typ:",
            ["📅 Zwischenstand (laufend)", "🏁 Projektabschluss (final)"],
            horizontal=True,
            key="erfassungs_modus"
        )

        actual_start = st.date_input("Start-Datum", start_date, key="proj_actual_start")

        if erfassungs_modus == "📅 Zwischenstand (laufend)":
            st.subheader("📅 Laufender Zwischenstand")
            col_zw1, col_zw2 = st.columns(2)
            with col_zw1:
                stand_datum = st.date_input("Stand-Datum (heute)", datetime.now(), key="stand_datum")
            with col_zw2:
                fertig_prozent = st.slider("Fertigstellungsgrad (%)", 0, 100, 50, key="fertig_prozent")

            tatsaechliche_tage_bisher = (stand_datum - actual_start).days
            geplante_tage_bisher = ed_t["Duration (Days)"].sum() * (fertig_prozent / 100)

            col_info1, col_info2 = st.columns(2)
            with col_info1:
                st.metric("Tage bisher (tatsächlich)", tatsaechliche_tage_bisher)
            with col_info2:
                if geplante_tage_bisher > 0:
                    velocity = geplante_tage_bisher / max(tatsaechliche_tage_bisher, 1)
                    st.metric("Velocity (Plan/Actual)", f"{velocity:.2f}",
                              delta="Im Plan" if velocity >= 0.9 else "Verzögert",
                              delta_color="normal" if velocity >= 0.9 else "inverse")

            risiken_jetzt = st.multiselect(
                "Aktuell aktive / eingetretene Risiken:",
                options=ed_r["Risk Name"].tolist() + [sr["name"] for sr in selected_std],
                key="risiken_zwischenstand"
            )
            notiz_zwischen = st.text_area("Notiz zum Zwischenstand", key="notiz_zwischen", height=80)

            if st.button("💾 Zwischenstand speichern", use_container_width=True):
                try:
                    if geplante_tage_bisher > 0 and velocity > 0:
                        projected_total_days = int(ed_t["Duration (Days)"].sum() / velocity)
                    else:
                        projected_total_days = int(ed_t["Duration (Days)"].sum() * 2)
                    projected_end = actual_start + pd.Timedelta(days=projected_total_days)
                    save_actual_result(
                        selected_proj,
                        str(actual_start),
                        str(commit_85.date()),
                        str(projected_end.date()),
                        str(st.session_state.last_top_r) if st.session_state.last_top_r else "N/A",
                        risiken_jetzt,
                        f"[ZWISCHENSTAND {fertig_prozent}%] {notiz_zwischen}"
                    )
                    st.success(f"✅ Zwischenstand gespeichert! Hochgerechnetes Ende: {projected_end.strftime('%d.%m.%Y')}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Fehler: {str(e)}")

        else:
            st.subheader("🏁 Projektabschluss")
            actual_end_final = st.date_input("Tatsächliches End-Datum", key="actual_end_final")
            all_risk_options = ed_r["Risk Name"].tolist() + [sr["name"] for sr in selected_std]
            risks_that_occurred = st.multiselect(
                "Eingetretene Risiken (gesamt):",
                options=all_risk_options if all_risk_options else ["Keine"],
                key="risks_occurred_final"
            )
            actual_notes = st.text_area("Lessons Learned", key="actual_notes_final", height=100)

            if st.button("💾 Projektabschluss speichern", use_container_width=True, type="primary"):
                try:
                    save_actual_result(
                        selected_proj,
                        str(actual_start),
                        str(commit_85.date() if st.session_state.last_commit_85 else str(actual_end_final)),
                        str(actual_end_final),
                        str(st.session_state.last_top_r) if st.session_state.last_top_r else "N/A",
                        risks_that_occurred,
                        f"[ABSCHLUSS] {actual_notes}"
                    )
                    st.success("✅ Projektabschluss gespeichert!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Fehler beim Speichern: {str(e)}")
