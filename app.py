import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
import traceback
from datetime import datetime
import plotly.graph_objects as go

# --- 1. SETUP & CONFIG ---
APP_VERSION = "3.0.0"
DB_FILE = "risk_management.db"

st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. SESSION STATE ---
for key in [
    "snapshot_durations", "snapshot_date", "auth_ok",
    "last_durations", "last_impact_df", "last_commit_85",
    "last_end_dates", "last_top_r", "last_diff",
    "last_warning_msg", "last_rec", "last_history_df",
    "demo_mode", "std_risk_df",
    "toast_msg", "toast_type",
]:
    if key not in st.session_state:
        st.session_state[key] = None

# --- 3. DATABASE ---
def get_db_connection():
    return sqlite3.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, task_name TEXT, duration REAL, description TEXT, team TEXT, sequence INTEGER)")
    conn.execute("CREATE TABLE IF NOT EXISTS risks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, risk_name TEXT, risk_type TEXT, target TEXT, prob REAL, impact_min REAL, impact_likely REAL, impact_max REAL, mitigation TEXT, effect TEXT DEFAULT 'Threat')")
    conn.execute("CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, timestamp TEXT, target_date TEXT, buffer REAL, top_risk TEXT)")
    conn.execute("""CREATE TABLE IF NOT EXISTS actual_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, start_date TEXT,
            planned_end_date TEXT, actual_end_date TEXT, planned_duration REAL,
            actual_duration REAL, buffer_used REAL, top_risk TEXT, risks_occurred TEXT, notes TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT,
            team_name TEXT, capacity REAL DEFAULT 1.0)""")
    for sql in [
        "ALTER TABLE risks ADD COLUMN effect TEXT DEFAULT 'Threat'",
        "ALTER TABLE tasks ADD COLUMN team TEXT DEFAULT 'Sequential'",
        "ALTER TABLE tasks ADD COLUMN sequence INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

init_db()

# --- 4. HELPER FUNCTIONS ---

def _norm_risk_name(value):
    """Normalize risk names for robust lookups."""
    return str(value).strip().casefold()

def _build_effect_lookup(ed_r_df, selected_std_list):
    """
    Mapping: normalized risk name -> 'Threat' | 'Opportunity'
    Project risks take precedence, standard risks default to Threat.
    """
    lookup = {}
    if ed_r_df is not None and not ed_r_df.empty:
        for _, rr in ed_r_df.iterrows():
            name = str(rr.get("Risk Name", "")).strip()
            if not name:
                continue
            raw_eff = str(rr.get("Effect", "Threat")).strip().casefold()
            lookup[_norm_risk_name(name)] = "Opportunity" if raw_eff == "opportunity" else "Threat"
    for sr in (selected_std_list or []):
        sname = str(sr.get("name", "")).strip()
        if sname:
            lookup.setdefault(_norm_risk_name(sname), "Threat")
    return lookup

def get_all_projects():
    conn = get_db_connection()
    df = pd.read_sql("""
        SELECT DISTINCT project FROM tasks
        UNION SELECT DISTINCT project FROM risks
        UNION SELECT DISTINCT project FROM teams
        UNION SELECT DISTINCT project FROM history
        UNION SELECT DISTINCT project FROM actual_results
    """, conn)
    conn.close()
    projs = df["project"].dropna().tolist()
    return projs if projs else ["Demo_Project"]

def load_tasks(project):
    conn = get_db_connection()
    cols = pd.read_sql("PRAGMA table_info(tasks)", conn)["name"].tolist()
    has_team = "team" in cols
    q = """
        SELECT task_name as 'Task Name', duration as 'Duration (Days)',
               description as 'Description',
               {} as 'team'
        FROM tasks WHERE project=? ORDER BY id
    """.format("COALESCE(team, 'Sequential')" if has_team else "'Sequential'")
    df = pd.read_sql(q, conn, params=(project,))
    conn.close()
    if df.empty:
        df = pd.DataFrame([{"Task Name": "Base Task", "Duration (Days)": 10.0,
                            "Description": "", "team": "Sequential"}])
    return df.reset_index(drop=True)

def load_risks(project):
    conn = get_db_connection()
    df = pd.read_sql("""
        SELECT risk_name as 'Risk Name', risk_type as 'Risk Type',
               target as 'Target (Global/Task)', prob as 'Probability (0-1)',
               impact_min as 'Impact Min', impact_likely as 'Impact Likely',
               impact_max as 'Impact Max', mitigation as 'Mitigation',
               COALESCE(effect, 'Threat') as 'Effect'
        FROM risks WHERE project=?
    """, conn, params=(project,))
    conn.close()
    if "Effect" not in df.columns:
        df["Effect"] = "Threat"
    df["Effect"] = df["Effect"].fillna("Threat")
    return df.reset_index(drop=True)

def save_data(project, df_tasks, df_risks):
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    for idx, row in df_tasks.iterrows():
        task_name = str(row.get("Task Name", "")).strip()
        if task_name:
            try:
                duration = float(row["Duration (Days)"])
                if duration > 0:
                    conn.execute(
                        "INSERT INTO tasks (project, task_name, duration, description, team, sequence) VALUES (?,?,?,?,?,?)",
                        (project, task_name, duration,
                         str(row.get("Description", "")),
                         str(row.get("team", "Sequential")),
                         int(idx)))
            except (ValueError, TypeError):
                pass
    for _, row in df_risks.iterrows():
        risk_name = str(row.get("Risk Name", "")).strip()
        if risk_name:
            try:
                prob = float(row.get("Probability (0-1)", 0))
                if 0 <= prob <= 1:
                    conn.execute("""
                        INSERT INTO risks
                        (project, risk_name, risk_type, target, prob,
                         impact_min, impact_likely, impact_max, mitigation, effect)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (project, risk_name, str(row.get("Risk Type", "Binary")),
                          str(row.get("Target (Global/Task)", "Global")),
                          prob, float(row.get("Impact Min", 0)),
                          float(row.get("Impact Likely", 0)),
                          float(row.get("Impact Max", 0)),
                          str(row.get("Mitigation", "")),
                          str(row.get("Effect", "Threat"))))
            except (ValueError, TypeError):
                pass
    conn.commit()
    conn.close()

def save_history(project, target_date, buffer, top_risk):
    conn = get_db_connection()
    conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                 (project, datetime.now().strftime('%Y-%m-%d %H:%M'), target_date, buffer, top_risk))
    conn.commit()
    conn.close()

def delete_history_only(project):
    conn = get_db_connection()
    conn.execute("DELETE FROM history WHERE project=?", (project,))
    conn.commit()
    conn.close()

def load_history(project):
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT timestamp, target_date, buffer, top_risk FROM history WHERE project=? ORDER BY timestamp ASC",
        conn, params=(project,))
    conn.close()
    return df

def delete_project_complete(project):
    conn = get_db_connection()
    for tbl in ["tasks", "risks", "history", "actual_results", "teams"]:
        conn.execute(f"DELETE FROM {tbl} WHERE project=?", (project,))
    conn.commit()
    conn.close()

def save_actual_result(project, start_date, planned_end, actual_end, top_risk, risks_occurred, notes=""):
    conn = get_db_connection()
    planned_days = (pd.to_datetime(planned_end) - pd.to_datetime(start_date)).days
    actual_days  = (pd.to_datetime(actual_end)  - pd.to_datetime(start_date)).days
    buffer_used  = actual_days - planned_days
    conn.execute("""
        INSERT INTO actual_results
        (project, start_date, planned_end_date, actual_end_date,
         planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (project, start_date, planned_end, actual_end,
          planned_days, actual_days, buffer_used,
          top_risk, json.dumps(risks_occurred), notes))
    conn.commit()
    conn.close()

def load_actual_results(project):
    conn = get_db_connection()
    df = pd.read_sql("""
        SELECT start_date, planned_end_date, actual_end_date,
               planned_duration, actual_duration, buffer_used,
               top_risk, risks_occurred, notes
        FROM actual_results WHERE project = ? ORDER BY actual_end_date ASC
    """, conn, params=(project,))
    conn.close()
    return df

def get_default_standard_risks_df():
    return pd.DataFrame([
        {"Active": True,  "name": "Estimation Inaccuracy",      "type": "Continuous", "prob": 1.00, "min": -0.03, "likely": 0.00, "max": 0.08},
        {"Active": True,  "name": "Scope Creep",                "type": "Binary",     "prob": 0.40, "min":  0.05, "likely": 0.15, "max": 0.30},
        {"Active": True,  "name": "Technical Debt",             "type": "Continuous", "prob": 1.00, "min":  0.03, "likely": 0.07, "max": 0.15},
        {"Active": True,  "name": "Staff Turnover",             "type": "Binary",     "prob": 0.20, "min":  0.05, "likely": 0.12, "max": 0.25},
        {"Active": True,  "name": "Illness / Absence",          "type": "Binary",     "prob": 0.25, "min":  0.03, "likely": 0.08, "max": 0.15},
        {"Active": True,  "name": "Integration Issues",         "type": "Binary",     "prob": 0.35, "min":  0.04, "likely": 0.10, "max": 0.22},
        {"Active": True,  "name": "Testing & QA Rework",        "type": "Continuous", "prob": 1.00, "min":  0.02, "likely": 0.06, "max": 0.12},
        {"Active": True,  "name": "External Dependencies",      "type": "Binary",     "prob": 0.30, "min":  0.05, "likely": 0.12, "max": 0.25},
        {"Active": True,  "name": "Deployment / Release Risk",  "type": "Binary",     "prob": 0.22, "min":  0.03, "likely": 0.07, "max": 0.16},
        {"Active": True,  "name": "Requirements Ambiguity",     "type": "Continuous", "prob": 1.00, "min":  0.02, "likely": 0.05, "max": 0.11},
        {"Active": False, "name": "Tooling / Environment",      "type": "Binary",     "prob": 0.18, "min":  0.02, "likely": 0.06, "max": 0.12},
        {"Active": False, "name": "Security / Compliance",      "type": "Binary",     "prob": 0.15, "min":  0.03, "likely": 0.09, "max": 0.20},
    ])

def load_teams(project):
    conn = get_db_connection()
    df = pd.read_sql(
        "SELECT team_name as 'Team', capacity as 'Capacity' FROM teams WHERE project=? ORDER BY team_name",
        conn, params=(project,))
    conn.close()
    if df.empty:
        df = pd.DataFrame([{"Team": "Standard", "Capacity": 1.0}])
    return df.reset_index(drop=True)

def save_teams(project, df_teams):
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

# ===== EXPORT COMPLETE PROJECT =====
def export_complete_project(project):
    """Export all project data as a complete JSON package."""
    tasks   = load_tasks(project)
    risks   = load_risks(project)
    teams   = load_teams(project)
    history = load_history(project)
    actual  = load_actual_results(project)

    payload = {
        "export_version": APP_VERSION,
        "export_date":    datetime.now().strftime('%Y-%m-%d %H:%M'),
        "project":        project,
        "tasks":          tasks.to_dict(orient="records"),
        "risks":          risks.to_dict(orient="records"),
        "teams":          teams.to_dict(orient="records"),
        "history":        history.to_dict(orient="records"),
        "actual_results": actual.to_dict(orient="records"),
    }
    return json.dumps(payload, indent=2, default=str)

def import_complete_project(json_str):
    """Import a complete project from JSON. Returns project name or raises."""
    data    = json.loads(json_str)
    project = data.get("project", "Imported_Project")

    if data.get("tasks"):
        save_data(project,
                  pd.DataFrame(data["tasks"]),
                  pd.DataFrame(data.get("risks", [])))
    if data.get("teams"):
        save_teams(project, pd.DataFrame(data["teams"]))

    conn = get_db_connection()
    for row in data.get("history", []):
        try:
            conn.execute(
                "INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                (project, row.get("timestamp"), row.get("target_date"),
                 row.get("buffer"), row.get("top_risk")))
        except Exception:
            pass
    for row in data.get("actual_results", []):
        try:
            conn.execute("""
                INSERT INTO actual_results
                (project, start_date, planned_end_date, actual_end_date,
                 planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (project, row.get("start_date"), row.get("planned_end_date"),
                  row.get("actual_end_date"), row.get("planned_duration"),
                  row.get("actual_duration"), row.get("buffer_used"),
                  row.get("top_risk"), row.get("risks_occurred"), row.get("notes")))
        except Exception:
            pass
    conn.commit()
    conn.close()
    return project

# ===== BUSINESS DAYS =====
GERMAN_HOLIDAYS = [
    "2024-01-01","2024-03-29","2024-04-01","2024-05-01","2024-05-09",
    "2024-05-19","2024-05-30","2024-10-03","2024-12-25","2024-12-26",
    "2025-01-01","2025-04-18","2025-04-21","2025-05-01","2025-05-29",
    "2025-06-09","2025-10-03","2025-12-25","2025-12-26",
    "2026-01-01","2026-04-10","2026-04-13","2026-05-01","2026-05-21",
    "2026-06-11","2026-10-03","2026-12-25","2026-12-26",
]

def add_business_days_vectorized(start_date, days_array):
    """
    Vectorized business day calculation — ~100x faster than loop.
    Converts an array of calendar durations to business day end dates.
    """
    holiday_set = set(pd.to_datetime(GERMAN_HOLIDAYS).strftime('%Y-%m-%d'))

    # Pre-build a lookup: calendar_day_offset -> actual end date
    # Max range we need to cover
    max_days = int(days_array.max()) + 500  # buffer for weekends/holidays

    start = pd.Timestamp(start_date)
    # Generate all calendar days from start
    all_dates = pd.date_range(start, periods=max_days, freq='D')

    # Boolean mask: is this a business day?
    is_business = np.array([
        d.weekday() < 5 and d.strftime('%Y-%m-%d') not in holiday_set
        for d in all_dates
    ])

    # Cumulative business day counter per calendar offset
    cum_bdays = np.cumsum(is_business)

    # For each required number of business days, find the calendar date
    # cum_bdays[i] = number of business days from start up to calendar day i
    results = []
    for n_bdays in days_array:
        n_bdays = max(1, int(n_bdays))
        # Find first calendar index where cum_bdays >= n_bdays
        idx = np.searchsorted(cum_bdays, n_bdays, side='left')
        if idx < len(all_dates):
            results.append(all_dates[idx])
        else:
            results.append(all_dates[-1])

    return pd.DatetimeIndex(results)

# --- 5. SIMULATION ---
def run_fast_simulation(tasks, risks, std_risks, teams, n):
    if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
        raise ValueError("No valid tasks defined or all durations = 0")

    tasks_copy = tasks.copy()
    if "team" not in tasks_copy.columns:
        tasks_copy["team"] = "Sequential"

    team_durations = {}
    for team_name in tasks_copy["team"].unique():
        team_tasks = tasks_copy[tasks_copy["team"] == team_name]
        team_sum   = team_tasks["Duration (Days)"].sum()
        if not teams.empty:
            team_row = teams[teams["Team"] == team_name]
            if not team_row.empty:
                capacity = float(team_row.iloc[0].get("Capacity", 1.0))
                team_sum = team_sum / capacity
        team_durations[team_name] = team_sum

    base_sum   = max(team_durations.values()) if team_durations else tasks_copy["Duration (Days)"].sum()
    total_days = np.full(n, base_sum, dtype=float)
    impact_results = []

    for _, r in risks.iterrows():
        p         = float(r.get("Probability (0-1)", 0))
        rtype     = str(r.get("Risk Type", "Binary")).strip().lower()
        target    = str(r.get("Target (Global/Task)", "Global")).strip()
        effect    = str(r.get("Effect", "Threat")).strip().casefold()
        direction = -1.0 if effect == "opportunity" else 1.0
        task_team = "Global"

        mins = float(r.get("Impact Min", 0))
        lik  = float(r.get("Impact Likely", mins))
        maxv = float(r.get("Impact Max", lik))

        impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
        hits    = np.ones(n, dtype=bool) if rtype in ("continuous", "kontinuierlich") else (np.random.random(n) < p)

        if target.lower() == "global":
            relevant_dur = base_sum
        else:
            match = tasks_copy[tasks_copy["Task Name"] == target]
            if not match.empty:
                relevant_dur = float(match["Duration (Days)"].iloc[0])
                task_team    = str(match.iloc[0].get("team", "Sequential"))
                if not teams.empty:
                    team_row = teams[teams["Team"] == task_team]
                    if not team_row.empty:
                        relevant_dur /= float(team_row.iloc[0].get("Capacity", 1.0))
            else:
                relevant_dur = 0.0

        if relevant_dur <= 0:
            continue

        delay   = impacts * relevant_dur
        applied = (delay if rtype in ("continuous", "kontinuierlich") else hits * delay) * direction
        total_days += applied

        impact_results.append({
            "Source":   str(r.get("Risk Name", "Unknown")),
            "Delay":    float(applied.mean()),
            "Type":     "Project",
            "Team":     task_team,
            "Effect":   "Opportunity" if direction < 0 else "Threat"
        })

    for sr in std_risks:
        rtype   = str(sr.get("type", "Binary")).strip().lower()
        p       = float(sr.get("prob", 0))
        mins    = float(sr.get("min", 0))
        lik     = float(sr.get("likely", mins))
        maxv    = float(sr.get("max", lik))
        impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
        hits    = np.ones(n, dtype=bool) if rtype in ("continuous", "kontinuierlich") else (np.random.random(n) < p)
        delay   = impacts * base_sum
        applied = delay if rtype in ("continuous", "kontinuierlich") else (hits * delay)
        total_days += applied
        impact_results.append({
            "Source": f"STD: {sr['name']}",
            "Delay":  float(applied.mean()),
            "Type":   "Standard",
            "Team":   "Global",
            "Effect": "Threat"
        })

    total_days = np.clip(total_days, 0, 10000)
    return total_days.astype(int), pd.DataFrame(impact_results)

# --- 6. LOGIN ---
if not st.session_state.auth_ok:
    st.title("🔐 Login")
    st.divider()
    col_login1, col_login2 = st.columns([1.5, 1])
    with col_login1:
        st.subheader("Sign In")
        u = st.text_input("👤 Username", placeholder="admin")
        p = st.text_input("🔑 Password", type="password", placeholder="••••••")
        if st.button("🔓 Sign In", use_container_width=True):
            if ("credentials" in st.secrets
                    and u == st.secrets["credentials"]["username"]
                    and p == st.secrets["credentials"]["password"]):
                st.session_state.auth_ok   = True
                st.session_state.demo_mode = False
                st.rerun()
            else:
                st.error("❌ Invalid credentials.")
    with col_login2:
        st.subheader("Demo Mode")
        st.info("ℹ️ Try the app without login.")
        if st.button("🎮 Start Demo", use_container_width=True):
            st.session_state.auth_ok   = True
            st.session_state.demo_mode = True
            st.rerun()
    st.stop()

# --- 7. SIDEBAR ---
with st.sidebar:
    st.header("📂 Project Control")
    if st.session_state.demo_mode:
        st.warning("🎮 **Demo Mode active**")

    all_projs     = get_all_projects()
    selected_proj = st.selectbox("Active Project:", all_projs)

    st.divider()
    st.subheader("📈 Tracking")
    do_history = st.checkbox("Save measurement to time series", value=True)

    with st.expander("🛠️ Admin, Export & Import"):
        st.subheader("📋 Project Management")
        new_p = st.text_input("New Project / Copy:")
        if st.button("🚀 Create", use_container_width=True):
            if new_p.strip():
                save_data(new_p, load_tasks(selected_proj), load_risks(selected_proj))
                st.session_state.toast_msg  = f"Project '{new_p}' created!"
                st.session_state.toast_type = "success"
                st.rerun()
            else:
                st.session_state.toast_msg  = "Project name required!"
                st.session_state.toast_type = "warning"
                st.rerun()

        st.divider()
        st.subheader("📤 Export")

        # Simple export (tasks + risks only)
        t_exp = load_tasks(selected_proj)
        r_exp = load_risks(selected_proj)
        simple_payload = json.dumps({
            "project": selected_proj,
            "tasks":   t_exp.to_dict(orient="records"),
            "risks":   r_exp.to_dict(orient="records")
        }, indent=2)
        st.download_button("📤 Export Tasks & Risks (JSON)",
                           simple_payload,
                           f"{selected_proj}_tasks_risks.json",
                           use_container_width=True)

        # Complete export (all data)
        complete_payload = export_complete_project(selected_proj)
        st.download_button("📦 Export Complete Project (JSON)",
                           complete_payload,
                           f"{selected_proj}_complete_{datetime.now().strftime('%Y%m%d')}.json",
                           mime="application/json",
                           use_container_width=True)

        st.divider()
        st.subheader("📥 Import")
        uploaded = st.file_uploader("Import Project (JSON)", type="json")
        if uploaded:
            if st.button("📥 Import Project", use_container_width=True):
                try:
                    imp_name = import_complete_project(uploaded.read().decode("utf-8"))
                    st.session_state.toast_msg  = f"Project '{imp_name}' imported!"
                    st.session_state.toast_type = "success"
                    st.rerun()
                except Exception as e:
                    st.session_state.toast_msg  = f"Import failed: {str(e)}"
                    st.session_state.toast_type = "error"
                    st.rerun()

        st.divider()
        st.subheader("🗑️ Clean Up")
        if st.button("📊 Delete History", use_container_width=True):
            delete_history_only(selected_proj)
            st.session_state.toast_msg  = "History deleted."
            st.session_state.toast_type = "success"
            st.rerun()

        st.divider()
        st.subheader("❗ Delete Project")
        st.warning("⚠️ Irreversible!")
        confirm_del = st.checkbox(f"Confirm deletion of '{selected_proj}'")
        if st.button(f"🗑️ Delete {selected_proj}", use_container_width=True, type="secondary"):
            if confirm_del:
                delete_project_complete(selected_proj)
                st.session_state.toast_msg  = f"Project '{selected_proj}' deleted."
                st.session_state.toast_type = "success"
                st.rerun()
            else:
                st.session_state.toast_msg  = "Confirmation required!"
                st.session_state.toast_type = "warning"
                st.rerun()

    st.divider()
    st.subheader("📊 Scenario Comparison")
    if st.session_state.snapshot_durations is not None and st.session_state.snapshot_date is not None:
        st.success(f"✅ **Snapshot active**\n\n📅 {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")
        if st.button("🗑️ Delete Snapshot", use_container_width=True, key="delete_snapshot"):
            st.session_state.snapshot_durations = None
            st.session_state.snapshot_date      = None
            st.rerun()
    else:
        st.info("ℹ️ No snapshot saved.\n\nAfter simulation: click '📸 Freeze State'.")

    st.divider()
    st.subheader("🏢 Global Standard Risks")

    if st.session_state.std_risk_df is None:
        st.session_state.std_risk_df = get_default_standard_risks_df()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("All on", use_container_width=True):
            df_tmp = st.session_state.std_risk_df.copy()
            df_tmp["Active"] = True
            st.session_state.std_risk_df = df_tmp
            st.rerun()
    with c2:
        if st.button("All off", use_container_width=True):
            df_tmp = st.session_state.std_risk_df.copy()
            df_tmp["Active"] = False
            st.session_state.std_risk_df = df_tmp
            st.rerun()

    with st.expander("⚙️ Configure Standard Risks", expanded=False):
        edited_std = st.data_editor(
            st.session_state.std_risk_df,
            use_container_width=True, num_rows="dynamic", key="std_risk_editor",
            column_config={
                "Active": st.column_config.CheckboxColumn("Active"),
                "name":   st.column_config.TextColumn("Risk"),
                "type":   st.column_config.SelectboxColumn("Type", options=["Binary", "Continuous"], required=True),
                "prob":   st.column_config.NumberColumn("Probability", min_value=0.0, max_value=1.0, step=0.01),
                "min":    st.column_config.NumberColumn("Impact Min",    step=0.01),
                "likely": st.column_config.NumberColumn("Impact Likely", step=0.01),
                "max":    st.column_config.NumberColumn("Impact Max",    step=0.01),
            })
        if st.button("💾 Apply", use_container_width=True):
            df_tmp = edited_std.copy()
            df_tmp["prob"] = df_tmp["prob"].clip(0, 1)
            vals = np.sort(df_tmp[["min", "likely", "max"]].values, axis=1)
            df_tmp["min"], df_tmp["likely"], df_tmp["max"] = vals[:, 0], vals[:, 1], vals[:, 2]
            st.session_state.std_risk_df = df_tmp
            st.session_state.toast_msg   = "Standard risks updated."
            st.session_state.toast_type  = "success"
            st.rerun()

    active_std_df = st.session_state.std_risk_df[st.session_state.std_risk_df["Active"] == True].copy()
    selected_std  = active_std_df[["name", "type", "prob", "min", "likely", "max"]].to_dict(orient="records")
    st.caption(f"Active: {len(selected_std)} of {len(st.session_state.std_risk_df)}")

    st.divider()
    st.subheader("⚙️ Simulation Parameters")
    n_sim      = st.number_input("Runs", min_value=1000, max_value=50000, value=10000, step=1000)
    start_date = st.date_input("Project Start", datetime.now())
    use_business_days = st.checkbox("📅 Use Business Days (Mon–Fri)",
                                    value=False,
                                    help="True = Mon–Fri only, German holidays excluded")

# --- 8. MAIN ---
st.title(f"🎲 {selected_proj} | Risk Sim Pro v{APP_VERSION}")

# Toast feedback after rerun
if st.session_state.toast_msg:
    _icon = {"success": "✅", "error": "❌", "warning": "⚠️"}.get(
        st.session_state.toast_type, "ℹ️")
    st.toast(st.session_state.toast_msg, icon=_icon)
    st.session_state.toast_msg  = None
    st.session_state.toast_type = None

st.info("📌 **Workflow:** 1️⃣ Define & save tasks → 2️⃣ Configure risks → 3️⃣ Run simulation")

# --- TEAMS ---
st.divider()
st.subheader("👥 Team Management")
st.info("💡 Tasks of the same team run **sequentially**, different teams run **in parallel**.\n"
        "Capacity > 1.0 = faster (e.g. 2.0 = double speed).")

tm_curr = load_teams(selected_proj)
col_a, col_b, col_c = st.columns(3)

with col_a:
    st.write("**Team Name**")
    team_names = st.data_editor(tm_curr[["Team"]].reset_index(drop=True),
                                use_container_width=True, num_rows="dynamic",
                                key=f"team_names_{selected_proj}", hide_index=True)
with col_b:
    st.write("**Capacity**")
    team_caps = st.data_editor(
        tm_curr[["Capacity"]].reset_index(drop=True),
        use_container_width=True, num_rows="dynamic",
        key=f"team_caps_{selected_proj}", hide_index=True,
        column_config={"Capacity": st.column_config.NumberColumn(
            "Capacity", min_value=0.1, max_value=5.0, step=0.1)})
with col_c:
    st.write("**Examples**")
    st.markdown("- 0.5 = **2× slower**\n- 1.0 = **Normal**\n- 2.0 = **2× faster**\n- 3.0 = **3× faster**")

if not team_names.empty and not team_caps.empty:
    ed_tm = pd.concat([team_names.reset_index(drop=True),
                       team_caps.reset_index(drop=True)], axis=1)
    if st.button("💾 Save Teams", use_container_width=True, key="save_teams"):
        save_teams(selected_proj, ed_tm)
        st.session_state.toast_msg  = "Teams saved!"
        st.session_state.toast_type = "success"
        st.rerun()
else:
    ed_tm = tm_curr.copy()

# --- TASKS ---
st.divider()
st.subheader("📋 1. Project Structure (Tasks)")
t_curr = load_tasks(selected_proj)
if "team" not in t_curr.columns:
    t_curr.insert(len(t_curr.columns), "team", "Sequential")

critical_path_calc = 0.0
tab_edit, tab_preview = st.tabs(["Edit", "Critical Path Preview"])

with tab_edit:
    team_options = ["Sequential"] + ed_tm["Team"].tolist() if not ed_tm.empty else ["Sequential"]
    ed_t = st.data_editor(
        t_curr, use_container_width=True, num_rows="dynamic", key=f"t_{selected_proj}",
        column_config={
            "Duration (Days)": st.column_config.NumberColumn("Duration (Days)", min_value=0.1, step=1.0),
            "team": st.column_config.SelectboxColumn("Team", options=team_options,
                                                      help="Assign team for parallel execution")
        })

with tab_preview:
    st.write("**Critical Path Calculation**")
    if not ed_t.empty and "team" in ed_t.columns:
        team_breakdown = ed_t.groupby("team")["Duration (Days)"].sum().sort_values(ascending=False)
        for team, duration in team_breakdown.items():
            capacity = 1.0
            if not ed_tm.empty:
                team_row = ed_tm[ed_tm["Team"] == team]
                if not team_row.empty:
                    capacity = float(team_row.iloc[0]["Capacity"])
            effective = duration / capacity
            col1, col2, col3, col4 = st.columns(4)
            col1.write(f"**{team}**")
            col2.metric("Sum", f"{duration:.0f}d")
            col3.metric("Capacity", f"{capacity}x")
            col4.metric("Effective", f"{effective:.1f}d")

        try:
            critical_path_calc = max([
                float(ed_t[ed_t["team"] == t]["Duration (Days)"].sum()) /
                (float(ed_tm[ed_tm["Team"] == t]["Capacity"].iloc[0])
                 if not ed_tm.empty and not ed_tm[ed_tm["Team"] == t].empty else 1.0)
                for t in ed_t["team"].unique()
            ])
        except Exception:
            critical_path_calc = float(ed_t["Duration (Days)"].sum())

        st.success(f"✅ **Critical Path = {critical_path_calc:.1f} days** "
                   f"(total sum would be: {ed_t['Duration (Days)'].sum():.0f} days)")

col_t1, col_t2 = st.columns(2)
with col_t1:
    if st.button("💾 Save Tasks", use_container_width=True):
        if "team" not in ed_t.columns:
            ed_t["team"] = "Sequential"
        ed_t["team"] = ed_t["team"].fillna("Sequential")
        save_data(selected_proj, ed_t, load_risks(selected_proj))
        st.session_state.toast_msg  = "Tasks saved!"
        st.session_state.toast_type = "success"
        st.rerun()
with col_t2:
    st.info(f"📊 {len(ed_t)} Tasks | CP: {critical_path_calc:.0f}d")

# --- RISKS ---
st.divider()
st.subheader("⚠️ 2. Risk Register")
r_curr = load_risks(selected_proj)
if "Effect" not in r_curr.columns:
    r_curr["Effect"] = "Threat"
r_curr["Effect"] = r_curr["Effect"].fillna("Threat")

t_opts = ["Global"] + t_curr["Task Name"].tolist()
ed_r = st.data_editor(
    r_curr, use_container_width=True, num_rows="dynamic", key=f"r_{selected_proj}",
    column_config={
        "Risk Type": st.column_config.SelectboxColumn("Logic", options=["Binary", "Continuous"], required=True),
        "Target (Global/Task)": st.column_config.SelectboxColumn("Target", options=t_opts, required=True),
        "Probability (0-1)": st.column_config.NumberColumn("Probability", min_value=0.0, max_value=1.0, step=0.01),
        "Effect": st.column_config.SelectboxColumn(
            "Effect", options=["Threat", "Opportunity"], required=True,
            help="Threat = delays project | Opportunity = accelerates project"),
    })

col_r1, _, col_r3 = st.columns([1, 1, 2])
with col_r1:
    if st.button("💾 Save Risks", use_container_width=True):
        save_data(selected_proj, ed_t, ed_r)
        st.session_state.toast_msg  = "Risks saved!"
        st.session_state.toast_type = "success"
        st.rerun()
with col_r3:
    st.metric("Total Risks", len(ed_r))

# --- SIMULATION ---
st.divider()
if st.button("🚀 Run Simulation & Analyze Trends", use_container_width=True, type="primary"):
    try:
        if ed_t.empty or ed_t["Duration (Days)"].sum() <= 0:
            st.error("❌ No valid tasks defined.")
            st.stop()

        with st.spinner("⏳ Running Monte-Carlo Simulation..."):
            durations, impact_df = run_fast_simulation(ed_t, ed_r, selected_std, ed_tm, n_sim)

            if use_business_days:
                # FAST: vectorized instead of per-simulation loop
                end_dates = add_business_days_vectorized(start_date, durations)
            else:
                start_np  = np.datetime64(start_date)
                end_dates = pd.to_datetime(
                    start_np + durations.astype('timedelta64[D]'), errors='coerce')

            commit_85 = pd.Series(end_dates).quantile(0.85)

            st.session_state.last_durations = durations
            st.session_state.last_impact_df = impact_df
            st.session_state.last_commit_85 = commit_85
            st.session_state.last_end_dates = end_dates

            history_df  = load_history(selected_proj)
            diff        = 0
            warning_msg = "✅ First measurement"
            rec         = "Project is on track."

            if not history_df.empty:
                try:
                    last_date = pd.to_datetime(history_df.iloc[-1]['target_date'])
                    diff = (commit_85 - last_date).days
                    if diff > 0:
                        warning_msg = f"⚠️ DEADLINE WARNING: Delay of {diff} days!"
                        rec = "🚨 Inform stakeholders & activate mitigation plan."
                    elif diff < 0:
                        warning_msg = f"✨ POSITIVE TREND: Improvement of {abs(diff)} days."
                except Exception:
                    pass

            top_r = (impact_df.sort_values("Delay", ascending=False).iloc[0]["Source"]
                     if not impact_df.empty else "N/A")

            st.session_state.last_top_r       = top_r
            st.session_state.last_diff        = diff
            st.session_state.last_warning_msg = warning_msg
            st.session_state.last_rec         = rec
            st.session_state.last_history_df  = history_df

            if do_history:
                save_history(selected_proj, commit_85.strftime('%Y-%m-%d'),
                             float(np.mean(durations)), top_r)
                st.session_state.last_history_df = load_history(selected_proj)

            if "⚠️" in warning_msg:
                st.error(warning_msg)
            elif "✨" in warning_msg:
                st.success(warning_msg)
            else:
                st.success(warning_msg)
            st.info(f"**Recommendation:** {rec}")

    except ValueError as e:
        st.error(f"❌ {str(e)}")
    except Exception as e:
        st.error(f"❌ {type(e).__name__}: {str(e)}")
        with st.expander("🔧 Debug"):
            st.code(traceback.format_exc())

# --- RESULTS ---
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["📊 Overview", "🔥 Risk Analysis", "📈 Trends", "📜 Report", "📊 Validation"])

    with tab1:
        st.subheader("Key Metrics & Distribution")
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("🎯 Target Date (P85)", commit_85.strftime('%d.%m.%Y'),
                      delta=f"{diff} days" if history_df is not None and not history_df.empty else None,
                      delta_color="inverse")
        col_m2.metric("📦 Buffer Required (Avg)",
                      f"{int(np.mean(durations) - ed_t['Duration (Days)'].sum())} days")
        col_m3.metric("📊 Min – Max",
                      f"{int(np.min(durations))} – {int(np.max(durations))} days")

        st.divider()
        col_chart, col_top = st.columns([2, 1])

        with col_chart:
            st.subheader("Distribution of Project End Dates")
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Current",
                                       marker_color="#1f77b4", opacity=0.7, nbinsx=40))
            if st.session_state.snapshot_durations is not None:
                ref_ends = pd.to_datetime(
                    start_np + st.session_state.snapshot_durations.astype('timedelta64[D]'),
                    errors='coerce')
                fig.add_trace(go.Histogram(x=ref_ends, name="Reference (old)",
                                           marker_color="#a0a0a0", opacity=0.5, nbinsx=40))
                fig.add_vline(x=st.session_state.snapshot_date.timestamp() * 1000,
                              line_dash="dash", line_color="#707070",
                              annotation_text=f"Ref: {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")
            fig.add_vline(x=commit_85.timestamp() * 1000, line_dash="solid", line_color="#d62728",
                          annotation_text=f"Target: {commit_85.strftime('%d.%m.%Y')}")
            fig.update_layout(template="plotly_white", barmode='overlay', height=450,
                              xaxis_title="End Date", yaxis_title="Frequency")
            st.plotly_chart(fig, use_container_width=True)

        with col_top:
            st.subheader("🔥 Top Risk Driver")
            if st.button("📸 Freeze State", use_container_width=True, key="freeze_snapshot"):
                st.session_state.snapshot_durations = durations.copy()
                st.session_state.snapshot_date      = commit_85
                st.session_state.toast_msg  = "Snapshot saved!"
                st.session_state.toast_type = "success"
                st.rerun()
            st.divider()
            st.markdown(f"### {top_r}")
            if not impact_df.empty:
                top_delay = impact_df.sort_values("Delay", ascending=False).iloc[0]["Delay"]
                st.write(f"**Avg Delay:** {top_delay:.1f} days")

    with tab2:
        st.subheader("Risk Impact Overview")

        if not impact_df.empty:
            # --- Summary metrics ---
            opp_mask    = impact_df["Effect"] == "Opportunity"
            threat_days = impact_df.loc[~opp_mask, "Delay"].sum()
            opp_days    = abs(impact_df.loc[opp_mask, "Delay"].sum())
            net_days    = threat_days - opp_days

            c1, c2, c3 = st.columns(3)
            c1.metric("⚠️ Threat Impact",      f"{threat_days:.1f} days")
            c2.metric("🚀 Opportunity Impact", f"{opp_days:.1f} days")
            c3.metric("📊 Net Impact",          f"{net_days:.1f} days", delta_color="inverse")

            st.divider()

            # --- Tornado chart coloured by Effect ---
            sorted_df = impact_df.sort_values("Delay", ascending=True).copy()
            colors = sorted_df["Effect"].map({
                "Threat":      "#EF553B",   # red
                "Opportunity": "#2ECC71"    # green
            }).fillna("#636EFA")

            fig_tornado = go.Figure(go.Bar(
                x=sorted_df["Delay"],
                y=sorted_df["Source"],
                orientation="h",
                marker_color=colors,
                text=sorted_df["Delay"].apply(lambda v: f"{v:+.1f}d"),
                textposition="outside",
                customdata=sorted_df["Effect"],
                hovertemplate="<b>%{y}</b><br>Delay: %{x:.1f}d<br>Effect: %{customdata}<extra></extra>"
            ))
            fig_tornado.add_vline(x=0, line_color="black", line_width=1)
            fig_tornado.update_layout(
                title="Tornado Chart  🔴 Threat  |  🟢 Opportunity",
                xaxis_title="Avg Delay (days)",
                template="plotly_white",
                height=max(350, len(sorted_df) * 38),
                margin=dict(l=250))
            st.plotly_chart(fig_tornado, use_container_width=True)

            st.divider()

            # --- Separate tables for Threats and Opportunities ---
            col_thr, col_opp = st.columns(2)

            with col_thr:
                st.markdown("### ⚠️ Threats")
                threats_df = (impact_df[~opp_mask]
                              .sort_values("Delay", ascending=False)
                              .copy())
                threats_df["Delay"] = threats_df["Delay"].round(2)
                st.dataframe(threats_df[["Source", "Delay", "Type", "Team"]],
                             use_container_width=True, hide_index=True)

            with col_opp:
                st.markdown("### 🚀 Opportunities")
                opps_df = (impact_df[opp_mask]
                           .sort_values("Delay", ascending=True)
                           .copy())
                opps_df["Delay"] = opps_df["Delay"].round(2)
                if not opps_df.empty:
                    st.dataframe(opps_df[["Source", "Delay", "Type", "Team"]],
                                 use_container_width=True, hide_index=True)
                else:
                    st.info("No opportunities defined.")
        else:
            st.info("ℹ️ No risks defined or simulation not yet run.")

    with tab3:
        st.subheader("📈 Time Series Trend")
        if history_df is not None and not history_df.empty:
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=history_df["timestamp"],
                y=pd.to_datetime(history_df["target_date"]),
                mode="lines+markers", name="Target Date (P85)",
                line=dict(color="#1f77b4", width=2), marker=dict(size=8)))
            fig_trend.update_layout(title="Target Date Development Over Time",
                                    xaxis_title="Measurement Point",
                                    yaxis_title="Target Date",
                                    template="plotly_white", height=400)
            st.plotly_chart(fig_trend, use_container_width=True)

            st.divider()
            dates = pd.to_datetime(history_df["target_date"])
            c1, c2, c3 = st.columns(3)
            c1.metric("📅 Earliest Date",  dates.min().strftime('%d.%m.%Y'))
            c2.metric("📅 Latest Date",    dates.max().strftime('%d.%m.%Y'))
            c3.metric("📊 Total Measurements", len(history_df))

            drift = (dates.iloc[-1] - dates.iloc[0]).days
            st.metric("📈 Total Drift (first vs. last)", f"{drift:+} days", delta_color="inverse")

            st.divider()
            st.subheader("📋 Measurement History")
            st.dataframe(history_df.sort_values("timestamp", ascending=False),
                         use_container_width=True, hide_index=True)
        else:
            st.info("ℹ️ No history data. Run simulation with tracking enabled.")

    with tab4:
        st.subheader("📜 Management Report")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🎯 Target Date (P85)", commit_85.strftime('%d.%m.%Y'))
        c2.metric("⏱️ Avg Duration",      f"{int(np.mean(durations))} days")
        c3.metric("📊 Min / Max",         f"{int(np.min(durations))} / {int(np.max(durations))} days")
        c4.metric("🔁 Simulation Runs",   f"{n_sim:,}")

        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("👥 Teams",  len(tm_curr))
        c2.metric("📋 Tasks",  len(t_curr))
        c3.metric("⚠️ Risks",  len(r_curr))

        if not impact_df.empty:
            opp_mask    = impact_df["Effect"] == "Opportunity"
            threat_days = impact_df.loc[~opp_mask, "Delay"].sum()
            opp_days    = abs(impact_df.loc[opp_mask, "Delay"].sum())
            net_days    = threat_days - opp_days

            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric("⚠️ Threat Impact",      f"{threat_days:.1f} days")
            c2.metric("🚀 Opportunity Impact", f"{opp_days:.1f} days")
            c3.metric("📊 Net Impact",          f"{net_days:.1f} days")

            st.write("**Top 5 Risks:**")
            top5 = (impact_df.sort_values("Delay", ascending=False)
                    .head(5)[["Source", "Delay", "Team", "Effect"]]
                    .reset_index(drop=True))
            top5["Delay"] = top5["Delay"].round(2)
            st.dataframe(top5, use_container_width=True, hide_index=True)

        st.divider()
        st.write("**Percentiles:**")
        pcts     = [50, 70, 80, 85, 90, 95]
        pct_data = []
        for pc in pcts:
            d  = int(np.percentile(durations, pc))
            ed = (add_business_days_with_holidays(start_date, d)
                  if use_business_days
                  else pd.to_datetime(np.datetime64(start_date) + np.timedelta64(d, 'D')))
            pct_data.append({"Percentile": f"P{pc}", "Days": d,
                             "End Date": ed.strftime('%d.%m.%Y')})
        st.dataframe(pd.DataFrame(pct_data), use_container_width=True, hide_index=True)

        st.divider()
        impact_ranking_str = ""
        if not impact_df.empty:
            for _, row in impact_df.sort_values("Delay", ascending=False).iterrows():
                impact_ranking_str += f"- {row['Source']} ({row['Effect']}): ~{row['Delay']:.1f} days\n"

        report_txt = f"""MONTE-CARLO REPORT – {selected_proj}
Created: {datetime.now().strftime('%d.%m.%Y %H:%M')} | Version: {APP_VERSION}
{'='*55}
STATUS:         {warning_msg}
RECOMMENDATION: {rec}

KEY METRICS
  Target Date (P85) : {commit_85.strftime('%d.%m.%Y')}
  Avg Duration      : {int(np.mean(durations))} days
  Buffer Required   : {int(np.mean(durations) - ed_t['Duration (Days)'].sum())} days
  Min / Max         : {int(np.min(durations))} / {int(np.max(durations))} days

RISK DRIVERS
{impact_ranking_str or "  None defined"}

STRUCTURE
  Teams  : {len(tm_curr)}
  Tasks  : {len(t_curr)}
  Risks  : {len(r_curr)}

SIMULATION PARAMETERS
  Runs       : {n_sim:,}
  Start Date : {start_date.strftime('%d.%m.%Y')}
  Std. Risks : {len(selected_std)} / {len(st.session_state.std_risk_df)} active
""".strip()

        st.text_area("Report Preview", report_txt, height=350, disabled=True)
        st.download_button("📥 Download Report (.txt)", report_txt,
                           f"Report_{selected_proj}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                           use_container_width=True)

    with tab5:
        st.subheader("📊 Risk Validation (Backtesting)")
        actual_df = load_actual_results(selected_proj)

        if not actual_df.empty:
            actual_df["planned_days_int"]   = actual_df["planned_duration"].astype(int)
            actual_df["actual_days_int"]    = actual_df["actual_duration"].astype(int)
            actual_df["deviation_pct"]      = (
                (actual_df["actual_duration"] - actual_df["planned_duration"])
                / actual_df["planned_duration"] * 100).round(1)

            # 1. Forecast accuracy
            st.write("### 🎯 P85 Forecast Accuracy")
            correct = sum(1 for _, row in actual_df.iterrows()
                          if row["actual_duration"] <= row["planned_duration"] * 1.15)
            total    = len(actual_df)
            accuracy = (correct / total * 100) if total > 0 else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("✅ Forecast Accuracy", f"{accuracy:.1f}%",
                      delta="Good" if accuracy > 75 else "Review",
                      delta_color="normal" if accuracy > 75 else "inverse")
            c2.metric("📊 Projects Recorded", total)
            c3.metric("✓ Correct Forecasts",  correct)

            # 2. Risks breakdown — Threats vs Opportunities
            st.divider()
            st.write("### 🔥 Occurred Risks — Threat vs Opportunity")

            effect_lookup = _build_effect_lookup(ed_r, selected_std)
            risk_rows = []
            for risks_json in actual_df["risks_occurred"]:
                if not risks_json:
                    continue
                try:
                    items = json.loads(risks_json)
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if isinstance(item, dict):
                            name    = str(item.get("name", "")).strip()
                            raw_eff = str(item.get("effect", "")).strip()
                            eff     = "Opportunity" if raw_eff.casefold() == "opportunity" else \
                                      effect_lookup.get(_norm_risk_name(name), "Threat")
                        else:
                            name = str(item).strip()
                            eff  = effect_lookup.get(_norm_risk_name(name), "Threat")
                        if name:
                            risk_rows.append({"Risk": name, "Effect": eff})
                except json.JSONDecodeError:
                    pass

            if risk_rows:
                risk_df  = pd.DataFrame(risk_rows)
                opp_mask = risk_df["Effect"].str.casefold() == "opportunity"

                c1, c2 = st.columns(2)
                c1.metric("⚠️ Threat Occurrences",      int((~opp_mask).sum()))
                c2.metric("🚀 Opportunity Occurrences", int(opp_mask.sum()))

                risk_counts = (risk_df.groupby(["Risk", "Effect"])
                               .size().reset_index(name="Count")
                               .sort_values("Count", ascending=True))

                colors = risk_counts["Effect"].map({
                    "Threat":      "#EF553B",
                    "Opportunity": "#2ECC71"
                }).fillna("#636EFA")

                fig_risk = go.Figure(go.Bar(
                    x=risk_counts["Count"], y=risk_counts["Risk"],
                    orientation='h', marker_color=colors,
                    text=risk_counts["Count"], textposition='auto',
                    customdata=risk_counts["Effect"],
                    hovertemplate="<b>%{y}</b><br>Count: %{x}<br>Effect: %{customdata}<extra></extra>"))
                fig_risk.update_layout(template="plotly_white", xaxis_title="Frequency",
                                       height=max(300, len(risk_counts) * 40), margin=dict(l=200))
                st.plotly_chart(fig_risk, use_container_width=True)

                # Separate tables
                col_thr, col_opp = st.columns(2)
                with col_thr:
                    st.markdown("#### ⚠️ Threats occurred")
                    thr_tbl = (risk_counts[risk_counts["Effect"] == "Threat"]
                               .sort_values("Count", ascending=False)
                               .reset_index(drop=True))
                    st.dataframe(thr_tbl[["Risk", "Count"]], use_container_width=True, hide_index=True)
                with col_opp:
                    st.markdown("#### 🚀 Opportunities occurred")
                    opp_tbl = (risk_counts[risk_counts["Effect"] == "Opportunity"]
                               .sort_values("Count", ascending=False)
                               .reset_index(drop=True))
                    if not opp_tbl.empty:
                        st.dataframe(opp_tbl[["Risk", "Count"]], use_container_width=True, hide_index=True)
                    else:
                        st.info("No opportunities recorded yet.")
            else:
                st.info("ℹ️ No risks documented yet.")

            # 3. Deviation trend
            st.divider()
            st.write("### 📈 Deviation Trend")
            fig_dev = go.Figure()
            fig_dev.add_trace(go.Scatter(
                x=actual_df.index, y=actual_df["deviation_pct"],
                mode='lines+markers', fill='tozeroy',
                fillcolor='rgba(239,85,59,0.2)',
                line=dict(color='#EF553B', width=3), marker=dict(size=8),
                name='Deviation (%)'))
            fig_dev.add_hline(y=0,  line_dash="dash", line_color="green",
                              annotation_text="Plan",    annotation_position="right")
            fig_dev.add_hline(y=15, line_dash="dot",  line_color="orange",
                              annotation_text="P85 Target", annotation_position="right")
            fig_dev.update_layout(template="plotly_white",
                                  yaxis_title="Deviation (%)", xaxis_title="Project #",
                                  height=400)
            st.plotly_chart(fig_dev, use_container_width=True)

            # 4. Calibration recommendations
            st.divider()
            st.write("### 🔧 Calibration Recommendations")
            avg_dev = actual_df["deviation_pct"].mean()
            std_dev = actual_df["deviation_pct"].std()

            c1, c2 = st.columns(2)
            c1.metric("Avg Deviation", f"{avg_dev:+.1f}%")
            c2.metric("Std Deviation", f"{std_dev:.1f}%")
            st.divider()

            if avg_dev > 15:
                st.warning(
                    f"⚠️ **Too optimistic** (avg {avg_dev:+.1f}%)\n\n"
                    "**Recommendation:**\n"
                    "- Increase standard risk parameters by ~10–15%\n"
                    "- Widen impact ranges for continuous risks\n"
                    "- Check if new standard risks are missing")
            elif avg_dev < -10:
                st.info(
                    f"ℹ️ **Too pessimistic** (avg {avg_dev:.1f}%)\n\n"
                    "**Recommendation:**\n"
                    "- Reduce standard risk parameters by ~5–10%\n"
                    "- Review probabilities (are they too high?)\n"
                    "- Deactivate non-relevant risks")
            else:
                st.success(
                    f"✅ **Well calibrated** (avg {avg_dev:+.1f}%)\n\n"
                    "Your risk parameters are well calibrated. Review monthly.")

            # 5. Detail table
            st.divider()
            st.write("### 📋 All Recorded Project Results")
            disp = actual_df[[
                "start_date", "planned_end_date", "actual_end_date",
                "planned_days_int", "actual_days_int", "deviation_pct", "notes"
            ]].copy()
            disp.columns = ["Start", "Planned", "Actual",
                            "Plan (days)", "Actual (days)", "Dev. (%)", "Notes"]
            st.dataframe(disp, use_container_width=True, hide_index=True)

        else:
            st.info(
                "ℹ️ **No project results recorded yet.**\n\n"
                "**How backtesting works:**\n"
                "1. Fill in the form below after project completion\n"
                "2. Collect at least 3–5 projects\n"
                "3. This tab will automatically show validation results\n\n"
                "With real data you can continuously improve your risk parameters!")

    # --- RECORD PROJECT PROGRESS ---
    st.divider()
    with st.expander("✅ Record Project Progress", expanded=False):

        mode = st.radio("Entry Type:",
                        ["📅 Interim Status (ongoing)", "🏁 Project Completion (final)"],
                        horizontal=True, key="erfassungs_modus")

        actual_start = st.date_input("Start Date", start_date, key="proj_actual_start")

        if mode == "📅 Interim Status (ongoing)":
            st.subheader("📅 Interim Status")
            col_zw1, col_zw2 = st.columns(2)
            with col_zw1:
                stand_datum = st.date_input("Status Date (today)", datetime.now(), key="stand_datum")
            with col_zw2:
                fertig_prozent = st.slider("Completion (%)", 0, 100, 50, key="fertig_prozent")

            tatsaechliche_tage_bisher = (stand_datum - actual_start).days
            geplante_tage_bisher      = ed_t["Duration (Days)"].sum() * (fertig_prozent / 100)

            col_i1, col_i2 = st.columns(2)
            with col_i1:
                st.metric("Days elapsed (actual)", tatsaechliche_tage_bisher)
            with col_i2:
                if geplante_tage_bisher > 0:
                    velocity = geplante_tage_bisher / max(tatsaechliche_tage_bisher, 1)
                    st.metric("Velocity (Plan/Actual)", f"{velocity:.2f}",
                              delta="On track" if velocity >= 0.9 else "Delayed",
                              delta_color="normal" if velocity >= 0.9 else "inverse")

            risiken_jetzt = st.multiselect(
                "Currently active / occurred risks:",
                options=ed_r["Risk Name"].tolist() + [sr["name"] for sr in selected_std],
                key="risiken_zwischenstand")
            notiz_zwischen = st.text_area("Note", key="notiz_zwischen", height=80)

            if st.button("💾 Save Interim Status", use_container_width=True):
                try:
                    velocity = (geplante_tage_bisher / max(tatsaechliche_tage_bisher, 1)
                                if geplante_tage_bisher > 0 else 0)
                    projected_total_days = (int(ed_t["Duration (Days)"].sum() / velocity)
                                            if velocity > 0 else int(ed_t["Duration (Days)"].sum() * 2))
                    projected_end = actual_start + pd.Timedelta(days=projected_total_days)

                    c85 = st.session_state.last_commit_85
                    planned_end_str = (c85.strftime('%Y-%m-%d')
                                       if c85 is not None and hasattr(c85, 'strftime')
                                       else str(projected_end.date()
                                                if hasattr(projected_end, 'date')
                                                else projected_end))

                    eff_lkp       = _build_effect_lookup(ed_r, selected_std)
                    risks_payload = [{"name": str(rn).strip(),
                                      "effect": eff_lkp.get(_norm_risk_name(rn), "Threat")}
                                     for rn in risiken_jetzt]

                    save_actual_result(
                        selected_proj, str(actual_start), planned_end_str,
                        str(projected_end.date() if hasattr(projected_end, 'date') else projected_end),
                        str(st.session_state.last_top_r) if st.session_state.last_top_r else "N/A",
                        risks_payload,
                        f"[INTERIM {fertig_prozent}%] {notiz_zwischen}")
                    st.session_state.toast_msg  = f"Interim status ({fertig_prozent}%) saved!"
                    st.session_state.toast_type = "success"
                    st.rerun()
                except Exception as e:
                    st.session_state.toast_msg  = f"Error: {str(e)}"
                    st.session_state.toast_type = "error"
                    st.rerun()

        else:
            st.subheader("🏁 Project Completion")
            actual_end_final = st.date_input("Actual End Date", key="actual_end_final")
            all_risk_options = ed_r["Risk Name"].tolist() + [sr["name"] for sr in selected_std]
            risks_that_occurred = st.multiselect(
                "Risks that occurred (total):",
                options=all_risk_options if all_risk_options else ["None"],
                key="risks_occurred_final")
            actual_notes = st.text_area("Lessons Learned", key="actual_notes_final", height=100)

            if st.button("💾 Save Project Completion", use_container_width=True, type="primary"):
                try:
                    c85 = st.session_state.last_commit_85
                    planned_end_str = (c85.strftime('%Y-%m-%d')
                                       if c85 is not None and hasattr(c85, 'strftime')
                                       else str(actual_end_final))

                    eff_lkp       = _build_effect_lookup(ed_r, selected_std)
                    risks_payload = [{"name": str(rn).strip(),
                                      "effect": eff_lkp.get(_norm_risk_name(rn), "Threat")}
                                     for rn in risks_that_occurred]

                    save_actual_result(
                        selected_proj, str(actual_start), planned_end_str,
                        str(actual_end_final),
                        str(st.session_state.last_top_r) if st.session_state.last_top_r else "N/A",
                        risks_payload,
                        f"[COMPLETION] {actual_notes}")
                    st.session_state.toast_msg  = "Project completion saved!"
                    st.session_state.toast_type = "success"
                    st.rerun()
                except Exception as e:
                    st.session_state.toast_msg  = f"Save error: {str(e)}"
                    st.session_state.toast_type = "error"
                    st.rerun()