import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection

# --- 1. KONFIGURATION & VERSION ---
APP_VERSION = "1.8.2"
st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. LOGIN (v1.7.6 STABLE NON-FORM) ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

def check_auth():
    u = st.session_state.get("login_user", "")
    p = st.session_state.get("login_pw", "")
    if "credentials" in st.secrets:
        if u == st.secrets["credentials"]["username"] and \
           p == st.secrets["credentials"]["password"]:
            st.session_state["auth_ok"] = True
        else:
            st.session_state["login_error"] = True

if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    if "credentials" not in st.secrets:
        st.error("Secrets fehlen!")
        st.stop()
    st.text_input("Benutzername", key="login_user")
    st.text_input("Passwort", type="password", key="login_pw", on_change=check_auth)
    st.button("Anmelden", on_click=check_auth, use_container_width=True)
    if st.session_state.get("login_error"):
        st.error("🚫 Login fehlgeschlagen.")
        st.session_state["login_error"] = False
    st.stop()

# --- 3. DATEN-HANDLING MIT TYP-FIX ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_df(df, type="task"):
    df.columns = [str(c).strip() for c in df.columns]
    
    # Numerische Spalten fixen
    num_cols = ["Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Duration (Days)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    
    # TEXT-FIX: Sicherstellen, dass Beschreibungs-Spalten Strings sind (verhindert den FLOAT Fehler)
    if type == "task":
        if "Beschreibung" not in df.columns: df["Beschreibung"] = ""
        df["Beschreibung"] = df["Beschreibung"].astype(str).replace(['nan', 'None', '0', '0.0'], '')
    
    if type == "risk":
        if "Maßnahme / Mitigation" not in df.columns: df["Maßnahme / Mitigation"] = ""
        df["Maßnahme / Mitigation"] = df["Maßnahme / Mitigation"].astype(str).replace(['nan', 'None', '0', '0.0'], '')
    
    return df

if "data_loaded" not in st.session_state:
    try:
        t_raw = conn.read(worksheet="Tasks", ttl=0)
        r_raw = conn.read(worksheet="Risks", ttl=0)
        st.session_state.tasks = clean_df(t_raw, "task")
        st.session_state.risks = clean_df(r_raw, "risk")
        st.session_state.data_loaded = True
    except:
        st.session_state.tasks = pd.DataFrame({"Task Name": ["Start"], "Duration (Days)": [10.0], "Beschreibung": [""]})
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
                vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
                imin, ilikely, imax = vals
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

# --- 5. SIDEBAR & SZENARIEN ---
if "snapshot_durations" not in st.session_state:
    st.session_state.snapshot_durations = None
    st.session_state.snapshot_date = None

with st.sidebar:
    st.header("👤 Setup")
    if st.button("Abmelden"):
        st.session_state.clear()
        st.rerun()
    st.divider()
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.subheader("🏢 Standards")
    STANDARD_RISKS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_RISKS if st.checkbox(sr["name"], value=True)]
    
    st.divider()
    st.subheader("📸 Szenarien-Management")
    if st.button("Aktuellen Stand einfrieren"):
        if "last_durations" in st.session_state:
            st.session_state.snapshot_durations = st.session_state.last_durations
            st.session_state.snapshot_date = st.session_state.last_commit_85
            st.success("Referenz gespeichert!")
        else: st.warning("Bitte erst Simulation starten.")
    
    if st.button("🗑️ Vergleich löschen"):
        st.session_state.snapshot_durations = None
        st.session_state.snapshot_date = None
        st.rerun()

# --- 6. INPUT ---
st.title(f"🎲 Risk Sim Pro v{APP_VERSION}")

with st.form("main_form"):
    st.subheader("📋 1. Aufgaben & Beschreibungen")
    task_cfg = {
        "Beschreibung": st.column_config.TextColumn("Beschreibung", width="large"),
        "Duration (Days)": st.column_config.NumberColumn("Dauer (Tage)", format="%d")
    }
    ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="t_edit", column_config=task_cfg)
    
    st.divider()
    
    st.subheader("⚠️ 2. Risiken & Mitigation")
    t_opts = ["Global"] + ([str(n) for n in ed_tasks["Task Name"].dropna() if str(n).strip() != ""] if "Task Name" in ed_tasks.columns else [])
    risk_cfg = {
        "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_opts, width="medium"),
        "Maßnahme / Mitigation": st.column_config.TextColumn("Maßnahme", width="large")
    }
    ed_risks = st.data_editor(st.session_state.risks, use_container_width=True, num_rows="dynamic", key="r_edit", column_config=risk_cfg)
    
    # SUBMIT BUTTON innerhalb des Formulars (behebt die rote Warnung)
    if st.form_submit_button("💾 Strategie & Daten speichern"):
        st.session_state.tasks, st.session_state.risks = clean_df(ed_tasks, "task"), clean_df(ed_risks, "risk")
        try:
            conn.update(worksheet="Tasks", data=st.session_state.tasks)
            conn.update(worksheet="Risks", data=st.session_state.risks)
            st.success("✅ Cloud-Sync erfolgreich!")
            st.cache_data.clear()
        except Exception as e: st.error(f"Fehler: {e}")

# --- 7. REPORTING ---
if st.button("🚀 Simulation starten"):
    tasks_df, risks_df = st.session_state.tasks, st.session_state.risks
    base_days = tasks_df["Duration (Days)"].sum()
    if base_days <= 0: st.warning("Keine Aufgaben definiert.")
    else:
        with st.spinner("Simulation läuft..."):
            durations = run_fast_simulation(tasks_df, risks_df, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            
            st.session_state.last_durations = durations
            st.session_state.last_commit_85 = commit_85

            # Grafiken
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Aktuelle Planung", marker_color="#1f77b4", opacity=0.6))
            if st.session_state.snapshot_durations is not None:
                ref_end_dates = pd.to_datetime(np.busday_offset(start_np, st.session_state.snapshot_durations, roll='forward'))
                fig.add_trace(go.Histogram(x=ref_end_dates, name="Referenz", marker_color="#7f7f7f", opacity=0.3))
                fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, line_dash="dash", line_color="#7f7f7f")

            s_dates = np.sort(end_dates)
            fig.add_trace(go.Scatter(x=s_dates, y=np.linspace(0, 100, n_sim), name="Sicherheit (%)", line=dict(color='orange', width=3), yaxis="y2"))
            fig.update_layout(barmode='overlay', template="plotly_white", yaxis2=dict(overlaying="y", side="right", range=[0, 100]))
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red")
            st.plotly_chart(fig, use_container_width=True)

            # Tornado & Metriken (gekürzt zur Übersicht)
            st.divider()
            m1, m2, m3 = st.columns(3)
            m1.metric("📅 85% Sicherheit", commit_85.strftime('%d.%m.%Y'))
            m2.metric("⏱️ Ø Projektdauer", f"{int(np.mean(durations))} Tage")
            m3.metric("📉 Pessimistisch (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))