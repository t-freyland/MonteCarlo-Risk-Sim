import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection

# --- 1. KONFIGURATION & PROJEKTLISTE ---
APP_VERSION = "1.9.4"
PROJECTS = ["Projekt_Alpha", "Projekt_Beta", "Projekt_Gamma"] 

st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. LOGIN-LOGIK ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

def check_auth():
    u, p = st.session_state.get("login_user", ""), st.session_state.get("login_pw", "")
    if "credentials" in st.secrets:
        if u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
            st.session_state["auth_ok"] = True
        else: st.session_state["login_error"] = True

# Falls NICHT eingeloggt: Nur Login-Maske anzeigen, KEINE Sidebar
if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    col1, col2 = st.columns([1, 1])
    with col1:
        st.text_input("Benutzername", key="login_user")
        st.text_input("Passwort", type="password", key="login_pw", on_change=check_auth)
        st.button("Anmelden", on_click=check_auth, use_container_width=True)
        if st.session_state.get("login_error"):
            st.error("🚫 Login fehlgeschlagen.")
            st.session_state["login_error"] = False
    st.stop() # HIER stoppt das Skript für nicht eingeloggte User komplett

# --- AB HIER: NUR FÜR EINGELOGGTE USER ---

# --- 3. DATEN-HANDLING ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_df(df, type="task"):
    if df is None or df.empty:
        if type == "task":
            return pd.DataFrame(columns=["Task Name", "Duration (Days)", "Beschreibung"])
        else:
            return pd.DataFrame(columns=["Risk Name", "Risk Type", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Maßnahme / Mitigation"])
    
    df.columns = [str(c).strip() for c in df.columns]
    num_cols = ["Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Duration (Days)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    
    text_cols = ["Task Name", "Risk Name", "Risk Type", "Target (Global/Task)", "Beschreibung", "Maßnahme / Mitigation"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', '0', '0.0'], '')
    return df

# --- 4. SIDEBAR (JETZT GESCHÜTZT) ---
if "snapshot_durations" not in st.session_state:
    st.session_state.snapshot_durations = None
    st.session_state.snapshot_date = None

with st.sidebar:
    st.header("📂 Projekt-Management")
    selected_proj = st.selectbox("Aktives Projekt:", PROJECTS)
    
    st.divider()
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.subheader("🏢 Globale Standards")
    STANDARD_DEFS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_DEFS if st.checkbox(sr["name"], value=True)]
    
    st.divider()
    if st.button("📸 Stand einfrieren"):
        if "last_durations" in st.session_state:
            st.session_state.snapshot_durations = st.session_state.last_durations
            st.session_state.snapshot_date = st.session_state.last_commit_85
            st.success("Referenz gespeichert!")
    
    if st.button("🗑️ Reset Vergleich"):
        st.session_state.snapshot_durations = None
        st.session_state.snapshot_date = None
        st.rerun()
    
    st.divider()
    if st.button("🚪 Abmelden"):
        st.session_state.clear()
        st.rerun()

# --- 5. DATEN LADEN ---
t_sheet = f"{selected_proj}_Tasks"
r_sheet = f"{selected_proj}_Risks"

try:
    t_raw = conn.read(worksheet=t_sheet, ttl=0)
    r_raw = conn.read(worksheet=r_sheet, ttl=0)
    tasks_data = clean_df(t_raw, "task")
    risks_data = clean_df(r_raw, "risk")
except Exception:
    st.warning(f"⚠️ Tabs '{t_sheet}' oder '{r_sheet}' nicht gefunden!")
    tasks_data = pd.DataFrame([{"Task Name": "Beispiel-Task", "Duration (Days)": 10.0, "Beschreibung": ""}], columns=["Task Name", "Duration (Days)", "Beschreibung"])
    risks_data = pd.DataFrame(columns=["Risk Name", "Risk Type", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Maßnahme / Mitigation"])

# --- 6. SIMULATIONS-KERN ---
def run_fast_simulation(tasks, risks, std_risks, n):
    task_durations = np.tile(tasks["Duration (Days)"].values, (n, 1)).astype(float)
    base_total = tasks["Duration (Days)"].sum()
    
    for _, r in risks.iterrows():
        try:
            p = float(r.get("Probability (0-1)", 0))
            if p <= 0: continue
            vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
            target = str(r.get("Target (Global/Task)", "Global")).strip()
            r_type = str(r.get("Risk Type", "Binär")).strip()
            
            hits = np.random.random(n) < p
            impacts = np.random.triangular(vals[0], vals[1], max(vals[1] + 0.001, vals[2]), n)
            
            if target.lower() == "global":
                if r_type == "Kontinuierlich": task_durations *= (1 + (hits * impacts))
                else: task_durations = np.hstack([task_durations, (hits * impacts * base_total).reshape(-1, 1)])
            else:
                idx = tasks.index[tasks["Task Name"] == target]
                if not idx.empty:
                    i = idx[0]
                    if r_type == "Kontinuierlich": task_durations[:, i] *= (1 + (hits * impacts))
                    else: task_durations[:, i] += (hits * impacts * tasks.iloc[i]["Duration (Days)"])
        except: continue

    total_days = task_durations.sum(axis=1)
    for sr in std_risks:
        total_days *= (1 + ((np.random.random(n) < sr["prob"]) * np.random.triangular(sr["min"], sr["likely"], sr["max"], n)))
    return total_days.astype(int)

# --- 7. INPUT (MIT ABSOLUTEM SYNC-FIX) ---
st.title(f"🎲 Risk Sim Pro v{APP_VERSION} | {selected_proj}")

with st.form("main_form"):
    st.subheader("📋 Projektstruktur")
    # Der Editor erhält die Daten. Wichtig: Wir nutzen ed_tasks danach nur zum Speichern.
    ed_tasks = st.data_editor(tasks_data, use_container_width=True, num_rows="dynamic", key=f"t_{selected_proj}")
    
    st.divider()
    st.subheader("⚠️ Risiko-Register")
    t_names = ["Global"] + [str(x).strip() for x in ed_tasks["Task Name"].dropna() if str(x).strip() != ""]
    
    risk_config = {
        "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], required=True),
        "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_names, required=True),
        "Maßnahme / Mitigation": st.column_config.TextColumn(width="large")
    }
    ed_risks = st.data_editor(risks_data, use_container_width=True, num_rows="dynamic", key=f"r_{selected_proj}", column_config=risk_config)
    
    if st.form_submit_button(f"💾 {selected_proj} speichern"):
        # SCHRITT 1: Daten aus dem Editor-Objekt extrahieren
        # (Dies fängt auch Änderungen ab, die noch "gelb" markiert sind)
        tasks_to_save = clean_df(ed_tasks, "task")
        risks_to_save = clean_df(ed_risks, "risk")
        
        try:
            # SCHRITT 2: Cloud-Update
            conn.update(worksheet=t_sheet, data=tasks_to_save)
            conn.update(worksheet=r_sheet, data=risks_to_save)
            
            # SCHRITT 3: Cache leeren, damit beim nächsten Laden frische Daten kommen
            st.cache_data.clear()
            
            # SCHRITT 4: Feedback geben
            st.success(f"✅ Daten für {selected_proj} gespeichert! Seite wird aktualisiert...")
            
            # SCHRITT 5: Kurze Pause und Rerun (außerhalb des Form-States)
            st.rerun()
            
        except Exception as e:
            st.error(f"Fehler beim Speichern: {e}")

# --- 8. AUSWERTUNG ---
if st.button("🚀 Simulation starten"):
    if ed_tasks["Duration (Days)"].sum() <= 0: st.warning("Keine Dauer definiert.")
    else:
        with st.spinner(f"Analysiere {selected_proj}..."):
            durations = run_fast_simulation(ed_tasks, ed_risks, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            st.session_state.last_durations = durations
            st.session_state.last_commit_85 = commit_85

            if st.session_state.snapshot_date is not None:
                delta = (commit_85 - st.session_state.snapshot_date).days
                status = "später" if delta > 0 else "früher"
                st.info(f"🔄 **Szenarien-Vergleich:** Projekt rückt um **{abs(delta)} Tage nach {status}**.")

            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Aktuell", marker_color="#1f77b4", opacity=0.7))
            if st.session_state.snapshot_durations is not None:
                ref_ends = pd.to_datetime(np.busday_offset(start_np, st.session_state.snapshot_durations, roll='forward'))
                fig.add_trace(go.Histogram(x=ref_ends, name="Referenz", marker_color="#7f7f7f", opacity=0.3))
                fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, line_dash="dash", line_color="#7f7f7f")
            
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red")
            fig.update_layout(barmode='overlay', template="plotly_white", margin=dict(t=20))
            st.plotly_chart(fig, use_container_width=True)

            m1, m2 = st.columns(2)
            m1.metric("Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'))
            m2.metric("Ø Dauer", f"{int(np.mean(durations))} Tage")