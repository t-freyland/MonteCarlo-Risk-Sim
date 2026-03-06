import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection

# --- 1. KONFIGURATION ---
APP_VERSION = "1.8.8"
st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. LOGIN ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

def check_auth():
    u, p = st.session_state.get("login_user", ""), st.session_state.get("login_pw", "")
    if "credentials" in st.secrets:
        if u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
            st.session_state["auth_ok"] = True
        else: st.session_state["login_error"] = True

if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    st.text_input("Benutzername", key="login_user")
    st.text_input("Passwort", type="password", key="login_pw", on_change=check_auth)
    st.button("Anmelden", on_click=check_auth, use_container_width=True)
    if st.session_state.get("login_error"):
        st.error("🚫 Login fehlgeschlagen.")
        st.session_state["login_error"] = False
    st.stop()

# --- 3. DATEN-HANDLING ---
conn = st.connection("gsheets", type=GSheetsConnection)

def clean_df(df, type="task"):
    df.columns = [str(c).strip() for c in df.columns]
    num_cols = ["Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max", "Duration (Days)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
    
    if type == "risk":
        if "Risk Type" not in df.columns: df["Risk Type"] = "Binär"
    
    for col in ["Risk Name", "Risk Type", "Target (Global/Task)", "Beschreibung", "Maßnahme / Mitigation"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', '0', '0.0'], '')
    return df

if "data_loaded" not in st.session_state:
    try:
        t_raw = conn.read(worksheet="Tasks", ttl=0)
        r_raw = conn.read(worksheet="Risks", ttl=0)
        st.session_state.tasks = clean_df(t_raw, "task")
        st.session_state.risks = clean_df(r_raw, "risk")
        st.session_state.data_loaded = True
    except:
        st.session_state.tasks = pd.DataFrame({"Task Name": ["Entwicklung"], "Duration (Days)": [20.0]})
        st.session_state.risks = pd.DataFrame(columns=["Risk Name", "Risk Type", "Target (Global/Task)", "Probability (0-1)", "Impact Min", "Impact Likely", "Impact Max"])
        st.session_state.data_loaded = True

# --- 4. SIMULATIONS-KERN ---
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
                if r_type == "Kontinuierlich":
                    task_durations *= (1 + (hits * impacts))
                else:
                    # Binär Global wirkt additiv auf das Gesamtprojekt (basierend auf Projektgröße)
                    extra = (hits * impacts * base_total).reshape(-1, 1)
                    task_durations = np.hstack([task_durations, extra])
            else:
                idx = tasks.index[tasks["Task Name"] == target]
                if not idx.empty:
                    i = idx[0]
                    if r_type == "Kontinuierlich":
                        task_durations[:, i] *= (1 + (hits * impacts))
                    else:
                        task_durations[:, i] += (hits * impacts * tasks.iloc[i]["Duration (Days)"])
        except: continue

    total_days = task_durations.sum(axis=1)
    for sr in std_risks:
        hits = np.random.random(n) < sr["prob"]
        impacts = np.random.triangular(sr["min"], sr["likely"], sr["max"], n)
        total_days *= (1 + (hits * impacts))
    return total_days.astype(int)

# --- 5. SIDEBAR ---
if "snapshot_durations" not in st.session_state:
    st.session_state.snapshot_durations = None
    st.session_state.snapshot_date = None

with st.sidebar:
    st.header("⚙️ Einstellungen")
    start_date = st.date_input("Projekt Startdatum", datetime.now())
    n_sim = st.number_input("Simulationen", 1000, 50000, 10000, 1000)
    
    st.subheader("🏢 Standard Risiken")
    STANDARD_DEFS = [
        {"name": "Schätz-Ungenauigkeit", "prob": 0.90, "min": -0.05, "likely": 0.05, "max": 0.15},
        {"name": "Scope Creep", "prob": 0.60, "min": 0.10, "likely": 0.25, "max": 0.50},
        {"name": "Unklare Anforderungen", "prob": 0.40, "min": 0.10, "likely": 0.20, "max": 0.40},
        {"name": "Technische Schulden", "prob": 0.30, "min": 0.05, "likely": 0.10, "max": 0.20},
    ]
    selected_std = [sr for sr in STANDARD_DEFS if st.checkbox(sr["name"], value=True)]
    
    st.divider()
    st.subheader("📸 Szenarien")
    if st.button("Aktuellen Stand einfrieren"):
        if "last_durations" in st.session_state:
            st.session_state.snapshot_durations = st.session_state.last_durations
            st.session_state.snapshot_date = st.session_state.last_commit_85
            st.success("Referenz gespeichert!")
    if st.button("🗑️ Vergleich löschen"):
        st.session_state.snapshot_durations = None
        st.session_state.snapshot_date = None
        st.rerun()

# --- 6. INPUT ---
st.title(f"🎲 Risk Sim Pro v{APP_VERSION}")

with st.form("main_form"):
    st.subheader("📋 1. Projektstruktur")
    ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="t_edit")
    
    st.divider()
    st.subheader("⚠️ 2. Risiko-Register")
    t_names = ["Global"] + [str(x).strip() for x in ed_tasks["Task Name"].dropna() if str(x).strip() != ""]
    
    risk_config = {
        "Risk Type": st.column_config.SelectboxColumn("Logik", options=["Binär", "Kontinuierlich"], width="small", required=True),
        "Target (Global/Task)": st.column_config.SelectboxColumn("Fokus", options=t_names, width="medium", required=True),
        "Probability (0-1)": st.column_config.NumberColumn("Wahrsch.", min_value=0.0, max_value=1.0, format="%.2f"),
        "Impact Min": st.column_config.NumberColumn("Min (%)"),
        "Impact Likely": st.column_config.NumberColumn("Likely (%)"),
        "Impact Max": st.column_config.NumberColumn("Max (%)")
    }
    ed_risks = st.data_editor(st.session_state.risks, use_container_width=True, num_rows="dynamic", key="r_edit", column_config=risk_config)
    
    if st.form_submit_button("💾 Speichern & Cloud-Sync"):
        st.session_state.tasks, st.session_state.risks = clean_df(ed_tasks, "task"), clean_df(ed_risks, "risk")
        conn.update(worksheet="Tasks", data=st.session_state.tasks)
        conn.update(worksheet="Risks", data=st.session_state.risks)
        st.success("Daten erfolgreich synchronisiert!")

# --- 7. AUSWERTUNG ---
if st.button("🚀 Simulation starten"):
    tasks_df = st.session_state.tasks
    if tasks_df["Duration (Days)"].sum() <= 0: st.warning("Keine Dauer definiert.")
    else:
        with st.spinner("Berechne Monte-Carlo Szenarien..."):
            durations = run_fast_simulation(tasks_df, st.session_state.risks, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            end_dates = pd.to_datetime(np.busday_offset(start_np, durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            st.session_state.last_durations = durations
            st.session_state.last_commit_85 = commit_85

            # Text-Message Vergleich
            if st.session_state.snapshot_date is not None:
                delta = (commit_85 - st.session_state.snapshot_date).days
                status = "später" if delta > 0 else "früher"
                st.info(f"🔄 **Szenarien-Vergleich:** Das 85%-Ziel verschiebt sich um **{abs(delta)} Tage {status}** gegenüber der Referenz.")

            # Grafik mit beiden Linien
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Aktuelle Planung", marker_color="#1f77b4", opacity=0.7))
            if st.session_state.snapshot_durations is not None:
                ref_ends = pd.to_datetime(np.busday_offset(start_np, st.session_state.snapshot_durations, roll='forward'))
                fig.add_trace(go.Histogram(x=ref_ends, name="Referenz", marker_color="#7f7f7f", opacity=0.3))
                fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, line_dash="dash", line_color="#7f7f7f", annotation_text="Ref 85%")
            
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red", annotation_text="Aktuell 85%")
            fig.update_layout(barmode='overlay', template="plotly_white", margin=dict(t=50))
            st.plotly_chart(fig, use_container_width=True)

            # Metriken
            m1, m2, m3 = st.columns(3)
            m1.metric("Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'))
            m2.metric("Durchschnitt", f"{int(np.mean(durations))} Tage")
            m3.metric("Worst-Case (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))