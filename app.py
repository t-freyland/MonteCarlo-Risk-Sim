import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection

# --- 1. KONFIGURATION & VERSION ---
APP_VERSION = "1.7.3"
st.set_page_config(page_title=f"Risk Sim Pro v{APP_VERSION}", layout="wide")

# --- 2. LOGIN (v1.7.6 - ULTIMATIVER NON-FORM FIX) ---
if "auth_ok" not in st.session_state:
    st.session_state["auth_ok"] = False

def check_auth():
    """Wird sofort ausgeführt, wenn Enter im Passwortfeld gedrückt wird"""
    u = st.session_state.get("login_user", "")
    p = st.session_state.get("login_pw", "")
    if u == st.secrets["credentials"]["username"] and \
       p == st.secrets["credentials"]["password"]:
        st.session_state["auth_ok"] = True
    else:
        st.session_state["login_error"] = True

if not st.session_state["auth_ok"]:
    st.title("🔐 Login")
    
    # Zwei saubere Felder ohne Form-Einschränkung
    st.text_input("Benutzername", key="login_user")
    
    # on_change sorgt dafür, dass ENTER hier sofort check_auth auslöst
    st.text_input("Passwort", type="password", key="login_pw", on_change=check_auth)
    
    # Der Button macht das gleiche für Klick-Fans
    st.button("Anmelden", on_click=check_auth, use_container_width=True)
    
    if st.session_state.get("login_error"):
        st.error("🚫 Benutzername oder Passwort falsch.")
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
    if type == "task" and "Beschreibung" not in df.columns: df["Beschreibung"] = ""
    if type == "risk" and "Maßnahme / Mitigation" not in df.columns: df["Maßnahme / Mitigation"] = ""
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

# --- 4. SIMULATION ---
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

# --- 5. SIDEBAR ---
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

# --- 6. INPUT ---
st.title(f"🎲 Risk Sim Pro v{APP_VERSION}")
with st.form("main_form"):
    st.subheader("📋 1. Aufgaben & Details")
    task_cfg = {"Beschreibung": st.column_config.TextColumn(width="large")}
    ed_tasks = st.data_editor(st.session_state.tasks, use_container_width=True, num_rows="dynamic", key="t_edit", column_config=task_cfg)
    st.divider()
    st.subheader("⚠️ 2. Risiken & Mitigation")
    t_opts = ["Global"] + ([str(n) for n in ed_tasks["Task Name"].dropna() if str(n).strip() != ""] if "Task Name" in ed_tasks.columns else [])
    risk_cfg = {
        "Target (Global/Task)": st.column_config.SelectboxColumn("Geltungsbereich", options=t_opts, width="medium"),
        "Maßnahme / Mitigation": st.column_config.TextColumn("Strategie", width="large")
    }
    ed_risks = st.data_editor(st.session_state.risks, use_container_width=True, num_rows="dynamic", key="r_edit", column_config=risk_cfg)
    if st.form_submit_button("💾 Strategie speichern"):
        st.session_state.tasks, st.session_state.risks = clean_df(ed_tasks, "task"), clean_df(ed_risks, "risk")
        try:
            conn.update(worksheet="Tasks", data=st.session_state.tasks)
            conn.update(worksheet="Risks", data=st.session_state.risks)
            st.success("✅ Gespeichert!")
            st.cache_data.clear()
        except Exception as e: st.error(f"Fehler: {e}")

# --- 7. REPORTING ---
if st.button("🚀 Simulation starten"):
    tasks_df, risks_df = st.session_state.tasks, st.session_state.risks
    base_days = tasks_df["Duration (Days)"].sum()
    if base_days <= 0: st.warning("Daten fehlen.")
    else:
        with st.spinner("Monte Carlo läuft..."):
            durations = run_fast_simulation(tasks_df, risks_df, selected_std, n_sim)
            end_dates = pd.to_datetime(np.busday_offset(np.datetime64(start_date), durations, roll='forward'))
            commit_85 = pd.Series(end_dates).quantile(0.85)
            
            # Histogramm
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=end_dates, name="Verteilung", marker_color="#1f77b4", opacity=0.7))
            s_dates = np.sort(end_dates)
            fig.add_trace(go.Scatter(x=s_dates, y=np.linspace(0, 100, n_sim), name="Sicherheit", line=dict(color='orange', width=3), yaxis="y2"))
            fig.update_layout(yaxis2=dict(overlaying="y", side="right", range=[0, 100]), template="plotly_white")
            fig.add_vline(x=commit_85.timestamp()*1000, line_dash="dash", line_color="red")
            st.plotly_chart(fig, use_container_width=True)

            # Tornado
            st.divider()
            st.subheader("🎯 Verzögerungs-Treiber")
            contrib = []
            for _, r in risks_df.iterrows():
                p = float(r.get("Probability (0-1)", 0))
                if p > 0:
                    avg_i = (float(r.get("Impact Min", 0)) + float(r.get("Impact Likely", 0)) + float(r.get("Impact Max", 0))) / 3
                    target = str(r.get("Target (Global/Task)", "Global")).strip()
                    ref = base_days if target.lower() == "global" else tasks_df[tasks_df["Task Name"].astype(str).str.strip() == target]["Duration (Days)"].values[0]
                    val = p * avg_i * ref
                    if val > 0: contrib.append({"Quelle": r.get("Risk Name", "Unbekannt"), "Tage": round(val, 1), "Art": "Projekt"})
            for sr in selected_std:
                val = sr["prob"] * ((sr["min"] + sr["likely"] + sr["max"]) / 3) * base_days
                contrib.append({"Quelle": sr["name"], "Tage": round(val, 1), "Art": "Standard"})

            if contrib:
                df_c = pd.DataFrame(contrib).sort_values(by="Tage", ascending=True)
                colors = ['#EF553B' if a == "Projekt" else '#636EFA' for a in df_c["Art"]]
                fig_c = go.Figure(go.Bar(x=df_c["Tage"], y=df_c["Quelle"], orientation='h', marker_color=colors, text=df_c["Tage"].astype(str) + " Tage", textposition='auto'))
                fig_c.update_layout(template="plotly_white", height=max(300, len(contrib)*50), margin=dict(l=250, r=50, t=20, b=20), yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_c, use_container_width=True)

            st.divider()
            m1, m2, m3 = st.columns(3)
            m1.metric("📅 85% Sicherheit", commit_85.strftime('%d.%m.%Y'))
            m2.metric("⏱️ Ø Dauer", f"{int(np.mean(durations))} Tage")
            m3.metric("📉 Pessimistisch (95%)", pd.Series(end_dates).quantile(0.95).strftime('%d.%m.%Y'))