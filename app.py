import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta
import plotly.graph_objects as go

# --- 1. SETUP & CONFIG ---
APP_VERSION = "2.2.0 (Fixed & Optimized)"
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
    conn.execute("CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, timestamp TEXT, target_date TEXT, buffer REAL, top_risk TEXT)")
    conn.commit()
    conn.close()

init_db()

# --- HILFSFUNKTIONEN ---
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
    df = pd.read_sql("SELECT timestamp, target_date, buffer, top_risk FROM history WHERE project=? ORDER BY id ASC", conn, params=(project,))
    conn.close()
    return df

def save_data(project, df_tasks, df_risks):
    """Speichert Tasks und Risks mit Validierung und Column-Normalisierung"""
    conn = get_db_connection()
    conn.execute("DELETE FROM tasks WHERE project=?", (project,))
    conn.execute("DELETE FROM risks WHERE project=?", (project,))
    
    # FIX 3: Spalten normalisieren vor Speicherung
    df_tasks_clean = df_tasks.copy()
    df_tasks_clean.columns = df_tasks_clean.columns.str.strip()
    
    df_risks_clean = df_risks.copy()
    df_risks_clean.columns = df_risks_clean.columns.str.strip()
    
    # Tasks speichern mit Validierung
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
    
    # Risks speichern mit Validierung
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

# --- 4. LOGIN ---
if not st.session_state.auth_ok:
    st.title("🔐 Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")
    if st.button("Anmelden"):
        if "credentials" in st.secrets and u == st.secrets["credentials"]["username"] and p == st.secrets["credentials"]["password"]:
            st.session_state.auth_ok = True
            st.rerun()
        else: st.error("❌ Login fehlgeschlagen. Ungültige Anmeldedaten.")
    st.stop()

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("📂 Projekt-Steuerung")
    all_projs = get_all_projects()
    selected_proj = st.selectbox("Aktives Projekt:", all_projs)
    
    st.divider()
    st.subheader("📈 Tracking")
    do_history = st.checkbox("Messung in Zeitreihe ablegen", value=True)
    
    with st.expander("🛠️ Admin, Export & Import"):
        new_p = st.text_input("Neues Projekt / Kopie:")
        if st.button("🚀 Erstellen"):
            save_data(new_p, load_tasks(selected_proj), load_risks(selected_proj))
            st.success(f"✅ Projekt '{new_p}' erstellt!")
            st.rerun()
        
        t_exp = load_tasks(selected_proj)
        r_exp = load_risks(selected_proj)
        export_payload = json.dumps({"project": selected_proj, "tasks": t_exp.to_dict(orient="records"), "risks": r_exp.to_dict(orient="records")}, indent=2)
        st.download_button("📤 Projekt Export (JSON)", export_payload, f"{selected_proj}_export.json")

        st.divider()
        st.subheader("🗑️ Daten bereinigen")
        if st.button("📊 Historie (Zeitreihe) löschen"):
            delete_history_only(selected_proj)
            st.success("✅ Historie gelöscht.")
            st.rerun()

        st.divider()
        st.subheader("❗ Projekt löschen")
        confirm_del = st.checkbox("Projekt unwiderruflich löschen")
        if st.button(f"Lösche {selected_proj}"):
            if confirm_del:
                delete_project_complete(selected_proj)
                st.success(f"✅ Projekt '{selected_proj}' gelöscht.")
                st.rerun()
            else:
                st.warning("⚠️ Bestätigung erforderlich!")

    st.divider()
    st.subheader("📊 Szenarien-Vergleich")
    if st.button("📸 Stand einfrieren"):
        if st.session_state.last_durations is not None:
            st.session_state.snapshot_durations = st.session_state.last_durations
            st.session_state.snapshot_date = st.session_state.last_commit_85
            st.success("✅ Referenz gespeichert!")
        else:
            st.warning("⚠️ Erst simulation laufen lassen!")
    
    if st.button("🗑️ Snapshot (Referenz) löschen"):
        st.session_state.snapshot_durations = None
        st.session_state.snapshot_date = None
        st.info("✅ Referenz entfernt.")
        st.rerun()
    
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
    save_data(selected_proj, ed_t, load_risks(selected_proj))
    st.success("✅ Tasks gespeichert!")
    st.rerun()

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
    save_data(selected_proj, ed_t, ed_r)
    st.success("✅ Risiken gespeichert!")
    st.rerun()

# --- 7. SIMULATION & ANALYTICS ---
def run_fast_simulation(tasks, risks, std_risks, n):
    """
    Monte-Carlo Simulation mit korrekter Behandlung von Standard-Risiken.
    FIX: Standard-Risiken werden NUR in der Analytics aufgenommen, nicht doppelt angewendet.
    """
    # FIX 2: Validierung vor Simulation
    if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
        raise ValueError("❌ Keine gültigen Tasks definiert oder alle Dauern = 0")
    
    task_durations = np.tile(tasks["Duration (Days)"].values, (n, 1)).astype(float)
    base_sum = tasks["Duration (Days)"].sum()
    impact_results = []
    
    # --- PROJEKT-SPEZIFISCHE RISIKEN ANWENDEN ---
    for _, r in risks.iterrows():
        p = float(r.get("Probability (0-1)", 0))
        if p <= 0:
            continue
        
        vals = sorted([float(r.get("Impact Min", 0)), float(r.get("Impact Likely", 0)), float(r.get("Impact Max", 0))])
        impacts = np.random.triangular(vals[0], vals[1], max(vals[1] + 0.001, vals[2]), n)
        hits = np.random.random(n) < p
        
        target = str(r.get("Target (Global/Task)", "Global")).strip()
        risk_type = str(r.get("Risk Type", "Binär")).strip()
        
        # Berechne Impact für Analytics
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
        
        # Wende Risiko auf task_durations an
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
    
    # --- STANDARD-RISIKEN NUR FÜR ANALYTICS (NICHT DOPPELT ANWENDEN) ---
    for sr in std_risks:
        hits = np.random.random(n) < sr["prob"]
        impacts = np.random.triangular(sr["min"], sr["likely"], sr["max"], n)
        
        # Berechne Durchschnittliche Verzögerung
        delay_contribution = (total_days * (hits * impacts)).mean()
        
        # Wende auf total_days für weitere Simulation an (NICHT nochmal auf task_durations)
        total_days = total_days * (1 + (hits * impacts))
        
        impact_results.append({"Quelle": f"STD: {sr['name']}", "Verzögerung": delay_contribution, "Typ": "Standard"})
    
    # Clipping zur Vermeidung unrealistischer Werte
    total_days = np.clip(total_days, 0, 10000)
    
    return total_days.astype(int), pd.DataFrame(impact_results)

st.divider()

if st.button("🚀 Simulation starten & Trend analysieren"):
    try:
        with st.spinner("⏳ Berechne Monte-Carlo Trends..."):
            durations, impact_df = run_fast_simulation(t_curr, ed_r, selected_std, n_sim)
            start_np = np.datetime64(start_date)
            
            # FIX 1: Korrekte Datenarithmetik
            offsets = durations.astype('timedelta64[D]')
            end_dates_np = start_np + offsets
            end_dates = pd.to_datetime(end_dates_np, errors='coerce')
            
            commit_85 = pd.Series(end_dates).quantile(0.85)
            st.session_state.last_durations, st.session_state.last_commit_85 = durations, commit_85
            
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
            if do_history:
                save_history(selected_proj, commit_85.strftime('%Y-%m-%d'), float(np.mean(durations)), top_r)
                history_df = load_history(selected_proj)

            if "⚠️" in warning_msg:
                st.error(warning_msg)
            else:
                st.success(warning_msg)
            st.info(f"**Anweisung:** {rec}")

            col_l, col_r = st.columns([2, 1])
            with col_l:
                fig = go.Figure()
                fig.add_trace(go.Histogram(x=end_dates, name="Aktuell", marker_color="#1f77b4", opacity=0.6, nbinsx=50))
                
                # FIX 1: Korrigierte Snapshot-Berechnung
                if st.session_state.snapshot_date and st.session_state.snapshot_durations is not None:
                    ref_offsets = st.session_state.snapshot_durations.astype('timedelta64[D]')
                    ref_ends_np = start_np + ref_offsets
                    ref_ends = pd.to_datetime(ref_ends_np, errors='coerce')
                    fig.add_trace(go.Histogram(x=ref_ends, name="Referenz", marker_color="#7f7f7f", opacity=0.3, nbinsx=50))
                    fig.add_vline(x=st.session_state.snapshot_date.timestamp()*1000, line_dash="dash", line_color="#7f7f7f", annotation_text=f"Ref: {st.session_state.snapshot_date.strftime('%d.%m.%Y')}")
                
                fig.add_vline(x=commit_85.timestamp()*1000, line_dash="solid", line_color="red", annotation_text="85% Ziel")
                fig.update_layout(template="plotly_white", barmode='overlay', height=400, title="Verteilung der Projektenddaten (Monte-Carlo)")
                st.plotly_chart(fig, use_container_width=True)

            with col_r:
                st.metric("Zieltermin (85%)", commit_85.strftime('%d.%m.%Y'), delta=f"{diff} Tage" if not history_df.empty else None, delta_color="inverse")
                st.metric("Pufferbedarf (Ø)", f"{int(np.mean(durations) - t_curr['Duration (Days)'].sum())} Tage")
                st.metric("Min - Max", f"{int(np.min(durations))} - {int(np.max(durations))} Tage")
                st.subheader("🔥 Top Risiko-Treiber")
                st.write(f"**{top_r}**")

            # --- TORNADO CHART ---
            st.subheader("🎯 Risiko Impact Overview (Tornado Chart)")
            
            if not impact_df.empty:
                impact_df_sorted = impact_df.sort_values("Verzögerung", ascending=True)
                fig_tornado = go.Figure(go.Bar(
                    x=impact_df_sorted["Verzögerung"], 
                    y=impact_df_sorted["Quelle"], 
                    orientation='h', 
                    marker_color=['#EF553B' if t == "Projekt" else '#636EFA' for t in impact_df_sorted["Typ"]]
                ))
                fig_tornado.update_layout(
                    template="plotly_white", 
                    xaxis_title="Ø Verzögerung in Arbeitstagen", 
                    height=max(300, len(impact_df_sorted)*35), 
                    margin=dict(l=250)
                )
                st.plotly_chart(fig_tornado, use_container_width=True)
            else:
                st.info("ℹ️ Keine Risiken definiert.")

            # --- FIEBERKURVE ---
            if not history_df.empty and len(history_df) > 1:
                st.subheader("📉 Fieberkurve (Trend des Zieltermins)")
                history_df['target_date_dt'] = pd.to_datetime(history_df['target_date'])
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=history_df['timestamp'], 
                    y=history_df['target_date_dt'], 
                    mode='lines+markers', 
                    line=dict(color='firebrick', width=3),
                    fill='tozeroy',
                    fillcolor='rgba(255,0,0,0.1)'
                ))
                fig_trend.update_layout(
                    template="plotly_white", 
                    yaxis_title="Prognostizierter Zieltermin", 
                    xaxis_title="Messzeitpunkt",
                    height=350,
                    hovermode='x unified'
                )
                st.plotly_chart(fig_trend, use_container_width=True)
            elif len(history_df) == 1:
                st.info("ℹ️ Mindestens 2 Messungen nötig für Trend-Visualisierung.")

            # --- MANAGEMENT REPORT ---
            st.subheader("📜 Management Report")
            task_list_str = "".join([f"- {row['Task Name']}: {row['Duration (Days)']} Tage\n" for _, row in t_curr.iterrows()])
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
- Notwendiger Risiko-Puffer: {int(np.mean(durations) - t_curr['Duration (Days)'].sum())} Tage
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
            st.text_area("Report Vorschau", report_content, height=400, disabled=True)
            st.download_button(
                "📥 Vollständigen Report exportieren (.txt)", 
                report_content, 
                f"Report_{selected_proj}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
            )
    
    except ValueError as e:
        st.error(f"❌ Simulationsfehler: {e}")
    except Exception as e:
        st.error(f"❌ Unerwarteter Fehler: {e}")
        st.info("📝 Bitte überprüfen Sie alle Eingaben auf Vollständigkeit und Gültigkeit.")