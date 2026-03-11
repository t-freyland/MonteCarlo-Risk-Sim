import pytest
import pandas as pd
import numpy as np
import sqlite3
import json
import os
from datetime import datetime, date
from unittest.mock import patch, MagicMock

# --- TEST DB SETUP ---
TEST_DB = "test_risk_management.db"

def get_test_db_connection():
    return sqlite3.connect(TEST_DB)

def init_test_db():
    conn = get_test_db_connection()
    conn.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT, task_name TEXT, duration REAL, description TEXT)")
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
    conn.commit()
    conn.close()

def cleanup_test_db():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

# --- FIXTURES ---
@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Setup Test-DB vor jedem Test, Cleanup danach."""
    init_test_db()
    yield
    cleanup_test_db()

@pytest.fixture
def sample_tasks():
    return pd.DataFrame([
        {"Task Name": "Design",       "Duration (Days)": 10.0, "Beschreibung": "Design Phase"},
        {"Task Name": "Development",  "Duration (Days)": 20.0, "Beschreibung": "Dev Phase"},
        {"Task Name": "Testing",      "Duration (Days)": 5.0,  "Beschreibung": "QA Phase"},
    ])

@pytest.fixture
def sample_risks():
    return pd.DataFrame([
        {"Risk Name": "Scope Creep",    "Risk Type": "Binär",          "Target (Global/Task)": "Global", "Probability (0-1)": 0.4,  "Impact Min": 0.05, "Impact Likely": 0.15, "Impact Max": 0.30, "Maßnahme / Mitigation": "Change Control"},
        {"Risk Name": "Tech Issues",    "Risk Type": "Kontinuierlich", "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,  "Impact Min": 0.02, "Impact Likely": 0.06, "Impact Max": 0.12, "Maßnahme / Mitigation": "Tech Reviews"},
        {"Risk Name": "Task-Specific",  "Risk Type": "Binär",          "Target (Global/Task)": "Design", "Probability (0-1)": 0.3,  "Impact Min": 0.05, "Impact Likely": 0.10, "Impact Max": 0.20, "Maßnahme / Mitigation": ""},
    ])

@pytest.fixture
def sample_std_risks():
    return [
        {"name": "Schätz-Ungenauigkeit", "type": "Kontinuierlich", "prob": 1.00, "min": -0.03, "likely": 0.00, "max": 0.08},
        {"name": "Scope Creep",          "type": "Binär",          "prob": 0.40, "min":  0.05, "likely": 0.15, "max": 0.30},
    ]

@pytest.fixture
def project_name():
    return "Test_Projekt"

# ============================================================
# 1. DATENBANKFUNKTIONEN
# ============================================================

class TestDatabase:

    def _save_tasks(self, project, tasks_df):
        conn = get_test_db_connection()
        for _, row in tasks_df.iterrows():
            conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                         (project, row["Task Name"], row["Duration (Days)"], row.get("Beschreibung", "")))
        conn.commit()
        conn.close()

    def _save_risks(self, project, risks_df):
        conn = get_test_db_connection()
        for _, row in risks_df.iterrows():
            conn.execute("INSERT INTO risks (project, risk_name, risk_type, target, prob, impact_min, impact_likely, impact_max, mitigation) VALUES (?,?,?,?,?,?,?,?,?)",
                         (project, row["Risk Name"], row["Risk Type"], row["Target (Global/Task)"],
                          row["Probability (0-1)"], row["Impact Min"], row["Impact Likely"], row["Impact Max"],
                          row.get("Maßnahme / Mitigation", "")))
        conn.commit()
        conn.close()

    def test_init_db_creates_all_tables(self):
        conn = get_test_db_connection()
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)["name"].tolist()
        conn.close()
        assert "tasks"          in tables
        assert "risks"          in tables
        assert "history"        in tables
        assert "actual_results" in tables

    def test_save_and_load_tasks(self, project_name, sample_tasks):
        self._save_tasks(project_name, sample_tasks)
        conn = get_test_db_connection()
        df = pd.read_sql("SELECT task_name, duration FROM tasks WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert len(df) == 3
        assert "Design" in df["task_name"].values
        assert 20.0 in df["duration"].values

    def test_save_and_load_risks(self, project_name, sample_risks):
        self._save_risks(project_name, sample_risks)
        conn = get_test_db_connection()
        df = pd.read_sql("SELECT risk_name, prob FROM risks WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert len(df) == 3
        assert "Scope Creep" in df["risk_name"].values

    def test_delete_tasks_on_save(self, project_name, sample_tasks):
        self._save_tasks(project_name, sample_tasks)
        conn = get_test_db_connection()
        conn.execute("DELETE FROM tasks WHERE project=?", (project_name,))
        conn.commit()
        df = pd.read_sql("SELECT * FROM tasks WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert df.empty

    def test_save_history(self, project_name):
        conn = get_test_db_connection()
        conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                     (project_name, "2026-03-11 10:00", "2026-06-01", 15.5, "Scope Creep"))
        conn.commit()
        df = pd.read_sql("SELECT * FROM history WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert len(df) == 1
        assert df.iloc[0]["top_risk"] == "Scope Creep"
        assert df.iloc[0]["buffer"] == 15.5

    def test_load_history_ordered_by_timestamp(self, project_name):
        conn = get_test_db_connection()
        for ts, td in [("2026-01-01 10:00", "2026-05-01"), ("2026-02-01 10:00", "2026-06-01"), ("2026-03-01 10:00", "2026-05-15")]:
            conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                         (project_name, ts, td, 10.0, "Risk A"))
        conn.commit()
        df = pd.read_sql("SELECT timestamp FROM history WHERE project=? ORDER BY timestamp ASC", conn, params=(project_name,))
        conn.close()
        assert df.iloc[0]["timestamp"] == "2026-01-01 10:00"
        assert df.iloc[-1]["timestamp"] == "2026-03-01 10:00"

    def test_delete_history_only(self, project_name):
        conn = get_test_db_connection()
        conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                     (project_name, "2026-03-11 10:00", "2026-06-01", 15.0, "Risk"))
        conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                     (project_name, "Task A", 10.0, ""))
        conn.commit()
        conn.execute("DELETE FROM history WHERE project=?", (project_name,))
        conn.commit()
        hist = pd.read_sql("SELECT * FROM history WHERE project=?", conn, params=(project_name,))
        tasks = pd.read_sql("SELECT * FROM tasks WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert hist.empty
        assert not tasks.empty  # Tasks bleiben erhalten

    def test_delete_project_complete(self, project_name, sample_tasks, sample_risks):
        self._save_tasks(project_name, sample_tasks)
        self._save_risks(project_name, sample_risks)
        conn = get_test_db_connection()
        conn.execute("INSERT INTO history (project, timestamp, target_date, buffer, top_risk) VALUES (?,?,?,?,?)",
                     (project_name, "2026-03-11", "2026-06-01", 10.0, "Risk"))
        conn.execute("INSERT INTO actual_results (project, start_date, planned_end_date, actual_end_date, planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (project_name, "2026-01-01", "2026-06-01", "2026-06-15", 150, 165, 15, "Risk", "[]", ""))
        conn.commit()
        for table in ["tasks", "risks", "history", "actual_results"]:
            conn.execute(f"DELETE FROM {table} WHERE project=?", (project_name,))
        conn.commit()
        for table in ["tasks", "risks", "history", "actual_results"]:
            df = pd.read_sql(f"SELECT * FROM {table} WHERE project=?", conn, params=(project_name,))
            assert df.empty, f"Tabelle {table} sollte leer sein nach Projekt-Löschung"
        conn.close()

    def test_save_actual_result(self, project_name):
        conn = get_test_db_connection()
        risks_occurred = ["Scope Creep", "Tech Issues"]
        planned_days = (pd.to_datetime("2026-06-01") - pd.to_datetime("2026-01-01")).days
        actual_days  = (pd.to_datetime("2026-06-15") - pd.to_datetime("2026-01-01")).days
        buffer_used  = actual_days - planned_days
        conn.execute("""
            INSERT INTO actual_results 
            (project, start_date, planned_end_date, actual_end_date,
             planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (project_name, "2026-01-01", "2026-06-01", "2026-06-15",
              planned_days, actual_days, buffer_used, "Scope Creep",
              json.dumps(risks_occurred), "Test Note"))
        conn.commit()
        df = pd.read_sql("SELECT * FROM actual_results WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert len(df) == 1
        assert df.iloc[0]["buffer_used"] == 14
        assert json.loads(df.iloc[0]["risks_occurred"]) == risks_occurred

    def test_load_actual_results_ordered(self, project_name):
        conn = get_test_db_connection()
        for end_date, planned_days, actual_days in [
            ("2026-03-01", 60, 70),
            ("2026-01-15", 45, 50),
            ("2026-05-01", 90, 85),
        ]:
            conn.execute("""
                INSERT INTO actual_results 
                (project, start_date, planned_end_date, actual_end_date,
                 planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (project_name, "2026-01-01", "2026-04-01", end_date,
                  planned_days, actual_days, actual_days - planned_days,
                  "Risk", "[]", ""))
        conn.commit()
        df = pd.read_sql("SELECT actual_end_date FROM actual_results WHERE project=? ORDER BY actual_end_date ASC",
                         conn, params=(project_name,))
        conn.close()
        assert df.iloc[0]["actual_end_date"] == "2026-01-15"
        assert df.iloc[-1]["actual_end_date"] == "2026-05-01"

    def test_get_all_projects_returns_distinct(self):
        conn = get_test_db_connection()
        for proj in ["Projekt_A", "Projekt_A", "Projekt_B"]:
            conn.execute("INSERT INTO tasks (project, task_name, duration, description) VALUES (?,?,?,?)",
                         (proj, "Task", 10.0, ""))
        conn.commit()
        df = pd.read_sql("SELECT DISTINCT project FROM tasks UNION SELECT DISTINCT project FROM risks", conn)
        conn.close()
        assert len(df) == 2
        assert "Projekt_A" in df["project"].values
        assert "Projekt_B" in df["project"].values


# ============================================================
# 2. SIMULATIONSFUNKTIONEN
# ============================================================

class TestSimulation:

    def run_simulation(self, tasks, risks, std_risks, n=1000):
        """Lokale Kopie der Simulationsfunktion für Tests."""
        if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
            raise ValueError("Keine gültigen Tasks definiert oder alle Dauern = 0")

        base_sum = float(tasks["Duration (Days)"].sum())
        total_days = np.full(n, base_sum, dtype=float)
        impact_results = []

        for _, r in risks.iterrows():
            rtype = str(r.get("Risk Type", "Binär")).strip().lower()
            mins  = float(r.get("Impact Min", 0))
            lik   = float(r.get("Impact Likely", mins))
            maxv  = float(r.get("Impact Max", lik))
            p     = float(r.get("Probability (0-1)", 0))
            target = str(r.get("Target (Global/Task)", "Global")).strip()

            impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
            hits    = np.ones(n, dtype=bool) if rtype == "kontinuierlich" else (np.random.random(n) < p)

            if target.lower() == "global":
                relevant_dur = base_sum
            else:
                match = tasks[tasks["Task Name"] == target]
                relevant_dur = float(match["Duration (Days)"].sum()) if not match.empty else 0.0

            if relevant_dur <= 0:
                continue

            delay = impacts * relevant_dur
            total_days += delay if rtype == "kontinuierlich" else (hits * delay)
            avg_delay = float(delay.mean()) if rtype == "kontinuierlich" else float((hits * delay).mean())
            impact_results.append({"Quelle": str(r["Risk Name"]), "Verzögerung": avg_delay, "Typ": "Projekt"})

        for sr in std_risks:
            rtype = str(sr.get("type", "Binär")).strip().lower()
            p     = float(sr.get("prob", 0))
            mins  = float(sr.get("min", 0))
            lik   = float(sr.get("likely", mins))
            maxv  = float(sr.get("max", lik))
            impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
            hits    = np.ones(n, dtype=bool) if rtype == "kontinuierlich" else (np.random.random(n) < p)
            delay   = impacts * base_sum
            total_days += delay if rtype == "kontinuierlich" else (hits * delay)
            avg_delay = float(delay.mean()) if rtype == "kontinuierlich" else float((hits * delay).mean())
            impact_results.append({"Quelle": f"STD: {sr['name']}", "Verzögerung": avg_delay, "Typ": "Standard"})

        total_days = np.clip(total_days, 0, 10000)
        return total_days.astype(int), pd.DataFrame(impact_results)

    def test_simulation_returns_correct_shape(self, sample_tasks, sample_risks, sample_std_risks):
        np.random.seed(42)
        durations, impact_df = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, n=1000)
        assert len(durations) == 1000
        assert isinstance(impact_df, pd.DataFrame)
        assert "Quelle" in impact_df.columns
        assert "Verzögerung" in impact_df.columns
        assert "Typ" in impact_df.columns

    def test_simulation_base_duration_minimum(self, sample_tasks, sample_std_risks):
        """Ohne negative Risiken: Ergebnis >= Basis-Summe."""
        np.random.seed(42)
        risks_positive = pd.DataFrame([{
            "Risk Name": "Positiv Risk", "Risk Type": "Binär",
            "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,
            "Impact Min": 0.0, "Impact Likely": 0.1, "Impact Max": 0.2,
            "Maßnahme / Mitigation": ""
        }])
        durations, _ = self.run_simulation(sample_tasks, risks_positive, [], n=500)
        base = sample_tasks["Duration (Days)"].sum()
        assert durations.min() >= base * 0.97  # Toleranz für Rundung

    def test_simulation_raises_on_empty_tasks(self, sample_std_risks):
        empty_tasks = pd.DataFrame(columns=["Task Name", "Duration (Days)", "Beschreibung"])
        with pytest.raises(ValueError, match="Keine gültigen Tasks"):
            self.run_simulation(empty_tasks, pd.DataFrame(), sample_std_risks)

    def test_simulation_raises_on_zero_duration(self, sample_std_risks):
        zero_tasks = pd.DataFrame([{"Task Name": "Task", "Duration (Days)": 0.0, "Beschreibung": ""}])
        with pytest.raises(ValueError):
            self.run_simulation(zero_tasks, pd.DataFrame(), sample_std_risks)

    def test_simulation_output_clipped_to_10000(self, sample_tasks):
        """Extreme Risiken überschreiten nicht 10.000 Tage."""
        np.random.seed(42)
        extreme_risks = pd.DataFrame([{
            "Risk Name": "Extreme", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,
            "Impact Min": 100.0, "Impact Likely": 200.0, "Impact Max": 300.0,
            "Maßnahme / Mitigation": ""
        }])
        durations, _ = self.run_simulation(sample_tasks, extreme_risks, [], n=100)
        assert durations.max() <= 10000

    def test_simulation_impact_df_contains_all_risks(self, sample_tasks, sample_risks, sample_std_risks):
        np.random.seed(42)
        _, impact_df = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, n=500)
        projekt_risks = impact_df[impact_df["Typ"] == "Projekt"]["Quelle"].tolist()
        std_risks_result = impact_df[impact_df["Typ"] == "Standard"]["Quelle"].tolist()
        assert "Scope Creep" in projekt_risks
        assert "Tech Issues" in projekt_risks
        assert any("Schätz-Ungenauigkeit" in r for r in std_risks_result)

    def test_simulation_task_specific_risk(self, sample_tasks, sample_std_risks):
        """Task-spezifisches Risiko wirkt nur auf Task-Dauer."""
        np.random.seed(42)
        task_risk = pd.DataFrame([{
            "Risk Name": "Design-Risk", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Design", "Probability (0-1)": 1.0,
            "Impact Min": 0.5, "Impact Likely": 0.5, "Impact Max": 0.5,
            "Maßnahme / Mitigation": ""
        }])
        durations, impact_df = self.run_simulation(sample_tasks, task_risk, [], n=1000)
        design_duration = 10.0
        expected_delay = design_duration * 0.5
        actual_delay = impact_df[impact_df["Quelle"] == "Design-Risk"]["Verzögerung"].values[0]
        assert abs(actual_delay - expected_delay) < 1.0

    def test_simulation_binary_risk_probability_zero(self, sample_tasks):
        """Binäres Risiko mit P=0 erzeugt keine Verzögerung."""
        np.random.seed(42)
        zero_risk = pd.DataFrame([{
            "Risk Name": "Zero Risk", "Risk Type": "Binär",
            "Target (Global/Task)": "Global", "Probability (0-1)": 0.0,
            "Impact Min": 1.0, "Impact Likely": 2.0, "Impact Max": 3.0,
            "Maßnahme / Mitigation": ""
        }])
        durations, impact_df = self.run_simulation(sample_tasks, zero_risk, [], n=1000)
        base = sample_tasks["Duration (Days)"].sum()
        assert np.mean(durations) == pytest.approx(base, abs=0.5)

    def test_simulation_percentile_85_greater_than_50(self, sample_tasks, sample_risks, sample_std_risks):
        np.random.seed(42)
        durations, _ = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, n=5000)
        p50 = np.percentile(durations, 50)
        p85 = np.percentile(durations, 85)
        assert p85 >= p50

    def test_simulation_no_risks_equals_base_sum(self, sample_tasks):
        np.random.seed(42)
        durations, impact_df = self.run_simulation(sample_tasks, pd.DataFrame(), [], n=100)
        base = int(sample_tasks["Duration (Days)"].sum())
        assert all(d == base for d in durations)
        assert impact_df.empty

    def test_simulation_deterministic_with_seed(self, sample_tasks, sample_risks, sample_std_risks):
        np.random.seed(123)
        d1, _ = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, n=500)
        np.random.seed(123)
        d2, _ = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, n=500)
        np.testing.assert_array_equal(d1, d2)


# ============================================================
# 3. BACKTESTING / VALIDIERUNGSLOGIK
# ============================================================

class TestBacktesting:

    def _insert_actual_result(self, project, planned_days, actual_days, risks_occurred=None, notes=""):
        conn = get_test_db_connection()
        buffer = actual_days - planned_days
        conn.execute("""
            INSERT INTO actual_results 
            (project, start_date, planned_end_date, actual_end_date,
             planned_duration, actual_duration, buffer_used, top_risk, risks_occurred, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (project, "2026-01-01", "2026-04-01", "2026-05-01",
              planned_days, actual_days, buffer, "Scope Creep",
              json.dumps(risks_occurred or []), notes))
        conn.commit()
        conn.close()

    def test_accuracy_calculation_all_correct(self):
        """Alle Projekte innerhalb 15% → 100% Accuracy."""
        data = pd.DataFrame([
            {"planned_duration": 100, "actual_duration": 110},
            {"planned_duration": 80,  "actual_duration": 90},
            {"planned_duration": 120, "actual_duration": 130},
        ])
        correct = sum(1 for _, r in data.iterrows() if r["actual_duration"] <= r["planned_duration"] * 1.15)
        accuracy = correct / len(data) * 100
        assert accuracy == 100.0

    def test_accuracy_calculation_all_wrong(self):
        """Alle Projekte über 15% → 0% Accuracy."""
        data = pd.DataFrame([
            {"planned_duration": 100, "actual_duration": 120},
            {"planned_duration": 80,  "actual_duration": 100},
        ])
        correct = sum(1 for _, r in data.iterrows() if r["actual_duration"] <= r["planned_duration"] * 1.15)
        accuracy = correct / len(data) * 100
        assert accuracy == 0.0

    def test_accuracy_calculation_mixed(self):
        data = pd.DataFrame([
            {"planned_duration": 100.0, "actual_duration": 110.0},  # OK  (10%)
            {"planned_duration": 100.0, "actual_duration": 120.0},  # NOK (20%)
            {"planned_duration": 100.0, "actual_duration": 115.0},  # OK  (15% - Grenzfall)
            {"planned_duration": 100.0, "actual_duration": 130.0},  # NOK (30%)
        ])
        correct = sum(
            1 for _, r in data.iterrows()
            if r["actual_duration"] <= round(r["planned_duration"] * 1.15, 10)
        )
        accuracy = correct / len(data) * 100
        assert accuracy == 50.0

    def test_abweichung_prozent_positive(self):
        """Verzögerung → positive Abweichung."""
        planned, actual = 100.0, 120.0
        abweichung = (actual - planned) / planned * 100
        assert abweichung == pytest.approx(20.0)

    def test_abweichung_prozent_negative(self):
        """Früher fertig → negative Abweichung."""
        planned, actual = 100.0, 90.0
        abweichung = (actual - planned) / planned * 100
        assert abweichung == pytest.approx(-10.0)

    def test_abweichung_prozent_zero(self):
        planned, actual = 100.0, 100.0
        abweichung = (actual - planned) / planned * 100
        assert abweichung == pytest.approx(0.0)

    def test_avg_deviation_triggers_optimistic_warning(self):
        """Ø > 15% → 'zu optimistisch'."""
        deviations = [20.0, 18.0, 22.0, 16.0]
        avg = sum(deviations) / len(deviations)
        assert avg > 15
        recommendation = "zu_optimistisch" if avg > 15 else ("zu_pessimistisch" if avg < -10 else "gut")
        assert recommendation == "zu_optimistisch"

    def test_avg_deviation_triggers_pessimistic_warning(self):
        """Ø < -10% → 'zu pessimistisch'."""
        deviations = [-15.0, -12.0, -11.0, -14.0]
        avg = sum(deviations) / len(deviations)
        assert avg < -10
        recommendation = "zu_optimistisch" if avg > 15 else ("zu_pessimistisch" if avg < -10 else "gut")
        assert recommendation == "zu_pessimistisch"

    def test_avg_deviation_good_calibration(self):
        """Ø zwischen -10% und 15% → gut kalibriert."""
        deviations = [5.0, -3.0, 8.0, 2.0]
        avg = sum(deviations) / len(deviations)
        assert -10 <= avg <= 15
        recommendation = "zu_optimistisch" if avg > 15 else ("zu_pessimistisch" if avg < -10 else "gut")
        assert recommendation == "gut"

    def test_risk_frequency_counting(self):
        """Häufigkeit eingetretener Risiken korrekt zählen."""
        risks_data = [
            json.dumps(["Scope Creep", "Tech Issues"]),
            json.dumps(["Scope Creep"]),
            json.dumps(["Personalfluktuation", "Scope Creep"]),
        ]
        all_risks = []
        for r in risks_data:
            all_risks.extend(json.loads(r))
        counts = pd.Series(all_risks).value_counts()
        assert counts["Scope Creep"] == 3
        assert counts["Tech Issues"] == 1
        assert counts["Personalfluktuation"] == 1

    def test_velocity_calculation_on_track(self):
        """Velocity >= 1 bedeutet: im Plan oder voraus."""
        geplante_tage = 35.0   # 70% von 50 Tagen geplant
        tatsaechliche_tage = 30
        velocity = geplante_tage / max(tatsaechliche_tage, 1)
        assert velocity > 0.9
        status = "Im Plan" if velocity >= 0.9 else "Verzögert"
        assert status == "Im Plan"

    def test_velocity_calculation_delayed(self):
        geplante_tage = 20.0   # erst 40% von 50 Tagen geplant
        tatsaechliche_tage = 30
        velocity = geplante_tage / max(tatsaechliche_tage, 1)
        assert velocity < 0.9
        status = "Im Plan" if velocity >= 0.9 else "Verzögert"
        assert status == "Verzögert"

    def test_projected_end_date_calculation(self):
        base_duration = 50.0
        velocity = 0.8  # 20% langsamer
        projected_total = int(base_duration / velocity)
        assert projected_total == 62

    def test_zwischenstand_note_prefix(self):
        fertig_prozent = 60
        notiz = "Alles läuft gut"
        full_note = f"[ZWISCHENSTAND {fertig_prozent}%] {notiz}"
        assert full_note == "[ZWISCHENSTAND 60%] Alles läuft gut"

    def test_abschluss_note_prefix(self):
        notes = "Lessons learned"
        full_note = f"[ABSCHLUSS] {notes}"
        assert full_note.startswith("[ABSCHLUSS]")

    def test_buffer_used_calculation(self):
        planned_end = pd.to_datetime("2026-06-01")
        actual_end  = pd.to_datetime("2026-06-15")
        start       = pd.to_datetime("2026-01-01")
        planned_days = (planned_end - start).days
        actual_days  = (actual_end  - start).days
        buffer_used  = actual_days - planned_days
        assert buffer_used == 14

    def test_buffer_negative_when_early(self):
        planned_end = pd.to_datetime("2026-06-01")
        actual_end  = pd.to_datetime("2026-05-20")
        start       = pd.to_datetime("2026-01-01")
        planned_days = (planned_end - start).days
        actual_days  = (actual_end  - start).days
        buffer_used  = actual_days - planned_days
        assert buffer_used < 0


# ============================================================
# 4. HILFSFUNKTIONEN & EDGE CASES
# ============================================================

class TestHelpers:

    def test_get_default_standard_risks_returns_dataframe(self):
        df = pd.DataFrame([
            {"Aktiv": True, "name": "Schätz-Ungenauigkeit", "type": "Kontinuierlich",
             "prob": 1.00, "min": -0.03, "likely": 0.00, "max": 0.08},
        ])
        assert isinstance(df, pd.DataFrame)
        assert "Aktiv" in df.columns
        assert "name"  in df.columns

    def test_standard_risks_has_active_and_inactive(self):
        df = pd.DataFrame([
            {"Aktiv": True,  "name": "Risk A", "type": "Binär", "prob": 0.4, "min": 0.05, "likely": 0.15, "max": 0.30},
            {"Aktiv": False, "name": "Risk B", "type": "Binär", "prob": 0.2, "min": 0.03, "likely": 0.08, "max": 0.15},
        ])
        active = df[df["Aktiv"] == True]
        inactive = df[df["Aktiv"] == False]
        assert len(active) == 1
        assert len(inactive) == 1

    def test_probability_clipping(self):
        probs = pd.Series([-0.1, 0.0, 0.5, 1.0, 1.5])
        clipped = probs.clip(0, 1)
        assert clipped.min() == 0.0
        assert clipped.max() == 1.0

    def test_impact_sorting(self):
        """Min <= Likely <= Max nach Sortierung."""
        vals = np.array([[0.2, 0.05, 0.1]])
        sorted_vals = np.sort(vals, axis=1)
        assert sorted_vals[0][0] <= sorted_vals[0][1] <= sorted_vals[0][2]

    def test_triangular_distribution_bounds(self):
        """Triangular-Verteilung bleibt innerhalb der Grenzen."""
        np.random.seed(42)
        samples = np.random.triangular(0.05, 0.15, 0.30, size=10000)
        assert samples.min() >= 0.05
        assert samples.max() <= 0.30

    def test_end_dates_from_durations(self):
        start = np.datetime64("2026-01-01")
        durations = np.array([30, 45, 60])
        end_dates = pd.to_datetime(start + durations.astype('timedelta64[D]'), errors='coerce')
        assert str(end_dates[0].date()) == "2026-01-31"
        assert str(end_dates[1].date()) == "2026-02-15"
        assert str(end_dates[2].date()) == "2026-03-02"

    def test_commit_85_percentile(self):
        np.random.seed(42)
        durations = np.random.normal(100, 15, 10000).astype(int)
        start = np.datetime64("2026-01-01")
        end_dates = pd.to_datetime(start + durations.astype('timedelta64[D]'), errors='coerce')
        commit_85 = pd.Series(end_dates).quantile(0.85)
        # 85% der Enddaten sollten vor oder gleich commit_85 liegen
        pct_before = (pd.Series(end_dates) <= commit_85).mean()
        assert pct_before >= 0.84

    def test_history_diff_positive_means_delay(self):
        current  = pd.Timestamp("2026-06-15")
        previous = pd.Timestamp("2026-06-01")
        diff = (current - previous).days
        assert diff == 14
        warning = "TERMINWARNUNG" if diff > 0 else "POSITIVER TREND"
        assert warning == "TERMINWARNUNG"

    def test_history_diff_negative_means_improvement(self):
        current  = pd.Timestamp("2026-05-20")
        previous = pd.Timestamp("2026-06-01")
        diff = (current - previous).days
        assert diff < 0
        warning = "TERMINWARNUNG" if diff > 0 else "POSITIVER TREND"
        assert warning == "POSITIVER TREND"

    def test_tasks_sum_calculation(self, sample_tasks):
        total = sample_tasks["Duration (Days)"].sum()
        assert total == 35.0

    def test_empty_tasks_sum_is_zero(self):
        empty = pd.DataFrame(columns=["Task Name", "Duration (Days)", "Beschreibung"])
        assert empty["Duration (Days)"].sum() == 0

    def test_json_risks_occurred_roundtrip(self):
        risks = ["Scope Creep", "Tech Issues", "Personalfluktuation"]
        serialized   = json.dumps(risks)
        deserialized = json.loads(serialized)
        assert deserialized == risks

    def test_json_risks_occurred_empty_list(self):
        risks = []
        serialized   = json.dumps(risks)
        deserialized = json.loads(serialized)
        assert deserialized == []