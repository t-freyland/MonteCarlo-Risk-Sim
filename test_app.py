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
        {"Task Name": "Design",       "Duration (Days)": 10.0, "Beschreibung": "Design Phase", "team": "Team A"},
        {"Task Name": "Development",  "Duration (Days)": 20.0, "Beschreibung": "Dev Phase", "team": "Team A"},
        {"Task Name": "Testing",      "Duration (Days)": 5.0,  "Beschreibung": "QA Phase", "team": "Team B"},
    ])

@pytest.fixture
def sample_teams():
    return pd.DataFrame([
        {"Team": "Team A", "Capacity": 1.0},
        {"Team": "Team B", "Capacity": 2.0},
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
            conn.execute("INSERT INTO tasks (project, task_name, duration, description, team) VALUES (?,?,?,?,?)",
                         (project, row["Task Name"], row["Duration (Days)"], row.get("Beschreibung", ""), row.get("team", "Sequenziell")))
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

    def _save_teams(self, project, teams_df):
        conn = get_test_db_connection()
        for _, row in teams_df.iterrows():
            conn.execute("INSERT INTO teams (project, team_name, capacity) VALUES (?,?,?)",
                         (project, row["Team"], row.get("Capacity", 1.0)))
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
        assert "teams"          in tables

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

    def test_save_and_load_teams(self, project_name, sample_teams):
        self._save_teams(project_name, sample_teams)
        conn = get_test_db_connection()
        df = pd.read_sql("SELECT team_name, capacity FROM teams WHERE project=?", conn, params=(project_name,))
        conn.close()
        assert len(df) == 2
        assert "Team A" in df["team_name"].values
        assert 2.0 in df["capacity"].values

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
        assert not tasks.empty

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
            assert df.empty
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
# 2. SIMULATIONSFUNKTIONEN (MIT TEAM-PARALLELISIERUNG)
# ============================================================

class TestSimulation:

    def run_simulation(self, tasks, risks, std_risks, teams, n=1000):
        """Lokale Kopie der Simulationsfunktion für Tests."""
        if tasks.empty or tasks["Duration (Days)"].sum() <= 0:
            raise ValueError("Keine gültigen Tasks definiert oder alle Dauern = 0")

        tasks_copy = tasks.copy()
        if "team" not in tasks_copy.columns:
            tasks_copy["team"] = "Sequenziell"
        
        team_durations = {}
        for team_name in tasks_copy["team"].unique():
            team_tasks = tasks_copy[tasks_copy["team"] == team_name]
            team_sum = team_tasks["Duration (Days)"].sum()
            
            if not teams.empty:
                team_row = teams[teams["Team"] == team_name]
                if not team_row.empty:
                    capacity = float(team_row.iloc[0].get("Capacity", 1.0))
                    team_sum = team_sum / capacity
            
            team_durations[team_name] = team_sum
        
        base_sum = max(team_durations.values()) if team_durations else tasks_copy["Duration (Days)"].sum()
        total_days = np.full(n, base_sum, dtype=float)
        impact_results = []

        for _, r in risks.iterrows():
            p = float(r.get("Probability (0-1)", 0))
            rtype = str(r.get("Risk Type", "Binär")).strip().lower()
            target = str(r.get("Target (Global/Task)", "Global")).strip()
            mins = float(r.get("Impact Min", 0))
            lik = float(r.get("Impact Likely", mins))
            maxv = float(r.get("Impact Max", lik))
            
            impacts = np.random.triangular(mins, lik, max(maxv, lik + 1e-6), size=n)
            hits = np.ones(n, dtype=bool) if rtype == "kontinuierlich" else (np.random.random(n) < p)

            if target.lower() == "global":
                relevant_dur = base_sum
                task_team = "Global"
            else:
                match = tasks_copy[tasks_copy["Task Name"] == target]
                if not match.empty:
                    relevant_dur = float(match["Duration (Days)"].iloc[0])
                    task_team = match.iloc[0].get("team", "Sequenziell")
                    if not teams.empty:
                        team_row = teams[teams["Team"] == task_team]
                        if not team_row.empty:
                            capacity = float(team_row.iloc[0].get("Capacity", 1.0))
                            relevant_dur = relevant_dur / capacity
                else:
                    relevant_dur = 0.0
                    task_team = "Unknown"

            if relevant_dur <= 0:
                continue

            delay = impacts * relevant_dur
            total_days += delay if rtype == "kontinuierlich" else (hits * delay)
            avg_delay = float(delay.mean()) if rtype == "kontinuierlich" else float((hits * delay).mean())
            impact_results.append({
                "Quelle": str(r["Risk Name"]), 
                "Verzögerung": avg_delay, 
                "Typ": "Projekt",
                "Team": task_team
            })

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

    def test_simulation_with_team_parallelization(self, sample_tasks, sample_teams):
        np.random.seed(42)
        durations, _ = self.run_simulation(sample_tasks, pd.DataFrame(), [], sample_teams, n=100)
        expected_base = 30.0
        assert durations[0] == int(expected_base)

    def test_simulation_respects_team_capacity(self, sample_teams):
        tasks = pd.DataFrame([
            {"Task Name": "Fast Task", "Duration (Days)": 10.0, "Beschreibung": "", "team": "Team B"}
        ])
        np.random.seed(42)
        durations, _ = self.run_simulation(tasks, pd.DataFrame(), [], sample_teams, n=100)
        assert durations[0] == 5

    def test_simulation_with_task_specific_risk_on_team(self, sample_tasks, sample_teams, sample_std_risks):
        np.random.seed(42)
        task_risk = pd.DataFrame([{
            "Risk Name": "Design-Risk", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Design", "Probability (0-1)": 1.0,
            "Impact Min": 0.5, "Impact Likely": 0.5, "Impact Max": 0.5,
            "Maßnahme / Mitigation": ""
        }])
        durations, impact_df = self.run_simulation(sample_tasks, task_risk, [], sample_teams, n=500)
        design_delay = 10.0 * 0.5
        actual_delay = impact_df[impact_df["Quelle"] == "Design-Risk"]["Verzögerung"].values[0]
        assert abs(actual_delay - design_delay) < 1.0

    def test_simulation_returns_correct_shape(self, sample_tasks, sample_risks, sample_std_risks):
        np.random.seed(42)
        durations, impact_df = self.run_simulation(sample_tasks, sample_risks, sample_std_risks, pd.DataFrame(), n=100)
        assert len(durations) == 100
        assert isinstance(impact_df, pd.DataFrame)

    def test_simulation_base_duration_minimum(self, sample_tasks, sample_std_risks):
        """Mit Teams: Basis = kritischer Pfad (30), nicht Summe (35)."""
        np.random.seed(42)
        risks_positive = pd.DataFrame([{
            "Risk Name": "Positiv Risk", "Risk Type": "Binär",
            "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,
            "Impact Min": 0.0, "Impact Likely": 0.1, "Impact Max": 0.2,
            "Maßnahme / Mitigation": ""
        }])
        teams = pd.DataFrame([
            {"Team": "Team A", "Capacity": 1.0},
            {"Team": "Team B", "Capacity": 2.0},
        ])
        durations, _ = self.run_simulation(sample_tasks, risks_positive, [], teams, n=500)
        # Kritischer Pfad = Team A (30 Tage), nicht Summe (35)
        critical_path = 30.0
        assert durations.min() >= critical_path * 0.97

    def test_simulation_no_risks_equals_base_sum(self, sample_tasks):
        """Ohne Risiken: kritischer Pfad konstant."""
        np.random.seed(42)
        teams = pd.DataFrame([
            {"Team": "Team A", "Capacity": 1.0},
            {"Team": "Team B", "Capacity": 2.0},
        ])
        durations, impact_df = self.run_simulation(sample_tasks, pd.DataFrame(), [], teams, n=100)
        # Kritischer Pfad = 30 (Team A), nicht 35 (Summe)
        critical_path = 30
        assert all(d == critical_path for d in durations)
        assert impact_df.empty

    # ==================== NEUE TESTS ====================

    def test_simulation_binary_vs_continuous_risk_impact(self, sample_tasks):
        """Binäre Risiken: stochastisch | Kontinuierlich: deterministisch."""
        np.random.seed(42)
        teams = pd.DataFrame([{"Team": "Team A", "Capacity": 1.0}, {"Team": "Team B", "Capacity": 2.0}])
        
        # Binär: manchmal 0, manchmal Impact
        binary_risk = pd.DataFrame([{
            "Risk Name": "Binary", "Risk Type": "Binär",
            "Target (Global/Task)": "Global", "Probability (0-1)": 0.5,
            "Impact Min": 5.0, "Impact Likely": 10.0, "Impact Max": 15.0,
            "Maßnahme / Mitigation": ""
        }])
        durations_bin, _ = self.run_simulation(sample_tasks, binary_risk, [], teams, n=1000)
        
        # Kontinuierlich: immer Impact
        cont_risk = pd.DataFrame([{
            "Risk Name": "Continuous", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Global", "Probability (0-1)": 0.5,
            "Impact Min": 5.0, "Impact Likely": 10.0, "Impact Max": 15.0,
            "Maßnahme / Mitigation": ""
        }])
        durations_cont, _ = self.run_simulation(sample_tasks, cont_risk, [], teams, n=1000)
        
        # Kontinuierlich sollte konsistent höher sein
        assert np.mean(durations_cont) > np.mean(durations_bin)

    def test_simulation_multiple_teams_bottleneck(self):
        """Kritischer Pfad = längster Team, nicht Summe."""
        tasks = pd.DataFrame([
            {"Task Name": "Quick", "Duration (Days)": 5.0, "Beschreibung": "", "team": "Fast Team"},
            {"Task Name": "Slow", "Duration (Days)": 50.0, "Beschreibung": "", "team": "Slow Team"},
        ])
        teams = pd.DataFrame([
            {"Team": "Fast Team", "Capacity": 10.0},  # 5/10 = 0.5 Tage
            {"Team": "Slow Team", "Capacity": 1.0},   # 50/1 = 50 Tage
        ])
        np.random.seed(42)
        durations, _ = self.run_simulation(tasks, pd.DataFrame(), [], teams, n=100)
        # Bottleneck = Slow Team (50 Tage)
        assert all(d == 50 for d in durations)

    def test_simulation_global_risk_on_critical_path(self):
        """Globales Risiko wirkt auf kritischen Pfad."""
        tasks = pd.DataFrame([
            {"Task Name": "T1", "Duration (Days)": 20.0, "Beschreibung": "", "team": "A"},
            {"Task Name": "T2", "Duration (Days)": 10.0, "Beschreibung": "", "team": "B"},
        ])
        teams = pd.DataFrame([
            {"Team": "A", "Capacity": 1.0},
            {"Team": "B", "Capacity": 1.0},
        ])
        global_risk = pd.DataFrame([{
            "Risk Name": "Global", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,
            "Impact Min": 0.5, "Impact Likely": 0.5, "Impact Max": 0.5,  # Nicht 1.0!
            "Maßnahme / Mitigation": ""
        }])
        np.random.seed(42)
        durations, impact_df = self.run_simulation(tasks, global_risk, [], teams, n=100)
        
        # Kritischer Pfad = 20 (Team A)
        # Risiko: 0.5 * 20 = 10 Tage
        # Ergebnis: 20 + 10 = 30
        assert np.mean(durations) == pytest.approx(30.0, abs=1.0)
        assert impact_df.iloc[0]["Verzögerung"] == pytest.approx(10.0, abs=0.5)

    def test_simulation_standard_risks_combined(self, sample_tasks):
        """Mehrere Standardrisiken addieren sich."""
        np.random.seed(42)
        teams = pd.DataFrame([
            {"Team": "Team A", "Capacity": 1.0},
            {"Team": "Team B", "Capacity": 2.0},
        ])
        std_risks = [
            {"name": "Risk 1", "type": "Kontinuierlich", "prob": 1.0, "min": 0.1, "likely": 0.1, "max": 0.1},
            {"name": "Risk 2", "type": "Kontinuierlich", "prob": 1.0, "min": 0.1, "likely": 0.1, "max": 0.1},
        ]
        durations, impact_df = self.run_simulation(sample_tasks, pd.DataFrame(), std_risks, teams, n=100)
        # Kritischer Pfad = 30, + 0.1*30 + 0.1*30 = 30 + 3 + 3 = 36
        expected_delay = (0.1 + 0.1) * 30.0
        total_delay = impact_df["Verzögerung"].sum()
        assert abs(total_delay - expected_delay) < 1.0

    def test_simulation_global_risk_percentage(self):
        """Globales Risiko = % des kritischen Pfads."""
        tasks = pd.DataFrame([
            {"Task Name": "T1", "Duration (Days)": 20.0, "Beschreibung": "", "team": "A"},
            {"Task Name": "T2", "Duration (Days)": 10.0, "Beschreibung": "", "team": "B"},
        ])
        teams = pd.DataFrame([
            {"Team": "A", "Capacity": 1.0},
            {"Team": "B", "Capacity": 1.0},
        ])
        # 20% Impact auf kritischen Pfad (20 Tage)
        global_risk = pd.DataFrame([{
            "Risk Name": "20% Risk", "Risk Type": "Kontinuierlich",
            "Target (Global/Task)": "Global", "Probability (0-1)": 1.0,
            "Impact Min": 0.2, "Impact Likely": 0.2, "Impact Max": 0.2,
            "Maßnahme / Mitigation": ""
        }])
        np.random.seed(42)
        durations, impact_df = self.run_simulation(tasks, global_risk, [], teams, n=100)
        
        # Kritischer Pfad = 20
        # Risiko: 0.2 * 20 = 4 Tage
        # Ergebnis: 20 + 4 = 24
        assert np.mean(durations) == pytest.approx(24.0, abs=1.0)
        assert impact_df.iloc[0]["Verzögerung"] == pytest.approx(4.0, abs=0.5)