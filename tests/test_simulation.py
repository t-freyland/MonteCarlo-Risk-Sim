import pytest
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime
from app import (
    run_fast_simulation,
    get_db_connection,
    init_db,
    save_data,
    load_tasks,
    load_risks,
    save_history,
    load_history,
    delete_project_complete,
)

# --- FIXTURES ---
@pytest.fixture
def sample_tasks():
    return pd.DataFrame({
        "Task Name": ["Task1", "Task2"],
        "Duration (Days)": [10.0, 20.0],
        "Beschreibung": ["", ""]
    })

@pytest.fixture
def sample_risks():
    return pd.DataFrame({
        "Risk Name": ["Risk1"],
        "Risk Type": ["Binär"],
        "Target (Global/Task)": ["Global"],
        "Probability (0-1)": [0.5],
        "Impact Min": [0.1],
        "Impact Likely": [0.2],
        "Impact Max": [0.3],
        "Maßnahme / Mitigation": [""]
    })

@pytest.fixture
def sample_std_risks():
    return [
        {"name": "Test Std Risk", "type": "Kontinuierlich", "prob": 1.0, "min": 0.05, "likely": 0.1, "max": 0.15}
    ]

@pytest.fixture(autouse=True)
def cleanup_db():
    """Cleanup DB nach jedem Test."""
    yield
    if os.path.exists("risk_management.db"):
        os.remove("risk_management.db")

# --- SIMULATION TESTS ---
def test_run_fast_simulation_basic(sample_tasks):
    """Test grundlegende Simulation ohne Risiken."""
    risks = pd.DataFrame()
    std_risks = []
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, risks, std_risks, n)

    assert len(durations) == n
    assert durations.min() >= 30  # Base sum: 10+20
    assert durations.max() >= 30
    assert impact_df.empty

def test_run_fast_simulation_binary_risk(sample_tasks, sample_risks):
    """Test Binär-Risiko mit p=1.0 (immer aktiv)."""
    sample_risks.loc[0, "Probability (0-1)"] = 1.0
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, sample_risks, [], n)

    assert len(durations) == n
    assert durations.mean() > 30  # Sollte erhöht sein
    assert not impact_df.empty
    assert impact_df.iloc[0]["Verzögerung"] > 0

def test_run_fast_simulation_binary_risk_zero_prob(sample_tasks, sample_risks):
    """Test Binär-Risiko mit p=0 (nie aktiv)."""
    sample_risks.loc[0, "Probability (0-1)"] = 0.0
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, sample_risks, [], n)

    assert len(durations) == n
    assert np.allclose(durations.mean(), 30.0)  # Keine Erhöhung
    assert not impact_df.empty
    assert impact_df.iloc[0]["Verzögerung"] == 0.0

def test_run_fast_simulation_continuous_risk(sample_tasks):
    """Test Kontinuierlich-Risiko (wirkt immer)."""
    risks = pd.DataFrame({
        "Risk Name": ["Cont Risk"],
        "Risk Type": ["Kontinuierlich"],
        "Target (Global/Task)": ["Global"],
        "Probability (0-1)": [0.0],  # Irrelevant
        "Impact Min": [0.05],
        "Impact Likely": [0.1],
        "Impact Max": [0.15],
        "Maßnahme / Mitigation": [""]
    })
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, risks, [], n)

    assert len(durations) == n
    assert durations.min() > 30  # Sollte immer erhöht sein
    assert not impact_df.empty
    assert impact_df.iloc[0]["Verzögerung"] > 0

def test_run_fast_simulation_task_specific_risk(sample_tasks):
    """Test Task-spezifisches Risiko."""
    risks = pd.DataFrame({
        "Risk Name": ["Task1 Risk"],
        "Risk Type": ["Binär"],
        "Target (Global/Task)": ["Task1"],  # Nur auf Task1
        "Probability (0-1)": [1.0],
        "Impact Min": [0.1],
        "Impact Likely": [0.2],
        "Impact Max": [0.3],
        "Maßnahme / Mitigation": [""]
    })
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, risks, [], n)

    assert len(durations) == n
    assert durations.mean() > 30  # Sollte erhöht sein
    assert not impact_df.empty

def test_run_fast_simulation_with_std_risks(sample_tasks, sample_std_risks):
    """Test Standardrisiken."""
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, pd.DataFrame(), sample_std_risks, n)

    assert len(durations) == n
    assert any("STD:" in row["Quelle"] for _, row in impact_df.iterrows())

def test_run_fast_simulation_empty_tasks():
    """Test mit leeren Tasks."""
    tasks = pd.DataFrame()
    risks = pd.DataFrame()
    std_risks = []
    n = 100

    with pytest.raises(ValueError, match="Keine gültigen Tasks"):
        run_fast_simulation(tasks, risks, std_risks, n)

def test_run_fast_simulation_zero_duration_tasks():
    """Test mit Task-Dauer = 0."""
    tasks = pd.DataFrame({
        "Task Name": ["Task1"],
        "Duration (Days)": [0.0],
        "Beschreibung": [""]
    })
    risks = pd.DataFrame()
    std_risks = []
    n = 100

    with pytest.raises(ValueError, match="Keine gültigen Tasks"):
        run_fast_simulation(tasks, risks, std_risks, n)

def test_run_fast_simulation_negative_impact(sample_tasks):
    """Test mit negativem Impact (z.B. Effizienzgewinne)."""
    risks = pd.DataFrame({
        "Risk Name": ["Negative Impact"],
        "Risk Type": ["Kontinuierlich"],
        "Target (Global/Task)": ["Global"],
        "Probability (0-1)": [1.0],
        "Impact Min": [-0.1],
        "Impact Likely": [-0.05],
        "Impact Max": [0.0],
        "Maßnahme / Mitigation": [""]
    })
    n = 100

    durations, impact_df = run_fast_simulation(sample_tasks, risks, [], n)

    assert len(durations) == n
    assert durations.mean() < 30  # Sollte reduziert sein

# --- DATABASE TESTS ---
def test_save_and_load_tasks(sample_tasks):
    """Test Speichern und Laden von Tasks."""
    project = "test_project"
    init_db()
    
    save_data(project, sample_tasks, pd.DataFrame())
    loaded = load_tasks(project)

    assert len(loaded) == 2
    assert loaded.iloc[0]["Task Name"] == "Task1"
    assert loaded.iloc[0]["Duration (Days)"] == 10.0

def test_save_and_load_risks(sample_tasks, sample_risks):
    """Test Speichern und Laden von Risiken."""
    project = "test_project"
    init_db()

    save_data(project, sample_tasks, sample_risks)
    loaded = load_risks(project)

    assert len(loaded) == 1
    assert loaded.iloc[0]["Risk Name"] == "Risk1"
    assert loaded.iloc[0]["Risk Type"] == "Binär"

def test_save_invalid_tasks(sample_tasks):
    """Test mit ungültigen Task-Daten (z.B. NaN, negative Dauer)."""
    project = "test_project"
    init_db()

    invalid_tasks = sample_tasks.copy()
    invalid_tasks.loc[0, "Duration (Days)"] = -5.0  # Negativ
    invalid_tasks.loc[1, "Duration (Days)"] = np.nan

    save_data(project, invalid_tasks, pd.DataFrame())
    loaded = load_tasks(project)

    # Ungültige sollten nicht gespeichert werden, nur Default-Task bleibt
    assert len(loaded) == 1
    assert loaded.iloc[0]["Task Name"] == "Basis-Task"
    assert loaded.iloc[0]["Duration (Days)"] == 10.0

def test_save_invalid_risks(sample_tasks):
    """Test mit ungültiger Wahrscheinlichkeit."""
    project = "test_project"
    init_db()

    invalid_risks = pd.DataFrame({
        "Risk Name": ["Invalid"],
        "Risk Type": ["Binär"],
        "Target (Global/Task)": ["Global"],
        "Probability (0-1)": [1.5],  # Ungültig (>1)
        "Impact Min": [0.1],
        "Impact Likely": [0.2],
        "Impact Max": [0.3],
        "Maßnahme / Mitigation": [""]
    })

    save_data(project, sample_tasks, invalid_risks)
    loaded = load_risks(project)

    assert len(loaded) == 0

def test_save_and_load_history(sample_tasks):
    """Test Speichern und Laden von History."""
    project = "test_project"
    init_db()

    save_data(project, sample_tasks, pd.DataFrame())
    target_date = "2025-12-31"
    save_history(project, target_date, 5.0, "Risk1")

    history = load_history(project)
    assert len(history) == 1
    assert history.iloc[0]["target_date"] == target_date
    assert history.iloc[0]["buffer"] == 5.0

def test_delete_project(sample_tasks):
    """Test Löschen eines Projekts."""
    project = "test_project"
    init_db()

    save_data(project, sample_tasks, pd.DataFrame())
    delete_project_complete(project)

    loaded = load_tasks(project)
    assert len(loaded) == 1  # Default-Task bleibt
    assert loaded.iloc[0]["Task Name"] == "Basis-Task"

# --- EDGE CASES ---
def test_multiple_projects():
    """Test mit mehreren Projekten."""
    init_db()
    
    tasks1 = pd.DataFrame({
        "Task Name": ["A"],
        "Duration (Days)": [5.0],
        "Beschreibung": [""]
    })
    tasks2 = pd.DataFrame({
        "Task Name": ["B"],
        "Duration (Days)": [15.0],
        "Beschreibung": [""]
    })

    save_data("proj1", tasks1, pd.DataFrame())
    save_data("proj2", tasks2, pd.DataFrame())

    loaded1 = load_tasks("proj1")
    loaded2 = load_tasks("proj2")

    assert loaded1.iloc[0]["Task Name"] == "A"
    assert loaded2.iloc[0]["Task Name"] == "B"

def test_simulation_with_large_n(sample_tasks):
    """Test mit großer Iterations-Anzahl."""
    risks = pd.DataFrame()
    std_risks = []
    n = 50000

    durations, impact_df = run_fast_simulation(sample_tasks, risks, std_risks, n)

    assert len(durations) == n
    # Zentraler Grenzwertsatz: Mittelwert sollte gegen Basis-Summe konvergieren
    assert np.isclose(durations.mean(), 30.0, rtol=0.01)

def test_simulation_probability_distribution(sample_tasks):
    """Test dass Wahrscheinlichkeits-Verteilung korrekt wirkt."""
    risks = pd.DataFrame({
        "Risk Name": ["Test"],
        "Risk Type": ["Binär"],
        "Target (Global/Task)": ["Global"],
        "Probability (0-1)": [0.5],
        "Impact Min": [1.0],
        "Impact Likely": [1.0],
        "Impact Max": [1.0],  # Konstanter Impact
        "Maßnahme / Mitigation": [""]
    })
    n = 10000

    durations, impact_df = run_fast_simulation(sample_tasks, risks, [], n)

    # Mit p=0.5 sollten ~50% der Läufe den Impact haben
    affected = np.sum(durations > 30)
    ratio = affected / n
    
    # Erlauben wir eine Toleranz von ±5% um Zufallsschwankungen zu erlauben
    assert 0.45 < ratio < 0.55, f"Expected ~50%, got {ratio*100:.1f}%"