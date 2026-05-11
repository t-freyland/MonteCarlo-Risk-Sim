# MonteCarlo Greenfield (No Migration)

Neuer Start ohne Migration aus `app.py`, aber mit den Learnings aus der alten App:

- **Klare Schichten**: `frontend` (UI), `backend` (API), `domain` (Logik)
- **Thin UI**: Frontend ruft nur HTTP-API auf
- **Single Source of Truth**: Persistenz und Simulation nur im Backend
- **Testbar**: Domain-Funktionen separat, API separat

## Arbeitsregel

- Die alte App in `MonteCarlo/` bleibt als Produktiv-App bestehen.
- Dort nur kleine, sichere Bugfixes und rückwärtskompatible Verbesserungen.
- Größere Architekturänderungen und neue Funktionen nur in `greenfield/`.
- Keine Migration der alten App auf die neue Architektur.

## Struktur

- `backend/`: FastAPI Service
- `frontend/`: Streamlit UI (API-Client only)
- `shared/`: optionale gemeinsame Contracts/DTOs

## Schnellstart

### 1) Backend

```powershell
cd greenfield/backend
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.api.main:app --reload --port 8010
```

### 2) Frontend

```powershell
cd greenfield/frontend
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
$env:MONTECARLO_API_URL = "http://localhost:8010"
streamlit run app.py
```
