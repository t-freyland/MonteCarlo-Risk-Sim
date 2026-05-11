from __future__ import annotations

from fastapi import FastAPI
import numpy as np

from src.api.schemas import SimRequest, SimResponse, PercentileEntry
from src.domain.simulation import run_simple_simulation

app = FastAPI(title="MonteCarlo Greenfield API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/simulate", response_model=SimResponse)
def simulate(body: SimRequest) -> SimResponse:
    durations = run_simple_simulation(
        base_duration_days=body.base_duration_days,
        runs=body.runs,
        sigma=body.sigma,
    )

    pcts = [50, 70, 80, 85, 90, 95]
    pct_days = [int(np.percentile(durations, p)) for p in pcts]

    sample_size = min(400, len(durations))
    sample = np.random.choice(durations, size=sample_size, replace=False).astype(float).tolist()

    return SimResponse(
        mean_days=float(np.mean(durations)),
        min_days=int(np.min(durations)),
        max_days=int(np.max(durations)),
        p85_days=int(np.percentile(durations, 85)),
        percentiles=[PercentileEntry(percentile=f"P{p}", days=d) for p, d in zip(pcts, pct_days)],
        durations_sample=sample,
    )
