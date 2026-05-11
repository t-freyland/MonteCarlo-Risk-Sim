from __future__ import annotations

from pydantic import BaseModel, Field


class SimRequest(BaseModel):
    base_duration_days: float = Field(..., gt=0)
    runs: int = Field(10000, ge=1000, le=100000)
    sigma: float = Field(0.15, gt=0, le=1.0)


class PercentileEntry(BaseModel):
    percentile: str
    days: int


class SimResponse(BaseModel):
    mean_days: float
    min_days: int
    max_days: int
    p85_days: int
    percentiles: list[PercentileEntry]
    durations_sample: list[float]
