from __future__ import annotations

import os
from typing import Any

import httpx


class ApiClient:
    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or os.getenv("MONTECARLO_API_URL", "http://localhost:8010")).rstrip("/")

    def health(self) -> dict[str, Any]:
        resp = httpx.get(f"{self.base_url}/health", timeout=5.0)
        resp.raise_for_status()
        return resp.json()

    def simulate(self, base_duration_days: float, runs: int, sigma: float) -> dict[str, Any]:
        payload = {
            "base_duration_days": base_duration_days,
            "runs": runs,
            "sigma": sigma,
        }
        resp = httpx.post(f"{self.base_url}/simulate", json=payload, timeout=60.0)
        resp.raise_for_status()
        return resp.json()
