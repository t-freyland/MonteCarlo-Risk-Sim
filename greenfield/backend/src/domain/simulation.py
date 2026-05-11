from __future__ import annotations

import numpy as np


def run_simple_simulation(base_duration_days: float, runs: int = 10000, sigma: float = 0.15) -> np.ndarray:
    """Minimal Greenfield simulation baseline.

    Uses lognormal samples around the base duration to avoid negative values.
    """
    if base_duration_days <= 0:
        raise ValueError("base_duration_days must be > 0")
    if runs < 1000:
        raise ValueError("runs must be >= 1000")

    mu = np.log(base_duration_days) - 0.5 * sigma**2
    durations = np.random.lognormal(mean=mu, sigma=sigma, size=runs)
    return durations.astype(float)
