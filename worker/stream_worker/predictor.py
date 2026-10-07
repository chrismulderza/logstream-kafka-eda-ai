"""Rolling-window time-to-exhaustion (TTE) using numpy."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

import numpy as np

# Alert type consumed by Event-Driven Ansible rules. Do not change.
ALERT_TYPE = "PREEMPTIVE_STORAGE_EXHAUSTION_RISK"


@dataclass(frozen=True)
class ExhaustionEstimate:
    tte_seconds: float
    rate_per_second: float
    used_current: float
    capacity: float
    samples: int
    window_span_seconds: float


class RollingWindowStore:
    """Per (host, instance) used/capacity samples over a rolling time window."""

    def __init__(self, window_seconds: float, min_samples: int) -> None:
        self.window_seconds = window_seconds
        self.min_samples = min_samples
        self._points: dict[tuple[str, str], deque[tuple[float, float, float]]] = defaultdict(
            deque
        )

    def add(
        self,
        host: str,
        instance: str,
        timestamp: float,
        used: float,
        capacity: float,
    ) -> None:
        key = (host, instance)
        series = self._points[key]
        series.append((float(timestamp), float(used), float(capacity)))
        cutoff = timestamp - self.window_seconds
        while series and series[0][0] < cutoff:
            series.popleft()

    def estimate(self, host: str, instance: str) -> ExhaustionEstimate | None:
        series = self._points.get((host, instance))
        if series is None or len(series) < self.min_samples:
            return None

        times = np.asarray([p[0] for p in series], dtype=np.float64)
        used = np.asarray([p[1] for p in series], dtype=np.float64)
        capacity = float(series[-1][2])
        used_current = float(used[-1])
        span = float(times[-1] - times[0])
        if span <= 0:
            return None

        rate = _usage_rate(times, used)
        # Non-positive fill rate: usage is flat or shrinking — no exhaustion alert.
        if rate <= 0:
            return None

        remaining = capacity - used_current
        tte = 0.0 if remaining <= 0 else float(remaining / rate)
        return ExhaustionEstimate(
            tte_seconds=tte,
            rate_per_second=float(rate),
            used_current=used_current,
            capacity=capacity,
            samples=int(len(series)),
            window_span_seconds=span,
        )


def _usage_rate(times: np.ndarray, used: np.ndarray) -> float:
    """dUsed/dt via least-squares slope on the rolling window (units per second)."""
    t0 = times - times[0]
    design = np.column_stack((t0, np.ones(len(t0))))
    slope, _intercept = np.linalg.lstsq(design, used, rcond=None)[0]
    return float(slope)
