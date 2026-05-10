from __future__ import annotations

import math
from bisect import bisect_right

NANOSECONDS_PER_SECOND = 1_000_000_000


class LiveBtcFeatureStore:
    """Rolling one-second BTC trade feature store for live snapshot models."""

    def __init__(self, *, buffer_seconds: int) -> None:
        self._buffer_seconds = int(buffer_seconds)
        self._prices_by_second: dict[int, float] = {}
        self._volumes_by_second: dict[int, float] = {}
        self._seconds: list[int] = []

    def record_trade(self, *, ts_ns: int, price: float, size: float) -> None:
        if not math.isfinite(price) or price <= 0.0:
            return
        second = int(ts_ns // NANOSECONDS_PER_SECOND)
        if second not in self._prices_by_second:
            self._seconds.append(second)
            self._seconds.sort()
        self._prices_by_second[second] = price
        self._volumes_by_second[second] = self._volumes_by_second.get(second, 0.0) + max(
            0.0,
            size if math.isfinite(size) else 0.0,
        )
        self._prune(current_second=second)

    def _prune(self, *, current_second: int) -> None:
        cutoff = current_second - self._buffer_seconds
        if not self._seconds or self._seconds[0] >= cutoff:
            return
        retained = [second for second in self._seconds if second >= cutoff]
        removed = set(self._seconds) - set(retained)
        for second in removed:
            self._prices_by_second.pop(second, None)
            self._volumes_by_second.pop(second, None)
        self._seconds = retained

    def price_at(self, ts: int) -> float:
        if not self._seconds:
            return math.nan
        index = bisect_right(self._seconds, int(ts)) - 1
        if index < 0:
            return math.nan
        return self._prices_by_second.get(self._seconds[index], math.nan)

    def observation_second_at(self, ts: int) -> int | None:
        if not self._seconds:
            return None
        index = bisect_right(self._seconds, int(ts)) - 1
        if index < 0:
            return None
        return self._seconds[index]

    def observation_age_seconds(self, ts: int) -> float:
        observed_second = self.observation_second_at(ts)
        if observed_second is None:
            return math.inf
        return float(int(ts) - observed_second)

    def momentum(self, ts: int, seconds: int) -> float:
        current = self.price_at(ts)
        prior = self.price_at(ts - seconds)
        if not math.isfinite(current) or not math.isfinite(prior):
            return math.nan
        return current - prior

    def volume(self, ts: int, seconds: int) -> float:
        start = int(ts) - int(seconds)
        end = int(ts)
        return float(
            sum(
                volume
                for second, volume in self._volumes_by_second.items()
                if start < second <= end
            )
        )

    def volatility(self, ts: int, seconds: int) -> float:
        prices = [self.price_at(second) for second in range(int(ts) - int(seconds), int(ts) + 1)]
        if len(prices) <= 2 or not all(math.isfinite(price) for price in prices):
            return math.nan
        deltas = [right - left for left, right in zip(prices, prices[1:], strict=False)]
        if len(deltas) <= 1:
            return 0.0
        mean = sum(deltas) / len(deltas)
        variance = sum((delta - mean) ** 2 for delta in deltas) / len(deltas)
        return math.sqrt(variance)
