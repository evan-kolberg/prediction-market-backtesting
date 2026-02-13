"""Performance metric computation for backtest results.

Computes risk/return metrics from equity curve snapshots and fill records.
Uses only stdlib math — no numpy dependency required.
"""

from __future__ import annotations

import math
from datetime import timedelta

from src.backtesting.models import Fill, PortfolioSnapshot


def compute_metrics(
    equity_curve: list[PortfolioSnapshot],
    fills: list[Fill],
    initial_cash: float,
    market_pnls: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute performance metrics from backtest results.

    Args:
        equity_curve: Time-ordered portfolio snapshots.
        fills: All order fills from the backtest.
        initial_cash: Starting cash balance.
        market_pnls: Realized P&L per market (including resolution payouts).
            If None, falls back to fill-only P&L estimation.

    Returns:
        Dict of metric_name -> value.
    """
    if not equity_curve:
        return _empty_metrics()

    metrics: dict[str, float] = {}
    equities = [s.total_equity for s in equity_curve]
    final = equities[-1]

    # Return metrics
    metrics["total_return"] = (final - initial_cash) / initial_cash if initial_cash != 0 else 0.0
    metrics["final_equity"] = final

    duration = equity_curve[-1].timestamp - equity_curve[0].timestamp
    years = duration.total_seconds() / (365.25 * 86400)

    if years < 1e-6 or metrics["total_return"] <= -1.0:
        metrics["annualized_return"] = 0.0 if years < 1e-6 else -1.0
    else:
        try:
            metrics["annualized_return"] = (1.0 + metrics["total_return"]) ** (1.0 / years) - 1.0
        except OverflowError:
            metrics["annualized_return"] = float("inf") if metrics["total_return"] > 0 else float("-inf")

    # Risk metrics from equity curve returns
    if len(equities) >= 2:
        returns = [
            (equities[i] - equities[i - 1]) / equities[i - 1] if equities[i - 1] != 0 else 0.0
            for i in range(1, len(equities))
        ]
        avg_ret = sum(returns) / len(returns)
        std_ret = _std(returns)
        downside = [r for r in returns if r < 0]
        downside_std = _std(downside)

        snapshots_per_year = len(equities) / max(years, 1e-6)
        sqrt_factor = math.sqrt(snapshots_per_year) if snapshots_per_year > 0 else 1.0

        metrics["sharpe_ratio"] = (avg_ret / std_ret * sqrt_factor) if std_ret > 0 else 0.0
        metrics["sortino_ratio"] = (avg_ret / downside_std * sqrt_factor) if downside_std > 0 else 0.0
    else:
        metrics["sharpe_ratio"] = 0.0
        metrics["sortino_ratio"] = 0.0

    # Drawdown
    peak = equities[0]
    max_dd = 0.0
    max_dd_duration = timedelta(0)
    dd_start = equity_curve[0].timestamp
    for i, eq in enumerate(equities):
        if eq > peak:
            peak = eq
            dd_start = equity_curve[i].timestamp
        dd = (peak - eq) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            max_dd_duration = equity_curve[i].timestamp - dd_start
    metrics["max_drawdown"] = max_dd
    metrics["max_drawdown_duration_days"] = max_dd_duration.total_seconds() / 86400

    # Trade metrics
    metrics["num_fills"] = float(len(fills))
    metrics["total_commission"] = sum(f.commission for f in fills)

    traded_markets = {f.market_id for f in fills}

    if fills:
        # Use portfolio-tracked P&L (includes resolution payouts) when available,
        # otherwise fall back to fill-only estimation
        if market_pnls is not None:
            pnl_list = [market_pnls[mid] for mid in traded_markets if mid in market_pnls]
        else:
            pnl_list = _market_pnls(fills)

        wins = [p for p in pnl_list if p > 0]
        losses = [p for p in pnl_list if p < 0]

        metrics["num_market_trades"] = float(len(pnl_list))
        metrics["win_rate"] = len(wins) / len(pnl_list) if pnl_list else 0.0
        metrics["avg_trade_pnl"] = sum(pnl_list) / len(pnl_list) if pnl_list else 0.0
        metrics["avg_win"] = sum(wins) / len(wins) if wins else 0.0
        metrics["avg_loss"] = sum(losses) / len(losses) if losses else 0.0
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        metrics["profit_factor"] = gross_profit / gross_loss if gross_loss > 0 else float("inf")
        metrics["total_realized_pnl"] = sum(pnl_list)
    else:
        metrics["num_market_trades"] = 0.0
        metrics["win_rate"] = 0.0
        metrics["avg_trade_pnl"] = 0.0
        metrics["avg_win"] = 0.0
        metrics["avg_loss"] = 0.0
        metrics["profit_factor"] = 0.0
        metrics["total_realized_pnl"] = 0.0

    return metrics


def _std(values: list[float]) -> float:
    """Population standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _market_pnls(fills: list[Fill]) -> list[float]:
    """Compute net P&L per market from fills (buy = cost, sell = proceeds)."""
    by_market: dict[str, float] = {}
    for fill in fills:
        if fill.market_id not in by_market:
            by_market[fill.market_id] = 0.0
        if fill.action.value == "buy":
            by_market[fill.market_id] -= fill.price * fill.quantity + fill.commission
        else:
            by_market[fill.market_id] += fill.price * fill.quantity - fill.commission
    return list(by_market.values())


def _empty_metrics() -> dict[str, float]:
    """Return all metrics as zero."""
    return {
        "total_return": 0.0,
        "final_equity": 0.0,
        "annualized_return": 0.0,
        "sharpe_ratio": 0.0,
        "sortino_ratio": 0.0,
        "max_drawdown": 0.0,
        "max_drawdown_duration_days": 0.0,
        "num_fills": 0.0,
        "total_commission": 0.0,
        "num_market_trades": 0.0,
        "win_rate": 0.0,
        "avg_trade_pnl": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "profit_factor": 0.0,
        "total_realized_pnl": 0.0,
    }
