"""Prediction market extensions for NautilusTrader."""

from __future__ import annotations


def install_commission_patch() -> None:
    """
    Monkey-patch upstream ``calculate_commission`` with the corrected fee curve.

    Upstream uses ``qty * price * fee_rate_bps_as_fraction`` (linear).
    The correct Polymarket formula is ``qty * feeRate * p * (1 - p)`` (curved),
    which peaks at p=0.50 and tapers toward the extremes.

    This is the only startup hook required. Call once at process start.
    """
    import nautilus_trader.adapters.polymarket.common.parsing as upstream_parsing

    from prediction_market_extensions.adapters.polymarket import parsing as pm_parsing

    upstream_parsing.calculate_commission = pm_parsing.calculate_commission
