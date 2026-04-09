# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#
"""
Corrected Polymarket fee calculation.

Upstream ``nautilus_trader.adapters.polymarket.common.parsing.calculate_commission``
uses a linear formula (``qty * price * fee_rate``).  Polymarket's actual fee
curve is ``qty * feeRate * p * (1 - p)`` which peaks at p=0.50 and tapers
toward the extremes.

This module provides the corrected formula and is installed as a monkey-patch
via ``prediction_market_extensions.install_commission_patch()``.
"""

from __future__ import annotations

from decimal import Decimal
from decimal import ROUND_HALF_UP


def basis_points_as_decimal(basis_points: Decimal) -> Decimal:
    """
    Convert basis points to a decimal fraction.

    Parameters
    ----------
    basis_points : Decimal
        The fee rate in basis points (1 bp = 0.01%).

    Returns
    -------
    Decimal
        The decimal fraction (e.g., 100 bp -> 0.01).

    """
    return basis_points / Decimal(10_000)


def infer_fee_exponent(fee_rate_bps: Decimal) -> int:
    """
    Return the legacy Polymarket fee exponent compatibility value.

    Older code paths inferred different exponents by market type. Polymarket's
    current fee documentation uses one shared fee curve for all fee-enabled
    categories, so callers should treat the exponent as ``1``.

    Parameters
    ----------
    fee_rate_bps : Decimal
        The fee rate in basis points. Retained for API compatibility.

    Returns
    -------
    int
        Always ``1``.

    """
    del fee_rate_bps
    return 1


def calculate_commission(
    quantity: Decimal,
    price: Decimal,
    fee_rate_bps: Decimal,
    fee_exponent: int = 1,
    **_kwargs: object,
) -> float:
    """
    Calculate commission from trade parameters and fee rate.

    Polymarket's current fee formula is::

        fee = C x feeRate x p x (1 - p)

    Where:
    - C = number of shares (quantity)
    - p = share price
    - feeRate = fee_rate_bps / 10_000

    The fee peaks at p = 0.50 and decreases symmetrically toward the
    extremes (p -> 0 or p -> 1).

    Polymarket rounds fees to 5 decimal places (0.00001 USDC minimum).

    References
    ----------
    https://docs.polymarket.com/trading/fees

    Parameters
    ----------
    quantity : Decimal
        The fill quantity.
    price : Decimal
        The fill price (0 to 1).
    fee_rate_bps : Decimal
        The fee rate in basis points.
    fee_exponent : int, default 1
        Retained for backward compatibility with <= 1.225.0 and ignored.

    Returns
    -------
    float
        The commission amount rounded to 5 decimal places.

    """
    if fee_rate_bps <= 0:
        return 0.0

    del fee_exponent
    fee_rate = basis_points_as_decimal(fee_rate_bps)
    commission = quantity * fee_rate * price * (Decimal(1) - price)
    return float(commission.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP))
