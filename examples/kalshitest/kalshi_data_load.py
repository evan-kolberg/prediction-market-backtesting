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
"""
Example: Load historical Kalshi trade and bar data and write to a ParquetDataCatalog.

Usage::

    python examples/kalshitest/kalshi_data_load.py
"""

import asyncio

import pandas as pd

from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.persistence.catalog import ParquetDataCatalog


MARKET_TICKER = "KXBTC-25MAR15-B100000"
CATALOG_PATH = "./kalshi_catalog"


async def main() -> None:
    print(f"Loading data for market: {MARKET_TICKER}")

    loader = await KalshiDataLoader.from_market_ticker(MARKET_TICKER)
    print(f"Instrument: {loader.instrument}")

    # Load all available trades
    trades = await loader.load_trades()
    print(f"Loaded {len(trades)} trades")

    # Load hourly bars for the past 30 days
    end = pd.Timestamp.utcnow()
    start = end - pd.Timedelta(days=30)
    bars = await loader.load_bars(start=start, end=end, interval="Hours1")
    print(f"Loaded {len(bars)} hourly bars")

    # Write to Parquet catalog
    catalog = ParquetDataCatalog(CATALOG_PATH)
    if trades:
        catalog.write_data(trades)
        print(f"Wrote trades to {CATALOG_PATH}")
    if bars:
        catalog.write_data(bars)
        print(f"Wrote bars to {CATALOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
