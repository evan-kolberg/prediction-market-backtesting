# License Notes

This repository uses mixed licensing because it extends
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader), which is
licensed under the
[GNU Lesser General Public License v3.0 or later (LGPL-3.0-or-later)](https://www.gnu.org/licenses/lgpl-3.0.en.html).

## Scope

| Scope | License | File |
|---|---|---|
| `prediction_market_extensions/` NautilusTrader extension package | LGPL-3.0-or-later | [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/NOTICE), [`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING.LESSER), [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING) |
| Root files with a "Derived from NautilusTrader" or "Modified by Evan Kolberg" notice | LGPL-3.0-or-later | [`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING.LESSER), [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING) |
| Everything else such as `main.py`, `Makefile`, docs, and repo metadata | MIT | [`LICENSE-MIT`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/LICENSE-MIT) |

The full LGPL and GPL texts are in
[`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING.LESSER)
and [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/COPYING).
The [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/NOTICE) file lists every
LGPL-covered file in the active tree, along with
modification dates and upstream lineage.

Notebook runner files under `backtests/` follow the same rule as `.py` runners:
license scope depends on file-level provenance, not on the file suffix. A
notebook that simply orchestrates repo runners is not automatically LGPL-covered
unless it also carries Nautilus-derived provenance.

## NautilusTrader Attribution

Local extensions live under `prediction_market_extensions/` in their own
namespace, importing from and subclassing upstream base classes. Those files
carry LGPL provenance headers where applicable and are listed in
[`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/NOTICE).
The earlier vendored NautilusTrader tree and `_nautilus_overrides/` overlay were
removed from the worktree on this branch; provenance lives in git history.

## Practical Meaning

- using this repo as-is: no extra action needed
- forking or redistributing: keep the LGPL license files, the
  [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/NOTICE), and the per-file modification headers intact
- if you modify `prediction_market_extensions/`, preserve those file-level
  notices the same way you would for any other LGPL-covered extension
- linking against LGPL-covered modules in a proprietary project: the LGPL still
  requires users to be able to relink against modified versions of that code

Use [`LICENSE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/LICENSE)
for the top-level guide and
[`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/NOTICE)
for the file-by-file breakdown.
