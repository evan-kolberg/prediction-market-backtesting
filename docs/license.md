# License Notes

This repository uses mixed licensing because it vendors, extends, and locally
overrides
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader), which is
licensed under the
[GNU Lesser General Public License v3.0 or later (LGPL-3.0-or-later)](https://www.gnu.org/licenses/lgpl-3.0.en.html).

## Scope

| Scope | License | File |
|---|---|---|
| `nautilus_pm/` temporary vendored NautilusTrader reference tree | LGPL-3.0-or-later | [`nautilus_pm/LICENSE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/nautilus_pm/LICENSE) |
| `_nautilus_overrides/` active local Nautilus override layer | LGPL-3.0-or-later | [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/NOTICE), [`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING.LESSER), [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING) |
| Root files with a "Derived from NautilusTrader" or "Modified by Evan Kolberg" notice | LGPL-3.0-or-later | [`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING.LESSER), [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING) |
| Everything else such as `main.py`, `Makefile`, docs, and repo metadata | MIT | [`LICENSE-MIT`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/LICENSE-MIT) |

The full LGPL and GPL texts are in
[`COPYING.LESSER`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING.LESSER)
and [`COPYING`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/COPYING).
The [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/NOTICE) file lists every
LGPL-covered file outside the temporary vendored reference tree, along with
modification dates and upstream lineage.

## NautilusTrader Attribution

This project keeps a vendored copy of
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
(Copyright 2015-2026 Nautech Systems Pty Ltd) under `nautilus_pm/` as a
temporary reference/provenance tree during the upstream migration.

The upstream LGPL-3.0-or-later license and copyright notices are preserved in
[`nautilus_pm/LICENSE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/nautilus_pm/LICENSE).

Active runtime overrides now live under `_nautilus_overrides/nautilus_trader/`.
Those files also carry LGPL provenance headers where applicable and are listed
in [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/NOTICE).
The vendored `nautilus_pm/` tree remains available for provenance comparison,
and the most recent subtree sync references upstream split commit `f51c805c9f`.

## Practical Meaning

- using this repo as-is: no extra action needed
- forking or redistributing: keep the LGPL license files, the
  [`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/NOTICE), and the per-file modification headers intact
- if you modify `_nautilus_overrides/`, preserve those file-level notices the
  same way you would for the older vendored tree
- linking against LGPL-covered modules in a proprietary project: the LGPL still
  requires users to be able to relink against modified versions of that code

Use [`LICENSE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/LICENSE)
for the top-level guide and
[`NOTICE`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/NOTICE)
for the file-by-file breakdown.
