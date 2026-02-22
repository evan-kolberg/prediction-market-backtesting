from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import cast

from simple_term_menu import TerminalMenu  # type: ignore[import-untyped]

from src.backtesting.strategy import Strategy

DIM = "\033[2m"
BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
RESET = "\033[0m"


def _ts() -> str:
    """Current wall-clock timestamp for log prefixing."""
    return f"{DIM}{datetime.now().strftime('%H:%M:%S')}{RESET}"


def _snake_to_title(s: str) -> str:
    return s.replace("_", " ").title()


def backtest(name: str | None = None, use_rust: bool = True):
    """Run a backtesting strategy by name or show interactive menu."""
    from src.backtesting.feeds.kalshi import KalshiFeed
    from src.backtesting.feeds.polymarket import PolymarketFeed

    strategies = Strategy.load()
    if not strategies:
        print("No strategies found in src/backtesting/examples/")
        return

    platforms = {
        "kalshi": ("Kalshi", lambda: KalshiFeed()),
        "polymarket": ("Polymarket", lambda: PolymarketFeed()),
    }

    if name:
        for strategy_cls in strategies:
            instance = strategy_cls()  # type: ignore[call-arg]
            if instance.name == name:
                _run_backtest_interactive(instance, platforms, use_rust=use_rust)
                return
        print(f"Strategy '{name}' not found. Available strategies:")
        for strategy_cls in strategies:
            instance = strategy_cls()  # type: ignore[call-arg]
            print(f"  - {instance.name}: {instance.description}")
        sys.exit(1)

    options = []
    for strategy_cls in strategies:
        instance = strategy_cls()  # type: ignore[call-arg]
        options.append(f"{_snake_to_title(instance.name)}: {instance.description}")
    options.append("[Exit]")

    menu = TerminalMenu(
        options,
        title="Select a strategy to backtest:",
        cycle_cursor=True,
        clear_screen=False,
    )
    choice = cast("int | None", menu.show())

    if choice is None or choice == len(options) - 1:
        print("Exiting.")
        return

    strategy_cls = strategies[choice]
    instance = strategy_cls()  # type: ignore[call-arg]
    _run_backtest_interactive(instance, platforms, use_rust=use_rust)


def _run_backtest_interactive(strategy, platforms: dict, use_rust: bool = True):
    """Select platform and run a backtest with the given strategy."""
    if not use_rust:
        from src.backtesting._archive.engine import Engine
    else:
        from src.backtesting.rust_engine import Engine

    def _pn(value: float, fmt: str) -> str:
        """Color a numeric value green if positive, red if negative."""
        color = GREEN if value >= 0 else RED
        return f"{color}{fmt.format(value)}{RESET}"

    platform_options = [f"{v[0]}" for v in platforms.values()]
    platform_options.append("[Exit]")

    menu = TerminalMenu(
        platform_options,
        title="Select data source:",
        cycle_cursor=True,
        clear_screen=False,
    )
    choice = cast("int | None", menu.show())

    if choice is None or choice == len(platform_options) - 1:
        print("Exiting.")
        return

    platform_key = list(platforms.keys())[choice]
    _, feed_factory = platforms[platform_key]
    feed = feed_factory()

    sample_options = [
        "Top 1% by volume (recommended — captures most trading activity)",
        "Top 5% by volume",
        "Top 10% by volume",
        "Top 20% by volume",
        "Top 50% by volume",
        "100% — all markets (not recommended — very slow, mostly illiquid)",
    ]
    sample_menu = TerminalMenu(
        sample_options,
        title=(
            "Market selection (by trading volume):\n"
            "  A small fraction of markets account for the vast majority of\n"
            "  trading volume on prediction market platforms. Filtering to\n"
            "  the top percentile gives you the most realistic backtest."
        ),
        cycle_cursor=True,
        clear_screen=False,
    )
    sample_choice = cast("int | None", sample_menu.show())
    sample_map: dict[int, float | None] = {0: 0.01, 1: 0.05, 2: 0.1, 3: 0.2, 4: 0.5, 5: None}
    market_sample = sample_map.get(sample_choice) if sample_choice is not None else 0.01

    sample_label = sample_options[sample_choice].split(" (")[0] if sample_choice is not None else "Top 1%"

    print(f"\n{_ts()}  Running backtest: {strategy.name} on {platform_key}")
    print(f"{_ts()}  Strategy:         {strategy.description}")
    print(f"{_ts()}  Initial cash:     ${strategy.initial_cash:,.2f}")
    print(f"{_ts()}  Market sample:    {sample_label}\n")

    print(f"{_ts()}  Warming up... Loading markets and indexing trades.")
    print(f"{_ts()}  Keep an eye on your memory usage.\n")

    engine = Engine(feed=feed, strategy=strategy, market_sample=market_sample)
    result = engine.run()

    m = result.metrics
    total_ret = m.get("total_return", 0)
    ann_ret = m.get("annualized_return", 0)
    sharpe = m.get("sharpe_ratio", 0)
    sortino = m.get("sortino_ratio", 0)
    max_dd = m.get("max_drawdown", 0)
    win_rate = m.get("win_rate", 0)
    pf = m.get("profit_factor", 0)
    avg_pnl = m.get("avg_trade_pnl", 0)
    commission = m.get("total_commission", 0)

    equity_color = GREEN if result.final_equity >= result.initial_cash else RED

    print(f"\n{_ts()}  {BOLD}Backtest Results: {result.strategy_name}{RESET}")
    print(f"{_ts()}  Platform:         {result.platform.value}")
    print(f"{_ts()}  Period:           {result.start_time} -> {result.end_time}")
    print(f"{_ts()}  Initial cash:     ${result.initial_cash:,.2f}")
    print(f"{_ts()}  Final equity:     {equity_color}${result.final_equity:,.2f}{RESET}")
    print(f"{_ts()}  Markets traded:   {result.num_markets_traded}")
    print(f"{_ts()}  Markets resolved: {result.num_markets_resolved}")
    print(f"{_ts()}  Total fills:      {int(m.get('num_fills', 0))}")
    print()

    print(f"{_ts()}  {BOLD}Performance:{RESET}")
    print(f"{_ts()}    Total return:   {_pn(total_ret, '{:.2%}')}")
    print(f"{_ts()}    Annualized:     {_pn(ann_ret, '{:.2%}')}")
    print(f"{_ts()}    Sharpe ratio:   {_pn(sharpe, '{:.3f}')}")
    print(f"{_ts()}    Sortino ratio:  {_pn(sortino, '{:.3f}')}")
    print(f"{_ts()}    Max drawdown:   {RED}{max_dd:.2%}{RESET}")
    print()

    print(f"{_ts()}  {BOLD}Trading:{RESET}")
    wr_color = GREEN if win_rate >= 0.5 else RED
    print(f"{_ts()}    Win rate:       {wr_color}{win_rate:.2%}{RESET}")
    print(f"{_ts()}    Profit factor:  {GREEN if pf >= 1 else RED}{pf:.3f}{RESET}")
    print(f"{_ts()}    Avg trade P&L:  {_pn(avg_pnl, '${:.4f}')}")
    print(f"{_ts()}    Commission:     ${commission:.2f}\n")

    if result.event_log:
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"backtest_{result.strategy_name}_{result.platform.value}.log"
        log_path.write_text("\n".join(result.event_log) + "\n")
        print(f"{_ts()}  Event log: {log_path} ({len(result.event_log)} events)\n")

    if result.equity_curve:
        plot_options = ["Open interactive chart", "Save chart to HTML only", "Skip"]
        plot_menu = TerminalMenu(
            plot_options,
            title="Plot results?",
            cycle_cursor=True,
            clear_screen=False,
        )
        plot_choice = cast("int | None", plot_menu.show())
        if plot_choice == 0:
            out_html = f"output/backtest_{result.strategy_name}_{result.platform.value}.html"
            print(f"\n{_ts()}  Rendering interactive chart -> {out_html}...")
            result.plot(filename=out_html, open_browser=True)
        elif plot_choice == 1:
            out_html = f"output/backtest_{result.strategy_name}_{result.platform.value}.html"
            print(f"\n{_ts()}  Saving chart to {out_html}...")
            result.plot(filename=out_html, open_browser=False)
            print(f"{_ts()}  {GREEN}Saved.{RESET}\n")


def _fmt_volume(v: float | None) -> str:
    """Format a dollar volume for compact display (e.g. $1.2M, $890K)."""
    if v is None:
        return "    N/A"
    try:
        v = float(v)
    except Exception:
        return "    N/A"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v / 1_000:.1f}K"
    else:
        return f"${v:.0f}"


def _print_market_table(rows: list[dict]) -> None:
    """Print a colour-coded table of Polymarket market data."""
    CYAN = "\033[36m"
    header = f"  {'#':>3}  {'Market Question':<55}  {'YES':>6}  {'Vol 24h':>8}  {'Closes'}"
    sep = f"  {'─' * 3}  {'─' * 55}  {'─' * 6}  {'─' * 8}  {'─' * 10}"
    print(f"\n{CYAN}{header}{RESET}")
    print(f"{DIM}{sep}{RESET}")
    for i, row in enumerate(rows, 1):
        title = (row.get("title") or "Unknown")[:55]
        yp = row.get("yes_price")
        if yp is not None:
            yes_str = f"{yp:.1%}"
            yes_color = GREEN if yp >= 0.5 else RED
        else:
            yes_str = "  N/A"
            yes_color = DIM
        vol = _fmt_volume(row.get("volume_24h"))
        end = (row.get("end_date") or "")[:10] or "  N/A    "
        print(f"  {DIM}{i:>3}{RESET}  {title:<55}  {yes_color}{yes_str:>6}{RESET}  {vol:>8}  {DIM}{end}{RESET}")
    print()


def fronttest(name: str | None = None):
    """Run a strategy against live market data (paper trading)."""
    import asyncio

    from src.backtesting.front_test_engine import FrontTestEngine

    strategies = Strategy.load()
    if not strategies:
        print("No strategies found in src/backtesting/strategies/")
        return

    # Select strategy
    if name:
        instance = None
        for strategy_cls in strategies:
            inst = strategy_cls()  # type: ignore[call-arg]
            if inst.name == name:
                instance = inst
                break
        if instance is None:
            print(f"Strategy '{name}' not found. Available strategies:")
            for strategy_cls in strategies:
                inst = strategy_cls()  # type: ignore[call-arg]
                print(f"  - {inst.name}: {inst.description}")
            sys.exit(1)
    else:
        options = []
        for strategy_cls in strategies:
            inst = strategy_cls()  # type: ignore[call-arg]
            options.append(f"{_snake_to_title(inst.name)}: {inst.description}")
        options.append("[Exit]")

        menu = TerminalMenu(
            options,
            title="Select a strategy for front testing:",
            cycle_cursor=True,
            clear_screen=False,
        )
        choice = cast("int | None", menu.show())
        if choice is None or choice == len(options) - 1:
            print("Exiting.")
            return
        strategy_cls = strategies[choice]
        instance = strategy_cls()  # type: ignore[call-arg]

    # Select platform — only Polymarket is active; Kalshi is shown but blocked.
    platform_options = ["Kalshi {inactive}", "Polymarket", "[Exit]"]
    menu = TerminalMenu(
        platform_options,
        title="Select live data source:",
        cycle_cursor=True,
        clear_screen=False,
    )
    platform_choice = cast("int | None", menu.show())
    if platform_choice is None or platform_choice == 2:
        print("Exiting.")
        return

    if platform_choice == 0:
        print(f"\n{_ts()}  {RED}Kalshi front testing is not yet available. Coming soon.{RESET}")
        return

    # --- Polymarket (auto-selects a sample of active markets) ---
    from src.backtesting.feeds.polymarket_live import PolymarketLiveFeed, fetch_polymarket_markets

    # Allow manual override, but default to auto-selecting 30 active markets
    print(
        "\nEnter Polymarket condition IDs (comma-separated) or press Enter to auto-select ~30 active markets by volume:"
    )
    raw = input("> ").strip()

    if raw:
        condition_ids = [c.strip() for c in raw.split(",") if c.strip()]
        feed = PolymarketLiveFeed(condition_ids=condition_ids)
    else:
        print(f"\n{_ts()}  Fetching active Polymarket markets...")
        gamma_markets = fetch_polymarket_markets(30)
        if not gamma_markets:
            print(f"{_ts()}  {RED}Could not fetch markets from Polymarket. Exiting.{RESET}")
            return

        condition_ids = [m["conditionId"] for m in gamma_markets if m.get("conditionId")]
        print(f"{_ts()}  {GREEN}Found {len(condition_ids)} markets.{RESET}")

        # Build display rows from Gamma API data (no CLOB call needed here)
        import json as _json

        display_rows = []
        for m in gamma_markets:
            cid = m.get("conditionId", "")
            if not cid:
                continue
            yes_price = None
            try:
                prices_raw = m.get("outcomePrices", "[]")
                prices = _json.loads(prices_raw) if isinstance(prices_raw, str) else list(prices_raw)
                if prices:
                    yes_price = float(prices[0])
            except Exception:
                pass
            end_raw = m.get("endDate") or m.get("endDateIso", "")
            display_rows.append(
                {
                    "title": m.get("question", cid[:40]),
                    "yes_price": yes_price,
                    "volume_24h": m.get("volume24hr") or m.get("oneDayVolume"),
                    "end_date": (end_raw[:10] if end_raw else None),
                }
            )

        _print_market_table(display_rows)
        feed = PolymarketLiveFeed(condition_ids=condition_ids, gamma_data=gamma_markets)

    engine = FrontTestEngine(feed=feed, strategy=instance)
    asyncio.run(engine.run())


def main():
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  uv run main.py backtest [strategy_name] [--python]")
        print("  uv run main.py fronttest [strategy_name]")
        sys.exit(0)

    command = sys.argv[1]

    if command == "backtest":
        args = sys.argv[2:]
        use_rust = "--python" not in args
        remaining = [a for a in args if a != "--python"]
        name = remaining[0] if remaining else None
        backtest(name, use_rust=use_rust)
        sys.exit(0)

    if command == "fronttest":
        args = sys.argv[2:]
        name = args[0] if args else None
        fronttest(name)
        sys.exit(0)

    print(f"Unknown command: {command}")
    print("Usage:")
    print("  uv run main.py backtest [strategy_name] [--python]")
    print("  uv run main.py fronttest [strategy_name]")
    sys.exit(1)


if __name__ == "__main__":
    main()
