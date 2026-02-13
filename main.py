from __future__ import annotations

import sys
from pathlib import Path
from typing import cast

from simple_term_menu import TerminalMenu  # type: ignore[import-untyped]

from src.backtesting.strategy import Strategy


def _snake_to_title(s: str) -> str:
    return s.replace("_", " ").title()


def backtest(name: str | None = None):
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
                _run_backtest_interactive(instance, platforms)
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
    _run_backtest_interactive(instance, platforms)


def _run_backtest_interactive(strategy, platforms: dict):
    """Select platform and run a backtest with the given strategy."""
    from src.backtesting.engine import Engine

    CYAN = "\033[36m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

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

    sample_options = ["100% (all markets)", "50%", "20%", "10%"]
    sample_menu = TerminalMenu(
        sample_options,
        title="Market sample size:",
        cycle_cursor=True,
        clear_screen=False,
    )
    sample_choice = cast("int | None", sample_menu.show())
    sample_map: dict[int, float | None] = {0: None, 1: 0.5, 2: 0.2, 3: 0.1}
    market_sample = sample_map.get(sample_choice) if sample_choice is not None else None

    sample_label = sample_options[sample_choice] if sample_choice is not None else "100%"

    print(f"\n{BOLD}{CYAN}Running backtest: {strategy.name} on {platform_key}{RESET}")
    print(f"  {DIM}Strategy:{RESET}     {strategy.description}")
    print(f"  {DIM}Initial cash:{RESET} $10,000.00")
    print(f"  {DIM}Market sample:{RESET} {sample_label}\n")

    YELLOW = "\033[33m"
    print(f"  {YELLOW}{BOLD}Warming up...{RESET} Loading markets and indexing trades.")
    print(f"  {DIM}Keep an eye on your memory usage.{RESET}\n")

    engine = Engine(feed=feed, strategy=strategy, initial_cash=10_000.0, market_sample=market_sample)
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

    print(f"\n  {BOLD}{CYAN}Backtest Results: {result.strategy_name}{RESET}")
    print(f"  {DIM}Platform:{RESET}         {result.platform.value}")
    print(f"  {DIM}Period:{RESET}           {result.start_time} -> {result.end_time}")
    print(f"  {DIM}Initial cash:{RESET}     ${result.initial_cash:,.2f}")
    print(f"  {DIM}Final equity:{RESET}     {equity_color}${result.final_equity:,.2f}{RESET}")
    print(f"  {DIM}Markets traded:{RESET}   {result.num_markets_traded}")
    print(f"  {DIM}Markets resolved:{RESET} {result.num_markets_resolved}")
    print(f"  {DIM}Total fills:{RESET}      {int(m.get('num_fills', 0))}")
    print()

    print(f"  {BOLD}Performance:{RESET}")
    print(f"    {DIM}Total return:{RESET}   {_pn(total_ret, '{:.2%}')}")
    print(f"    {DIM}Annualized:{RESET}     {_pn(ann_ret, '{:.2%}')}")
    print(f"    {DIM}Sharpe ratio:{RESET}   {_pn(sharpe, '{:.3f}')}")
    print(f"    {DIM}Sortino ratio:{RESET}  {_pn(sortino, '{:.3f}')}")
    print(f"    {DIM}Max drawdown:{RESET}   {RED}{max_dd:.2%}{RESET}")
    print()

    print(f"  {BOLD}Trading:{RESET}")
    wr_color = GREEN if win_rate >= 0.5 else RED
    print(f"    {DIM}Win rate:{RESET}       {wr_color}{win_rate:.2%}{RESET}")
    print(f"    {DIM}Profit factor:{RESET}  {GREEN if pf >= 1 else RED}{pf:.3f}{RESET}")
    print(f"    {DIM}Avg trade P&L:{RESET}  {_pn(avg_pnl, '${:.4f}')}")
    print(f"    {DIM}Commission:{RESET}     ${commission:.2f}\n")

    if result.event_log:
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / f"backtest_{result.strategy_name}_{result.platform.value}.log"
        log_path.write_text("\n".join(result.event_log) + "\n")
        print(f"  {DIM}Event log:{RESET}      {log_path} ({len(result.event_log)} events)\n")


def main():
    if len(sys.argv) < 2:
        print("\nUsage: uv run main.py backtest [strategy_name]")
        sys.exit(0)

    command = sys.argv[1]

    if command == "backtest":
        name = sys.argv[2] if len(sys.argv) > 2 else None
        backtest(name)
        sys.exit(0)

    print(f"Unknown command: {command}")
    print("Usage: uv run main.py backtest [strategy_name]")
    sys.exit(1)


if __name__ == "__main__":
    main()
