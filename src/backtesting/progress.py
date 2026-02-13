"""Flicker-free progress bar using ANSI scroll regions.

Reserves the bottom line of the terminal for a pinned status bar.
Log events scroll naturally in the region above without ever clearing
or redrawing the bar, eliminating the flashing that tqdm.write() causes.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Iterator, TypeVar

T = TypeVar("T")

_ESC = "\033["
_CYAN = f"{_ESC}36m"
_DIM = f"{_ESC}2m"
_RESET = f"{_ESC}0m"


def _term_width() -> int:
    try:
        return os.get_terminal_size().columns
    except OSError:
        return 80


def _term_height() -> int:
    try:
        return os.get_terminal_size().lines
    except OSError:
        return 24


class PinnedProgress:
    """Iterator wrapper that pins a progress bar to the terminal bottom.

    Uses ANSI scroll-region escape codes so that normal print output
    scrolls only within the upper portion of the terminal.  The bottom
    line is reserved for the progress bar and updated in-place.
    """

    def __init__(
        self,
        iterable: Iterator[T],
        total: int,
        desc: str = "",
        unit: str = " it",
        refresh_interval: float = 0.05,
    ):
        self._iter = iter(iterable)
        self.total = total
        self.desc = desc
        self.unit = unit
        self._refresh_interval = refresh_interval
        self._n = 0
        self._start = time.monotonic()
        self._last_refresh = 0.0
        self._active = False

    # -- Setup / teardown --

    def _setup(self) -> None:
        """Reserve the bottom line by setting the scroll region."""
        rows = _term_height()
        sys.stdout.write(f"{_ESC}{rows};1H")  # move to last row
        sys.stdout.write(f"{_ESC}2K")  # clear it
        sys.stdout.write(f"{_ESC}1;{rows - 1}r")  # set scroll region
        sys.stdout.write(f"{_ESC}1;1H")  # move cursor into scroll region
        sys.stdout.flush()
        self._active = True
        self._refresh_bar()

    def _teardown(self) -> None:
        """Restore full terminal and print final bar state."""
        if not self._active:
            return
        rows = _term_height()
        sys.stdout.write(f"{_ESC}1;{rows}r")
        sys.stdout.write(f"{_ESC}{rows};1H\n")
        sys.stdout.flush()
        self._active = False

    # -- Bar rendering --

    def _refresh_bar(self) -> None:
        """Redraw the bar on the fixed bottom line."""
        now = time.monotonic()
        if not self._active:
            return

        elapsed = now - self._start
        rate = self._n / elapsed if elapsed > 0 else 0
        pct = self._n / self.total if self.total else 0

        # Time formatting
        elapsed_str = self._fmt_time(elapsed)
        if rate > 0 and self._n < self.total:
            remaining = (self.total - self._n) / rate
            remaining_str = self._fmt_time(remaining)
        else:
            remaining_str = "00:00"

        rate_str = f"{rate:,.0f}{self.unit}/s"

        # Build the bar
        width = _term_width()
        info_left = f"{self.desc}: {pct:>3.0%}|"
        info_right = f"| {self._n:,}/{self.total:,} [{elapsed_str}<{remaining_str}, {rate_str}]"
        bar_width = width - len(self._strip_ansi(info_left)) - len(info_right)

        if bar_width > 2:
            filled = int(bar_width * pct)
            bar = f"{_CYAN}{'█' * filled}{'░' * (bar_width - filled)}{_RESET}"
        else:
            bar = ""

        line = f"{info_left}{bar}{info_right}"

        rows = _term_height()
        sys.stdout.write(f"{_ESC}s{_ESC}{rows};1H{_ESC}2K{line}{_ESC}u")
        sys.stdout.flush()
        self._last_refresh = now

    @staticmethod
    def _strip_ansi(s: str) -> str:
        import re
        return re.sub(r"\033\[[0-9;]*m", "", s)

    @staticmethod
    def _fmt_time(seconds: float) -> str:
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    # -- Printing within the scroll region --

    def write(self, msg: str) -> None:
        """Print a message in the scroll region above the bar."""
        if self._active:
            sys.stdout.write(f"{msg}\n")
            sys.stdout.flush()
        else:
            print(msg)

    # -- Iterator protocol --

    def __iter__(self):
        self._setup()
        try:
            for item in self._iter:
                self._n += 1
                now = time.monotonic()
                if now - self._last_refresh >= self._refresh_interval:
                    self._refresh_bar()
                yield item
        finally:
            self._refresh_bar()  # final update
            self._teardown()
