from __future__ import annotations

import os
from inspect import isawaitable
from inspect import iscoroutinefunction
from collections.abc import Awaitable
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec
from typing import TypeVar


P = ParamSpec("P")
T = TypeVar("T")

ENABLE_TIMING_ENV = "BACKTEST_ENABLE_TIMING"


def _timing_enabled() -> bool:
    value = os.getenv(ENABLE_TIMING_ENV)
    if value is None:
        return True
    return value.strip().casefold() not in {"0", "false", "no", "off"}


def install_timing_harness() -> None:
    if not _timing_enabled():
        return

    from backtests._shared._timing_test import install_timing

    install_timing()


def timing_harness(
    func: Callable[P, T] | Callable[P, Awaitable[T]] | None = None,
) -> (
    Callable[
        [Callable[P, T] | Callable[P, Awaitable[T]]],
        Callable[P, T] | Callable[P, Awaitable[T]],
    ]
    | Callable[P, T]
    | Callable[P, Awaitable[T]]
):
    def decorator(
        run_func: Callable[P, T] | Callable[P, Awaitable[T]],
    ) -> Callable[P, T] | Callable[P, Awaitable[T]]:
        if iscoroutinefunction(run_func):

            @wraps(run_func)
            async def wrapped_async(*args: P.args, **kwargs: P.kwargs) -> T:
                install_timing_harness()
                result = run_func(*args, **kwargs)
                assert isawaitable(result)
                return await result

            return wrapped_async

        @wraps(run_func)
        def wrapped_sync(*args: P.args, **kwargs: P.kwargs) -> T:
            install_timing_harness()
            return run_func(*args, **kwargs)

        return wrapped_sync

    if func is None:
        return decorator
    return decorator(func)
