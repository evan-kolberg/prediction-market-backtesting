from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataType:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().casefold())

    def __str__(self) -> str:
        return self.name


Book = MarketDataType("book")

BOOK_DATA = Book


__all__ = [
    "BOOK_DATA",
    "Book",
    "MarketDataType",
]
