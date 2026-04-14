from __future__ import annotations

import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ARCHIVE_LINK_RE = re.compile(r"/dumps/(polymarket_orderbook_\d{4}-\d{2}-\d{2}T\d{2}\.parquet)")


def extract_archive_filenames(html: str) -> list[str]:
    filenames: list[str] = []
    seen: set[str] = set()
    for filename in ARCHIVE_LINK_RE.findall(html):
        if filename in seen:
            continue
        filenames.append(filename)
        seen.add(filename)
    return filenames


def fetch_archive_page(archive_listing_url: str, page: int, timeout_secs: int) -> str:
    query = urlencode({"page": page})
    separator = "&" if "?" in archive_listing_url else "?"
    url = f"{archive_listing_url}{separator}{query}"
    request = Request(url, headers={"User-Agent": "pmxt-relay/1.0"})
    with urlopen(request, timeout=timeout_secs) as response:
        return response.read().decode("utf-8")
