"""Micro-benchmark to understand Telonex download per-request breakdown.

Measures: presigned resolve time, TLS setup, content fetch, parquet parse.
Compares urllib (current) vs httpx.Client (pooled, single-request-with-redirect).
"""
from __future__ import annotations

import io
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
API_KEY = os.environ["TELONEX_API_KEY"]
BASE = "https://api.telonex.io"
UA = "prediction-market-backtesting/profile"


# Pull a realistic set of recent days + slugs from the markets dataset.
print("Fetching markets catalog...", flush=True)
t0 = time.time()
req = Request(f"{BASE}/v1/datasets/polymarket/markets", headers={"User-Agent": UA})
with urlopen(req, timeout=120) as r:
    markets_bytes = r.read()
print(f"  catalog {len(markets_bytes)/1024/1024:.1f} MiB in {time.time()-t0:.1f}s", flush=True)
markets = pd.read_parquet(io.BytesIO(markets_bytes))

# Pick markets with recent quotes availability — use quotes_from which should
# have a non-zero yes-outcome file.
from datetime import date, timedelta

def _parse_d(v):
    if pd.isna(v):
        return None
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None

frame = markets.dropna(subset=["quotes_from", "quotes_to"])
jobs = []
for _, row in frame.iterrows():
    slug = row["slug"]
    d_from = _parse_d(row["quotes_from"])
    d_to = _parse_d(row["quotes_to"])
    if d_from is None or d_to is None or d_from >= d_to:
        continue
    # Midpoint day — likely to have data on both outcomes
    mid = d_from + (d_to - d_from) // 2
    jobs.append((slug, mid.isoformat()))
    if len(jobs) >= 60:
        break
print(f"Built {len(jobs)} sample jobs", flush=True)


CHANNEL = os.environ.get("PROFILE_CHANNEL", "book_snapshot_25")

def build_url(slug: str, date: str, channel: str = CHANNEL) -> str:
    return f"{BASE}/v1/downloads/polymarket/{channel}/{date}?slug={slug}&outcome_id=0"


# === Method 1: urllib (current) ===
def urllib_fetch(slug: str, date: str) -> tuple[float, float, int]:
    class _NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    t_resolve = time.time()
    url = build_url(slug, date)
    req = Request(url, headers={"Authorization": f"Bearer {API_KEY}", "User-Agent": UA})
    opener = build_opener(_NoRedirect())
    try:
        opener.open(req, timeout=60).close()
        return (0, 0, 0)  # unreachable
    except Exception as e:
        if getattr(e, "code", 0) in (301, 302, 303, 307, 308):
            location = e.headers.get("Location")
        elif getattr(e, "code", 0) == 404:
            return (time.time() - t_resolve, 0.0, 404)
        else:
            raise
    t_resolve_end = time.time()
    req2 = Request(location, headers={"User-Agent": UA})
    with urlopen(req2, timeout=60) as r:
        data = r.read()
    t_end = time.time()
    return (t_resolve_end - t_resolve, t_end - t_resolve_end, len(data))


# === Method 2: httpx.Client with pool, auto-follow ===
client = httpx.Client(
    http2=False,
    limits=httpx.Limits(max_connections=256, max_keepalive_connections=256, keepalive_expiry=60),
    follow_redirects=True,
    timeout=httpx.Timeout(60.0),
    headers={"User-Agent": UA},
)

def httpx_fetch(slug: str, date: str) -> tuple[float, float, int]:
    t0 = time.time()
    url = build_url(slug, date)
    r = client.get(url, headers={"Authorization": f"Bearer {API_KEY}"})
    if r.status_code == 404:
        return (time.time() - t0, 0.0, 404)
    r.raise_for_status()
    data = r.content
    t_end = time.time()
    return (0.0, t_end - t0, len(data))


def bench(label: str, fn, workers: int):
    print(f"\n--- {label} (workers={workers}) ---", flush=True)
    t0 = time.time()
    bytes_total = 0
    ok = 0
    missed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fn, slug, date) for slug, date in jobs]
        for f in as_completed(futures):
            resolve, fetch, size = f.result()
            if size == 404:
                missed += 1
                continue
            ok += 1
            bytes_total += size
    elapsed = time.time() - t0
    print(f"  {ok} ok, {missed} missed in {elapsed:.1f}s ({len(jobs)/elapsed:.1f} req/s, "
          f"{bytes_total/elapsed/1024:.0f} KiB/s)", flush=True)


# Warmup (prime DNS/TLS for urllib/httpx roughly equally)
try:
    urllib_fetch(jobs[0][0], jobs[0][1])
except Exception:
    pass
try:
    httpx_fetch(jobs[0][0], jobs[0][1])
except Exception:
    pass

bench("urllib 64-thread (current)", urllib_fetch, 64)
bench("httpx.Client 32-thread", httpx_fetch, 32)
bench("httpx.Client 48-thread", httpx_fetch, 48)
bench("httpx.Client 64-thread", httpx_fetch, 64)
bench("httpx.Client 96-thread", httpx_fetch, 96)


# Test parquet parse overhead: does parsing in the worker slow us down?
def httpx_fetch_and_parse(slug: str, date: str) -> tuple[float, float, int]:
    t0 = time.time()
    url = build_url(slug, date)
    r = client.get(url, headers={"Authorization": f"Bearer {API_KEY}"})
    if r.status_code == 404:
        return (time.time() - t0, 0.0, 404)
    r.raise_for_status()
    data = r.content
    t_fetch = time.time()
    df = pd.read_parquet(io.BytesIO(data))
    _ = len(df)
    t_parse = time.time()
    return (t_fetch - t0, t_parse - t_fetch, len(data))


print("\n=== Fetch+parse breakdown (64 workers) ===", flush=True)
t0 = time.time()
fetch_total = 0.0
parse_total = 0.0
with ThreadPoolExecutor(max_workers=64) as pool:
    futures = [pool.submit(httpx_fetch_and_parse, slug, date) for slug, date in jobs]
    for f in as_completed(futures):
        fetch, parse, size = f.result()
        if size == 404:
            continue
        fetch_total += fetch
        parse_total += parse
elapsed = time.time() - t0
print(f"  total={elapsed:.1f}s, wall req/s={len(jobs)/elapsed:.1f}, "
      f"sum fetch={fetch_total:.1f}s, sum parse={parse_total:.1f}s "
      f"(parse is {parse_total/fetch_total*100:.0f}% of fetch time across threads)", flush=True)
