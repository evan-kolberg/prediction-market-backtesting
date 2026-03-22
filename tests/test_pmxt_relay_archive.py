from pmxt_relay.archive import extract_archive_filenames


def test_extract_archive_filenames_deduplicates_and_preserves_order():
    html = """
    <a href="/dumps/polymarket_orderbook_2026-03-21T12.parquet">latest</a>
    <a href="/dumps/polymarket_orderbook_2026-03-21T11.parquet">previous</a>
    <a href="/dumps/polymarket_orderbook_2026-03-21T12.parquet">duplicate</a>
    """

    assert extract_archive_filenames(html) == [
        "polymarket_orderbook_2026-03-21T12.parquet",
        "polymarket_orderbook_2026-03-21T11.parquet",
    ]
