use std::collections::HashMap;
use std::path::{Path, PathBuf};

const NANOS_PER_DAY: i128 = 86_400_000_000_000;

pub const TELONEX_DEFAULT_API_BASE_URL: &str = "https://api.telonex.io";
pub const TELONEX_FULL_BOOK_CHANNEL: &str = "book_snapshot_full";
pub const TELONEX_ONCHAIN_FILLS_CHANNEL: &str = "onchain_fills";
pub const TELONEX_TRADES_CHANNEL: &str = "trades";
pub const POLYMARKET_PUBLIC_TRADES_API_URL: &str = "https://data-api.polymarket.com/trades";

const TELONEX_LOCAL_PREFIX: &str = "local:";
const TELONEX_API_PREFIX: &str = "api:";
const TELONEX_EXCHANGE: &str = "polymarket";
const TELONEX_CACHE_SUBDIR: &str = "api-days";
const TELONEX_DELTAS_CACHE_SUBDIR: &str = "book-deltas-v1";
const TELONEX_TRADE_TICKS_CACHE_SUBDIR: &str = "trade-ticks-v1";
const ORDER_SIDE_NO_ORDER_SIDE: u8 = 0;
const ORDER_SIDE_BUY: u8 = 1;
const ORDER_SIDE_SELL: u8 = 2;
const BOOK_ACTION_ADD: u8 = 1;
const BOOK_ACTION_UPDATE: u8 = 2;
const BOOK_ACTION_DELETE: u8 = 3;
const BOOK_ACTION_CLEAR: u8 = 4;
const RECORD_FLAG_LAST: u8 = 128;
const AGGRESSOR_NO_AGGRESSOR: u8 = 0;
const AGGRESSOR_BUYER: u8 = 1;
const AGGRESSOR_SELLER: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelonexSourceEntryKind {
    Local,
    Api,
}

impl TelonexSourceEntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Api => "api",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelonexSourceEntry {
    pub kind: TelonexSourceEntryKind,
    pub target: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TelonexFlatBookDiffRows {
    pub first_snapshot_index: Option<usize>,
    pub event_index: Vec<i32>,
    pub action: Vec<u8>,
    pub side: Vec<u8>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub flags: Vec<u8>,
    pub sequence: Vec<i32>,
    pub ts_event: Vec<i64>,
    pub ts_init: Vec<i64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TelonexTradeRows {
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub aggressor_side: Vec<u8>,
    pub trade_id: Vec<String>,
    pub ts_event: Vec<i64>,
    pub ts_init: Vec<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TelonexBookLevel<'a> {
    price_text: &'a str,
    size_text: &'a str,
    original_index: usize,
    price: f64,
    size: f64,
}

impl TelonexSourceEntry {
    pub fn local(target: impl Into<String>) -> Self {
        Self {
            kind: TelonexSourceEntryKind::Local,
            target: Some(target.into()),
            api_key: None,
        }
    }

    pub fn api(target: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            kind: TelonexSourceEntryKind::Api,
            target: Some(target.into()),
            api_key,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelonexSourceLabelKind {
    Cache,
    Local,
    Remote,
}

impl TelonexSourceLabelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelonexSourceStage {
    CacheRead,
    Fetch,
}

impl TelonexSourceStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CacheRead => "cache_read",
            Self::Fetch => "fetch",
        }
    }
}

pub fn classify_telonex_sources<I, S>(sources: I) -> Result<Vec<TelonexSourceEntry>, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut entries = Vec::new();
    for source in sources {
        let raw_source = source.as_ref();
        let expanded = expand_source_vars(raw_source);
        let stripped = expanded.trim();
        if stripped.is_empty() {
            continue;
        }
        let folded = stripped.to_ascii_lowercase();
        if folded.starts_with(TELONEX_LOCAL_PREFIX) {
            let remainder = stripped[TELONEX_LOCAL_PREFIX.len()..].trim();
            if remainder.is_empty() {
                return Err(format!(
                    "Telonex explicit source {raw_source:?} is missing a local path."
                ));
            }
            entries.push(TelonexSourceEntry::local(normalize_local_path(remainder)));
            continue;
        }
        if folded.starts_with(TELONEX_API_PREFIX) {
            let remainder = stripped[TELONEX_API_PREFIX.len()..].trim();
            let (target, api_key) = if remainder.is_empty() {
                (TELONEX_DEFAULT_API_BASE_URL.to_string(), None)
            } else if starts_with_http_scheme(remainder) {
                (normalize_api_base_url(Some(remainder))?, None)
            } else {
                (
                    TELONEX_DEFAULT_API_BASE_URL.to_string(),
                    Some(remainder.to_string()),
                )
            };
            entries.push(TelonexSourceEntry::api(target, api_key));
            continue;
        }
        return Err(format!(
            "Unsupported Telonex explicit source {stripped:?}. Use one of: local:, api:."
        ));
    }
    if entries.is_empty() {
        return Err("Telonex requires at least one source. Use local:/path or api:.".to_string());
    }
    Ok(entries)
}

pub fn normalize_api_base_url(value: Option<&str>) -> Result<String, String> {
    let Some(value) = value else {
        return Ok(TELONEX_DEFAULT_API_BASE_URL.to_string());
    };
    let stripped = value.trim();
    if stripped.is_empty() {
        return Ok(TELONEX_DEFAULT_API_BASE_URL.to_string());
    }
    let normalized = if stripped.contains("://") {
        stripped.to_string()
    } else {
        format!("https://{stripped}")
    };
    let Some((scheme, rest)) = normalized.split_once("://") else {
        return Err(format!("Expected a URL or host, got {value:?}"));
    };
    if scheme.is_empty() || rest.is_empty() || rest.starts_with('/') {
        return Err(format!("Expected a URL or host, got {value:?}"));
    }
    Ok(normalized.trim_end_matches('/').to_string())
}

pub fn telonex_source_label_kind(source: &str) -> Option<TelonexSourceLabelKind> {
    if source == "none" {
        return None;
    }
    if source.contains("cache") {
        return Some(TelonexSourceLabelKind::Cache);
    }
    if source.starts_with("telonex-local") {
        return Some(TelonexSourceLabelKind::Local);
    }
    if source.starts_with("telonex-api") {
        return Some(TelonexSourceLabelKind::Remote);
    }
    None
}

pub fn telonex_stage_for_source(source: &str) -> TelonexSourceStage {
    if source.contains("cache") {
        TelonexSourceStage::CacheRead
    } else {
        TelonexSourceStage::Fetch
    }
}

pub fn telonex_source_summary(entries: &[TelonexSourceEntry], api_cache_enabled: bool) -> String {
    format!(
        "{}\n{}",
        source_summary_line(
            "book",
            &book_source_summary_parts(entries, api_cache_enabled)
        ),
        source_summary_line(
            "trade",
            &trade_source_summary_parts(entries, api_cache_enabled)
        )
    )
}

pub fn book_source_summary_parts(
    entries: &[TelonexSourceEntry],
    api_cache_enabled: bool,
) -> Vec<String> {
    let mut parts = Vec::new();
    if api_cache_enabled {
        parts.push("cache".to_string());
    }
    for entry in entries {
        push_entry_summary_part(&mut parts, entry);
    }
    parts
}

pub fn trade_source_summary_parts(
    entries: &[TelonexSourceEntry],
    api_cache_enabled: bool,
) -> Vec<String> {
    let has_api_entry = entries
        .iter()
        .any(|entry| entry.kind == TelonexSourceEntryKind::Api);
    let mut parts = Vec::new();
    if has_api_entry && api_cache_enabled {
        parts.push("cache".to_string());
    }
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == TelonexSourceEntryKind::Local)
    {
        push_entry_summary_part(&mut parts, entry);
    }
    for entry in entries
        .iter()
        .filter(|entry| entry.kind == TelonexSourceEntryKind::Api)
    {
        push_entry_summary_part(&mut parts, entry);
    }
    parts.push("polymarket cache".to_string());
    parts.push(format!("api {POLYMARKET_PUBLIC_TRADES_API_URL}"));
    parts
}

pub fn source_summary_line(label: &str, parts: &[String]) -> String {
    format!(
        "Telonex {label} source: explicit priority ({})",
        parts.join(" -> ")
    )
}

pub fn telonex_source_days_for_window(start_ns: i128, end_ns: i128) -> Vec<String> {
    if end_ns < start_ns {
        return Vec::new();
    }
    let first_day = start_ns.div_euclid(NANOS_PER_DAY);
    let last_day = end_ns.div_euclid(NANOS_PER_DAY);
    (first_day..=last_day).map(format_utc_day).collect()
}

pub fn telonex_day_window_ns(date: &str, start_ns: i128, end_ns: i128) -> Option<(i128, i128)> {
    let day_start = parse_utc_day_start_ns(date).ok()?;
    let day_end = day_start + NANOS_PER_DAY - 1;
    let clipped_start = start_ns.max(day_start);
    let clipped_end = end_ns.min(day_end);
    if clipped_start > clipped_end {
        return None;
    }
    Some((clipped_start, clipped_end))
}

pub fn flat_book_snapshot_diff_rows(
    timestamp_ns: Vec<i64>,
    bid_prices: Vec<Vec<String>>,
    bid_sizes: Vec<Vec<String>>,
    ask_prices: Vec<Vec<String>>,
    ask_sizes: Vec<Vec<String>>,
    start_ns: i64,
    end_ns: i64,
) -> Result<TelonexFlatBookDiffRows, String> {
    let row_count = timestamp_ns.len();
    if bid_prices.len() != row_count
        || bid_sizes.len() != row_count
        || ask_prices.len() != row_count
        || ask_sizes.len() != row_count
    {
        return Err(format!(
            "Telonex flat book columns have inconsistent lengths: timestamp_ns={}, bid_prices={}, bid_sizes={}, ask_prices={}, ask_sizes={}",
            timestamp_ns.len(),
            bid_prices.len(),
            bid_sizes.len(),
            ask_prices.len(),
            ask_sizes.len()
        ));
    }

    let mut order: Vec<usize> = timestamp_ns
        .iter()
        .enumerate()
        .filter_map(|(idx, timestamp)| (*timestamp <= end_ns).then_some(idx))
        .collect();
    if order.is_empty() {
        return Ok(TelonexFlatBookDiffRows::default());
    }
    order.sort_by_key(|idx| timestamp_ns[*idx]);

    let mut rows = TelonexFlatBookDiffRows::default();
    let mut previous_bids = None;
    let mut previous_asks = None;
    let mut emitted_snapshot = false;
    let mut output_event_index: i32 = 0;

    for idx in order {
        let ts_event = timestamp_ns[idx];
        let current_bids = flat_book_side_levels(&bid_prices[idx], &bid_sizes[idx])?;
        let current_asks = flat_book_side_levels(&ask_prices[idx], &ask_sizes[idx])?;

        if ts_event < start_ns {
            previous_bids = Some(current_bids);
            previous_asks = Some(current_asks);
            continue;
        }

        if !emitted_snapshot {
            if append_snapshot_rows(
                &mut rows,
                output_event_index,
                &current_bids,
                &current_asks,
                ts_event,
            ) {
                rows.first_snapshot_index = Some(idx);
                emitted_snapshot = true;
                output_event_index += 1;
            }
        } else if let (Some(prev_bids), Some(prev_asks)) =
            (previous_bids.as_ref(), previous_asks.as_ref())
        {
            let mut changes =
                flat_book_side_changes(ORDER_SIDE_BUY, prev_bids, &current_bids, false);
            changes.extend(flat_book_side_changes(
                ORDER_SIDE_SELL,
                prev_asks,
                &current_asks,
                true,
            ));
            let change_count = changes.len();
            if change_count > 0 {
                for (change_idx, change) in changes.into_iter().enumerate() {
                    rows.event_index.push(output_event_index);
                    rows.action.push(if change.size > 0.0 {
                        BOOK_ACTION_UPDATE
                    } else {
                        BOOK_ACTION_DELETE
                    });
                    rows.side.push(change.side);
                    rows.price.push(change.price);
                    rows.size.push(change.size);
                    rows.flags.push(if change_idx + 1 == change_count {
                        RECORD_FLAG_LAST
                    } else {
                        0
                    });
                    rows.sequence
                        .push(i32::try_from(change_idx + 1).unwrap_or(i32::MAX));
                    rows.ts_event.push(ts_event);
                    rows.ts_init.push(ts_event);
                }
                output_event_index += 1;
            }
        }

        previous_bids = Some(current_bids);
        previous_asks = Some(current_asks);
    }

    Ok(rows)
}

fn append_snapshot_rows(
    rows: &mut TelonexFlatBookDiffRows,
    event_index: i32,
    bids: &[TelonexBookLevel<'_>],
    asks: &[TelonexBookLevel<'_>],
    ts_event: i64,
) -> bool {
    if bids.is_empty() && asks.is_empty() {
        return false;
    }

    rows.event_index.push(event_index);
    rows.action.push(BOOK_ACTION_CLEAR);
    rows.side.push(ORDER_SIDE_NO_ORDER_SIDE);
    rows.price.push(0.0);
    rows.size.push(0.0);
    rows.flags.push(0);
    rows.sequence.push(0);
    rows.ts_event.push(ts_event);
    rows.ts_init.push(ts_event);

    for (idx, bid) in bids.iter().enumerate() {
        let is_last = asks.is_empty() && idx + 1 == bids.len();
        append_snapshot_level_row(rows, event_index, ORDER_SIDE_BUY, bid, is_last, ts_event);
    }
    for (idx, ask) in asks.iter().rev().enumerate() {
        let is_last = idx + 1 == asks.len();
        append_snapshot_level_row(rows, event_index, ORDER_SIDE_SELL, ask, is_last, ts_event);
    }
    true
}

fn append_snapshot_level_row(
    rows: &mut TelonexFlatBookDiffRows,
    event_index: i32,
    side: u8,
    level: &TelonexBookLevel<'_>,
    is_last: bool,
    ts_event: i64,
) {
    rows.event_index.push(event_index);
    rows.action.push(BOOK_ACTION_ADD);
    rows.side.push(side);
    rows.price.push(level.price);
    rows.size.push(level.size);
    rows.flags.push(if is_last { RECORD_FLAG_LAST } else { 0 });
    rows.sequence.push(0);
    rows.ts_event.push(ts_event);
    rows.ts_init.push(ts_event);
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct FlatBookChange {
    side: u8,
    price: f64,
    size: f64,
}

fn flat_book_side_levels<'a>(
    prices: &'a [String],
    sizes: &'a [String],
) -> Result<Vec<TelonexBookLevel<'a>>, String> {
    let mut levels = Vec::with_capacity(prices.len().min(sizes.len()));
    for (original_index, (price_text, size_text)) in prices.iter().zip(sizes.iter()).enumerate() {
        let size = parse_finite_f64(size_text, "size")?;
        if size <= 0.0 {
            continue;
        }
        let price = parse_finite_f64(price_text, "price")?;
        levels.push(TelonexBookLevel {
            price_text,
            size_text,
            original_index,
            price,
            size,
        });
    }
    levels.sort_by(compare_level);
    let mut deduped: Vec<TelonexBookLevel<'a>> = Vec::with_capacity(levels.len());
    for level in levels {
        if let Some(previous) = deduped.last_mut()
            && previous.price_text == level.price_text
        {
            *previous = level;
            continue;
        }
        deduped.push(level);
    }
    Ok(deduped)
}

fn flat_book_side_changes(
    side: u8,
    previous: &[TelonexBookLevel<'_>],
    current: &[TelonexBookLevel<'_>],
    reverse: bool,
) -> Vec<FlatBookChange> {
    let mut changes = Vec::new();
    let mut previous_index = 0;
    let mut current_index = 0;
    while previous_index < previous.len() || current_index < current.len() {
        match (previous.get(previous_index), current.get(current_index)) {
            (Some(previous_level), Some(current_level)) => {
                match compare_level_key(previous_level, current_level) {
                    std::cmp::Ordering::Less => {
                        changes.push(FlatBookChange {
                            side,
                            price: previous_level.price,
                            size: 0.0,
                        });
                        previous_index += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        changes.push(FlatBookChange {
                            side,
                            price: current_level.price,
                            size: current_level.size,
                        });
                        current_index += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        if previous_level.size_text != current_level.size_text {
                            changes.push(FlatBookChange {
                                side,
                                price: current_level.price,
                                size: current_level.size,
                            });
                        }
                        previous_index += 1;
                        current_index += 1;
                    }
                }
            }
            (Some(previous_level), None) => {
                changes.push(FlatBookChange {
                    side,
                    price: previous_level.price,
                    size: 0.0,
                });
                previous_index += 1;
            }
            (None, Some(current_level)) => {
                changes.push(FlatBookChange {
                    side,
                    price: current_level.price,
                    size: current_level.size,
                });
                current_index += 1;
            }
            (None, None) => break,
        }
    }
    if reverse {
        changes.reverse();
    }
    changes
}

fn compare_level(left: &TelonexBookLevel<'_>, right: &TelonexBookLevel<'_>) -> std::cmp::Ordering {
    compare_level_key(left, right).then_with(|| left.original_index.cmp(&right.original_index))
}

fn compare_level_key(
    left: &TelonexBookLevel<'_>,
    right: &TelonexBookLevel<'_>,
) -> std::cmp::Ordering {
    left.price
        .partial_cmp(&right.price)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.price_text.cmp(right.price_text))
}

fn parse_finite_f64(value: &str, field: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("invalid Telonex book level {field}: {value:?}"))?;
    if !parsed.is_finite() {
        return Err(format!("invalid Telonex book level {field}: {value:?}"));
    }
    Ok(parsed)
}

#[allow(clippy::too_many_arguments)]
pub fn onchain_fill_trade_rows(
    timestamp_ns: &[i64],
    prices: &[Option<String>],
    sizes: &[Option<String>],
    sides: Option<&[Option<String>]>,
    ids: Option<&[Option<String>]>,
    start_ns: i64,
    end_ns: i64,
    token_suffix: &str,
) -> Result<TelonexTradeRows, String> {
    let row_count = timestamp_ns.len();
    if prices.len() != row_count || sizes.len() != row_count {
        return Err(format!(
            "Telonex trade columns have inconsistent lengths: timestamp_ns={}, prices={}, sizes={}",
            timestamp_ns.len(),
            prices.len(),
            sizes.len()
        ));
    }
    if let Some(sides) = sides
        && sides.len() != row_count
    {
        return Err(format!(
            "Telonex trade side column length does not match timestamp_ns: timestamp_ns={}, sides={}",
            timestamp_ns.len(),
            sides.len()
        ));
    }
    if let Some(ids) = ids
        && ids.len() != row_count
    {
        return Err(format!(
            "Telonex trade id column length does not match timestamp_ns: timestamp_ns={}, ids={}",
            timestamp_ns.len(),
            ids.len()
        ));
    }

    let mut order: Vec<usize> = timestamp_ns
        .iter()
        .enumerate()
        .filter_map(|(idx, timestamp)| {
            (*timestamp >= start_ns && *timestamp <= end_ns).then_some(idx)
        })
        .collect();
    order.sort_by_key(|idx| timestamp_ns[*idx]);

    let mut rows = TelonexTradeRows::default();
    let mut timestamp_counts: HashMap<i64, usize> = HashMap::new();
    let mut trade_id_counts: HashMap<String, usize> = HashMap::new();
    let token_suffix = token_suffix.trim();

    for (sorted_index, idx) in order.into_iter().enumerate() {
        let Some(price) = parse_optional_finite_f64(prices[idx].as_deref()) else {
            continue;
        };
        let Some(size) = parse_optional_finite_f64(sizes[idx].as_deref()) else {
            continue;
        };
        if !(0.0 < price && price < 1.0) || size <= 0.0 {
            continue;
        }

        let base_ts_event = timestamp_ns[idx];
        let occurrence = *timestamp_counts.get(&base_ts_event).unwrap_or(&0);
        timestamp_counts.insert(base_ts_event, occurrence + 1);
        let ts_event =
            base_ts_event.saturating_add(i64::try_from(occurrence.min(999)).unwrap_or(999));

        let aggressor_side = sides
            .and_then(|side_values| side_values.get(idx).and_then(Option::as_deref))
            .map_or(AGGRESSOR_NO_AGGRESSOR, telonex_aggressor_side);

        let raw_id = match ids {
            Some(id_values) => id_values
                .get(idx)
                .and_then(Option::as_deref)
                .map(str::to_string)
                .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("nan"))
                .unwrap_or_else(|| format!("telonex-{base_ts_event}")),
            None => format!("telonex-{base_ts_event}-{sorted_index}"),
        };
        let sequence = *trade_id_counts.get(&raw_id).unwrap_or(&0);
        trade_id_counts.insert(raw_id.clone(), sequence + 1);
        let id_suffix = suffix_chars(&raw_id, 24);
        let trade_id = if token_suffix.is_empty() {
            format!("{id_suffix}-{sequence:06}")
        } else {
            format!("{id_suffix}-{token_suffix}-{sequence:06}")
        };

        rows.price.push(price);
        rows.size.push(size);
        rows.aggressor_side.push(aggressor_side);
        rows.trade_id.push(trade_id);
        rows.ts_event.push(ts_event);
        rows.ts_init.push(ts_event);
    }

    Ok(rows)
}

fn parse_optional_finite_f64(value: Option<&str>) -> Option<f64> {
    let value = value?.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("nan") {
        return None;
    }
    let parsed = value.parse::<f64>().ok()?;
    parsed.is_finite().then_some(parsed)
}

fn telonex_aggressor_side(value: &str) -> u8 {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "buy" | "buyer" | "bid" | "bidder" | "taker_buy" | "buying" => AGGRESSOR_BUYER,
        "sell" | "seller" | "ask" | "offer" | "taker_sell" | "selling" => AGGRESSOR_SELLER,
        _ => AGGRESSOR_NO_AGGRESSOR,
    }
}

fn suffix_chars(value: &str, count: usize) -> String {
    let mut chars = value.chars().rev().take(count).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

pub fn outcome_segments(token_index: i64, outcome: Option<&str>) -> Vec<String> {
    let mut segments = vec![format!("outcome_id={token_index}"), token_index.to_string()];
    if let Some(outcome) = nonempty(outcome) {
        segments.insert(0, outcome.to_string());
    }
    segments
}

pub fn outcome_cache_segment(token_index: i64, outcome: Option<&str>) -> String {
    match nonempty(outcome) {
        Some(outcome) => format!("outcome={}", percent_encode_path_component(outcome)),
        None => format!("outcome_id={token_index}"),
    }
}

pub fn api_url(
    base_url: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> String {
    let mut params = format!("slug={}", form_encode(market_slug));
    if let Some(outcome) = nonempty(outcome) {
        params.push_str("&outcome=");
        params.push_str(&form_encode(outcome));
    } else {
        params.push_str("&outcome_id=");
        params.push_str(&token_index.to_string());
    }
    format!(
        "{}/v1/downloads/{TELONEX_EXCHANGE}/{channel}/{date}?{params}",
        base_url.trim_end_matches('/')
    )
}

pub fn telonex_api_source_label(
    base_url: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> String {
    format!(
        "telonex-api::{}",
        api_url(base_url, channel, date, market_slug, token_index, outcome)
    )
}

pub fn api_cache_relative_path(
    base_url_key: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> PathBuf {
    PathBuf::from(TELONEX_CACHE_SUBDIR)
        .join(base_url_key)
        .join(TELONEX_EXCHANGE)
        .join(channel)
        .join(percent_encode_path_component(market_slug))
        .join(outcome_cache_segment(token_index, outcome))
        .join(api_cache_file_name(date))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializedCachePathSpec<'a> {
    pub channel: &'a str,
    pub date: &'a str,
    pub market_slug: &'a str,
    pub token_index: i64,
    pub outcome: Option<&'a str>,
    pub instrument_key: &'a str,
    pub start_ns: i128,
    pub end_ns: i128,
}

pub fn deltas_cache_relative_path(spec: MaterializedCachePathSpec<'_>) -> PathBuf {
    PathBuf::from(TELONEX_DELTAS_CACHE_SUBDIR)
        .join(TELONEX_EXCHANGE)
        .join(spec.channel)
        .join(percent_encode_path_component(spec.market_slug))
        .join(outcome_cache_segment(spec.token_index, spec.outcome))
        .join(format!("instrument={}", spec.instrument_key))
        .join(materialized_cache_file_name(
            spec.date,
            spec.start_ns,
            spec.end_ns,
        ))
}

pub fn trade_ticks_cache_relative_path(spec: MaterializedCachePathSpec<'_>) -> PathBuf {
    PathBuf::from(TELONEX_TRADE_TICKS_CACHE_SUBDIR)
        .join(TELONEX_EXCHANGE)
        .join(spec.channel)
        .join(percent_encode_path_component(spec.market_slug))
        .join(outcome_cache_segment(spec.token_index, spec.outcome))
        .join(format!("instrument={}", spec.instrument_key))
        .join(materialized_cache_file_name(
            spec.date,
            spec.start_ns,
            spec.end_ns,
        ))
}

pub fn api_cache_file_name(date: &str) -> String {
    format!("{date}.parquet")
}

pub fn fast_api_cache_file_name(date: &str) -> String {
    format!("{date}.fast.parquet")
}

pub fn materialized_cache_file_name(date: &str, start_ns: i128, end_ns: i128) -> String {
    format!("{date}.{start_ns}-{end_ns}.parquet")
}

pub fn local_consolidated_candidate_paths(
    root: impl AsRef<Path>,
    channel: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> Vec<PathBuf> {
    let root = root.as_ref();
    let outcome_parts = outcome_segments(token_index, outcome);
    let mut candidates = Vec::new();
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(TELONEX_EXCHANGE)
                .join(market_slug)
                .join(outcome_part)
                .join(format!("{channel}.parquet")),
        );
    }
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(TELONEX_EXCHANGE)
                .join(channel)
                .join(market_slug)
                .join(format!("{outcome_part}.parquet")),
        );
    }
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(channel)
                .join(market_slug)
                .join(format!("{outcome_part}.parquet")),
        );
    }
    candidates
}

pub fn local_daily_candidate_paths(
    root: impl AsRef<Path>,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> Vec<PathBuf> {
    let root = root.as_ref();
    let outcome_parts = outcome_segments(token_index, outcome);
    let mut candidates = Vec::new();
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(TELONEX_EXCHANGE)
                .join(market_slug)
                .join(outcome_part)
                .join(channel)
                .join(format!("{date}.parquet")),
        );
    }
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(TELONEX_EXCHANGE)
                .join(channel)
                .join(market_slug)
                .join(outcome_part)
                .join(format!("{date}.parquet")),
        );
    }
    for outcome_part in &outcome_parts {
        candidates.push(
            root.join(channel)
                .join(market_slug)
                .join(outcome_part)
                .join(format!("{date}.parquet")),
        );
    }
    candidates.extend([
        root.join(TELONEX_EXCHANGE)
            .join(channel)
            .join(format!("{market_slug}_{token_index}_{date}.parquet")),
        root.join(channel)
            .join(format!("{market_slug}_{token_index}_{date}.parquet")),
        root.join(format!("{market_slug}_{token_index}_{date}.parquet")),
        root.join(format!("{date}.parquet")),
    ]);
    candidates
}

fn push_entry_summary_part(parts: &mut Vec<String>, entry: &TelonexSourceEntry) {
    match entry.kind {
        TelonexSourceEntryKind::Local => {
            let target = entry.target.as_deref().unwrap_or("");
            parts.push(format!("local {target}"));
        }
        TelonexSourceEntryKind::Api => {
            let target = entry
                .target
                .as_deref()
                .unwrap_or(TELONEX_DEFAULT_API_BASE_URL);
            let suffix = if entry.api_key.is_some() {
                " (key set)"
            } else {
                " (key missing)"
            };
            parts.push(format!("api {target}{suffix}"));
        }
    }
}

fn expand_source_vars(source: &str) -> String {
    let mut expanded = String::with_capacity(source.len());
    let mut cursor = 0;
    while cursor < source.len() {
        let remainder = &source[cursor..];
        if let Some(name_start) = remainder.strip_prefix("${")
            && let Some(end_offset) = name_start.find('}')
        {
            let name = &name_start[..end_offset];
            if let Ok(value) = std::env::var(name) {
                expanded.push_str(&value);
            }
            cursor += 2 + end_offset + 1;
            continue;
        }
        if remainder.starts_with('$') {
            let name_start = cursor + 1;
            let mut name_end = name_start;
            for (offset, ch) in source[name_start..].char_indices() {
                if offset == 0 {
                    if ch == '_' || ch.is_ascii_alphabetic() {
                        name_end = name_start + ch.len_utf8();
                        continue;
                    }
                    break;
                }
                if ch == '_' || ch.is_ascii_alphanumeric() {
                    name_end = name_start + offset + ch.len_utf8();
                    continue;
                }
                break;
            }
            if name_end > name_start {
                let name = &source[name_start..name_end];
                if let Ok(value) = std::env::var(name) {
                    expanded.push_str(&value);
                }
                cursor = name_end;
                continue;
            }
        }
        let ch = remainder.chars().next().expect("cursor is inside string");
        expanded.push(ch);
        cursor += ch.len_utf8();
    }
    expanded
}

fn normalize_local_path(value: &str) -> String {
    if let Some(rest) = value.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home)
            .join(rest)
            .to_string_lossy()
            .into_owned();
    }
    value.to_string()
}

fn starts_with_http_scheme(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn form_encode(value: &str) -> String {
    percent_encode(value, true)
}

fn percent_encode_path_component(value: &str) -> String {
    percent_encode(value, false)
}

fn percent_encode(value: &str, space_as_plus: bool) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else if byte == b' ' && space_as_plus {
            encoded.push('+');
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + value - 10),
        _ => unreachable!("nibble is always <= 15"),
    }
}

fn parse_utc_day_start_ns(date: &str) -> Result<i128, String> {
    let (year, month, day) = parse_utc_day(date)?;
    Ok(days_from_civil(year, month, day) * NANOS_PER_DAY)
}

fn parse_utc_day(date: &str) -> Result<(i128, u32, u32), String> {
    let mut parts = date.split('-');
    let year = parts
        .next()
        .ok_or_else(|| format!("invalid UTC day {date:?}"))?
        .parse::<i128>()
        .map_err(|_| format!("invalid UTC day year {date:?}"))?;
    let month = parts
        .next()
        .ok_or_else(|| format!("invalid UTC day {date:?}"))?
        .parse::<u32>()
        .map_err(|_| format!("invalid UTC day month {date:?}"))?;
    let day = parts
        .next()
        .ok_or_else(|| format!("invalid UTC day {date:?}"))?
        .parse::<u32>()
        .map_err(|_| format!("invalid UTC day day {date:?}"))?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(format!("invalid UTC day {date:?}"));
    }
    let (roundtrip_year, roundtrip_month, roundtrip_day) =
        civil_from_days(days_from_civil(year, month, day));
    if (roundtrip_year, roundtrip_month, roundtrip_day) != (year, month, day) {
        return Err(format!("invalid UTC day {date:?}"));
    }
    Ok((year, month, day))
}

fn format_utc_day(days_since_unix_epoch: i128) -> String {
    let (year, month, day) = civil_from_days(days_since_unix_epoch);
    format!("{year:04}-{month:02}-{day:02}")
}

fn civil_from_days(days_since_unix_epoch: i128) -> (i128, u32, u32) {
    let shifted_days = days_since_unix_epoch + 719_468;
    let era = shifted_days.div_euclid(146_097);
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let adjusted_year = year + if month <= 2 { 1 } else { 0 };

    (adjusted_year, month as u32, day as u32)
}

fn days_from_civil(year: i128, month: u32, day: u32) -> i128 {
    let adjusted_year = year - i128::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let month = i128::from(month);
    let day = i128::from(day);
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::{
        MaterializedCachePathSpec, TelonexSourceEntry, TelonexSourceEntryKind,
        TelonexSourceLabelKind, TelonexSourceStage, api_cache_file_name, api_cache_relative_path,
        api_url, classify_telonex_sources, deltas_cache_relative_path, fast_api_cache_file_name,
        flat_book_snapshot_diff_rows, local_consolidated_candidate_paths,
        local_daily_candidate_paths, materialized_cache_file_name, onchain_fill_trade_rows,
        outcome_cache_segment, outcome_segments, source_summary_line, telonex_api_source_label,
        telonex_day_window_ns, telonex_source_days_for_window, telonex_source_label_kind,
        telonex_source_summary, telonex_stage_for_source, trade_ticks_cache_relative_path,
    };
    use std::path::PathBuf;

    const APR_21_2026_NS: i128 = 1_776_729_600_000_000_000;
    const APR_28_2026_NS: i128 = 1_777_334_400_000_000_000;
    const APR_27_2026_END_NS: i128 = 1_777_334_399_999_999_999;

    #[test]
    fn classifies_explicit_sources_in_order() {
        let entries = classify_telonex_sources([
            " local:/tmp/telonex ",
            "api:SECRET",
            "api:https://api.example.test/",
        ])
        .unwrap();

        assert_eq!(
            entries,
            vec![
                TelonexSourceEntry::local("/tmp/telonex"),
                TelonexSourceEntry::api("https://api.telonex.io", Some("SECRET".to_string())),
                TelonexSourceEntry::api("https://api.example.test", None),
            ]
        );
    }

    #[test]
    fn classification_rejects_empty_or_unknown_sources() {
        assert!(classify_telonex_sources(["local:"]).is_err());
        assert!(classify_telonex_sources(["archive:r2.example"]).is_err());
        assert!(classify_telonex_sources([" ", ""]).is_err());
    }

    #[test]
    fn classification_strips_unresolved_source_variables() {
        let entries =
            classify_telonex_sources(["api:${TELONEX_TEST_KEY_THAT_SHOULD_NOT_EXIST}"]).unwrap();

        assert_eq!(
            entries,
            vec![TelonexSourceEntry::api("https://api.telonex.io", None)]
        );
    }

    #[test]
    fn source_label_kind_matches_loader_accounting_rules() {
        assert_eq!(telonex_source_label_kind("none"), None);
        assert_eq!(
            telonex_source_label_kind("telonex-cache-fast::/tmp/day.fast.parquet"),
            Some(TelonexSourceLabelKind::Cache)
        );
        assert_eq!(
            telonex_source_label_kind("telonex-deltas-cache::/tmp/day.parquet"),
            Some(TelonexSourceLabelKind::Cache)
        );
        assert_eq!(
            telonex_source_label_kind("telonex-local-trades::/data/telonex"),
            Some(TelonexSourceLabelKind::Local)
        );
        assert_eq!(
            telonex_source_label_kind("telonex-api::https://api.telonex.io/v1/downloads"),
            Some(TelonexSourceLabelKind::Remote)
        );
        assert_eq!(telonex_source_label_kind("polymarket-api"), None);
    }

    #[test]
    fn stage_for_source_splits_cache_reads_from_fetches() {
        assert_eq!(
            telonex_stage_for_source("telonex-cache::/tmp/day.parquet"),
            TelonexSourceStage::CacheRead
        );
        assert_eq!(
            telonex_stage_for_source("telonex-local::/data/telonex"),
            TelonexSourceStage::Fetch
        );
        assert_eq!(telonex_stage_for_source("none"), TelonexSourceStage::Fetch);
    }

    #[test]
    fn source_summary_matches_python_priority_text() {
        let entries = vec![
            TelonexSourceEntry {
                kind: TelonexSourceEntryKind::Local,
                target: Some("/tmp/telonex".to_string()),
                api_key: None,
            },
            TelonexSourceEntry {
                kind: TelonexSourceEntryKind::Api,
                target: Some("https://api.example.test".to_string()),
                api_key: Some("KEY".to_string()),
            },
        ];

        assert_eq!(
            telonex_source_summary(&entries, true),
            "Telonex book source: explicit priority (cache -> local /tmp/telonex -> api https://api.example.test (key set))\nTelonex trade source: explicit priority (cache -> local /tmp/telonex -> api https://api.example.test (key set) -> polymarket cache -> api https://data-api.polymarket.com/trades)"
        );
        assert_eq!(
            source_summary_line("book", &["local /tmp/telonex".to_string()]),
            "Telonex book source: explicit priority (local /tmp/telonex)"
        );
    }

    #[test]
    fn telonex_source_days_use_inclusive_window_semantics() {
        assert_eq!(
            telonex_source_days_for_window(APR_21_2026_NS, APR_27_2026_END_NS),
            vec![
                "2026-04-21",
                "2026-04-22",
                "2026-04-23",
                "2026-04-24",
                "2026-04-25",
                "2026-04-26",
                "2026-04-27",
            ]
        );
        assert_eq!(
            telonex_source_days_for_window(APR_21_2026_NS, APR_28_2026_NS)
                .last()
                .map(String::as_str),
            Some("2026-04-28")
        );
        assert!(telonex_source_days_for_window(5, 4).is_empty());
    }

    #[test]
    fn day_window_clips_to_requested_date() {
        assert_eq!(
            telonex_day_window_ns("2026-04-21", APR_21_2026_NS + 10, APR_28_2026_NS),
            Some((APR_21_2026_NS + 10, APR_21_2026_NS + 86_400_000_000_000 - 1))
        );
        assert_eq!(
            telonex_day_window_ns("2026-04-20", APR_21_2026_NS, APR_28_2026_NS),
            None
        );
        assert_eq!(
            telonex_day_window_ns("2026-02-31", APR_21_2026_NS, APR_28_2026_NS),
            None
        );
    }

    #[test]
    fn outcome_segments_match_local_lookup_order() {
        assert_eq!(
            outcome_segments(2, Some("Yes")),
            vec!["Yes", "outcome_id=2", "2"]
        );
        assert_eq!(outcome_segments(2, None), vec!["outcome_id=2", "2"]);
    }

    #[test]
    fn api_url_matches_python_urlencode_shape() {
        assert_eq!(
            api_url(
                "https://api.telonex.io/",
                "book_snapshot_full",
                "2026-01-20",
                "my market/slug",
                0,
                None,
            ),
            "https://api.telonex.io/v1/downloads/polymarket/book_snapshot_full/2026-01-20?slug=my+market%2Fslug&outcome_id=0"
        );
        assert_eq!(
            telonex_api_source_label(
                "https://api.example.test",
                "onchain_fills",
                "2026-01-20",
                "slug",
                1,
                Some("Yes/No"),
            ),
            "telonex-api::https://api.example.test/v1/downloads/polymarket/onchain_fills/2026-01-20?slug=slug&outcome=Yes%2FNo"
        );
    }

    #[test]
    fn cache_segments_and_filenames_match_python_layout() {
        assert_eq!(outcome_cache_segment(1, Some("Yes/No")), "outcome=Yes%2FNo");
        assert_eq!(outcome_cache_segment(1, None), "outcome_id=1");
        assert_eq!(api_cache_file_name("2026-01-20"), "2026-01-20.parquet");
        assert_eq!(
            fast_api_cache_file_name("2026-01-20"),
            "2026-01-20.fast.parquet"
        );
        assert_eq!(
            materialized_cache_file_name("2026-01-20", 10, 99),
            "2026-01-20.10-99.parquet"
        );
    }

    #[test]
    fn cache_relative_paths_match_python_subdirectories() {
        assert_eq!(
            api_cache_relative_path(
                "abcdef0123456789",
                "book_snapshot_full",
                "2026-01-20",
                "slug/with space",
                0,
                None,
            ),
            PathBuf::from("api-days")
                .join("abcdef0123456789")
                .join("polymarket")
                .join("book_snapshot_full")
                .join("slug%2Fwith%20space")
                .join("outcome_id=0")
                .join("2026-01-20.parquet")
        );
        assert_eq!(
            deltas_cache_relative_path(MaterializedCachePathSpec {
                channel: "book_snapshot_full",
                date: "2026-01-20",
                market_slug: "slug",
                token_index: 0,
                outcome: Some("Yes"),
                instrument_key: "instrumenthash",
                start_ns: 10,
                end_ns: 99,
            }),
            PathBuf::from("book-deltas-v1")
                .join("polymarket")
                .join("book_snapshot_full")
                .join("slug")
                .join("outcome=Yes")
                .join("instrument=instrumenthash")
                .join("2026-01-20.10-99.parquet")
        );
        assert_eq!(
            trade_ticks_cache_relative_path(MaterializedCachePathSpec {
                channel: "onchain_fills",
                date: "2026-01-20",
                market_slug: "slug",
                token_index: 0,
                outcome: None,
                instrument_key: "instrumenthash",
                start_ns: 10,
                end_ns: 99,
            }),
            PathBuf::from("trade-ticks-v1")
                .join("polymarket")
                .join("onchain_fills")
                .join("slug")
                .join("outcome_id=0")
                .join("instrument=instrumenthash")
                .join("2026-01-20.10-99.parquet")
        );
    }

    #[test]
    fn flat_book_snapshot_diff_rows_diffs_after_first_window_snapshot() {
        let rows = flat_book_snapshot_diff_rows(
            vec![90, 100, 110, 120],
            vec![
                vec!["0.10".to_string()],
                vec!["0.10".to_string(), "0.09".to_string()],
                vec!["0.10".to_string(), "0.08".to_string()],
                vec!["0.08".to_string()],
            ],
            vec![
                vec!["1".to_string()],
                vec!["1".to_string(), "2".to_string()],
                vec!["3".to_string(), "4".to_string()],
                vec!["4".to_string()],
            ],
            vec![
                vec!["0.80".to_string()],
                vec!["0.80".to_string(), "0.90".to_string()],
                vec!["0.80".to_string(), "0.95".to_string()],
                vec!["0.95".to_string()],
            ],
            vec![
                vec!["1".to_string()],
                vec!["1".to_string(), "2".to_string()],
                vec!["3".to_string(), "4".to_string()],
                vec!["4".to_string()],
            ],
            100,
            120,
        )
        .unwrap();

        assert_eq!(rows.first_snapshot_index, Some(1));
        assert_eq!(
            rows.event_index,
            vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2]
        );
        assert_eq!(rows.action, vec![4, 1, 1, 1, 1, 2, 3, 2, 2, 3, 2, 3, 3]);
        assert_eq!(rows.side, vec![0, 1, 1, 2, 2, 1, 1, 1, 2, 2, 2, 1, 2]);
        assert_eq!(
            rows.price,
            vec![
                0.0, 0.09, 0.10, 0.90, 0.80, 0.08, 0.09, 0.10, 0.95, 0.90, 0.80, 0.10, 0.80
            ]
        );
        assert_eq!(
            rows.size,
            vec![
                0.0, 2.0, 1.0, 2.0, 1.0, 4.0, 0.0, 3.0, 4.0, 0.0, 3.0, 0.0, 0.0
            ]
        );
        assert_eq!(
            rows.flags,
            vec![0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 128, 0, 128]
        );
        assert_eq!(rows.sequence, vec![0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 1, 2]);
        assert_eq!(
            rows.ts_event,
            vec![
                100, 100, 100, 100, 100, 110, 110, 110, 110, 110, 110, 120, 120
            ]
        );
        assert_eq!(rows.ts_init, rows.ts_event);
    }

    #[test]
    fn flat_book_snapshot_diff_rows_keeps_stable_order_for_equal_timestamps() {
        let rows = flat_book_snapshot_diff_rows(
            vec![100, 100],
            vec![vec!["0.10".to_string()], vec!["0.10".to_string()]],
            vec![vec!["1".to_string()], vec!["2".to_string()]],
            vec![Vec::new(), Vec::new()],
            vec![Vec::new(), Vec::new()],
            100,
            100,
        )
        .unwrap();

        assert_eq!(rows.first_snapshot_index, Some(0));
        assert_eq!(rows.event_index, vec![0, 0, 1]);
        assert_eq!(rows.action, vec![4, 1, 2]);
        assert_eq!(rows.side, vec![0, 1, 1]);
        assert_eq!(rows.price, vec![0.0, 0.10, 0.10]);
        assert_eq!(rows.size, vec![0.0, 1.0, 2.0]);
        assert_eq!(rows.flags, vec![0, 128, 128]);
        assert_eq!(rows.sequence, vec![0, 0, 1]);
        assert_eq!(rows.ts_event, vec![100, 100, 100]);
        assert_eq!(rows.ts_init, rows.ts_event);
    }

    #[test]
    fn onchain_fill_trade_rows_filters_sorts_and_deduplicates_ids() {
        let rows = onchain_fill_trade_rows(
            &[120, 100, 100, 110, 111],
            &[
                Some("0.42".to_string()),
                Some("0.50".to_string()),
                Some("1.00".to_string()),
                Some("nan".to_string()),
                Some("0.49".to_string()),
            ],
            &[
                Some("2".to_string()),
                Some("3".to_string()),
                Some("4".to_string()),
                Some("5".to_string()),
                Some("0".to_string()),
            ],
            Some(&[
                Some("sell".to_string()),
                Some("buyer".to_string()),
                Some("buy".to_string()),
                Some("ask".to_string()),
                Some("unknown".to_string()),
            ]),
            Some(&[
                Some("0xabc000000000000000000001".to_string()),
                Some("0xabc000000000000000000002".to_string()),
                Some("0xabc000000000000000000003".to_string()),
                Some("0xabc000000000000000000004".to_string()),
                Some("0xabc000000000000000000005".to_string()),
            ]),
            100,
            120,
            "YES",
        )
        .unwrap();

        assert_eq!(rows.price, vec![0.50, 0.42]);
        assert_eq!(rows.size, vec![3.0, 2.0]);
        assert_eq!(rows.aggressor_side, vec![1, 2]);
        assert_eq!(
            rows.trade_id,
            vec![
                "abc000000000000000000002-YES-000000",
                "abc000000000000000000001-YES-000000",
            ]
        );
        assert_eq!(rows.ts_event, vec![100, 120]);
        assert_eq!(rows.ts_init, rows.ts_event);
    }

    #[test]
    fn onchain_fill_trade_rows_disambiguates_same_timestamp_and_id() {
        let rows = onchain_fill_trade_rows(
            &[100, 100],
            &[Some("0.40".to_string()), Some("0.41".to_string())],
            &[Some("1".to_string()), Some("2".to_string())],
            None,
            Some(&[
                Some("same-trade-id".to_string()),
                Some("same-trade-id".to_string()),
            ]),
            100,
            100,
            "",
        )
        .unwrap();

        assert_eq!(
            rows.trade_id,
            vec!["same-trade-id-000000", "same-trade-id-000001"]
        );
        assert_eq!(rows.ts_event, vec![100, 101]);
        assert_eq!(rows.aggressor_side, vec![0, 0]);
    }

    #[test]
    fn local_consolidated_candidates_match_python_order() {
        let paths = local_consolidated_candidate_paths(
            "/root",
            "book_snapshot_full",
            "slug",
            0,
            Some("Yes"),
        );
        assert_eq!(
            paths,
            vec![
                PathBuf::from("/root/polymarket/slug/Yes/book_snapshot_full.parquet"),
                PathBuf::from("/root/polymarket/slug/outcome_id=0/book_snapshot_full.parquet"),
                PathBuf::from("/root/polymarket/slug/0/book_snapshot_full.parquet"),
                PathBuf::from("/root/polymarket/book_snapshot_full/slug/Yes.parquet"),
                PathBuf::from("/root/polymarket/book_snapshot_full/slug/outcome_id=0.parquet"),
                PathBuf::from("/root/polymarket/book_snapshot_full/slug/0.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug/Yes.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug/outcome_id=0.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug/0.parquet"),
            ]
        );
    }

    #[test]
    fn local_daily_candidates_match_python_order() {
        let paths = local_daily_candidate_paths(
            "/root",
            "book_snapshot_full",
            "2026-01-20",
            "slug",
            0,
            None,
        );
        assert_eq!(
            paths,
            vec![
                PathBuf::from(
                    "/root/polymarket/slug/outcome_id=0/book_snapshot_full/2026-01-20.parquet"
                ),
                PathBuf::from("/root/polymarket/slug/0/book_snapshot_full/2026-01-20.parquet"),
                PathBuf::from(
                    "/root/polymarket/book_snapshot_full/slug/outcome_id=0/2026-01-20.parquet"
                ),
                PathBuf::from("/root/polymarket/book_snapshot_full/slug/0/2026-01-20.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug/outcome_id=0/2026-01-20.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug/0/2026-01-20.parquet"),
                PathBuf::from("/root/polymarket/book_snapshot_full/slug_0_2026-01-20.parquet"),
                PathBuf::from("/root/book_snapshot_full/slug_0_2026-01-20.parquet"),
                PathBuf::from("/root/slug_0_2026-01-20.parquet"),
                PathBuf::from("/root/2026-01-20.parquet"),
            ]
        );
    }
}
