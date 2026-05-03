use std::collections::HashMap;

use native_core::pmxt::{
    PmxtUpdateClass, extract_payload_fields as core_pmxt_extract_payload_fields,
    payload_sort_key as core_pmxt_payload_sort_key,
    sort_payload_columns as core_pmxt_sort_payload_columns,
    sort_payloads as core_pmxt_sort_payloads,
};
use native_core::telonex::{
    MaterializedCachePathSpec, api_cache_relative_path as core_telonex_api_cache_relative_path,
    api_url as core_telonex_api_url,
    deltas_cache_relative_path as core_telonex_deltas_cache_relative_path,
    local_consolidated_candidate_paths as core_telonex_local_consolidated_candidate_paths,
    local_daily_candidate_paths as core_telonex_local_daily_candidate_paths,
    telonex_day_window_ns as core_telonex_day_window_ns,
    telonex_source_days_for_window as core_telonex_source_days_for_window,
    telonex_source_label_kind as core_telonex_source_label_kind,
    telonex_stage_for_source as core_telonex_stage_for_source,
    trade_ticks_cache_relative_path as core_telonex_trade_ticks_cache_relative_path,
};
use native_core::time::{
    decimal_seconds_to_ns as core_decimal_seconds_to_ns,
    float_seconds_to_ms_string as core_float_seconds_to_ms_string,
};
use native_core::trades::{
    polymarket_is_tradable_probability_price as core_polymarket_is_tradable_probability_price,
    polymarket_normalize_trade_side as core_polymarket_normalize_trade_side,
    polymarket_trade_event_timestamp_ns as core_polymarket_trade_event_timestamp_ns,
    polymarket_trade_id as core_polymarket_trade_id,
    polymarket_trade_sort_key as core_polymarket_trade_sort_key,
};
use native_core::windows::{
    WindowSemantics, pmxt_archive_hours_for_window as core_pmxt_archive_hours_for_window,
    source_days_for_window as core_source_days_for_window,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

type PyPmxtPayloadFields = (
    String,
    String,
    i64,
    String,
    String,
    Option<(String, String, String)>,
);

type PyTelonexFlatBookDiffRows = (
    Option<usize>,
    Vec<i32>,
    Vec<u8>,
    Vec<u8>,
    Vec<f64>,
    Vec<f64>,
    Vec<u8>,
    Vec<i32>,
    Vec<i64>,
    Vec<i64>,
);

type PyTelonexTradeTickRows = (Vec<f64>, Vec<f64>, Vec<u8>, Vec<String>, Vec<i64>, Vec<i64>);

#[derive(Clone, Debug, Default)]
struct PyTelonexFlatBookRows {
    first_snapshot_index: Option<usize>,
    event_index: Vec<i32>,
    action: Vec<u8>,
    side: Vec<u8>,
    price: Vec<f64>,
    size: Vec<f64>,
    flags: Vec<u8>,
    sequence: Vec<i32>,
    ts_event: Vec<i64>,
    ts_init: Vec<i64>,
}

#[derive(Clone, Debug)]
struct PyTelonexBookLevel {
    price_text: String,
    size_text: String,
    price: f64,
    size: f64,
}

#[derive(Clone, Copy, Debug)]
struct PyFlatBookChange {
    side: u8,
    price: f64,
    size: f64,
}

const PY_ORDER_SIDE_BUY: u8 = 1;
const PY_ORDER_SIDE_SELL: u8 = 2;
const PY_BOOK_ACTION_UPDATE: u8 = 2;
const PY_BOOK_ACTION_DELETE: u8 = 3;
const PY_RECORD_FLAG_LAST: u8 = 128;
const PY_AGGRESSOR_NO_AGGRESSOR: u8 = 0;
const PY_AGGRESSOR_BUYER: u8 = 1;
const PY_AGGRESSOR_SELLER: u8 = 2;

#[pyfunction]
fn native_available() -> bool {
    native_core::native_available()
}

#[pyfunction]
fn source_days_for_window(start_ns: i64, end_ns: i64, semantics: &str) -> PyResult<Vec<String>> {
    let semantics = match semantics.trim().to_ascii_lowercase().as_str() {
        "half_open" | "half-open" => WindowSemantics::HalfOpen,
        "inclusive" => WindowSemantics::Inclusive,
        value => {
            return Err(PyValueError::new_err(format!(
                "unsupported window semantics {value:?}; use 'half_open' or 'inclusive'"
            )));
        }
    };
    Ok(core_source_days_for_window(
        i128::from(start_ns),
        i128::from(end_ns),
        semantics,
    ))
}

#[pyfunction]
fn telonex_source_days_for_window(start_ns: i64, end_ns: i64) -> Vec<String> {
    core_telonex_source_days_for_window(i128::from(start_ns), i128::from(end_ns))
}

#[pyfunction]
fn telonex_day_window_ns(date: &str, start_ns: i64, end_ns: i64) -> Option<(i64, i64)> {
    core_telonex_day_window_ns(date, i128::from(start_ns), i128::from(end_ns)).and_then(
        |(start_ns, end_ns)| Some((i64::try_from(start_ns).ok()?, i64::try_from(end_ns).ok()?)),
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn telonex_flat_book_snapshot_diff_rows(
    py: Python<'_>,
    timestamp_ns: Vec<i64>,
    bid_prices: Vec<Py<PyAny>>,
    bid_sizes: Vec<Py<PyAny>>,
    ask_prices: Vec<Py<PyAny>>,
    ask_sizes: Vec<Py<PyAny>>,
    start_ns: i64,
    end_ns: i64,
) -> PyResult<PyTelonexFlatBookDiffRows> {
    let rows = py_telonex_flat_book_snapshot_diff_rows(
        py,
        &timestamp_ns,
        &bid_prices,
        &bid_sizes,
        &ask_prices,
        &ask_sizes,
        start_ns,
        end_ns,
    )?;
    Ok((
        rows.first_snapshot_index,
        rows.event_index,
        rows.action,
        rows.side,
        rows.price,
        rows.size,
        rows.flags,
        rows.sequence,
        rows.ts_event,
        rows.ts_init,
    ))
}

#[allow(clippy::too_many_arguments)]
fn py_telonex_flat_book_snapshot_diff_rows(
    py: Python<'_>,
    timestamp_ns: &[i64],
    bid_prices: &[Py<PyAny>],
    bid_sizes: &[Py<PyAny>],
    ask_prices: &[Py<PyAny>],
    ask_sizes: &[Py<PyAny>],
    start_ns: i64,
    end_ns: i64,
) -> PyResult<PyTelonexFlatBookRows> {
    let row_count = timestamp_ns.len();
    if bid_prices.len() != row_count
        || bid_sizes.len() != row_count
        || ask_prices.len() != row_count
        || ask_sizes.len() != row_count
    {
        return Err(PyValueError::new_err(format!(
            "Telonex flat book columns have inconsistent lengths: timestamp_ns={}, bid_prices={}, bid_sizes={}, ask_prices={}, ask_sizes={}",
            timestamp_ns.len(),
            bid_prices.len(),
            bid_sizes.len(),
            ask_prices.len(),
            ask_sizes.len()
        )));
    }

    let mut order: Vec<usize> = timestamp_ns
        .iter()
        .enumerate()
        .filter_map(|(idx, timestamp)| (*timestamp <= end_ns).then_some(idx))
        .collect();
    if order.is_empty() {
        return Ok(PyTelonexFlatBookRows::default());
    }
    order.sort_by_key(|idx| timestamp_ns[*idx]);

    let mut rows = PyTelonexFlatBookRows::default();
    let mut previous_bids: Option<Vec<PyTelonexBookLevel>> = None;
    let mut previous_asks: Option<Vec<PyTelonexBookLevel>> = None;
    let mut emitted_snapshot = false;
    let mut output_event_index: i32 = 0;

    for idx in order {
        let ts_event = timestamp_ns[idx];
        let current_bids =
            py_flat_book_side_map(py, bid_prices[idx].bind(py), bid_sizes[idx].bind(py))?;
        let current_asks =
            py_flat_book_side_map(py, ask_prices[idx].bind(py), ask_sizes[idx].bind(py))?;

        if ts_event < start_ns {
            previous_bids = Some(current_bids);
            previous_asks = Some(current_asks);
            continue;
        }

        if !emitted_snapshot {
            rows.first_snapshot_index = Some(idx);
            emitted_snapshot = true;
        } else if let (Some(prev_bids), Some(prev_asks)) =
            (previous_bids.as_ref(), previous_asks.as_ref())
        {
            let mut changes =
                py_flat_book_side_changes(PY_ORDER_SIDE_BUY, prev_bids, &current_bids, false);
            changes.extend(py_flat_book_side_changes(
                PY_ORDER_SIDE_SELL,
                prev_asks,
                &current_asks,
                true,
            ));
            let change_count = changes.len();
            if change_count > 0 {
                for (change_idx, change) in changes.into_iter().enumerate() {
                    rows.event_index.push(output_event_index);
                    rows.action.push(if change.size > 0.0 {
                        PY_BOOK_ACTION_UPDATE
                    } else {
                        PY_BOOK_ACTION_DELETE
                    });
                    rows.side.push(change.side);
                    rows.price.push(change.price);
                    rows.size.push(change.size);
                    rows.flags.push(if change_idx + 1 == change_count {
                        PY_RECORD_FLAG_LAST
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

fn py_flat_book_side_map(
    _py: Python<'_>,
    prices: &Bound<'_, PyAny>,
    sizes: &Bound<'_, PyAny>,
) -> PyResult<Vec<PyTelonexBookLevel>> {
    let mut price_iter = prices.try_iter()?;
    let mut size_iter = sizes.try_iter()?;
    let mut levels: Vec<PyTelonexBookLevel> = Vec::new();

    while let (Some(price_obj), Some(size_obj)) = (price_iter.next(), size_iter.next()) {
        let price_obj = price_obj?;
        let size_obj = size_obj?;
        let size_text = py_string(&size_obj)?;
        let size = parse_py_finite_f64(&size_text, "size")?;
        if size <= 0.0 {
            continue;
        }
        let price_text = py_string(&price_obj)?;
        let price = parse_py_finite_f64(&price_text, "price")?;
        if let Some(existing) = levels
            .iter_mut()
            .find(|level| level.price_text == price_text)
        {
            existing.size_text = size_text;
            existing.price = price;
            existing.size = size;
        } else {
            levels.push(PyTelonexBookLevel {
                price_text,
                size_text,
                price,
                size,
            });
        }
    }
    Ok(levels)
}

fn py_flat_book_side_changes(
    side: u8,
    previous: &[PyTelonexBookLevel],
    current: &[PyTelonexBookLevel],
    reverse: bool,
) -> Vec<PyFlatBookChange> {
    let mut keys: Vec<(&str, f64)> = Vec::with_capacity(previous.len() + current.len());
    for level in previous.iter().chain(current.iter()) {
        if !keys
            .iter()
            .any(|(price_text, _)| *price_text == level.price_text)
        {
            keys.push((level.price_text.as_str(), level.price));
        }
    }
    keys.sort_by(|left, right| {
        let ordering = left
            .1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal);
        if reverse {
            ordering.reverse()
        } else {
            ordering
        }
    });

    let mut changes = Vec::new();
    for (key, _price) in keys {
        let previous_size = previous
            .iter()
            .find(|level| level.price_text == key)
            .map(|level| level.size_text.as_str());
        let current_level = current.iter().find(|level| level.price_text == key);
        let current_size = current_level.map(|level| level.size_text.as_str());
        if current_size == previous_size {
            continue;
        }
        if let Some(level) = current_level {
            changes.push(PyFlatBookChange {
                side,
                price: level.price,
                size: level.size,
            });
        } else if let Some(level) = previous.iter().find(|level| level.price_text == key) {
            changes.push(PyFlatBookChange {
                side,
                price: level.price,
                size: 0.0,
            });
        }
    }
    changes
}

fn py_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(value.str()?.to_str()?.to_string())
}

fn parse_py_finite_f64(value: &str, field: &str) -> PyResult<f64> {
    let parsed = value.parse::<f64>().map_err(|_| {
        PyValueError::new_err(format!("invalid Telonex book level {field}: {value:?}"))
    })?;
    if !parsed.is_finite() {
        return Err(PyValueError::new_err(format!(
            "invalid Telonex book level {field}: {value:?}"
        )));
    }
    Ok(parsed)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn telonex_onchain_fill_trade_rows(
    py: Python<'_>,
    timestamp_ns: Vec<i64>,
    prices: Vec<Py<PyAny>>,
    sizes: Vec<Py<PyAny>>,
    sides: Option<Vec<Py<PyAny>>>,
    ids: Option<Vec<Py<PyAny>>>,
    start_ns: i64,
    end_ns: i64,
    token_suffix: &str,
) -> PyResult<PyTelonexTradeTickRows> {
    let row_count = timestamp_ns.len();
    if prices.len() != row_count || sizes.len() != row_count {
        return Err(PyValueError::new_err(format!(
            "Telonex trade columns have inconsistent lengths: timestamp_ns={}, prices={}, sizes={}",
            timestamp_ns.len(),
            prices.len(),
            sizes.len()
        )));
    }
    if let Some(sides) = sides.as_ref()
        && sides.len() != row_count
    {
        return Err(PyValueError::new_err(format!(
            "Telonex trade side column length does not match timestamp_ns: timestamp_ns={}, sides={}",
            timestamp_ns.len(),
            sides.len()
        )));
    }
    if let Some(ids) = ids.as_ref()
        && ids.len() != row_count
    {
        return Err(PyValueError::new_err(format!(
            "Telonex trade id column length does not match timestamp_ns: timestamp_ns={}, ids={}",
            timestamp_ns.len(),
            ids.len()
        )));
    }

    let mut order: Vec<usize> = timestamp_ns
        .iter()
        .enumerate()
        .filter_map(|(idx, timestamp)| {
            (*timestamp >= start_ns && *timestamp <= end_ns).then_some(idx)
        })
        .collect();
    order.sort_by_key(|idx| timestamp_ns[*idx]);

    let mut out_prices = Vec::new();
    let mut out_sizes = Vec::new();
    let mut out_aggressor_sides = Vec::new();
    let mut out_trade_ids = Vec::new();
    let mut out_ts_events = Vec::new();
    let mut out_ts_inits = Vec::new();
    let mut timestamp_counts: HashMap<i64, usize> = HashMap::new();
    let mut trade_id_counts: HashMap<String, usize> = HashMap::new();
    let token_suffix = token_suffix.trim();

    for (sorted_index, idx) in order.into_iter().enumerate() {
        let raw_price = prices[idx].bind(py);
        let raw_size = sizes[idx].bind(py);
        let Some(price) = py_float_value(raw_price) else {
            continue;
        };
        let Some(size) = py_float_value(raw_size) else {
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
            .as_ref()
            .map_or(PY_AGGRESSOR_NO_AGGRESSOR, |side_values| {
                py_aggressor_side_from_value(side_values[idx].bind(py))
            });

        let raw_id = ids.as_ref().map_or_else(
            || format!("telonex-{base_ts_event}-{sorted_index}"),
            |id_values| py_string(id_values[idx].bind(py)).unwrap_or_default(),
        );
        let raw_id = if raw_id.is_empty() || raw_id.eq_ignore_ascii_case("nan") {
            format!("telonex-{base_ts_event}")
        } else {
            raw_id
        };
        let sequence = *trade_id_counts.get(&raw_id).unwrap_or(&0);
        trade_id_counts.insert(raw_id.clone(), sequence + 1);
        let id_suffix = suffix_chars(&raw_id, 24);
        let trade_id = if token_suffix.is_empty() {
            format!("{id_suffix}-{sequence:06}")
        } else {
            format!("{id_suffix}-{token_suffix}-{sequence:06}")
        };

        out_prices.push(price);
        out_sizes.push(size);
        out_aggressor_sides.push(aggressor_side);
        out_trade_ids.push(trade_id);
        out_ts_events.push(ts_event);
        out_ts_inits.push(ts_event);
    }

    Ok((
        out_prices,
        out_sizes,
        out_aggressor_sides,
        out_trade_ids,
        out_ts_events,
        out_ts_inits,
    ))
}

fn py_float_value(value: &Bound<'_, PyAny>) -> Option<f64> {
    if value.is_none() {
        return None;
    }
    if let Ok(parsed) = value.extract::<f64>()
        && parsed.is_finite()
    {
        return Some(parsed);
    }
    let text_obj = value.str().ok()?;
    let text = text_obj.to_str().ok()?;
    let parsed = text.parse::<f64>().ok()?;
    parsed.is_finite().then_some(parsed)
}

fn py_aggressor_side_from_value(value: &Bound<'_, PyAny>) -> u8 {
    let Ok(text) = py_string(value) else {
        return PY_AGGRESSOR_NO_AGGRESSOR;
    };
    let normalized = text.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "buy" | "buyer" | "bid" | "bidder" | "taker_buy" | "buying" => PY_AGGRESSOR_BUYER,
        "sell" | "seller" | "ask" | "offer" | "taker_sell" | "selling" => PY_AGGRESSOR_SELLER,
        _ => PY_AGGRESSOR_NO_AGGRESSOR,
    }
}

fn suffix_chars(value: &str, count: usize) -> String {
    let mut chars = value.chars().rev().take(count).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

#[pyfunction]
fn telonex_source_label_kind(source: &str) -> Option<String> {
    core_telonex_source_label_kind(source).map(|kind| kind.as_str().to_string())
}

#[pyfunction]
fn telonex_stage_for_source(source: &str) -> String {
    core_telonex_stage_for_source(source).as_str().to_string()
}

#[pyfunction]
fn telonex_api_url(
    base_url: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> String {
    core_telonex_api_url(base_url, channel, date, market_slug, token_index, outcome)
}

#[pyfunction]
fn telonex_api_cache_relative_path(
    base_url_key: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> String {
    core_telonex_api_cache_relative_path(
        base_url_key,
        channel,
        date,
        market_slug,
        token_index,
        outcome,
    )
    .to_string_lossy()
    .into_owned()
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn telonex_deltas_cache_relative_path(
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
    instrument_key: &str,
    start_ns: i64,
    end_ns: i64,
) -> String {
    core_telonex_deltas_cache_relative_path(MaterializedCachePathSpec {
        channel,
        date,
        market_slug,
        token_index,
        outcome,
        instrument_key,
        start_ns: i128::from(start_ns),
        end_ns: i128::from(end_ns),
    })
    .to_string_lossy()
    .into_owned()
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn telonex_trade_ticks_cache_relative_path(
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
    instrument_key: &str,
    start_ns: i64,
    end_ns: i64,
) -> String {
    core_telonex_trade_ticks_cache_relative_path(MaterializedCachePathSpec {
        channel,
        date,
        market_slug,
        token_index,
        outcome,
        instrument_key,
        start_ns: i128::from(start_ns),
        end_ns: i128::from(end_ns),
    })
    .to_string_lossy()
    .into_owned()
}

#[pyfunction]
fn telonex_local_consolidated_candidate_paths(
    root: &str,
    channel: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> Vec<String> {
    core_telonex_local_consolidated_candidate_paths(
        root,
        channel,
        market_slug,
        token_index,
        outcome,
    )
    .into_iter()
    .map(|path| path.to_string_lossy().into_owned())
    .collect()
}

#[pyfunction]
fn telonex_local_daily_candidate_paths(
    root: &str,
    channel: &str,
    date: &str,
    market_slug: &str,
    token_index: i64,
    outcome: Option<&str>,
) -> Vec<String> {
    core_telonex_local_daily_candidate_paths(root, channel, date, market_slug, token_index, outcome)
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

#[pyfunction]
fn pmxt_archive_hours_for_window(start_ns: i64, end_ns: i64) -> PyResult<Vec<i64>> {
    core_pmxt_archive_hours_for_window(i128::from(start_ns), i128::from(end_ns))
        .into_iter()
        .map(|hour_ns| {
            i64::try_from(hour_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT archive hour nanoseconds are outside Python int64 timestamp bounds: {hour_ns}"
                ))
            })
        })
        .collect()
}

#[pyfunction]
fn decimal_seconds_to_ns(value: &str) -> PyResult<i64> {
    let timestamp_ns = core_decimal_seconds_to_ns(value).map_err(PyValueError::new_err)?;
    i64::try_from(timestamp_ns).map_err(|_| {
        PyValueError::new_err(format!(
            "timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
        ))
    })
}

#[pyfunction]
fn float_seconds_to_ms_string(value: f64) -> String {
    core_float_seconds_to_ms_string(value)
}

#[pyfunction]
fn pmxt_payload_sort_key(update_type: &str, payload_text: &str) -> PyResult<(i64, u8)> {
    let (timestamp_ns, priority) =
        core_pmxt_payload_sort_key(update_type, payload_text).map_err(PyValueError::new_err)?;
    let timestamp_ns = i64::try_from(timestamp_ns).map_err(|_| {
        PyValueError::new_err(format!(
            "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
        ))
    })?;
    Ok((timestamp_ns, priority))
}

#[pyfunction]
fn pmxt_payload_sort_keys(items: Vec<(String, String)>) -> PyResult<Vec<(i64, u8)>> {
    items
        .into_iter()
        .map(|(update_type, payload_text)| {
            let (timestamp_ns, priority) = core_pmxt_payload_sort_key(&update_type, &payload_text)
                .map_err(PyValueError::new_err)?;
            let timestamp_ns = i64::try_from(timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
                ))
            })?;
            Ok((timestamp_ns, priority))
        })
        .collect()
}

#[pyfunction]
fn pmxt_sort_payloads(items: Vec<(String, String)>) -> PyResult<Vec<(i64, u8, String, String)>> {
    core_pmxt_sort_payloads(items)
        .map_err(PyValueError::new_err)?
        .into_iter()
        .map(|payload| {
            let timestamp_ns = i64::try_from(payload.timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {}",
                    payload.timestamp_ns
                ))
            })?;
            Ok((
                timestamp_ns,
                payload.priority,
                payload.update_type,
                payload.payload_text,
            ))
        })
        .collect()
}

#[pyfunction]
fn pmxt_sort_payload_columns(
    update_type_columns: Vec<Vec<String>>,
    payload_text_columns: Vec<Vec<String>>,
) -> PyResult<Vec<(i64, u8, String, String)>> {
    core_pmxt_sort_payload_columns(update_type_columns, payload_text_columns)
        .map_err(PyValueError::new_err)?
        .into_iter()
        .map(|payload| {
            let timestamp_ns = i64::try_from(payload.timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {}",
                    payload.timestamp_ns
                ))
            })?;
            Ok((
                timestamp_ns,
                payload.priority,
                payload.update_type,
                payload.payload_text,
            ))
        })
        .collect()
}

#[pyfunction]
fn pmxt_extract_payload_fields(payload_text: &str) -> PyResult<PyPmxtPayloadFields> {
    let fields = core_pmxt_extract_payload_fields(payload_text).map_err(PyValueError::new_err)?;
    let update_class = match fields.update_class {
        PmxtUpdateClass::BookSnapshot => "book_snapshot",
        PmxtUpdateClass::PriceChange => "price_change",
        PmxtUpdateClass::Other => "other",
    };
    let timestamp_ns = i64::try_from(fields.timestamp_ns).map_err(|_| {
        PyValueError::new_err(format!(
            "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {}",
            fields.timestamp_ns
        ))
    })?;
    Ok((
        fields.update_type.to_string(),
        update_class.to_string(),
        timestamp_ns,
        fields.market_id.to_string(),
        fields.token_id.to_string(),
        fields.price_change.map(|price_change| {
            (
                price_change.side.to_string(),
                price_change.price.to_string(),
                price_change.size.to_string(),
            )
        }),
    ))
}

#[pyfunction]
fn polymarket_trade_sort_key(
    timestamp: i64,
    transaction_hash: &str,
    asset: &str,
    side: &str,
    price: &str,
    size: &str,
) -> (i64, String, String, String, String, String) {
    let key = core_polymarket_trade_sort_key(timestamp, transaction_hash, asset, side, price, size);
    (
        key.timestamp,
        key.transaction_hash,
        key.asset,
        key.side,
        key.price,
        key.size,
    )
}

#[pyfunction]
fn polymarket_trade_sort_keys(
    rows: Vec<(i64, String, String, String, String, String)>,
) -> Vec<(i64, String, String, String, String, String)> {
    rows.into_iter()
        .map(|(timestamp, transaction_hash, asset, side, price, size)| {
            let key = core_polymarket_trade_sort_key(
                timestamp,
                &transaction_hash,
                &asset,
                &side,
                &price,
                &size,
            );
            (
                key.timestamp,
                key.transaction_hash,
                key.asset,
                key.side,
                key.price,
                key.size,
            )
        })
        .collect()
}

#[pyfunction]
fn polymarket_trade_id(transaction_hash: &str, asset: &str, sequence: usize) -> String {
    core_polymarket_trade_id(transaction_hash, asset, sequence)
}

#[pyfunction]
fn polymarket_trade_ids(rows: Vec<(String, String, usize)>) -> Vec<String> {
    rows.into_iter()
        .map(|(transaction_hash, asset, sequence)| {
            core_polymarket_trade_id(&transaction_hash, &asset, sequence)
        })
        .collect()
}

#[pyfunction]
fn polymarket_normalize_trade_side(side: &str) -> String {
    core_polymarket_normalize_trade_side(side)
        .as_str()
        .to_string()
}

#[pyfunction]
fn polymarket_normalize_trade_sides(sides: Vec<String>) -> Vec<String> {
    sides
        .into_iter()
        .map(|side| {
            core_polymarket_normalize_trade_side(&side)
                .as_str()
                .to_string()
        })
        .collect()
}

#[pyfunction]
fn polymarket_is_tradable_probability_price(price: &str) -> bool {
    core_polymarket_is_tradable_probability_price(price)
}

#[pyfunction]
fn polymarket_are_tradable_probability_prices(prices: Vec<String>) -> Vec<bool> {
    prices
        .into_iter()
        .map(|price| core_polymarket_is_tradable_probability_price(&price))
        .collect()
}

#[pyfunction]
fn polymarket_trade_event_timestamp_ns(
    base_timestamp_ns: i64,
    occurrence_in_second: usize,
) -> PyResult<i64> {
    core_polymarket_trade_event_timestamp_ns(base_timestamp_ns, occurrence_in_second)
        .map_err(PyValueError::new_err)
}

#[pyfunction]
fn polymarket_trade_event_timestamp_ns_batch(rows: Vec<(i64, usize)>) -> PyResult<Vec<i64>> {
    rows.into_iter()
        .map(|(base_timestamp_ns, occurrence_in_second)| {
            core_polymarket_trade_event_timestamp_ns(base_timestamp_ns, occurrence_in_second)
                .map_err(PyValueError::new_err)
        })
        .collect()
}

#[pymodule]
fn _native_ext(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(decimal_seconds_to_ns, module)?)?;
    module.add_function(wrap_pyfunction!(float_seconds_to_ms_string, module)?)?;
    module.add_function(wrap_pyfunction!(native_available, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_extract_payload_fields, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_payload_sort_key, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_payload_sort_keys, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_sort_payload_columns, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_sort_payloads, module)?)?;
    module.add_function(wrap_pyfunction!(
        polymarket_are_tradable_probability_prices,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        polymarket_is_tradable_probability_price,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(polymarket_normalize_trade_side, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_normalize_trade_sides, module)?)?;
    module.add_function(wrap_pyfunction!(
        polymarket_trade_event_timestamp_ns,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        polymarket_trade_event_timestamp_ns_batch,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_id, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_ids, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_sort_key, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_sort_keys, module)?)?;
    module.add_function(wrap_pyfunction!(source_days_for_window, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_archive_hours_for_window, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_api_cache_relative_path, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_api_url, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_day_window_ns, module)?)?;
    module.add_function(wrap_pyfunction!(
        telonex_deltas_cache_relative_path,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        telonex_flat_book_snapshot_diff_rows,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        telonex_local_consolidated_candidate_paths,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        telonex_local_daily_candidate_paths,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(telonex_onchain_fill_trade_rows, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_source_days_for_window, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_source_label_kind, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_stage_for_source, module)?)?;
    module.add_function(wrap_pyfunction!(
        telonex_trade_ticks_cache_relative_path,
        module
    )?)?;
    Ok(())
}
