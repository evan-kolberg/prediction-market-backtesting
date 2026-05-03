use std::collections::{HashMap, HashSet};

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
fn telonex_flat_book_snapshot_diff_rows(
    timestamp_ns: Vec<i64>,
    bid_prices: Vec<Vec<String>>,
    bid_sizes: Vec<Vec<String>>,
    ask_prices: Vec<Vec<String>>,
    ask_sizes: Vec<Vec<String>>,
    start_ns: i64,
    end_ns: i64,
) -> PyResult<PyTelonexFlatBookDiffRows> {
    let rows = core_telonex_flat_book_snapshot_diff_rows(
        timestamp_ns,
        bid_prices,
        bid_sizes,
        ask_prices,
        ask_sizes,
        start_ns,
        end_ns,
    )
    .map_err(PyValueError::new_err)?;
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
    module.add_function(wrap_pyfunction!(telonex_source_days_for_window, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_source_label_kind, module)?)?;
    module.add_function(wrap_pyfunction!(telonex_stage_for_source, module)?)?;
    module.add_function(wrap_pyfunction!(
        telonex_trade_ticks_cache_relative_path,
        module
    )?)?;
    Ok(())
}
