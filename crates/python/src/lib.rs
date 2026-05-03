use native_core::merge::{ReplayRecordKind, replay_merge_plan as core_replay_merge_plan};
use native_core::pmxt::{
    payload_delta_rows as core_pmxt_payload_delta_rows,
    payload_sort_key as core_pmxt_payload_sort_key,
    sort_payload_columns as core_pmxt_sort_payload_columns,
};
use native_core::telonex::{
    MaterializedCachePathSpec, api_cache_relative_path as core_telonex_api_cache_relative_path,
    api_url as core_telonex_api_url,
    deltas_cache_relative_path as core_telonex_deltas_cache_relative_path,
    flat_book_snapshot_diff_rows as core_telonex_flat_book_snapshot_diff_rows,
    local_consolidated_candidate_paths as core_telonex_local_consolidated_candidate_paths,
    local_daily_candidate_paths as core_telonex_local_daily_candidate_paths,
    onchain_fill_trade_rows as core_telonex_onchain_fill_trade_rows,
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
    PolymarketPublicTradeInput,
    polymarket_is_tradable_probability_price as core_polymarket_is_tradable_probability_price,
    polymarket_normalize_trade_side as core_polymarket_normalize_trade_side,
    polymarket_public_trade_rows as core_polymarket_public_trade_rows,
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

type PyPmxtDeltaRows = (
    bool,
    Option<i64>,
    Option<u8>,
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

type PyPolymarketPublicTradeRows = (
    Vec<f64>,
    Vec<f64>,
    Vec<u8>,
    Vec<String>,
    Vec<i64>,
    Vec<i64>,
    Vec<(usize, String)>,
    Vec<(usize, f64)>,
);

type PyNestedBookSideColumns = (Vec<Vec<String>>, Vec<Vec<String>>);

type PyReplayMergePlan = Vec<(u8, usize)>;

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
    let rows = core_telonex_flat_book_snapshot_diff_rows(
        timestamp_ns,
        py_sequence_string_columns(py, &bid_prices)?,
        py_sequence_string_columns(py, &bid_sizes)?,
        py_sequence_string_columns(py, &ask_prices)?,
        py_sequence_string_columns(py, &ask_sizes)?,
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
#[allow(clippy::too_many_arguments)]
fn telonex_nested_book_snapshot_diff_rows(
    py: Python<'_>,
    timestamp_ns: Vec<i64>,
    bids: Vec<Py<PyAny>>,
    asks: Vec<Py<PyAny>>,
    start_ns: i64,
    end_ns: i64,
) -> PyResult<PyTelonexFlatBookDiffRows> {
    let (bid_prices, bid_sizes) = py_nested_book_side_columns(py, &bids, false)?;
    let (ask_prices, ask_sizes) = py_nested_book_side_columns(py, &asks, true)?;
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

fn py_sequence_string_columns(py: Python<'_>, values: &[Py<PyAny>]) -> PyResult<Vec<Vec<String>>> {
    values
        .iter()
        .map(|value| {
            value
                .bind(py)
                .try_iter()?
                .map(|item| py_required_string(&item?))
                .collect()
        })
        .collect()
}

fn py_nested_book_side_columns(
    py: Python<'_>,
    values: &[Py<PyAny>],
    reverse: bool,
) -> PyResult<PyNestedBookSideColumns> {
    let mut price_columns = Vec::with_capacity(values.len());
    let mut size_columns = Vec::with_capacity(values.len());
    for value in values {
        let (prices, sizes) = py_nested_book_side(value.bind(py), reverse)?;
        price_columns.push(prices);
        size_columns.push(sizes);
    }
    Ok((price_columns, size_columns))
}

fn py_nested_book_side(
    value: &Bound<'_, PyAny>,
    reverse: bool,
) -> PyResult<(Vec<String>, Vec<String>)> {
    if value.is_none() {
        return Ok((Vec::new(), Vec::new()));
    }
    let Ok(iterator) = value.try_iter() else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut levels: Vec<(f64, String, String)> = Vec::new();
    for raw_level in iterator {
        let raw_level = raw_level?;
        if raw_level.is_none() {
            continue;
        }
        let Some(price_text) = py_level_field_string(&raw_level, "price")? else {
            continue;
        };
        let Some(size_text) = py_level_field_string(&raw_level, "size")? else {
            continue;
        };
        let size = parse_py_book_level_f64(&size_text, "size")?;
        if size <= 0.0 {
            continue;
        }
        let price = parse_py_book_level_f64(&price_text, "price")?;
        levels.push((price, price_text, size_text));
    }

    levels.sort_by(|left, right| {
        let ordering = left
            .0
            .partial_cmp(&right.0)
            .unwrap_or(std::cmp::Ordering::Equal);
        if reverse {
            ordering.reverse()
        } else {
            ordering
        }
    });
    let mut prices = Vec::with_capacity(levels.len());
    let mut sizes = Vec::with_capacity(levels.len());
    for (_price, price_text, size_text) in levels {
        prices.push(price_text);
        sizes.push(size_text);
    }
    Ok((prices, sizes))
}

fn py_level_field_string(value: &Bound<'_, PyAny>, field: &str) -> PyResult<Option<String>> {
    if let Ok(field_value) = value.getattr(field) {
        if field_value.is_none() {
            return Ok(None);
        }
        return Ok(Some(py_required_string(&field_value)?));
    }
    if let Ok(field_value) = value.call_method1("get", (field,)) {
        if field_value.is_none() {
            return Ok(None);
        }
        return Ok(Some(py_required_string(&field_value)?));
    }
    Ok(None)
}

fn parse_py_book_level_f64(value: &str, field: &str) -> PyResult<f64> {
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

fn py_required_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(value.str()?.to_str()?.to_string())
}

fn py_optional_string(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if value.is_none() {
        return Ok(None);
    }
    Ok(Some(py_required_string(value)?))
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
    let prices = py_optional_string_values(py, &prices)?;
    let sizes = py_optional_string_values(py, &sizes)?;
    let sides = sides
        .as_ref()
        .map(|values| py_optional_string_values(py, values))
        .transpose()?;
    let ids = ids
        .as_ref()
        .map(|values| py_optional_string_values(py, values))
        .transpose()?;
    let rows = core_telonex_onchain_fill_trade_rows(
        &timestamp_ns,
        &prices,
        &sizes,
        sides.as_deref(),
        ids.as_deref(),
        start_ns,
        end_ns,
        token_suffix,
    )
    .map_err(PyValueError::new_err)?;
    Ok((
        rows.price,
        rows.size,
        rows.aggressor_side,
        rows.trade_id,
        rows.ts_event,
        rows.ts_init,
    ))
}

fn py_optional_string_values(
    py: Python<'_>,
    values: &[Py<PyAny>],
) -> PyResult<Vec<Option<String>>> {
    values
        .iter()
        .map(|value| py_optional_string(value.bind(py)))
        .collect()
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
fn pmxt_sort_payload_columns(
    py: Python<'_>,
    update_type_columns: Vec<Py<PyAny>>,
    payload_text_columns: Vec<Py<PyAny>>,
) -> PyResult<Vec<(i64, u8, String, String)>> {
    core_pmxt_sort_payload_columns(
        py_sequence_string_columns(py, &update_type_columns)?,
        py_sequence_string_columns(py, &payload_text_columns)?,
    )
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
#[allow(clippy::too_many_arguments)]
fn pmxt_payload_delta_rows(
    py: Python<'_>,
    update_type_columns: Vec<Py<PyAny>>,
    payload_text_columns: Vec<Py<PyAny>>,
    token_id: &str,
    start_ns: i64,
    end_ns: i64,
    has_snapshot: bool,
    last_timestamp_ns: Option<i64>,
    last_priority: Option<u8>,
) -> PyResult<PyPmxtDeltaRows> {
    let rows = core_pmxt_payload_delta_rows(
        py_sequence_string_columns(py, &update_type_columns)?,
        py_sequence_string_columns(py, &payload_text_columns)?,
        token_id,
        i128::from(start_ns),
        i128::from(end_ns),
        has_snapshot,
        last_timestamp_ns.map(i128::from),
        last_priority,
    )
    .map_err(PyValueError::new_err)?;
    let last_timestamp_ns = rows
        .last_timestamp_ns
        .map(|timestamp_ns| {
            i64::try_from(timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
                ))
            })
        })
        .transpose()?;
    let ts_event = rows
        .ts_event
        .into_iter()
        .map(|timestamp_ns| {
            i64::try_from(timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
                ))
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let ts_init = rows
        .ts_init
        .into_iter()
        .map(|timestamp_ns| {
            i64::try_from(timestamp_ns).map_err(|_| {
                PyValueError::new_err(format!(
                    "PMXT payload timestamp nanoseconds are outside Python int64 timestamp bounds: {timestamp_ns}"
                ))
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok((
        rows.has_snapshot,
        last_timestamp_ns,
        rows.last_priority,
        rows.event_index,
        rows.action,
        rows.side,
        rows.price,
        rows.size,
        rows.flags,
        rows.sequence,
        ts_event,
        ts_init,
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

#[pyfunction]
fn polymarket_public_trade_rows(
    rows: Vec<(usize, i64, String, String, String, String, String)>,
    token_id: &str,
    sort: bool,
) -> PyResult<PyPolymarketPublicTradeRows> {
    let inputs = rows
        .into_iter()
        .map(
            |(original_index, timestamp, transaction_hash, asset, side, price, size)| {
                PolymarketPublicTradeInput {
                    original_index,
                    timestamp,
                    transaction_hash,
                    asset,
                    side,
                    price,
                    size,
                }
            },
        )
        .collect::<Vec<_>>();
    let rows = core_polymarket_public_trade_rows(&inputs, token_id, sort)
        .map_err(PyValueError::new_err)?;
    Ok((
        rows.price,
        rows.size,
        rows.aggressor_side,
        rows.trade_id,
        rows.ts_event,
        rows.ts_init,
        rows.unexpected_side_records,
        rows.skipped_price_records,
    ))
}

#[pyfunction]
fn replay_merge_plan(
    book_ts_events: Vec<i64>,
    book_ts_inits: Vec<i64>,
    trade_ts_events: Vec<i64>,
    trade_ts_inits: Vec<i64>,
) -> PyResult<PyReplayMergePlan> {
    core_replay_merge_plan(
        &book_ts_events,
        &book_ts_inits,
        &trade_ts_events,
        &trade_ts_inits,
    )
    .map(|entries| {
        entries
            .into_iter()
            .map(|entry| {
                (
                    match entry.kind {
                        ReplayRecordKind::Book => 0,
                        ReplayRecordKind::Trade => 1,
                    },
                    entry.index,
                )
            })
            .collect()
    })
    .map_err(PyValueError::new_err)
}

#[pymodule]
fn _native_ext(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(decimal_seconds_to_ns, module)?)?;
    module.add_function(wrap_pyfunction!(float_seconds_to_ms_string, module)?)?;
    module.add_function(wrap_pyfunction!(native_available, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_payload_delta_rows, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_payload_sort_key, module)?)?;
    module.add_function(wrap_pyfunction!(pmxt_sort_payload_columns, module)?)?;
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
    module.add_function(wrap_pyfunction!(polymarket_public_trade_rows, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_sort_key, module)?)?;
    module.add_function(wrap_pyfunction!(polymarket_trade_sort_keys, module)?)?;
    module.add_function(wrap_pyfunction!(replay_merge_plan, module)?)?;
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
    module.add_function(wrap_pyfunction!(
        telonex_nested_book_snapshot_diff_rows,
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
