use crate::time::decimal_seconds_to_ns;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PmxtUpdateClass {
    BookSnapshot,
    PriceChange,
    Other,
}

impl PmxtUpdateClass {
    pub fn from_update_type(update_type: &str) -> Self {
        match update_type {
            "book_snapshot" => Self::BookSnapshot,
            "price_change" => Self::PriceChange,
            _ => Self::Other,
        }
    }

    pub fn sort_priority(self) -> u8 {
        match self {
            Self::BookSnapshot => 0,
            Self::PriceChange => 1,
            Self::Other => 2,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PmxtPriceChangeFields<'a> {
    pub side: &'a str,
    pub price: &'a str,
    pub size: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PmxtPayloadFields<'a> {
    pub update_type: &'a str,
    pub update_class: PmxtUpdateClass,
    pub timestamp_ns: i128,
    pub market_id: &'a str,
    pub token_id: &'a str,
    pub price_change: Option<PmxtPriceChangeFields<'a>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PmxtSortedPayload {
    pub timestamp_ns: i128,
    pub priority: u8,
    pub update_type: String,
    pub payload_text: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PmxtDeltaRows {
    pub has_snapshot: bool,
    pub last_timestamp_ns: Option<i128>,
    pub last_priority: Option<u8>,
    pub event_index: Vec<i32>,
    pub action: Vec<u8>,
    pub side: Vec<u8>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub flags: Vec<u8>,
    pub sequence: Vec<i32>,
    pub ts_event: Vec<i128>,
    pub ts_init: Vec<i128>,
}

const ORDER_SIDE_NO_ORDER_SIDE: u8 = 0;
const ORDER_SIDE_BUY: u8 = 1;
const ORDER_SIDE_SELL: u8 = 2;
const BOOK_ACTION_ADD: u8 = 1;
const BOOK_ACTION_UPDATE: u8 = 2;
const BOOK_ACTION_DELETE: u8 = 3;
const BOOK_ACTION_CLEAR: u8 = 4;
const RECORD_FLAG_LAST: u8 = 128;

pub fn extract_payload_fields(payload_text: &str) -> Result<PmxtPayloadFields<'_>, String> {
    let update_type = json_string_field_literal(payload_text, "update_type")?;
    let update_class = PmxtUpdateClass::from_update_type(update_type);
    let timestamp = json_number_field_literal(payload_text, "timestamp")?;
    let timestamp_ns = decimal_seconds_to_ns(timestamp)?;
    let market_id = json_string_field_literal(payload_text, "market_id")?;
    let token_id = json_string_field_literal(payload_text, "token_id")?;
    let price_change = match update_class {
        PmxtUpdateClass::PriceChange => Some(PmxtPriceChangeFields {
            side: json_string_field_literal(payload_text, "change_side")?,
            price: json_string_field_literal(payload_text, "change_price")?,
            size: json_string_field_literal(payload_text, "change_size")?,
        }),
        PmxtUpdateClass::BookSnapshot | PmxtUpdateClass::Other => None,
    };

    Ok(PmxtPayloadFields {
        update_type,
        update_class,
        timestamp_ns,
        market_id,
        token_id,
        price_change,
    })
}

pub fn payload_sort_key(update_type: &str, payload_text: &str) -> Result<(i128, u8), String> {
    let update_class = PmxtUpdateClass::from_update_type(update_type);
    let priority = update_class.sort_priority();
    if update_class == PmxtUpdateClass::Other {
        return Ok((0, priority));
    }
    let timestamp = json_number_field_literal(payload_text, "timestamp")?;
    let timestamp_ns = decimal_seconds_to_ns(timestamp)?;
    Ok((timestamp_ns, priority))
}

pub fn sort_payloads(
    items: impl IntoIterator<Item = (String, String)>,
) -> Result<Vec<PmxtSortedPayload>, String> {
    let mut sorted_payloads = Vec::new();
    for (update_type, payload_text) in items {
        let (timestamp_ns, priority) = payload_sort_key(&update_type, &payload_text)?;
        sorted_payloads.push(PmxtSortedPayload {
            timestamp_ns,
            priority,
            update_type,
            payload_text,
        });
    }
    sorted_payloads.sort_by(|left, right| {
        (left.timestamp_ns, left.priority).cmp(&(right.timestamp_ns, right.priority))
    });
    Ok(sorted_payloads)
}

pub fn sort_payload_columns(
    update_type_columns: Vec<Vec<String>>,
    payload_text_columns: Vec<Vec<String>>,
) -> Result<Vec<PmxtSortedPayload>, String> {
    if update_type_columns.len() != payload_text_columns.len() {
        return Err(format!(
            "PMXT payload column count mismatch: {} update_type column(s), {} payload column(s)",
            update_type_columns.len(),
            payload_text_columns.len()
        ));
    }

    let row_count = update_type_columns.iter().map(Vec::len).sum();
    let mut sorted_payloads = Vec::with_capacity(row_count);
    for (column_index, (update_types, payload_texts)) in update_type_columns
        .into_iter()
        .zip(payload_text_columns)
        .enumerate()
    {
        if update_types.len() != payload_texts.len() {
            return Err(format!(
                "PMXT payload row count mismatch in column {column_index}: {} update_type row(s), {} payload row(s)",
                update_types.len(),
                payload_texts.len()
            ));
        }
        for (row_index, (update_type, payload_text)) in
            update_types.into_iter().zip(payload_texts).enumerate()
        {
            let (timestamp_ns, priority) =
                payload_sort_key(&update_type, &payload_text).map_err(|err| {
                    format!("PMXT payload sort key failed in column {column_index}, row {row_index}: {err}")
                })?;
            sorted_payloads.push(PmxtSortedPayload {
                timestamp_ns,
                priority,
                update_type,
                payload_text,
            });
        }
    }
    sorted_payloads.sort_by(|left, right| {
        (left.timestamp_ns, left.priority).cmp(&(right.timestamp_ns, right.priority))
    });
    Ok(sorted_payloads)
}

#[allow(clippy::too_many_arguments)]
pub fn payload_delta_rows(
    update_type_columns: Vec<Vec<String>>,
    payload_text_columns: Vec<Vec<String>>,
    token_id: &str,
    start_ns: i128,
    end_ns: i128,
    initial_has_snapshot: bool,
    last_timestamp_ns: Option<i128>,
    last_priority: Option<u8>,
) -> Result<PmxtDeltaRows, String> {
    let sorted_payloads = sort_payload_columns(update_type_columns, payload_text_columns)?;
    let mut rows = PmxtDeltaRows {
        has_snapshot: initial_has_snapshot,
        last_timestamp_ns,
        last_priority,
        ..PmxtDeltaRows::default()
    };
    let mut output_event_index: i32 = 0;

    for payload in sorted_payloads {
        let payload_key = (payload.timestamp_ns, payload.priority);
        if let (Some(last_timestamp_ns), Some(last_priority)) =
            (rows.last_timestamp_ns, rows.last_priority)
            && payload_key < (last_timestamp_ns, last_priority)
        {
            continue;
        }

        match payload.update_type.as_str() {
            "book_snapshot" => {
                process_book_snapshot_payload(
                    &mut rows,
                    &mut output_event_index,
                    &payload.payload_text,
                    token_id,
                    payload.timestamp_ns,
                    start_ns,
                    end_ns,
                )?;
                rows.last_timestamp_ns = Some(payload.timestamp_ns);
                rows.last_priority = Some(payload.priority);
            }
            "price_change" => {
                process_price_change_payload(
                    &mut rows,
                    &mut output_event_index,
                    &payload.payload_text,
                    token_id,
                    payload.timestamp_ns,
                    start_ns,
                    end_ns,
                )?;
                rows.last_timestamp_ns = Some(payload.timestamp_ns);
                rows.last_priority = Some(payload.priority);
            }
            _ => {}
        }
    }

    Ok(rows)
}

fn process_book_snapshot_payload(
    rows: &mut PmxtDeltaRows,
    output_event_index: &mut i32,
    payload_text: &str,
    token_id: &str,
    ts_event: i128,
    start_ns: i128,
    end_ns: i128,
) -> Result<(), String> {
    let fields = extract_payload_fields(payload_text)?;
    if fields.token_id != token_id {
        return Ok(());
    }

    let bids = json_string_pair_array_field(payload_text, "bids")?;
    let asks = json_string_pair_array_field(payload_text, "asks")?;
    if bids.is_empty() && asks.is_empty() {
        return Ok(());
    }
    rows.has_snapshot = true;
    if ts_event < start_ns || ts_event > end_ns {
        return Ok(());
    }

    let event_index = *output_event_index;
    push_delta_row(
        rows,
        event_index,
        BOOK_ACTION_CLEAR,
        ORDER_SIDE_NO_ORDER_SIDE,
        0.0,
        0.0,
        0,
        0,
        ts_event,
    );

    let bids_len = bids.len();
    let asks_len = asks.len();
    for (idx, (price_text, size_text)) in bids.into_iter().enumerate() {
        let flags = if idx + 1 == bids_len && asks_len == 0 {
            RECORD_FLAG_LAST
        } else {
            0
        };
        push_delta_row(
            rows,
            event_index,
            BOOK_ACTION_ADD,
            ORDER_SIDE_BUY,
            parse_finite_f64(price_text, "price")?,
            parse_finite_f64(size_text, "size")?,
            flags,
            0,
            ts_event,
        );
    }
    for (idx, (price_text, size_text)) in asks.into_iter().enumerate() {
        let flags = if idx + 1 == asks_len {
            RECORD_FLAG_LAST
        } else {
            0
        };
        push_delta_row(
            rows,
            event_index,
            BOOK_ACTION_ADD,
            ORDER_SIDE_SELL,
            parse_finite_f64(price_text, "price")?,
            parse_finite_f64(size_text, "size")?,
            flags,
            0,
            ts_event,
        );
    }
    *output_event_index += 1;
    Ok(())
}

fn process_price_change_payload(
    rows: &mut PmxtDeltaRows,
    output_event_index: &mut i32,
    payload_text: &str,
    token_id: &str,
    ts_event: i128,
    start_ns: i128,
    end_ns: i128,
) -> Result<(), String> {
    let fields = extract_payload_fields(payload_text)?;
    if fields.token_id != token_id || !rows.has_snapshot {
        return Ok(());
    }
    let Some(price_change) = fields.price_change else {
        return Ok(());
    };
    if ts_event < start_ns || ts_event > end_ns {
        return Ok(());
    }

    let side = match price_change.side {
        "BUY" => ORDER_SIDE_BUY,
        "SELL" => ORDER_SIDE_SELL,
        value => return Err(format!("invalid PMXT price_change side: {value:?}")),
    };
    let size = parse_finite_f64(price_change.size, "size")?;
    push_delta_row(
        rows,
        *output_event_index,
        if size > 0.0 {
            BOOK_ACTION_UPDATE
        } else {
            BOOK_ACTION_DELETE
        },
        side,
        parse_finite_f64(price_change.price, "price")?,
        size,
        RECORD_FLAG_LAST,
        0,
        ts_event,
    );
    *output_event_index += 1;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_delta_row(
    rows: &mut PmxtDeltaRows,
    event_index: i32,
    action: u8,
    side: u8,
    price: f64,
    size: f64,
    flags: u8,
    sequence: i32,
    ts_event: i128,
) {
    rows.event_index.push(event_index);
    rows.action.push(action);
    rows.side.push(side);
    rows.price.push(price);
    rows.size.push(size);
    rows.flags.push(flags);
    rows.sequence.push(sequence);
    rows.ts_event.push(ts_event);
    rows.ts_init.push(ts_event);
}

fn json_string_pair_array_field<'a>(
    payload_text: &'a str,
    field_name: &str,
) -> Result<Vec<(&'a str, &'a str)>, String> {
    let bytes = payload_text.as_bytes();
    let mut cursor = skip_json_whitespace(
        payload_text,
        json_field_value_start(payload_text, field_name)?,
    );
    if bytes.get(cursor) != Some(&b'[') {
        return Err(format!("JSON field {field_name:?} is not an array"));
    }
    cursor += 1;
    let mut pairs = Vec::new();
    loop {
        cursor = skip_json_whitespace(payload_text, cursor);
        match bytes.get(cursor) {
            Some(b']') => return Ok(pairs),
            Some(b'[') => cursor += 1,
            Some(_) => return Err(format!("JSON field {field_name:?} expected pair array")),
            None => return Err(format!("unterminated JSON array field {field_name:?}")),
        }

        cursor = skip_json_whitespace(payload_text, cursor);
        let first = json_string_literal_contents(payload_text, cursor, field_name)?;
        cursor = skip_json_whitespace(payload_text, json_string_end(payload_text, cursor)?);
        if bytes.get(cursor) != Some(&b',') {
            return Err(format!("JSON field {field_name:?} pair is missing a comma"));
        }
        cursor = skip_json_whitespace(payload_text, cursor + 1);
        let second = json_string_literal_contents(payload_text, cursor, field_name)?;
        cursor = skip_json_whitespace(payload_text, json_string_end(payload_text, cursor)?);
        if bytes.get(cursor) != Some(&b']') {
            return Err(format!("JSON field {field_name:?} pair is not closed"));
        }
        cursor += 1;
        pairs.push((first, second));

        cursor = skip_json_whitespace(payload_text, cursor);
        match bytes.get(cursor) {
            Some(b',') => cursor += 1,
            Some(b']') => return Ok(pairs),
            Some(_) => return Err(format!("JSON field {field_name:?} expected ',' or ']'")),
            None => return Err(format!("unterminated JSON array field {field_name:?}")),
        }
    }
}

fn parse_finite_f64(value: &str, field: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("invalid PMXT book level {field}: {value:?}"))?;
    if !parsed.is_finite() {
        return Err(format!("invalid PMXT book level {field}: {value:?}"));
    }
    Ok(parsed)
}

fn json_number_field_literal<'a>(
    payload_text: &'a str,
    field_name: &str,
) -> Result<&'a str, String> {
    let mut cursor = json_field_value_start(payload_text, field_name)?;
    let start = cursor;
    while let Some(byte) = payload_text.as_bytes().get(cursor) {
        if matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
            cursor += 1;
            continue;
        }
        break;
    }
    if cursor == start {
        return Err(format!("JSON field {field_name:?} is not a number"));
    }
    Ok(&payload_text[start..cursor])
}

fn json_string_field_literal<'a>(
    payload_text: &'a str,
    field_name: &str,
) -> Result<&'a str, String> {
    let cursor = json_field_value_start(payload_text, field_name)?;
    json_string_literal_contents(payload_text, cursor, field_name)
}

fn json_field_value_start(payload_text: &str, field_name: &str) -> Result<usize, String> {
    let bytes = payload_text.as_bytes();
    let mut cursor = skip_json_whitespace(payload_text, 0);
    if bytes.get(cursor) != Some(&b'{') {
        return Err("PMXT payload is not a JSON object".to_string());
    }
    cursor += 1;

    loop {
        cursor = skip_json_whitespace(payload_text, cursor);
        match bytes.get(cursor) {
            Some(b'}') => return Err(format!("missing JSON field {field_name:?}")),
            Some(b'"') => {}
            Some(_) => return Err("expected JSON object field name".to_string()),
            None => return Err("unterminated JSON object".to_string()),
        }

        let key_start = cursor + 1;
        let key_end = json_string_end(payload_text, cursor)?;
        let key = &payload_text[key_start..key_end - 1];
        cursor = skip_json_whitespace(payload_text, key_end);
        if bytes.get(cursor) != Some(&b':') {
            return Err(format!("JSON field {key:?} is missing a ':' separator"));
        }
        cursor = skip_json_whitespace(payload_text, cursor + 1);
        if key == field_name {
            return Ok(cursor);
        }

        cursor = skip_json_value(payload_text, cursor)?;
        cursor = skip_json_whitespace(payload_text, cursor);
        match bytes.get(cursor) {
            Some(b',') => cursor += 1,
            Some(b'}') => return Err(format!("missing JSON field {field_name:?}")),
            Some(_) => return Err("expected ',' or '}' after JSON field value".to_string()),
            None => return Err("unterminated JSON object".to_string()),
        }
    }
}

fn json_string_literal_contents<'a>(
    payload_text: &'a str,
    cursor: usize,
    field_name: &str,
) -> Result<&'a str, String> {
    if payload_text.as_bytes().get(cursor) != Some(&b'"') {
        return Err(format!("JSON field {field_name:?} is not a string"));
    }
    let start = cursor + 1;
    let end = json_string_end(payload_text, cursor)?;
    let value = &payload_text[start..end - 1];
    if value.as_bytes().contains(&b'\\') {
        return Err(format!(
            "JSON field {field_name:?} contains escaped characters, which are not supported"
        ));
    }
    Ok(value)
}

fn json_string_end(payload_text: &str, cursor: usize) -> Result<usize, String> {
    let bytes = payload_text.as_bytes();
    if bytes.get(cursor) != Some(&b'"') {
        return Err("expected JSON string".to_string());
    }
    let mut cursor = cursor + 1;
    while let Some(byte) = bytes.get(cursor) {
        match byte {
            b'"' => return Ok(cursor + 1),
            b'\\' => {
                cursor += 2;
                continue;
            }
            0x00..=0x1f => return Err("JSON string contains a control character".to_string()),
            _ => cursor += 1,
        }
    }
    Err("unterminated JSON string".to_string())
}

fn skip_json_value(payload_text: &str, mut cursor: usize) -> Result<usize, String> {
    cursor = skip_json_whitespace(payload_text, cursor);
    let bytes = payload_text.as_bytes();
    match bytes.get(cursor) {
        Some(b'"') => json_string_end(payload_text, cursor),
        Some(b'{') | Some(b'[') => skip_json_compound(payload_text, cursor),
        Some(_) => {
            let start = cursor;
            while let Some(byte) = bytes.get(cursor) {
                if matches!(byte, b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t') {
                    break;
                }
                cursor += 1;
            }
            if cursor == start {
                return Err("expected JSON value".to_string());
            }
            Ok(cursor)
        }
        None => Err("expected JSON value".to_string()),
    }
}

fn skip_json_compound(payload_text: &str, cursor: usize) -> Result<usize, String> {
    let bytes = payload_text.as_bytes();
    let first_close = match bytes.get(cursor) {
        Some(b'{') => b'}',
        Some(b'[') => b']',
        _ => return Err("expected JSON object or array".to_string()),
    };
    let mut close_stack = vec![first_close];
    let mut cursor = cursor + 1;
    while let Some(byte) = bytes.get(cursor) {
        match byte {
            b'"' => cursor = json_string_end(payload_text, cursor)?,
            b'{' => {
                close_stack.push(b'}');
                cursor += 1;
            }
            b'[' => {
                close_stack.push(b']');
                cursor += 1;
            }
            b'}' | b']' => {
                if close_stack.pop() != Some(*byte) {
                    return Err("mismatched JSON object or array delimiter".to_string());
                }
                cursor += 1;
                if close_stack.is_empty() {
                    return Ok(cursor);
                }
            }
            _ => cursor += 1,
        }
    }
    Err("unterminated JSON object or array".to_string())
}

fn skip_json_whitespace(payload_text: &str, mut cursor: usize) -> usize {
    while matches!(
        payload_text.as_bytes().get(cursor),
        Some(b' ' | b'\n' | b'\r' | b'\t')
    ) {
        cursor += 1;
    }
    cursor
}

#[cfg(test)]
mod tests {
    use super::{
        PmxtPayloadFields, PmxtPriceChangeFields, PmxtSortedPayload, PmxtUpdateClass,
        extract_payload_fields, payload_delta_rows, payload_sort_key, sort_payload_columns,
        sort_payloads,
    };

    #[test]
    fn extracts_book_snapshot_sort_key() {
        assert_eq!(
            payload_sort_key(
                "book_snapshot",
                r#"{"update_type":"book_snapshot","timestamp":1771767624.001295}"#
            )
            .unwrap(),
            (1_771_767_624_001_295_000, 0)
        );
    }

    #[test]
    fn extracts_price_change_sort_key() {
        assert_eq!(
            payload_sort_key(
                "price_change",
                r#"{"update_type":"price_change","timestamp":1771767624.001296}"#
            )
            .unwrap(),
            (1_771_767_624_001_296_000, 1)
        );
    }

    #[test]
    fn unknown_update_type_matches_python_fallback_priority() {
        assert_eq!(payload_sort_key("unknown", "{}").unwrap(), (0, 2));
    }

    #[test]
    fn missing_timestamp_is_an_error_for_known_payloads() {
        assert!(payload_sort_key("book_snapshot", "{}").is_err());
    }

    #[test]
    fn sorts_payload_block_by_timestamp_and_update_priority() {
        assert_eq!(
            sort_payloads([
                (
                    "price_change".to_string(),
                    r#"{"update_type":"price_change","timestamp":2.0}"#.to_string(),
                ),
                (
                    "book_snapshot".to_string(),
                    r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string(),
                ),
                (
                    "price_change".to_string(),
                    r#"{"update_type":"price_change","timestamp":1.0}"#.to_string(),
                ),
            ])
            .unwrap(),
            vec![
                PmxtSortedPayload {
                    timestamp_ns: 1_000_000_000,
                    priority: 0,
                    update_type: "book_snapshot".to_string(),
                    payload_text: r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string(),
                },
                PmxtSortedPayload {
                    timestamp_ns: 1_000_000_000,
                    priority: 1,
                    update_type: "price_change".to_string(),
                    payload_text: r#"{"update_type":"price_change","timestamp":1.0}"#.to_string(),
                },
                PmxtSortedPayload {
                    timestamp_ns: 2_000_000_000,
                    priority: 1,
                    update_type: "price_change".to_string(),
                    payload_text: r#"{"update_type":"price_change","timestamp":2.0}"#.to_string(),
                },
            ]
        );
    }

    #[test]
    fn sorts_payload_columns_by_timestamp_and_update_priority() {
        assert_eq!(
            sort_payload_columns(
                vec![
                    vec!["price_change".to_string(), "book_snapshot".to_string()],
                    vec!["price_change".to_string()],
                ],
                vec![
                    vec![
                        r#"{"update_type":"price_change","timestamp":2.0}"#.to_string(),
                        r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string(),
                    ],
                    vec![r#"{"update_type":"price_change","timestamp":1.0}"#.to_string()],
                ],
            )
            .unwrap(),
            vec![
                PmxtSortedPayload {
                    timestamp_ns: 1_000_000_000,
                    priority: 0,
                    update_type: "book_snapshot".to_string(),
                    payload_text: r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string(),
                },
                PmxtSortedPayload {
                    timestamp_ns: 1_000_000_000,
                    priority: 1,
                    update_type: "price_change".to_string(),
                    payload_text: r#"{"update_type":"price_change","timestamp":1.0}"#.to_string(),
                },
                PmxtSortedPayload {
                    timestamp_ns: 2_000_000_000,
                    priority: 1,
                    update_type: "price_change".to_string(),
                    payload_text: r#"{"update_type":"price_change","timestamp":2.0}"#.to_string(),
                },
            ]
        );
    }

    #[test]
    fn rejects_mismatched_payload_columns() {
        assert!(
            sort_payload_columns(
                vec![vec!["book_snapshot".to_string()]],
                vec![
                    vec![r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string()],
                    vec![r#"{"update_type":"price_change","timestamp":2.0}"#.to_string()],
                ],
            )
            .is_err()
        );
        assert!(
            sort_payload_columns(
                vec![vec![
                    "book_snapshot".to_string(),
                    "price_change".to_string()
                ]],
                vec![vec![
                    r#"{"update_type":"book_snapshot","timestamp":1.0}"#.to_string()
                ]],
            )
            .is_err()
        );
    }

    #[test]
    fn extracts_book_snapshot_payload_fields() {
        assert_eq!(
            extract_payload_fields(
                r#"{
                    "update_type":"book_snapshot",
                    "market_id":"condition-123",
                    "token_id":"token-yes-123",
                    "timestamp":1771767624.001295,
                    "bids":[["0.48","10"]],
                    "asks":[["0.52","11"]]
                }"#
            )
            .unwrap(),
            PmxtPayloadFields {
                update_type: "book_snapshot",
                update_class: PmxtUpdateClass::BookSnapshot,
                timestamp_ns: 1_771_767_624_001_295_000,
                market_id: "condition-123",
                token_id: "token-yes-123",
                price_change: None,
            }
        );
    }

    #[test]
    fn extracts_price_change_payload_fields() {
        assert_eq!(
            extract_payload_fields(
                r#"{
                    "best_ask":"0.51",
                    "change_size":"42.5",
                    "change_price":"0.49",
                    "token_id":"token-no-999",
                    "timestamp":1771767624.001296,
                    "change_side":"BUY",
                    "market_id":"condition-123",
                    "update_type":"price_change",
                    "best_bid":"0.49"
                }"#
            )
            .unwrap(),
            PmxtPayloadFields {
                update_type: "price_change",
                update_class: PmxtUpdateClass::PriceChange,
                timestamp_ns: 1_771_767_624_001_296_000,
                market_id: "condition-123",
                token_id: "token-no-999",
                price_change: Some(PmxtPriceChangeFields {
                    side: "BUY",
                    price: "0.49",
                    size: "42.5",
                }),
            }
        );
    }

    #[test]
    fn rejects_price_change_payload_without_change_fields() {
        assert!(
            extract_payload_fields(
                r#"{
                    "update_type":"price_change",
                    "market_id":"condition-123",
                    "token_id":"token-yes-123",
                    "timestamp":1771767624.001296
                }"#
            )
            .is_err()
        );
    }

    #[test]
    fn top_level_field_parser_ignores_nested_matching_names() {
        let fields = extract_payload_fields(
            r#"{
                "update_type":"book_snapshot",
                "market_id":"condition-123",
                "token_id":"outer-token",
                "timestamp":1771767624.001295,
                "bids":[{"token_id":"nested-token","market_id":"nested-market"}],
                "asks":[]
            }"#,
        )
        .unwrap();

        assert_eq!(fields.market_id, "condition-123");
        assert_eq!(fields.token_id, "outer-token");
    }

    #[test]
    fn builds_delta_rows_for_snapshots_and_price_changes() {
        let snapshot = r#"{
            "update_type":"book_snapshot",
            "market_id":"condition-123",
            "token_id":"target-token",
            "timestamp":100.0,
            "bids":[["0.40","5"],["0.39","6"]],
            "asks":[["0.60","7"]]
        }"#;
        let price_change = r#"{
            "update_type":"price_change",
            "market_id":"condition-123",
            "token_id":"target-token",
            "timestamp":101.0,
            "change_side":"SELL",
            "change_price":"0.61",
            "change_size":"0"
        }"#;

        let rows = payload_delta_rows(
            vec![vec![
                "price_change".to_string(),
                "book_snapshot".to_string(),
            ]],
            vec![vec![price_change.to_string(), snapshot.to_string()]],
            "target-token",
            100_000_000_000,
            101_000_000_000,
            false,
            None,
            None,
        )
        .unwrap();

        assert!(rows.has_snapshot);
        assert_eq!(rows.last_timestamp_ns, Some(101_000_000_000));
        assert_eq!(rows.last_priority, Some(1));
        assert_eq!(rows.event_index, vec![0, 0, 0, 0, 1]);
        assert_eq!(rows.action, vec![4, 1, 1, 1, 3]);
        assert_eq!(rows.side, vec![0, 1, 1, 2, 2]);
        assert_eq!(rows.price, vec![0.0, 0.40, 0.39, 0.60, 0.61]);
        assert_eq!(rows.size, vec![0.0, 5.0, 6.0, 7.0, 0.0]);
        assert_eq!(rows.flags, vec![0, 0, 0, 128, 128]);
        assert_eq!(rows.sequence, vec![0, 0, 0, 0, 0]);
        assert_eq!(
            rows.ts_event,
            vec![
                100_000_000_000,
                100_000_000_000,
                100_000_000_000,
                100_000_000_000,
                101_000_000_000,
            ]
        );
        assert_eq!(rows.ts_init, rows.ts_event);
    }

    #[test]
    fn payload_delta_rows_keeps_snapshot_state_without_emitting_before_window() {
        let snapshot = r#"{
            "update_type":"book_snapshot",
            "market_id":"condition-123",
            "token_id":"target-token",
            "timestamp":99.0,
            "bids":[["0.40","5"]],
            "asks":[]
        }"#;
        let price_change = r#"{
            "update_type":"price_change",
            "market_id":"condition-123",
            "token_id":"target-token",
            "timestamp":100.0,
            "change_side":"BUY",
            "change_price":"0.41",
            "change_size":"8"
        }"#;

        let rows = payload_delta_rows(
            vec![vec![
                "book_snapshot".to_string(),
                "price_change".to_string(),
            ]],
            vec![vec![snapshot.to_string(), price_change.to_string()]],
            "target-token",
            100_000_000_000,
            101_000_000_000,
            false,
            None,
            None,
        )
        .unwrap();

        assert!(rows.has_snapshot);
        assert_eq!(rows.event_index, vec![0]);
        assert_eq!(rows.action, vec![2]);
        assert_eq!(rows.side, vec![1]);
        assert_eq!(rows.price, vec![0.41]);
        assert_eq!(rows.size, vec![8.0]);
    }
}
