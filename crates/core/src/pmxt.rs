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
        extract_payload_fields, payload_sort_key, sort_payload_columns, sort_payloads,
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
}
