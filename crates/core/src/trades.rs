use std::collections::HashMap;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const AGGRESSOR_NO_AGGRESSOR: u8 = 0;
const AGGRESSOR_BUYER: u8 = 1;
const AGGRESSOR_SELLER: u8 = 2;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PolymarketTradeSortKey {
    pub timestamp: i64,
    pub transaction_hash: String,
    pub asset: String,
    pub side: String,
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PolymarketTradeSide {
    Buy,
    Sell,
    Unknown,
}

impl PolymarketTradeSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct PolymarketTradeSequenceKey {
    pub transaction_hash: String,
    pub asset: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketPublicTradeInput {
    pub original_index: usize,
    pub timestamp: i64,
    pub transaction_hash: String,
    pub asset: String,
    pub side: String,
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolymarketPublicTradeRows {
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub aggressor_side: Vec<u8>,
    pub trade_id: Vec<String>,
    pub ts_event: Vec<i64>,
    pub ts_init: Vec<i64>,
    pub unexpected_side_records: Vec<(usize, String)>,
    pub skipped_price_records: Vec<(usize, f64)>,
}

pub fn polymarket_trade_sort_key(
    timestamp: i64,
    transaction_hash: &str,
    asset: &str,
    side: &str,
    price: &str,
    size: &str,
) -> PolymarketTradeSortKey {
    PolymarketTradeSortKey {
        timestamp,
        transaction_hash: transaction_hash.to_string(),
        asset: asset.to_string(),
        side: side.to_string(),
        price: price.to_string(),
        size: size.to_string(),
    }
}

pub fn polymarket_normalize_trade_side(side: &str) -> PolymarketTradeSide {
    let side = side.trim();
    if side.eq_ignore_ascii_case("BUY") {
        PolymarketTradeSide::Buy
    } else if side.eq_ignore_ascii_case("SELL") {
        PolymarketTradeSide::Sell
    } else {
        PolymarketTradeSide::Unknown
    }
}

pub fn polymarket_parse_probability_price(price: &str) -> Result<f64, String> {
    price
        .trim()
        .parse::<f64>()
        .map_err(|error| format!("invalid Polymarket probability price {price:?}: {error}"))
}

pub fn polymarket_is_tradable_probability_price(price: &str) -> bool {
    match polymarket_parse_probability_price(price) {
        Ok(value) => 0.0 < value && value < 1.0,
        Err(_) => false,
    }
}

pub fn polymarket_trade_timestamp_tiebreaker_ns(occurrence_in_second: usize) -> i64 {
    let occurrence = i64::try_from(occurrence_in_second).unwrap_or(i64::MAX);
    occurrence.min(999_999_999)
}

pub fn polymarket_trade_event_timestamp_ns(
    base_timestamp_ns: i64,
    occurrence_in_second: usize,
) -> Result<i64, String> {
    let tiebreaker_ns = polymarket_trade_timestamp_tiebreaker_ns(occurrence_in_second);
    base_timestamp_ns.checked_add(tiebreaker_ns).ok_or_else(|| {
        format!(
            "Polymarket trade timestamp overflows i64 nanoseconds: {base_timestamp_ns} + {tiebreaker_ns}"
        )
    })
}

pub fn polymarket_trade_sequence_key(
    transaction_hash: &str,
    asset: &str,
) -> PolymarketTradeSequenceKey {
    PolymarketTradeSequenceKey {
        transaction_hash: transaction_hash.to_string(),
        asset: asset.to_string(),
    }
}

pub fn polymarket_trade_id(transaction_hash: &str, asset: &str, sequence: usize) -> String {
    let hash_suffix = suffix_chars(transaction_hash, 24);
    let asset_suffix = suffix_chars(asset, 4);
    format!("{hash_suffix}-{asset_suffix}-{sequence:06}")
}

pub fn polymarket_public_trade_rows(
    trades: &[PolymarketPublicTradeInput],
    token_id: &str,
    sort: bool,
) -> Result<PolymarketPublicTradeRows, String> {
    let mut candidate_trades: Vec<&PolymarketPublicTradeInput> = trades
        .iter()
        .filter(|trade| trade.asset == token_id)
        .collect();
    if sort {
        candidate_trades.sort_by(|left, right| {
            (
                left.timestamp,
                left.transaction_hash.as_str(),
                left.asset.as_str(),
                left.side.as_str(),
                left.price.as_str(),
                left.size.as_str(),
            )
                .cmp(&(
                    right.timestamp,
                    right.transaction_hash.as_str(),
                    right.asset.as_str(),
                    right.side.as_str(),
                    right.price.as_str(),
                    right.size.as_str(),
                ))
        });
    }

    let mut rows = PolymarketPublicTradeRows::default();
    let mut timestamp_counts: HashMap<i64, usize> = HashMap::new();
    let mut tx_asset_counts: HashMap<(String, String), usize> = HashMap::new();

    for (candidate_index, trade) in candidate_trades.into_iter().enumerate() {
        let record_index = if sort {
            candidate_index
        } else {
            trade.original_index
        };
        let base_ts_event = trade
            .timestamp
            .checked_mul(NANOS_PER_SECOND)
            .ok_or_else(|| {
                format!(
                    "Polymarket trade timestamp overflows i64 nanoseconds: {} * {NANOS_PER_SECOND}",
                    trade.timestamp
                )
            })?;
        let occurrence_in_second = *timestamp_counts.get(&base_ts_event).unwrap_or(&0);
        timestamp_counts.insert(base_ts_event, occurrence_in_second + 1);

        let tx_asset_key = (trade.transaction_hash.clone(), trade.asset.clone());
        let tx_asset_sequence = *tx_asset_counts.get(&tx_asset_key).unwrap_or(&0);
        tx_asset_counts.insert(tx_asset_key, tx_asset_sequence + 1);

        let ts_event = polymarket_trade_event_timestamp_ns(base_ts_event, occurrence_in_second)?;
        let aggressor_side = match polymarket_normalize_trade_side(&trade.side) {
            PolymarketTradeSide::Buy => AGGRESSOR_BUYER,
            PolymarketTradeSide::Sell => AGGRESSOR_SELLER,
            PolymarketTradeSide::Unknown => {
                rows.unexpected_side_records
                    .push((record_index, trade.side.clone()));
                AGGRESSOR_NO_AGGRESSOR
            }
        };

        let price = polymarket_parse_probability_price(&trade.price)?;
        if !(0.0 < price && price < 1.0) {
            rows.skipped_price_records.push((record_index, price));
            continue;
        }
        let size = trade.size.trim().parse::<f64>().map_err(|error| {
            format!(
                "invalid Polymarket trade size {:?} at record {record_index}: {error}",
                trade.size
            )
        })?;
        if !size.is_finite() {
            return Err(format!(
                "invalid Polymarket trade size {:?} at record {record_index}",
                trade.size
            ));
        }

        rows.price.push(price);
        rows.size.push(size);
        rows.aggressor_side.push(aggressor_side);
        rows.trade_id.push(polymarket_trade_id(
            &trade.transaction_hash,
            &trade.asset,
            tx_asset_sequence,
        ));
        rows.ts_event.push(ts_event);
        rows.ts_init.push(ts_event);
    }

    Ok(rows)
}

fn suffix_chars(value: &str, count: usize) -> String {
    let mut chars = value.chars().rev().take(count).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::{
        PolymarketPublicTradeInput, PolymarketTradeSide, polymarket_is_tradable_probability_price,
        polymarket_normalize_trade_side, polymarket_parse_probability_price,
        polymarket_public_trade_rows, polymarket_trade_event_timestamp_ns, polymarket_trade_id,
        polymarket_trade_sequence_key, polymarket_trade_sort_key,
        polymarket_trade_timestamp_tiebreaker_ns,
    };

    #[test]
    fn builds_public_trade_sort_key() {
        let key =
            polymarket_trade_sort_key(1_771_767_624, "0xabcdef", "123456", "BUY", "0.42", "10");

        assert_eq!(key.timestamp, 1_771_767_624);
        assert_eq!(key.transaction_hash, "0xabcdef");
        assert_eq!(key.asset, "123456");
        assert_eq!(key.side, "BUY");
        assert_eq!(key.price, "0.42");
        assert_eq!(key.size, "10");
    }

    #[test]
    fn builds_collision_resistant_trade_id_suffix() {
        assert_eq!(
            polymarket_trade_id("0x1234567890abcdef1234567890abcdef", "asset9876", 42),
            "90abcdef1234567890abcdef-9876-000042"
        );
    }

    #[test]
    fn normalizes_public_trade_side() {
        assert_eq!(
            polymarket_normalize_trade_side("BUY"),
            PolymarketTradeSide::Buy
        );
        assert_eq!(
            polymarket_normalize_trade_side(" sell "),
            PolymarketTradeSide::Sell
        );
        assert_eq!(polymarket_normalize_trade_side("buy").as_str(), "BUY");
        assert_eq!(polymarket_normalize_trade_side("SELL").as_str(), "SELL");
    }

    #[test]
    fn treats_unexpected_public_trade_side_as_unknown() {
        assert_eq!(
            polymarket_normalize_trade_side(""),
            PolymarketTradeSide::Unknown
        );
        assert_eq!(
            polymarket_normalize_trade_side("MINT"),
            PolymarketTradeSide::Unknown
        );
        assert_eq!(polymarket_normalize_trade_side("buyer").as_str(), "unknown");
    }

    #[test]
    fn validates_tradable_probability_price_from_string() {
        assert!(polymarket_is_tradable_probability_price("0.42"));
        assert!(polymarket_is_tradable_probability_price(" .5 "));
        assert!(polymarket_is_tradable_probability_price("1e-3"));
        assert_eq!(polymarket_parse_probability_price("0.42").unwrap(), 0.42);
    }

    #[test]
    fn rejects_boundary_non_finite_and_invalid_probability_prices() {
        assert!(!polymarket_is_tradable_probability_price("0"));
        assert!(!polymarket_is_tradable_probability_price("0.0"));
        assert!(!polymarket_is_tradable_probability_price("1"));
        assert!(!polymarket_is_tradable_probability_price("1.0"));
        assert!(!polymarket_is_tradable_probability_price("-0.01"));
        assert!(!polymarket_is_tradable_probability_price("nan"));
        assert!(!polymarket_is_tradable_probability_price("inf"));
        assert!(!polymarket_is_tradable_probability_price("not-a-price"));
        assert!(polymarket_parse_probability_price("not-a-price").is_err());
    }

    #[test]
    fn calculates_public_trade_timestamp_tiebreaker() {
        assert_eq!(polymarket_trade_timestamp_tiebreaker_ns(0), 0);
        assert_eq!(polymarket_trade_timestamp_tiebreaker_ns(42), 42);
        assert_eq!(
            polymarket_trade_timestamp_tiebreaker_ns(999_999_999),
            999_999_999
        );
        assert_eq!(
            polymarket_trade_timestamp_tiebreaker_ns(1_000_000_000),
            999_999_999
        );
    }

    #[test]
    fn adds_public_trade_timestamp_tiebreaker_to_base_timestamp() {
        assert_eq!(
            polymarket_trade_event_timestamp_ns(1_771_767_624_000_000_000, 42).unwrap(),
            1_771_767_624_000_000_042
        );
        assert_eq!(
            polymarket_trade_event_timestamp_ns(1_771_767_624_000_000_000, 1_000_000_000).unwrap(),
            1_771_767_624_999_999_999
        );
        assert!(polymarket_trade_event_timestamp_ns(i64::MAX, 1).is_err());
    }

    #[test]
    fn builds_trade_sequence_key_from_transaction_hash_and_asset() {
        let key = polymarket_trade_sequence_key("0xabcdef", "asset9876");

        assert_eq!(key.transaction_hash, "0xabcdef");
        assert_eq!(key.asset, "asset9876");
    }

    #[test]
    fn builds_public_trade_rows_with_sorting_tiebreakers_and_warnings() {
        let rows = polymarket_public_trade_rows(
            &[
                PolymarketPublicTradeInput {
                    original_index: 0,
                    timestamp: 10,
                    transaction_hash: "0xbbbb".to_string(),
                    asset: "other-token".to_string(),
                    side: "BUY".to_string(),
                    price: "0.40".to_string(),
                    size: "1".to_string(),
                },
                PolymarketPublicTradeInput {
                    original_index: 1,
                    timestamp: 10,
                    transaction_hash: "0xcccc".to_string(),
                    asset: "token-yes".to_string(),
                    side: "MINT".to_string(),
                    price: "0.42".to_string(),
                    size: "2".to_string(),
                },
                PolymarketPublicTradeInput {
                    original_index: 2,
                    timestamp: 10,
                    transaction_hash: "0xaaaa".to_string(),
                    asset: "token-yes".to_string(),
                    side: "SELL".to_string(),
                    price: "1.0".to_string(),
                    size: "3".to_string(),
                },
                PolymarketPublicTradeInput {
                    original_index: 3,
                    timestamp: 10,
                    transaction_hash: "0xaaaa".to_string(),
                    asset: "token-yes".to_string(),
                    side: "BUY".to_string(),
                    price: "0.41".to_string(),
                    size: "4".to_string(),
                },
            ],
            "token-yes",
            true,
        )
        .unwrap();

        assert_eq!(rows.price, vec![0.41, 0.42]);
        assert_eq!(rows.size, vec![4.0, 2.0]);
        assert_eq!(rows.aggressor_side, vec![1, 0]);
        assert_eq!(rows.ts_event, vec![10_000_000_000, 10_000_000_002]);
        assert_eq!(rows.ts_init, rows.ts_event);
        assert_eq!(
            rows.trade_id,
            vec!["0xaaaa--yes-000000", "0xcccc--yes-000000"]
        );
        assert_eq!(rows.skipped_price_records, vec![(1, 1.0)]);
        assert_eq!(rows.unexpected_side_records, vec![(2, "MINT".to_string())]);
    }

    #[test]
    fn public_trade_rows_preserve_input_record_indexes_without_sorting() {
        let rows = polymarket_public_trade_rows(
            &[
                PolymarketPublicTradeInput {
                    original_index: 0,
                    timestamp: 10,
                    transaction_hash: "0xaaaa".to_string(),
                    asset: "other-token".to_string(),
                    side: "BUY".to_string(),
                    price: "0.40".to_string(),
                    size: "1".to_string(),
                },
                PolymarketPublicTradeInput {
                    original_index: 1,
                    timestamp: 10,
                    transaction_hash: "0xbbbb".to_string(),
                    asset: "token-yes".to_string(),
                    side: "MINT".to_string(),
                    price: "0.41".to_string(),
                    size: "2".to_string(),
                },
            ],
            "token-yes",
            false,
        )
        .unwrap();

        assert_eq!(rows.unexpected_side_records, vec![(1, "MINT".to_string())]);
        assert_eq!(rows.aggressor_side, vec![0]);
    }
}
