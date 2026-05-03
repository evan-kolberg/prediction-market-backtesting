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

fn suffix_chars(value: &str, count: usize) -> String {
    let mut chars = value.chars().rev().take(count).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::{
        PolymarketTradeSide, polymarket_is_tradable_probability_price,
        polymarket_normalize_trade_side, polymarket_parse_probability_price,
        polymarket_trade_event_timestamp_ns, polymarket_trade_id, polymarket_trade_sequence_key,
        polymarket_trade_sort_key, polymarket_trade_timestamp_tiebreaker_ns,
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
}
