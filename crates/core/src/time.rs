const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NAUTILUS_FIXED_SCALAR: f64 = 10_000_000_000_000_000.0;

pub fn decimal_seconds_to_ns(value: &str) -> Result<i128, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("timestamp cannot be empty".to_string());
    }

    let (negative, unsigned) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if unsigned.is_empty() {
        return Err(format!("invalid timestamp {value:?}"));
    }
    if unsigned.contains('e') || unsigned.contains('E') {
        return Err(format!(
            "scientific notation timestamp is not supported: {value:?}"
        ));
    }

    let (seconds_part, fraction_part) = match unsigned.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (unsigned, ""),
    };
    if seconds_part.is_empty() && fraction_part.is_empty() {
        return Err(format!("invalid timestamp {value:?}"));
    }
    if !seconds_part.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid timestamp seconds {value:?}"));
    }
    if !fraction_part.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid timestamp fraction {value:?}"));
    }

    let seconds = parse_decimal_digits(seconds_part)?;
    let mut nanos = seconds
        .checked_mul(NANOS_PER_SECOND)
        .ok_or_else(|| format!("timestamp overflows nanoseconds: {value:?}"))?;

    let mut fraction_nanos = 0_i128;
    let mut consumed_digits = 0_usize;
    for byte in fraction_part.bytes().take(9) {
        fraction_nanos = fraction_nanos
            .checked_mul(10)
            .and_then(|part| part.checked_add(i128::from(byte - b'0')))
            .ok_or_else(|| format!("timestamp fraction overflows nanoseconds: {value:?}"))?;
        consumed_digits += 1;
    }
    for _ in consumed_digits..9 {
        fraction_nanos *= 10;
    }

    if should_round_fraction_up(fraction_part, consumed_digits, fraction_nanos) {
        fraction_nanos += 1;
    }
    nanos = nanos
        .checked_add(fraction_nanos)
        .ok_or_else(|| format!("timestamp overflows nanoseconds: {value:?}"))?;

    if negative { Ok(-nanos) } else { Ok(nanos) }
}

pub fn float_seconds_to_ms_string(value: f64) -> String {
    format!("{:.6}", value * 1000.0)
}

pub fn fixed_raw_values(values: &[f64], precision: u8) -> Result<Vec<i128>, String> {
    let precision_factor = 10_f64.powi(i32::from(precision));
    values
        .iter()
        .map(|value| fixed_raw_value(*value, precision_factor))
        .collect()
}

fn fixed_raw_value(value: f64, precision_factor: f64) -> Result<i128, String> {
    if !value.is_finite() {
        return Err(format!("fixed-point value must be finite: {value:?}"));
    }
    let rounded = (value * precision_factor).round_ties_even() / precision_factor;
    let raw = (rounded * NAUTILUS_FIXED_SCALAR).round_ties_even();
    if !raw.is_finite() {
        return Err(format!("fixed-point raw value must be finite: {value:?}"));
    }
    Ok(raw as i128)
}

fn parse_decimal_digits(value: &str) -> Result<i128, String> {
    let mut parsed = 0_i128;
    for byte in value.bytes() {
        parsed = parsed
            .checked_mul(10)
            .and_then(|part| part.checked_add(i128::from(byte - b'0')))
            .ok_or_else(|| format!("decimal integer overflows i128: {value:?}"))?;
    }
    Ok(parsed)
}

fn should_round_fraction_up(
    fraction_part: &str,
    consumed_digits: usize,
    fraction_nanos: i128,
) -> bool {
    let mut extra_digits = fraction_part.bytes().skip(consumed_digits);
    let Some(first_extra_digit) = extra_digits.next() else {
        return false;
    };
    if first_extra_digit > b'5' {
        return true;
    }
    if first_extra_digit < b'5' {
        return false;
    }
    if extra_digits.any(|byte| byte != b'0') {
        return true;
    }
    fraction_nanos % 2 != 0
}

#[cfg(test)]
mod tests {
    use super::{decimal_seconds_to_ns, fixed_raw_values, float_seconds_to_ms_string};

    #[test]
    fn converts_seconds_decimal_to_nanoseconds() {
        assert_eq!(
            decimal_seconds_to_ns("1771767624.001295").unwrap(),
            1_771_767_624_001_295_000
        );
    }

    #[test]
    fn pads_short_fraction_to_nanoseconds() {
        assert_eq!(decimal_seconds_to_ns("1.5").unwrap(), 1_500_000_000);
    }

    #[test]
    fn rounds_long_fraction_half_even() {
        assert_eq!(
            decimal_seconds_to_ns("1.0000000005").unwrap(),
            1_000_000_000
        );
        assert_eq!(
            decimal_seconds_to_ns("1.0000000015").unwrap(),
            1_000_000_002
        );
    }

    #[test]
    fn rejects_invalid_timestamp() {
        assert!(decimal_seconds_to_ns("1771767624.abc").is_err());
    }

    #[test]
    fn formats_float_seconds_as_existing_millisecond_string() {
        assert_eq!(
            float_seconds_to_ms_string(1_771_767_624.001_295),
            "1771767624001.295166"
        );
    }

    #[test]
    fn converts_fixed_raw_values_with_precision_rounding() {
        assert_eq!(
            fixed_raw_values(&[0.105, 1009.1234564], 2).unwrap(),
            vec![1_000_000_000_000_000_i128, 10_091_200_000_000_000_000_i128]
        );
    }
}
