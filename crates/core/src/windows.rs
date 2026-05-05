const NANOS_PER_DAY: i128 = 86_400_000_000_000;
const NANOS_PER_HOUR: i128 = 3_600_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowSemantics {
    HalfOpen,
    Inclusive,
}

pub fn source_days_for_window(
    start_ns: i128,
    end_ns: i128,
    semantics: WindowSemantics,
) -> Vec<String> {
    let effective_end_ns = match semantics {
        WindowSemantics::HalfOpen => {
            if end_ns <= start_ns {
                return Vec::new();
            }
            end_ns - 1
        }
        WindowSemantics::Inclusive => {
            if end_ns < start_ns {
                return Vec::new();
            }
            end_ns
        }
    };

    let first_day = start_ns.div_euclid(NANOS_PER_DAY);
    let last_day = effective_end_ns.div_euclid(NANOS_PER_DAY);

    (first_day..=last_day).map(format_utc_day).collect()
}

pub fn pmxt_archive_hours_for_window(start_ns: i128, end_ns: i128) -> Vec<i128> {
    if end_ns <= start_ns {
        return Vec::new();
    }

    let first_hour = start_ns.div_euclid(NANOS_PER_HOUR) - 1;
    let last_hour = end_ns.div_euclid(NANOS_PER_HOUR);

    (first_hour..=last_hour)
        .map(|hour| hour * NANOS_PER_HOUR)
        .collect()
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

#[cfg(test)]
mod tests {
    use super::{WindowSemantics, pmxt_archive_hours_for_window, source_days_for_window};

    const APR_21_2026_NS: i128 = 1_776_729_600_000_000_000;
    const APR_28_2026_NS: i128 = 1_777_334_400_000_000_000;
    const APR_27_2026_END_NS: i128 = 1_777_334_399_999_999_999;
    const NANOS_PER_HOUR: i128 = 3_600_000_000_000;
    const NANOS_PER_MINUTE: i128 = 60_000_000_000;

    #[test]
    fn half_open_week_excludes_boundary_day() {
        assert_eq!(
            source_days_for_window(APR_21_2026_NS, APR_28_2026_NS, WindowSemantics::HalfOpen),
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
    }

    #[test]
    fn inclusive_boundary_includes_boundary_day() {
        assert_eq!(
            source_days_for_window(APR_21_2026_NS, APR_28_2026_NS, WindowSemantics::Inclusive),
            vec![
                "2026-04-21",
                "2026-04-22",
                "2026-04-23",
                "2026-04-24",
                "2026-04-25",
                "2026-04-26",
                "2026-04-27",
                "2026-04-28",
            ]
        );
    }

    #[test]
    fn inclusive_end_of_day_week_matches_half_open_boundary() {
        assert_eq!(
            source_days_for_window(
                APR_21_2026_NS,
                APR_27_2026_END_NS,
                WindowSemantics::Inclusive
            ),
            source_days_for_window(APR_21_2026_NS, APR_28_2026_NS, WindowSemantics::HalfOpen)
        );
    }

    #[test]
    fn empty_half_open_window_has_no_source_days() {
        assert!(source_days_for_window(5, 5, WindowSemantics::HalfOpen).is_empty());
    }

    #[test]
    fn pmxt_archive_hours_include_prior_snapshot_hour_and_final_hour() {
        let start_ns = APR_21_2026_NS + 9 * NANOS_PER_HOUR + 15 * NANOS_PER_MINUTE;
        let end_ns = APR_21_2026_NS + 10 * NANOS_PER_HOUR + 10 * NANOS_PER_MINUTE;

        assert_eq!(
            pmxt_archive_hours_for_window(start_ns, end_ns),
            vec![
                APR_21_2026_NS + 8 * NANOS_PER_HOUR,
                APR_21_2026_NS + 9 * NANOS_PER_HOUR,
                APR_21_2026_NS + 10 * NANOS_PER_HOUR,
            ]
        );
    }

    #[test]
    fn pmxt_archive_hours_include_boundary_hour() {
        assert_eq!(
            pmxt_archive_hours_for_window(APR_21_2026_NS, APR_21_2026_NS + NANOS_PER_HOUR),
            vec![
                APR_21_2026_NS - NANOS_PER_HOUR,
                APR_21_2026_NS,
                APR_21_2026_NS + NANOS_PER_HOUR,
            ]
        );
    }

    #[test]
    fn pmxt_archive_hours_empty_for_empty_window() {
        assert!(pmxt_archive_hours_for_window(5, 5).is_empty());
    }
}
