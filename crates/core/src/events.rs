use std::fmt;

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const SECONDS_PER_DAY: i128 = 86_400;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseLogLevelError {
    value: String,
}

impl fmt::Display for ParseLogLevelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unsupported log level {:?}", self.value)
    }
}

impl std::error::Error for ParseLogLevelError {}

impl TryFrom<&str> for LogLevel {
    type Error = ParseLogLevelError;

    fn try_from(value: &str) -> Result<Self, ParseLogLevelError> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DEBUG" => Ok(Self::Debug),
            "INFO" => Ok(Self::Info),
            "WARNING" => Ok(Self::Warning),
            "ERROR" => Ok(Self::Error),
            _ => Err(ParseLogLevelError {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoaderEvent {
    pub level: LogLevel,
    pub message: String,
    pub origin: String,
    pub timestamp_ns: i128,
    pub stage: String,
    pub vendor: String,
    pub status: String,
}

impl LoaderEvent {
    pub fn new(
        level: LogLevel,
        message: impl Into<String>,
        origin: impl Into<String>,
        timestamp_ns: i128,
        stage: impl Into<String>,
        vendor: impl Into<String>,
        status: impl Into<String>,
    ) -> Self {
        Self {
            level,
            message: message.into(),
            origin: origin.into(),
            timestamp_ns,
            stage: stage.into(),
            vendor: vendor.into(),
            status: status.into(),
        }
    }

    pub fn render_console_line(&self) -> String {
        format!(
            "{} [{}] {}: {}",
            format_utc_timestamp_ns(self.timestamp_ns),
            self.level.as_str(),
            self.origin,
            self.message
        )
    }
}

pub fn format_utc_timestamp_ns(epoch_ns: i128) -> String {
    let seconds = epoch_ns.div_euclid(NANOS_PER_SECOND);
    let nanos = epoch_ns.rem_euclid(NANOS_PER_SECOND);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z")
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
    use super::{LoaderEvent, LogLevel, format_utc_timestamp_ns};

    #[test]
    fn timestamp_format_preserves_nanoseconds() {
        assert_eq!(format_utc_timestamp_ns(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            format_utc_timestamp_ns(1_774_092_445_353_784_800),
            "2026-03-21T11:27:25.353784800Z"
        );
    }

    #[test]
    fn log_level_parses_known_values() {
        assert_eq!(LogLevel::try_from("info").unwrap(), LogLevel::Info);
        assert_eq!(LogLevel::try_from("WARNING").unwrap(), LogLevel::Warning);
        assert!(LogLevel::try_from("notice").is_err());
    }

    #[test]
    fn loader_event_renders_console_line() {
        let event = LoaderEvent::new(
            LogLevel::Info,
            "loaded PMXT cache",
            "core::pmxt::load",
            1_774_092_445_353_784_800,
            "cache_read",
            "pmxt",
            "cache_hit",
        );

        assert_eq!(
            event.render_console_line(),
            "2026-03-21T11:27:25.353784800Z [INFO] core::pmxt::load: loaded PMXT cache"
        );
    }
}
