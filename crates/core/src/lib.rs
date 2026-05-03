pub mod events;
pub mod pmxt;
pub mod telonex;
pub mod time;
pub mod trades;
pub mod windows;

pub fn native_available() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::native_available;

    #[test]
    fn native_available_reports_true() {
        assert!(native_available());
    }
}
