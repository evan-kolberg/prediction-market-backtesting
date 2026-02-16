use pyo3::prelude::*;

mod broker;
mod engine;
mod models;
mod portfolio;

#[pymodule]
fn backtesting_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<engine::RustEngine>()?;
    Ok(())
}
