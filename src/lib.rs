pub mod cycle;
pub mod math;
pub mod momentum;
pub mod overlap;
pub mod pattern;
pub mod statistic;
pub mod transform;
pub mod utils;
pub mod volatility;
pub mod volume;
pub use talib::common::{ta_initialize, ta_shutdown, ta_version};
// use talib_sys::{TA_Initialize, TA_Shutdown, TA_RetCode};

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;
