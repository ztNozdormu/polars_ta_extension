mod cycle;
mod math;
mod momentum;
pub mod overlap;
mod pattern;
mod statistic;
mod transform;
mod utils;
mod volatility;
mod volume;
use talib::common::{ta_initialize, ta_shutdown, ta_version};
// use talib_sys::{TA_Initialize, TA_Shutdown, TA_RetCode};

#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[global_allocator]
#[cfg(target_os = "linux")]
static ALLOC: Jemalloc = Jemalloc;
