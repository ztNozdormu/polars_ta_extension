use talib::common::{TimePeriodKwargs, ta_initialize, ta_shutdown};
use talib::overlap::ta_ema;

fn main() {
    let _ = ta_initialize();
     // Series: 'close' [f64]
        let close_prices: Vec<f64> = vec![
                                    56523.0,
                                    56461.1,
                                    56425.8,
                                    56445.99,
                                    56404.0,
                                    56367.22,
                                    56405.49,
                                    56519.01,
                                    56519.96,
                                    56556.01,
                                    56655.76,
                                    56594.66,
                                    56610.0,
                                    56642.0,
                                    56661.97,
                                    56633.5,
                                    56696.01,
                                    56695.49
                                ];

    let kwargs = TimePeriodKwargs { timeperiod: 5 };
    let res = ta_ema(close_prices.as_ptr(), close_prices.len(), &kwargs);

    match res {
        Ok(rsi_values) => {
            for (index, value) in rsi_values.iter().enumerate() {
                println!("Close index {} = {}", index, value);
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
        }
    }
   
    let _ = ta_shutdown();
}
