use crate::utils::{get_series_f64_ptr, ta_code2err};
use polars::prelude::*;
use talib::volatility::{ta_natr, NATRKwargs, ta_atr, ATRKwargs, ta_trange};

fn atr(inputs: &[Series], kwargs: ATRKwargs) -> PolarsResult<Series> {
    let close = &mut inputs[0].to_float()?.rechunk();
    let high = &mut inputs[1].to_float()?.rechunk();
    let low = &mut inputs[2].to_float()?.rechunk();
    let (high_ptr, _high) = get_series_f64_ptr(high)?;
    let (low_ptr, _low) = get_series_f64_ptr(low)?;
    let (close_ptr, _close) = get_series_f64_ptr(close)?;
    let len = close.len();
    let res = ta_atr(high_ptr, low_ptr, close_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn trange(inputs: &[Series]) -> PolarsResult<Series> {
    let close = &mut inputs[0].to_float()?.rechunk();
    let high = &mut inputs[1].to_float()?.rechunk();
    let low = &mut inputs[2].to_float()?.rechunk();
    let (high_ptr, _high) = get_series_f64_ptr(high)?;
    let (low_ptr, _low) = get_series_f64_ptr(low)?;
    let (close_ptr, _close) = get_series_f64_ptr(close)?;
    let len = close.len();
    let res = ta_trange(high_ptr, low_ptr, close_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn natr(inputs: &[Series], kwargs: NATRKwargs) -> PolarsResult<Series> {
    let close = &mut inputs[0].to_float()?.rechunk();
    let high = &mut inputs[1].to_float()?.rechunk();
    let low = &mut inputs[2].to_float()?.rechunk();
    let (high_ptr, _high) = get_series_f64_ptr(high)?;
    let (low_ptr, _low) = get_series_f64_ptr(low)?;
    let (close_ptr, _close) = get_series_f64_ptr(close)?;
    let len = close.len();

    let res = ta_natr(high_ptr, low_ptr, close_ptr, len, &kwargs);

    match res {
        Ok(out) => {
            let out_ser = Float64Chunked::from_vec("", out);
            Ok(out_ser.into_series())
        }
        Err(ret_code) => ta_code2err(ret_code),
    }
}
