use crate::utils::{get_series_f64_ptr, ta_code2err};
use polars::prelude::*;
use talib::cycle::{ta_ht_dcperiod, ta_ht_dcphase, ta_ht_phasor, ta_ht_sine, ta_ht_trendmode};

fn ht_dcperiod(inputs: &[Series]) -> PolarsResult<Series> {
    let mut real = inputs[0].to_float()?.rechunk();
    let (real_ptr, _real) = get_series_f64_ptr(&mut real)?;
    let res = ta_ht_dcperiod(real_ptr, real.len());
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn ht_dcphase(inputs: &[Series]) -> PolarsResult<Series> {
    let mut real = inputs[0].to_float()?.rechunk();
    let (real_ptr, _real) = get_series_f64_ptr(&mut real)?;
    let res = ta_ht_dcphase(real_ptr, real.len());
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

pub fn ht_phasor_output(_: &[Field]) -> PolarsResult<Field> {
    let inphase = Field::new("inphase", DataType::Float64);
    let quadrature = Field::new("quadrature", DataType::Float64);
    let v: Vec<Field> = vec![inphase, quadrature];
    Ok(Field::new("", DataType::Struct(v)))
}

fn ht_phasor(inputs: &[Series]) -> PolarsResult<Series> {
    let mut real = inputs[0].to_float()?.rechunk();
    let (real_ptr, _real) = get_series_f64_ptr(&mut real)?;
    let res = ta_ht_phasor(real_ptr, real.len());
    match res {
        Ok((outinphase, outquadrature)) => {
            let i = Series::from_vec("inphase", outinphase);
            let q = Series::from_vec("quadrature", outquadrature);
            let out = StructChunked::new("", &[i, q])?;
            Ok(out.into_series())
        }
        Err(ret_code) => ta_code2err(ret_code),
    }
}

pub fn ht_sine_output(_: &[Field]) -> PolarsResult<Field> {
    let sine = Field::new("sine", DataType::Float64);
    let leadsine = Field::new("leadsine", DataType::Float64);
    let v: Vec<Field> = vec![sine, leadsine];
    Ok(Field::new("", DataType::Struct(v)))
}

fn ht_sine(inputs: &[Series]) -> PolarsResult<Series> {
    let mut real = inputs[0].to_float()?.rechunk();
    let (real_ptr, _real) = get_series_f64_ptr(&mut real)?;
    let res = ta_ht_sine(real_ptr, real.len());
    match res {
        Ok((outsine, outleadsine)) => {
            let s = Series::from_vec("sine", outsine);
            let l = Series::from_vec("leadsine", outleadsine);
            let out = StructChunked::new("", &[s, l])?;
            Ok(out.into_series())
        }
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn ht_trendmode(inputs: &[Series]) -> PolarsResult<Series> {
    let mut real = inputs[0].to_float()?.rechunk();
    let (real_ptr, _real) = get_series_f64_ptr(&mut real)?;
    let res = ta_ht_trendmode(real_ptr, real.len());
    match res {
        Ok(out) => Ok(Int32Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}
