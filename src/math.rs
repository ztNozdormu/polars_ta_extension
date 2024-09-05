use crate::utils::{get_series_f64_ptr, ta_code2err};
use polars::prelude::*;
use talib::common::TimePeriodKwargs;
use talib::math::{
    ta_add, ta_div, ta_max, ta_maxindex, ta_min, ta_minindex, ta_minmax, ta_minmaxindex, ta_mult,
    ta_sub, ta_sum,
};

use talib::math::{
    ta_acos, ta_asin, ta_atan, ta_ceil, ta_cos, ta_cosh, ta_exp, ta_floor, ta_ln, ta_log10, ta_sin,
    ta_sinh, ta_sqrt, ta_tan, ta_tanh,
};

fn add(inputs: &[Series]) -> PolarsResult<Series> {
    let input1 = &mut inputs[0].to_float()?.rechunk();
    let input2 = &mut inputs[1].to_float()?.rechunk();
    let (input1_ptr, _input1) = get_series_f64_ptr(input1)?;
    let (input2_ptr, _input2) = get_series_f64_ptr(input2)?;
    let len = input1.len();
    let res = ta_add(input1_ptr, input2_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn div(inputs: &[Series]) -> PolarsResult<Series> {
    let input1 = &mut inputs[0].to_float()?.rechunk();
    let input2 = &mut inputs[1].to_float()?.rechunk();
    let (input1_ptr, _input1) = get_series_f64_ptr(input1)?;
    let (input2_ptr, _input2) = get_series_f64_ptr(input2)?;
    let len = input1.len();
    let res = ta_div(input1_ptr, input2_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn max(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;
    let len = input.len();
    let res = ta_max(input_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn maxindex(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;
    let len = input.len();
    let res = ta_maxindex(input_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Int32Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn min(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;
    let len = input.len();
    let res = ta_min(input_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn minindex(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;
    let len = input.len();
    let res = ta_minindex(input_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Int32Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

pub fn minmax_output(_: &[Field]) -> PolarsResult<Field> {
    let min = Field::new("min", DataType::Float64);
    let max = Field::new("max", DataType::Float64);
    let v: Vec<Field> = vec![min, max];
    Ok(Field::new("", DataType::Struct(v)))
}

fn minmax(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_minmax(input_ptr, len, &kwargs);
    match res {
        Ok((outmin, outmax)) => {
            let min = Series::from_vec("min", outmin);
            let max = Series::from_vec("max", outmax);
            let out = StructChunked::new("", &[min, max])?;
            Ok(out.into_series())
        }
        Err(ret_code) => ta_code2err(ret_code),
    }
}

pub fn minmaxindex_output(_: &[Field]) -> PolarsResult<Field> {
    let minidx = Field::new("minidx", DataType::Int32);
    let maxidx = Field::new("maxidx", DataType::Int32);
    let v: Vec<Field> = vec![minidx, maxidx];
    Ok(Field::new("", DataType::Struct(v)))
}

fn minmaxindex(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_minmaxindex(input_ptr, len, &kwargs);
    match res {
        Ok((outminidx, outmaxidx)) => {
            let minidx = Series::from_vec("minidx", outminidx);
            let maxidx = Series::from_vec("maxidx", outmaxidx);
            let out = StructChunked::new("", &[minidx, maxidx])?;
            Ok(out.into_series())
        }
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn mult(inputs: &[Series]) -> PolarsResult<Series> {
    let input1 = &mut inputs[0].to_float()?.rechunk();
    let input2 = &mut inputs[1].to_float()?.rechunk();
    let (input1_ptr, _input1) = get_series_f64_ptr(input1)?;
    let (input2_ptr, _input2) = get_series_f64_ptr(input2)?;
    let len = input1.len();
    let res = ta_mult(input1_ptr, input2_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn sub(inputs: &[Series]) -> PolarsResult<Series> {
    let input1 = &mut inputs[0].to_float()?.rechunk();
    let input2 = &mut inputs[1].to_float()?.rechunk();
    let (input1_ptr, _input1) = get_series_f64_ptr(input1)?;
    let (input2_ptr, _input2) = get_series_f64_ptr(input2)?;
    let len = input1.len();
    let res = ta_sub(input1_ptr, input2_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn sum(inputs: &[Series], kwargs: TimePeriodKwargs) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_sum(input_ptr, len, &kwargs);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn acos(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_acos(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn asin(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_asin(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn atan(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_atan(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn ceil(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_ceil(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn cos(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_cos(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn cosh(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_cosh(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => {
            println!("ret_code: {:?}", ret_code);
            ta_code2err(ret_code)
        }
    }
}

fn exp(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_exp(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn floor(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_floor(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn ln(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_ln(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn log10(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_log10(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn sin(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_sin(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn sinh(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_sinh(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn sqrt(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_sqrt(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn tan(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_tan(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}

fn tanh(inputs: &[Series]) -> PolarsResult<Series> {
    let input = &mut inputs[0].to_float()?.rechunk();
    let (input_ptr, _input) = get_series_f64_ptr(input)?;

    let len = input.len();
    let res = ta_tanh(input_ptr, len);
    match res {
        Ok(out) => Ok(Float64Chunked::from_vec("", out).into_series()),
        Err(ret_code) => ta_code2err(ret_code),
    }
}
