use std::sync::Arc;

use hashbrown::HashMap;
use rerun::external::{
    anyhow::{self, Context, anyhow, bail},
    arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray,
    },
};

#[derive(Clone, Debug, PartialEq)]
pub enum EntryValue {
    Arrow(ArrayRef),
    Map(HashMap<String, EntryValue>),
}

impl EntryValue {
    pub fn parse_from_wpilog(ty: &str, data: &[u8]) -> Result<Self, anyhow::Error> {
        Ok(match ty {
            // the raw data
            "raw" => Self::Arrow(Arc::new(BinaryArray::from_iter_values([data]))),
            // single byte (0=false, 1=true)
            "boolean" => Self::Arrow(Arc::new(BooleanArray::from_iter(
                data.first().map(|&b| Some(b != 0)),
            ))),
            // 8-byte (64-bit) signed value
            "int64" => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for int64"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Int64Array::from_iter_values([
                        i64::from_le_bytes(b),
                    ])))
                })?,
            // 4-byte (32-bit) IEEE-754 value
            "float" => data
                .get(0..4)
                .with_context(|| anyhow!("not enough data for float"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Float32Array::from_iter_values([
                        f32::from_le_bytes(b),
                    ])))
                })?,
            // 8-byte (64-bit) IEEE-754 value
            "double" => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for double"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Float64Array::from_iter_values([
                        f64::from_le_bytes(b),
                    ])))
                })?,
            // UTF-8 encoded string data
            "string" => Self::Arrow(Arc::new(StringArray::from_iter_values([
                String::from_utf8_lossy(data),
            ]))),
            // a single byte (0=false, 1=true) for each entry in the array[1]
            "boolean[]" => Self::Arrow(Arc::new(BooleanArray::from_iter(
                data.iter().map(|v| Some(*v != 0)),
            ))),
            // 8-byte (64-bit) signed value for each entry in the array[1]
            "int64[]" => Self::Arrow(Arc::new(Int64Array::from_iter(
                data.chunks_exact(8)
                    .map(|b| Some(i64::from_le_bytes(b.try_into().ok()?))),
            ))),
            // 4-byte (32-bit) value for each entry in the array[1]
            "float[]" => Self::Arrow(Arc::new(Float32Array::from_iter(
                data.chunks_exact(4)
                    .map(|b| Some(f32::from_le_bytes(b.try_into().ok()?))),
            ))),
            // 8-byte (64-bit) value for each entry in the array[1]
            "double[]" => Self::Arrow(Arc::new(Float64Array::from_iter(
                data.chunks_exact(8)
                    .map(|b| Some(f64::from_le_bytes(b.try_into().ok()?))),
            ))),
            // Starts with a 4-byte (32-bit) array length. Each string is stored as a 4-byte (32-bit) length followed by the UTF-8 string data            _ => None,
            "string[]" => {
                let (mut real_input, arr_len) = nom::number::complete::le_u32::<_, ()>(data)?;
                let mut vals = Vec::with_capacity(arr_len as usize);

                for _ in 0..arr_len {
                    let (input, str_len) = nom::number::complete::le_u32::<_, ()>(real_input)?;
                    let (input, str_data) = nom::bytes::complete::take::<_, _, ()>(str_len)(input)?;
                    real_input = input;
                    let str_data = String::from_utf8_lossy(str_data).into_owned();
                    vals.push(str_data);
                }

                Self::Arrow(Arc::new(StringArray::from_iter_values(vals)))
            }
            "json" => bail!("json not implemented"),
            "structschema" => {
                // todo: parse struct
                bail!("structschema not implemented")
            }
            _ => bail!("unknown data type {ty} (data length: {})", data.len()),
        })
    }
}
