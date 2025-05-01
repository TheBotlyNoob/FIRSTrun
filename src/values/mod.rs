use hashbrown::HashMap;
use rerun::external::anyhow::{self, Context, anyhow};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub enum EntryValue {
    Nil,
    Raw(Vec<u8>),
    Boolean(bool),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(String),
    BooleanArray(Vec<bool>),
    Int64Array(Vec<i64>),
    FloatArray(Vec<f32>),
    DoubleArray(Vec<f64>),
    StringArray(Vec<String>),
    Map(HashMap<String, EntryValue>),
    Other(String, Vec<u8>),
}

impl EntryValue {
    pub fn parse_from_wpilog(ty: &str, data: &[u8]) -> Result<Self, anyhow::Error> {
        match ty {
            // the raw data
            "raw" => Ok(Self::Other(ty.to_owned(), data.to_vec())),
            // single byte (0=false, 1=true)
            "boolean" => data
                .first()
                .map(|&b| Self::Boolean(b != 0))
                .with_context(|| anyhow!("not enough data for boolean")),
            // 8-byte (64-bit) signed value
            "int64" => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for int64"))?
                .try_into()
                .map(|b| Self::Int64(i64::from_le_bytes(b)))
                .map_err(Into::into),
            // 4-byte (32-bit) IEEE-754 value
            "float" => data
                .get(0..4)
                .with_context(|| anyhow!("not enough data for float"))?
                .try_into()
                .map(|b| Self::Float(f32::from_le_bytes(b)))
                .map_err(Into::into),
            // 8-byte (64-bit) IEEE-754 value
            "double" => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for double"))?
                .try_into()
                .map(|b| Self::Double(f64::from_le_bytes(b)))
                .map_err(Into::into),
            // UTF-8 encoded string data
            "string" => Ok(Self::String(String::from_utf8_lossy(data).into_owned())),
            // a single byte (0=false, 1=true) for each entry in the array[1]
            "boolean[]" => Ok(Self::BooleanArray(data.iter().map(|v| *v != 0).collect())),
            // 8-byte (64-bit) signed value for each entry in the array[1]
            "int64[]" => Ok(Self::Int64Array(
                data.chunks_exact(8)
                    .map(|b| Some(i64::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Option<Vec<i64>>>()
                    .with_context(|| anyhow!("not enough data for int64[]"))?,
            )),
            // 4-byte (32-bit) value for each entry in the array[1]
            "float[]" => Ok(Self::FloatArray(
                data.chunks_exact(4)
                    .map(|b| Some(f32::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Option<Vec<f32>>>()
                    .with_context(|| anyhow!("not enough data for float[]"))?,
            )),
            // 8-byte (64-bit) value for each entry in the array[1]
            "double[]" => Ok(Self::DoubleArray(
                data.chunks_exact(8)
                    .map(|b| Some(f64::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Option<Vec<f64>>>()
                    .with_context(|| anyhow!("not enough data for double[]"))?,
            )),
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

                Ok(Self::StringArray(vals))
            }
            "json" => serde_json::from_slice(data)
                .map(|v: HashMap<String, EntryValue>| Self::Map(v))
                .map_err(Into::into),
            "structschema" => {
                // todo: parse struct
                Err(anyhow!("structschema not implemented"))
            }
            _ => Err(anyhow!(
                "unknown data type {ty} (data length: {})",
                data.len()
            )),
        }
    }
}
