use std::{fmt::Display, sync::Arc};

use hashbrown::HashMap;
use nom::{Finish as _, IResult};
use parse::wpistruct::{
    UnresolvedWpiLibStructType, WpiLibStructData, WpiLibStructPrimitives, WpiLibStructSchema,
    WpiLibStructType,
};
use rerun::external::{
    anyhow::{self, Context, anyhow, bail},
    arrow::{
        array::{
            ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int64Array, NullArray,
            StringArray, StructArray,
        },
        datatypes::DataType,
    },
};

use crate::log::EntryLog;

pub mod parse;

#[derive(Clone, Debug, PartialEq)]
pub enum EntryValue {
    Arrow(ArrayRef),
    Map(HashMap<String, EntryValue>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidFormat(nom::error::ErrorKind),
}

impl std::error::Error for ParseError {}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat(kind) => write!(f, "Invalid format: {kind:?}"),
        }
    }
}
impl<T> nom::error::ParseError<T> for ParseError {
    fn from_error_kind(_input: T, kind: nom::error::ErrorKind) -> Self {
        Self::InvalidFormat(kind)
    }

    fn append(_input: T, kind: nom::error::ErrorKind, _other: Self) -> Self {
        Self::InvalidFormat(kind)
    }
}

impl EntryValue {
    fn parse_datatype(data: &[u8], array: bool, ty: DataType) -> Result<Self, anyhow::Error> {
        Ok(match ty {
            // the raw data
            DataType::Binary if !array => {
                Self::Arrow(Arc::new(BinaryArray::from_iter_values([data])))
            }
            // single byte (0=false, 1=true)
            DataType::Boolean if !array => Self::Arrow(Arc::new(BooleanArray::from_iter(
                data.first().map(|&b| Some(b != 0)),
            ))),
            // 8-byte (64-bit) signed value
            DataType::Int64 if !array => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for int64"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Int64Array::from_iter_values([
                        i64::from_le_bytes(b),
                    ])))
                })?,
            // 4-byte (32-bit) IEEE-754 value
            DataType::Float32 if !array => data
                .get(0..4)
                .with_context(|| anyhow!("not enough data for float"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Float32Array::from_iter_values([
                        f32::from_le_bytes(b),
                    ])))
                })?,
            // 8-byte (64-bit) IEEE-754 value
            DataType::Float64 if !array => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for double"))?
                .try_into()
                .map(|b| {
                    Self::Arrow(Arc::new(Float64Array::from_iter_values([
                        f64::from_le_bytes(b),
                    ])))
                })?,
            // UTF-8 encoded string data
            DataType::Utf8 if !array => Self::Arrow(Arc::new(StringArray::from_iter_values([
                String::from_utf8_lossy(data),
            ]))),
            // a single byte (0=false, 1=true) for each entry in the array[1]
            DataType::Boolean if array => Self::Arrow(Arc::new(
                data.iter().map(|v| Some(*v != 0)).collect::<BooleanArray>(),
            )),
            // 8-byte (64-bit) signed value for each entry in the array[1]
            DataType::Int64 if array => Self::Arrow(Arc::new(
                data.chunks_exact(8)
                    .map(|b| Some(i64::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Int64Array>(),
            )),
            // 4-byte (32-bit) value for each entry in the array[1]
            DataType::Float32 if array => Self::Arrow(Arc::new(
                data.chunks_exact(4)
                    .map(|b| Some(f32::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Float32Array>(),
            )),
            // 8-byte (64-bit) value for each entry in the array[1]
            DataType::Float64 if array => Self::Arrow(Arc::new(
                data.chunks_exact(8)
                    .map(|b| Some(f64::from_le_bytes(b.try_into().ok()?)))
                    .collect::<Float64Array>(),
            )),
            // Starts with a 4-byte (32-bit) array length. Each string is stored as a 4-byte (32-bit) length followed by the UTF-8 string data
            DataType::Utf8 if array => {
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
            _ => bail!("unsupported datatype"),
        })
    }

    pub fn parse_from_wpilog(
        ty: &str,
        data: &[u8],
        entry_name: impl Into<String>,
        logger: &mut EntryLog,
    ) -> Result<Self, anyhow::Error> {
        Ok(match ty {
            "raw" => Self::parse_datatype(data, false, DataType::Binary)?,
            "boolean" => Self::parse_datatype(data, false, DataType::Boolean)?,
            "int64" => Self::parse_datatype(data, false, DataType::Int64)?,
            "float" => Self::parse_datatype(data, false, DataType::Float32)?,
            "double" => Self::parse_datatype(data, false, DataType::Float64)?,
            "string" => Self::parse_datatype(data, false, DataType::Utf8)?,
            "boolean[]" => Self::parse_datatype(data, true, DataType::Boolean)?,
            "int64[]" => Self::parse_datatype(data, true, DataType::Int64)?,
            "float[]" => Self::parse_datatype(data, true, DataType::Float32)?,
            "double[]" => Self::parse_datatype(data, true, DataType::Float64)?,
            "string[]" => Self::parse_datatype(data, true, DataType::Utf8)?,
            "json" => bail!("json not implemented"),
            "structschema" => {
                let s = WpiLibStructSchema::parse(data)?;

                logger.add_struct(entry_name, s);

                Self::Arrow(Arc::new(StringArray::from_iter_values([
                    String::from_utf8_lossy(data).into_owned(),
                ])))
            }
            s => {
                if let Some(mut s) = s.strip_prefix("struct:") {
                    let is_array = s.strip_suffix("[]").map(|st| s = st).is_some();

                    let schema = logger
                        .resolve_struct(s)
                        .with_context(|| anyhow!("couldn't resolve struct {s}"))?;

                    dbg!(Self::parse_from_struct(data, schema)?.1)
                } else {
                    bail!("unknown data type {ty} (data length: {})", data.len());
                }
            }
        })
    }

    fn parse_from_struct<'d>(
        mut data: &'d [u8],
        schema: WpiLibStructSchema<WpiLibStructType>,
    ) -> Result<(&'d [u8], Self), anyhow::Error> {
        let mut new_map = HashMap::new();

        for (name, field) in schema.fields.into_iter() {
            let this = match field.ty {
                WpiLibStructType::Primitive(p) => {
                    let (new_data, this) = Self::parse_from_primitive(data, field, p)?;
                    data = new_data;

                    this
                }
                WpiLibStructType::Custom(s) => {
                    let (new_data, this) = Self::parse_from_struct(data, s)?;
                    data = new_data;

                    this
                }
            };
            new_map.insert(name.clone(), this);
        }

        Ok((data, Self::Map(new_map)))
    }

    fn parse_from_primitive<'d>(
        data: &'d [u8],
        field: WpiLibStructData<WpiLibStructType>,
        ty: WpiLibStructPrimitives,
    ) -> Result<(&'d [u8], Self), anyhow::Error> {
        let (data, value) = nom::bytes::complete::take::<_, _, ()>(ty.size())(data)?;

        let value = Self::parse_datatype(value, field.count.is_some(), ty.datatype())?;

        Ok((data, value))
    }
}
