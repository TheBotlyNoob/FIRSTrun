use std::{fmt::Display, num::NonZero, sync::Arc};

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
    re_log,
};

use crate::log::EntryLog;

pub mod parse;

#[derive(Clone, Debug, PartialEq)]
pub enum EntryValue {
    Arrow(ArrayRef),
    ArrayArrow(Vec<ArrayRef>),
    StructSchema(WpiLibStructSchema<UnresolvedWpiLibStructType>),

    Map(HashMap<String, EntryValue>),
    ArrayMap(Vec<HashMap<String, EntryValue>>),
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

#[derive(Debug)]
pub enum EntryValueParseError {
    StructNotFound(String),
    Other(anyhow::Error),
}
impl std::fmt::Display for EntryValueParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StructNotFound(s) => write!(f, "Struct not found: {s}"),
            Self::Other(err) => write!(f, "{}", err),
        }
    }
}
impl std::error::Error for EntryValueParseError {}
impl From<anyhow::Error> for EntryValueParseError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl EntryValue {
    pub fn parse_from_wpilog(
        mut ty: &str,
        data: &[u8],
        struct_map: &HashMap<String, WpiLibStructSchema<UnresolvedWpiLibStructType>>,
    ) -> Result<EntryValue, EntryValueParseError> {
        let is_array = ty.strip_suffix("[]").map(|st| ty = st).is_some();

        Ok(match ty {
            "raw" => Self::parse_datatype(data, is_array, DataType::Binary)?,
            "boolean" => Self::parse_datatype(data, is_array, DataType::Boolean)?,
            "int64" => Self::parse_datatype(data, is_array, DataType::Int64)?,
            "float" => Self::parse_datatype(data, is_array, DataType::Float32)?,
            "double" => Self::parse_datatype(data, is_array, DataType::Float64)?,
            "string" => Self::parse_datatype(data, is_array, DataType::Utf8)?,
            "json" => return Err(anyhow!("json not implemented").into()),
            "structschema" => {
                let s = WpiLibStructSchema::parse(data)?;

                re_log::info!(?s);

                Self::StructSchema(s)
            }
            s => {
                if s.starts_with("struct:") {
                    let resolved = struct_map
                        .get(s)
                        .ok_or_else(|| EntryValueParseError::StructNotFound(ty.into()))
                        .and_then(|s| {
                            s.resolve(struct_map)
                                .map_err(|s| EntryValueParseError::StructNotFound(s))
                        })?;

                    dbg!(Self::parse_from_struct(data, resolved, is_array)?)
                } else {
                    return Err(
                        anyhow!("unknown data type {ty} (data length: {})", data.len()).into(),
                    );
                }
            }
        })
    }

    fn parse_datatype(
        data: &[u8],
        is_array: bool,
        ty: DataType,
    ) -> Result<EntryValue, EntryValueParseError> {
        if is_array {
            // TODO: handle strings
            let size = Self::datatype_size(ty.clone())
                .ok_or_else(|| anyhow!("datatype {ty} cannot be used as an array"))?;
            let array = data
                .windows(size)
                .map(|d| Self::parse_datatype_single(d, ty.clone()))
                .collect::<Result<_, _>>()?;
            Ok(EntryValue::ArrayArrow(array))
        } else {
            let array = Self::parse_datatype_single(data, ty)?;
            Ok(EntryValue::Arrow(array))
        }
    }

    // Returns the size of the datatype in the datalog spec.
    //
    // A return value of `None` indicates that the datatype is variable-sized, and cannot be used
    // as an array.
    fn datatype_size(ty: DataType) -> Option<usize> {
        match ty {
            DataType::Binary | DataType::Utf8 => None,
            DataType::Boolean => Some(1),
            DataType::Float32 => Some(4),
            DataType::Int64 | DataType::Float64 => Some(8),
            _ => None,
        }
    }

    fn parse_datatype_single(data: &[u8], ty: DataType) -> Result<ArrayRef, anyhow::Error> {
        Ok(match ty {
            // the raw data
            DataType::Binary => Arc::new(BinaryArray::from_iter_values([data])),
            // single byte (0=false, 1=true)
            DataType::Boolean => {
                Arc::new(BooleanArray::from_iter(data.first().map(|&b| Some(b != 0))))
            }
            // 8-byte (64-bit) signed value
            DataType::Int64 => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for int64"))?
                .try_into()
                .map(|b| Arc::new(Int64Array::from_iter_values([i64::from_le_bytes(b)])))?,
            // 4-byte (32-bit) IEEE-754 value
            DataType::Float32 => data
                .get(0..4)
                .with_context(|| anyhow!("not enough data for float"))?
                .try_into()
                .map(|b| Arc::new(Float32Array::from_iter_values([f32::from_le_bytes(b)])))?,
            // 8-byte (64-bit) IEEE-754 value
            DataType::Float64 => data
                .get(0..8)
                .with_context(|| anyhow!("not enough data for double"))?
                .try_into()
                .map(|b| Arc::new(Float64Array::from_iter_values([f64::from_le_bytes(b)])))?,
            // UTF-8 encoded string data
            DataType::Utf8 => Arc::new(StringArray::from_iter_values([String::from_utf8_lossy(
                data,
            )])),
            _ => bail!("unsupported datatype"),
        })
    }

    fn parse_from_struct(
        data: &[u8],
        schema: WpiLibStructSchema<WpiLibStructType>,
        is_array: bool,
    ) -> Result<EntryValue, anyhow::Error> {
        let value = if is_array {
            re_log::warn!(
                "parsing array value of {} bytes. schema size: {}. {} instances.",
                data.len(),
                schema.size(),
                data.len() as f32 / schema.size() as f32
            );
            EntryValue::ArrayMap(
                data.windows(schema.size())
                    .map(|d| {
                        let (data, this) = Self::parse_from_struct_single(d, &schema)?;

                        dbg!(&this);
                        debug_assert_eq!(data.len(), 0);

                        Ok::<_, anyhow::Error>(this)
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
            )
        } else {
            EntryValue::Map(Self::parse_from_struct_single(data, &schema)?.1)
        };

        Ok(value)
    }

    fn parse_from_struct_single<'d>(
        mut data: &'d [u8],
        schema: &WpiLibStructSchema<WpiLibStructType>,
    ) -> Result<(&'d [u8], HashMap<String, EntryValue>), anyhow::Error> {
        let mut new_map = HashMap::new();

        for (name, field) in &schema.fields {
            let this = match &field.ty {
                WpiLibStructType::Primitive(p) => {
                    let (new_data, this) = Self::parse_from_primitive(data, field, p)?;
                    data = new_data;

                    this
                }
                WpiLibStructType::Custom(s) => {
                    let (new_data, this) = Self::parse_from_struct_single(data, &s)?;
                    data = new_data;

                    EntryValue::Map(this)
                }
            };
            new_map.insert(name.clone(), this);
        }

        Ok((data, new_map))
    }

    fn parse_from_primitive<'d>(
        data: &'d [u8],
        field: &WpiLibStructData<WpiLibStructType>,
        ty: &WpiLibStructPrimitives,
    ) -> Result<(&'d [u8], EntryValue), anyhow::Error> {
        let (data, value) = nom::bytes::complete::take::<_, _, ()>(
            ty.size() * field.count.map_or(1, NonZero::get),
        )(data)?;

        let value = Self::parse_datatype(value, field.count.is_some(), ty.datatype())?;

        Ok((data, value))
    }
}
