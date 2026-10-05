use std::{num::NonZero, sync::Arc};

use ::anyhow::{Context, anyhow, bail};
use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray,
    },
    compute::concat,
    datatypes::DataType,
};
use hashbrown::HashMap;
use parse::wpistruct::{
    UnresolvedWpiLibStructType, WpiLibStructData, WpiLibStructPrimitives, WpiLibStructSchema,
    WpiLibStructType,
};
use re_log;

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

                    Self::parse_from_struct(data, resolved, is_array)?
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
            let size = Self::datatype_size(ty.clone())
                .ok_or_else(|| anyhow!("datatype {ty} cannot be used as an array"))?;
            let chunks = data.chunks_exact(size);
            if !chunks.remainder().is_empty() {
                return Err(anyhow!(
                    "array payload has {} trailing bytes for {ty}",
                    chunks.remainder().len()
                )
                .into());
            }
            let arrays = data
                .chunks_exact(size)
                .map(|d| Self::parse_datatype_single(d, ty.clone()))
                .collect::<Result<_, _>>()?;
            let arrays: Vec<ArrayRef> = arrays;
            let arrays = arrays.iter().map(|array| &**array).collect::<Vec<_>>();
            Ok(EntryValue::Arrow(
                concat(&arrays).map_err(anyhow::Error::from)?,
            ))
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
            if schema.size() == 0 {
                bail!("cannot parse an array of zero-sized structs");
            }
            let mut chunks = data.chunks_exact(schema.size());
            if !chunks.remainder().is_empty() {
                bail!(
                    "struct array payload has {} trailing bytes",
                    chunks.remainder().len()
                );
            }
            EntryValue::ArrayMap(
                chunks
                    .by_ref()
                    .map(|d| {
                        let (remaining, this) = Self::parse_from_struct_single(d, &schema)?;
                        anyhow::ensure!(remaining.is_empty(), "struct parser left bytes");
                        Ok::<_, anyhow::Error>(this)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            let (remaining, value) = Self::parse_from_struct_single(data, &schema)?;
            anyhow::ensure!(remaining.is_empty(), "struct payload has trailing bytes");
            EntryValue::Map(value)
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

#[cfg(test)]
mod tests {
    use super::EntryValue;
    use hashbrown::HashMap;
    use rerun::external::arrow::array::{Float64Array, Int64Array};

    #[test]
    fn parses_fixed_width_arrays_without_overlapping_elements() {
        let value = EntryValue::parse_from_wpilog(
            "int64[]",
            &[
                1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            ],
            &HashMap::new(),
        )
        .unwrap();

        let EntryValue::Arrow(value) = value else {
            panic!("expected one Arrow array");
        };
        let values = value.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn rejects_partial_fixed_width_array_payloads() {
        assert!(EntryValue::parse_from_wpilog("int64[]", &[1, 2, 3], &HashMap::new()).is_err());
    }

    #[test]
    fn decodes_struct_fields_in_schema_order() {
        let schema =
            super::parse::wpistruct::WpiLibStructSchema::parse(b"double x; double y;").unwrap();
        let mut structs = HashMap::new();
        structs.insert("struct:Translation2d".to_string(), schema);

        let mut payload = Vec::new();
        payload.extend_from_slice(&1.25f64.to_le_bytes());
        payload.extend_from_slice(&(-2.5f64).to_le_bytes());

        let value =
            EntryValue::parse_from_wpilog("struct:Translation2d", &payload, &structs).unwrap();
        let EntryValue::Map(fields) = value else {
            panic!("expected a struct field map");
        };
        let EntryValue::Arrow(x) = fields.get("x").unwrap() else {
            panic!("expected x to be an Arrow value");
        };
        let EntryValue::Arrow(y) = fields.get("y").unwrap() else {
            panic!("expected y to be an Arrow value");
        };
        assert_eq!(
            x.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
            1.25
        );
        assert_eq!(
            y.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
            -2.5
        );
    }

    #[test]
    fn decodes_struct_arrays_into_ordered_indexed_maps() {
        let schema =
            super::parse::wpistruct::WpiLibStructSchema::parse(b"double x; double y;").unwrap();
        let mut structs = HashMap::new();
        structs.insert("struct:Translation2d".to_string(), schema);

        let mut payload = Vec::new();
        for (x, y) in [(1.0f64, 2.0f64), (3.0f64, 4.0f64)] {
            payload.extend_from_slice(&x.to_le_bytes());
            payload.extend_from_slice(&y.to_le_bytes());
        }

        let value =
            EntryValue::parse_from_wpilog("struct:Translation2d[]", &payload, &structs).unwrap();
        let EntryValue::ArrayMap(elements) = value else {
            panic!("expected an array of struct maps");
        };
        assert_eq!(elements.len(), 2);
        for (element, expected) in elements.iter().zip([(1.0, 2.0), (3.0, 4.0)]) {
            let EntryValue::Arrow(x) = element.get("x").unwrap() else {
                panic!("expected x to be an Arrow value");
            };
            let EntryValue::Arrow(y) = element.get("y").unwrap() else {
                panic!("expected y to be an Arrow value");
            };
            assert_eq!(
                x.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
                expected.0
            );
            assert_eq!(
                y.as_any().downcast_ref::<Float64Array>().unwrap().value(0),
                expected.1
            );
        }
    }
}
