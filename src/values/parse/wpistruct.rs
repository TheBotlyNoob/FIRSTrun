use std::borrow::Cow;

use hashbrown::HashMap;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{multispace0, multispace1},
    multi::many0,
    sequence::{delimited, tuple},
};
use rerun::external::{
    anyhow::{self, bail},
    arrow::datatypes::DataType,
};

pub enum WpiLibStructTypes {
    Bool,
    Char,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float,
    Double,

    Custom(String),
}

impl WpiLibStructTypes {
    pub fn to_arrow(&self) -> DataType {
        match self {
            WpiLibStructTypes::Bool => DataType::Boolean,
            WpiLibStructTypes::Char => DataType::Utf8,
            WpiLibStructTypes::Int8 => DataType::Int8,
            WpiLibStructTypes::Int16 => DataType::Int16,
            WpiLibStructTypes::Int32 => DataType::Int32,
            WpiLibStructTypes::Int64 => DataType::Int64,
            WpiLibStructTypes::Uint8 => DataType::UInt8,
            WpiLibStructTypes::Uint16 => DataType::UInt16,
            WpiLibStructTypes::Uint32 => DataType::UInt32,
            WpiLibStructTypes::Uint64 => DataType::UInt64,
            WpiLibStructTypes::Float => DataType::Float32,
            WpiLibStructTypes::Double => DataType::Float64,
            WpiLibStructTypes::Custom(_) => todo!(),
        }
    }

    pub fn from_wpi_struct_str(s: Cow<str>) -> Self {
        match s.as_ref() {
            "bool" => WpiLibStructTypes::Bool,
            "char" => WpiLibStructTypes::Char,
            "int8" => WpiLibStructTypes::Int8,
            "int16" => WpiLibStructTypes::Int16,
            "int32" => WpiLibStructTypes::Int32,
            "int64" => WpiLibStructTypes::Int64,
            "uint8" => WpiLibStructTypes::Uint8,
            "uint16" => WpiLibStructTypes::Uint16,
            "uint32" => WpiLibStructTypes::Uint32,
            "uint64" => WpiLibStructTypes::Uint64,
            "float" | "float32" => WpiLibStructTypes::Float,
            "double" | "float64" => WpiLibStructTypes::Double,
            _ => WpiLibStructTypes::Custom(s.into_owned()),
        }
    }
}

pub enum WpiLibStructData {
    Value(WpiLibStructTypes),
    Enum(HashMap<String, i64>),
    Array(Box<WpiLibStructData>, usize),
}

pub struct WpiLibStruct {
    pub fields: HashMap<String, WpiLibStructData>,
}

fn struct_parser(data: &[u8]) -> IResult<&[u8], (String, WpiLibStructData)> {
    let (data, typename) = nom::character::complete::alphanumeric1
        .and_then(multispace1)
        .parse(data)?;

    let (mut data, identifier_name) = nom::character::complete::alphanumeric1
        .and_then(multispace0)
        .parse(data)?;

    let count = if let Ok((new, count)) = delimited(
        tag("[").and_then(multispace0::<&[u8], nom::error::Error<&[u8]>>),
        nom::character::complete::usize,
        multispace0.and_then(tag("]")),
    )
    .parse(data)
    {
        data = new;
        Some(count)
    } else {
        None
    };

    let name = String::from_utf8_lossy(identifier_name).into_owned();
    let ty = WpiLibStructTypes::from_wpi_struct_str(String::from_utf8_lossy(typename));

    let wpistruct = WpiLibStructData::Value(ty);

    Ok((
        data,
        (
            name,
            if let Some(count) = count {
                WpiLibStructData::Array(Box::new(wpistruct), count)
            } else {
                wpistruct
            },
        ),
    ))
}

fn enum_parser(data: &[u8]) -> IResult<&[u8], (String, WpiLibStructData)> {
    let (data, _) = nom::bytes::complete::tag("enum")
        .and_then(multispace0)
        .parse(data)?;
    todo!()
}

impl WpiLibStruct {
    pub fn parse(mut data: &[u8]) -> Result<Self, anyhow::Error> {
        let mut fields = HashMap::new();

        loop {
            data = match multispace0::<_, nom::error::Error<_>>(data) {
                Ok((remaining, _)) => remaining,
                Err(_) => break,
            };

            if let Ok((remaining, parsed)) = enum_parser(data) {
            } else if let Ok((remaining, (name, inner))) = struct_parser(data) {
                data = remaining;
                fields.insert(name, inner);
            }

            if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<_>>(";")(data) {
                data = remaining;
            } else {
                break;
            }
        }

        if fields.is_empty() {
            bail!("No valid struct or enum found");
        }
        Ok(WpiLibStruct { fields })
    }
}
