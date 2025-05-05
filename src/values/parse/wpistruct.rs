use std::borrow::Cow;

use hashbrown::HashMap;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::recognize,
    error::Error as NomErr,
    multi::many0_count,
    sequence::{delimited, pair},
};
use rerun::external::{
    anyhow::{self},
    arrow::datatypes::DataType,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    pub const fn to_arrow(&self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::Char => DataType::Utf8,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Uint8 => DataType::UInt8,
            Self::Uint16 => DataType::UInt16,
            Self::Uint32 => DataType::UInt32,
            Self::Uint64 => DataType::UInt64,
            Self::Float => DataType::Float32,
            Self::Double => DataType::Float64,
            Self::Custom(_) => DataType::Binary,
        }
    }

    pub fn from_wpi_struct_str(s: Cow<str>) -> Self {
        match s.as_ref() {
            "bool" => Self::Bool,
            "char" => Self::Char,
            "int8" => Self::Int8,
            "int16" => Self::Int16,
            "int32" => Self::Int32,
            "int64" => Self::Int64,
            "uint8" => Self::Uint8,
            "uint16" => Self::Uint16,
            "uint32" => Self::Uint32,
            "uint64" => Self::Uint64,
            "float" | "float32" => Self::Float,
            "double" | "float64" => Self::Double,
            _ => Self::Custom(s.into_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WpiLibStructData {
    Value(WpiLibStructTypes),
    Enum(HashMap<String, i64>, WpiLibStructTypes),
    Array(Box<WpiLibStructData>, usize),
}

pub struct WpiLibStruct {
    pub fields: HashMap<String, WpiLibStructData>,
}

pub fn identifier(input: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

fn struct_parser(data: &[u8]) -> IResult<&[u8], (String, WpiLibStructData)> {
    println!("struct parsing");
    dbg!(String::from_utf8_lossy(data));

    let (data, wpienum) = enum_parser(data)
        .map(|(d, h)| (d, Some(h)))
        .unwrap_or((data, None));

    let (data, _) = multispace0(data)?;

    dbg!(String::from_utf8_lossy(data));

    let (data, typename) = identifier(data)?;

    dbg!(String::from_utf8_lossy(typename));

    let (data, _) = multispace1(data)?;

    dbg!(String::from_utf8_lossy(data));

    let (data, identifier_name) = identifier(data).unwrap();

    dbg!(String::from_utf8_lossy(identifier_name));

    let (data, _) = multispace0::<_, nom::error::Error<_>>(data)?;

    dbg!(String::from_utf8_lossy(data));

    let (data, count) = delimited(
        tag("["),
        (
            multispace0::<_, nom::error::Error<_>>,
            nom::character::complete::usize,
            multispace0,
        ),
        tag("]"),
    )
    .map(|(_, n, _)| Some(n))
    .parse(data)
    .unwrap_or((data, None));

    dbg!(count);

    let name = String::from_utf8_lossy(identifier_name).into_owned();
    let ty = WpiLibStructTypes::from_wpi_struct_str(String::from_utf8_lossy(typename));

    let wpistruct = if let Some(wenum) = wpienum {
        WpiLibStructData::Enum(wenum, ty)
    } else {
        WpiLibStructData::Value(ty)
    };

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

fn enum_parser(data: &[u8]) -> IResult<&[u8], HashMap<String, i64>> {
    println!("enum parsing");

    let mut values = HashMap::new();

    let (data, _) = tag::<_, _, NomErr<_>>("enum")
        .parse(data)
        .unwrap_or((data, &[]));

    let (data, _) = multispace0(data)?;

    let (mut data, _) = tag("{")(data)?;

    dbg!(String::from_utf8_lossy(data));

    loop {
        let (new_data, _) = multispace0::<_, NomErr<_>>(data).unwrap_or((data, &[]));

        if let Ok((remaining, _)) = tag::<_, _, NomErr<_>>("}")(new_data) {
            data = remaining;
            break;
        }

        dbg!(String::from_utf8_lossy(new_data));

        let (new_data, identifier) = identifier(new_data)?;
        let identifier = String::from_utf8_lossy(identifier).into_owned();

        dbg!(&identifier);

        let (new_data, _) = multispace0(new_data)?;

        let (new_data, _) = tag("=")(new_data)?;

        let (new_data, _) = multispace0(new_data)?;

        let (new_data, value) = nom::character::complete::i64(new_data)?;

        dbg!(value);

        let (new_data, _) = multispace0(new_data)?;

        values.insert(identifier, value);

        let (new_data, _) = tag::<_, _, NomErr<_>>(";")(new_data).unwrap_or((new_data, &[]));

        dbg!(String::from_utf8_lossy(new_data));

        data = new_data;
    }

    dbg!(&values);
    dbg!(String::from_utf8_lossy(data));

    Ok((data, values))
}

impl WpiLibStruct {
    pub fn parse(mut data: &[u8]) -> Result<Self, anyhow::Error> {
        let mut fields = HashMap::new();

        loop {
            data = match multispace0::<_, nom::error::Error<_>>(data) {
                Ok((remaining, _)) => remaining,
                Err(_) => data,
            };

            println!("Parsing data: {:?}", data);

            let Ok((remaining, (name, inner))) = struct_parser(data) else {
                break;
            };

            fields.insert(name, inner);

            if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<_>>(";")(remaining) {
                data = remaining;
            } else {
                break;
            }
        }

        Ok(Self { fields })
    }
}

#[cfg(test)]
mod test {
    use crate::values::parse::wpistruct::WpiLibStructTypes;

    use super::WpiLibStruct;
    use hashbrown::HashMap;

    #[test]
    fn basic_struct() {
        let schema = b"  bool  value  ";

        let wpistruct = WpiLibStruct::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "value".to_string(),
                super::WpiLibStructData::Value(super::WpiLibStructTypes::Bool),
            )])
        );
    }

    #[test]
    fn array_struct() {
        let schema = b"  double  arr  [  4  ]  ";

        let wpistruct = WpiLibStruct::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "arr".to_string(),
                super::WpiLibStructData::Array(
                    Box::new(super::WpiLibStructData::Value(
                        super::WpiLibStructTypes::Double
                    )),
                    4
                ),
            )])
        )
    }

    #[test]
    fn basic_enum() {
        let schema = b"  enum  {  }  int8  val";

        let wpistruct = WpiLibStruct::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "val".to_string(),
                super::WpiLibStructData::Enum(HashMap::new(), WpiLibStructTypes::Int8),
            )])
        )
    }

    #[test]
    fn multi_struct() {
        let schema = b"  enum  {  a  =  3  }  int64  something  ;  int8  other  ;  enum  {  multi  = 64  }  uint16  number_3[3]";

        let wpistruct = WpiLibStruct::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([
                (
                    "something".to_string(),
                    super::WpiLibStructData::Enum(
                        HashMap::from([("a".to_string(), 3)]),
                        WpiLibStructTypes::Int64
                    ),
                ),
                (
                    "other".to_string(),
                    super::WpiLibStructData::Value(super::WpiLibStructTypes::Int8),
                ),
                (
                    "number_3".to_string(),
                    super::WpiLibStructData::Array(
                        Box::new(super::WpiLibStructData::Enum(
                            HashMap::from([("multi".to_string(), 64)]),
                            WpiLibStructTypes::Uint16
                        )),
                        3
                    ),
                )
            ])
        )
    }
}
