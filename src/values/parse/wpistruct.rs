use std::{borrow::Cow, num::NonZeroUsize};

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
    arrow::datatypes::{DataType, Field, SchemaBuilder},
    re_log,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WpiLibStructPrimitives {
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
}
impl TryFrom<&str> for WpiLibStructPrimitives {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value.as_ref() {
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
            _ => return Err(()),
        })
    }
}
impl WpiLibStructPrimitives {
    pub fn size(self) -> usize {
        use WpiLibStructPrimitives::*;
        match self {
            Bool | Char | Int8 | Uint8 => 1,
            Int16 | Uint16 => 2,
            Int32 | Uint32 | Float => 3,
            Int64 | Uint64 | Double => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnresolvedWpiLibStructType {
    Primitive(WpiLibStructPrimitives),
    Custom(String),
}

impl WpiLibStructPrimitives {
    #[must_use]
    /// Converts the current type to an Arrow primitive [`DataType`]
    pub const fn datatype(&self) -> DataType {
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
        }
    }
}

impl<'a> From<Cow<'a, str>> for UnresolvedWpiLibStructType {
    fn from(value: Cow<'a, str>) -> Self {
        if let Ok(primitive) = WpiLibStructPrimitives::try_from(value.as_ref()) {
            Self::Primitive(primitive)
        } else {
            Self::Custom(value.into_owned())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WpiLibStructType {
    Primitive(WpiLibStructPrimitives),
    Custom(WpiLibStructSchema<WpiLibStructType>),
}

impl WpiLibStructType {
    pub fn datatype(&self) -> DataType {
        match self {
            Self::Primitive(p) => p.datatype(),
            Self::Custom(s) => s.datatype(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WpiLibStructValues {
    Value,
    Enum(HashMap<String, i64>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WpiLibStructData<ValueType> {
    /// A Some value dictates that this is an array
    pub count: Option<NonZeroUsize>,
    pub value: WpiLibStructValues,
    pub ty: ValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WpiLibStructSchema<ValueType> {
    pub fields: HashMap<String, WpiLibStructData<ValueType>>,
}

impl WpiLibStructSchema<WpiLibStructType> {
    pub fn datatype(&self) -> DataType {
        let mut builder = SchemaBuilder::new();

        for (name, data) in &self.fields {
            builder.push(Field::new(name, data.ty.datatype(), true));
        }

        let fields = builder.finish().fields;

        re_log::info!(?fields);

        DataType::Struct(fields)
    }
}

pub fn identifier(input: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

fn struct_parser(
    data: &[u8],
) -> IResult<&[u8], (String, WpiLibStructData<UnresolvedWpiLibStructType>)> {
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
    // TODO: we shouldn't treat zero-sized arrays as a single value,
    // but what else can we do?
    .map(|(_, n, _)| NonZeroUsize::new(n))
    .parse(data)
    .unwrap_or((data, None));

    dbg!(count);

    let name = String::from_utf8_lossy(identifier_name).into_owned();
    let ty = UnresolvedWpiLibStructType::from(String::from_utf8_lossy(typename));

    let wpistruct = if let Some(wenum) = wpienum {
        WpiLibStructValues::Enum(wenum)
    } else {
        WpiLibStructValues::Value
    };

    Ok((
        data,
        (
            name,
            WpiLibStructData {
                count,
                value: wpistruct,
                ty,
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

impl WpiLibStructSchema<UnresolvedWpiLibStructType> {
    pub fn parse(mut data: &[u8]) -> Result<Self, anyhow::Error> {
        let mut fields = HashMap::new();

        loop {
            data = match multispace0::<_, nom::error::Error<_>>(data) {
                Ok((remaining, _)) => remaining,
                Err(_) => data,
            };

            println!("Parsing data: {data:?}");

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
    use std::num::NonZeroUsize;

    use crate::values::parse::wpistruct::{
        UnresolvedWpiLibStructType, WpiLibStructData, WpiLibStructPrimitives, WpiLibStructValues,
    };

    use super::WpiLibStructSchema;
    use hashbrown::HashMap;

    #[test]
    fn basic_struct() {
        let schema = b"  bool  value  ";

        let wpistruct = WpiLibStructSchema::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "value".to_string(),
                WpiLibStructData {
                    count: None,
                    value: WpiLibStructValues::Value,
                    ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Bool)
                }
            )])
        );
    }

    #[test]
    fn array_struct() {
        let schema = b"  double  arr  [  4  ]  ";

        let wpistruct = WpiLibStructSchema::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "arr".to_string(),
                WpiLibStructData {
                    count: NonZeroUsize::new(4),
                    value: WpiLibStructValues::Value,
                    ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Bool)
                }
            )])
        );
    }

    #[test]
    fn basic_enum() {
        let schema = b"  enum  {  }  int8  val";

        let wpistruct = WpiLibStructSchema::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([(
                "val".to_string(),
                WpiLibStructData {
                    count: None,
                    value: WpiLibStructValues::Enum(HashMap::new()),
                    ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Int8)
                }
            )])
        );
    }

    #[test]
    fn multi_struct() {
        let schema = b"  enum  {  a  =  3  ,  }  int64  something  ;  int8  other  ;  enum  {  multi  =  64,  other=24  }  uint16  number_3[3]";

        let wpistruct = WpiLibStructSchema::parse(schema).unwrap();

        assert_eq!(
            wpistruct.fields,
            HashMap::from([
                (
                    "something".to_string(),
                    WpiLibStructData {
                        count: None,
                        value: WpiLibStructValues::Enum(HashMap::from([("a".to_string(), 3)]),),
                        ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Int64,)
                    },
                ),
                (
                    "other".to_string(),
                    WpiLibStructData {
                        count: None,
                        value: WpiLibStructValues::Value,
                        ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Int8)
                    }
                ),
                (
                    "number_3".to_string(),
                    WpiLibStructData {
                        count: None,
                        value: WpiLibStructValues::Enum(HashMap::from([
                            ("multi".to_string(), 64),
                            ("other".to_string(), 24)
                        ]),),
                        ty: UnresolvedWpiLibStructType::Primitive(WpiLibStructPrimitives::Uint16),
                    }
                )
            ])
        );
    }
}
