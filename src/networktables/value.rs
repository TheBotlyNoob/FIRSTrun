use strum::EnumDiscriminants;

#[derive(Clone, Debug, EnumDiscriminants)]
pub enum EntryValue {
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
    Other(String, Vec<u8>),
}
