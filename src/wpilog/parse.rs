use nom::{
    IResult, Parser, bytes::streaming as bstreaming, error::ErrorKind,
    number::streaming as nstreaming,
};

struct RecordHeaderLengths(u8);
impl RecordHeaderLengths {
    /// Returns the size of the entry ID field in bytes.
    pub fn size_entry_id(&self) -> u8 {
        (self.0 & 0b0000_0011) + 1
    }

    /// Returns the size of the payload length field in bytes.
    pub fn size_payload_len(&self) -> u8 {
        ((self.0 & 0b0000_1100) >> 2) + 1
    }

    /// Returns the size of the timestamp field in bytes.
    pub fn size_timestamp(&self) -> u8 {
        ((self.0 & 0b0111_0000) >> 4) + 1
    }
}
impl From<u8> for RecordHeaderLengths {
    fn from(value: u8) -> Self {
        RecordHeaderLengths(value)
    }
}

fn parse_string(input: &[u8], len: usize) -> IResult<&[u8], String, ParseError> {
    let (input, string_bytes) = bstreaming::take(len)(input)?;
    let string = std::str::from_utf8(string_bytes)
        .map(String::from)
        .map_err(|_| nom::Err::Failure(ParseError::InvalidString))?;
    Ok((input, string))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    InvalidFormat(nom::error::ErrorKind),
    InvalidVersion,
    InvalidString,
    InvalidIntegerSize,
    EOF,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidFormat(kind) => write!(f, "Invalid format: {:?}", kind),
            ParseError::InvalidVersion => write!(f, "Invalid version"),
            ParseError::InvalidString => write!(f, "Invalid string"),
            ParseError::InvalidIntegerSize => write!(f, "Invalid integer size"),
            ParseError::EOF => write!(f, "EOF"),
        }
    }
}
impl std::error::Error for ParseError {}

impl<T> nom::error::ParseError<T> for ParseError {
    fn from_error_kind(_input: T, kind: nom::error::ErrorKind) -> Self {
        ParseError::InvalidFormat(kind)
    }

    fn append(_input: T, kind: nom::error::ErrorKind, _other: Self) -> Self {
        ParseError::InvalidFormat(kind)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    // control records
    /// The Start control record provides information about the specified entry ID. It must appear prior to any records using that entry ID. The format of the Start control record’s payload data is as follows:
    Start {
        entry_id: u32,
        entry_name: String,
        entry_type: String,
        entry_metadata: String,
    },
    /// The Finish control record indicates the entry ID is no longer valid. The format of the Finish control record’s payload data is as follows:
    Finish {
        entry_id: u32,
    },
    /// The Set metadata control record updates the metadata for an entry. The format of the record’s payload data is as follows:
    SetMetadata {
        entry_id: u32,
        entry_metadata: String,
    },

    Raw {
        entry_id: u32,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub struct WpiRecord {
    pub timestamp: u64,

    pub payload: Payload,
}

impl WpiRecord {
    const START_CONTROL_RECORD: u8 = 0x00;
    const FINISH_CONTROL_RECORD: u8 = 0x01;
    const SET_METADATA_CONTROL_RECORD: u8 = 0x02;

    fn parse_dyn_int(input: &[u8], size: u8) -> IResult<&[u8], u64, ParseError> {
        let (input, s) = bstreaming::take(size)(input)?;
        if s.len() > 8 {
            return Err(nom::Err::Failure(ParseError::InvalidIntegerSize));
        }

        const ARR_SIZE: usize = u64::BITS as usize / 8;
        let mut buf = [0; ARR_SIZE];
        buf[..s.len()].copy_from_slice(s);

        Ok((input, u64::from_le_bytes(buf)))
    }
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, lengths) = match nstreaming::u8(input) {
            Ok((input, lengths)) => (input, lengths),
            Err(nom::Err::Incomplete(_)) => {
                return Err(nom::Err::Error(ParseError::EOF));
            }
            e => e?,
        };
        let lengths = RecordHeaderLengths::from(lengths);

        let (input, entry_id) = Self::parse_dyn_int(input, lengths.size_entry_id())?;
        let (input, payload_len) = Self::parse_dyn_int(input, lengths.size_payload_len())?;
        let (input, timestamp) = Self::parse_dyn_int(input, lengths.size_timestamp())?;

        let (leftover, input) = bstreaming::take(payload_len)(input)?;

        if entry_id == 0 {
            let (input, control_record_type) = nstreaming::u8(input)?;
            let (input, entry_id) = nstreaming::le_u32(input)?;

            match control_record_type {
                Self::START_CONTROL_RECORD => {
                    let (input, entry_name_len) = nstreaming::le_u32(input)?;
                    let (input, entry_name) = parse_string(input, entry_name_len as usize)?;
                    let (input, entry_type_len) = nstreaming::le_u32(input)?;
                    let (input, entry_type) = parse_string(input, entry_type_len as usize)?;
                    let (input, entry_metadata_len) = nstreaming::le_u32(input)?;
                    let (input, entry_metadata) = parse_string(input, entry_metadata_len as usize)?;

                    debug_assert!(input.is_empty(), "didn't consume all input",);

                    Ok((
                        leftover,
                        WpiRecord {
                            timestamp,
                            payload: Payload::Start {
                                entry_id,
                                entry_name,
                                entry_type,
                                entry_metadata,
                            },
                        },
                    ))
                }
                Self::FINISH_CONTROL_RECORD => {
                    debug_assert!(input.is_empty(), "didn't consume all input");

                    Ok((
                        leftover,
                        WpiRecord {
                            timestamp,
                            payload: Payload::Finish { entry_id },
                        },
                    ))
                }
                Self::SET_METADATA_CONTROL_RECORD => {
                    // the spec doesn't specify whether _this_ must be valid UTF-8 or not,
                    // but the previous one does, so let's assume it is
                    let (input, entry_metadata_len) = nstreaming::le_u32(input)?;
                    let (input, entry_metadata) = parse_string(input, entry_metadata_len as usize)?;

                    debug_assert!(input.is_empty(), "didn't consume all input");

                    Ok((
                        leftover,
                        WpiRecord {
                            timestamp,
                            payload: Payload::SetMetadata {
                                entry_id,
                                entry_metadata,
                            },
                        },
                    ))
                }
                _ => Err(nom::Err::Failure(ParseError::InvalidFormat(ErrorKind::Tag))),
            }
        } else {
            Ok((
                leftover,
                WpiRecord {
                    timestamp,
                    payload: Payload::Raw {
                        entry_id: entry_id as u32,
                        data: input.to_vec(),
                    },
                },
            ))
        }
    }
}

/// A simple binary logging format designed for high speed logging of timestamped data values (e.g. numeric sensor values).
#[derive(Debug, Clone, Default)]
pub struct WpiLogFile {
    /// version number.
    /// The most significant byte of the version
    /// indicates the major version and
    /// the least significant byte indicates the minor version.
    ///
    /// For this version of the data format, the value is thus 0x0100,
    /// indicating version 1.0.
    pub version: u16,
    /// The extra header string has arbitrary contents
    /// (e.g. the contents are set by the application that wrote the data log) but it must be UTF-8 encoded.
    pub extra_header: String,

    /// There is no timestamp ordering requirement for records. This is true for control records as well—
    /// ​a Start control record with a later timestamp may be followed by data records for that entry with earlier timestamps.
    pub records: Vec<WpiRecord>,
}

impl WpiLogFile {
    fn parse_header(input: &[u8]) -> IResult<&[u8], (u16, String), ParseError> {
        let (input, _) = nom::bytes::streaming::tag(&b"WPILOG"[..])(input)?;

        let (input, version) = nom::number::streaming::le_u16(input)?;
        if version != 0x0100 {
            return Err(nom::Err::Failure(ParseError::InvalidVersion));
        }

        let (input, header_len) = nom::number::streaming::le_u32(input)?;
        let (input, extra_header) = parse_string(input, header_len as usize)?;
        Ok((input, (version, extra_header)))
    }

    pub fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, (version, extra_header)) = Self::parse_header(input)?;

        let (input, records) = nom::multi::many0(WpiRecord::parse).parse(input)?;

        Ok((
            input,
            WpiLogFile {
                version,
                extra_header,
                records,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    #[test]
    fn test_example_header() {
        // 57 50 49 4c 4f 47 00 01 00 00 00 00
        let example_header = [
            0x57, // W
            0x50, // P
            0x49, // I
            0x4c, // L
            0x4f, // O
            0x47, // G
            // 0x0100 - version
            0x00, // b0000_0000 - low byte of version
            0x01, // b0000_0001 - high byte of version
            0x00, 0x00, 0x00, 0x00, // b0000_0000 - length of extra header
        ];

        let (input, (version, extra_header)) =
            super::WpiLogFile::parse_header(&example_header).unwrap();

        assert_eq!(input.len(), 0);
        assert_eq!(extra_header, "");
        assert_eq!(version, 0x0100);
    }

    #[test]
    fn test_bad_magic() {
        let example_header = [
            0x00, // \0 - should be 'W'
            0x50, // P
            0x49, // I
            0x4c, // L
            0x4f, // O
            0x47, // G
            // 0x0100 - version
            0x00, // b0000_0000 - low byte of version
            0x01, // b0000_0001 - high byte of version
            0x00, 0x00, 0x00, 0x00, // b0000_0000 - length of extra header
        ];

        let err = super::WpiLogFile::parse_header(&example_header).unwrap_err();

        assert_eq!(
            err,
            nom::Err::Error(super::ParseError::InvalidFormat(nom::error::ErrorKind::Tag))
        );
    }

    #[test]
    fn test_bad_extra_header() {
        let example_header = [
            0x57, // W
            0x50, // P
            0x49, // I
            0x4c, // L
            0x4f, // O
            0x47, // G
            // 0x0100 - version
            0x00, // b0000_0000 - low byte of version
            0x01, // b0000_0001 - high byte of version
            0x01, 0x00, 0x00, 0x00, // b0000_0001 - length of extra header
        ];

        let err = super::WpiLogFile::parse_header(&example_header).unwrap_err();

        assert_eq!(
            err,
            nom::Err::Incomplete(nom::Needed::Size(NonZeroUsize::new(1).unwrap()))
        );
    }

    #[test]
    fn test_extra_header() {
        let example_header = [
            0x57, // W
            0x50, // P
            0x49, // I
            0x4c, // L
            0x4f, // O
            0x47, // G
            // 0x0100 - version
            0x00, // b0000_0000 - low byte of version
            0x01, // b0000_0001 - high byte of version
            0x01, 0x00, 0x00, 0x00, // b0000_0001 - length of extra header
            b'a', // extra header
        ];

        let (input, (_version, extra_header)) =
            super::WpiLogFile::parse_header(&example_header).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(extra_header, "a");
    }

    #[test]
    fn test_example_record() {
        // 20 (ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes)
        //
        // 01 (entry ID = 1)
        //
        // 08 (payload size = 8 bytes)
        //
        // 40 42 0f (timestamp = 1,000,000 us)
        //
        // 03 00 00 00 00 00 00 00 (value = 3)
        //

        let example_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x01, // b0000_0001 - entry ID = 1
            0x08, // b0000_1000 - payload size = 8 bytes
            0x40, 0x42, 0x0f, // timestamp = 1,000,000 us
            //
            0x03, 0x00, 0x00, 0x00, //
            0x00, 0x00, 0x00, 0x00,
        ];

        let (input, record) = super::WpiRecord::parse(&example_record).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(record.timestamp, 1_000_000);
        assert_eq!(
            record.payload,
            super::Payload::Raw {
                entry_id: 1,
                data: vec![
                    0x03, 0x00, 0x00, 0x00, //
                    0x00, 0x00, 0x00, 0x00
                ],
            }
        );
    }

    #[test]
    fn test_start_cr() {
        // 20 (ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes)
        //
        // 00 (entry ID = 0)
        //
        // 1a (payload size = 26 bytes)
        //
        // 40 42 0f (timestamp = 1,000,000 us)
        //
        // 00 (control record type = Start (0))
        //
        // 01 00 00 00 (entry ID 1 being started)
        //
        // 04 00 00 00 (length of name string = 4)
        //
        // 74 65 73 74 (entry name = test)
        //
        // 05 00 00 00 (length of type string = 5)
        //
        // 69 6e 74 66 64 (type string = int64)
        //
        // 00 00 00 00 (length of metadata string = 0)

        let example_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x00, // b0000_0000 - entry ID = 0
            0x1a, // b0001_1010 - payload size = 26 bytes
            0x40, 0x42, 0x0f, // timestamp = 1,000,000 us
            //
            0x00, // b0000_0000 - control record type = Start (0)
            //
            0x01, 0x00, 0x00, 0x00, // entry ID 1 being started
            //
            0x04, 0x00, 0x00, 0x00, // length of name string = 4
            //
            b't', b'e', b's', b't', // entry name = test
            //
            0x05, 0x00, 0x00, 0x00, // length of type string = 5
            //
            b'i', b'n', b't', b'6', b'4', // type string = int64
            //
            0x00, 0x00, 0x00, 0x00, // metadata string length = 0
        ];

        let (input, record) = super::WpiRecord::parse(&example_record).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(record.timestamp, 1_000_000);
        assert_eq!(
            record.payload,
            super::Payload::Start {
                entry_id: 1,
                entry_name: "test".into(),
                entry_type: "int64".into(),
                entry_metadata: "".into(),
            }
        );
    }

    #[test]
    fn test_finish_cr() {
        // 20 (ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes)
        //
        // 00 (entry ID = 0)
        //
        // 05 (payload size = 5 bytes)
        //
        // 40 42 0f (timestamp = 1,000,000 us)
        //
        // 01 (control record type = Finish (1))
        //
        // 01 00 00 00 (entry ID being finished)

        let example_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x00, // b0000_0000 - entry ID = 0
            0x05, // b0000_1000 - payload size = 5 bytes
            0x40, 0x42, 0x0f, // timestamp = 1,000,000 us
            //
            0x01, // b0000_0001 - control record type = Finish (1)
            //
            0x01, 0x00, 0x00, 0x00, // entry ID being finished
        ];

        let (input, record) = super::WpiRecord::parse(&example_record).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(record.timestamp, 1_000_000);
        assert_eq!(record.payload, super::Payload::Finish { entry_id: 1 });
    }

    #[test]
    fn test_set_metadata_cr() {
        // 20 (ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes)
        //
        // 00 (entry ID = 0)
        //
        // 18 (payload size = 24 bytes)
        //
        // 40 42 0f (timestamp = 1,000,000 us)
        //
        // 02 (control record type = Set Metadata (2))
        //
        // 01 00 00 00 (setting metadata for entry ID 1)
        //
        // 0f 00 00 00 (length of metadata string = 15)
        //
        // 7b 22 73 6f 75 72 63 65 22 3a 22 4e 54 22 7d (metadata string = {"source":"NT"})

        let example_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x00, // b0000_0000 - entry ID = 0
            0x18, // b0001_1000 - payload size = 24 bytes
            0x40, 0x42, 0x0f, // timestamp = 1,000,000 us
            //
            0x02, // b0000_0010 - control record type = Set Metadata (2)
            //
            0x01, 0x00, 0x00, 0x00, // setting metadata for entry ID 1
            //
            0x0f, 0x00, 0x00, 0x00, // length of metadata string = 15
            //
            b'{', b'"', b's', b'o', b'u', b'r', b'c', b'e', b'"', b':', b'"', b'N', b'T', b'"',
            b'}',
        ];

        let (input, record) = super::WpiRecord::parse(&example_record).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(record.timestamp, 1_000_000);
        assert_eq!(
            record.payload,
            super::Payload::SetMetadata {
                entry_id: 1,
                entry_metadata: r#"{"source":"NT"}"#.into(),
            }
        );
    }

    #[test]
    fn test_multi_record() {
        let mut file = Vec::new();

        let file_header = [
            0x57, // W
            0x50, // P
            0x49, // I
            0x4c, // L
            0x4f, // O
            0x47, // G
            // 0x0100 - version
            0x00, // b0000_0000 - low byte of version
            0x01, // b0000_0001 - high byte of version
            0x00, 0x00, 0x00, 0x00, // b0000_0000 - length of extra header
        ];

        file.extend_from_slice(&file_header);

        let start_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x00, // b0000_0000 - entry ID = 0
            0x2b, // b101011 - payload size = 43 bytes
            0x40, 0x42, 0x0f, // timestamp = 1,000,000 us
            //
            0x00, // b0000_0000 - control record type = Start (0)
            //
            0x01, 0x00, 0x00, 0x00, // entry ID 1 being started
            //
            0x05, 0x00, 0x00, 0x00, // length of name string = 5
            //
            b'r', b'e', b'r', b'u', b'n', // entry name = rerun
            //
            0x05, 0x00, 0x00, 0x00, // length of type string = 5
            //
            b'i', b'n', b't', b'6', b'4', // type string = int64
            //
            0x10, 0x00, 0x00, 0x00, // length of metadata string = 16
            //
            b'{', b'"', b's', b'o', b'u', b'r', b'c', b'e', b'"', b':', b'"', b'l', b'o', b'g',
            b'"', b'}', // metadata string = {"source":"log"}
        ];

        file.extend_from_slice(&start_record);

        let raw_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x01, // b0000_0001 - entry ID = 1
            0x04, // b0000_0100 - payload size = 4 bytes
            0x72, 0x42, 0x0f, // timestamp = 1,000,050 us
            //
            b'c', b'o', b'o', b'l', //
        ];

        file.extend_from_slice(&raw_record);

        let finish_record = [
            0x20, // b0010_0000 - ID length = 1 byte, payload size length = 1 byte, timestamp length = 3 bytes
            0x00, // b0000_0000 - entry ID = 0
            0x05, // b0000_0101 - payload size = 5 bytes
            0xA4, 0x42, 0x0f, // timestamp = 1,000,100 us
            //
            0x01, // b0000_0001 - control record type = Finish (1)
            //
            0x01, 0x00, 0x00, 0x00, // entry ID being finished
        ];
        file.extend_from_slice(&finish_record);

        let (input, wpi_log) = super::WpiLogFile::parse(&file).unwrap();

        assert_eq!(input.len(), 0);

        assert_eq!(wpi_log.version, 0x0100);
        assert_eq!(wpi_log.extra_header, "");
        assert_eq!(wpi_log.records.len(), 3);
        assert_eq!(wpi_log.records[0].timestamp, 1_000_000);
        assert_eq!(
            wpi_log.records[0].payload,
            super::Payload::Start {
                entry_id: 1,
                entry_name: "rerun".into(),
                entry_type: "int64".into(),
                entry_metadata: r#"{"source":"log"}"#.into(),
            }
        );
        assert_eq!(wpi_log.records[1].timestamp, 1_000_050);
        assert_eq!(
            wpi_log.records[1].payload,
            super::Payload::Raw {
                entry_id: 1,
                data: "cool".as_bytes().to_vec(),
            }
        );
        assert_eq!(wpi_log.records[2].timestamp, 1_000_100);
        assert_eq!(
            wpi_log.records[2].payload,
            super::Payload::Finish { entry_id: 1 }
        );
    }

    #[test]
    fn test_real_world() {
        let example = include_bytes!("../../test_data/FRC_20250321_184359_FLOR_Q38.wpilog");

        let (input, _wpi_log) = super::WpiLogFile::parse(example).unwrap();

        assert_eq!(input.len(), 0);
    }
}
