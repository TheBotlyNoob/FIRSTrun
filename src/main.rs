//! This example demonstrates how to implement and register a [`re_data_loader::DataLoader`] into
//! the Rerun Viewer in order to add support for loading arbitrary files.
//!
//! Usage:
//! ```sh
//! $ cargo r -p custom_data_loader -- path/to/some/file
//! ```

#![warn(clippy::nursery)]

pub mod wpilog;

use std::iter::Peekable;
use std::path::PathBuf;
use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::anyhow;
use rerun::datatypes::Bool;
use rerun::external::anyhow::Context;
use rerun::external::re_log_types::{SetStoreInfo, StoreInfo, StoreSource};
use rerun::log::LogMsg;
use rerun::{ApplicationId, Loggable};
use rerun::{
    AsComponents, DataLoader as _, EntityPath, LoadedData, Scalar, TextDocument, TimePoint,
    Timeline,
    external::{anyhow, re_build_info, re_data_loader, re_log, re_log_types::NonMinI64},
    log::{Chunk, RowId},
    time::TimeInt,
};
use wpilog::parse::{Payload, WpiLogFile, WpiRecord};

fn main() -> anyhow::Result<std::process::ExitCode> {
    let main_thread_token = rerun::MainThreadToken::i_promise_i_am_on_the_main_thread();
    re_log::setup_logging();

    re_data_loader::register_custom_data_loader(WpiLogLoader);

    let build_info = re_build_info::build_info!();
    rerun::run(
        main_thread_token,
        build_info,
        rerun::CallSource::Cli,
        std::env::args(),
    )
    .map(std::process::ExitCode::from)
}

pub enum EntryValue<'log> {
    Raw(&'log [u8]),
    Boolean(bool),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(&'log str),
    BooleanArray(Vec<bool>),
    Int64Array(&'log [pack1::I64LE]),
    FloatArray(&'log [pack1::F32LE]),
    DoubleArray(&'log [pack1::F64LE]),
    StringArray(Vec<&'log str>),
    Other(&'log str, &'log [u8]),
}

impl<'log> EntryValue<'log> {
    pub fn parse(ty: &'log str, data: &'log [u8]) -> Result<Self, anyhow::Error> {
        re_log::info!("FIRST BYTE: {:#?}", data.first());

        match ty {
            // the raw data
            "raw" => Ok(Self::Other(ty, data)),
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
            "string" => Ok(Self::String(std::str::from_utf8(data)?)),
            // a single byte (0=false, 1=true) for each entry in the array[1]
            "boolean[]" => Ok(Self::BooleanArray(
                data.iter().map(|v| *v != 0).collect::<Vec<_>>(),
            )),
            // 8-byte (64-bit) signed value for each entry in the array[1]
            "int64[]" => Ok(Self::Int64Array(bytemuck::try_cast_slice(data)?)),
            // 4-byte (32-bit) value for each entry in the array[1]
            "float[]" => Ok(Self::FloatArray(bytemuck::try_cast_slice(data)?)),
            // 8-byte (64-bit) value for each entry in the array[1]
            "double[]" => Ok(Self::DoubleArray(bytemuck::try_cast_slice(data)?)),
            // Starts with a 4-byte (32-bit) array length. Each string is stored as a 4-byte (32-bit) length followed by the UTF-8 string data            _ => None,
            "string[]" => {
                let (mut real_input, arr_len) = nom::number::complete::le_u32::<_, ()>(data)?;
                let mut vals = Vec::with_capacity(arr_len as usize);

                for _ in 0..arr_len {
                    let (input, str_len) = nom::number::complete::le_u32::<_, ()>(real_input)?;
                    let (input, str_data) = nom::bytes::complete::take::<_, _, ()>(str_len)(input)?;
                    real_input = input;
                    let str_data = std::str::from_utf8(str_data)?;
                    vals.push(str_data);
                }

                Ok(Self::StringArray(vals))
            }
            _ => Err(anyhow!(
                "unknown data type {ty} (data length: {})",
                data.len()
            )),
        }
    }

    pub fn as_chunk(&self) -> Chunk {
        todo!()
    }
}

/// A custom [`re_data_loader::DataLoader`] that logs the hash of file as a [`rerun::TextDocument`].
struct WpiLogLoader;

impl re_data_loader::DataLoader for WpiLogLoader {
    fn name(&self) -> String {
        "rerun.data_loaders.frc.WpiLog".into()
    }

    fn load_from_path(
        &self,
        settings: &rerun::external::re_data_loader::DataLoaderSettings,
        path: std::path::PathBuf,
        tx: std::sync::mpsc::Sender<re_data_loader::LoadedData>,
    ) -> Result<(), re_data_loader::DataLoaderError> {
        let contents = std::fs::read(&path)?;
        if path.is_dir() {
            return Err(re_data_loader::DataLoaderError::Incompatible(path)); // simply not interested
        }
        parse_and_log(settings, &tx, &path, &contents)
    }

    fn load_from_file_contents(
        &self,
        settings: &rerun::external::re_data_loader::DataLoaderSettings,
        filepath: std::path::PathBuf,
        contents: std::borrow::Cow<'_, [u8]>,
        tx: std::sync::mpsc::Sender<re_data_loader::LoadedData>,
    ) -> Result<(), re_data_loader::DataLoaderError> {
        parse_and_log(settings, &tx, &filepath, &contents)
    }
}

struct SkipLastIterator<I: Iterator>(Peekable<I>);
impl<I: Iterator> Iterator for SkipLastIterator<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next();
        self.0.peek().map(|_| item.unwrap())
    }
}
trait SkipLast: Iterator + Sized {
    fn skip_last(self) -> SkipLastIterator<Self> {
        SkipLastIterator(self.peekable())
    }
}
impl<I: Iterator> SkipLast for I {}

struct EntryContext<'log> {
    id: u32,
    metadata: &'log str,
    ty: &'log str,
    name: &'log str,
}

fn send_record<'log>(
    settings: &rerun::external::re_data_loader::DataLoaderSettings,
    tx: &std::sync::mpsc::Sender<re_data_loader::LoadedData>,
    ctxs: &mut HashMap<u32, EntryContext<'log>>,
    timeline: Timeline,
    record: WpiRecord<'log>,
) {
    match record.payload {
        Payload::Start {
            entry_id,
            entry_name,
            entry_type,
            entry_metadata,
        } => {
            ctxs.insert(
                entry_id,
                EntryContext {
                    id: entry_id,
                    metadata: entry_metadata,
                    ty: entry_type,
                    name: entry_name,
                },
            );

            let _ = tx.send(LoadedData::LogMsg(
                WpiLogLoader::name(&WpiLogLoader),
                LogMsg::SetStoreInfo(SetStoreInfo {
                    row_id: *RowId::new(),
                    info: StoreInfo {
                        // TODO: specify an application_id
                        application_id: settings
                            .application_id
                            .clone()
                            .unwrap_or_else(ApplicationId::random),
                        store_id: settings
                            .opened_store_id
                            .clone()
                            .unwrap_or_else(|| settings.store_id.clone()),
                        cloned_from: None,
                        store_source: StoreSource::Other("NetworkTables".into()),
                        store_version: None,
                    },
                }),
            ));
        }
        Payload::Raw { entry_id, data } => {
            let Some(ctx) = ctxs.get(&entry_id) else {
                re_log::warn!("No context for entry id {entry_id}");
                return;
            };

            let ty = match EntryValue::parse(ctx.ty, data) {
                Ok(ty) => ty,
                Err(e) => {
                    re_log::warn!(
                        "Failed to parse entry type {} (data length: {}): {e}",
                        ctx.ty,
                        data.len()
                    );
                    return;
                }
            };

            let entity_path = Path::new(&ctx.name);

            let entity_path = entity_path
                .components()
                .skip(1)
                .skip_last()
                .collect::<PathBuf>();

            let info = TextDocument::new(ctx.ty).with_media_type(rerun::MediaType::TEXT);

            let entity_path = EntityPath::from_file_path(&entity_path);
            let chunk = Chunk::builder(entity_path)
                .with_archetype(
                    RowId::new(),
                    TimePoint::from([(
                        timeline,
                        TimeInt::from_nanos(NonMinI64::new(record.timestamp as i64).unwrap()),
                    )]),
                    &info,
                )
                .build()
                .unwrap();
            let store_id = settings
                .opened_store_id
                .clone()
                .unwrap_or_else(|| settings.store_id.clone());
            let data = LoadedData::Chunk(WpiLogLoader::name(&WpiLogLoader), store_id, chunk);

            tx.send(data).ok();
        }
        _ => (),
    }
}

fn parse_and_log(
    settings: &rerun::external::re_data_loader::DataLoaderSettings,
    tx: &std::sync::mpsc::Sender<re_data_loader::LoadedData>,
    filepath: &std::path::Path,
    contents: &[u8],
) -> Result<(), re_data_loader::DataLoaderError> {
    if !WpiLogFile::is_wpilog(contents) {
        return Err(re_data_loader::DataLoaderError::Incompatible(
            filepath.to_owned(),
        ));
    }

    let timeline = Timeline::new_temporal("robotime");

    let contents = contents.to_vec();
    let tx = tx.clone();
    let settings = settings.clone();
    std::thread::Builder::new()
        .name("WpiLogFile::parse".into())
        .spawn(move || {
            let mut ctxs = HashMap::new();
            let contents = contents;
            let tx = tx;
            let settings = settings;

            let (_, _log) = WpiLogFile::parse(contents.as_slice(), |record| {
                send_record(&settings, &tx, &mut ctxs, timeline, record)
            })
            .map_err(|e| {
                re_log::error!("WPI DataLog file error: {e}");
                re_data_loader::DataLoaderError::Other(e.into())
            })
            .unwrap();
        })
        .with_context(|| "failed to spawn WpiLogFile parsing thread".to_owned())?;

    Ok(())
}
