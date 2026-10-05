//! This example demonstrates how to implement and register a [`re_data_loader::DataLoader`] into
//! the Rerun Viewer in order to add support for loading arbitrary files.
//!
//! Usage:
//! ```sh
//! $ cargo r -p custom_data_loader -- path/to/some/file
//! ```

#![warn(clippy::nursery, clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod wpilog;

use std::path::Path;

use conv::log_changes_to_chunks;
use hashbrown::HashMap;

use log::{EntryLog, Timestamp};
use rerun::external::anyhow::Context;
use rerun::external::re_log_types::{SetStoreInfo, StoreInfo, StoreSource};
use rerun::{
    ApplicationId, EntityPath, ImportedData, Importer, ImporterError, ImporterSettings, Timeline,
    external::{re_build_info, re_importer, re_log},
    log::{LogMsg, RowId},
};
use tokio::runtime::Runtime;
use wpilog::parse::{Payload, WpiLogFile, WpiRecord};

pub mod conv;
pub mod log;
pub mod nt;
pub mod values;

fn main() -> anyhow::Result<std::process::ExitCode> {
    std::thread::Builder::new()
        .name("networktables".into())
        .spawn(|| {
            let rt = Runtime::new().unwrap();
            rt.block_on(
                // Initialize the NetworkTables client
                nt::begin_logging(),
            );
        })?;

    let main_thread_token = rerun::MainThreadToken::i_promise_i_am_on_the_main_thread();
    re_log::setup_logging();

    re_importer::register_custom_importer(WpiLogLoader);

    let build_info = re_build_info::build_info!();
    rerun::run(
        main_thread_token,
        build_info,
        rerun::CallSource::Cli,
        std::env::args(),
    )
    .map(std::process::ExitCode::from)
}

/// A custom importer for WPI log files.
struct WpiLogLoader;

impl Importer for WpiLogLoader {
    fn name(&self) -> String {
        "rerun.importers.frc.WpiLog".into()
    }

    fn import_from_path(
        &self,
        settings: &ImporterSettings,
        path: std::path::PathBuf,
        tx: crossbeam::channel::Sender<ImportedData>,
    ) -> Result<(), ImporterError> {
        let contents = std::fs::read(&path)?;
        if path.is_dir() {
            return Err(ImporterError::Incompatible(path));
        }
        parse_and_log(settings, &tx, &path, &contents)
    }

    fn import_from_file_contents(
        &self,
        settings: &ImporterSettings,
        filepath: std::path::PathBuf,
        contents: std::borrow::Cow<'_, [u8]>,
        tx: crossbeam::channel::Sender<ImportedData>,
    ) -> Result<(), ImporterError> {
        parse_and_log(settings, &tx, &filepath, &contents)
    }
}

struct EntryContext<'log> {
    ty: &'log str,
    name: &'log str,
}

fn handle_data(
    ty: &str,
    timestamp: Timestamp,
    key: EntityPath,
    data: &[u8],
    logger: &mut EntryLog,
) {
    let kstr = key.to_string();
    let error = |e: anyhow::Error| {
        re_log::warn!(
            "handle_data: Failed to parse entry type {} (data length: {}) (key: {}): {e}",
            ty,
            data.len(),
            kstr,
        );
    };

    match logger.add_entry(key, timestamp, ty, data) {
        Ok(_) => {}
        Err(e) => error(e),
    }
}

fn fill_log<'file>(
    ctxs: &mut HashMap<u32, EntryContext<'file>>,
    nt_ctx: &mut EntryLog,
    record: WpiRecord<'file>,
) {
    match record.payload {
        Payload::Start {
            entry_id,
            entry_name,
            entry_type,
            entry_metadata: _,
        } => {
            // strip the NT: prefix from the entry name
            let mut entry_name = entry_name.strip_prefix("NT:").unwrap_or(entry_name);
            // also strip any leading slashes
            while let Some(new) = entry_name.strip_prefix('/') {
                entry_name = new;
            }
            ctxs.insert(
                entry_id,
                EntryContext {
                    // NOTE: we _could_ have metadata if we start using it
                    // metadata: entry_metadata,
                    ty: entry_type,
                    name: entry_name,
                },
            );
        }
        Payload::Raw { entry_id, data } => {
            let Some(ctx) = ctxs.get(&entry_id) else {
                re_log::warn!("No context for entry id {entry_id}");
                return;
            };

            let key = EntityPath::from_file_path(Path::new(ctx.name));

            handle_data(ctx.ty, record.timestamp, key, data, nt_ctx);
        }
        _ => (),
    }
}

fn parse_and_log(
    settings: &ImporterSettings,
    tx: &crossbeam::channel::Sender<ImportedData>,
    filepath: &std::path::Path,
    contents: &[u8],
) -> Result<(), ImporterError> {
    if !WpiLogFile::is_wpilog(contents) {
        return Err(ImporterError::Incompatible(filepath.to_owned()));
    }

    let store_id = settings.opened_store_id_or_recommended();

    let _ = tx.send(ImportedData::LogMsg(
        WpiLogLoader::name(&WpiLogLoader),
        LogMsg::SetStoreInfo(SetStoreInfo {
            row_id: *RowId::new(),
            info: StoreInfo::new(store_id.clone(), StoreSource::Other("WpiLog".into())),
        }),
    ));

    let timeline = Timeline::new_duration("robotime");

    let contents = contents.to_vec();
    let tx = tx.clone();
    let settings = settings.clone();
    std::thread::Builder::new()
        .name("WpiLogFile::parse".into())
        .spawn(move || {
            let tx = tx;
            let settings = settings;
            let contents = contents;

            {
                let mut ctxs = HashMap::new();
                let mut nt_ctx = EntryLog::new();

                let (_, _log) = WpiLogFile::parse(contents.as_slice(), |record| {
                    fill_log(&mut ctxs, &mut nt_ctx, record);
                })
                .map_err(|e| {
                    re_log::error!("WPI DataLog file error: {e}");
                    ImporterError::Other(e.into())
                })
                .unwrap();

                for chunk in log_changes_to_chunks(
                    &store_id,
                    &settings
                        .application_id
                        .clone()
                        .unwrap_or_else(ApplicationId::random),
                    timeline,
                    &mut nt_ctx,
                ) {
                    tx.send(ImportedData::Chunk(
                        WpiLogLoader::name(&WpiLogLoader),
                        store_id.clone(),
                        chunk,
                    ))
                    .unwrap();
                }
            }

            re_log::info!("finished parsing WpiLog");
        })
        .with_context(|| "failed to spawn WpiLogFile parsing thread".to_owned())?;

    Ok(())
}
