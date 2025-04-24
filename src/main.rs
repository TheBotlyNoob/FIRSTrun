//! This example demonstrates how to implement and register a [`re_data_loader::DataLoader`] into
//! the Rerun Viewer in order to add support for loading arbitrary files.
//!
//! Usage:
//! ```sh
//! $ cargo r -p custom_data_loader -- path/to/some/file
//! ```

pub mod wpilog;

use std::{collections::HashMap, path::Path};

use rerun::{
    DataLoader as _, EntityPath, LoadedData, TimePoint, Timeline,
    external::{anyhow, re_build_info, re_data_loader, re_log, re_log_types::NonMinI64},
    log::{Chunk, RowId},
    time::TimeInt,
};
use wpilog::{
    WpiLog,
    parse::{Payload, WpiLogFile},
};

fn main() -> anyhow::Result<std::process::ExitCode> {
    let main_thread_token = rerun::MainThreadToken::i_promise_i_am_on_the_main_thread();
    re_log::setup_logging();

    re_data_loader::register_custom_data_loader(HashLoader);

    let build_info = re_build_info::build_info!();
    rerun::run(
        main_thread_token,
        build_info,
        rerun::CallSource::Cli,
        std::env::args(),
    )
    .map(std::process::ExitCode::from)
}

// ---

/// A custom [`re_data_loader::DataLoader`] that logs the hash of file as a [`rerun::TextDocument`].
struct HashLoader;

impl re_data_loader::DataLoader for HashLoader {
    fn name(&self) -> String {
        "rerun.data_loaders.HashLoader".into()
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

fn parse_and_log(
    settings: &rerun::external::re_data_loader::DataLoaderSettings,
    tx: &std::sync::mpsc::Sender<re_data_loader::LoadedData>,
    filepath: &std::path::Path,
    contents: &[u8],
) -> Result<(), re_data_loader::DataLoaderError> {
    let timeline = Timeline::new_temporal("robotime");

    let (_, log) = WpiLogFile::parse(contents)
        .map_err(|e| re_data_loader::DataLoaderError::Other(e.into()))?;

    struct Entry {
        pub name: String,
        pub entry_type: String,
    }

    let mut ids = HashMap::<u32, Entry>::new();

    for record in log.records {
        let record = match record.payload {
            Payload::Start {
                entry_id,
                entry_name,
                entry_type,
                entry_metadata,
            } => {
                if !["string", "int64", "float", "double"].contains(&entry_type.as_str()) {
                    continue;
                }
                ids.entry(entry_id).or_insert(Entry {
                    name: entry_name,
                    entry_type,
                });
            }
            Payload::Raw { entry_id, data } => {
                let Some(entry) = ids.get(&entry_id) else {
                    continue;
                };

                let doc = rerun::TextDocument::new(entry.name.clone())
                    .with_media_type(rerun::MediaType::TEXT);

                let entity_path = EntityPath::from_file_path(Path::new(&entry.name));
                let chunk = Chunk::builder(entity_path)
                    .with_archetype(
                        RowId::new(),
                        TimePoint::from([(
                            timeline,
                            TimeInt::from_nanos(NonMinI64::new(record.timestamp as i64).unwrap()),
                        )]),
                        &doc,
                    )
                    .build()?;

                let store_id = settings
                    .opened_store_id
                    .clone()
                    .unwrap_or_else(|| settings.store_id.clone());
                let data = LoadedData::Chunk(HashLoader::name(&HashLoader), store_id, chunk);
                tx.send(data).ok();
                // TODO
            }
            _ => {
                continue;
            }
        };
    }

    Ok(())
}
