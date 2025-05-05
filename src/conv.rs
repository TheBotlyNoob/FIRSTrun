use std::path::Path;

use rerun::{
    ApplicationId, ComponentBatch, EntityPath, Loggable, StoreId, TimePoint,
    Timeline,
    external::{
        anyhow::{self},
        arrow::{
            self,
            array::AsArray,
            datatypes::{DataType, Float64Type, Utf8Type},
        },
        nohash_hasher::IntMap,
        re_chunk::ChunkBuilder,
        re_log,
    },
    log::{Chunk, RowId},
};

use crate::log::{EntryLog, Timestamp};

fn retrieve_component(
    log: &EntryLog,
    timestamp: Timestamp,
    parent: &EntityPath,
    component: impl AsRef<Path>,
) -> Result<impl ComponentBatch + std::fmt::Debug, anyhow::Error> {
    let key = parent.join(&EntityPath::from_file_path(component.as_ref()));

    let array = arrow::compute::cast(
        &log.get_latest_from(&key, timestamp)
            .map(|(_, t)| t.clone())
            .ok_or_else(|| {
                anyhow::anyhow!("couldn't find latest value for {key} at {timestamp:?}")
            })?,
        &DataType::Float64,
    )?;

    Ok(rerun::components::Scalar::from_arrow(
        array.as_primitive::<Float64Type>(),
    )?)
}

pub fn log_changes_to_chunks(
    store_id: &StoreId,
    application_id: &ApplicationId,
    timeline: Timeline,
    log: &mut EntryLog,
) -> Vec<Chunk> {
    let mut entities = IntMap::<EntityPath, ChunkBuilder>::default();

    for (key, timestamp, _val) in log.get_changed() {
        let builder = || Chunk::builder(key.clone());

        let parent = key.parent().unwrap_or_else(|| key.clone());

        let ty = log
            .get_latest_entry(&parent.join(&EntityPath::from_single_string(".type")))
            .map(|(_, t)| &**t)
            .and_then(|a| a.as_bytes_opt::<Utf8Type>());

        let components = log
            .get_latest_entry(&parent.join(&EntityPath::from_single_string(".components")))
            .map(|(_, t)| t.clone());
        let components = components
            .as_ref()
            .and_then(|a| a.as_bytes_opt::<Utf8Type>());

        match (ty, components) {
            (Some(ty), Some(components)) if ty.iter().next().unwrap().unwrap() == "Entity" => {
                let chunk = entities.entry(parent.clone()).or_insert_with(builder);

                re_log::info!("Skipping entity entry: {}; {:#?}", key, components);
                for component in components.iter().flatten() {
                    let component = match retrieve_component(log, timestamp, &parent, component) {
                        Ok(c) => c,
                        Err(e) => {
                            re_log::error!("error retrieving component: {e}");
                            continue;
                        }
                    };
                    dbg!(&component);
                    replace_with::replace_with(chunk, builder, |c| {
                        c.with_component_batch(
                            RowId::new(),
                            TimePoint::default().with(timeline, timestamp),
                            &component,
                        )
                    });
                }
                continue;
            }
            _ => {
                // not an entity
            }
        }
    }

    entities.into_values().map(|builder| builder.build().unwrap())
        .collect()
}
