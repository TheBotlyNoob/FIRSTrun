use hashbrown::HashMap;
use rerun::{
    ApplicationId, AsComponents, ComponentBatch, EntityPath, Loggable, Scalars, StoreId, TimePoint,
    Timeline,
    external::{
        anyhow::{self, bail},
        arrow::{
            self,
            array::{AsArray, Float64Array},
            datatypes::{DataType, Float64Type, Utf8Type},
        },
        re_chunk::ChunkBuilder,
        re_log,
    },
    log::{Chunk, ChunkId, RowId},
};

use crate::{
    log::{EntryLog, Key, Timestamp},
    values::EntryValue,
};

fn retrieve_component(
    log: &EntryLog,
    timestamp: Timestamp,
    parent: &Key,
    component: &str,
) -> Result<impl ComponentBatch + std::fmt::Debug, anyhow::Error> {
    let key = parent.join_str(component);
    if component != "Scalar" {
        bail!("wrong component type");
    }
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
    let mut entities = HashMap::<Key, ChunkBuilder>::new();

    for (key, timestamp, _val) in log.get_changed() {
        let builder = || Chunk::builder(key.clone().into());

        let parent = key.parent();

        let ty = log
            .get_latest_entry(&parent.join_str(".type"))
            .map(|(_, t)| &**t)
            .and_then(|a| a.as_bytes_opt::<Utf8Type>());

        let components = log
            .get_latest_entry(&parent.join_str(".components"))
            .map(|(_, t)| t.clone());
        let components = components
            .as_ref()
            .and_then(|a| a.as_bytes_opt::<Utf8Type>());

        match (ty, components) {
            (Some(ty), Some(components)) if ty.iter().next().unwrap().unwrap() == "Entity" => {
                let mut chunk = entities.entry(parent.clone()).or_insert_with(builder);

                re_log::info!("Skipping entity entry: {}; {:#?}", key.0, components);
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

    entities
        .into_iter()
        .map(|(_, builder)| builder.build().unwrap())
        .collect()
}
