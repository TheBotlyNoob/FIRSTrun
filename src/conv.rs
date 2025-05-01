use hashbrown::HashMap;
use rerun::{
    ApplicationId, AsComponents, ComponentBatch, EntityPath, Scalars, StoreId, TimePoint, Timeline,
    external::{
        anyhow::{self, bail},
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
    log: &mut EntryLog,
    timestamp: Timestamp,
    parent: &Key,
    component: &str,
) -> Result<impl ComponentBatch + std::fmt::Debug, anyhow::Error> {
    let key = parent.join_str(component);
    if component != "Scalar" {
        bail!("wrong component type");
    }

    let Some(EntryValue::Int64Array(vals)) =
        dbg!(log.get_latest_from(&key, timestamp).map(|(_, t)| t.clone()))
    else {
        bail!("couldn't find latest value");
    };

    Ok(vals
        .into_iter()
        .map(|i| rerun::components::Scalar::from(i as f64))
        .collect::<Vec<rerun::components::Scalar>>())
}

pub fn log_changes_to_chunks(
    store_id: &StoreId,
    application_id: &ApplicationId,
    timeline: Timeline,
    log: &mut EntryLog,
) -> Vec<Chunk> {
    let mut entities = HashMap::<Key, ChunkBuilder>::new();

    for (key, timestamp, val) in log.get_changed() {
        let builder = || Chunk::builder(key.clone().into());

        let parent = key.parent();
        match (
            log.get_latest_entry(&parent.join_str(".type"))
                .map(|(_, t)| t.clone()),
            log.get_latest_entry(&parent.join_str(".components"))
                .map(|(_, t)| t.clone()),
        ) {
            (Some(EntryValue::String(s)), Some(EntryValue::StringArray(components)))
                if s == "Entity" =>
            {
                let mut chunk = entities.entry(parent.clone()).or_insert_with(builder);

                re_log::info!("Skipping entity entry: {}; {:#?}", key.0, components);
                for component in components {
                    let component = match retrieve_component(log, timestamp, &parent, &component) {
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
