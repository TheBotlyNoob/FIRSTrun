use rerun::{
    ApplicationId, EntityPath, FromArrow, StoreId, TimePoint, Timeline,
    components::Scalar,
    external::{
        arrow::{
            array::AsArray,
            compute::cast,
            datatypes::{DataType, Utf8Type},
        },
        nohash_hasher::IntMap,
        re_chunk::ChunkBuilder,
        re_log,
    },
    log::{Chunk, RowId},
};

use crate::log::EntryLog;

pub fn log_changes_to_chunks(
    _store_id: &StoreId,
    _application_id: &ApplicationId,
    timeline: Timeline,
    log: &mut EntryLog,
) -> Vec<Chunk> {
    let mut entities = IntMap::<EntityPath, ChunkBuilder>::default();

    for (key, timestamp, val) in log.get_changed() {
        re_log::info!("{key} changed");

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

        let chunk = entities.entry(parent.clone()).or_insert_with(builder);

        match (ty, components) {
            // (Some(ty), Some(components)) if ty.iter().next().unwrap().unwrap() == "Entity" => {
            //     re_log::info!("Skipping entity entry: {}; {:#?}", key, components);
            //     for component in components.iter().flatten() {
            //         let component = match retrieve_component(log, timestamp, &parent, component) {
            //             Ok(c) => c,
            //             Err(e) => {
            //                 re_log::error!("error retrieving component: {e}");
            //                 continue;
            //             }
            //         };
            //         replace_with::replace_with(chunk, builder, |c| {
            //             c.with_component_batch(
            //                 RowId::new(),
            //                 TimePoint::default().with(timeline, timestamp),
            //                 &*component,
            //             )
            //         });
            //     }
            // }
            _ => {
                // not a component

                let datatype = val.data_type();
                let adhoc_component = if datatype.is_numeric() {
                    Scalar::from_arrow(&*cast(&*val, &DataType::Float64).unwrap()).unwrap()
                } else {
                    continue;
                };

                replace_with::replace_with(chunk, builder, |c| {
                    c.with_component_batch(
                        RowId::new(),
                        TimePoint::default().with(timeline, timestamp),
                        (
                            rerun::ComponentDescriptor::partial("rerun.components.Scalar"),
                            &adhoc_component,
                        ),
                    )
                });
            }
        }
    }
    entities
        .into_values()
        .map(|builder| builder.build().unwrap())
        .collect()
}
