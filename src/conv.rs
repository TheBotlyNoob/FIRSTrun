use nohash_hasher::IntMap;
use re_chunk::{Chunk, ChunkBuilder};
use re_log;
use re_log_types::{ApplicationId, EntityPath, StoreId, Timeline};

use crate::log::EntryLog;
mod components;

use components::{
    StructuredComponentKind, append_component, point_component, primitive_component,
    structured_component, structured_type,
};

pub fn log_changes_to_chunks(
    _store_id: &StoreId,
    _application_id: &ApplicationId,
    timeline: Timeline,
    log: &mut EntryLog,
) -> Vec<Chunk> {
    let mut entities = IntMap::<EntityPath, ChunkBuilder>::default();
    let mut type_cache = IntMap::<EntityPath, Option<String>>::default();

    for (key, timestamp, val) in log.get_changed() {
        re_log::info!("{key} changed");

        let parent = key.parent().unwrap_or_else(|| key.clone());
        let builder = || Chunk::builder(parent.clone());

        let ty = type_cache
            .entry(parent.clone())
            .or_insert_with(|| structured_type(log, &parent))
            .as_deref();

        let chunk = entities.entry(parent.clone()).or_insert_with(builder);

        if let Some(ty) = ty
            && StructuredComponentKind::from_type(ty).is_some()
        {
            if let Some((component, coordinates)) =
                point_component(log, &parent, &key, timestamp, ty)
            {
                append_component(
                    chunk,
                    builder,
                    timeline,
                    timestamp,
                    structured_component(component, &coordinates),
                );
            }
            continue;
        }

        if let Some(component) = primitive_component(&val) {
            append_component(chunk, builder, timeline, timestamp, component);
        }
    }
    entities
        .into_values()
        .map(|builder| builder.build().unwrap())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rerun::{
        ApplicationId, EntityPath, StoreId, Timeline,
        external::arrow::array::{Float64Array, StringArray},
        external::re_log_types::StoreKind,
    };

    use super::{StructuredComponentKind, log_changes_to_chunks, point_component, structured_type};
    use crate::{
        log::{EntryLog, Timestamp},
        values::EntryValue,
    };

    fn add_coordinate(log: &mut EntryLog, path: &EntityPath, timestamp: u64, value: f64) {
        log.add_entryvalue(
            path.clone(),
            Timestamp(timestamp),
            EntryValue::Arrow(Arc::new(Float64Array::from(vec![value]))),
        )
        .unwrap();
    }

    #[test]
    fn assembles_translation3d_from_latest_struct_fields() {
        let mut log = EntryLog::new();
        let parent = EntityPath::from_single_string("translation");
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("x")),
            10,
            1.0,
        );
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("y")),
            10,
            2.0,
        );
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("z")),
            10,
            3.0,
        );
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("y")),
            20,
            4.0,
        );

        let (component, coordinates) = point_component(
            &log,
            &parent,
            &parent.join(&EntityPath::from_single_string("x")),
            Timestamp(15),
            "struct:Translation3d",
        )
        .unwrap();

        assert_eq!(component, StructuredComponentKind::Vector3D);
        assert_eq!(coordinates, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn only_x_emits_a_structured_component() {
        let mut log = EntryLog::new();
        let parent = EntityPath::from_single_string("point");
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("x")),
            10,
            1.0,
        );
        add_coordinate(
            &mut log,
            &parent.join(&EntityPath::from_single_string("y")),
            10,
            2.0,
        );

        assert!(
            point_component(
                &log,
                &parent,
                &parent.join(&EntityPath::from_single_string("y")),
                Timestamp(10),
                "struct:Point2d",
            )
            .is_none()
        );
        assert_eq!(
            StructuredComponentKind::from_type("struct:Point2d"),
            Some(StructuredComponentKind::Position2D)
        );
        assert_eq!(
            StructuredComponentKind::from_type("struct:Translation3d"),
            Some(StructuredComponentKind::Vector3D)
        );
        assert_eq!(StructuredComponentKind::from_type("double"), None);
    }

    #[test]
    fn emits_vector_component_for_struct_array_elements() {
        let mut log = EntryLog::new();
        let entity = EntityPath::from_single_string("Point3d");
        let element = entity.join(&EntityPath::from_single_string("0"));
        log.add_entryvalue(
            entity.join(&EntityPath::from_single_string(".type")),
            Timestamp(10),
            EntryValue::Arrow(Arc::new(StringArray::from(vec!["struct:Translation3d[]"]))),
        )
        .unwrap();
        add_coordinate(
            &mut log,
            &element.join(&EntityPath::from_single_string("x")),
            10,
            1.0,
        );
        add_coordinate(
            &mut log,
            &element.join(&EntityPath::from_single_string("y")),
            10,
            2.0,
        );
        add_coordinate(
            &mut log,
            &element.join(&EntityPath::from_single_string("z")),
            10,
            3.0,
        );

        assert_eq!(
            structured_type(&log, &element).as_deref(),
            Some("struct:Translation3d[]")
        );
        let chunks = log_changes_to_chunks(
            &StoreId::random(StoreKind::Recording, ApplicationId::random()),
            &ApplicationId::random(),
            Timeline::new_duration("robotime"),
            &mut log,
        );
        let chunk = chunks
            .iter()
            .find(|chunk| chunk.entity_path() == &element)
            .unwrap();
        assert!(
            chunk
                .components()
                .contains_component("rerun.components.Vector3D".into())
        );
        assert!(
            !chunk
                .components()
                .contains_component("rerun.components.Scalar".into())
        );
    }
}
