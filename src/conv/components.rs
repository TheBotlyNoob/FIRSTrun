use arrow::{
    array::{ArrayRef, AsArray, BooleanArray, StringArray},
    compute::cast,
    datatypes::{DataType, Utf8Type},
};
use re_chunk::{ChunkBuilder, RowId};
use re_log_types::{EntityPath, TimePoint, Timeline};
use re_sdk_types::components::{Blob, Position2D, Position3D, Scalar, Text, Vector2D, Vector3D};
use re_types_core::{ComponentDescriptor, FromArrow};

use crate::log::{EntryLog, Timestamp};

pub(super) fn structured_type(log: &EntryLog, parent: &EntityPath) -> Option<String> {
    let mut candidate = Some(parent.clone());
    while let Some(path) = candidate {
        let type_path = path.join(&EntityPath::from_single_string(".type"));
        if let Some((_, value)) = log.get_latest_entry(&type_path) {
            if let Some(values) = value.as_bytes_opt::<Utf8Type>() {
                if let Some(value) = values.iter().next().flatten() {
                    return Some(value.to_owned());
                }
            }
        }
        candidate = path.parent();
    }
    None
}

fn latest_number(log: &EntryLog, path: &EntityPath, timestamp: Timestamp) -> Option<f32> {
    let value = log.get_latest_from(path, timestamp)?.1;
    let value = cast(&**value, &DataType::Float64).ok()?;
    Some(
        value
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()?
            .value(0) as f32,
    )
}

pub(super) fn point_component(
    log: &EntryLog,
    parent: &EntityPath,
    key: &EntityPath,
    timestamp: Timestamp,
    ty: &str,
) -> Option<(StructuredComponentKind, Vec<f32>)> {
    if key.last()?.unescaped_str() != "x" {
        return None;
    }

    let ty = ty.to_ascii_lowercase();
    let component = StructuredComponentKind::from_type(&ty)?;
    let coordinates = ["x", "y", "z"]
        .into_iter()
        .take(component.dimensions())
        .map(|name| {
            latest_number(
                log,
                &parent.join(&EntityPath::from_single_string(name)),
                timestamp,
            )
        })
        .collect::<Option<Vec<_>>>()?;

    Some((component, coordinates))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StructuredComponentKind {
    Position2D,
    Position3D,
    Vector2D,
    Vector3D,
}

impl StructuredComponentKind {
    pub(super) fn from_type(ty: &str) -> Option<Self> {
        let ty = ty.to_ascii_lowercase();
        if ty.contains("point2d") {
            Some(Self::Position2D)
        } else if ty.contains("point3d") {
            Some(Self::Position3D)
        } else if ty.contains("translation2d") {
            Some(Self::Vector2D)
        } else if ty.contains("translation3d") {
            Some(Self::Vector3D)
        } else {
            None
        }
    }

    const fn dimensions(self) -> usize {
        match self {
            Self::Position2D | Self::Vector2D => 2,
            Self::Position3D | Self::Vector3D => 3,
        }
    }
}

pub(super) enum NativeComponent {
    Position2D([f32; 2]),
    Position3D([f32; 3]),
    Vector2D([f32; 2]),
    Vector3D([f32; 3]),
    Scalar(Vec<Scalar>),
    Text(Text),
    Blob(Vec<Blob>),
}

impl NativeComponent {
    pub(super) fn add_to(
        self,
        chunk: ChunkBuilder,
        row_id: RowId,
        timepoint: TimePoint,
    ) -> ChunkBuilder {
        match self {
            Self::Position2D(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Position2D"),
                    &[Position2D::from(value)],
                ),
            ),
            Self::Position3D(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Position3D"),
                    &[Position3D::from(value)],
                ),
            ),
            Self::Vector2D(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Vector2D"),
                    &[Vector2D::from(value)],
                ),
            ),
            Self::Vector3D(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Vector3D"),
                    &[Vector3D::from(value)],
                ),
            ),
            Self::Scalar(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Scalar"),
                    &value,
                ),
            ),
            Self::Text(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Text"),
                    &value,
                ),
            ),
            Self::Blob(value) => chunk.with_component_batch(
                row_id,
                timepoint,
                (
                    ComponentDescriptor::partial("rerun.components.Blob"),
                    &value,
                ),
            ),
        }
    }
}

pub(super) fn append_component(
    chunk: &mut ChunkBuilder,
    builder: impl Fn() -> ChunkBuilder,
    timeline: Timeline,
    timestamp: Timestamp,
    component: NativeComponent,
) {
    replace_with::replace_with(chunk, builder, |chunk| {
        component.add_to(
            chunk,
            RowId::new(),
            TimePoint::default().with(timeline, timestamp),
        )
    });
}

pub(super) fn structured_component(
    kind: StructuredComponentKind,
    coordinates: &[f32],
) -> NativeComponent {
    match kind {
        StructuredComponentKind::Position2D => {
            NativeComponent::Position2D([coordinates[0], coordinates[1]])
        }
        StructuredComponentKind::Position3D => {
            NativeComponent::Position3D([coordinates[0], coordinates[1], coordinates[2]])
        }
        StructuredComponentKind::Vector2D => {
            NativeComponent::Vector2D([coordinates[0], coordinates[1]])
        }
        StructuredComponentKind::Vector3D => {
            NativeComponent::Vector3D([coordinates[0], coordinates[1], coordinates[2]])
        }
    }
}

pub(super) fn primitive_component(value: &ArrayRef) -> Option<NativeComponent> {
    match value.data_type() {
        DataType::Boolean => {
            let value = value.as_any().downcast_ref::<BooleanArray>()?.value(0);
            Some(NativeComponent::Text(Text::from(if value {
                "true"
            } else {
                "false"
            })))
        }
        DataType::Utf8 => Some(NativeComponent::Text(Text::from(
            value.as_any().downcast_ref::<StringArray>()?.value(0),
        ))),
        DataType::Binary => Some(NativeComponent::Blob(Blob::from_arrow(value).ok()?)),
        datatype if datatype.is_numeric() => Some(NativeComponent::Scalar(
            Scalar::from_arrow(&*cast(value, &DataType::Float64).ok()?).ok()?,
        )),
        _ => None,
    }
}
