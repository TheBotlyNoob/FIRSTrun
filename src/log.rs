use std::{
    collections::{BTreeMap, BTreeSet},
    num::TryFromIntError,
    path::Path,
    sync::Arc,
};

use arrow::array::{ArrayRef, Int64Array};
use hashbrown::HashMap;
use nohash_hasher::IntMap;
use re_log;
use re_log_types::{EntityPath, NonMinI64, TimeInt};

use crate::values::{
    EntryValue, EntryValueParseError,
    parse::wpistruct::{UnresolvedWpiLibStructType, WpiLibStructSchema},
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// The timestamp of an entry in the log.
///
/// Measured in microseconds since the RIO was enabled.
pub struct Timestamp(pub u64);

impl TryInto<TimeInt> for Timestamp {
    type Error = TryFromIntError;
    fn try_into(self) -> Result<TimeInt, Self::Error> {
        let nanos: i64 = (i128::from(self.0) * 1000).try_into()?;
        Ok(TimeInt::from_nanos(
            NonMinI64::new(nanos).unwrap_or_default(),
        ))
    }
}

pub struct EntryLog {
    entries: IntMap<EntityPath, BTreeMap<Timestamp, ArrayRef>>,
    changed: BTreeSet<(Timestamp, EntityPath)>,
    struct_map: HashMap<String, WpiLibStructSchema<UnresolvedWpiLibStructType>>,
    pub queued_structs: HashMap<String, Vec<(EntityPath, Timestamp, String, Vec<u8>)>>,
}

impl Default for EntryLog {
    fn default() -> Self {
        Self::new()
    }
}

impl EntryLog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: IntMap::default(),
            changed: BTreeSet::new(),
            struct_map: HashMap::new(),
            queued_structs: HashMap::new(),
        }
    }

    pub fn add_struct(
        &mut self,
        name: impl Into<String>,
        s: WpiLibStructSchema<UnresolvedWpiLibStructType>,
    ) {
        self.struct_map.insert(name.into(), s);
    }

    pub fn add_entry(
        &mut self,
        key: EntityPath,
        timestamp: Timestamp,
        ty: &str,
        value: &[u8],
    ) -> Result<(), anyhow::Error> {
        match EntryValue::parse_from_wpilog(ty, value, &self.struct_map) {
            Ok(v) => self.add_entryvalue(key, timestamp, v),
            Err(EntryValueParseError::StructNotFound(s)) => {
                re_log::info!("struct not found: {s} for key {key} at {}", timestamp.0);
                self.queued_structs.entry(s).or_default().push((
                    key,
                    timestamp,
                    ty.into(),
                    value.to_vec(),
                ));

                Ok(())
            }
            Err(EntryValueParseError::Other(e)) => Err(e),
        }
    }

    pub fn add_entryvalue(
        &mut self,
        key: EntityPath,
        timestamp: Timestamp,
        value: EntryValue,
    ) -> Result<(), anyhow::Error> {
        match value {
            EntryValue::Arrow(array) => {
                let entry = self.entries.entry(key.clone()).or_default();
                entry.insert(timestamp, array);

                self.changed.insert((timestamp, key));
            }
            EntryValue::StructSchema(s) => {
                let name = key.last().map_or("struct:Unknown", |s| s.unescaped_str());
                self.add_struct(name, s);

                re_log::info!("new struct schema {name} at {}", timestamp.0);

                if let Some(queued) = self.queued_structs.remove(name) {
                    for (key, timestamp, ty, data) in queued {
                        re_log::info!("unqueued struct {name} for {key} at {}", timestamp.0);
                        self.add_entry(key, timestamp, &ty, &data)?;
                    }
                }
            }
            // treat maps transparently as a set of entries
            EntryValue::Map(map) => {
                for (k, v) in map {
                    self.add_entryvalue(
                        key.join(&EntityPath::from_file_path(Path::new(&k))),
                        timestamp,
                        v,
                    )?;
                }
            }

            EntryValue::ArrayMap(m) => {
                self.handle_array(&key, timestamp, m.into_iter().map(EntryValue::Map))?;
            }
            EntryValue::ArrayArrow(a) => {
                self.handle_array(&key, timestamp, a.into_iter().map(EntryValue::Arrow))?;
            }
        }

        Ok(())
    }

    fn handle_array(
        &mut self,
        path: &EntityPath,
        timestamp: Timestamp,
        arr: impl ExactSizeIterator<Item = EntryValue>,
    ) -> Result<(), anyhow::Error> {
        let count = arr.len();
        self.add_entryvalue(
            path.join(&EntityPath::from_single_string("length")),
            timestamp,
            EntryValue::Arrow(Arc::new(Int64Array::from_iter_values([count as i64]))),
        )?;

        for (i, value) in arr.enumerate() {
            self.add_entryvalue(
                path.join(&EntityPath::from_single_string(i.to_string())),
                timestamp,
                value,
            )?;
        }

        Ok(())
    }

    /// Gets the changed entries with their values and clears the changed set.
    pub fn get_changed(&mut self) -> Vec<(EntityPath, Timestamp, ArrayRef)> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .filter_map(|(time, key)| {
                self.entries
                    .get(&key)
                    .and_then(|entry| entry.get(&time))
                    .map(|value| (key, time, value.clone()))
            })
            .collect()
    }

    #[must_use]
    pub fn get_entry(&self, key: &EntityPath) -> Option<&BTreeMap<Timestamp, ArrayRef>> {
        self.entries.get(key)
    }

    pub fn get_latest_entry(&self, key: &EntityPath) -> Option<(&Timestamp, &ArrayRef)> {
        self.entries.get(key).and_then(BTreeMap::last_key_value)
    }
    #[must_use]
    pub fn get_latest_from(
        &self,
        key: &EntityPath,
        time: Timestamp,
    ) -> Option<(&Timestamp, &ArrayRef)> {
        self.entries
            .get(key)
            .and_then(|entry| entry.range(..=time).last())
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryLog, Timestamp};
    use crate::values::parse::wpistruct::WpiLibStructSchema;
    use rerun::{
        EntityPath,
        external::arrow::array::{Float64Array, Int64Array},
    };

    #[test]
    fn keeps_primitive_arrays_as_one_changed_entry() {
        let mut log = EntryLog::new();
        let payload = [
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ];
        let key = EntityPath::from_single_string("values");

        log.add_entry(key.clone(), Timestamp(42), "int64[]", &payload)
            .unwrap();

        let changed = log.get_changed();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, key);
        let values = changed[0].2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn selects_the_latest_value_at_or_before_a_timestamp() {
        let mut log = EntryLog::new();
        let key = EntityPath::from_single_string("value");
        log.add_entry(key.clone(), Timestamp(10), "double", &1.0f64.to_le_bytes())
            .unwrap();
        log.add_entry(key.clone(), Timestamp(20), "double", &2.0f64.to_le_bytes())
            .unwrap();

        let (_, value) = log.get_latest_from(&key, Timestamp(15)).unwrap();
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );
        let (_, value) = log.get_latest_from(&key, Timestamp(20)).unwrap();
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
    }

    #[test]
    fn replacing_a_value_at_one_timestamp_creates_one_change() {
        let mut log = EntryLog::new();
        let key = EntityPath::from_single_string("value");
        log.add_entry(key.clone(), Timestamp(10), "double", &1.0f64.to_le_bytes())
            .unwrap();
        log.add_entry(key.clone(), Timestamp(10), "double", &2.0f64.to_le_bytes())
            .unwrap();

        let changed = log.get_changed();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, key);
        assert_eq!(
            changed[0]
                .2
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            2.0
        );
    }

    #[test]
    fn rejects_timestamps_that_do_not_fit_rerun_time() {
        let result: Result<rerun::time::TimeInt, _> = Timestamp(u64::MAX).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn returns_changes_in_timestamp_order() {
        let mut log = EntryLog::new();
        log.add_entry(
            EntityPath::from_single_string("later"),
            Timestamp(20),
            "int64",
            &2i64.to_le_bytes(),
        )
        .unwrap();
        log.add_entry(
            EntityPath::from_single_string("earlier"),
            Timestamp(10),
            "int64",
            &1i64.to_le_bytes(),
        )
        .unwrap();

        let changed = log.get_changed();
        assert_eq!(
            changed
                .iter()
                .map(|(_, time, _)| time.0)
                .collect::<Vec<_>>(),
            [10, 20]
        );
    }

    #[test]
    fn expands_struct_array_elements_under_stable_indices() {
        let mut log = EntryLog::new();
        log.add_struct(
            "struct:Translation2d",
            WpiLibStructSchema::parse(b"double x; double y;").unwrap(),
        );
        let payload = [
            1.0f64.to_le_bytes(),
            2.0f64.to_le_bytes(),
            3.0f64.to_le_bytes(),
            4.0f64.to_le_bytes(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let key = EntityPath::from_single_string("translations");

        log.add_entry(
            key.clone(),
            Timestamp(10),
            "struct:Translation2d[]",
            &payload,
        )
        .unwrap();

        for (index, values) in [(0, [1.0, 2.0]), (1, [3.0, 4.0])] {
            for (field, expected) in [("x", values[0]), ("y", values[1])] {
                let path = key
                    .join(&EntityPath::from_single_string(index.to_string()))
                    .join(&EntityPath::from_single_string(field));
                let (_, value) = log.get_latest_entry(&path).unwrap();
                assert_eq!(
                    value
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    expected
                );
            }
        }
        assert!(
            log.get_entry(&key.join(&EntityPath::from_single_string("length")))
                .is_some()
        );
    }
}
