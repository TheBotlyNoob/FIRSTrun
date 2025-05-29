use std::{collections::BTreeMap, num::TryFromIntError, path::Path, sync::Arc};

use hashbrown::{HashMap, HashSet};
use rerun::{
    EntityPath,
    external::{
        anyhow,
        arrow::array::{ArrayRef, Int64Array},
        nohash_hasher::IntMap,
        re_log,
        re_log_types::NonMinI64,
    },
    time::TimeInt,
};

use crate::values::{
    EntryValue, EntryValueParseError,
    parse::wpistruct::{UnresolvedWpiLibStructType, WpiLibStructSchema, WpiLibStructType},
};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// The timestamp of an entry in the log.
///
/// Measured in microseconds since the RIO was enabled.
pub struct Timestamp(pub u64);

impl TryInto<TimeInt> for Timestamp {
    type Error = TryFromIntError;
    fn try_into(self) -> Result<TimeInt, Self::Error> {
        Ok(TimeInt::from_nanos(
            NonMinI64::new((self.0 * 1000).try_into()?).unwrap_or_default(),
        ))
    }
}

pub struct EntryLog {
    entries: IntMap<EntityPath, BTreeMap<Timestamp, ArrayRef>>,
    changed: HashSet<(EntityPath, Timestamp)>,
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
            changed: HashSet::new(),
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

                self.changed.insert((key, timestamp));
            }
            EntryValue::StructSchema(s) => {
                let name = key.last().map_or("struct:Unknown", |s| s.unescaped_str());
                self.add_struct(name, s);

                re_log::info!("new struct schema {name} at {}", timestamp.0);

                if let Some(queued) = self.queued_structs.remove(name) {
                    for (key, timestamp, ty, data) in queued {
                        re_log::info!("unqueued struct {name} for {key} at {}", timestamp.0);
                        self.add_entry(key, timestamp, &ty, &data).unwrap();
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
                    )
                    .unwrap();
                }
            }

            EntryValue::ArrayMap(m) => {
                let count = m.len();
                self.handle_array(key, timestamp, m.into_iter().map(EntryValue::Map), count)?;
            }
            EntryValue::ArrayArrow(a) => {
                let count = a.len();
                self.handle_array(key, timestamp, a.into_iter().map(EntryValue::Arrow), count)?;
            }
        }

        Ok(())
    }

    fn handle_array(
        &mut self,
        path: EntityPath,
        timestamp: Timestamp,
        arr: impl Iterator<Item = EntryValue>,
        count: usize,
    ) -> Result<(), anyhow::Error> {
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
        self.changed
            .drain()
            .filter_map(|(key, time)| {
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
