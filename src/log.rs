use std::{collections::BTreeMap, num::TryFromIntError, path::Path};

use hashbrown::{HashMap, HashSet};
use rerun::{
    EntityPath,
    external::{arrow::array::ArrayRef, nohash_hasher::IntMap, re_log_types::NonMinI64},
    time::TimeInt,
};

use crate::values::{
    EntryValue,
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
        }
    }

    pub fn add_struct(
        &mut self,
        name: impl Into<String>,
        s: WpiLibStructSchema<UnresolvedWpiLibStructType>,
    ) {
        self.struct_map.insert(name.into(), s);
    }

    pub fn resolve_struct(&self, name: &str) -> Option<WpiLibStructSchema<WpiLibStructType>> {
        self.struct_map.get(name)?.resolve(&self.struct_map)
    }

    pub fn add_entry(
        &mut self,
        key: EntityPath,
        timestamp: Timestamp,
        value: EntryValue,
    ) -> Result<(), String> {
        match value {
            EntryValue::Arrow(array) => {
                let entry = self.entries.entry(key.clone()).or_default();
                entry.insert(timestamp, array);

                self.changed.insert((key, timestamp));
            }
            // treat maps transparently as a set of entries
            EntryValue::Map(map) => {
                for (k, v) in map {
                    self.add_entry(
                        key.join(&EntityPath::from_file_path(Path::new(&k))),
                        timestamp,
                        v,
                    )?;
                }
            }
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
