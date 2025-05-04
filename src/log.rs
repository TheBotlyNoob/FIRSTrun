use std::{collections::BTreeMap, num::TryFromIntError};

use camino::{Utf8Path, Utf8PathBuf};
use hashbrown::{HashMap, HashSet};
use rerun::{
    EntityPath,
    external::{arrow::array::ArrayRef, re_log_types::NonMinI64},
    time::TimeInt,
};
use uom::si::{time::nanosecond, u64::Time};

use crate::values::EntryValue;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// The timestamp of an entry in the log.
pub struct Timestamp(pub Time);

impl TryInto<TimeInt> for Timestamp {
    type Error = TryFromIntError;
    fn try_into(self) -> Result<TimeInt, Self::Error> {
        Ok(TimeInt::from_nanos(
            NonMinI64::new(self.0.get::<nanosecond>().try_into()?).unwrap_or(NonMinI64::default()),
        ))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Key(pub Utf8PathBuf);
impl Key {
    pub fn join(&self, other: &Utf8Path) -> Self {
        Self(self.0.join(other))
    }
    pub fn join_str(&self, other: &str) -> Self {
        Self(self.0.join(other))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn parent(&self) -> Self {
        Self(self.0.parent().unwrap_or(&self.0).to_path_buf())
    }
}
impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<EntityPath> for Key {
    fn into(self) -> EntityPath {
        EntityPath::from_file_path(self.0.as_path().as_std_path())
    }
}

pub struct EntryLog {
    entries: HashMap<Key, BTreeMap<Timestamp, ArrayRef>>,
    changed: HashSet<(Key, Timestamp)>,
}

impl EntryLog {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            changed: HashSet::new(),
        }
    }

    pub fn add_entry(
        &mut self,
        key: Key,
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
                    self.add_entry(key.join(Utf8Path::new(&k)), timestamp, v)?;
                }
            }
        }

        Ok(())
    }

    /// Gets the changed entries with their values and clears the changed set.
    pub fn get_changed(&mut self) -> Vec<(Key, Timestamp, ArrayRef)> {
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

    pub fn get_entry(&self, key: &Key) -> Option<&BTreeMap<Timestamp, ArrayRef>> {
        self.entries.get(key)
    }

    pub fn get_latest_entry(&self, key: &Key) -> Option<(&Timestamp, &ArrayRef)> {
        self.entries.get(key).and_then(BTreeMap::last_key_value)
    }
    pub fn get_latest_from(&self, key: &Key, time: Timestamp) -> Option<(&Timestamp, &ArrayRef)> {
        self.entries
            .get(key)
            .and_then(|entry| entry.range(..=time).last())
    }
}
