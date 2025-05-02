use rerun::external::egui::ahash::HashMap;

#[repr(transparent)]
#[derive(Clone, Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct Topic(String);

pub enum NetworkTablesMessage {
    Updated(Topic),
}
