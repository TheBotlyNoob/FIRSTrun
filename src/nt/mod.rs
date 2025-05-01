use nt_client::{Client, NTAddr, NewClientOptions, subscribe::ReceivedMessage};
use rerun::external::re_log;

pub async fn begin_logging() {
    // TODO
    re_log::info!("Starting NetworkTables client");
}
