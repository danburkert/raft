use std::sync::mpsc;

/// The messenger sends messages to the other Raft nodes in the cluster.
///
/// All messages are sent asynchronously. Messages that do not complete will be retried
/// indefinitely.
///
struct Messenger {
    // threadpool
    // socket
    //

    response_channel: mpsc::SyncSender<u8>,

}

impl Messenger {

}
