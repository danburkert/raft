use std::old_io::net::tcp::TcpStream;

use capnp::serialize::OwnedSpaceMessageReader;

pub enum Event {
    Rpc {
        message: OwnedSpaceMessageReader,
        connection: TcpStream,
    },
    RequestVoteResult {
        message: OwnedSpaceMessageReader,
    },
    AppendEntriesResult {
        message: OwnedSpaceMessageReader,
    },
    Shutdown
}
