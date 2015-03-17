use std::net::TcpStream;

use capnp::serialize::OwnedSpaceMessageReader;

pub enum Event {
    Rpc {
        message: OwnedSpaceMessageReader,
        connection: TcpStream,
    },
    RequestVoteResponse {
        message: OwnedSpaceMessageReader,
    },
    AppendEntriesResponse {
        message: OwnedSpaceMessageReader,
    },
    Shutdown
}
