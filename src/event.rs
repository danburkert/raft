use std::net::TcpStream;

use capnp::serialize::OwnedSpaceMessageReader;

pub enum Event {
    RpcRequest {
        message: OwnedSpaceMessageReader,
        connection: TcpStream,
    },
    RpcResponse {
        message: OwnedSpaceMessageReader,
    },
    Shutdown
}
