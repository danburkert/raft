use std::{io, thread};
use std::clone::Clone;
use std::collections::{hash_map, HashMap};
use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;
use std::error::FromError;

use threadpool::ThreadPool;

use capnp::{
    self,
    serialize_packed,
    MallocMessageBuilder,
    MessageBuilder,
    MessageReader,
    OwnedSpaceMessageReader,
    ReaderOptions,
};

pub trait Transport {

    /// Send a message to a remote peer. The message will be delivered asynchronously, and the
    /// result will be put on the returned channel.
    fn send(&mut self,
            peer: &SocketAddr,
            message: MallocMessageBuilder)
            -> io::Result<mpsc::Receiver<capnp::Result<OwnedSpaceMessageReader>>>;
}

pub struct TcpTransport {
    connections: HashMap<SocketAddr, TcpStream>,
    threads: ThreadPool,
    cxn_channel: mpsc::Receiver<TcpStream>,
    release_channel: mpsc::Sender<TcpStream>,
}

impl TcpTransport {

    /// Return all pending connections to the pool.
    fn return_connections(&mut self) {
        while let Ok(connection) = self.cxn_channel.try_recv() {
            if let Ok(peer) = connection.peer_addr() {
                match self.connections.entry(peer) {
                    hash_map::Entry::Occupied(_) => drop(connection),
                    hash_map::Entry::Vacant(entry) => {
                        entry.insert(connection);
                    },
                }
            }
        }
    }

    /// Borrow a connection from the connection pool, or if the connection pool does not contain a
    /// pooled connection to the peer, create a new one.
    fn get_connection(&mut self, peer: &SocketAddr) -> io::Result<TcpStream> {
        self.return_connections();
        self.connections.remove(peer)
                        .map(|cxn| Ok(cxn))
                        .unwrap_or_else(|| TcpStream::connect(peer))
    }
}

impl Transport for TcpTransport {

    fn send(&mut self,
            peer: &SocketAddr,
            mut message: MallocMessageBuilder)
            -> io::Result<mpsc::Receiver<capnp::Result<OwnedSpaceMessageReader>>> {
        let mut cxn = try!(self.get_connection(peer));
        let (send, recv) = mpsc::channel::<capnp::Result<OwnedSpaceMessageReader>>();

        let release_channel = self.release_channel.clone();

        thread::spawn(move || {
            match serialize_packed::write_packed_message_unbuffered(&mut cxn, &mut message) {
                Err(error) => {
                    // The TcpStream is possibly tainted, so don't return it to the pool
                    send.send(Err(FromError::from_error(error)));
                }
                _ => (),
            }

            let message_reader = serialize_packed::new_reader_unbuffered(&cxn, ReaderOptions::new());
        });

        Ok(recv)
    }
}
