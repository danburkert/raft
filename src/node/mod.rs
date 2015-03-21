mod inner;

use std::{fmt, marker, thread};
use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io;
use std::sync::mpsc;

use capnp::{serialize_packed, ReaderOptions};

use node::inner::InnerNode;
use store::Store;
use Result;
use StateMachine;
use event::Event;


pub struct Node<S, P> where S: StateMachine {

    /// Thread in charge of listening to the TCP socket and sending events to
    /// the event handler thread.
    rpc_listener: thread::JoinHandle,

    /// Thread in charge of processing incoming events.
    event_loop: thread::JoinHandle,

    /// Shutdown hook
    event_channel: mpsc::SyncSender<Event>,

    local_addr: SocketAddr,

    _state_machine: marker::PhantomData<S>,

    _persistent_state: marker::PhantomData<P>,
}

impl <S, P> Node<S, P> where S: StateMachine, P: Store {

    /// Create a new Raft node.
    ///
    /// # Returns
    ///
    /// The bound socket address and the Raft node are returned. The Raft node will be shut down
    /// when dropped.
    pub fn new(address: SocketAddr,
               cluster_members: HashSet<SocketAddr>,
               state_machine: S,
               store: P)
           -> Result<Node<S, P>, S> {

        let (event_tx, event_rx) = mpsc::sync_channel::<Event>(128);

        let local_addr;
        let rpc_listener = {
            let socket = try!(TcpListener::bind(&address));
            local_addr = try!(socket.local_addr());
            let event_channel = event_tx.clone();

            thread::spawn(move || {
                for connection in socket.incoming() {
                    handle_rpc(connection, &event_channel);
                }
            })
        };

        let mut peers = cluster_members;
        peers.remove(&address);

        let event_loop = thread::spawn(move || {
            let mut node_machine = InnerNode::new(address, peers, store, state_machine);

            for event in event_rx.iter() {
                node_machine.apply(event);
            }
        });

        Ok(Node {
            rpc_listener: rpc_listener,
            event_loop: event_loop,
            event_channel: event_tx,
            local_addr: local_addr,
            _state_machine: marker::PhantomData,
            _persistent_state: marker::PhantomData,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

impl <S, P> fmt::Debug for Node<S, P> where S: StateMachine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format!("Node({})", self.local_addr).fmt(f)
    }
}

#[unsafe_destructor]
impl <S, P> Drop for Node<S, P> where S: StateMachine {
    fn drop(&mut self) {
        let _ = self.event_channel.send(Event::Shutdown);
    }
}

/// Deserializes a new RPC connection into a message, and sends that message to
/// the event channel. If the RPC fails with an IO error or the message cannot
/// be created, the error is logged. If the event channel is closed, a panic
/// is raised.
fn handle_rpc(stream: io::Result<TcpStream>, event_channel: &mpsc::SyncSender<Event>) {
    match stream {
        Ok(connection) => {
            let message = serialize_packed::new_reader_unbuffered(&connection, ReaderOptions::new());
            match message {
                Ok(message) => {
                    let event = Event::Rpc { message: message, connection: connection };
                    event_channel.send(event).ok().expect("Event channel closed. Listener thread stopping.")
                }
                Err(error) => { warn!("{}", error) }
            }
        },
        Err(error) => { warn!("{}", error) },
    }
}

#[cfg(test)]
mod test {

    use std::str::FromStr;
    use std::net::SocketAddr;

    use super::*;
    use state_machine::NullStateMachine;
    use store;

    #[test]
    fn test_node_creation() {
        let address = SocketAddr::from_str("127.0.0.1:0").unwrap();
        Node::new(address,
                  [address].iter().cloned().collect(),
                  NullStateMachine,
                  store::MemStore::new())
            .unwrap();
    }
}
