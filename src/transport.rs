use std::net::{SocketAddr, TcpStream};
use std::sync::mpsc;

use threadpool::ThreadPool;

use capnp::{
    self,
    serialize_packed,
    MallocMessageBuilder,
    OwnedSpaceMessageReader,
    ReaderOptions,
};

pub trait Transport {

    /// Send a message to a remote peer. The message will be delivered asynchronously. If sending
    /// the message fails, then it will be automatically retried.
    fn send(&self,
            peer: SocketAddr,
            message: MallocMessageBuilder)
            -> mpsc::Receiver<OwnedSpaceMessageReader>;
}

pub struct TcpTransport {
    threads: ThreadPool,
}

impl TcpTransport {

    fn new() -> TcpTransport {
        TcpTransport {
            threads: ThreadPool::new(16),
        }
    }
}

impl Transport for TcpTransport {

    fn send(&self,
            peer: SocketAddr,
            mut message: MallocMessageBuilder)
            -> mpsc::Receiver<OwnedSpaceMessageReader> {

        let (send, recv) = mpsc::channel::<OwnedSpaceMessageReader>();

        self.threads.execute(move || {
            loop {
                match send_tcp_rpc(&peer, &mut message) {
                    Ok(reader) => {
                        let _ = send.send(reader);
                        break;
                    },
                    Err(error) => warn!("Retrying RPC to peer {}. Failure cause: {}", peer, error),
                }
            }
        });
        recv
    }
}

/// Send a message to a peer via TCP, and wait for a response. This method will block the caller.
fn send_tcp_rpc(peer: &SocketAddr,
                message: &mut MallocMessageBuilder)
                -> capnp::Result<OwnedSpaceMessageReader> {
    let mut cxn = try!(TcpStream::connect(peer));
    try!(serialize_packed::write_packed_message_unbuffered(&mut cxn, message));
    serialize_packed::new_reader_unbuffered(&cxn, ReaderOptions::new())
}

#[cfg(test)]
mod test_tcp {

    use std::net::TcpListener;
    use super::{Transport, TcpTransport};

    use capnp::{
        serialize_packed,
        BuilderOptions,
        MallocMessageBuilder,
        MessageBuilder,
        MessageReader,
        ReaderOptions,
    };

    use messages_capnp::{
        append_entries_response,
        append_entries_request,
    };

    fn get_append_entries_request() -> MallocMessageBuilder {
        let mut msg = MallocMessageBuilder::new(BuilderOptions::new());
        {
            let mut req = msg.init_root::<append_entries_request::Builder>();
            req.set_term(42);
            req.set_leader("some-leader");
            req.set_prev_log_index(43);
            req.set_prev_log_term(44);
            req.set_leader_commit(45);
            req.init_entries(0);
        }
        msg
    }

    fn get_append_entries_response() -> MallocMessageBuilder {
        let mut msg = MallocMessageBuilder::new(BuilderOptions::new());
        {
            let mut resp = msg.init_root::<append_entries_response::Builder>();
            resp.set_success(());
        }
        msg
    }

    #[test]
    fn send() {
        let _ = ::env_logger::init();

        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let req_msg = get_append_entries_request();
        let mut resp_msg = get_append_entries_response();

        let transport = TcpTransport::new();

        let resp_chan = transport.send(listener_addr, req_msg);
        let (mut cxn, _) = listener.accept().unwrap();

        let req_reader = serialize_packed::new_reader_unbuffered(&cxn, ReaderOptions::new()).unwrap();
        serialize_packed::write_packed_message_unbuffered(&mut cxn, &mut resp_msg).unwrap();

        let resp = resp_chan.recv().unwrap();

        { // Check request
            let req = req_reader.get_root::<append_entries_request::Reader>().unwrap();
            assert_eq!(42, req.get_term());
            assert_eq!("some-leader", req.get_leader().unwrap());
            assert_eq!(43, req.get_prev_log_index());
            assert_eq!(44, req.get_prev_log_term());
            assert_eq!(45, req.get_leader_commit());
            assert_eq!(0, req.get_entries().unwrap().len());
        }

        { // Check response
            match resp.get_root::<append_entries_response::Reader>()
                      .unwrap()
                      .which()
                      .unwrap() {

                append_entries_response::Which::Success(_) => (),
                _ => panic!("unexpected response"),
            }
        }
    }

    #[test]
    fn retry() {
        let _ = ::env_logger::init();

        let listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let req_msg = get_append_entries_request();
        let mut resp_msg = get_append_entries_response();

        let transport = TcpTransport::new();

        let resp_chan = transport.send(listener_addr, req_msg);
        { // Accept connection and drop it
            let _ = listener.accept().unwrap();
        }

        // Accept another connection
        let (mut cxn, _) = listener.accept().unwrap();

        let req_reader = serialize_packed::new_reader_unbuffered(&cxn, ReaderOptions::new()).unwrap();
        serialize_packed::write_packed_message_unbuffered(&mut cxn, &mut resp_msg).unwrap();

        let resp = resp_chan.recv().unwrap();

        { // Check request
            let req = req_reader.get_root::<append_entries_request::Reader>().unwrap();
            assert_eq!(42, req.get_term());
            assert_eq!("some-leader", req.get_leader().unwrap());
            assert_eq!(43, req.get_prev_log_index());
            assert_eq!(44, req.get_prev_log_term());
            assert_eq!(45, req.get_leader_commit());
            assert_eq!(0, req.get_entries().unwrap().len());
        }

        { // Check response
            match resp.get_root::<append_entries_response::Reader>()
                      .unwrap()
                      .which()
                      .unwrap() {

                append_entries_response::Which::Success(_) => (),
                _ => panic!("unexpected response"),
            }
        }
    }
}
