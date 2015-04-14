use std::sync::mpsc;
use std::net::{SocketAddr, TcpStream, Shutdown};
use std::thread;
use std::io::ErrorKind;

use capnp::{self, serialize_packed};
use capnp::message::{MallocMessageBuilder, ReaderOptions};
use capnp::serialize::OwnedSpaceMessageReader;

/// Creates a new connection to the peer.
///
/// Messages may be sent to the peer using the returned channel. Response messages will be added
/// to `reply_chan`.
pub fn create_peer_connection(peer: SocketAddr,
                              reply_chan: mpsc::SyncSender<OwnedSpaceMessageReader>)
                              -> mpsc::SyncSender<MallocMessageBuilder> {
    let (send, recv) = mpsc::sync_channel(16);

    thread::Builder::new().name(format!("recv({})", peer)).spawn(move || {
        // setup connection to the peer
        let recv_stream = TcpStream::connect(peer)
                                   .map_err(|err| error!("unable to open connection to peer {}: {}", peer, err))
                                   .unwrap();

        let send_stream = recv_stream.try_clone()
                                     .map_err(|err| error!("unable to clone connection to peer {}: {}", peer, err))
                                     .unwrap();

        // spawn a seperate thread to handle sending messages to the peer
        thread::Builder::new()
                       .name(format!("send({})", peer))
                       .spawn(move || connection_send(peer, send_stream, recv));

        connection_recv(peer, recv_stream, reply_chan);
    });

    send
}

fn connection_send(peer: SocketAddr,
                   mut stream: TcpStream,
                   messages: mpsc::Receiver<MallocMessageBuilder>) {

    for mut msg in messages.iter() {
        match serialize_packed::write_packed_message_unbuffered(&mut stream, &mut msg) {
            Err(err) => warn!("send({}): unable to send message: {}", peer, err),
            _ => (),
        }
    }
    debug!("send({}): closing connection to peer", peer);
    let _ = stream.shutdown(Shutdown::Both);
}

fn connection_recv(peer: SocketAddr,
                   mut stream: TcpStream,
                   mut reply_chan: mpsc::SyncSender<OwnedSpaceMessageReader>) {
    loop {
        match serialize_packed::new_reader_unbuffered(&mut stream, ReaderOptions::new()) {
            Ok(msg) => match reply_chan.send(msg) {
                Ok(_) => (),
                Err(err) => {
                    debug!("recv({}): reply channel is closed. shutting down...", peer);
                    stream.shutdown(Shutdown::Both);
                    break;
                }

            },
            Err(err @ capnp::Error::Decode { .. }) => {
                warn!("recv({}): unable to decode message from peer: {}", peer, err);
            },
            Err(capnp::Error::Io(err)) => match err.kind() {
                ErrorKind::NotConnected => {
                    debug!("recv({}): connection to peer closed", peer);
                    break;
                },
                ErrorKind::ConnectionReset => {
                    debug!("recv({}): connection closed by peer", peer);
                    break;
                },
                _ => {
                    let msg = format!("recv({}): unable to receive message from peer: {}", peer, err);
                    warn!("{}", msg);
                },
            },
        }
    }
}

#[cfg(test)]
mod test {

    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use env_logger;
    use capnp::message::{
        MessageBuilder,
        MessageReader,
        MallocMessageBuilder,
        ReaderOptions
    };
    use capnp::serialize::OwnedSpaceMessageReader;
    use capnp::serialize_packed;

    use super::*;
    use messages_capnp::{
        request_vote_request,
    };

    fn create_message() -> MallocMessageBuilder {
        let mut msg = MallocMessageBuilder::new_default();
        {
            let mut builder = msg.init_root::<request_vote_request::Builder>();
            builder.set_term(1);
            builder.set_last_log_index(2);
            builder.set_last_log_term(3);
        }
        msg
    }

    fn compare_message(reader: request_vote_request::Reader) {
        let mut expected_msg = create_message();
        let expected = expected_msg.get_root::<request_vote_request::Builder>()
                                   .unwrap()
                                   .as_reader();
        assert_eq!(expected.get_term(), reader.get_term());
        assert_eq!(expected.get_last_log_index(), reader.get_last_log_index());
        assert_eq!(expected.get_last_log_term(), reader.get_last_log_term());
    }

    #[test]
    fn test_connection() {
        let _ = env_logger::init();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let (cxn_send, cxn_recv) = {
            let (send, recv) = mpsc::sync_channel(16);
            (create_peer_connection(listener.local_addr().unwrap(), send), recv)
        };

        let (mut stream, _) = listener.accept().unwrap();

        // send a message to the connection
        cxn_send.send(create_message()).unwrap();

        // make sure the received message matches
        let received = serialize_packed::new_reader_unbuffered(&mut stream, ReaderOptions::new())
                                       .unwrap();
        compare_message(received.get_root::<request_vote_request::Reader>().unwrap());

        // respond with the same message
        serialize_packed::write_packed_message_unbuffered(&mut stream, &mut create_message())
                        .unwrap();

        // make sure the response matches
        let response = cxn_recv.recv().unwrap();
        compare_message(response.get_root::<request_vote_request::Reader>().unwrap());
    }

    // The connection threads should automatically shut themselves down if the peer closes the
    // connection.
    #[test]
    fn test_peer_shutdown() {
        let _ = env_logger::init();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let (cxn_send, cxn_recv) = {
            let (send, recv) = mpsc::sync_channel(16);
            (create_peer_connection(listener.local_addr().unwrap(), send), recv)
        };

        {
            // momentarily open the connection
            let (stream, _) = listener.accept().unwrap();
        }

        // send a message to the connection
        cxn_send.send(create_message()).unwrap();
    }
}
