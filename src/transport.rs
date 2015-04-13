use std::collection::{HashMap, HashSet};
use std::sync::mpsc;
use std::net::{SocketAddr, TcpStream};
use std::thread;

use capnp::message::MallocMessageBuilder;

struct Transport {
    streams: HashMap<SocketAddr, mpsc::SyncSender<MallocMessageBuilder>>,
}

impl Transport {

    fn new(peers: &HashSet<SocketAddr>,
           event_loop: mpsc::SyncSender<MallocMessageBuilder>)
           -> Transport {




        let streams = peers.map(|peer| {
            let (send, recv) = mpsc::sync_channel(16);

            thread::spawn(move || {
                let stream = TcpStream::connect(addr).unwrap();
            })

        })
        .collect::<HashMap<_>>();


        let mut wait_ms: u32 = 2;







        Transport { send: send }
   }

    fn send(peer: &SocketAddr, msg: MallocMessageBuilder) {


    }

    fn broadcast(msg: MallocMessageBuilder) {
    }
}

fn new_connection(peer: SocketAddr,
                  event_loop: mpsc::SyncSender<MallocMessageBuilder>)
                  -> mpsc::SyncSender<MallocMessageBuilder> {
    let (send, recv) = mpsc::sync_channel(16);

    thread::spawn(move || {
        let mut stream = TcpStream::connect(addr).unwrap();
        let mut listener_stream = stream.try_clone().unwrap();

        thread::spawn(move || {
            listener_stream.iter()


        });

    });

    send
}

struct Connection {
    stream: TcpStream,
    send: mpsc::SyncSender<MallocMessageBuilder>,
}


impl Connection {

    fn new(addr: SocketAddr,
           recv: mpsc::SyncSender<MallocMessageBuilder>)
           -> Connection {

        
    }
}
