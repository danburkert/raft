use std::collections::{hash_map, HashMap, HashSet};
use std::old_io::net::ip::SocketAddr;
use std::old_io::net::tcp::TcpStream;
use std::str::FromStr;

use capnp::serialize::OwnedSpaceMessageReader;
use capnp::{serialize_packed, MessageBuilder, MessageReader, ReaderOptions, MallocMessageBuilder};

use messages_capnp::{
    append_entries_response,
    append_entries_request,
    request_vote_response,
    request_vote_request,
    rpc,
};
use event::Event;
use LogIndex;
use Term;
use StateMachine;
use store::Store;

/// Nodes can be in one of three state:
///
/// * `Follower` - which replicates AppendEntries requests and votes for it's leader.
/// * `Leader` - which leads the cluster by serving incoming requests, ensuring
///              data is replicated, and issuing heartbeats.
/// * `Candidate` -  which campaigns in an election and may become a `Leader`
///                  (if it gets enough votes) or a `Follower`, if it hears from
///                  a `Leader`.
#[derive(Clone)]
enum NodeState {
    Follower,
    Candidate,
    Leader(LeaderState),
}

#[derive(Clone)]
struct LeaderState {
    last_index: LogIndex,
    next_index: HashMap<SocketAddr, LogIndex>,
    match_index: HashMap<SocketAddr, LogIndex>,
}

impl LeaderState {

    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `last_index` - The index of the leader's most recent log entry at the
    ///                  time of election.
    pub fn new(last_index: LogIndex) -> LeaderState {
        LeaderState {
            last_index: last_index,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Returns the next log entry index of the follower node.
    pub fn next_index(&mut self, node: SocketAddr) -> LogIndex {
        match self.next_index.entry(node) {
            hash_map::Entry::Occupied(entry) => *entry.get(),
            hash_map::Entry::Vacant(entry) => *entry.insert(self.last_index + 1),
        }
    }

    /// Sets the next log entry index of the follower node.
    pub fn set_next_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.next_index.insert(node, index);
    }

    /// Returns the index of the highest log entry known to be replicated on
    /// the follower node.
    pub fn match_index(&self, node: SocketAddr) -> LogIndex {
        *self.match_index.get(&node).unwrap_or(&LogIndex(0))
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower node.
    pub fn set_match_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.match_index.insert(node, index);
    }

    /// Counts the number of follower nodes containing the given log index.
    pub fn count_match_indexes(&self, index: LogIndex) -> usize {
        self.match_index.values().filter(|&&i| i >= index).count()
    }
}

/// The Raft state machine.
///
/// The inner node implements the core Raft logic. The inner node itself is a
/// state machine which responds to events according to the Raft protocol.
///
/// The inner node coordinates with other Raft nodes to apply client commands
/// to a state machine in a globally consistent order.
pub struct InnerNode<S, M> {
    address: SocketAddr,
    peers: HashSet<SocketAddr>,
    node_state: NodeState,
    commit_index: LogIndex,
    last_applied: LogIndex,
    store: S,
    state_machine: M,
}

impl <S, M> InnerNode<S, M> where S: Store, M: StateMachine {

    pub fn new(address: SocketAddr,
               peers: HashSet<SocketAddr>,
               store: S,
               state_machine: M)
               -> InnerNode<S, M> {
        InnerNode {
            address: address,
            peers: peers,
            node_state: NodeState::Follower,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            store: store,
            state_machine: state_machine,
        }
    }

    /// Apply a Raft event to this inner node.
    pub fn apply(&mut self, event: Event) {
        match event {
            Event::Rpc { message, connection } => self.apply_rpc(message, connection),
            Event::RequestVoteResult { message} => self.apply_request_vote_result(message),
            Event::AppendEntriesResult { message } => self.apply_append_entries_result(message),
            Event::Shutdown => self.shutdown()
        }
    }

    fn apply_rpc(&mut self, message: OwnedSpaceMessageReader, connection: TcpStream) {
        let rpc = message.get_root::<rpc::Reader>();
        match rpc.which() {
            Some(rpc::AppendEntries(request)) => self.apply_append_entries_request(request, connection),
            Some(rpc::RequestVote(request_vote_request)) => {

            },
            None => {
            }
        }
    }

    fn apply_append_entries_request(&mut self,
                                    request: append_entries_request::Reader,
                                    mut connection: TcpStream) {
        let mut response_message = MallocMessageBuilder::new_default();
        {
            let mut response = response_message.init_root::<append_entries_response::Builder>();

            let leader_term = Term(request.get_term());
            let current_term = self.store.current_term().unwrap(); // TODO: error handling

            if leader_term < current_term {
                response.set_stale_term(current_term.as_u64())
            } else {
                match self.node_state {
                    NodeState::Follower => {
                        let prev_log_index = LogIndex(request.get_prev_log_index());
                        let prev_log_term = Term(request.get_prev_log_term());

                        let local_latest_index = self.store.latest_index().unwrap(); // TODO: error handling
                        if local_latest_index < prev_log_index {
                            response.set_inconsistent_prev_entry(());
                        } else {
                            let (existing_term, _) = self.store.entry(prev_log_index).unwrap();
                            if existing_term != prev_log_term {
                                self.store.truncate_entries(prev_log_index);
                                response.set_inconsistent_prev_entry(());
                            } else {

                                let entries = request.get_entries();
                                let num_entries = entries.len();
                                if num_entries > 0 {
                                    let mut entries_vec = Vec::with_capacity(num_entries as usize);
                                    for i in 0..num_entries as u32 {
                                        entries_vec.push((leader_term, entries.get(i)));
                                    }

                                    self.store.append_entries(prev_log_index + 1, &entries_vec).unwrap(); // TODO: error handling
                                }

                                response.set_success(());
                            }
                        }
                    },
                    NodeState::Candidate | NodeState::Leader(..) => {
                        // recognize the new leader, return to follower state, and apply the entries
                        self.node_state = NodeState::Follower;
                        return self.apply_append_entries_request(request, connection)
                    },
                }
            }
        }

        serialize_packed::write_packed_message_unbuffered(&mut connection, &mut response_message).unwrap(); // TODO: error handling
    }

    fn apply_request_vote_request(&mut self,
                                  request: request_vote_request::Reader,
                                  mut connection: TcpStream) {
        let mut response_message = MallocMessageBuilder::new_default();
        {
            let mut response = response_message.init_root::<request_vote_response::Builder>();

            let candidate_term = Term(request.get_term());
            let candidate_index = LogIndex(request.get_last_log_index());
            let local_term = self.store.current_term().unwrap(); // TODO: error handling
            let local_index = self.store.latest_index().unwrap(); // TODO: error handling

            if candidate_term < local_term {
                response.set_stale_term(local_term.as_u64());
            } else if candidate_term == local_term && candidate_index < local_index {
                response.set_inconsistent_log(());
            } else if candidate_term == local_term && self.store.voted_for().unwrap().is_some() {
                response.set_already_voted(());
            } else {
                if candidate_term != local_term {
                    self.store.set_current_term(candidate_term);
                }
                let candidate = SocketAddr::from_str(request.get_candidate()).unwrap(); // TODO: error handling
                self.store.set_voted_for(Some(candidate));
                response.set_granted(());
            }
        }

        serialize_packed::write_packed_message_unbuffered(&mut connection, &mut response_message).unwrap(); // TODO: error handling
    }

    fn apply_request_vote_result(&mut self, message: OwnedSpaceMessageReader) {
    }

    fn apply_append_entries_result(&mut self, message: OwnedSpaceMessageReader) {
    }

    fn shutdown(&mut self) { }
}
