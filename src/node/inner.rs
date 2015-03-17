use std::collections::{hash_map, HashMap, HashSet};
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::error::Error;

use capnp::serialize::OwnedSpaceMessageReader;
use capnp::{self, serialize_packed, MessageBuilder, MessageReader, MallocMessageBuilder};

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
#[derive(Clone, Debug)]
enum NodeState {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct CandidateState {
    granted_votes: usize,
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
            Event::RequestVoteResponse { message } => {
                let response = message.get_root::<request_vote_response::Reader>();
                self.apply_request_vote_response(response)
            },
            Event::AppendEntriesResponse { message } => {
                let response = message.get_root::<append_entries_response::Reader>();
                self.apply_append_entries_response(response)
            },
            Event::Shutdown => self.shutdown()
        }
    }

    fn apply_rpc(&mut self, rpc_message: OwnedSpaceMessageReader, connection: TcpStream) {
        let rpc = rpc_message.get_root::<rpc::Reader>();
        let mut response_message = MallocMessageBuilder::new_default();

        match rpc.which() {
            Ok(rpc::AppendEntries(request)) => self.apply_append_entries_request(request, &mut response_message),
            Ok(rpc::RequestVote(request_vote_request)) => self.apply_request_vote_request(request_vote_request, &mut response_message),
            Err(_) => {
                let mut response = response_message.init_root::<append_entries_response::Builder>();
                response.set_internal_error("RPC type not recognized");
            }
        }

        serialize_packed::write_packed_message_unbuffered(&mut capnp::io::WriteOutputStream::new(connection),
                                                          &mut response_message)
                         .unwrap(); // TODO: error handling
    }

    fn apply_append_entries_request(&mut self,
                                    request: append_entries_request::Reader,
                                    response_message: &mut MallocMessageBuilder) {
        let leader_term = Term(request.get_term());
        let current_term = rpc_try!(response_message.init_root::<append_entries_response::Builder>(),
                                    self.store.current_term());

        if leader_term < current_term {
            let mut response = response_message.init_root::<append_entries_response::Builder>();
            response.set_stale_term(current_term.as_u64())
        } else {
            match self.node_state {
                NodeState::Follower => {
                    let mut response = response_message.init_root::<append_entries_response::Builder>();
                    let prev_log_index = LogIndex(request.get_prev_log_index());
                    let prev_log_term = Term(request.get_prev_log_term());

                    let local_latest_index = rpc_try!(response, self.store.latest_index());
                    if local_latest_index < prev_log_index {
                        response.set_inconsistent_prev_entry(());
                    } else {
                        let (existing_term, _) = self.store.entry(prev_log_index).unwrap();
                        if existing_term != prev_log_term {
                            rpc_try!(response, self.store.truncate_entries(prev_log_index));
                            response.set_inconsistent_prev_entry(());
                        } else {

                            let entries = request.get_entries();
                            let num_entries = entries.len();
                            if num_entries > 0 {
                                let mut entries_vec = Vec::with_capacity(num_entries as usize);
                                for i in 0..num_entries as u32 {
                                    entries_vec.push((leader_term, entries.get(i)));
                                }

                                rpc_try!(response, self.store.append_entries(prev_log_index + 1, &entries_vec));
                            }

                            response.set_success(());
                        }
                    }
                },
                NodeState::Candidate(..) | NodeState::Leader(..) => {
                    // recognize the new leader, return to follower state, and apply the entries
                    self.node_state = NodeState::Follower;
                    return self.apply_append_entries_request(request, response_message)
                },
            }
        }
    }

    fn apply_request_vote_request(&mut self,
                                  request: request_vote_request::Reader,
                                  response_message: &mut MallocMessageBuilder) {
        let mut response = response_message.init_root::<request_vote_response::Builder>();

        let candidate_term = Term(request.get_term());
        let candidate_index = LogIndex(request.get_last_log_index());
        let local_term = rpc_try!(response, self.store.current_term());
        let local_index = rpc_try!(response, self.store.latest_index());

        if candidate_term < local_term {
            response.set_stale_term(local_term.as_u64());
        } else if candidate_term == local_term && candidate_index < local_index {
            response.set_inconsistent_log(());
        } else if candidate_term == local_term && self.store.voted_for().unwrap().is_some() {
            response.set_already_voted(());
        } else {
            if candidate_term != local_term {
                rpc_try!(response, self.store.set_current_term(candidate_term));
            }
            // TODO: this should use rpc_try!, but std::net::parser::ParseError currently does not impl Error
            let candidate = match SocketAddr::from_str(request.get_candidate()) {
                Ok(addr) => addr,
                Err(_) => {
                    response.set_internal_error("Unable to parse candidate socket address");
                    return;
                }
            };
            rpc_try!(response, self.store.set_voted_for(Some(candidate)));
            response.set_granted(candidate_term.as_u64());
        }
    }

    fn apply_request_vote_response(&mut self, response: request_vote_response::Reader) {

        let new_state: Option<NodeState> = match self.node_state {
            NodeState::Candidate(ref mut state) => {
                match response.which() {
                    Ok(request_vote_response::Granted(term)) => {
                        let term = Term(term);
                        let current_term = self.store.current_term().unwrap(); // TODO: error handling
                        assert!(term <= current_term);
                        if term != current_term {
                            // A response to an older election; ignore it
                            None
                        } else {
                            state.granted_votes = state.granted_votes + 1;

                            if state.granted_votes > quorum(self.peers.len() + 1) {
                                // Transition to leader
                                let latest_index = self.store.latest_index().unwrap(); // TODO: error handling
                                Some(NodeState::Leader(LeaderState::new(latest_index)))
                            } else {
                                None
                            }
                        }
                    },
                    Ok(request_vote_response::StaleTerm(term)) => {
                        None
                    },
                    Ok(request_vote_response::AlreadyVoted(_)) => {
                        None
                    },
                    Ok(request_vote_response::InconsistentLog(_)) => {
                        None
                    },
                    Ok(request_vote_response::InternalError(error)) => {
                        None
                    },
                    Err(_) => {
                        error!("Request Vote Response type not recognized.");
                        None
                    },
                }
            },

            NodeState::Follower | NodeState::Leader(..) => None,
        };

        if let Some(state) = new_state {
            self.node_state = state;
        }
    }

    fn apply_append_entries_response(&mut self, response: append_entries_response::Reader) {
        match response.which() {
            Ok(append_entries_response::Success(_)) => {
            },
            Ok(append_entries_response::StaleTerm(term)) => {
            },
            Ok(append_entries_response::InconsistentPrevEntry(_)) => {
            },
            Ok(append_entries_response::InternalError(error)) => {
            },
            Err(_) => error!("Append Entries Response type not recognized"),
        }
    }

    fn shutdown(&mut self) { }
}

fn quorum(num_peers: usize) -> usize {
    0
}
