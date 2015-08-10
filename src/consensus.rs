//! `Consensus` is a state-machine (not to be confused with the `StateMachine`
//! trait) which implements the logic of the Raft Protocol. A `Consensus`
//! receives events from the local `Server`. The set of possible events is
//! specified by the Raft Protocol:
//!
//! ```text
//! Event = AppendEntriesRequest | AppendEntriesResponse
//!       | RequestVoteRequest   | RequestVoteResponse
//!       | ElectionTimeout      | HeartbeatTimeout
//!       | ClientProposal       | ClientQuery
//! ```
//!
//! In response to an event, the `Consensus` may mutate its own state,
//! apply a command to the local `StateMachine`, or return an event to be sent
//! to one or more remote peers or clients.

use std::{cmp, fmt, iter};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use capnp::{
    MallocMessageBuilder,
    MessageBuilder,
    MessageReader,
};
use rand::{self, Rng};

use {LogIndex, Term, ServerId, ClientId, messages};
use messages_capnp::{
    append_entries_request,
    append_entries_response,
    client_request,
    proposal_request,
    query_request,
    message,
    request_vote_request,
    request_vote_response,
};
use state::{ConsensusState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use persistent_log::Log;

const ELECTION_MIN: u64 = 1500;
const ELECTION_MAX: u64 = 3000;
const HEARTBEAT_DURATION: u64 = 1000;

/// Consensus timeout types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ConsensusTimeout {
    // An election timeout. Randomized value.
    Election,
    // A heartbeat timeout. Stable value.
    Heartbeat(ServerId),
}

impl ConsensusTimeout {
    /// Returns the timeout period in milliseconds.
    pub fn duration_ms(&self) -> u64 {
        match *self {
            ConsensusTimeout::Election => rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX),
            ConsensusTimeout::Heartbeat(..) => HEARTBEAT_DURATION,
        }
    }
}

/// The set of actions for the server to carry out after applying requests to the consensus module.
pub struct Actions {
    /// Messages to be sent to peers.
    pub peer_messages: Vec<(ServerId, Rc<MallocMessageBuilder>)>,
    /// Messages to be send to clients.
    pub client_messages: Vec<(ClientId, Rc<MallocMessageBuilder>)>,
    /// Whether to clear existing consensus timeouts.
    pub clear_timeouts: bool,
    /// Any new timeouts to create.
    pub timeouts: Vec<ConsensusTimeout>,
    /// Whether to clear outbound peer messages.
    pub clear_peer_messages: bool,
}

impl fmt::Debug for Actions {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let peer_messages: Vec<ServerId> = self.peer_messages
                                               .iter().map(|peer_message| peer_message.0)
                                               .collect();
        let client_messages: Vec<ClientId> = self.client_messages
                                                 .iter().map(|client_message| client_message.0)
                                                 .collect();
        write!(fmt, "Actions {{ peer_messages: {:?}, client_messages: {:?}, \
                     clear_timeouts: {:?}, timeouts: {:?}, clear_peer_messages: {} }}",
               peer_messages, client_messages, self.clear_timeouts, self.timeouts,
               self.clear_peer_messages)
    }
}

impl Actions {
    /// Creates an empty `Actions` set.
    pub fn new() -> Actions {
        Actions {
            peer_messages: vec![],
            client_messages: vec![],
            clear_timeouts: false,
            timeouts: vec![],
            clear_peer_messages: false,
        }
    }
}

/// An instance of a Raft state machine. The Consensus controls a client state
/// machine, to which it applies entries in a globally consistent order.
pub struct Consensus<L, M> {
    /// The ID of this consensus instance.
    id: ServerId,
    /// The IDs of peers in the consensus group.
    peers: HashMap<ServerId, SocketAddr>,

    /// The persistent log.
    log: L,
    /// The client state machine to which client commands are applied.
    state_machine: M,

    /// Index of the latest entry known to be committed.
    commit_index: LogIndex,
    /// Index of the latest entry applied to the state machine.
    last_applied: LogIndex,

    /// The current state of the `Consensus` (`Leader`, `Candidate`, or `Follower`).
    state: ConsensusState,
    /// State necessary while a `Leader`. Should not be used otherwise.
    leader_state: LeaderState,
    /// State necessary while a `Candidate`. Should not be used otherwise.
    candidate_state: CandidateState,
    /// State necessary while a `Follower`. Should not be used otherwise.
    follower_state: FollowerState,
}

impl <L, M> Consensus<L, M> where L: Log, M: StateMachine {
    /// Creates a `Consensus`.
    pub fn new(id: ServerId,
               peers: HashMap<ServerId, SocketAddr>,
               log: L,
               state_machine: M)
               -> Consensus<L, M> {
        let leader_state = LeaderState::new(log.latest_log_index(),
                                            &peers.keys().cloned().collect());
        Consensus {
            id: id,
            peers: peers,
            log: log,
            state_machine: state_machine,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            state: ConsensusState::Follower,
            leader_state: leader_state,
            candidate_state: CandidateState::new(),
            follower_state: FollowerState::new(),
        }
    }

    /// Returns the set of initial action which should be executed upon startup.
    pub fn init(&self) -> Actions {
        let mut actions = Actions::new();
        actions.timeouts.push(ConsensusTimeout::Election);
        actions
    }

    /// Returns the consenus peers.
    pub fn peers(&self) -> &HashMap<ServerId, SocketAddr> {
        &self.peers
    }

    /// Applies a peer message to the consensus state machine.
    pub fn apply_peer_message<R>(&mut self, from: ServerId, message: &R, actions: &mut Actions)
    where R: MessageReader {
        push_log_scope!("{:?}", self);
        let reader = message.get_root::<message::Reader>().unwrap().which().unwrap();
        match reader {
            message::Which::AppendEntriesRequest(Ok(request)) =>
                self.append_entries_request(from, request, actions),
            message::Which::AppendEntriesResponse(Ok(response)) =>
                self.append_entries_response(from, response, actions),
            message::Which::RequestVoteRequest(Ok(request)) =>
                self.request_vote_request(from, request, actions),
            message::Which::RequestVoteResponse(Ok(response)) =>
                self.request_vote_response(from, response, actions),
            _ => panic!("cannot handle message"),
        };
    }

    /// Applies a client message to the consensus module. This function dispatches a generic
    /// request to its appropriate handler.
    pub fn apply_client_message<R>(&mut self,
                                   from: ClientId,
                                   message: &R,
                                   actions: &mut Actions)
    where R: MessageReader {
        push_log_scope!("{:?}", self);
        let reader = message.get_root::<client_request::Reader>().unwrap().which().unwrap();
        match reader {
            client_request::Which::Proposal(Ok(request)) =>
                self.proposal_request(from, request, actions),
            client_request::Which::Query(Ok(query)) =>
                self.query_request(from, query, actions),
            _ => panic!("cannot handle message"),
        }
    }

    /// Applies a timeout's actions to the `Consensus`.
    pub fn apply_timeout(&mut self, timeout: ConsensusTimeout, actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        match timeout {
            ConsensusTimeout::Election => self.election_timeout(actions),
            ConsensusTimeout::Heartbeat(peer) => self.heartbeat_timeout(peer, actions),
        }
    }

    pub fn peer_connection_reset(&mut self, peer: ServerId, actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        match self.state {
            ConsensusState::Leader => {
                // Send any outstanding entries to the peer, or an empty heartbeat if there are no
                // outstanding entries.

                let from_index = self.leader_state.next_index(&peer);
                let until_index = self.latest_log_index() + 1;
                let num_entries = until_index.as_u64() - from_index.as_u64();

                let prev_log_index = from_index - 1;
                let prev_log_term = self.log.entry_term(prev_log_index);


                let message = messages::append_entries_request(self.log.current_term(),
                                                               prev_log_index,
                                                               prev_log_term,
                                                               num_entries as u32,
                                                               self.log.entries(from_index, until_index),
                                                               self.commit_index);

                self.leader_state.set_next_index(peer, until_index);
                actions.peer_messages.push((peer, message));
            },
            ConsensusState::Candidate => {
                // Resend the request vote request if a response has not yet been receieved.
                if !self.candidate_state.peer_voted(peer) {
                    let message = messages::request_vote_request(self.current_term(),
                                                                 self.latest_log_index(),
                                                                 self.log.latest_log_term());
                    actions.peer_messages.push((peer, message));
                }
            },
            ConsensusState::Follower => {
                // No message is necessary; if the peer is a leader or candidate they will send a
                // message.
            },
        }
    }

    /// Apply an append entries request to the consensus state machine.
    fn append_entries_request(&mut self,
                              from: ServerId,
                              request: append_entries_request::Reader,
                              actions: &mut Actions) {
        scoped_trace!("AppendEntriesRequest from peer {}", &from);

        let leader_term = Term(request.get_term());
        let current_term = self.current_term();

        if leader_term < current_term {
            let message = messages::append_entries_response_stale_term(current_term);
            actions.peer_messages.push((from, message));
            return;
        }

        let leader_commit_index = LogIndex::from(request.get_leader_commit());
        scoped_assert!(self.commit_index <= leader_commit_index);

        match self.state {
            ConsensusState::Follower => {
                let message = {
                    if current_term < leader_term {
                        self.log.set_current_term(leader_term);
                        self.follower_state.set_leader(from);
                    }

                    let leader_prev_log_index = LogIndex(request.get_prev_log_index());
                    let leader_prev_log_term = Term(request.get_prev_log_term());

                    let latest_log_index = self.latest_log_index();
                    if latest_log_index < leader_prev_log_index {
                        scoped_debug!("AppendEntriesRequest: inconsistent previous log index: \
                                      leader: {}, local: {}",
                                      leader_prev_log_index, latest_log_index);
                        messages::append_entries_response_inconsistent_prev_entry(self.current_term())
                    } else {
                        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                            Term::from(0)
                        } else {
                            self.log.entry_term(leader_prev_log_index)
                        };

                        if existing_term != leader_prev_log_term {
                            scoped_debug!("AppendEntriesRequest: inconsistent previous log term: \
                                          leader term: {}, local term: {}",
                                          leader_prev_log_term, existing_term);
                            messages::append_entries_response_inconsistent_prev_entry(self.current_term())
                        } else {
                            let entries = request.get_entries().unwrap();
                            let num_entries = entries.len();
                            scoped_debug!("AppendEntriesRequest: {} entries from leader: {}", num_entries, from);
                            if num_entries > 0 {
                                let mut entries_vec = Vec::with_capacity(num_entries as usize);
                                for i in 0..num_entries {
                                    entries_vec.push((leader_term, entries.get(i).unwrap()));
                                }
                                self.log.append_entries(leader_prev_log_index + 1, &entries_vec);
                            }
                            let latest_log_index = leader_prev_log_index + num_entries as u64;
                            // We are matching the leaders log up to and including `latest_log_index`.
                            self.commit_index = cmp::min(leader_commit_index, latest_log_index);
                            self.apply_commits();
                            messages::append_entries_response_success(self.current_term(), self.log.latest_log_index())
                        }
                    }
                };
                actions.peer_messages.push((from, message));
                actions.timeouts.push(ConsensusTimeout::Election);
            },
            ConsensusState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!("received AppendEntriesRequest from Consensus {{ id: {}, term: {} }} \
                             with newer term", from, leader_term);
                self.transition_to_follower(leader_term, from, actions);
                return self.append_entries_request(from, request, actions)
            },
            ConsensusState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    panic!("{:?}: peer leader {} with matching term {:?} detected.",
                           self, from, current_term);
                }

                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!("received AppendEntriesRequest from Consensus {{ id: {}, term: {} }} \
                             with newer term", from, leader_term);
                self.transition_to_follower(leader_term, from, actions);
                return self.append_entries_request(from, request, actions)
            },
        }
    }

    /// Apply an append entries response to the consensus state machine.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(&mut self,
                               from: ServerId,
                               response: append_entries_response::Reader,
                               actions: &mut Actions) {
        let local_term = self.current_term();
        let responder_term = Term::from(response.get_term());

        if local_term < responder_term {
            // Responder has a higher term number. Relinquish leader position (if it is held), and
            // return to follower status.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            scoped_info!("AppendEntriesResponse from peer {} with newer term: {}",
                         from, responder_term);
            self.transition_to_follower(responder_term, from, actions);
            return;
        } else if local_term > responder_term {
            scoped_debug!("AppendEntriesResponse from peer {} with a different term: {}",
                          from, responder_term);
            // Responder is responding to an AppendEntries request from a different term. Ignore
            // the response.
            return;
        }

        match response.which() {
            Ok(append_entries_response::Which::Success(follower_latest_log_index)) => {
                scoped_trace!("AppendEntriesResponse from peer {}: success", from);
                scoped_assert!(self.is_leader());
                let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                scoped_assert!(follower_latest_log_index <= self.latest_log_index());
                self.leader_state.set_match_index(from, follower_latest_log_index);
                self.advance_commit_index(actions);
            }
            Ok(append_entries_response::Which::InconsistentPrevEntry(..)) => {
                scoped_debug!("AppendEntriesResponse from peer {}: inconsistent previous entry", from);
                scoped_assert!(self.is_leader());
                let next_index = self.leader_state.next_index(&from) - 1;
                self.leader_state.set_next_index(from, next_index);
            }
            Ok(append_entries_response::Which::StaleTerm(..)) => {
                // The peer is reporting a stale term, but the term number matches the local term.
                // Ignore the response, since it is to a message from a prior term, and this server
                // has already transitioned to the new term.
                scoped_debug!("AppendEntriesResponse from peer {}: stale term (outdated)", from);
                return;
            }
            Ok(append_entries_response::Which::InternalError(error_result)) => {
                let error = error_result.unwrap_or("[unable to decode internal error]");
                scoped_warn!("AppendEntriesResponse from peer {}: internal error: {}",
                             from, error);
            }
            Err(error) => {
                scoped_warn!("AppendEntriesResponse from peer {}: unable to deserialize response: {}",
                             from, error);
            }
        }

        let next_index = self.leader_state.next_index(&from);
        if next_index <= self.latest_log_index() {
            // If the peer is behind, send it entries to catch up.
            scoped_debug!("AppendEntriesResponse: peer {} is missing entries {} through {}",
                          from, next_index, self.latest_log_index());
            let prev_log_index = next_index - 1;
            let prev_log_term = self.log.entry_term(prev_log_index);

            let from_index = self.leader_state.next_index(&from);
            let until_index = self.latest_log_index() + 1;
            let num_entries = until_index.as_u64() - from_index.as_u64();

            let prev_log_index = from_index - 1;
            let prev_log_term = self.log.entry_term(prev_log_index);

            let message = messages::append_entries_request(self.log.current_term(),
                                                           prev_log_index,
                                                           prev_log_term,
                                                           num_entries as u32,
                                                           self.log.entries(from_index, until_index),
                                                           self.commit_index);

            self.leader_state.set_next_index(from, until_index);
            actions.peer_messages.push((from, message));
        } else {
            // If the peer is caught up, set a heartbeat timeout.
            scoped_trace!("AppendEntriesResponse: scheduling heartbeat for peer {}", from);
            let timeout = ConsensusTimeout::Heartbeat(from);
            actions.timeouts.push(timeout);
        }
    }

    /// Applies a peer request vote request to the consensus state machine.
    fn request_vote_request(&mut self,
                            candidate: ServerId,
                            request: request_vote_request::Reader,
                            actions: &mut Actions) {
        let candidate_term = Term(request.get_term());
        let candidate_log_term = Term(request.get_last_log_term());
        let candidate_log_index = LogIndex(request.get_last_log_index());
        scoped_debug!("RequestVoteRequest from Consensus {{ id: {}, term: {}, latest_log_term: {}, \
                      latest_log_index: {} }}",
                      &candidate, candidate_term, candidate_log_term, candidate_log_index);
        let local_term = self.current_term();

        let new_local_term = if candidate_term > local_term {
            scoped_info!("received RequestVoteRequest from Consensus {{ id: {}, term: {} }} \
                         with newer term", candidate, candidate_term);
            self.transition_to_follower(candidate_term, candidate, actions);
            candidate_term
        } else {
            local_term
        };

        let message = if candidate_term < local_term {
            messages::request_vote_response_stale_term(new_local_term)
        } else if candidate_log_term < self.latest_log_term()
               || candidate_log_index < self.latest_log_index() {
            messages::request_vote_response_inconsistent_log(new_local_term)
        } else {
            match self.log.voted_for() {
                None => {
                    self.log.set_voted_for(candidate);
                    messages::request_vote_response_granted(new_local_term)
                },
                Some(voted_for) if voted_for == candidate => {
                    messages::request_vote_response_granted(new_local_term)
                },
                _ => {
                    messages::request_vote_response_already_voted(new_local_term)
                },
            }
        };
        actions.peer_messages.push((candidate, message));
    }

    /// Applies a request vote response to the consensus state machine.
    fn request_vote_response(&mut self,
                             from: ServerId,
                             response: request_vote_response::Reader,
                             actions: &mut Actions) {
        scoped_debug!("RequestVoteResponse from peer {}", from);

        let local_term = self.current_term();
        let voter_term = Term::from(response.get_term());

        let majority = self.majority();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            scoped_info!("received RequestVoteResponse from Consensus {{ id: {}, term: {} }} \
                         with newer term", from, voter_term);
            self.transition_to_follower(voter_term, from, actions);
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
        } else if self.is_candidate() {
            // A vote was received!
            if let Ok(request_vote_response::Granted(_)) = response.which() {
                self.candidate_state.record_vote(from.clone());
                if self.candidate_state.count_votes() >= majority {
                    scoped_info!("election for term {} won; transitioning to Leader",
                                 local_term);
                    self.transition_to_leader(actions);
                }
            }
        };
    }

    /// Applies a client proposal to the consensus state machine.
    fn proposal_request(&mut self,
                        from: ClientId,
                        request: proposal_request::Reader,
                        actions: &mut Actions) {
        if self.is_candidate() || (self.is_follower() && self.follower_state.leader.is_none()) {
            actions.client_messages.push((from, messages::command_response_unknown_leader()));
        } else if self.is_follower() {
            let message =
                messages::command_response_not_leader(&self.peers[&self.follower_state.leader.unwrap()]);
            actions.client_messages.push((from, message));
        } else {
            let prev_log_index = self.latest_log_index();
            let prev_log_term = self.latest_log_term();
            let term = self.current_term();
            let log_index = prev_log_index + 1;
            // TODO: This is probably not exactly safe.
            let entry = request.get_entry().unwrap();
            self.log.append_entries(log_index, &[(term, entry)]);
            self.leader_state.proposals.push_back((from, log_index));
            scoped_debug!("ProposalRequest from client {}: sending entry {} to peers",
                          from, log_index);
            if self.peers.len() == 0 {
                self.advance_commit_index(actions);
            } else {
                let message = messages::append_entries_request(term,
                                                               prev_log_index,
                                                               prev_log_term,
                                                               1,
                                                               iter::once(entry),
                                                               self.commit_index);
                for &peer in self.peers.keys() {
                    if self.leader_state.next_index(&peer) == prev_log_index + 1 {
                        actions.peer_messages.push((peer, message.clone()));
                    }
                }
            }
        }
    }

    /// Applies a client query to the state machine.
    fn query_request(&mut self,
                    from: ClientId,
                    request: query_request::Reader,
                    actions: &mut Actions) {
        scoped_trace!("query from Client({})", from);

        if self.is_candidate() || (self.is_follower() && self.follower_state.leader.is_none()) {
            actions.client_messages.push((from, messages::command_response_unknown_leader()));
        } else if self.is_follower() {
            let message =
                messages::command_response_not_leader(&self.peers[&self.follower_state.leader.unwrap()]);
            actions.client_messages.push((from, message));
        } else {
            // TODO: This is probably not exactly safe.
            let query = request.get_query().unwrap();
            let result = self.state_machine.query(query);
            let message = messages::command_response_success(&result);
            actions.client_messages.push((from, message));
        }
    }

    /// Triggers a heartbeat timeout for the peer.
    fn heartbeat_timeout(&mut self, peer: ServerId, actions: &mut Actions) {
        scoped_assert!(self.is_leader());
        scoped_debug!("HeartbeatTimeout for peer: {}", peer);
        let mut message = MallocMessageBuilder::new_default();
        {
            let mut request = message.init_root::<message::Builder>()
                                     .init_append_entries_request();
            request.set_term(self.current_term().as_u64());
            request.set_prev_log_index(self.latest_log_index().as_u64());
            request.set_prev_log_term(self.log.latest_log_term().as_u64());
            request.set_leader_commit(self.commit_index.as_u64());
            request.init_entries(0);
        }
        actions.peer_messages.push((peer, Rc::new(message)));
    }

    /// Triggers an election timeout.
    fn election_timeout(&mut self, actions: &mut Actions) {
        scoped_assert!(!self.is_leader());
        if self.peers.is_empty() {
            // Solitary consensus module special case; jump straight to leader status
            scoped_info!("ElectionTimeout: transitioning to Leader");
            scoped_assert!(self.is_follower());
            scoped_assert!(self.log.voted_for().is_none());
            self.log.inc_current_term();
            self.log.set_voted_for(self.id);
            let latest_log_index = self.latest_log_index();
            self.state = ConsensusState::Leader;
            self.leader_state.reinitialize(latest_log_index);
        } else {
            scoped_info!("ElectionTimeout: transitioning to Candidate");
            self.transition_to_candidate(actions);
        }
    }

    /// Transitions this consensus state machine to Leader.
    fn transition_to_leader(&mut self, actions: &mut Actions) {
        scoped_trace!("transitioning to Leader");
        let current_term = self.current_term();
        let latest_log_index = self.latest_log_index();
        let latest_log_term = self.log.latest_log_term();
        self.state = ConsensusState::Leader;
        self.leader_state.reinitialize(latest_log_index);

        let message = messages::append_entries_request(current_term,
                                                       latest_log_index,
                                                       latest_log_term,
                                                       0,
                                                       iter::empty(),
                                                       self.commit_index);
        for &peer in self.peers().keys() {
            actions.peer_messages.push((peer, message.clone()));
        }

        actions.clear_timeouts = true;
    }

    /// Transitions the conensus state machine to Candidate.
    fn transition_to_candidate(&mut self, actions: &mut Actions) {
        scoped_info!("transitioning to Candidate");
        self.log.inc_current_term();
        self.log.set_voted_for(self.id);
        self.state = ConsensusState::Candidate;
        self.candidate_state.clear();
        self.candidate_state.record_vote(self.id);

        let message = messages::request_vote_request(self.current_term(),
                                                     self.latest_log_index(),
                                                     self.log.latest_log_term());

        for &peer in self.peers().keys() {
            actions.peer_messages.push((peer, message.clone()));
        }
        actions.timeouts.push(ConsensusTimeout::Election);
    }

    /// Advances the commit index and applies committed entries to the state machine.
    fn advance_commit_index(&mut self, actions: &mut Actions) {
        scoped_assert!(self.is_leader());
        let majority = self.majority();
        // TODO: Figure out failure condition here.
        while self.commit_index < self.log.latest_log_index() {
            if self.leader_state.count_match_indexes(self.commit_index + 1) >= majority {
                self.commit_index = self.commit_index + 1;
                scoped_debug!("commit index advanced to {}", self.commit_index);
            } else {
                break; // If there isn't a majority now, there won't be one later.
            }
        }

        let results = self.apply_commits();

        while let Some(&(client, index)) = self.leader_state.proposals.get(0) {
            if index <= self.commit_index {
                scoped_trace!("responding to client {} for entry {}", client, index);
                // We know that there will be an index here since it was commited
                // and the index is less than that which has been commited.
                let result = results.get(&index).unwrap();
                let message = messages::command_response_success(result);
                actions.client_messages.push((client, message));
                self.leader_state.proposals.pop_front();
            } else {
                break;
            }
        }
    }

    /// Applies all committed but unapplied log entries to the state machine.
    /// Returns the set of return values from the commits applied.
    fn apply_commits(&mut self) -> HashMap<LogIndex, Vec<u8>> {
        let mut results = HashMap::new();

        if self.last_applied < self.commit_index {
            for entry in self.log.entries(self.last_applied, self.commit_index + 1) {
                if !entry.is_empty() {
                    let result = self.state_machine.apply(entry);
                    results.insert(self.last_applied + 1, result);
                }
            }
            self.last_applied = self.commit_index;
        }
        results
    }

    /// Transitions the consensus state machine to Follower with the provided
    /// term. The `voted_for` field will be reset. The provided leader hint will
    /// replace the last known leader.
    fn transition_to_follower(&mut self,
                              term: Term,
                              leader: ServerId,
                              actions: &mut Actions) {
        scoped_info!("transitioning to Follower");
        self.log.set_current_term(term);
        self.state = ConsensusState::Follower;
        self.follower_state.set_leader(leader);
        actions.clear_timeouts = true;
        actions.timeouts.push(ConsensusTimeout::Election);
    }

    /// Returns whether the consensus state machine is currently a Leader.
    fn is_leader(&self) -> bool {
        self.state == ConsensusState::Leader
    }

    /// Returns whether the consensus state machine is currently a Follower.
    fn is_follower(&self) -> bool {
        self.state == ConsensusState::Follower
    }

    /// Returns whether the consensus module is currently a Candidate.
    fn is_candidate(&self) -> bool {
        self.state == ConsensusState::Candidate
    }

    /// Returns the current term.
    fn current_term(&self) -> Term {
        self.log.current_term()
    }

    /// Returns the term of the latest applied log entry.
    fn latest_log_term(&self) -> Term {
        self.log.latest_log_term()
    }

    /// Returns the index of the latest applied log entry.
    fn latest_log_index(&self) -> LogIndex {
        self.log.latest_log_index()
    }

    /// Get the cluster quorum majority size.
    fn majority(&self) -> usize {
        let peers = self.peers.len();
        let cluster_members = peers.checked_add(1).expect(&format!("unable to support {} cluster members", peers));
        (cluster_members >> 1) + 1
    }
}

impl <L, M> fmt::Debug for Consensus<L, M> where L: Log, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Consensus {{ id: {}, state: {:?}, term: {}, index: {} }}",
               self.id, self.state, self.current_term(), self.latest_log_index())
    }
}

#[cfg(test)]
mod tests {

    extern crate env_logger;

    use std::collections::{HashMap, VecDeque};
    use std::io::Cursor;
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::str::FromStr;

    use capnp::{MallocMessageBuilder, MessageBuilder, ReaderOptions};
    use capnp::serialize::{self, OwnedSpaceMessageReader};

    use ClientId;
    use LogIndex;
    use ServerId;
    use Term;
    use messages;
    use consensus::{Actions, Consensus, ConsensusTimeout};
    use state_machine::NullStateMachine;
    use persistent_log::{MemLog, Log};

    type TestModule = Consensus<MemLog, NullStateMachine>;

    fn new_cluster(size: u64) -> HashMap<ServerId, TestModule> {
        let ids: HashMap<ServerId, SocketAddr> =
            (0..size).map(Into::into)
                     .map(|id| (id, SocketAddr::from_str(&format!("127.0.0.1:{}", id)).unwrap()))
                     .collect();
        ids.iter().map(|(&id, _)| {
            let mut peers = ids.clone();
            peers.remove(&id);
            let store = MemLog::new();
            (id, Consensus::new(id, peers, store, NullStateMachine))
        }).collect()
    }

    fn into_reader<M>(message: &M) -> OwnedSpaceMessageReader where M: MessageBuilder {
        let mut buf = Cursor::new(Vec::new());

        serialize::write_message(&mut buf, message).unwrap();
        buf.set_position(0);
        serialize::read_message(&mut buf, ReaderOptions::new()).unwrap()
    }

    /// Applies the actions to the consensus modules (and recursively applies any resulting
    /// actions), and returns any client messages.
    fn apply_actions(from: ServerId,
                     mut actions: Actions,
                     consensus_modules: &mut HashMap<ServerId, TestModule>)
                     -> Vec<(ClientId, Rc<MallocMessageBuilder>)> {
        let mut queue: VecDeque<(ServerId, ServerId, Rc<MallocMessageBuilder>)> = VecDeque::new();

        for (to, message) in actions.peer_messages.iter().cloned() {
            queue.push_back((from, to, message));
        }
        actions.peer_messages.clear();

        while let Some((from, to, message)) = queue.pop_front() {
            let reader = into_reader(&*message);
            consensus_modules.get_mut(&to).unwrap().apply_peer_message(from, &reader, &mut actions);
            let inner_from = to;
            for (inner_to, message) in actions.peer_messages.iter().cloned() {
                queue.push_back((inner_from, inner_to, message));
            }
            actions.peer_messages.clear();
        }

        let Actions { client_messages, .. } = actions;
        client_messages
    }

    /// Elect `leader` as the leader of a cluster with the provided followers.
    /// The leader and the followers must be in the same term.
    fn elect_leader(leader: ServerId,
                    consensus_modules: &mut HashMap<ServerId, TestModule>) {
        let mut actions = Actions::new();
        consensus_modules.get_mut(&leader).unwrap().apply_timeout(ConsensusTimeout::Election, &mut actions);
        let client_messages = apply_actions(leader, actions, consensus_modules);
        assert!(client_messages.is_empty());
        assert!(consensus_modules[&leader].is_leader());
    }

    /// Tests the majority function.
    #[test]
    fn test_majority () {
        let (_, consensus_module) = new_cluster(1).into_iter().next().unwrap();
        assert_eq!(1, consensus_module.majority());

        let (_, consensus_module) = new_cluster(2).into_iter().next().unwrap();
        assert_eq!(2, consensus_module.majority());

        let (_, consensus_module) = new_cluster(3).into_iter().next().unwrap();
        assert_eq!(2, consensus_module.majority());

        let (_, consensus_module) = new_cluster(4).into_iter().next().unwrap();
        assert_eq!(3, consensus_module.majority());
    }

    /// Tests that a single-consensus module cluster will behave appropriately.
    ///
    /// The single consensus module should transition straight to the Leader state upon the first
    /// timeout.
    #[test]
    fn test_solitary_consensus_transition_to_leader() {
        setup_test!("test_solitary_consensus_transition_to_leader");
        let (_, mut consensus_module) = new_cluster(1).into_iter().next().unwrap();
        assert!(consensus_module.is_follower());

        let mut actions = Actions::new();
        consensus_module.apply_timeout(ConsensusTimeout::Election, &mut actions);
        assert!(consensus_module.is_leader());
        assert!(actions.peer_messages.is_empty());
        assert!(actions.client_messages.is_empty());
        assert!(actions.timeouts.is_empty());
    }

    /// A simple election test over multiple group sizes.
    #[test]
    fn test_election() {
        setup_test!("test_election");

        for group_size in 1..10 {
            let mut consensus_modules = new_cluster(group_size);
            let module_ids: Vec<ServerId> = consensus_modules.keys().cloned().collect();
            let leader = &module_ids[0];
            elect_leader(leader.clone(), &mut consensus_modules);
            assert!(consensus_modules[leader].is_leader());
            for follower in module_ids.iter().skip(1) {
                assert!(consensus_modules[follower].is_follower());
            }
        }
    }

    /// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
    /// timeout, and in response sends an AppendEntries message to the follower.
    /// The follower in turn resets its election timout, and replies to the
    /// leader.
    #[test]
    fn test_heartbeat() {
        setup_test!("test_heartbeat");
        let mut consensus_modules = new_cluster(2);
        let module_ids: Vec<ServerId> = consensus_modules.keys().cloned().collect();
        let leader_id = &module_ids[0];
        let follower_id = &module_ids[1];
        elect_leader(leader_id.clone(), &mut consensus_modules);

        // Leader pings with a heartbeat timeout.
        let leader_append_entries = {
            let mut actions = Actions::new();
            let leader = consensus_modules.get_mut(&leader_id).unwrap();
            leader.heartbeat_timeout(follower_id.clone(), &mut actions);

            let peer_message = actions.peer_messages.iter().next().unwrap();
            assert_eq!(peer_message.0, follower_id.clone());
            peer_message.1.clone()
        };
        let reader = into_reader(&*leader_append_entries);

        // Follower responds.
        let follower_response = {
            let mut actions = Actions::new();
            let follower = consensus_modules.get_mut(&follower_id).unwrap();
            follower.apply_peer_message(leader_id.clone(), &reader, &mut actions);

            let election_timeout = actions.timeouts.iter().next().unwrap();
            assert_eq!(election_timeout, &ConsensusTimeout::Election);

            let peer_message = actions.peer_messages.iter().next().unwrap();
            assert_eq!(peer_message.0, leader_id.clone());
            peer_message.1.clone()
        };
        let reader = into_reader(&*follower_response);

        // Leader applies and sends back a heartbeat to establish leadership.
        let leader = consensus_modules.get_mut(&leader_id).unwrap();
        let mut actions = Actions::new();
        leader.apply_peer_message(follower_id.clone(), &reader, &mut actions);
        let heartbeat_timeout = actions.timeouts.iter().next().unwrap();
        assert_eq!(heartbeat_timeout, &ConsensusTimeout::Heartbeat(follower_id.clone()));
    }

    /// Emulates a slow heartbeat message in a two-node cluster.
    ///
    /// The initial leader (Consensus 0) sends a heartbeat, but before it is received by the follower
    /// (Consensus 1), Consensus 1's election timeout fires. Consensus 1 transitions to candidate state
    /// and attempts to send a RequestVote to Consensus 0. When the partition is fixed, the
    /// RequestVote should prompt Consensus 0 to step down. Consensus 1 should send a stale term
    /// message in response to the heartbeat from Consensus 0.
    #[test]
    fn test_slow_heartbeat() {
        setup_test!("test_heartbeat");
        let mut consensus_modules = new_cluster(2);
        let module_ids: Vec<ServerId> = consensus_modules.keys().cloned().collect();
        let module_0 = &module_ids[0];
        let module_1 = &module_ids[1];
        elect_leader(module_0.clone(), &mut consensus_modules);

        let mut module_0_actions = Actions::new();
        consensus_modules.get_mut(module_0)
                .unwrap()
                .apply_timeout(ConsensusTimeout::Heartbeat(*module_1), &mut module_0_actions);
        assert!(consensus_modules[module_0].is_leader());

        let mut module_1_actions = Actions::new();
        consensus_modules.get_mut(module_1)
                .unwrap()
                .apply_timeout(ConsensusTimeout::Election, &mut module_1_actions);
        assert!(consensus_modules[module_1].is_candidate());

        // Apply candidate messages.
        assert!(apply_actions(*module_1, module_1_actions, &mut consensus_modules).is_empty());
        assert!(consensus_modules[module_0].is_follower());
        assert!(consensus_modules[module_1].is_leader());

        // Apply stale heartbeat.
        assert!(apply_actions(*module_0, module_0_actions, &mut consensus_modules).is_empty());
        assert!(consensus_modules[module_0].is_follower());
        assert!(consensus_modules[module_1].is_leader());
    }

    /// Tests that a client proposal is correctly replicated to peers, and the client is notified
    /// of the success.
    #[test]
    fn test_proposal() {
        setup_test!("test_proposal");
        // Test various sizes.
        for i in 1..7 {
            scoped_debug!("testing size {} cluster", i);
            let mut consensus_modules = new_cluster(i);
            let module_ids: Vec<ServerId> = consensus_modules.keys().cloned().collect();
            let leader = module_ids[0];
            elect_leader(leader, &mut consensus_modules);

            let value: &[u8] = b"foo";
            let proposal = into_reader(&messages::proposal_request(value));
            let mut actions = Actions::new();

            let client = ClientId::new();

            consensus_modules.get_mut(&leader)
                    .unwrap()
                    .apply_client_message(client, &proposal, &mut actions);

            let client_messages = apply_actions(leader, actions, &mut consensus_modules);
            assert_eq!(1, client_messages.len());
            for module in consensus_modules.values() {
                assert_eq!(value, module.log.entry(LogIndex(1)));
                assert_eq!(Term(1), module.log.entry_term(LogIndex(1)));
            }
        }
    }

    /// Tests that a client query is correctly responded to, and the client is notified
    /// of the success.
    #[test]
    fn test_query() {
        setup_test!("test_query");
        // Test various sizes.
        for i in 1..7 {
            scoped_debug!("testing size {} cluster", i);
            let mut consensus_modules = new_cluster(i);
            let module_ids: Vec<ServerId> = consensus_modules.keys().cloned().collect();
            let leader = module_ids[0];
            elect_leader(leader, &mut consensus_modules);

            let value: &[u8] = b"foo";
            let query = into_reader(&messages::proposal_request(value));
            let mut actions = Actions::new();

            let client = ClientId::new();

            consensus_modules.get_mut(&leader)
                             .unwrap()
                             .apply_client_message(client, &query, &mut actions);

            let client_messages = apply_actions(leader, actions, &mut consensus_modules);
            assert_eq!(1, client_messages.len());
            for module in consensus_modules.values() {
                assert_eq!(value, module.log.entry(LogIndex(1)));
                assert_eq!(Term(1), module.log.entry_term(LogIndex(1)));
            }
        }
    }
}
