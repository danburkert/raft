extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;

use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{Write, ReadExt, Seek};
use std::str::{self, StrExt};
use std::{error, io, marker};

use rustc_serialize::base64::{ToBase64, FromBase64, Config, CharacterSet, Newline};
use rustc_serialize::{json, Encodable, Decodable};
use uuid::Uuid;

use state::NodeState::{Leader, Follower, Candidate};
use state::TransactionState::{Polling, Accepted, Rejected};

/// The identifier of a candidate.
pub type CandidateId = u64;

/// The term of a log entry.
pub type LogTerm = u64;

/// The index of a log entry.
pub type LogIndex = u64;

pub struct LogEntry<T> {
    index: LogIndex,
    term: LogTerm,
    value: T,
}

pub trait PersistentState<T> where T: Encodable + Decodable + Send + Clone {

    type Error: error::Error;

    /// Returns the latest known term.
    fn current_term(&self) -> LogTerm;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term.
    fn set_current_term(&self, LogTerm) -> Result<(), Self::Error>;

    fn inc_current_term(&self) -> Result<LogTerm, Self::Error>;

    /// Returns the candidate id of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> Option<CandidateId>;

    /// Sets the candidate id voted for in the current term.
    fn set_voted_for(&self, Option<CandidateId>) -> Result<(), Self::Error>;

    /// Returns the index of the latest persisted log entry (0 if the log is empty).
    fn latest_index(&self) -> LogIndex;

    /// Returns the entry at the provided log index.
    fn entry(&self, LogIndex) -> Result<LogEntry<T>, Self::Error>;

    /// Returns the entries between the start log index (inclusive),
    /// and the end log index (exclusive).
    fn entries(&self, start: LogIndex, end: LogIndex) -> Result<Vec<LogEntry<T>>, Self::Error>;

    /// Appends the provided entries to the log beginning with the given index.
    fn store_entries(&self, &[LogEntry<T>]) -> Result<(), Self::Error>;

    /// Remote all log entries after the provided log index, inclusive.
    fn remove_entries(&self, index: LogIndex) -> Result<(), Self::Error>;
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {

    /// Index of highest log entry known to be committed (initialized to 0,
    /// increases monotonically)
    pub commit_index: LogIndex,

    /// Index of highest log entry applied to state machine (initialized to 0,
    /// increases monotonically)
    pub last_applied: LogIndex,
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq, Clone)]
pub struct LeaderState {
    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>
}

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(PartialEq, Eq, Clone)]
pub enum NodeState {
    Follower(VecDeque<Transaction>),
    Leader(LeaderState),
    Candidate(Vec<Transaction>),
}

#[derive(PartialEq, Eq, Clone)]
pub struct Transaction {
    pub uuid: Uuid,
    pub state: TransactionState,
}

/// Used to signify the state of of a Request/Response pair. This is only needed
/// on the original sender... not on the reciever.
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum TransactionState {
    Polling,
    Accepted,
    Rejected,
}

#[test]
fn test_persistent_state() {
    use std::fs;
    let path = Path::new("/tmp/test_path");
    fs::remove_file(&path.clone()).ok();
    let mut state = PersistentState::new(0, path.clone());
    // Add 1
    assert_eq!(state.append_entries(0, 0, // Zero is the initialization state.
        vec![(1, "One".to_string())]),
        Ok(()));
    // Check 1
    assert_eq!(state.retrieve_entry(1),
        Ok((1, "One".to_string())));
    assert_eq!(state.get_last_index(), 1);
    assert_eq!(state.get_last_term(), 1);
    // Do a blank check.
    assert_eq!(state.append_entries(1,1, vec![]),
        Ok(()));
        assert_eq!(state.get_last_index(), 1);
        assert_eq!(state.get_last_term(), 1);
    // Add 2
    assert_eq!(state.append_entries(1, 1,
        vec![(2, "Two".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), 2);
    assert_eq!(state.get_last_term(), 2);
    // Check 1, 2
    assert_eq!(state.retrieve_entries(1, 2),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string())
        ]));
    // Check 2
    assert_eq!(state.retrieve_entry(2),
        Ok((2, "Two".to_string())));
    // Add 3, 4
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string())]),
        Ok(()));
    // Check 3, 4
    assert_eq!(state.retrieve_entries(3, 4),
        Ok(vec![(3, "Three".to_string()),
                (4, "Four".to_string())
        ]));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    // Remove 3, 4
    assert_eq!(state.purge_from_index(3),
        Ok(()));
    assert_eq!(state.get_last_index(), 2);
    assert_eq!(state.get_last_term(), 2);
    // Check 3, 4 are removed, and that code handles lack of entry gracefully.
    assert_eq!(state.retrieve_entries(0, 4),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string())
        ]));
    // Add 3, 4, 5
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string()),
             (5, "Five".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), 5);
    assert_eq!(state.get_last_term(), 5);
    // Add 3, 4 again. (5 should be purged)
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string())]),
        Ok(()));
    assert_eq!(state.retrieve_entries(0, 4),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string()),
                (3, "Three".to_string()),
                (4, "Four".to_string()),
        ]));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    // Do a blank check.
    assert_eq!(state.append_entries(4,4, vec![]),
        Ok(()));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    fs::remove_file(&path.clone()).ok();
}
