//! The persistent storage of Raft state.
//!
//! In your consuming application you will want to implement this trait on one of your structures.
//! This could adapt to a database, a file, or even just POD.
//!
//! *Note:* Your consuming application should not necessarily interface with this data. It is meant
//! for internal use by the library, we simply chose not to be opinionated about how data is stored.
mod mem;

use std::fmt::Debug;

pub use persistent_log::mem::MemLog;

use LogIndex;
use Term;
use ServerId;

/// The persistent Raft log.
///
/// The log durably stores entries, snapshots, the current term, and votes.
pub trait Log: Clone + Debug + Send + 'static {

    /// Returns the latest known term.
    fn current_term(&self) -> Term;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term. The `voted_for` value will be reset`.
    fn set_current_term(&mut self, term: Term);

    /// Increment the current term. The `voted_for` value will be reset.
    fn inc_current_term(&mut self) -> Term;

    /// Returns the candidate ID of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> Option<ServerId>;

    /// Sets the candidate ID voted for in the current term.
    fn set_voted_for(&mut self, server: ServerId);

    /// Returns the index of the latest persisted log entry (0 if the log is empty).
    fn latest_log_index(&self) -> LogIndex;

    /// Returns the term of the latest persisted log entry (0 if the log is empty).
    fn latest_log_term(&self) -> Term;

    /// Returns the entry at the provided log index.
    ///
    /// # Panic
    ///
    /// This method will panic if the entry does not exist.
    fn entry(&self, index: LogIndex) -> &[u8];

    /// Returns the term of the entry at the provided log index.
    ///
    /// # Panic
    ///
    /// This method will panic if the entry does not exist.
    fn entry_term(&self, index: LogIndex) -> Term;

    /// Appends the provided entries to the log beginning at the given index.
    fn append_entries(&mut self, from: LogIndex, entries: &[(Term, &[u8])]);
}
