//! A `StateMachine` is a single instance of a distributed application. It is the `raft` libraries
//! responsibility to take commands from the `Client` and apply them to each `StateMachine`
//! instance in a globally consistent order.
//!
//! The `StateMachine` is interface is intentionally generic so that any distributed application
//! needing consistent state can be built on it.  For instance, a distributed hash table
//! application could implement `StateMachine`, with commands corresponding to `insert`, and
//! `remove`. The `raft` library would guarantee that the same order of `insert` and `remove`
//! commands would be seen by all consensus modules.

use std::fmt::Debug;

mod channel;
mod null;

pub use state_machine::channel::ChannelStateMachine;
pub use state_machine::null::NullStateMachine;

/// `StateMachine` is meant to be implemented such that the commands issued to
/// it via `apply()` will be reflected in your consuming application. Commands
/// sent via `apply()` have been committed in the cluser. Unlike `store`, your
/// application should consume data produced by this and accept it as truth.
///
/// If a state machine panics during any operation the raft server will crash.
pub trait StateMachine: Debug + Send + 'static {

    /// Applies a command to the state machine and returns an
    /// application-specific value.
    ///
    /// This method may mutate the state machine.
    fn apply(&mut self, command: &[u8]) -> Vec<u8>;

    /// Queries the state machine and returns an application-specific value.
    ///
    /// The method must not mutate the state machine.
    fn query(&self, query: &[u8]) -> Vec<u8>;

    /// Takes a snapshot of the state machine.
    fn snapshot(&self) -> Vec<u8>;

    /// Restores the state machine from a snapshot.
    fn restore_snapshot(&mut self, snapshot: Vec<u8>);
}
