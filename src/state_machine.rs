use std::{error, result};
use std::io;
use std::slice::SliceExt;
use std::sync::mpsc;

pub trait StateMachine: Send + 'static {

    type Error: error::Error + Send;

    /// Applies a command to the state machine.
    fn apply(&mut self, command: &[u8]) -> result::Result<(), Self::Error>;

    /// Take a snapshot of the state machine.
    fn snapshot(&self) -> result::Result<Vec<u8>, Self::Error>;

    /// Restore a snapshot of the state machine.
    fn restore_snapshot(&mut self, snapshot: Vec<u8>) -> result::Result<(), Self::Error>;
}

/// A state machine that simply redirects all commands to a channel.
///
/// This state machine is chiefly meant for testing.
pub struct ChannelStateMachine {
    tx: mpsc::Sender<Vec<u8>>
}

impl StateMachine for ChannelStateMachine {

    // This should really be mpsc::SendError<Vec<u8>>, but it doesn't impl Error at the moment
    type Error = io::Error;

    fn apply(&mut self, command: &[u8]) -> result::Result<(), io::Error> {
        Ok(self.tx.send(command.to_vec()).unwrap())
    }

    fn snapshot(&self) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snapshot: Vec<u8>) -> result::Result<(), io::Error> {
        Ok(())
    }
}

/// A state machine with no states.
pub struct NullStateMachine;

impl StateMachine for NullStateMachine {

    // The error type is not significant to this state machine
    type Error = io::Error;

    fn apply(&mut self, _command: &[u8]) -> result::Result<(), io::Error> {
        Ok(())
    }

    fn snapshot(&self) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snapshot: Vec<u8>) -> result::Result<(), io::Error> {
        Ok(())
    }
}

