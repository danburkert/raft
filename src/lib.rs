#![allow(dead_code)]

#![feature(
    core,
    io,
    net,
    std_misc,
    unsafe_destructor,
)]

extern crate capnp;
extern crate rand;
#[macro_use] extern crate log;

pub mod store;

mod error;
mod event;
mod messenger;
mod node;
mod state_machine;

use std::result;
use std::num::Int;
use std::ops;

pub use error::Error;

pub mod messages_capnp {
  include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

pub use state_machine::StateMachine;

pub type Result<T, S> where S: StateMachine = result::Result<T, Error<S>>;

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(u64);
impl ops::Add<u64> for Term {
    type Output = Term;
    fn add(self, rhs: u64) -> Term {
        Term(self.0.checked_add(rhs).expect("overflow while incrementing Term"))
    }
}
impl ops::Sub<u64> for Term {
    type Output = Term;
    fn sub(self, rhs: u64) -> Term {
        Term(self.0.checked_sub(rhs).expect("underflow while decrementing Term"))
    }
}
impl Term {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// The index of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(pub u64);
impl ops::Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_add(rhs).expect("overflow while incrementing LogIndex"))
    }
}
impl ops::Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_sub(rhs).expect("underflow while decrementing LogIndex"))
    }
}
impl LogIndex {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
