use std::{error, fmt};
use std::error::Error as StdError;
use std::io;

use capnp;

use StateMachine;

pub enum Error<S> where S: StateMachine {
    StateMachine(S::Error),
    Capnp(capnp::Error),
    Io(io::Error),
}

impl <S> error::Error for Error<S> where S: StateMachine {
    fn description(&self) -> &str {
        match *self {
            Error::StateMachine(ref error) => error.description(),
            Error::Capnp(ref error) => error.description(),
            Error::Io(ref error) => error.description(),
        }
    }
}

impl <S> fmt::Display for Error<S> where S: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.description())
    }
}

impl <S> fmt::Debug for Error<S> where S: StateMachine, S::Error: fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::StateMachine(ref error) => error.fmt(fmt),
            Error::Capnp(ref error) => error.fmt(fmt),
            Error::Io(ref error) => error.fmt(fmt),
        }
    }
}

impl <S> error::FromError<io::Error> for Error<S> where S: StateMachine {
    fn from_error(error: io::Error) -> Error<S> {
        Error::Io(error)
    }
}

impl <S> error::FromError<capnp::Error> for Error<S> where S: StateMachine {
    fn from_error(error: capnp::Error) -> Error<S> {
        match error {
            capnp::Error::Io(error) => Error::Io(error),
            other => Error::Capnp(other),
        }
    }
}

// TODO: figure out how to make this work
/*
impl <S> error::FromError<S::Error> for Error<S> where S: StateMachine {
    fn from_error(error: S::Error) -> Error<S> {
        Error::StateMachine(error)
    }
}
*/
