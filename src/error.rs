use std::{error, fmt};
use std::error::Error as StdError;
use std::old_io::IoError;

use StateMachine;

pub enum Error<S> where S: StateMachine {
    StateMachine(S::Error),
    Io(IoError),
}

impl <S> error::Error for Error<S> where S: StateMachine {
    fn description(&self) -> &str {
        match *self {
            Error::StateMachine(ref error) => error.description(),
            Error::Io(ref error) => error.description(),
        }
    }
}

impl <S> fmt::Display for Error<S> where S: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.description())
    }
}

impl <S> error::FromError<IoError> for Error<S> where S: StateMachine {
    fn from_error(error: IoError) -> Error<S> {
        Error::Io(error)
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
