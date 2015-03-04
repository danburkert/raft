use std::{io, result};
use std::old_io::net::ip::SocketAddr;

use store::Store;

use LogIndex;
use Term;

#[derive(Clone, Debug)]
pub struct MemStore {
    current_term: Term,
    voted_for: Option<SocketAddr>,
    entries: Vec<(Term, Vec<u8>)>,
}

impl MemStore {

    pub fn new() -> MemStore {
        MemStore {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

impl Store for MemStore {

    type Error = io::Error;

    fn current_term(&self) -> result::Result<Term, io::Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> result::Result<(), io::Error> {
        Ok(self.current_term = term)
    }

    fn inc_current_term(&mut self) -> result::Result<Term, io::Error> {
        self.current_term = self.current_term + 1;
        self.current_term()
    }

    fn voted_for(&self) -> result::Result<Option<SocketAddr>, io::Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: Option<SocketAddr>) -> result::Result<(), io::Error> {
        Ok(self.voted_for = address)
    }

    fn latest_index(&self) -> result::Result<LogIndex, io::Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn entry(&self, index: LogIndex) -> result::Result<(Term, &[u8]), io::Error> {
        let (term, ref bytes) = self.entries[index.as_u64() as usize - 1];
        Ok((term, &bytes))
    }

    fn append_entries(&mut self,
                      from: LogIndex,
                      entries: &[(Term, &[u8])])
                      -> result::Result<(), io::Error> {
        assert!(self.latest_index().unwrap() + 1 >= from);
        self.entries.truncate(from.as_u64() as usize - 1);
        Ok(self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec()))))
    }

    fn truncate_entries(&mut self, index: LogIndex) -> result::Result<(), io::Error> {
        assert!(self.latest_index().unwrap() >= index);
        Ok(self.entries.truncate(index.as_u64() as usize - 1))
    }
}

#[cfg(test)]
mod test {

    use std::str::FromStr;
    use std::old_io::net::ip::SocketAddr;

    use super::*;
    use LogIndex;
    use Term;
    use store::Store;

    #[test]
    fn test_current_term() {
        let mut store = MemStore::new();
        assert_eq!(Term(0), store.current_term().unwrap());
        store.set_current_term(Term(42));
        assert_eq!(Term(42), store.current_term().unwrap());
        store.inc_current_term();
        assert_eq!(Term(43), store.current_term().unwrap());
    }

    #[test]
    fn test_voted_for() {
        let mut store = MemStore::new();
        assert_eq!(None, store.voted_for().unwrap());
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        store.set_voted_for(Some(addr.clone()));
        assert_eq!(Some(addr), store.voted_for().unwrap());
    }

    #[test]
    fn test_append_entries() {
        let mut store = MemStore::new();

        store.append_entries(LogIndex(1), &[(Term(0), &[1]),
                                            (Term(0), &[2]),
                                            (Term(0), &[3]),
                                            (Term(1), &[4])]);
        assert_eq!(LogIndex(4), store.latest_index().unwrap());
        assert_eq!((Term(0), &*vec![1u8]), store.entry(LogIndex(1)).unwrap());
        assert_eq!((Term(0), &*vec![2u8]), store.entry(LogIndex(2)).unwrap());
        assert_eq!((Term(0), &*vec![3u8]), store.entry(LogIndex(3)).unwrap());
        assert_eq!((Term(1), &*vec![4u8]), store.entry(LogIndex(4)).unwrap());

        store.append_entries(LogIndex(4), &[]);
        assert_eq!(LogIndex(3), store.latest_index().unwrap());
        assert_eq!((Term(0), &*vec![1u8]), store.entry(LogIndex(1)).unwrap());
        assert_eq!((Term(0), &*vec![2u8]), store.entry(LogIndex(2)).unwrap());
        assert_eq!((Term(0), &*vec![3u8]), store.entry(LogIndex(3)).unwrap());

        store.append_entries(LogIndex(3), &[(Term(2), &[3]),
                                            (Term(3), &[4])]);
        assert_eq!(LogIndex(4), store.latest_index().unwrap());
        assert_eq!((Term(0), &*vec![1u8]), store.entry(LogIndex(1)).unwrap());
        assert_eq!((Term(0), &*vec![2u8]), store.entry(LogIndex(2)).unwrap());
        assert_eq!((Term(2), &*vec![3u8]), store.entry(LogIndex(3)).unwrap());
        assert_eq!((Term(3), &*vec![4u8]), store.entry(LogIndex(4)).unwrap());
    }
}
