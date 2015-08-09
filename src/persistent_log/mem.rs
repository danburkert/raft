use persistent_log::Log;
use LogIndex;
use ServerId;
use Term;

/// This is a `Log` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
#[derive(Clone, Debug)]
pub struct MemLog {
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<(Term, Vec<u8>)>,
}

impl MemLog {

    pub fn new() -> MemLog {
        MemLog {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

impl Log for MemLog {

    fn current_term(&self) -> Term {
        self.current_term
    }

    fn set_current_term(&mut self, term: Term) {
        self.voted_for = None;
        self.current_term = term;
    }

    fn inc_current_term(&mut self) -> Term {
        self.voted_for = None;
        self.current_term = self.current_term + 1;
        self.current_term()
    }

    fn voted_for(&self) -> Option<ServerId> {
        self.voted_for
    }

    fn set_voted_for(&mut self, address: ServerId) {
        self.voted_for = Some(address);
    }

    fn latest_log_index(&self) -> LogIndex {
        LogIndex(self.entries.len() as u64)
    }

    fn latest_log_term(&self) -> Term {
        let len = self.entries.len();
        if len == 0 {
            Term::from(0)
        } else {
            self.entries[len - 1].0
        }
    }

    fn entry(&self, index: LogIndex) -> &[u8] {
        &self.entries[(index - 1).as_u64() as usize].1
    }

    fn entry_term(&self, index: LogIndex) -> Term {
        self.entries[(index - 1).as_u64() as usize].0
    }

    fn append_entries(&mut self, from: LogIndex, entries: &[(Term, &[u8])]) {
        assert!(self.latest_log_index() + 1 >= from);
        self.entries.truncate((from - 1).as_u64() as usize);
        self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec())));
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use LogIndex;
    use ServerId;
    use Term;
    use persistent_log::Log;

    #[test]
    fn test_current_term() {
        let mut store = MemLog::new();
        assert_eq!(Term(0), store.current_term());
        store.set_voted_for(ServerId::from(0));
        store.set_current_term(Term(42));
        assert_eq!(None, store.voted_for());
        assert_eq!(Term(42), store.current_term());
        store.inc_current_term();
        assert_eq!(Term(43), store.current_term());
    }

    #[test]
    fn test_voted_for() {
        let mut store = MemLog::new();
        assert_eq!(None, store.voted_for());
        let id = ServerId::from(0);
        store.set_voted_for(id);
        assert_eq!(Some(id), store.voted_for());
    }

    #[test]
    fn test_append_entries() {
        let mut store = MemLog::new();
        assert_eq!(LogIndex::from(0), store.latest_log_index());
        assert_eq!(Term::from(0), store.latest_log_term());

        store.append_entries(LogIndex(1), &[(Term::from(0), &[1]),
                                            (Term::from(0), &[2]),
                                            (Term::from(0), &[3]),
                                            (Term::from(1), &[4])]);
        assert_eq!(LogIndex::from(4), store.latest_log_index());
        assert_eq!(Term::from(1), store.latest_log_term());
        assert_eq!(&*vec![1u8], store.entry(LogIndex::from(1)));
        assert_eq!(&*vec![2u8], store.entry(LogIndex::from(2)));
        assert_eq!(&*vec![3u8], store.entry(LogIndex::from(3)));
        assert_eq!(&*vec![4u8], store.entry(LogIndex::from(4)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(1)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(2)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(3)));
        assert_eq!(Term::from(1), store.entry_term(LogIndex::from(4)));

        store.append_entries(LogIndex::from(4), &[]);
        assert_eq!(LogIndex(3), store.latest_log_index());
        assert_eq!(Term::from(0), store.latest_log_term());
        assert_eq!(&*vec![1u8], store.entry(LogIndex::from(1)));
        assert_eq!(&*vec![2u8], store.entry(LogIndex::from(2)));
        assert_eq!(&*vec![3u8], store.entry(LogIndex::from(3)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(1)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(2)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(3)));

        store.append_entries(LogIndex::from(3), &[(Term(2), &[3]), (Term(3), &[4])]);
        assert_eq!(LogIndex(4), store.latest_log_index());
        assert_eq!(Term::from(3), store.latest_log_term());

        assert_eq!(&*vec![1u8], store.entry(LogIndex::from(1)));
        assert_eq!(&*vec![2u8], store.entry(LogIndex::from(2)));
        assert_eq!(&*vec![3u8], store.entry(LogIndex::from(3)));
        assert_eq!(&*vec![4u8], store.entry(LogIndex::from(4)));

        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(1)));
        assert_eq!(Term::from(0), store.entry_term(LogIndex::from(2)));
        assert_eq!(Term::from(2), store.entry_term(LogIndex::from(3)));
        assert_eq!(Term::from(3), store.entry_term(LogIndex::from(4)));
    }
}
