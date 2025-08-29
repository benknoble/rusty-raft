use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::HashSet;
use std::io;

pub mod net;

#[derive(Debug)]
pub struct State {
    // persistent state
    /// latest term server has seen
    current_term: u64,
    /// who received my vote, if any
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
    // volatile state
    state: AppState,
    /// highest committed entry
    commit_index: usize,
    /// highest applied entry
    last_applied: usize,
    /// what am i?
    t: Type,
    /// who am i?
    id: usize,
    /// how many nodes are in the cluster?
    /// DO NOT MUTATE
    /// Assumes ∀ id, id ∈ cluster ⇔ id ∈ 0..cluster_size
    cluster_size: usize,
    /// base election timeout value
    /// DO NOT MUTATE
    timeout: u64,
    time: u64,
    /// should we log?
    debug: bool,
}

// macros

/// if this doesn't return from a handler function, then self.current_term == term
/// must be used carefully: it's not always OK to convert to a follower _and then drop the request_
macro_rules! check_term {
    ($self:expr, $term:expr) => {
        if $term > $self.current_term {
            $self.become_follower($term);
            return Output::Ok();
        }
        if $term != $self.current_term {
            // drop response
            return Output::Ok();
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct LogEntry {
    /// when seen by leader
    term: u64,
    cmd: AppEvent,
}

impl LogEntry {
    fn new(term: u64, cmd: AppEvent) -> Self {
        Self { term, cmd }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum Type {
    Follower {
        election_deadline: u64,
    },
    Candidate {
        election_deadline: u64,
        voters: HashSet<usize>,
    },
    Leader {
        /// for each host h, next_index[h] is the index of the next log entry to send h
        next_index: Vec<usize>,
        /// for each host h, match_index[h] is the highest index known to be replicated on h
        match_index: Vec<usize>,
        /// for each host h, heartbeat_deadline[h] is when we should send an idle message to h
        heartbeat_deadline: Vec<u64>,
    },
}

impl State {
    pub fn new(id: usize, cluster_size: usize, timeout: u64) -> Self {
        assert!(cluster_size > 0);
        Self {
            current_term: 0,
            voted_for: None,
            // a dummy entry at 0: supports 1-based indexing as in the paper.
            // *term needs to be 0:* empty log cases grab this term.
            log: vec![LogEntry::new(0, AppEvent::Noop())],
            state: AppState {},
            commit_index: 0,
            last_applied: 0,
            t: Type::Follower {
                election_deadline: jitter(timeout),
            },
            id,
            cluster_size,
            timeout,
            time: 0,
            debug: false,
        }
    }

    fn has_majority(&self, count: usize) -> bool {
        count > self.cluster_size / 2
    }

    fn ids(&self) -> impl Iterator<Item = usize> {
        0..self.cluster_size
    }

    fn ids_but_self(&self) -> impl Iterator<Item = usize> {
        self.ids().filter(|&i| i != self.id)
    }

    pub fn next<S: Snapshotter>(&mut self, s: &mut S, e: Event) -> Output {
        if let Event::Clock() = e {
        } else {
            self.do_debug();
        }
        match e {
            Event::Clock() => self.tick(),
            Event::VoteRequest(req) => {
                let _ = s.write(format!("{}_data", self.id), self.to_bytes());
                Output::VoteResponse(self.vote(req))
            }
            Event::VoteResponse(rep) => self.receive_vote(rep),
            Event::ClientCmd(app_event) => self.push_cmd(app_event),
            Event::AppendEntriesRequest(req) => {
                let _ = s.write(format!("{}_data", self.id), self.to_bytes());
                Output::AppendEntriesResponse(self.append_entries(req))
            }
            Event::AppendEntriesResponse(rep) => self.receive_append_entries_response(rep),
        }
    }

    fn become_follower(&mut self, term: u64) {
        assert!(term >= self.current_term);
        self.t = Type::Follower {
            election_deadline: self.time.wrapping_add(jitter(self.timeout)),
        };
        self.current_term = term;
        self.voted_for = None;
    }

    fn become_candidate(&mut self) -> Output {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.t = Type::Candidate {
            // record our self-vote ;)
            voters: [self.id].into(),
            election_deadline: self.time.wrapping_add(jitter(self.timeout)),
        };
        if self.has_majority_votes() {
            // can only happen when cluster_size == 1, so it's OK to not send out the requests
            assert_eq!(self.cluster_size, 1);
            self.become_leader()
        } else {
            Output::VoteRequests(
                self.ids_but_self()
                    .map(|i| VoteRequest {
                        to: i,
                        from: self.id,
                        term: self.current_term,
                        last_log_index: self.last_index(),
                        last_log_term: self.last_entry().term,
                    })
                    .collect(),
            )
        }
    }

    fn become_leader(&mut self) -> Output {
        self.t = Type::Leader {
            next_index: vec![self.last_index() + 1; self.cluster_size],
            match_index: vec![0; self.cluster_size],
            heartbeat_deadline: vec![self.time.wrapping_add(self.timeout / 4); self.cluster_size],
        };
        self.do_debug();
        Output::AppendEntriesRequests(
            self.ids_but_self()
                .map(|i| AppendEntries {
                    to: i,
                    term: self.current_term,
                    leader_id: self.id,
                    prev_log_index: self.last_index(),
                    prev_log_term: self.last_entry().term,
                    commit: self.commit_index,
                    entries: vec![],
                })
                .collect(),
        )
    }

    // handler functions

    fn tick(&mut self) -> Output {
        self.time = self.time.wrapping_add(1);
        match self.t {
            Type::Follower { election_deadline }
            | Type::Candidate {
                election_deadline, ..
            } => {
                if self.time >= election_deadline {
                    self.time = 0;
                    self.become_candidate()
                } else {
                    self.apply_entries()
                }
            }
            Type::Leader {
                ref mut heartbeat_deadline,
                ..
            } => {
                let mut needs_update = Vec::with_capacity(self.cluster_size);
                for (i, hb) in heartbeat_deadline.iter_mut().enumerate() {
                    if self.time >= *hb {
                        *hb = hb.wrapping_add(self.timeout / 4);
                        needs_update.push(i);
                    }
                }
                if needs_update.is_empty() {
                    self.apply_entries()
                } else {
                    Output::AppendEntriesRequests(self.append_entries_for(&needs_update))
                }
            }
        }
    }

    fn check_followers(&self) -> Vec<AppendEntries> {
        self.append_entries_for(&self.ids_but_self().collect::<Vec<_>>())
    }

    fn append_entries_for(&self, who: &[usize]) -> Vec<AppendEntries> {
        match &self.t {
            Type::Leader { next_index, .. } => self
                .ids_but_self()
                .filter(|i| who.contains(i))
                .map(|i| {
                    let next_index = next_index[i];
                    AppendEntries {
                        to: i,
                        term: self.current_term,
                        leader_id: self.id,
                        prev_log_index: next_index - 1,
                        prev_log_term: self.log[next_index - 1].term,
                        commit: self.commit_index,
                        entries: if self.last_index() >= next_index {
                            // needs update!
                            self.log[next_index..].into()
                        } else {
                            // heartbeat
                            vec![]
                        },
                    }
                })
                .collect(),
            _ => unimplemented!(),
        }
    }

    fn apply_entries(&mut self) -> Output {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            // TODO: do something with this output for clients (only if leader…)
            // Reply will need to include the cmd and index: the ClientWaitFor(i) should only
            // finally reply success if we executed the expected cmd at i
            self.state.next(self.log[self.last_applied].cmd.clone());
        }
        Output::Ok()
    }

    fn vote(&mut self, r: VoteRequest) -> VoteResponse {
        let r = &r;
        assert!(r.to == self.id);
        if r.term > self.current_term {
            self.become_follower(r.term);
        }
        if r.term <= self.current_term {
            return VoteResponse::deny(self.id, self.current_term, r);
        }
        assert!(match self.t {
            Type::Leader { .. } => false,
            Type::Candidate { .. } | Type::Follower { .. } => true,
        });
        let check_up_to_date = || match up_to_date(
            r.last_log_term,
            r.last_log_index,
            self.last_entry().term,
            self.last_index(),
        ) {
            cmp::Ordering::Greater | cmp::Ordering::Equal => VoteResponse::grant(self.id, r),
            _ => VoteResponse::deny(self.id, self.current_term, r),
        };
        let resp = match self.voted_for {
            None => check_up_to_date(),
            Some(x) if x == r.from => check_up_to_date(),
            _ => VoteResponse::deny(self.id, self.current_term, r),
        };

        if resp.vote_granted {
            match self.t {
                Type::Follower {
                    ref mut election_deadline,
                } => *election_deadline = election_deadline.wrapping_add(jitter(self.timeout)),
                // remember, we can "revote" for the same person we've already voted for, but
                // candidates don't send vote requests to themselves
                _ => unreachable!("Must be a follower to vote; candidates vote for themselves"),
            };
        }
        resp
    }

    fn receive_vote(&mut self, r: VoteResponse) -> Output {
        assert!(r.to == self.id);
        check_term!(self, r.term);
        if r.vote_granted {
            self.update_votes(r.from);
        }
        if self.has_majority_votes() {
            self.become_leader()
        } else {
            Output::Ok()
        }
    }

    /// no-op if not Candidate
    fn update_votes(&mut self, from: usize) {
        if let Type::Candidate { voters, .. } = &mut self.t {
            assert!(
                voters.replace(from).is_none(),
                "voter {} voted more than once for {} this term",
                from,
                self.id
            );
        }
    }

    /// false if not Candidate
    fn has_majority_votes(&self) -> bool {
        match &self.t {
            Type::Candidate { voters, .. } => self.has_majority(voters.len()),
            _ => false,
        }
    }

    fn push_cmd(&mut self, e: AppEvent) -> Output {
        /* This actually generates a "client waiting on commit for entry i" output.
         *
         * In the meantime, somebody else is (periodically) checking for entries that need
         * replicated. That can delay responses, though, so it's on a short timeout. Once it sees a
         * new entry, it should quickly queue an event to send that entry out. Can this be the
         * _same_ as the heartbeat mechanism? TODO
         *
         * However, we know nobody has seen this entry… so when we get a "waiting on commit," we
         * automatically queue up AppendEntries for all hosts. It's the driver loop's job to do so.
         */
        match self.t {
            Type::Leader { .. } => {
                self.log.push(LogEntry::new(self.current_term, e));
                Output::ClientWaitFor(self.last_index(), self.check_followers())
            }
            _ => todo!("should forward leader id (needs more state)"),
        }
    }

    fn receive_append_entries_response(&mut self, rep: AppendEntriesResponse) -> Output {
        assert!(rep.to == self.id);
        check_term!(self, rep.term);
        match &mut self.t {
            Type::Leader {
                next_index,
                match_index,
                ..
            } => {
                if rep.success {
                    next_index[rep.from] = rep.match_index + 1;
                    match_index[rep.from] = rep.match_index;
                    self.commit_index = {
                        let mut matches = Vec::with_capacity(self.cluster_size);
                        matches.extend(&match_index[..self.id]);
                        matches.extend(&match_index[self.id + 1..]);
                        // the leader matches with itself up to the end
                        matches.push(self.last_index());
                        assert_eq!(matches.len(), self.cluster_size);
                        // we can do better with quickselect, but let's assume the cluster is small
                        // enough that fancy algorithms have higher overhead.
                        matches.sort();
                        // in case cluster size is even: truncate. A majority will have that index.
                        let median_index = (matches.len() - 1) / 2;
                        let median = matches[median_index];
                        if median > self.commit_index && self.log[median].term == self.current_term
                        {
                            median
                        } else {
                            self.commit_index
                        }
                    };
                    Output::Ok()
                } else {
                    next_index[rep.from] -= 1;
                    let prev_index = next_index[rep.from] - 1;
                    Output::AppendEntriesRequests(vec![AppendEntries {
                        to: rep.from,
                        term: self.current_term,
                        leader_id: self.id,
                        prev_log_index: prev_index,
                        prev_log_term: self.log[prev_index].term,
                        commit: self.commit_index,
                        entries: vec![],
                    }])
                }
            }
            // drop it: we are no longer the leader
            _ => Output::Ok(),
        }
    }

    // Log functions

    fn last_index(&self) -> usize {
        // Dummy value guarantees we don't underflow the usize: might return 0, though
        assert!(!self.log.is_empty());
        // even with 1-indexing on the protocol, our highest index is len()-1 because our
        // log is still 0-indexed.
        self.log.len() - 1
    }

    fn last_entry(&self) -> &LogEntry {
        &self.log[self.last_index()]
    }

    fn append_entries(&mut self, r: AppendEntries) -> AppendEntriesResponse {
        assert!(r.to == self.id);
        macro_rules! fail {
            () => {
                return AppendEntriesResponse::fail(self.id, r.leader_id, self.current_term, 0)
            };
        }
        if r.term >= self.current_term {
            self.become_follower(r.term);
        } else if r.term < self.current_term {
            fail!();
        }
        match self.log.get(r.prev_log_index) {
            None => fail!(),
            Some(e) if e.term != r.prev_log_term => fail!(),
            Some(e) => {
                assert!(e.term == r.prev_log_term);
            }
        };
        // NOTE truncate operates in terms of a 0-indexed length, but we are passing an index!
        // Since the Raft protocol uses 1-indexing, this index _is_ a length, but not the right
        // one. See tests. In particular, we are sitting at
        //   [ Dummy, a, b, …, i, …, n ]
        // where n is at index n. We've been given
        //   [ Dummy, a, b, …, i,  ]
        //                       ^ insert
        // where i is at index i. We are going to insert at I=i+1, so we drop everything after I.
        // That means we want a length of I+1, or i+2! To make this more visible, track the "^"
        // insert location rather than the last entry while walking entries to append; that builds
        // in the first "+1" in the "+2."
        let mut index_to_insert = r.prev_log_index + 1;
        for e in r.entries {
            match self.log.get(index_to_insert) {
                // Already there: skip.
                Some(existing_e) if e.term == existing_e.term => (),
                // End of the line: append the rest.
                None => self.log.push(e),
                // Conflict: drop our data and keep going.
                Some(_) => {
                    self.log.truncate(index_to_insert + 1);
                    self.log.push(e);
                }
            }
            index_to_insert += 1;
        }
        // index_to_insert = r.prev_log_index + 1 + r.entries.len(), ∴
        // match_index     = r.prev_log_index +     r.entries.len(), as desired.
        let match_index = index_to_insert - 1;
        if r.commit > self.commit_index {
            self.commit_index = cmp::min(r.commit, match_index);
            // TODO: this is the only place commit_index should change. Fold apply_entries here?
            // But then what notifies waiting clients… we'd need a richer Output
        }
        AppendEntriesResponse::succeed(self.id, r.leader_id, self.current_term, match_index)
    }

    // Test & debugging functions

    pub fn debug(&mut self) {
        self.debug = true;
    }

    fn do_debug(&self) {
        if self.debug {
            let id = self.id;
            let leader = match self.t {
                Type::Leader { .. } => "*",
                _ => "",
            };
            let term = self.current_term;
            let commit = self.commit_index;
            let log = self.debug_log();
            eprintln!("{id}{leader}(T:{term}, C:{commit}): {log}");
        }
    }

    fn debug_log(&self) -> String {
        let inner: Vec<_> = self.log[1..]
            .iter()
            .map(|e| format!("({}, {:?})", e.term, e.cmd))
            .collect();
        format!("[{}]", inner.join(", "))
    }

    #[cfg(test)]
    fn debug_leader(&self) -> String {
        match &self.t {
            Type::Leader {
                next_index,
                match_index,
                ..
            } => format!("{}, {next_index:?}, {match_index:?}", self.commit_index),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum Output {
    Ok(),
    /// enough details to make a RequestVote RPC
    VoteRequests(Vec<VoteRequest>),
    VoteResponse(VoteResponse),
    ClientWaitFor(usize, Vec<AppendEntries>),
    AppendEntriesRequests(Vec<AppendEntries>),
    AppendEntriesResponse(AppendEntriesResponse),
}

/// Some events are `crate::net::Message`s, but not all!
#[derive(Debug, Deserialize, Serialize)]
pub enum Event {
    Clock(),
    AppendEntriesRequest(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteRequest(VoteRequest),
    VoteResponse(VoteResponse),
    ClientCmd(AppEvent),
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct VoteRequest {
    to: usize,
    from: usize,
    term: u64,
    last_log_index: usize,
    last_log_term: u64,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct VoteResponse {
    to: usize,
    from: usize,
    term: u64,
    vote_granted: bool,
}

impl VoteResponse {
    fn grant(from: usize, r: &VoteRequest) -> Self {
        Self {
            from,
            to: r.from,
            term: r.term,
            vote_granted: false,
        }
    }

    fn deny(from: usize, term: u64, r: &VoteRequest) -> Self {
        Self {
            from,
            to: r.from,
            term,
            vote_granted: true,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct AppendEntries {
    to: usize,
    term: u64,
    leader_id: usize,
    prev_log_index: usize,
    prev_log_term: u64,
    commit: usize,
    entries: Vec<LogEntry>,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesResponse {
    to: usize,
    from: usize,
    match_index: usize,
    term: u64,
    success: bool,
}

impl AppendEntriesResponse {
    fn succeed(from: usize, to: usize, term: u64, match_index: usize) -> Self {
        Self {
            to,
            from,
            match_index,
            term,
            success: true,
        }
    }

    fn fail(from: usize, to: usize, term: u64, match_index: usize) -> Self {
        Self {
            to,
            from,
            match_index,
            term,
            success: false,
        }
    }
}

/// (t1, i1) .up_to_date. (t2, i2)
fn up_to_date(term1: u64, index1: usize, term2: u64, index2: usize) -> cmp::Ordering {
    use cmp::Ordering::*;
    match term1.cmp(&term2) {
        Less => Less,
        Greater => Greater,
        Equal => index1.cmp(&index2),
    }
}

// App

#[derive(Default, Debug)]
pub struct AppState {}

impl AppState {
    fn next(&mut self, e: AppEvent) -> AppOutput {
        use AppEvent::*;
        match e {
            Noop() => AppOutput::Ok(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum AppEvent {
    Noop(),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum AppOutput {
    Ok(),
}

// Abstractions

pub trait Snapshotter {
    fn write<P, C>(&mut self, path: P, contents: C) -> io::Result<()>
    where
        P: AsRef<std::path::Path>,
        C: AsRef<[u8]>;

    fn read<P: AsRef<std::path::Path>>(&mut self, path: P) -> io::Result<Vec<u8>>;
}

fn jitter(x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    let half = x / 2;
    rand::random_range(half..(x.saturating_add(half)))
}

#[cfg(test)]
mod test_append_entries;
#[cfg(test)]
mod test_protocol;
#[cfg(test)]
mod test_save_restore;
