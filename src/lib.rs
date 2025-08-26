use serde::{Deserialize, Serialize};
use std::io;

pub mod net;

fn has_majority(count: usize) -> bool {
    count > net::config::COUNT / 2
}

#[derive(Debug)]
pub struct State {
    // persistent state
    /// latest term server has seen
    current_term: usize,
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
}

#[derive(Debug, Deserialize, Serialize)]
struct LogEntry {
    /// when seen by leader
    term: usize,
    cmd: AppEvent,
}

#[derive(Debug, Deserialize, Serialize)]
enum Type {
    Follower(),
    Candidate {
        votes: usize,
    },
    Leader {
        /// for each host h, next_index[h] is the index of the next log entry to send h
        next_index: Vec<usize>,
        /// for each host h, match_index[h] is the highest index known to be replicated on h
        match_index: Vec<usize>,
    },
}

impl State {
    pub fn new(id: usize) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            // a dummy entry at 0: supports 1-based indexing as in the paper.
            // *term needs to be 0:* empty log cases grab this term.
            log: vec![LogEntry {
                term: 0,
                cmd: AppEvent::Noop(),
            }],
            state: AppState {},
            commit_index: 0,
            last_applied: 0,
            t: Type::Follower(),
            id,
        }
    }

    pub fn next<S: Snapshotter>(&mut self, #[expect(unused)] s: &mut S, e: Event) -> Response {
        use Event::*;
        match e {
            ApplyEntries() => {
                #[expect(unreachable_code)]
                while self.commit_index > self.last_applied {
                    self.last_applied += 1;
                    self.state.next(self.log[self.last_applied].cmd.clone());
                }
                Response::Ok()
            }
            ElectionTimeout() => self.become_candidate(),
            VoteResponse { term, vote_granted } => {
                if term > self.current_term {
                    self.become_follower();
                    self.current_term = term;
                    return Response::Ok();
                }
                use Type::*;
                match self.t {
                    Candidate { votes } => {
                        let new_votes = votes + if vote_granted { 1 } else { 0 };
                        self.t = Candidate { votes: new_votes };
                        if has_majority(new_votes) {
                            self.become_leader()
                        } else {
                            Response::Ok()
                        }
                    }
                    // maybe we've converted since the request went out; ignore it
                    _ => Response::Ok(),
                }
            }
        }
    }

    fn become_follower(&mut self) {
        self.t = Type::Follower();
    }

    fn become_candidate(&mut self) -> Response {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.t = Type::Candidate { votes: 1 };
        // who sends out the RPCs? if we had a Vec<Networkable>, we could, but we would also need
        // some form of (fake?) parallelism
        Response::StartElection {
            term: self.current_term,
            candidate_id: self.id,
            // even with 1-indexing on the protocol, our highest index is len()-1 because our
            // log is still 0-indexed.
            last_log_index: self.last_index(),
            last_log_term: self.last_entry().term,
        }
    }

    fn become_leader(&mut self) -> Response {
        self.t = Type::Leader {
            next_index: vec![self.last_index() + 1; net::config::COUNT],
            match_index: vec![0; net::config::COUNT],
        };
        Response::Heartbeat {
            term: self.current_term,
            id: self.id,
            prev_log_index: self.last_index(),
            prev_log_term: self.last_entry().term,
            commit: self.commit_index,
        }
    }

    fn last_index(&self) -> usize {
        // Dummy value guarantees we don't underflow the usize: might return 0, though
        assert!(!self.log.is_empty());
        self.log.len() - 1
    }

    fn last_entry(&self) -> &LogEntry {
        &self.log[self.last_index()]
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum Response {
    Ok(),
    /// enough details to make a RequestVote RPC
    StartElection {
        term: usize,
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: usize,
    },
    /// enough details to make a slim AppendEntries RPC
    Heartbeat {
        term: usize,
        id: usize,
        prev_log_index: usize,
        prev_log_term: usize,
        commit: usize,
    },
}

/// Some events are `crate::net::Request`s, but not all!
#[derive(Debug, Deserialize, Serialize)]
pub enum Event {
    ApplyEntries(),
    ElectionTimeout(),
    VoteResponse { term: usize, vote_granted: bool },
}

// App

#[derive(Default, Debug)]
pub struct AppState {}

impl AppState {
    fn next(&mut self, e: AppEvent) -> AppResponse {
        use AppEvent::*;
        match e {
            Noop() => todo!(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum AppEvent {
    Noop(),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum AppResponse {}

// Abstractions

pub trait Snapshotter {
    fn write<P, C>(&mut self, path: P, contents: C) -> io::Result<()>
    where
        P: AsRef<std::path::Path>,
        C: AsRef<[u8]>;

    fn read<P: AsRef<std::path::Path>>(&mut self, path: P) -> io::Result<Vec<u8>>;
}

#[cfg(test)]
mod save_restore_tests;
