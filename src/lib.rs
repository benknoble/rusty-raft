use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct LogEntry {
    /// when seen by leader
    term: usize,
    cmd: AppEvent,
}

impl LogEntry {
    fn new(term: usize, cmd: AppEvent) -> Self {
        Self { term, cmd }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum Type {
    Follower(),
    Candidate {
        voters: HashSet<usize>,
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
            log: vec![LogEntry::new(0, AppEvent::Noop())],
            state: AppState {},
            commit_index: 0,
            last_applied: 0,
            t: Type::Follower(),
            id,
        }
    }

    pub fn next<S: Snapshotter>(&mut self, #[expect(unused)] s: &mut S, e: Event) -> Response {
        match e {
            Event::ApplyEntries() => {
                #[expect(unreachable_code)]
                while self.commit_index > self.last_applied {
                    self.last_applied += 1;
                    // TODO: do something with this response for clients (only if leader…)
                    self.state.next(self.log[self.last_applied].cmd.clone());
                }
                Response::Ok()
            }
            Event::ElectionTimeout() => {
                use Type::*;
                match self.t {
                    // maybe we've converted before the most recent timeout; ignore it
                    Leader { .. } => Response::Ok(),
                    _ => self.become_candidate(),
                }
            }
            Event::VoteResponse {
                from,
                term,
                vote_granted,
            } => {
                if term > self.current_term {
                    self.become_follower(term);
                    return Response::Ok();
                }
                if term != self.current_term {
                    // drop response
                    return Response::Ok();
                }
                use Type::*;
                match &mut self.t {
                    Candidate { voters } => {
                        if vote_granted {
                            assert!(
                                voters.replace(from).is_none(),
                                "voter {} voted more than once for {} this term",
                                from,
                                self.id
                            );
                        }
                        if has_majority(voters.len()) {
                            self.become_leader()
                        } else {
                            Response::Ok()
                        }
                    }
                    // maybe we've converted since the request went out; ignore it
                    _ => Response::Ok(),
                }
            }
            Event::ClientCmd(app_event) => {
                /* This actually generates a "client waiting on commit for entry i" response.
                 *
                 * In the meantime, somebody else is (periodically) checking for entries that need
                 * replicated. That can delay responses, though, so it's on a short timeout. Once
                 * it sees a new entry, it should quickly queue an event to send that entry out.
                 * Can this be the _same_ as the heartbeat mechanism? TODO
                 * Heartbeat loop per follower:
                 *      - hey, time to fire off an AE again!
                 *      - might be empty, might not
                 *      - no need to reset timer: just send more than strictly necessary
                 *
                 * However, we know nobody has seen this entry… so when we get a "waiting on
                 * commit," we automatically queue up AppendEntries for all hosts. It's the driver
                 * loop's job to do so.
                 */
                use Type::*;
                match self.t {
                    Leader { .. } => {
                        self.log.push(LogEntry::new(self.current_term, app_event));
                        Response::ClientWaitFor(self.last_index())
                    }
                    _ => todo!("should forward leader id (needs more state)"),
                }
            }
            Event::AppendEntriesRequest(req) => {
                Response::AppendEntriesResponse(self.append_entries(req))
            }
            #[expect(unused)]
            Event::AppendEntriesResponse(rep) => {
                todo!()
            }
            Event::CheckFollowers() => self.check_followers(),
        }
    }

    fn become_follower(&mut self, term: usize) {
        assert!(term > self.current_term);
        self.t = Type::Follower();
        self.current_term = term;
        self.voted_for = None;
    }

    fn become_candidate(&mut self) -> Response {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.t = Type::Candidate {
            voters: HashSet::new(),
        };
        // who sends out the RPCs? if we had a Vec<Networkable>, we could, but we would also need
        // some form of (fake?) parallelism
        Response::StartElection {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_index(),
            last_log_term: self.last_entry().term,
        }
    }

    fn become_leader(&mut self) -> Response {
        self.t = Type::Leader {
            next_index: vec![self.last_index() + 1; net::config::COUNT],
            match_index: vec![0; net::config::COUNT],
        };
        Response::Heartbeat(
            net::config::ids()
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

    fn check_followers(&self) -> Response {
        use Type::*;
        match &self.t {
            Leader { next_index, .. } => Response::AppendEntriesRequests(
                net::config::ids()
                    .filter(|&i| i != self.id)
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
            ),
            // drop it: we're no longer leader
            _ => Response::Ok(),
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
        // TODO: save state before responding
        macro_rules! fail {
            () => {
                return AppendEntriesResponse::fail(self.current_term)
            };
        }
        if r.term < self.current_term {
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
        if r.commit > self.commit_index {
            self.commit_index = std::cmp::min(r.commit, self.last_index());
        }
        AppendEntriesResponse::succeed(self.current_term)
    }

    #[cfg(test)]
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
            } => format!("{next_index:?}, {match_index:?}"),
            _ => unimplemented!(),
        }
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
    ClientWaitFor(usize),
    Heartbeat(Vec<AppendEntries>),
    AppendEntriesRequests(Vec<AppendEntries>),
    AppendEntriesResponse(AppendEntriesResponse),
}

/// Some events are `crate::net::Request`s, but not all!
#[derive(Debug, Deserialize, Serialize)]
pub enum Event {
    ApplyEntries(),
    ElectionTimeout(),
    CheckFollowers(),
    AppendEntriesRequest(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    VoteResponse {
        from: usize,
        term: usize,
        vote_granted: bool,
    },
    ClientCmd(AppEvent),
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct AppendEntries {
    to: usize,
    term: usize,
    leader_id: usize,
    prev_log_index: usize,
    prev_log_term: usize,
    commit: usize,
    entries: Vec<LogEntry>,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AppendEntriesResponse {
    term: usize,
    success: bool,
}

impl AppendEntriesResponse {
    fn succeed(term: usize) -> Self {
        Self {
            term,
            success: true,
        }
    }

    fn fail(term: usize) -> Self {
        Self {
            term,
            success: false,
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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
mod append_entries_test;
#[cfg(test)]
mod replication_tests;
#[cfg(test)]
mod save_restore_tests;
