use crate::*;
use test_save_restore::Snapshot;

fn find_req(id: usize, reqs: Vec<AppendEntries>) -> AppendEntries {
    let mut req = None;
    for req_to_send in reqs {
        if req_to_send.to == id {
            req = Some(req_to_send);
            break;
        }
    }
    req.expect("Should send a message to host 1")
}

#[test]
fn test_2_servers_manual() {
    let mut sn: Snapshot = Default::default();
    let mut s1 = State::new(0, 2, 0);
    let mut s2 = State::new(1, 2, 0);

    let Output::AppendEntriesRequests(reqs) = s1.become_leader() else {
        return assert!(false, "don't know how to process the output");
    };
    let req = find_req(1, reqs);
    let ae: net::Message = req.into();
    let ae: Event = ae.into();
    s2.next(&mut sn, ae);
    assert_eq!(s2.debug_log(), "[]");
    assert_eq!(s1.debug_leader(), "0, [1, 1], [0, 0]");

    let Output::ClientWaitFor(idx) = s1.next(&mut sn, Event::ClientCmd(AppEvent::Noop())) else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(idx, 1);
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, Event::CheckFollowers()) else {
        return assert!(false, "don't know how to process the output");
    };
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert!(rep.success);
    assert_eq!(s2.debug_log(), "[(0, Noop)]");
    let Output::Ok() = s1.next(&mut sn, net::Message::from(rep).into()) else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(s1.debug_leader(), "1, [1, 2], [0, 1]");

    // drop a few AppendEntries calls: driver loop would normally trigger a CheckFollowers and
    // handle any results immediately when it gets the ClientWaitFor outputs.
    for _ in 1..=3 {
        s1.next(&mut sn, Event::ClientCmd(AppEvent::Noop()));
    }
    assert_eq!(
        s1.debug_log(),
        "[(0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
    );
    // new term: s1 is leader again
    s1.become_candidate();
    // skip simulated voting…
    s1.become_leader();
    assert_eq!(s1.debug_leader(), "1, [5, 5], [0, 0]");

    // send heartbeats
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, Event::CheckFollowers()) else {
        return assert!(false, "don't know how to process the output");
    };
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    // fail! missing entries
    assert!(!rep.success);
    // handle failure
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, net::Message::from(rep).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(s1.debug_leader(), "1, [5, 4], [0, 0]");

    // try again
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    // fail! still missing entries
    assert!(!rep.success);
    // handle failure
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, net::Message::from(rep).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(s1.debug_leader(), "1, [5, 3], [0, 0]");

    // try again
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    // fail! still missing entries
    assert!(!rep.success);
    assert_eq!(s2.debug_log(), "[(0, Noop)]");
    // handle failure
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, net::Message::from(rep).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(s1.debug_leader(), "1, [5, 2], [0, 0]");

    // try again
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    // success!
    assert!(rep.success);
    // handle it
    let Output::Ok() = s1.next(&mut sn, net::Message::from(rep).into()) else {
        return assert!(false, "don't know how to process the output");
    };
    assert_eq!(s1.debug_leader(), "1, [5, 2], [0, 1]");

    // keep going to get up to speed…
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, Event::CheckFollowers()) else {
        return assert!(false, "don't know how to process the output");
    };
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert!(rep.success);
    let Output::Ok() = s1.next(&mut sn, net::Message::from(rep).into()) else {
        return assert!(false, "don't know how to process the output");
    };
    // not committed yet: these entries are from the previous term, and we _cannot_ commit those
    // (see Figure 8)
    assert_eq!(s1.debug_leader(), "1, [5, 5], [0, 4]");
    assert_eq!(
        s2.debug_log(),
        "[(0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
    );

    // replicate a new entry from our term, committing everything
    s1.next(&mut sn, Event::ClientCmd(AppEvent::Noop()));
    let Output::AppendEntriesRequests(reqs) = s1.next(&mut sn, Event::CheckFollowers()) else {
        return assert!(false, "don't know how to process the output");
    };
    let req = find_req(1, reqs);
    let Output::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Message::from(req).into())
    else {
        return assert!(false, "don't know how to process the output");
    };
    assert!(rep.success);
    let Output::Ok() = s1.next(&mut sn, net::Message::from(rep).into()) else {
        return assert!(false, "don't know how to process the output");
    };
    // NB we don't update next_index on the leader ;)
    assert_eq!(s1.debug_leader(), "5, [5, 6], [0, 5]");
    assert_eq!(
        s2.debug_log(),
        "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (1, Noop)]"
    );
}

use std::sync::*;
use std::thread;
use std::time::Duration;

enum TestEvent {
    Quit,
    Pause,
    Resume,
    E(Event),
}

fn driver(
    s: &mut State,
    rx: mpsc::Receiver<Event>,
    tx: mpsc::Sender<net::Message>,
    test_rx: mpsc::Receiver<TestEvent>,
) {
    let mut sn: Snapshot = Default::default();
    let mut go = true;

    enum Cont {
        None,
        Some(Event),
        Abort,
    }
    use Cont::*;

    let mut handle_event = |e: Event| match s.next(&mut sn, e) {
        Output::Ok() => None,
        Output::VoteRequests(reqs) => {
            for req in reqs {
                if tx.send(req.into()).is_err() {
                    return Abort;
                }
            }
            None
        }
        Output::VoteResponse(rep) => {
            if tx.send(rep.into()).is_err() {
                Abort
            } else {
                None
            }
        }
        Output::ClientWaitFor(_) => Some(Event::CheckFollowers()),
        Output::AppendEntriesRequests(reqs) => {
            for req in reqs {
                if tx.send(req.into()).is_err() {
                    return Abort;
                }
            }
            None
        }
        Output::AppendEntriesResponse(rep) => {
            if tx.send(rep.into()).is_err() {
                Abort
            } else {
                None
            }
        }
    };

    let mut rec_event = |e: Event| {
        let mut e = e;
        loop {
            match handle_event(e) {
                None => break,
                Some(new_e) => e = new_e,
                Abort => return Abort,
            }
        }
        None
    };

    use TestEvent::*;
    loop {
        match test_rx.try_recv() {
            Ok(Quit) => break,
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => break,
            Ok(Pause) => go = false,
            Ok(Resume) => go = true,
            Ok(E(e)) => match rec_event(e) {
                Abort => break,
                None => (),
                Some(_) => unimplemented!("recursive error!"),
            },
        }
        if !go {
            continue;
        }
        match rx.try_recv() {
            Ok(e) => match rec_event(e) {
                Abort => break,
                None => (),
                Some(_) => unimplemented!("recursive error!"),
            },
            Err(mpsc::TryRecvError::Empty) => (),
            Err(mpsc::TryRecvError::Disconnected) => break,
        }
    }
}

fn start_net_and_states<'scope, 'env>(
    s: &'scope thread::Scope<'scope, 'env>,
    states: &'scope mut Vec<State>,
    election_timeout: Option<Duration>,
) -> Vec<mpsc::Sender<TestEvent>> {
    let (net_tx, net_rx) = mpsc::channel();
    let mut test_txs = vec![];
    let mut inboxes = vec![];
    for state in states.iter_mut() {
        let (test_tx, test_rx) = mpsc::channel();
        test_txs.push(test_tx);
        let (inbox, rx) = mpsc::channel();
        inboxes.push(inbox.clone());
        let net_tx = net_tx.clone();
        s.spawn(|| driver(state, rx, net_tx, test_rx));
        let election_timeout = election_timeout;
        if let Some(t) = election_timeout {
            s.spawn(move || {
                loop {
                    thread::sleep(t);
                    // TODO: sends too many (or handler needs to be smarter): otherwise we trigger an
                    // election when we don't need to (say, because we've just seen a heartbeat and should
                    // restart our timer). The effect is that we reset leaders more often than we need!
                    if inbox.send(Event::ElectionTimeout()).is_err() {
                        break;
                    }
                }
            });
        }
    }

    // network
    s.spawn(move || {
        loop {
            let Ok(r) = net_rx.recv() else {
                break;
            };
            if inboxes[r.to()].send(r.into()).is_err() {
                break;
            }
        }
    });

    test_txs
}

#[test]
fn test_many_auto() {
    let test_wait = Duration::from_millis(25);

    let mut states: Vec<_> = (0..net::config::COUNT)
        .map(|i| State::new(i, net::config::COUNT, 0))
        .collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, None);

        // could try to fuzz (ignoring elections):
        // - send a ClientCmd to test_txs[0]
        // - sleep for a bit
        // - pause any non-0
        // - resume any non-0

        // send a few commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait);

        // pause 2, 3
        test_txs[2].send(TestEvent::Pause).expect("sent");
        test_txs[3].send(TestEvent::Pause).expect("sent");

        // send a few more commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait);

        // resume 2, 3
        test_txs[2].send(TestEvent::Resume).expect("sent");
        test_txs[3].send(TestEvent::Resume).expect("sent");

        // flaky?
        thread::sleep(test_wait * 2);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });

    // all 6 messages were delivered
    assert_eq!(
        states[0].debug_leader(),
        "6, [1, 7, 7, 7, 7], [0, 6, 6, 6, 6]"
    );
    for state in states {
        assert_eq!(
            state.debug_log(),
            "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
        );
    }
}

#[test]
fn test_commits_with_majority_odd() {
    let test_wait = Duration::from_millis(50);

    let mut states: Vec<_> = (0..5).map(|i| State::new(i, 5, 0)).collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, None);

        // send a few commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait);

        // pause 2, 3
        test_txs[2].send(TestEvent::Pause).expect("sent");
        test_txs[3].send(TestEvent::Pause).expect("sent");

        // send a few more commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait * 2);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });

    // not all 6 messages were delivered, but we committed all of them
    let s = &states[0].debug_leader();
    assert!(
        // states 2 and 3 can be in a few different positions
        // see test_commits_with_majority_even assertions for details
        (3..=5)
            .flat_map(|two| (3..=5).map(move |three| *s
                == format!(
                    "6, [1, 7, {two}, {three}, 7], [0, 6, {}, {}, 6]",
                    two - 1,
                    three - 1
                )))
            .reduce(|x, y| x || y)
            .expect("non-empty"),
        "{}",
        s
    );
    for state in states {
        if state.id != 2 && state.id != 3 {
            assert_eq!(
                state.debug_log(),
                "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
            );
        }
    }
}

#[test]
fn test_commits_with_majority_even() {
    let test_wait = Duration::from_millis(50);

    let mut states: Vec<_> = (0..4).map(|i| State::new(i, 4, 0)).collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, None);

        // send a few commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait);

        // pause 2
        test_txs[2].send(TestEvent::Pause).expect("sent");

        // send a few more commands
        for _ in 1..=3 {
            test_txs[0]
                .send(TestEvent::E(Event::ClientCmd(AppEvent::Noop())))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait * 2);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });

    // not all 6 messages were delivered, but we committed all of them
    let s = states[0].debug_leader();
    assert!(
        // according to plan
        s == "6, [1, 7, 4, 7], [0, 6, 3, 6]"
        // one fewer
            || s == "6, [1, 7, 3, 7], [0, 6, 2, 6]"
            // one extra
            || s == "6, [1, 7, 5, 7], [0, 6, 4, 6]",
        "{}",
        s
    );
    for state in states {
        if state.id != 2 {
            assert_eq!(
                state.debug_log(),
                "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
            );
        }
    }
}

#[test]
fn candidate_converts_when_it_sees_a_leader() {
    let mut s1 = State::new(0, 2, 0);
    let mut s2 = State::new(1, 2, 0);
    let mut sn: Snapshot = Default::default();
    s1.become_candidate();
    s2.become_candidate();

    // assume s2 wins election…

    let Output::AppendEntriesRequests(reqs) = s2.become_leader() else {
        return assert!(false, "don't know how to process the output");
    };

    let req = find_req(0, reqs);
    let ae: net::Message = req.into();
    s1.next(&mut sn, ae.into());
    assert!(match s1.t {
        Type::Follower { .. } => true,
        _ => false,
    });
}

#[test]
fn election_degenerate() {
    let mut states = vec![State::new(0, 1, 100)];
    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, Some(Duration::from_millis(25)));
        test_txs[0]
            .send(TestEvent::E(Event::ElectionTimeout()))
            .expect("sent");
        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });
    assert!(match states[0].t {
        Type::Leader { .. } => true,
        _ => false,
    });
}

#[test]
fn election_two() {
    let test_wait = Duration::from_millis(50);
    let mut states = (0..2).map(|i| State::new(i, 2, 200)).collect();
    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, Some(test_wait * 3));
        test_txs[0]
            .send(TestEvent::E(Event::ElectionTimeout()))
            .expect("sent");

        // flaky?
        thread::sleep(test_wait);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });
    assert!(
        match states[0].t {
            Type::Leader { .. } => true,
            _ => false,
        } || match states[1].t {
            Type::Follower { .. } => true,
            _ => false,
        }
    );
}

#[test]
fn election_many_one() {
    let test_wait = Duration::from_millis(50);
    let mut states = (0..5).map(|i| State::new(i, 5, 200)).collect();
    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, Some(test_wait * 5));
        test_txs[0]
            .send(TestEvent::E(Event::ElectionTimeout()))
            .expect("sent");

        // flaky?
        thread::sleep(test_wait * 2);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });
    assert!(match states[0].t {
        Type::Leader { .. } => true,
        _ => false,
    });
    for state in &states[1..] {
        assert!(match state.t {
            Type::Follower { .. } => true,
            _ => false,
        });
    }
}

#[test]
fn election_many_many() {
    let test_wait = Duration::from_millis(50);
    let mut states = (0..5).map(|i| State::new(i, 5, 200)).collect();
    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states, Some(test_wait * 3));
        for tx in test_txs.iter() {
            tx.send(TestEvent::E(Event::ElectionTimeout()))
                .expect("sent");
        }

        // flaky?
        thread::sleep(test_wait);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });
    // no assertions; just check we don't break an internal invariant
}
