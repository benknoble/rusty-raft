use crate::*;
use save_restore_tests::Snapshot;

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
    let mut s1 = State::new(0, 2);
    let mut s2 = State::new(1, 2);

    let Output::Heartbeat(reqs) = s1.become_leader() else {
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
    assert_eq!(s1.debug_leader(), "4, [5, 5], [0, 4]");
    assert_eq!(
        s2.debug_log(),
        "[(0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
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
        Output::StartElection { .. } => unimplemented!("not needed in this test"),
        Output::ClientWaitFor(_) => Some(Event::CheckFollowers()),
        Output::Heartbeat(reqs) | Output::AppendEntriesRequests(reqs) => {
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
) -> Vec<mpsc::Sender<TestEvent>> {
    let (net_tx, net_rx) = mpsc::channel();
    let mut test_txs = vec![];
    let mut inboxes = vec![];
    for state in states.iter_mut() {
        let (test_tx, test_rx) = mpsc::channel();
        test_txs.push(test_tx);
        let (inbox, rx) = mpsc::channel();
        inboxes.push(inbox);
        let net_tx = net_tx.clone();
        s.spawn(|| driver(state, rx, net_tx, test_rx));
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
    let test_wait = Duration::from_millis(250);

    let mut states: Vec<_> = (0..net::config::COUNT)
        .map(|i| State::new(i, net::config::COUNT))
        .collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states);

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
        thread::sleep(test_wait);

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
    let test_wait = Duration::from_millis(450);

    let mut states: Vec<_> = (0..5).map(|i| State::new(i, 5)).collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states);

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

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });

    // not all 6 messages were delivered, but we committed all of them
    assert_eq!(
        states[0].debug_leader(),
        "6, [1, 7, 4, 4, 7], [0, 6, 3, 3, 6]"
    );
    for state in states {
        if state.id == 2 || state.id == 3 {
            assert_eq!(state.debug_log(), "[(0, Noop), (0, Noop), (0, Noop)]");
        } else {
            assert_eq!(
                state.debug_log(),
                "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
            );
        }
    }
}

#[test]
fn test_commits_with_majority_even() {
    let test_wait = Duration::from_millis(350);

    let mut states: Vec<_> = (0..4).map(|i| State::new(i, 4)).collect();
    states[0].become_leader();

    thread::scope(|s| {
        let test_txs = start_net_and_states(&s, &mut states);

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
        thread::sleep(test_wait);

        // shutdown
        for tx in test_txs {
            tx.send(TestEvent::Quit).expect("sent");
        }
    });

    // not all 6 messages were delivered, but we committed all of them
    assert_eq!(states[0].debug_leader(), "6, [1, 7, 4, 7], [0, 6, 3, 6]");
    for state in states {
        if state.id == 2 {
            assert_eq!(state.debug_log(), "[(0, Noop), (0, Noop), (0, Noop)]");
        } else {
            assert_eq!(
                state.debug_log(),
                "[(0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop), (0, Noop)]"
            );
        }
    }
}

#[test]
fn candidate_converts_when_it_sees_a_leader() {
    let mut s1 = State::new(0, 2);
    let mut s2 = State::new(1, 2);
    let mut sn: Snapshot = Default::default();
    s1.become_candidate();
    s2.become_candidate();

    // assume s2 wins election…

    let Output::Heartbeat(reqs) = s2.become_leader() else {
        return assert!(false, "don't know how to process the output");
    };

    let req = find_req(0, reqs);
    let ae: net::Message = req.into();
    s1.next(&mut sn, ae.into());
    assert!(match s1.t {
        Type::Follower() => true,
        _ => false,
    });
}
