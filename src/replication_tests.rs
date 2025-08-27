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
    let mut s1 = State::new(0);
    let mut s2 = State::new(1);

    let Response::Heartbeat(reqs) = s1.become_leader() else {
        return assert!(false, "don't know how to process the response");
    };
    let req = find_req(1, reqs);
    let ae: net::Request = req.into();
    let ae: Event = ae.into();
    s2.next(&mut sn, ae);
    assert_eq!(s2.debug_log(), "[]");
    assert_eq!(s1.debug_leader(), "[1, 1, 1, 1, 1], [0, 0, 0, 0, 0]");

    let Response::ClientWaitFor(idx) = s1.next(&mut sn, Event::ClientCmd(AppEvent::Noop())) else {
        return assert!(false, "don't know how to process the response");
    };
    assert_eq!(idx, 1);
    let Response::AppendEntriesRequests(reqs) = s1.next(&mut sn, Event::CheckFollowers()) else {
        return assert!(false, "don't know how to process the response");
    };
    let req = find_req(1, reqs);
    let Response::AppendEntriesResponse(rep) = s2.next(&mut sn, net::Request::from(req).into())
    else {
        return assert!(false, "don't know how to process the response");
    };
    assert!(rep.success);
    assert_eq!(s2.debug_log(), "[(0, Noop)]");
    let Response::Ok() = s1.next(&mut sn, net::Request::from(rep).into()) else {
        return assert!(false, "don't know how to process the response");
    };
    assert_eq!(s1.debug_leader(), "[1, 2, 1, 1, 1], [0, 1, 0, 0, 0]");

    // TODO: drop a few messages, observe logs + indices, then send some more messages (good test
    // of match_index)
}

// TODO: a version with the "driver loop" that handles events
