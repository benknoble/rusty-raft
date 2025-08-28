use super::*;

fn assert_fail(r: AppendEntriesResponse) {
    assert!(!r.success);
}

fn assert_success(r: AppendEntriesResponse) {
    assert!(r.success);
}

#[test]
fn test_append_entries() {
    // this particular test doesn't look at "commit" fields at all
    use AppEvent::*;

    let base = vec![
        LogEntry::new(1, Noop()),
        LogEntry::new(1, Noop()),
        LogEntry::new(1, Noop()),
        LogEntry::new(2, Noop()),
        LogEntry::new(2, Noop()),
        LogEntry::new(3, Noop()),
    ];

    let mut s = State::new(0, 1);
    s.log.extend(base.clone());
    s.current_term = 3;
    // "normal" heartbeat append
    assert_success(s.append_entries(AppendEntries {
        to: 0,
        term: 3,
        leader_id: 1,
        prev_log_index: 6,
        prev_log_term: 3,
        commit: 0,
        entries: vec![],
    }));
    assert_eq!(s.log[1..], base);

    // "normal" actual append
    assert_success(s.append_entries(AppendEntries {
        to: 0,
        term: 3,
        leader_id: 1,
        prev_log_index: 6,
        prev_log_term: 3,
        commit: 0,
        entries: vec![LogEntry::new(3, Noop())],
    }));
    assert_eq!(s.log[1..], {
        let mut x = base.clone();
        x.push(LogEntry::new(3, Noop()));
        x
    });

    // "overwrite" append
    assert_success(s.append_entries(AppendEntries {
        to: 0,
        term: 4, // this term increased: new leader
        leader_id: 1,
        prev_log_index: 4,
        prev_log_term: 2,
        commit: 0,
        // conflict! follow the leader
        entries: vec![LogEntry::new(3, Noop())],
    }));
    assert_eq!(s.log[1..], {
        let mut x: Vec<_> = base[..=4].into();
        x.push(LogEntry::new(3, Noop()));
        x
    });

    // out of order heartbeat
    assert_success(s.append_entries(AppendEntries {
        to: 0,
        term: 4, // this term increased: new leader
        leader_id: 1,
        prev_log_index: 4,
        prev_log_term: 2,
        commit: 0,
        entries: vec![],
    }));
    assert_eq!(s.log[1..], {
        let mut x: Vec<_> = base[..=4].into();
        x.push(LogEntry::new(3, Noop()));
        x
    });

    // old leader
    assert_fail(s.append_entries(AppendEntries {
        to: 0,
        term: 3,
        leader_id: 1,
        prev_log_index: 5,
        prev_log_term: 3,
        commit: 0,
        entries: vec![],
    }));

    // disagreement about old state
    assert_fail(s.append_entries(AppendEntries {
        to: 0,
        term: 4,
        leader_id: 1,
        prev_log_index: 5,
        prev_log_term: 4,
        commit: 0,
        entries: vec![],
    }));
}
