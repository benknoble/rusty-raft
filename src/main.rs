use raft::*;
use std::collections::*;
use std::io::{self, prelude::*};
use std::net as snet;
use std::sync::*;
use std::thread;

enum ClientData {
    WaitFor(usize),
    AppOutput(Arc<Vec<(AppOutput, usize, AppEvent)>>),
}

type HostOutbox = mpsc::Sender<net::Message>;
type ClientOutbox = mpsc::Sender<ClientData>;
type OutputBox = Option<ClientOutbox>;

fn main() -> Result<(), io::Error> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() <= 2 {
        eprintln!("Usage: {} <id> [<debug>]", args[0]);
        std::process::exit(1);
    }
    let id: usize = match args[1].parse() {
        Ok(id) => id,
        Err(e) => return Err(io::Error::other(e)),
    };
    if id >= net::config::HOSTS.len() {
        eprintln!("{id} out of range 0..{}", net::config::HOSTS.len());
        std::process::exit(1);
    }
    let addr = net::config::HOSTS[id];

    let debug = args.len() == 3;

    let mut state = State::new(id, net::config::HOSTS.len(), 10_000);
    // HACK: the State knows when to write, but doesn't tell us where. This is a bit intimate with
    // the internals for my taste.
    if let Ok(bytes) = FsSnapshot.read(format!("{}_data", id)) {
        state = State::from_bytes(id, net::config::HOSTS.len(), 10_000, &bytes)?;
    }
    if debug {
        state.debug();
    }
    let listener = snet::TcpListener::bind(addr)?;

    let (tx, rx) = mpsc::channel::<(Event, OutputBox)>();

    // TODO: this should probably use a ThreadPool
    // <https://github.com/benknoble/rust-book-webserver> so that clients can't DoS me?
    thread::scope(|s| {
        // manage host connections
        let mut host_outboxes: HashMap<usize, HostOutbox> = HashMap::new();
        for (host_id, &addr) in net::config::HOSTS.iter().enumerate() {
            if host_id == id {
                continue;
            }
            let (host_tx, host_rx) = mpsc::channel();
            host_outboxes.insert(host_id, host_tx);
            s.spawn(move || manage_host(id, addr, host_id, host_rx));
        }
        let send = |m: net::Message| {
            if let Err(e) = host_outboxes[&m.to()].send(m) {
                eprintln!("{id}: error sending message: {e:?}");
            }
        };
        // manage clock
        s.spawn(|| clock(id, &tx));
        // Handle client connections. These may be command "clients" or cluster "nodes". But when
        // we see a Event::ClientCmd, we'll know ;)
        s.spawn(|| {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        s.spawn(|| handle_client(stream, &tx));
                    }
                    Err(e) => {
                        eprintln!("{id}: client connection error: {e:?}");
                    }
                }
            }
        });
        // TODO: use a HashMap with random usize keys: HashSet requires Eq + Hash (which senders
        // aren't), Vec means that removing random boxes moves a bunch of memory around.
        let mut client_outboxes: Vec<ClientOutbox> = Vec::new();
        // TODO: maybe this type should enforce that we only get outboxes with client events?
        while let Ok((e, ob)) = rx.recv() {
            match state.next(&mut FsSnapshot, e) {
                Output::Ok() => continue,
                Output::Results(results) => {
                    let results = Arc::new(results);
                    client_outboxes
                        .retain(|c| c.send(ClientData::AppOutput(results.clone())).is_ok())
                }
                Output::VoteRequests(reqs) => {
                    for req in reqs {
                        send(req.into())
                    }
                }
                Output::VoteResponse(rep) => send(rep.into()),
                Output::ClientWaitFor(i, reqs) => {
                    for req in reqs {
                        send(req.into())
                    }
                    if let Some(ob) = ob
                        && ob.send(ClientData::WaitFor(i)).is_ok()
                    {
                        client_outboxes.push(ob);
                    }
                }
                Output::AppendEntriesRequests(reqs) => {
                    for req in reqs {
                        send(req.into())
                    }
                }
                Output::AppendEntriesResponse(rep) => send(rep.into()),
            }
        }
    });

    Ok(())
}

fn clock(id: usize, tx: &mpsc::Sender<(Event, OutputBox)>) {
    loop {
        thread::sleep(std::time::Duration::from_millis(1));
        if let Err(e) = tx.send((Event::Clock(), None)) {
            eprintln!("{id}: clock stopping: {e:?}");
            break;
        }
    }
}

fn handle_client(stream: snet::TcpStream, queue: &mpsc::Sender<(Event, OutputBox)>) {
    let mut parse = net::bytes::Parser::from_reader(&stream);
    for value in parse.iter() {
        let Ok(value) = value else {
            return;
        };
        match value {
            Event::ClientCmd(event) => {
                let (tx, rx) = mpsc::channel();
                if queue
                    .send((Event::ClientCmd(event.clone()), Some(tx)))
                    .is_err()
                {
                    // main server is gone, disconnect
                    return;
                }
                let log_index = loop {
                    let Ok(x) = rx.recv() else {
                        // main server is gone, disconnect
                        return;
                    };
                    if let ClientData::WaitFor(i) = x {
                        // our cmd was received
                        break i;
                    }
                };
                let result = 'result: loop {
                    let Ok(x) = rx.recv() else {
                        // main server is gone, disconnect
                        return;
                    };
                    if let ClientData::AppOutput(results) = x {
                        for (output, index, cmd) in results.iter() {
                            if log_index == *index {
                                if event == *cmd {
                                    // our cmd was committed at the expected index
                                    break 'result output.clone()
                                } else {
                                    // committed a different cmd at the index we were waiting for;
                                    // die so client can retry
                                    return;
                                }
                            }
                        }
                    }
                };
                if let Err(e) = (&stream).write_all(result.to_bytes().as_slice()) {
                    eprintln!("{e}");
                    return;
                }
            }
            _ => {
                if queue.send((value, None)).is_err() {
                    // main server is gone, disconnect
                    return;
                }
            }
        }
    }
}

fn manage_host(id: usize, addr: &str, host_id: usize, host_rx: mpsc::Receiver<net::Message>) {
    loop {
        match snet::TcpStream::connect(addr) {
            Ok(mut conn) => {
                while let Ok(m) = host_rx.recv() {
                    if let Err(e) = conn.write_all(&m.to_bytes()) {
                        eprintln!("{id}: error writing to {host_id}: {e:?}");
                        break;
                    }
                }
            }
            Err(_) => {
                let _drop_message = host_rx.try_recv();
            }
        }
    }
}

struct FsSnapshot;
impl Snapshotter for FsSnapshot {
    fn write<P, C>(&mut self, path: P, contents: C) -> io::Result<()>
    where
        P: AsRef<std::path::Path>,
        C: AsRef<[u8]>,
    {
        std::fs::write(path, contents)
    }

    fn read<P: AsRef<std::path::Path>>(&mut self, path: P) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }
}
