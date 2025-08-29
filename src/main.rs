use raft::*;
use std::collections::*;
use std::io::{self, prelude::*};
use std::net as snet;
use std::sync::*;
use std::thread;

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

    let mut state = State::new(id, net::config::HOSTS.len(), 100_000);
    if debug {
        state.debug();
    }
    let listener = snet::TcpListener::bind(addr)?;
    let (tx, rx) = mpsc::channel::<Event>();

    type Outbox = mpsc::Sender<net::Message>;

    // TODO: this should probably use a ThreadPool
    // <https://github.com/benknoble/rust-book-webserver> so that clients can't DoS me?
    thread::scope(|s| {
        // manage host connections
        let mut outboxes: HashMap<usize, Outbox> = HashMap::new();
        for (host_id, &addr) in net::config::HOSTS.iter().enumerate() {
            if host_id == id {
                continue;
            }
            let (host_tx, host_rx) = mpsc::channel();
            outboxes.insert(host_id, host_tx);
            s.spawn(move || manage_host(id, addr, host_id, host_rx));
        }
        let send = |m: net::Message| {
            if let Err(e) = outboxes[&m.to()].send(m) {
                eprintln!("{id}: error sending message: {e:?}");
            }
        };
        // manage clock
        s.spawn(|| clock(id, &tx));
        // Handle client connections. These may be command "clients" or cluster "nodes". But when
        // we see a Event::ClientCmd, we'll know ;)
        // TODO: need to be able to reply to clients
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
        while let Ok(e) = rx.recv() {
            match state.next(&mut FsSnapshot, e) {
                Output::Ok() => continue,
                Output::VoteRequests(reqs) => {
                    for req in reqs {
                        send(req.into())
                    }
                }
                Output::VoteResponse(rep) => send(rep.into()),
                Output::ClientWaitFor(_, reqs) => {
                    for req in reqs {
                        send(req.into())
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

fn clock(id: usize, tx: &mpsc::Sender<Event>) {
    loop {
        thread::sleep(std::time::Duration::from_millis(1));
        if let Err(e) = tx.send(Event::Clock()) {
            eprintln!("{id}: clock stopping: {e:?}");
            break;
        }
    }
}

fn handle_client(stream: snet::TcpStream, queue: &mpsc::Sender<Event>) {
    let mut parse = net::bytes::Parser::from_reader(&stream);
    for value in parse.iter() {
        let Ok(value) = value else {
            break;
        };
        if queue.send(value).is_err() {
            // main server is gone, disconnect
            return;
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
            Err(e) => {
                eprintln!("{id}: can't connect to {host_id}: {e:?}");
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
