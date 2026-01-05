use raft::*;
use std::io::{self, prelude::*};
use std::net as snet;

fn main() -> Result<(), io::Error> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        println!("Usage: {} <leader_id>", args[0]);
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
    let client = snet::TcpStream::connect(addr).expect("could not connect");

    let mut buf = String::new();
    let mut parse = net::bytes::Parser::from_reader(&client);

    loop {
        buf.clear();
        print!("kv.raft> ");
        io::stdout().flush()?;
        match io::stdin().read_line(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        };
        match buf.parse::<AppEvent>() {
            Err(e) => {
                println!("{e:?}");
                continue;
            }
            Ok(e) => {
                let e = Event::ClientCmd(e);
                (&client).write_all(&e.to_bytes())?;
                let value: AppOutput = parse.parse()?;
                println!("{:?}", value);
            }
        }
    }
}
