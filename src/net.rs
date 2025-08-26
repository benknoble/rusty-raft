use crate::Response;
use serde::{Deserialize, Serialize};
use std::io::{self, prelude::*};
use std::net;

pub mod bytes;
mod config;

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {}

pub trait Networkable {
    fn send_request(&self, req: Request) -> Result<(), io::Error>;
    fn send_reply(&self, resp: Response) -> Result<(), io::Error>;
}

/// for servers listening to requests
pub fn request_iter(
    stream: &net::TcpStream,
) -> impl Iterator<Item = Result<Request, bytes::ParseError>> {
    let parse = bytes::Parser::from_reader(stream);
    parse.into_iter()
}

mod tcp {
    use super::*;

    pub fn reply(stream: &mut &net::TcpStream, resp: Response) -> Result<(), io::Error> {
        stream.write_all(resp.to_bytes().as_slice())
    }

    pub fn request(stream: &mut &net::TcpStream, req: Request) -> Result<(), io::Error> {
        stream.write_all(req.to_bytes().as_slice())
    }
}

/// Represents a connection to Host(id), not from
#[derive(Debug)]
pub struct Host {
    #[expect(unused)]
    id: usize,
    stream: net::TcpStream,
}

impl Host {
    pub fn new(id: usize) -> Result<Self, io::Error> {
        assert!(id < config::HOSTS.len());
        Ok(Host {
            id,
            stream: net::TcpStream::connect(config::HOSTS[id])?,
        })
    }
}

impl Networkable for Host {
    fn send_request(&self, req: Request) -> Result<(), io::Error> {
        tcp::request(&mut &self.stream, req)
    }

    fn send_reply(&self, resp: Response) -> Result<(), io::Error> {
        tcp::reply(&mut &self.stream, resp)
    }
}
