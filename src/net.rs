use crate::*;
use serde::{Deserialize, Serialize};
use std::io::{self, prelude::*};
use std::net;

pub mod bytes;
pub mod config;

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    AppendEntriesRequest(super::AppendEntries),
    AppendEntriesResponse(super::AppendEntriesResponse),
}

impl From<Request> for Event {
    fn from(r: Request) -> Self {
        match r {
            Request::AppendEntriesRequest(inner) => Event::AppendEntriesRequest(inner),
            Request::AppendEntriesResponse(inner) => Event::AppendEntriesResponse(inner),
        }
    }
}

impl AppendEntries {
    fn to_request(&self) -> Request {
        Request::AppendEntriesRequest(self.clone())
    }
}

impl From<AppendEntries> for Request {
    fn from(a: AppendEntries) -> Self {
        a.to_request()
    }
}

impl From<&AppendEntries> for Request {
    fn from(a: &AppendEntries) -> Self {
        a.to_request()
    }
}

impl AppendEntriesResponse {
    fn into_request(self) -> Request {
        Request::AppendEntriesResponse(self)
    }
}

impl From<AppendEntriesResponse> for Request {
    fn from(a: AppendEntriesResponse) -> Self {
        a.into_request()
    }
}

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
