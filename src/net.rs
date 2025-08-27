use crate::*;
use serde::{Deserialize, Serialize};
use std::io::{self, prelude::*};
use std::net;

pub mod bytes;
pub mod config;

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    AppendEntriesRequest(super::AppendEntries),
    AppendEntriesResponse(super::AppendEntriesResponse),
}

impl Message {
    pub fn to(&self) -> usize {
        match self {
            Message::AppendEntriesRequest(r) => r.to,
            Message::AppendEntriesResponse(r) => r.to,
        }
    }
}

impl From<Message> for Event {
    fn from(r: Message) -> Self {
        match r {
            Message::AppendEntriesRequest(inner) => Event::AppendEntriesRequest(inner),
            Message::AppendEntriesResponse(inner) => Event::AppendEntriesResponse(inner),
        }
    }
}

impl AppendEntries {
    fn to_message(&self) -> Message {
        Message::AppendEntriesRequest(self.clone())
    }
}

impl From<AppendEntries> for Message {
    fn from(a: AppendEntries) -> Self {
        a.to_message()
    }
}

impl From<&AppendEntries> for Message {
    fn from(a: &AppendEntries) -> Self {
        a.to_message()
    }
}

impl AppendEntriesResponse {
    fn into_message(self) -> Message {
        Message::AppendEntriesResponse(self)
    }
}

impl From<AppendEntriesResponse> for Message {
    fn from(a: AppendEntriesResponse) -> Self {
        a.into_message()
    }
}

pub trait Networkable {
    fn send_message(&self, req: Message) -> Result<(), io::Error>;
}

/// for servers listening to messages
pub fn message_iter(
    stream: &net::TcpStream,
) -> impl Iterator<Item = Result<Message, bytes::ParseError>> {
    let parse = bytes::Parser::from_reader(stream);
    parse.into_iter()
}

mod tcp {
    use super::*;

    pub fn message(stream: &mut &net::TcpStream, req: Message) -> Result<(), io::Error> {
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
    fn send_message(&self, req: Message) -> Result<(), io::Error> {
        tcp::message(&mut &self.stream, req)
    }
}
