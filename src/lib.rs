use serde::{Deserialize, Serialize};
use std::io;

pub mod net;

// might need to _only_ ser/de certain components?
#[derive(Debug, Deserialize, Serialize)]
pub struct State {}

impl State {
    pub fn next<S: Snapshotter>(&mut self, #[expect(unused)] s: &mut S, e: Event) -> Response {
        #[expect(unused)]
        use Event::*;
        match e {}
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum Response {}

/// Some events are `crate::net::Request`s, but not all!
#[derive(Debug, Deserialize, Serialize)]
pub enum Event {}

pub trait Snapshotter {
    fn write<P, C>(&mut self, path: P, contents: C) -> io::Result<()>
    where
        P: AsRef<std::path::Path>,
        C: AsRef<[u8]>;

    fn read<P: AsRef<std::path::Path>>(&mut self, path: P) -> io::Result<Vec<u8>>;
}

#[cfg(test)]
mod save_restore_tests;
