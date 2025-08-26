use crate::*;
use std::collections::HashMap;

#[derive(Debug, Default)]
struct Snapshot {
    snaps: HashMap<std::path::PathBuf, Vec<u8>>,
}

impl Snapshotter for Snapshot {
    fn write<P, C>(&mut self, path: P, contents: C) -> io::Result<()>
    where
        P: AsRef<std::path::Path>,
        C: AsRef<[u8]>,
    {
        self.snaps
            .insert(path.as_ref().into(), contents.as_ref().into());
        Ok(())
    }

    fn read<P: AsRef<std::path::Path>>(&mut self, path: P) -> io::Result<Vec<u8>> {
        match self.snaps.get(path.as_ref().into()) {
            Some(v) => Ok(v.clone()),
            None => Err(io::Error::from(io::ErrorKind::NotFound)),
        }
    }
}
