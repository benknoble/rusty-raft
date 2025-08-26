use raft::*;
use std::io;

fn main() -> Result<(), io::Error> {
    Ok(())
}

#[expect(unused)]
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
