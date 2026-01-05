# Raft (from [Rusty Boat](http://www.dabeaz.com/rusty_boat.html) August 2025)

This (not quite complete) toy Raft implementation is extracted from my work
during the Rusty Boat course.

What's missing:
- Any kind of security (notably: clients can send Raft commands, not just
  application commands… eek!)
- More testing/fuzzing/verification

## Running Raft

I like to make 3 terminals:

- `./start-cluster`: spawns jobs, and lets you manipulate them (`kill <i>`,
  `restart <i>`, `q`). Not very protective of it's data, so don't be stupid ;)
  Will kill all cluster jobs upon exit. Requires Bash v5+ (on macOS, `brew
  install …`).
- `./watch-cluster`: tail the debug logs
- `./start-client <i>`: connect to a node. The leader accepts `AppEvents` in
  S-expression notation, like `(Noop)`, `(Set "key" "val")`, or `(Get . "key")`.

"Durable storage" is in `<i>_data` in directory where the node runs. You can
do `rm *_data` to wipe the cluster clean.
