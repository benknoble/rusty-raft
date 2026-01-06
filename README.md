# Raft (from [Rusty Boat](http://www.dabeaz.com/rusty_boat.html) August 2025)

This (not quite complete) toy Raft implementation is extracted from my work
during the Rusty Boat course.

What's missing:
- Any kind of security (notably: clients can send Raft commands, not just
  application commands… eek!)
- More testing/fuzzing/verification

One notable difference from the original paper: instead of requiring read-only
(`Get`) requests to send out heartbeats (and committing no-ops when a leader
takes over), we commit them. This simplifies the protocol at what seems like the
expense of some performance (since we now have to wait until the request is
committed), but waiting for a majority of heartbeats is essentially the same
cost unless there's a significant skew in logs, which is not the normal state by
far. There _is_ a cost in storage to committing the read-only requests, but it
also means they are present in logs, which is great for debugging: we can
compute what the response would have been and track that they happened.

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
