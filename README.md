# Raft (from [Rusty Boat](http://www.dabeaz.com/rusty_boat.html) August 2025)

[![This project is considered experimental](https://img.shields.io/badge/status-experimental-critical.svg)](https://benknoble.github.io/status/experimental/)

This (incomplete) toy Raft implementation is extracted from my work
during the Rusty Boat course.

What's missing:
- Any kind of security (notably: clients can send Raft commands, not just
  application commands… eek!)
- More testing/fuzzing/verification: there are probably bugs in the protocol
  implementation `raft::State::next`.
- Leader forwarding: if a client connects to the wrong node, it doesn't know
  where the leader is to send its command
- Intelligent client replies: any event that is _not_ sending the results of a
  client command to the client hangs up on the client unceremoniously, so it
  isn't clear whether the command was not committed or some other thing
  happened.
- Generic application: the implementation is cleanly wed to a specific key-value
  store. I believe it would be easy to permit generically specifying the actual
  application via a trait.
- (Nice-to-have) Programmatic description of the driver loop: the architecture
  is described below, but it would be even better to move the driver to a module
  and explain in code what shape it needs to take.

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

## Architecture

This implementation is heavily inspired by:

- the [actor model](https://en.wikipedia.org/wiki/Actor_model) and [message passing](https://wiki.c2.com/?MessagePassingConcurrency) concurrency;
- [functional core, imperative shell architecture](https://www.destroyallsoftware.com/talks/boundaries); and
- the [Command pattern](https://en.wikipedia.org/wiki/Command_pattern).

In other words, it's one giant (nested) state machine.

This code considers each Raft or client node in the system an actor, which can
send and receive messages. Raft nodes typically execute code like the main
executable of this project, which drives a `raft::State` through various
operations. Client nodes typically execute code like the `client` executable of
this project: send an application command to the leader and receive a reply.

System configuration of the Raft cluster is specified in `raft::net::config`,
and the `raft` executable has some source-tunable configuration values as well.
(Hard-coding configuration---and application, for that matter---keeps the
implementation focused on the essentials.)

The driver loop for the main `raft` executable is further implemented using
internal (on-node) actors: each of multiple threads of execution including the
main thread might send and receive messages to keep the system going. While I'm
used to message passing concurrency treating message sends and receives as
synchronization points, this code uses only asynchronous buffered channels.
_Other implementations of the driver loop are possible and encouraged._

The main algorithm is implemented in `raft::State`'s transitions, though getting
it correct also requires the driver loop to handle certain aspects. The
`raft::State` object is "pure" in that it is self-contained and performs no I/O
except through memory (de)allocation. Thus it needs a driver loop as an
imperative shell to interact with truly stateful systems (see previous
paragraph). A state object yields commands (`raft::Output`) for the driver to
execute when taking a transition in response to an input (`raft::Event`).

In order to handle persistent storage, `raft::State` objects consume a
`raft::Snapshotter` trait. Implementing it for tests, _e.g._, with a `HashMap`,
is straightforward. Similarly we hang it off a
[ZST](https://doc.rust-lang.org/nomicon/exotic-sizes.html#zero-sized-types-zsts)
to interact with the file system for real use.

There are some byte-munging utilities in the `raft::net` module tree that I
won't discuss here. Ditto the tests, some of which use an unrolled,
single-threaded driver loop that may be of interest (some tests use a faked
network, too).

What follow is a short diagram + description of the `raft::State` protocol and
driver loop from the perspective of a single node. Connections on the right side
of the `Node` represent internal actors; connections on the left represent
external actors (other nodes and clients). The `driver` loop runs on the main
thread.

```

     ⋮
 +-------+                                                  +-------+
 |       |                                              +---| clock |
…| Hosts |… <----[ Raft messages ]--------+             |   +-------+
 |       |                                |             v
 +-------+                                |      +--------+
     ⋮                                    |      |        |    +--------------------+
                                          |      |  Node  |<-->| Raft host outboxes |
                                          |      |  ----  |    +--------------------+
                                          +----> |        |
                                          |      |  main  |    +------------------------+
                                          |      |   =    |<-->| New client connections |
                                          |      | driver |    +------------------------+
                                          |      |        |
      ⋮                                   |      +--------+
 +---------+                              |
 |         |                              |
…| Clients |… <----[ Client messages ]----+
 |         |
 +---------+
      ⋮

```

### Driver Loop

The driver loop's job is _mostly_ simple: each other actor has a way to write
to the driver's inbox. The driver takes `raft::Event`s off that inbox, feeds
them through the state machine, dispatches the `raft::Output` accordingly, and
loops. The interesting bit is dispatching the outputs.

Some outputs are noops or requests to send messages (_e.g._,
`raft::Output::VoteRequests` tells the driver to send messages to the other
hosts asking for votes). We always (attempt to) send all the messages we're
asked to send.

The interesting outputs are:

- `raft::Output::Results`: we've committed some application commands and are
  ready to share the results, and
- `raft::Output::ClientWaitFor`: we've received an application command and need
  to tell the client to wait for the results.

To handle these, we also allow receiving an outbox with the `raft::Event` we
need to process, so that we can send information back to actors. We store the
active outboxes, and when we receive application results in
`raft::Output::Results` we send them to each outbox. We only retain the ones
that successfully sent, so a client who has since hung up because it got the
results it wanted will become inactive and leave the driver's storage.

Unfortunately, this means our notion of "active outboxes" is always one cycle
stale; we won't know until the next `raft::Output::Results` if a particular
outbox can be dropped. A more complex inter-actor protocol could alleviate this
problem.

We only ever store outboxes when dispatching `raft::Output::ClientWaitFor`
(which by the way _also_ comes with an AppendEntries RPC call for the other
hosts). The node tells the client which index to wait for, then keeps it in the
active outboxes (unless they are full, in which case we drop it for
rate-limiting). Every so often, we try to shrink the space used for these
outboxes to prevent wasting memory; tune with `CLIENT_RECLAMATION` in the `raft`
executable source.

### Clock actor

This actor sends `raft::Event::Clock()` to the driver loop's inbox. This
implementation does so _roughly_ every 1ms, but other implementations are
possible. The clock's beats control

- whether the node starts an election, if it is a follower or candidate
- whether the node sends heartbeat AppendEntries RPCs, if it is a leader

Election and heartbeat deadlines are jittered from a source-tunable
configuration value in the `raft` executable (or passed to the `raft::State`
constructor). Heartbeat deadlines are occur about 4 times per election deadline.

In either case, if no special action is needed, the node instead applies
committed entries. Doing so every clock tick helps minimize the delay between
knowing an entry is committed and applying it to the internal application state
machine (but assumes such an application is fast; otherwise, delays on each
clock tick could compound). If the internal application is slow, a slower clock
frequency might help. Alternatively, a redesign with separates the internal
application on a separate thread would avoid such delay.

NB due to the driver loop's inbox, even if clock ticks create a delay, we won't
_miss_ an event that's been generated. It just may take a while to get to it. In
extreme cases, the inbox could fill up and block threads waiting to send (which
might causes us to miss externally generated events), but as long each go-round
of the driver loop completes without halting we'll eventually keep taking
elements out of the inbox.

### Raft host outbox actors

Each Raft host, including the current Node, maintains a set of outboxes: one for
each other host in the cluster. Each host outbox actor is responsible for a
single other host in the network: it listens on that host's outbox for messages,
and then sends those messages over TCP to the host.

A message is any of the RPC request or response for Vote and AppendEntries RPCs.

If the connection is broken, we try to reestablish it; if that fails, we attempt
to pull a messages off the outbox and drop it before trying to reconnect again.
Dropping messages helps prevent the outbox from filling up with unsendable
messages (or flooding a newly awake host with old messages), but does mean the
protocol has to tolerate messages being dropped. Since this is a networked
distributed system, that's perfectly within the bounds of the Raft protocol.

### Client connection actors

There are 2 kinds of actors here. The first is your typical "when we get a TCP
connection, spawn a thread to handle it." The second are those threads.

Each such client connection actor reads `raft::Event` values off the network
stream and hands them over to the driver loop. (This is what I meant by "clients
can send Raft commands, not just application commands.") These actors are used
to process both Raft protocol messages from other hosts in the cluster and
application commands from a client. A more secure design would enforce that
these 2 are separate, although in this design it is possible to implement a
"debugging client" (or "chaos client" depending on your tastes) that injects
arbitrary Raft protocol messages into the driver loop.

In the case of application commands from clients specifically, we do some extra
work to return the results of the command to the client. We provide an outbox
with the command to the driver loop, and we wait for information on the inbox
that tells us (a) which committed index to wait for and then (b) that we
committed the expected command at that index with an output. If all that
succeeds, we send the result back over the connection to the client. Otherwise,
we ungracefully hangup on the client as a way of signaling an error.

A more robust mechanism might distinguish "client should retry" (committed a
different command, other scenarios) from "there was an error".
