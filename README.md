# pegasusDB
PegasusDB is a fault tolerant, linearizable, distributed key-value store that is built on top of [Raft](https://github.com/a3y3/Raft). It offers the following operations:
- Put(k, v): sets value `v` for the key `k`, overwriting any previous value.  
- Append(k, v): appends value `v` to key `k`. If `k` doesn't exist, behaves as a `Put`.
- Get(k): fetches the value stored for key `k`.

This database works well in the case of unreliable networks (where messages can be lost or reordered), networks where partitions can randomly occur and heal, and servers that can crash and reboot at any time. Additionally, multiple clients (AKA `clerks`) can send concurrent read/write requests, and pegasus guarantees _linearizability_ for all such requests.

### What does linearizability mean?
Put simply, linearizability offers a guarantee that once a request returns successfully, all clients immediately observe the results of that request. And for concurrent requests, linearizability guarantees that the requests execute in some specific, deterministic order.

For example:
- Client X sends `Put(balance, 5)`, Client Y sends `Put(balance, 6)`, and X sends `Append(balance, 0)`

In this case, should the value for `balance` be 50 or 60? Can it be 6? It depends on several factors. If the first call returned before Y sent its Put, the only possibility is that balance is 60 (note that X will wait for it's own call to return before sending append 0). However, if the call does not return, it can be either 50 or 60.

Linearizability is easy to guarantee if there's a single server. However, when there are multiple servers and especially when the network is unreliable and servers can crash, we need to ensure that all servers execute the same execution order, regardless of what it is. So in our case of "it can be either 50 or 60", it can be one of the two. Not both. If a server decides an execution order of 50, then ALL servers must decide the same order.

#### How linearizability is guaranteed
For multiple servers, guaranteeing linearizability is easy to explain, but hard to implement. The idea is basically: accept a command through an RPC, replicate that command across multiple servers, and once a majority of the servers have stored that command, return success to that RPC.

Where's the hard part then?
- A request RPC (from client to a key/value server) fails but the replication (from kv server to multiple kv servers) succeeds, so the client retries the request and now we have duplicate executions for the same command (for example, duplicate appends)
- Network partitions could arise such that a leader accepts a command, but then loses leadership and no longer tries to replicate the command.
- A leader could accept a request, replicate it successfully, but fail just before responding to the client, who will then retry.

Of course, these are just some of the problems you could run into - the full list is super long!

### How to run
The database relies on specific assumptions on how clients connect and talk to it. The easiest way to "run" this is to test it.
- `export PEGASUS_VERBOSE=1` (will turn on logs for pegasus)
- `export VERBOSE=1` (will turn on logs for Raft)
- `go test 3A` (will test using concurrent clients, unreliable networks, partitions, faulty/failing servers, and a combination of all of these)

### Acknowledgements
[MIT's course on Distributed Systems](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html) \[6.824\] provided the skeleton for this, with an absolutely ruthless testing framework. Props to them for making it free and accessible! 
