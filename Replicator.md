The replicator is a combined client and server. Each instance of a lmdb database has one associated replicator.
The replicator runs in its own process independently of the apps using the database.

A replicator and its database has a uniqwue node name. When the replicator starts it must be given
the node names and internet addresses that it should connect to. The replicator creates a thread per
remote node that it should connect to. Furthermore the replicator listens for incoming tcp
connections, and creates a thread for each accepted connection. Ea h of the threads operate
independently. If a pair of replicators have the other's node name specified in the conf file, there
could be two tcp connections between the same pair of nodes. This does not matter much since the
replicators use the same database, and the database keeps track of the important state.

When two replicators are connected with a tcp connection, they behave symmetrically; in other words there is no
client server distinction. The distinction only exists as reagrds creating the tcp connection.

From now on we will describe the operation of a pair of nodes with a connection between them. The
nodes send messages between themselves.

Format of messages:

A message consists of an array of binaries, or byte arrays, with length prefixes. A message containing
the binaries, B1, B2, ..., Bn has the following form

message_length length_B1 B1 length_B2 B2 .... length_Bn Bn

The messages are defined in message.c.


The messages are:

1.
"node" - node_name

This message is sent by the nodes at the beginning of the tcp connection. The replicators will not
proceed unless they know each other's node name.

2.

"time" - flag - time - key - value

flag is "tt", "tf", "ft", or "ff" where f is false and t is true.
The first boolean in the flag describes whether the sender knows the key/value for the time stamp.
The second boolean describes whether the sender knows that the receiver has the key/value for the time stamp.

The end goal of the two communicating nodes is to get into the "tt" state for both nodes.

time is the 20 byte time stamp.

key and value are optional. Their presence is seen from the length.

The two nodes send the second type of message back and forth and update each other. The gal is to have both nodes know all time stamps and key values and know that the other node knows them as well.


State machine

The replicator 



