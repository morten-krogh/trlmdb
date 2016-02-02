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

The length do not include themselves, so the total length is message_length plus the length of the prefix itself.

A length is encoded in big endian format using a vriable number of bytes.  The rule is that the most
significant bit in the bytes of the length are control bits.  The last byte in the length has a
value of 0 for the most significant bit. The previous bytes start with the bit 1.  The length is the
valuer obtained by conctenating the lower 7 bits from all bytes.

Examples:

0 < length < 127: 0xxxxxxx
200 = 128 + 64 + 8: 10000001 01001000

The messages 

