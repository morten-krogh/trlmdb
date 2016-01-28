## General principles

The general principles applies to all key value stores. We will use lmdb terminology.
There are a set of distributed nodes. Each node has keeps a replica of a lmdb environment. The environments have 
named databases, and each database has a set of key-value pairs. The goal is to have the nodes agree on the content of the environments. The agreement is eventual, so it is possible that clients see different values for a key. However, the nodes should update each other quite fast.

### Extended key

Values in the environment are index by the named database and the key in that database. The combination is an extended key. Extended keys are byte encoded as

extended-key = database-name-including-null-byte key 

Two extended keys are qequal if and only if the databases are equal and the keys are equal. This follows from the fact that database names do not contain the null byte.


### Extended time stamp

The principle of trlmdb is that each change of a value is tagged with an extended time stamp. 
An extened time stamp contans the time of the update, a random value separate ties, and information about whether a value is present or deleted.

An extended time stamps consists of three uint32:

1. seconds after the 1. january 1970.
2. fractional seconds after the first part.
3. A random uint32 where the least significant byte is 0 for deletion and 1 for insertion/update. The random value can be reused by an open environment to increase performance. The important point is that the random uint32 can distinguish nodes from each other. The last bit must of course be adjusted for each operation. 

An extended time stamp is 12 bytes long. The time stamp has a sub nano-second precision and is applicable till around year 2100.  


### _trlmdb_time, _trlmdb_recent

Two database names are reserved: _trlmdb_time and _trlmdb_recent

They are used by the replicator. Both have extended keys as keys and extended time stamps as values.

_trlmdb_time contains all extended keys that ever existed in the environment.

_trlmdb_recent contains recent changes. This database is used by the replicator to locate changes since it last visited the environment. The replicator could in principle traverse _trlmdb_time to collect all information, but it would slow to do that often.

When user code updates a database, _trlmdb_time and _trlmdb_recent must be updated in the same transaction.

The trlmdb library does this automatically. If the user inserts values outside trlmdb, the environment might become inconsistent from the point of view of the replicator.

## User code api






