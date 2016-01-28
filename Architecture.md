## General principles

The general principles applies to all key value stores. We will use lmdb terminology.
There are a set of distributed nodes. Each node has keeps a replica of a lmdb environment. The environments have 
named databases, and each database has a set of key-value pairs. The goal is to have the nodes agree on the content of the environments. The agreement is eventual, so it is possible that clients see different values for a key. However, the nodes should update each other quite fast.

### Time stamp

The principle of trlmdb is that each value is augmented with a time stamp. The time stamps consists of three uint32:

1. seconds after the 1. january 1970.
2. fractional seconds after the first part.
3. A random uint32 that is fixed each time an environment is opened.

The reason for the random part is to avoid ties in the unlikely event that two nodes update the same key at the exact same time point. The random part is, for performance reasons, constant within a given opening of an environment of a node becasuse updates of a node happens sequentially and have a natural ordering; in the unlikely event that two updates of a key came at the same fraction of a second in the same environment instance, the latter one would overwrite the first one in any case.  

A time stamp has the following byte format

time-stamp = seconds fration-seconds random

A time stamp is 12 bytes long.

### Extended key

Values in the environment are index by the named database and the key in that database. The combination is an extended key. Extended keys are byte encoded as

extended-key = database-name-including-null-byte key 



### Extended values

Values inserted into an environment for a given database and key are extended with the time stamp and a presence byte that describes whether the value is present or absent. The presence byte is needed to keep track of deletions; information about a deleted key is needed to update the replicating nodes.

The exact byte format of the inserted value is

extended-value = time-stamp presence-byte value

The extended value is hence 13 bytes larger than the value.

### _trlmdb

A special reserved database of name _trlmdb is kept in the environment by trlmdb. 
This database is used to tell the replicator that there are changes in the envrionment. 
The replicator will read the _trlmdb database, store the information in its own environment and delete the entries afterwards. The special databsase, _trlmdb, enhances performance; the information in _trlmdb could be found by the replicator by traversing all databases.
The keys in _trlmdb are extended keys, and the values are time stamps.

When user code updates a database, _trlmdb must be updated int he same transaction.

The trlmdb libvrary does this automatically.


## User code api





