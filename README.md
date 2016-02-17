# Trlmdb(time replicating lightning memory database)

Trlmdb is a database built on top of LMDB (lightning memory database, http://symas.com/mdb/doc/index.html).
Trlmdb is a key-value store where eack put or get operation is time stamped and replicated to remote nodes.

## Programming language 
Trlmdb is written in C.

## Two parts
Trlmdb consists of two parts: An API that applications must use to access the database and an executable program, the replicator, that replocates the databasse to remote nodes.

## Platform
Trlmdb runs on Posix compliant platforms such as Linux, Unix, and Mac OS X. Trlmdb has not been tested on Windows.
Only minor changes shopuld be necessary to make trlmdb work on Windows.

## Dependencies
The only dependency, besides a Poix platform, is LMDB. The relevant LMDB files are included in the repository, so 
no additional installations are needed. 

## License
The license is the MIT license described in the file LICENSE. LMDB comes with itw own license which is 
described in the file LMDB_LICENSE. 

##Intallation
Clone the git repository and run 

```
make
```

To use the API, the file trlmdb.h must be included in the application and the application must be linked to
mdb.o, midl.o, trlmdb.o. The files mdb.o and midl.o originate from LMDB.

The replicator is started with 

```
./replicator conf-file 
```
The details are described below.

Check the files test_single.c and test_multi.c for examples of how to use trlmdb.

The file trmldmb.c contains the source code for both the API and the replicator.
The header file trlmdb.h contains the API types and functions.

## Brief overview of trlmdb
A trlmdb system consists of one or more nodes. A node consists of
 
 * LMDB database with a fixed set of tables(LMDB dbi).
 * The replicator executable running in its own process. 
 * An application, or several, reading and writing to the LMDB database through the trlmdb API. 

The replicators establish TCP connections to each other and send messages back and forth in order to synchronize the nodes. Some of the tables in the LMDB database are used internally by the replicator to store information about the known state of remote nodes. The replicator can be stopped and restarted at any time.

Every put or delete to trlmdb is automatically time stamped. The value of a key is the determined by the latest time stamp. Applications can use distinct nodes simultaneously. Any conflicting updates to the database will be resolved using the time stamps. Trlmdb is eventually consistent. There is an unknown time interval during which a read from one node can give an old result. If the replicators are running, the delay will be short in practice. 

There can be more than one replicator and application using the same LMDB database simultaneously. It does not make sense to have more than one replicator per node, but it is possible. The replicator is multi-threaded internally, which is equivalent to having several replicators attached to the same node. Consistency is guaranteed by the LMDB transaction system. 

Further details are described below.

## API

#### Return values

Most of the functions in the API return an int as a status code. In those cases, 0 denotes success and a non-zero
value denotes failure or absence.
The non-zero values are ENOMEM for memory allocation failure, and the LMDB return codes in other cases.

The value MDB_NOTFOUND denotes the absence of a key. This return value is not really an error.

#### Types
The typedefs define opaque types that should be used through the functions below.  
The types are extended versions of the equivalent data structures in LMDB.
A trlmdb_env environemnt is used to access a given database.  
A trlmdb_txn transaction is used to access the database in an atomic manner.
A cursor is used to traverse a table.


```
typedef struct trlmdb_env trlmdb_env;
typedef struct trlmdb_txn trlmdb_txn;
typedef struct trlmdb_cursor trlmdb_cursor;
```

#### Create environment
`trlmdb_create_env` is the first function to call. It creates an MDB_env, generates a random id
associated with each trlmd environment, and sets the number of LMDB databases to 5, which is the
number of LMDB databases used internally by trlmdb. To close the environment, call
`trlmdb_env_close`. Before the environment may be used, it must be opened using `trlmdb_env_open`.

* trlmdb_env **env 

```
int trlmdb_env_create(trlmdb_env **env);
```

#### Set mapsize
`trlmdb_env_set_mapsize` sets the total size of the memory map.
`trlmdb_env_set_mapsize` calls directly through to the equivalent lmdb function.

 * trlmdb_env created by `trlmdb_env_create`
 * size of the memory map. It must be a multiplum of the page size.

```
int  trlmdb_env_set_mapsize(trlmdb_env *env, uint64_t size);
```

#### Open environment
`trlmdb_env_open` opens the lmdb environment and opens the internal databases used bny trlmdb.
  
 * trlmdb_env created by trlmdb_env_create
 * path to directory of lmdb files.
 * flags goes directly through to lmdb, choosing 0 is fine.
 * mode unix file modes, 0644 is fine.

```
int trlmdb_env_open(trlmdb_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
```

#### Close environment
`trlmdb_env_close` is called at termination.

 * env is the environment created and opened by the functions above.

```
void trlmdb_env_close(trlmdb_env *env);
```

#### Begin transaction
`trlmdb_txn_begin` begins a lmdb transaction and takes a time stamp that will be used for operations within the transaction.

* env as above.
* flags, same flags as mdb_txn_begin, 0 is read and write, MDB_RDONLY for read only.
* txn, a trlmdb_txn object will be created and returned in this pointer argument.
 
```
int trlmdb_txn_begin(trlmdb_env *env, unsigned int flags, trlmdb_txn **txn); 
```

#### Commit transaction
`trlmdb_txn_commit` commits the transaction.
 
 * txn, the open transaction. 

It returns 0 on succes, non-zero on failure.

 ```
int  trlmdb_txn_commit(trlmdb_txn *txn);
```

#### Abort transaction
`trlmdb_txn_abort` aborts the transaction. The database will be in the same state as before the transaction was begun.
 
 * txn, the open transaction.

 ```
void trlmdb_txn_abort(trlmdb_txn *txn);
```

#### Get value for key in table
`trlmdb_get` gets a the value for a key in a table. 
 
 * txn, an open transaction.
 * table, a null-terminated string
 * key, a byte buffer and a length in an MDB_val struct.
    typedef struct MDB_val {
            size_t mv_size;
            void *mv_data;
    } MDB_val;
 * value, the result will be available in value. Copy the buffer before the transaction is done if the result is needed.

 ```
int trlmdb_get(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);
```

#### Put value for key in table
`trlmdb_put` puts the value for a key in a table. 
 
 * txn, an open transaction.
 * table, a null-terminated string
 * key, a byte buffer and a length in an MDB_val struct.
 * value, the value to store in trlmdb.

 ```
int trlmdb_put(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);
```

#### Delete key/value pair in table
`trlmdb_del` deletes the key and associated value in a table. 
 
 * txn, an open transaction.
 * table, a null-terminated string
 * key, a byte buffer and a length in an MDB_val struct.

 ```
int trlmdb_del(trlmdb_txn *txn, char *table, MDB_val *key);
```

#### Open cursor for table
`trlmdb_cursor_open` opens a cursor that can be used to traverse a table.
 
 * txn, an open transaction
 * table, the table to traverse.
 * cursor, a pointer to the cursor to create

 ```
int trlmdb_cursor_open(trlmdb_txn *txn, char *table, trlmdb_cursor **cursor);
```

#### Close cursor for table
`trlmdb_cursor_close` closes the cursor
 
 * cursor

```
void trlmdb_cursor_close(trlmdb_cursor *cursor);
```

#### Position cursor at start of table
`trlmdb_cursor_first` positions the cursor at the first key in the table used to open the cursor.

 * cursor.

 ```
int trlmdb_cursor_first(struct trlmdb_cursor *cursor);
```

#### Position cursor at end of table
trlmdb_cursor_last positions the cursor at the last key in the table.

 * cursor.

 ```
int trlmdb_cursor_last(struct trlmdb_cursor *cursor);
```

#### Move cursor to next element
trlmdb_cursor_next positions the cursor at the next key in the table.
 * @param[in] cursor.
 * @return 0 on success, MDB_NOTFOUND if the end of the table has been reached.

 ```
int trlmdb_cursor_next(struct trlmdb_cursor *cursor);
```

#### Move cursor to previous element
`trlmdb_cursor_prev` positions the cursor at the previous key in the table.
 * cursor.

 ```
int trlmdb_cursor_prev(struct trlmdb_cursor *cursor);
```

#### Get key/value for cursor
`trlmdb_cursor_get` gets the key and value for current cursor.
 
 * cursor.
 * key, the current key is placed in the MDB_val key.
 * val, the current value is placed in the MDB_val val.

```
int trlmdb_cursor_get(struct trlmdb_cursor *cursor, MDB_val *key, MDB_val *val);
```

## Example API usage

See the files `test_single.c` and `test_multi.c` for examples

A simple example is shown here

```
int rc = 0;

trlmdb_env *env;
rc = trlmdb_env_create(&env);
assert(!rc);

rc = trlmdb_env_open(env_1, './database_dir', 0, 0644);
assert(!rc);

trlmdb_txn *txn;
rc = trlmdb_txn_begin(env_1, 0, &txn);
assert(!rc);

char *table_1 = "table-1";
MDB_val key_1 = {5, "key_1"};
MDB_val val_1 = {5, "val_1"};
                                        
rc = trlmdb_put(txn, table_1, &key_1, &val_1);
assert(!rc);

MDB_val val_2;
rc = trlmdb_get(txn, table_1, &key_1, &val_2);
assert(!rc);
assert(!cmp_mdb_val(&val_1, &val_2));

rc = trlmdb_txn_commit(txn);
assert(!rc);
```

Assert is used to illustrate the expected return values. 

## Replicator usage

The replicator is a stand alone executable that reads and writes to its associated database and communicates to remote nodes. A replicator is started by

```
./replicator conf-info
```

where conf-info is the file name of a configuration file. Examples are shown in the `databases` directory.
The most general configuration has the following format:

```
node = node-2
database = trlmdb-2
timeout = 2000
port = 8002
accept = node-3
connect = node-1 localhost:8001
```

Lines have the form

```
name = value
```

with a set of prdefined names. All other types of lines are ignored.
The names are

`node` is a string denoting the name of the node, e.g., node-1. `node` should occur exactly once.

`database` is the directory of the LMDB database, `database` should occur exactly once.

`timeout` is the waiting time in milliseconds before the replicator checks the database for updates after finishing all jobs. The replicator keeps working as long as there is work for it to do. When, the replicator is done, it waits a for the period timeout before it queires the database to see if there are changes since last. If there are changes, it will send them to the remote nodes. The advantage of a short timeout is that remote nodes will get their updates faster. The disadvantage is that CPU power is used. For very busy applications, the timout will be irrelevant, because the replicator never goes to sleep. The default value is 1000.

`port` is the listening port for the server part of the replicator. The replicator will accept incoming tcp connections on this port. `port` should occur at most once. If `port` is absent, the replicator will not act as a server.

`accept` is the name of a remote node, that this replicator will communicate with. Secret node names can be used for security.

`connect` is a node name of remote node followed by the internet address of the remote node. the replicator will attempt to connect to the remote node at this address. `connect` can occur zero or more times.

A replicator with both zero `port` and zero `connect` is useless.


## Detailed explanation of trlmdb

#### Time stamps

trlmdb keeps time stamps, just called time in the code. A time is 20 bytes long and consists of four parts 
of length 4, 4, 4, and 8 bytes

```
time(20) = seconds-since-epoch(4) fraction-seconds(4) environment-id(4) counter(8)
```
The first two are set at the beginning of each transaction. The environment-id is set at the creation of the environment. The counter is set to zero at the beginning of each transaction and incremented by two at every operation. The last bit of th counter is 1 for a put operation and 0 for s delete operation. The last part is long to avoid overflows in very very long transactions. 

Time stamps are unique. It is unlikely that two distinct nodes start a transaction at the same time, and if they do the environment ids are unlikely to be equal. The environment ids are randomly chosen. The time precision is below a nano second. 

#### Multiple tables

The API for trlmdb allows key/value pairs to be inserted in named tables. At a low level, trlmdb only keeps track of one huge key-value table. The keys of the huge table are concatenations of the table name and the key.

```
extended-key = table-name null-byte key
```

Because a table name does not contain the null byte, there is a one to one mapping between extended key and table, key pairs. The ordering of extended keys place all extended keys belonging to the same table in consecutive order.
  
Because tables are just prefixes to extended keys, a table does not need to be created and destroyed; a table is automatically created when the first key is inserted.
  
#### LMDB databases

A trlmdb database contains exactly 5 LMDB databases(dbi).

##### db_time_to_key

The table db_time_to_key has the 20 byte time stamps as keys and extended keys as values. 
Every put or delete operation is recorded in this table.
 
##### db_time_to_data

The table db_time_to_data has the 20 byte time stamps as keys and values as values.
Every put operation is recorded in this table. Delete operations do not need to be recorded here, since the last bit of the time stamp denotes whether the operation is a put or delete operation. 

##### db_key_to_time

The table db_key_to_time has extended keys as values and the most recent time for that key as value.

##### db_nodes

The table db_nodes has remote node names as keys and empty values. 

##### db_node_time

The table db_node_time has concatenated node names and time stamps as keys and two byte flags as values.
This table is used by the replicator to keep track of remote nodes.
The two byte flags can be either "ff", "ft", "tf", where f is false and t is true. The meaning of the flgas is explained below. Absence of a node-time is defined to have the same meaning as the flag "tt". So, for purely performance reasons, the flag "tt" is never used. 

#### Put operations

A put operation has a (extended) key and a value. The time stamp is calculated and the last bit is 1. During a put operation, the (time, key) pair inserted in db_time_to_key, the (time, value) pair is inserted in (time, value). The (key, time) pair is insereted in db_key_to_time unless there already is a more recent time for that key. When an application calls `trlmdb_put` the time stamp will almost always be the most recent one. The only exception would be if a remote node is inserting the same key a little later, and the replicator works fast, and there is a problem with the clocks.

Furthermore, for each node in db_nodes, the concatenated node-time is inserted in db_node_time with value of "ff".

#### Delete operations

A delete operation works as a put operation with the exception that nothing is inserted in db_time_to_data. The time stamp has its last bit set to 0.


#### Get operations

A get operation looks in the table db_key_to_time. If the key is absent in this table, the result MDB_NOTFOUND is returned. If a time stamp is found, the last bit is checked. If the last bit is zero, MDB_NOTFOUND is returned.
If the last bit is one, the value is found in db_time_to_data.

#### Cursors

Cursors work by mapping to cursors of LMDB and keeping track of the null terminated prefix table name.


#### The replicator

A replicator is associated with a database and has a node name. At start up, the replicator reads the configuration file. Part of the configuration file is a specification of the remote nodes that this replicator can communicate with.
The replicator opens the database and checks whwether all nodes from the configuration file are present in db_nodes. Those nodes that are absent are inserted into db_node, and for every time in db_time_to_key, the concatenated key node-time is inserted into db_node_time with value "ff".

##### Tcp connections

The replicators communicate throught tcp connections. A replicator can act as a server, a client, or both.
The replicators communicate with messages encoded in a simple custom format.

```
message = total-lengt field-1-length field-1 field-2-length field-2 ...
```

The first field denotes the message type. There are two message types right now, "node" and "time".
At connection establishment, "node" messages are sent and received. The "node" messages are used for both nodesa to establish the identity of the remote node on that connection. If the remote node is not mentioned in the configuration file, the tcp connection is closed and an error message is printed to stderr.

After identity establishment, all messages are of type "time".

##### Knowledge of a time stamp

Time stamp are globally unique. The goal of a replicator is to make sure that all its remote peers know all time stamps that the replicator itself knows. Knowing a time stamp means knowing the time stamp and the corresponding key and value. There is only a value if the time stamp originates from a put operation. The last bit of the time stamp 
encodes the put/delete type. 

##### Time messages

A time message has the form

```
time-message = "time" flags time [key] [value] 
``` 

where the presence of key and value depend on the flags.














## Performance

For both get and put operations, trlmdb does more work than a pure LMDB database. The file `perf` measures the 
time taken to perform various operations. Running `perf` on a macbook pro 2011 gave

```
trlmdb, 1000 insertions, one transaction per insertion, duration = 1.940385 seconds
trlmdb, 1000 insertions, one transaction in total, duration = 0.089183 seconds
trlmdb, 1000 reads, one transaction per read, duration = 0.003333 seconds
trlmdb, 1000 reads, one transaction in total, duration = 0.001925 seconds
lmdb, 1000 insertions, one transaction per insertion, duration = 0.389939 seconds
lmdb, 1000 insertions, one transaction in total, duration = 0.001488 seconds
lmdb, 1000 reads, one transaction per read, duration = 0.000761 seconds
lmdb, 1000 reads, one transaction in total, duration = 0.000554 seconds
```

LMDB is around 5 times faster than TRLMDB which is consistent with the amount of extra work trlmdb. These timings do not include replication or insertions in the table `node_time`. The LMDB insertions in one transaction are abnormally fast, which is probably because the insertions in this test are sequential appendings.   

## History

Besides replication, trlmdb keeps a full history of all operations. Many applications actually need the full history instead of just the current state. The history comes along for free as a by product of the time replication strategy. 


## Time stamps and consistency

Key-value store replication using time stamps.
