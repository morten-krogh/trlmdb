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
The first three are set at the beginning of a transaction. The counter is incremented by two at every operation, and the last bit is 1 for a pit operation and 0 for s delete operation. The last part is long to avoid overflows in very long transactions. 

Time stamps are unique. It is unlikely that two distinct nodes start a transaction at the same time, and if they do the environment ids are unlikely to be equal. The environment ids are randomly chosen. The time precision is below a nano second. 

#### LMDB databases

A trlmdb database contains exactly 5 LMDB databases(dbi).

 * db_time_to_key
 
 * db_time_to_data

* db_key_to_time"

* db_nodes
* db_node_time







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
