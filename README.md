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
trlmdb_env is the first function to call.  It creates an MDB_env, generates a random id
associated with each trlmd environment, and sets the number of LMDB databases to 5, which is the
number of LMDB databases used internally by trlmdb. To close the environment, call
trlmdb_env_close(). Before the environment may be used, it must be opened using trlmdb_env_open().
It returns 0 on success, and non-zero on failure.

```
int trlmdb_env_create(trlmdb_env **env);
```

#### Mapsize
trlmdb_env_set_mapsize sets the total size of the memory map.
trlmdb_env_set_mapsize calls directly throught to the equivalent lmdb function.
It returns 0 on success, and non-zero on failure.

```
int  trlmdb_env_set_mapsize(trlmdb_env *env, uint64_t size);
```

#### Open environment
trlmdb_env_open opens the lmdb environment and opens the internal databases used bny trlmdb.
The parameters are:
  
  * trlmdb_env created by trlmdb_env_create
  * path to directory of lmdb files.
  * flags goes directly through to lmdb, choosing 0 is fine.
  * mode unix file modes, 0644 is fine.

It returns 0 on succes, non-zero on failure.

```
int trlmdb_env_open(trlmdb_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
```

#### Close environment

/* trlmdb_env_close is called at termination.
 * @param[in] env is the environment created and opened by the functions above.
 */
void trlmdb_env_close(trlmdb_env *env);

#### Begin transaction

/* trlmdb_txn_begin begins a lmdb transaction and takes a time stamp that will be used for
 * operations within the transaction.
 * @param[in] env as above.
 * @param[in] flags, same flags as mdb_txn_begin, 0 is read and write, MDB_RDONLY for read only.
 * @param[out] txn, a trlmdb_txn object will be created and returned in this pointer argument.
 * @return 0 on succes, non-zero on failure. 
*/
int trlmdb_txn_begin(trlmdb_env *env, unsigned int flags, trlmdb_txn **txn); 

#### Commit transaction

/* trlmdb_txn_commit commits the transaction.
 * @param[in] txn, the open transaction. 
 * @return 0 on succes, non-zero on failure.
 */
int  trlmdb_txn_commit(trlmdb_txn *txn);

#### Abort transaction

/* trlmdb_txn_abort aborts the transaction.  The database will be in the same state as before the
 * transaction was begun.
 * @param[in] txn, the open transaction.
 */
void trlmdb_txn_abort(trlmdb_txn *txn);

#### Get value for key in table

/* trlmdb_get gets a the value for a key in a table. 
 * @param[in] txn, an open transaction.
 * @param[in] table, a null-terminated string
 * @param[in] key, a byte buffer and a length in an MDB_val struct.
 *   typedef struct MDB_val {
 *           size_t mv_size;
 *           void *mv_data;
 *   } MDB_val;
 * @param[out] value, the result will be available in value. Copy the buffer before the transaction
 * is done if the result is needed.
 * @return, 0 on success, MDB_NOTFOUND if the key is absent, ENOMEM if memory allocation fails. 
 */
int trlmdb_get(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);

#### Put value for key in table

/* trlmdb_put puts the value for a key in a table. 
 * @param[in] txn, an open transaction.
 * @param[in] table, a null-terminated string
 * @param[in] key, a byte buffer and a length in an MDB_val struct.
 * @param[in] value, the value to store in trlmdb.
 * @return, 0 on success, ENOMEM if memory allocation fails, LMDB error codes for mdb_put. 
 */
int trlmdb_put(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);


#### Delete key/value pair in table

/* trlmdb_del deletes the key and associated value in a table. 
 * @param[in] txn, an open transaction.
 * @param[in] table, a null-terminated string
 * @param[in] key, a byte buffer and a length in an MDB_val struct.
 * @return, 0 on success, ENOMEM if memory allocation fails, LMDB error codes for mdb_del. 
 */
int trlmdb_del(trlmdb_txn *txn, char *table, MDB_val *key);

#### Opewn cursor for table

/* trlmdb_cursor_open opens a cursor that can be used to traverse a table.
 * @param[in] txn, an open transaction
 * @param[in] table, the table to traverse.
 * @param[out] cursor, a pointer to the cursor to create
 * @return 0 on success, ENOMEM if memory allocation failed.
 */
int trlmdb_cursor_open(trlmdb_txn *txn, char *table, trlmdb_cursor **cursor);

#### Close cursor for table

/* trlmdb_cursor_close closes the cursor
 * @param[in] cursor
 */
void trlmdb_cursor_close(trlmdb_cursor *cursor);

#### Position cursor at start of table

/* trlmdb_cursor_first positions the cursor at the first key in the table used to open the cursor.
 * @param[in] cursor.
 * @return 0 on success, MDB_NOTFOUND if the table is empty.
 */
int trlmdb_cursor_first(struct trlmdb_cursor *cursor);

#### Position cursor at end of table

/* trlmdb_cursor_last positions the cursor at the last key in the table.
 * @param[in] cursor.
 * @return 0 on success, MDB_NOTFOUND if the table is empty.
 */
int trlmdb_cursor_last(struct trlmdb_cursor *cursor);

#### Move cursor to next element

/* trlmdb_cursor_next positions the cursor at the next key in the table.
 * @param[in] cursor.
 * @return 0 on success, MDB_NOTFOUND if the end of the table has been reached.
 */
int trlmdb_cursor_next(struct trlmdb_cursor *cursor);

#### Move cursor to previous element

/* trlmdb_cursor_prev positions the cursor at the previous key in the table.
 * @param[in] cursor.
 * @return 0 on success, MDB_NOTFOUND if the end of the table has been reached.
 */
int trlmdb_cursor_prev(struct trlmdb_cursor *cursor);

#### Get key/value for cursor

/* trlmdb_cursor_get gets the key and value for current cursor.
 * @param[in] cursor.
 * @param[out] key, the current key is placed in the MDB_val key.
 * @param[out] val, the current value is placed in the MDB_val val.
 * @return 0 on success, MDB_NOTFOUND if the key, value is absent.
 *   MDB_NOTFOUND will only be returned if one of the four functions above 
 *   returned MDB_NOTFOUND. 
*/
int trlmdb_cursor_get(struct trlmdb_cursor *cursor, MDB_val *key, MDB_val *val);



## Example API usage

## Replicator usage


## Detailed explanation of trlmdb

## Performance

## Time stamps and consistency

Key-value store replication using time stamps.
