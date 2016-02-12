/* Time replicating lightning memory database, trlmdb.
 *
 * Trlmdb is a replicating key/value store built on top of 
 * LMDB, the lightning memory database.
 *
 * The MIT License (MIT)
 * Copyright (c) 2016 Morten Krogh
 *
 * Trlmdb consists of two parts: An api that applications should use to access the database and an
 * executable program, the replicator. The replicator keeps the database in sync with remote nodes.
 * Applications can access local database at a node through the API, and the replicators will
 * distribute the changes to all other nodes. All updates to the database gets a time stamp. The tme
 * stamps are used to replicate the data, and to resolve conflicting updates to thw same key.
 *
 * The application API is described below. The underlying LMDB database should only be changed
 * through this interface.
 *
 * Familiarity with LMDB will make it easier to understand trlmdb. The trlmdb API is much smaller
 * and should be simple to understand
 *
 */

#ifndef TRLMDB_H
#define TRLMDB_H

#include "lmdb.h"

/* The typedefs define opaque types that should be used throught the functions below.  
 * They are extended wrappers around equivalent data structures in LMDB.
 * A trlmdb_env environemnt is used to access a given database.  
 * A trlmdb_txn transaction is used to access the database in an atomic manner.
 * A cursor is used to traverse a table.
 */
typedef struct trlmdb_env trlmdb_env;
typedef struct trlmdb_txn trlmdb_txn;
typedef struct trlmdb_cursor trlmdb_cursor;


/* trlmdb_env is the first function to call.  It creates an MDB_env, generates a random id
 * associated with each trlmd environmnt, and sets the number of LMDB databases to 5, which the
 * number of LMDB databases used internally by trlmdb. To close the environment, call
 * trlmdb_env_close(). Before the environment may be used, it must be opened using trlmdb_env_open().
 */
int trlmdb_env_create(trlmdb_env **env);


/* trlmdb_env_set_mapsize sets the total size of the memory map.
 * trlmdb_env_set_mapsize calls directly throught to the equivalent lmdb function.
 * @return 0 on success, and non-zero on failure.
*/
int  trlmdb_env_set_mapsize(trlmdb_env *env, uint64_t size);


/* trlmdb_env_open opens the lmdb environment and opens the internal databases
 * used bny trlmdb.
 * @param[in] trlmdb_env created by trlmdb_env_create
 * @param[in] path to directory of lmdb files.
 * @param[in] flags goes directly through to lmdb, choosing 0 is fine.
 * @param[i]n mode unix file modes, 0644 is fine.
 * @return 0 on succes, non-zero on failure.
 */
int trlmdb_env_open(trlmdb_env *env, const char *path, unsigned int flags, mdb_mode_t mode);


/* trlmdb_env_close is called at termination.
 * @param[in] env is the environment created and opened by the functions above.
 */
void trlmdb_env_close(trlmdb_env *env);


/* trlmdb_txn_begin begins a lmdb transaction and takes a time stamp that will be used for
 * operations within the transaction.
 * @param[in] env as above.
 * @param[in] flags, same flags as mdb_txn_begin, 0 is read and write, MDB_RDONLY for read only.
 * @param[out] txn, a trlmdb_txn object will be created and returned in this pointer argument.
 * @return 0 on succes, non-zero on failure. 
*/
int trlmdb_txn_begin(trlmdb_env *env, unsigned int flags, trlmdb_txn **txn); 


/* trlmdb_txn_commit commits the transaction.
 * @param[in] txn, the open transaction. 
 * @return 0 on succes, non-zero on failure.
 */
int  trlmdb_txn_commit(trlmdb_txn *txn);

/* trlmdb_txn_abort aborts the transaction.  The database will be in the same state as before the
 * transaction was begun.
 * @param[in] txn, the open transaction.
 */
void trlmdb_txn_abort(trlmdb_txn *txn);


int trlmdb_get(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);
int trlmdb_put(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value);
int trlmdb_del(trlmdb_txn *txn, char *table, MDB_val *key);

int trlmdb_cursor_open(trlmdb_txn *txn, char *table, trlmdb_cursor **cursor);
void trlmdb_cursor_close(trlmdb_cursor *cursor);
int trlmdb_cursor_first(struct trlmdb_cursor *cursor);
int trlmdb_cursor_last(struct trlmdb_cursor *cursor);
int trlmdb_cursor_next(struct trlmdb_cursor *cursor);
int trlmdb_cursor_prev(struct trlmdb_cursor *cursor);
int trlmdb_cursor_get(struct trlmdb_cursor *cursor, MDB_val *key, MDB_val *value);


#endif
