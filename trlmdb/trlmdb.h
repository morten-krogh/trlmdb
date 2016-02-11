#ifndef TRLMDB_H
#define TRLMDB_H

#include "lmdb.h"

typedef struct trlmdb_env trlmdb_env;
typedef struct trlmdb_txn trlmdb_txn;
typedef struct trlmdb_cursor trlmdb_cursor;

int trlmdb_env_create(trlmdb_env **env);
int trlmdb_env_open(trlmdb_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
void trlmdb_env_close(trlmdb_env *env);

int trlmdb_txn_begin(trlmdb_env *env, unsigned int flags, trlmdb_txn **txn); 
int  trlmdb_txn_commit(trlmdb_txn *txn);
void trlmdb_txn_abort(trlmdb_txn *txn);

int trlmdb_get(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *data);
int trlmdb_put(trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *data);
int trlmdb_del(trlmdb_txn *txn, char *table, MDB_val *key);

int trlmdb_cursor_open(trlmdb_txn *txn, trlmdb_cursor **cursor);
void trlmdb_cursor_close(trlmdb_cursor *cursor);
int trlmdb_cursor_get_first(trlmdb_cursor *cursor, MDB_val *key, MDB_val *data);
int trlmdb_cursor_get_next(trlmdb_cursor *cursor, MDB_val *key, MDB_val *data);	


#endif
