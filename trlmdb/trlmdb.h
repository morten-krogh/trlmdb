#ifndef TRLMDB_H
#define TRLMDB_H

#include "lmdb.h"

typedef struct TRLMDB_env TRLMDB_env;
typedef struct TRLMDB_txn TRLMDB_txn;

int trlmdb_env_create(TRLMDB_env **env);
int trlmdb_env_open(TRLMDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
void trlmdb_env_close(TRLMDB_env *env);
MDB_env *trlmdb_mdb_env(TRLMDB_env *env);

int trlmdb_txn_begin(TRLMDB_env *env, TRLMDB_txn *parent, unsigned int flags, TRLMDB_txn **txn); 
int  trlmdb_txn_commit(TRLMDB_txn *txn);
void trlmdb_txn_abort(TRLMDB_txn *txn);
MDB_txn *trlmdb_mdb_txn(TRLMDB_txn *txn);

int  trlmdb_dbi_open(TRLMDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi);

int trlmdb_get(TRLMDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int trlmdb_put(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key, MDB_val *data); 
int trlmdb_del(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key);






#endif
