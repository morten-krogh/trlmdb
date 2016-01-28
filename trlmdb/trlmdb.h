#ifndef TRLMDB_H
#define TRLMDB_H

#include "lmdb.h"

typedef struct TRLMDB_env TRLMDB_env;

int trlmdb_env_create(TRLMDB_env **env);
int trlmdb_env_open(TRLMDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
void trlmdb_env_close(TRLMDB_env *env);
MDB_env *trlmdb_mdb_env(TRLMDB_env *env);









#endif
