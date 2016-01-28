#include <stdlib.h>
#include <errno.h>

#include "trlmdb.h"

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi *trlmdb_time_dbi;
	MDB_dbi *trlmdb_recent_dbi;
	u_int32_t random;
};

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *trlmdb_env = calloc(1, sizeof *trlmdb_env);
	if (!trlmdb_env) return ENOMEM;

	trlmdb_env->random = arc4random();
	
	*env = trlmdb_env;
	
	return mdb_env_create(&(trlmdb_env->mdb_env));
}

int trlmdb_env_open(TRLMDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode)
{
	int rc = mdb_env_open(env->mdb_env, path, flags, mode);
	if (!rc) return rc;

	MDB_txn *txn;
	rc = mdb_txn_begin(env->mdb_env, NULL, 0, &txn);
	if (!rc) return rc;

	rc = mdb_dbi_open(txn, "_trlmdb_time", MDB_CREATE, env->trlmdb_time_dbi);
	if (!rc) return rc;

	rc = mdb_dbi_open(txn, "_trlmdb_recent", MDB_CREATE, env->trlmdb_recent_dbi);
	if (!rc) return rc;

	return mdb_txn_commit(txn);
}

void trlmdb_env_close(TRLMDB_env *env)
{
	mdb_env_close(env->mdb_env);
	free(env);
}


MDB_env *trlmdb_mdb_env(TRLMDB_env *env)
{
	return env->mdb_env;
}
