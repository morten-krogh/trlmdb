#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

#include "trlmdb.h"

#define _TRLMDB_TIME "_trlmdb_time"
#define _TRLMDB_RECENT "_trlmdb_recent"

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi trlmdb_time_dbi;
	MDB_dbi trlmdb_recent_dbi;
	u_int32_t random;
};

struct TRLMDB_txn {
	MDB_txn *mdb_txn;
	TRLMDB_env *env;
	u_int64_t time;
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

	rc = mdb_dbi_open(txn, _TRLMDB_TIME, MDB_CREATE, &env->trlmdb_time_dbi);
	if (!rc) return rc;

	rc = mdb_dbi_open(txn, _TRLMDB_RECENT, MDB_CREATE, &env->trlmdb_recent_dbi);
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

int trlmdb_txn_begin(TRLMDB_env *env, TRLMDB_txn *parent, unsigned int flags, TRLMDB_txn **txn)
{
	TRLMDB_txn *trlmdb_txn = calloc(1, sizeof *trlmdb_txn);
	if (!trlmdb_txn) return ENOMEM;

	trlmdb_txn->env = env; 
	
	struct timeval tv;
	int rc = gettimeofday(&tv, NULL);
	if (!rc) return rc;

	u_int64_t usec = (u_int64_t) tv.tv_usec;
	usec =  (usec << 32) / 1000000;
	
       	trlmdb_txn->time = ((u_int64_t) (tv.tv_sec << 32)) | usec; 

	*txn = trlmdb_txn;
	
	return mdb_txn_begin(env->mdb_env, parent->mdb_txn, flags, &(trlmdb_txn->mdb_txn));
}

int  trlmdb_txn_commit(TRLMDB_txn *txn)
{
	return mdb_txn_commit(txn->mdb_txn);
}

void trlmdb_txn_abort(TRLMDB_txn *txn)
{
	mdb_txn_abort(txn->mdb_txn);
}

MDB_txn *trlmdb_mdb_txn(TRLMDB_txn *txn)
{
	return txn->mdb_txn;
}

int  trlmdb_dbi_open(TRLMDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi)
{
	if (strcmp(name, _TRLMDB_TIME) == 0 || strcmp(name, _TRLMDB_RECENT) == 0) return EPERM;
	if (flags | MDB_DUPSORT) return EPERM;
	
	return mdb_dbi_open(txn->mdb_txn, name, flags, dbi);
}

int trlmdb_get(TRLMDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
{
	return mdb_get(txn->mdb_txn, dbi, key, data);
}

static MDB_val void *extended_key(const char* name, MDB_val *key)
{
	size_t len_name = strlen(name);
	size_t size = len_name + 1 + key->mv_size;
	void *ext_key = malloc(size);
	if (!ext_key) return NULL;

	strcpy(ext_key, name);
	memcpy(ext_key + len_name + 1, key->mv_data, key->mv_size);

	return ext_key;
}

/* put = 1 for put and put = 0 for del */
static void *extended_time(u_int64_t time, u_int32_t random, int put)
{
	void *ext_time = malloc(12);
	if (!ext_time) return NULL;

	*(u_int64_t*)ext_time = time;
	u_int32_t last = put ? (random | 1) : (random | ~(u_int32_t)1);

	*(u_int32_t*)(ext_time + 8) = last;
	
	return ext_time;
}

static int put_trlmdb_tables(MDB_txn *txn, MDB_dbi trlmdb_time_dbi, MDB_dbi trlmdb_recent_dbi, MDB_val *key, MDB_val *value)
{
	int rc = mdb_put(txn, trlmdb_time_dbi, key, value, 0);
	if (!rc) return rc;

	rc = mdb_put(txn, trlmdb_recent_dbi, key, value, 0);

	return rc;
}

int trlmdb_put(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key, MDB_val *data)
{
	void *ext_key = extended_key(name, key);
	if (!ext_key) return ENOMEM;

	void *ext_time = extended_time(txn->time, txn->env->random, 1);
	if (!ext_time) return ENOMEM;

	MDB_txn *child_txn;
	int rc = mdb_txn_begin(txn->env->mdb_env, txn->mdb_txn, 0, &child_txn);
	if (!rc) return rc;

	rc = put_trlmdb_tables(child_txn, txn->env->trlmdb_time_dbi, txn->env->trlmdb_recent_dbi, ext_key, ext_time);
	if (!rc) {
		mdb_txn_abort(child_txn);
		return rc;
	}

	rc = mdb_put(child_txn, dbi, key, data, 0); 
	if (!rc) {
		mdb_txn_abort(child_txn);
		return rc;
	}
	
	return mdb_txn_commit(child_txn);	
}

int trlmdb_del(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key)
{
	void *ext_key = extended_key(name, key);
	if (!ext_key) return ENOMEM;

	void *ext_time = extended_time(txn->time, txn->env->random, 1);
	if (!ext_time) return ENOMEM;

	
}
