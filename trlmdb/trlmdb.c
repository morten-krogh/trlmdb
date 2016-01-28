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
	int rc = mdb_txn_commit(txn->mdb_txn);
	free(txn);

	return rc;
}

void trlmdb_txn_abort(TRLMDB_txn *txn)
{
	mdb_txn_abort(txn->mdb_txn);
	free(txn);
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

static int extended_key(const char* name, const MDB_val *key, MDB_val *ext_key)
{
	size_t len_name = strlen(name);
	size_t size = len_name + 1 + key->mv_size;
	void *data = malloc(size);
	if (!data) return ENOMEM;

	strcpy(data, name);
	memcpy(data + len_name + 1, key->mv_data, key->mv_size);

	ext_key->mv_size = size;
	ext_key->mv_data = data;
	
	return 0;
}

/* put = 1 for put and put = 0 for del */
static int extended_time(u_int64_t time, u_int32_t random, int put, MDB_val *ext_time)
{
	void *data = malloc(12);
	if (!data) return ENOMEM;

	*(u_int64_t*)data = time;
	u_int32_t last = put ? (random | 1) : (random | ~(u_int32_t)1);

	*(u_int32_t*)(data + 8) = last;

	ext_time->mv_size = 12;
	ext_time->mv_data = data;
	
	return 0;
}

static int put_trlmdb_tables(MDB_txn *txn, MDB_dbi trlmdb_time_dbi, MDB_dbi trlmdb_recent_dbi, MDB_val *key, MDB_val *value)
{
	int rc = mdb_put(txn, trlmdb_time_dbi, key, value, 0);
	if (!rc) return rc;

	rc = mdb_put(txn, trlmdb_recent_dbi, key, value, 0);

	return rc;
}

/* if data == NULL it is a delete operation otherwise a put operation */
static int trlmdb_put_del(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key, MDB_val *data)
{
	int rc = 0;

	MDB_val ext_key;
	rc = extended_key(name, key, &ext_key);
	if (!rc) return rc;

	MDB_val ext_time;
	rc = extended_time(txn->time, txn->env->random, data != NULL, &ext_time);
	if (!rc) goto cleanup_ext_key;

	MDB_txn *child_txn;
	rc = mdb_txn_begin(txn->env->mdb_env, txn->mdb_txn, 0, &child_txn);
	if (!rc) goto cleanup_ext_time;

	rc = put_trlmdb_tables(child_txn, txn->env->trlmdb_time_dbi, txn->env->trlmdb_recent_dbi, &ext_key, &ext_time);
	if (!rc) goto cleanup_child_txn;

	if (data) {
		rc = mdb_put(child_txn, dbi, key, data, 0); 
		if (!rc) goto cleanup_child_txn;
	} else {
		rc = mdb_del(child_txn, dbi, key, NULL);
		if (!rc) goto cleanup_child_txn;
	}

	rc = mdb_txn_commit(child_txn);
	goto cleanup_ext_time;
	
cleanup_child_txn:
	mdb_txn_abort(child_txn);
cleanup_ext_time:
	free(ext_time.mv_data);
cleanup_ext_key:
	free(ext_key.mv_data);
	
	return rc;
}	

int trlmdb_put(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key, MDB_val *data)
{
	return trlmdb_put_del(txn, name, dbi, key, data);
}

int trlmdb_del(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key)
{
	return trlmdb_put_del(txn, name, dbi, key, NULL);
}
