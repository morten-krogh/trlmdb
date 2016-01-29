#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

#include "trlmdb.h"

#define _TRLMDB_TIME "_trlmdb_time"
#define _TRLMDB_HISTORY "_trlmdb_history"
#define _TRLMDB_NODES "_trlmdb_nodes"

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi trlmdb_time_dbi;
	MDB_dbi trlmdb_history_dbi;
	MDB_dbi trlmdb_nodes_dbi;
	u_int32_t random;
};

struct TRLMDB_txn {
	MDB_txn *mdb_txn;
	TRLMDB_env *env;
	u_int64_t time;
};

struct TRLMDB_dbi {
	MDB_dbi mdb_dbi;
	char *name;
};

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *trlmdb_env = calloc(1, sizeof *trlmdb_env);
	if (!trlmdb_env) return ENOMEM;

	trlmdb_env->random = arc4random();
	
	int rc = mdb_env_create(&(trlmdb_env->mdb_env));
	if (rc) free(trlmdb_env);

	*env = trlmdb_env;
	return rc;
}

int trlmdb_env_open(TRLMDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode)
{
	int rc = 0;

	rc = mdb_env_open(env->mdb_env, path, flags, mode);
	if (rc) return rc;

	MDB_txn *txn;
	rc = mdb_txn_begin(env->mdb_env, NULL, 0, &txn);
	if (rc) goto cleanup_env;

	rc = mdb_dbi_open(txn, _TRLMDB_TIME, MDB_CREATE, &env->trlmdb_time_dbi);
	if (rc) goto cleanup_txn;

	rc = mdb_dbi_open(txn, _TRLMDB_HISTORY, MDB_INTEGERKEY | MDB_CREATE, &env->trlmdb_history_dbi);
	if (rc) goto cleanup_txn;

	rc = mdb_dbi_open(txn, _TRLMDB_NODES, MDB_CREATE, &env->trlmdb_nodes_dbi);
	if (rc) goto cleanup_txn;
	
	rc = mdb_txn_commit(txn);
	if (rc) goto cleanup_env;
	
	goto out;

cleanup_txn:
	mdb_txn_abort(txn);
cleanup_env:
	mdb_env_close(env->mdb_env);
out:
	return rc;
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
	if (rc) goto cleanup_txn;
	
	u_int64_t usec = (u_int64_t) tv.tv_usec;
	usec =  (usec << 32) / 1000000;
	
       	trlmdb_txn->time = ((u_int64_t) (tv.tv_sec << 32)) | usec; 

	*txn = trlmdb_txn;

	rc = mdb_txn_begin(env->mdb_env, parent ? parent->mdb_txn : NULL, flags, &(trlmdb_txn->mdb_txn));
	if (rc) goto cleanup_txn;

	goto out;
	
cleanup_txn:
	free(trlmdb_txn);
out:
	return rc;
}

int  trlmdb_txn_commit(TRLMDB_txn *txn)
{
	int rc = 0;

	rc = mdb_txn_commit(txn->mdb_txn);
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

int  trlmdb_dbi_open(TRLMDB_txn *txn, const char *name, unsigned int flags, TRLMDB_dbi **dbi)
{
	if (strcmp(name, _TRLMDB_TIME) == 0 || strcmp(name, _TRLMDB_HISTORY) == 0 || strcmp(name, _TRLMDB_NODES) == 0) return EPERM;
	if (flags & MDB_DUPSORT) return EPERM;

	TRLMDB_dbi *trlmdb_dbi = calloc(1, sizeof *trlmdb_dbi); 
	if (!trlmdb_dbi) return ENOMEM;
	
	trlmdb_dbi->name = strdup(name);
	int rc = trlmdb_dbi->name ? 0 : ENOMEM;
	if (rc) goto cleanup_dbi;

	rc = mdb_dbi_open(txn->mdb_txn, name, flags, &trlmdb_dbi->mdb_dbi);	
	if (rc) goto cleanup_name;
	
	*dbi = trlmdb_dbi;
	goto out;

cleanup_name:
	free(trlmdb_dbi->name);
cleanup_dbi:
	free(trlmdb_dbi);
out:
	return rc;
}

MDB_dbi trlmdb_mdb_dbi(TRLMDB_dbi *dbi)
{
	return dbi->mdb_dbi;
}

int trlmdb_get(TRLMDB_txn *txn, TRLMDB_dbi *dbi, MDB_val *key, MDB_val *data)
{
	return mdb_get(txn->mdb_txn, dbi->mdb_dbi, key, data);
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

/* put = 1 for put and put = 0 for del. ext_time must have size 12 or more. */
static void extended_time(u_int64_t time, u_int32_t random, int put, void *ext_time)
{
	*(u_int64_t*)ext_time = time;

	u_int32_t last = put ? (random | 1) : (random | ~(u_int32_t)1);
	*(u_int32_t*)(ext_time + 8) = last;

	return;
}

static int put_trlmdb_time(MDB_txn *txn, u_int64_t time, u_int32_t random, int put, MDB_dbi trlmdb_time_dbi, MDB_val *ext_key)
{
	char ext_time[12];
	extended_time(time, random, put, ext_time);
	MDB_val val = {12, ext_time};
	return mdb_put(txn, trlmdb_time_dbi, ext_key, &val, 0);
}

static int put_trlmdb_history(MDB_txn *txn, MDB_dbi trlmdb_history_dbi, MDB_val *ext_key)
{
	MDB_cursor *cursor;
	mdb_cursor_open(txn, trlmdb_history_dbi, &cursor);

	MDB_val key;
	MDB_val data;
	int rc = mdb_cursor_get(cursor, &key, &data, MDB_LAST);

	size_t index = 0;
	if (!rc) {
		index = (size_t) key.mv_data;
		index++;
	}

	key.mv_size = sizeof(size_t);
	key.mv_data = &index;
	
	return mdb_put(txn, trlmdb_history_dbi, &key, ext_key, 0);
}

/* if data == NULL it is a delete operation otherwise a put operation */
static int trlmdb_put_del(TRLMDB_txn *txn, const char *name, MDB_dbi dbi, MDB_val *key, MDB_val *data)
{
	int rc = 0;

	MDB_val ext_key;
	rc = extended_key(name, key, &ext_key);
	if (rc) return rc;

	MDB_txn *child_txn;
	rc = mdb_txn_begin(txn->env->mdb_env, txn->mdb_txn, 0, &child_txn);
	if (rc) goto cleanup_ext_key;

	rc = put_trlmdb_time(child_txn, txn->time, txn->env->random, data != NULL, txn->env->trlmdb_time_dbi, &ext_key);
	if (rc) goto cleanup_child_txn;

	rc = put_trlmdb_history(child_txn, txn->env->trlmdb_history_dbi, &ext_key);
	if (rc) goto cleanup_child_txn;

	if (data) {
		rc = mdb_put(child_txn, dbi, key, data, 0); 
		if (rc) goto cleanup_child_txn;
	} else {
		rc = mdb_del(child_txn, dbi, key, NULL);
		if (rc) goto cleanup_child_txn;
	}

	rc = mdb_txn_commit(child_txn);
	goto cleanup_ext_key;
	
cleanup_child_txn:
	mdb_txn_abort(child_txn);
cleanup_ext_key:
	free(ext_key.mv_data);
	
	return rc;
}	

int trlmdb_put(TRLMDB_txn *txn, TRLMDB_dbi *dbi, MDB_val *key, MDB_val *data)
{
	return trlmdb_put_del(txn, dbi->name, dbi->mdb_dbi, key, data);
}

int trlmdb_del(TRLMDB_txn *txn, TRLMDB_dbi *dbi, MDB_val *key)
{
	return trlmdb_put_del(txn, dbi->name, dbi->mdb_dbi, key, NULL);
}
