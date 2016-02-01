#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

#include "trlmdb.h"

#define DB_TIME_TO_KEY "db_time_to_key"
#define DB_TIME_TO_VALUE "db_time_to_value"
#define DB_KEY_TO_TIME "db_key_to_time"
#define DB_NODES "db_nodes"

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi dbi_time_to_key;
	MDB_dbi dbi_time_to_value;
	MDB_dbi dbi_key_to_time;
	MDB_dbi dbi_nodes;
	u_int8_t time_component[4];
};

struct TRLMDB_txn {
	MDB_txn *mdb_txn;
	TRLMDB_env *env;
	unsigned int flags;
	u_int8_t time[12];
	u_int32_t counter;
};

static void insert_uint32(u_int8_t *dst, const u_int32_t src)
{
	*dst++ = (u_int8_t) ((src << 24) >> 24);
	*dst++ = (u_int8_t) ((src << 16) >> 24);
	*dst++ = (u_int8_t) ((src << 8) >> 24);
	*dst++ = (u_int8_t) (src >> 24);
}

static int is_put_op(u_int8_t *time)
{
	return *(time + 15) & 1;
}
	

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *db_env = calloc(1, sizeof *db_env);
	if (!db_env) return ENOMEM;

	u_int32_t random = arc4random();
	insert_uint32(db_env->time_component, random);
	
	int rc = mdb_env_create(&(db_env->mdb_env));
	if (rc) free(db_env);

	*env = db_env;
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

	rc = mdb_dbi_open(txn, DB_TIME_TO_KEY, MDB_CREATE, &env->dbi_time_to_key);
	if (rc) goto cleanup_txn;

	rc = mdb_dbi_open(txn, DB_TIME_TO_VALUE, MDB_CREATE, &env->dbi_time_to_value);
	if (rc) goto cleanup_txn;

	rc = mdb_dbi_open(txn, DB_KEY_TO_TIME, MDB_CREATE, &env->dbi_key_to_time);
	if (rc) goto cleanup_txn;

	rc = mdb_dbi_open(txn, DB_NODES, MDB_CREATE, &env->dbi_nodes);
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
	TRLMDB_txn *db_txn = calloc(1, sizeof *db_txn);
	if (!db_txn) return ENOMEM;

	db_txn->env = env;
	db_txn->flags = flags;

	if (parent) {
		memmove(db_txn->time, parent->time, 8);
	} else {
		struct timeval tv;
		int rc = gettimeofday(&tv, NULL);
		if (rc) goto cleanup_txn;

		insert_uint32(db_txn->time, (u_int32_t) tv.tv_sec);
		
		u_int64_t usec = (u_int64_t) tv.tv_usec;
		u_int64_t frac_sec = (usec << 32) / 1000000;
		insert_uint32(db_txn->time + 4, (u_int32_t) frac_sec);
	}

	memmove(db_txn->time + 8, env->time_component, 4);
	
	db_txn->counter = 0;
	
	int rc = mdb_txn_begin(env->mdb_env, parent ? parent->mdb_txn : NULL, flags, &(db_txn->mdb_txn));
	if (rc) goto cleanup_txn;

	*txn = db_txn;
	
	goto out;
	
cleanup_txn:
	free(db_txn);
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

int trlmdb_get(TRLMDB_txn *txn, MDB_val *key, MDB_val *data)
{
	MDB_val time_val;
	int rc = mdb_get(txn->mdb_txn, txn->env->dbi_key_to_time, key, &time_val);
	if (rc) return rc;

	if (!is_put_op(time_val.mv_data)) return MDB_NOTFOUND;

	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_value, &time_val, data);
}

int trlmdb_insert_time_key_val(TRLMDB_txn *txn, u_int8_t *time, MDB_val *key, MDB_val *data)
{
	int rc = 0;

	MDB_val time_val = {16, time};
	
	MDB_txn *child_txn;
	rc = mdb_txn_begin(txn->env->mdb_env, txn->mdb_txn, 0, &child_txn);
	if (rc) return rc;

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
	goto out;
	
cleanup_child_txn:
	mdb_txn_abort(child_txn);
out:	
	return rc;
}	




/*
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
		index = *(size_t*) key.mv_data;
		index++;
	}
	
	key.mv_size = sizeof(size_t);
	key.mv_data = &index;
	
	return mdb_put(txn, trlmdb_history_dbi, &key, ext_key, 0);
}
*/

int trlmdb_put(TRLMDB_txn *txn, TRLMDB_dbi *dbi, MDB_val *key, MDB_val *data)
{
	return trlmdb_put_del(txn, dbi->name, dbi->mdb_dbi, key, data);
}

int trlmdb_del(TRLMDB_txn *txn, TRLMDB_dbi *dbi, MDB_val *key)
{
	return trlmdb_put_del(txn, dbi->name, dbi->mdb_dbi, key, NULL);
}
