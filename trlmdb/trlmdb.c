#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

#include "trlmdb.h"

#define DB_TIME_TO_KEY "db_time_to_key"
#define DB_TIME_TO_DATA "db_time_to_data"
#define DB_KEY_TO_TIME "db_key_to_time"
#define DB_NODES "db_nodes"

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi dbi_time_to_key;
	MDB_dbi dbi_time_to_data;
	MDB_dbi dbi_key_to_time;
	MDB_dbi dbi_nodes;
	uint8_t time_component[4];
};

struct TRLMDB_txn {
	MDB_txn *mdb_txn;
	TRLMDB_env *env;
	unsigned int flags;
	uint8_t time[12];
	uint64_t counter;
	TRLMDB_txn *parent;
};

struct TRLMDB_cursor {
	TRLMDB_txn *txn;
	MDB_cursor *mdb_cursor;
};

static void insert_uint32(uint8_t *dst, const uint32_t src)
{
	uint32_t be = htonl(src);
	memcpy(dst, &be, 4);
}

static void insert_uint64(uint8_t *dst, const uint64_t src)
{
	uint32_t upper = (uint32_t) (src >> 32);
	insert_uint32(dst + 4, upper);
	uint32_t lower = (uint32_t) src & 0xFFFFFFFF;
	insert_uint32(dst + 4, lower);
}

static int is_put_op(uint8_t *time)
{
	return *(time + 19) & 1;
}

static int cmp_time(uint8_t *time1, uint8_t *time2)
{
	return memcmp(time1, time2, 20);
}

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *db_env = calloc(1, sizeof *db_env);
	if (!db_env) return ENOMEM;

	uint32_t random = arc4random();
	insert_uint32(db_env->time_component, random);
	
	int rc = mdb_env_create(&(db_env->mdb_env));
	if (rc) free(db_env);

	mdb_env_set_maxdbs(db_env->mdb_env, 4);
	
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

	rc = mdb_dbi_open(txn, DB_TIME_TO_DATA, MDB_CREATE, &env->dbi_time_to_data);
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
	db_txn->parent = parent;
	
	if (parent) {
		memmove(db_txn->time, parent->time, 8);
		db_txn->counter = parent->counter;
	} else {
		struct timeval tv;
		int rc = gettimeofday(&tv, NULL);
		if (rc) goto cleanup_txn;
		insert_uint32(db_txn->time, (uint32_t) tv.tv_sec);
		uint64_t usec = (uint64_t) tv.tv_usec;
		uint64_t frac_sec = (usec << 32) / 1000000;
		insert_uint32(db_txn->time + 4, (uint32_t) frac_sec);
		db_txn->counter = 0;		
	}

	memmove(db_txn->time + 8, env->time_component, 4);
	
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

	if (txn->parent) {
		txn->parent->counter = txn->counter;
	}
	rc = mdb_txn_commit(txn->mdb_txn);
	free(txn);

	return rc;
}

void trlmdb_txn_abort(TRLMDB_txn *txn)
{
	if (txn->parent) {
		txn->parent->counter = txn->counter;
	}
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

	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, data);
}

int trlmdb_insert_time_key_data(TRLMDB_txn *txn, uint8_t *time, MDB_val *key, MDB_val *data)
{
	int rc = 0;

	MDB_val time_val = {20, time};
	
	MDB_txn *child_txn;
	rc = mdb_txn_begin(txn->env->mdb_env, txn->mdb_txn, 0, &child_txn);
	if (rc) return rc;

	rc = mdb_put(child_txn, txn->env->dbi_time_to_key, &time_val, key, 0);
	if (rc) goto cleanup_child_txn;

	if (is_put_op(time)) {
		rc = mdb_put(child_txn, txn->env->dbi_time_to_data, &time_val,data, 0);
		if (rc) goto cleanup_child_txn;
	}

	int is_time_most_recent = 1;
	MDB_val current_time_val;
	rc = mdb_get(child_txn, txn->env->dbi_key_to_time, key, &current_time_val);
	if (!rc) {
		is_time_most_recent = cmp_time(time, current_time_val.mv_data) > 0;
	}

	if (is_time_most_recent) {
		rc = mdb_put(child_txn, txn->env->dbi_key_to_time, key, &time_val, 0);
		if (rc) goto cleanup_child_txn;
	}

	rc = mdb_txn_commit(child_txn);
	goto out;
	
cleanup_child_txn:
	mdb_txn_abort(child_txn);
out:	
	return rc;
}	

static int trlmdb_put_del(TRLMDB_txn *txn, MDB_val *key, MDB_val *data)
{
	uint8_t time[20];
	memmove(time, txn->time, 12);
	uint64_t counter = txn->counter + ((data != NULL) ? 1 : 0);
	insert_uint64(time + 12, counter);
	txn->counter += 2;

	return trlmdb_insert_time_key_data(txn, time, key, data);
}

int trlmdb_put(TRLMDB_txn *txn, MDB_val *key, MDB_val *data)
{
	return trlmdb_put_del(txn, key, data);
}

int trlmdb_del(TRLMDB_txn *txn, MDB_val *key)
{
	MDB_val time_val;
	int rc = mdb_get(txn->mdb_txn, txn->env->dbi_key_to_time, key, &time_val);
	if (rc) return rc;

	if (!is_put_op(time_val.mv_data)) return MDB_NOTFOUND;

	return trlmdb_put_del(txn, key, NULL);
}

int trlmdb_cursor_open(TRLMDB_txn *txn, TRLMDB_cursor **cursor)
{
	TRLMDB_cursor *db_cursor = calloc(1, sizeof *db_cursor);
	if (!db_cursor) return ENOMEM; 

	*cursor = db_cursor;
	db_cursor->txn = txn;
	return mdb_cursor_open(txn->mdb_txn, txn->env->dbi_key_to_time, &db_cursor->mdb_cursor);
}

void trlmdb_cursor_close(TRLMDB_cursor *cursor){
	mdb_cursor_close(cursor->mdb_cursor);
	free(cursor);
}

int  trlmdb_cursor_get(TRLMDB_cursor *cursor, MDB_val *key, MDB_val *data, int *is_deleted, MDB_cursor_op op)
{
	MDB_val time_val;
	
	int rc = mdb_cursor_get(cursor->mdb_cursor, key, &time_val, op);
	if (rc) return rc;

	if (is_put_op(time_val.mv_data)) {
		*is_deleted = 0;
		return mdb_get(cursor->txn->mdb_txn, cursor->txn->env->dbi_time_to_data, &time_val, data);
	} else {
		*is_deleted = 1;
		data->mv_size = 0;
		data->mv_data = NULL;
		return 0;
	}
}
