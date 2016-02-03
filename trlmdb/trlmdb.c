#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>

#include "trlmdb.h"

#define DB_TIME_TO_KEY "db_time_to_key"
#define DB_TIME_TO_DATA "db_time_to_data"
#define DB_KEY_TO_TIME "db_key_to_time"
#define DB_NODES "db_nodes"
#define DB_NODE_TIME "db_node_time"

#define TIME_LENGTH 20


/* Managing the lmdb environment with the special databases */  

struct TRLMDB_env {
	MDB_env *mdb_env;
	MDB_dbi dbi_time_to_key;
	MDB_dbi dbi_time_to_data;
	MDB_dbi dbi_key_to_time;
	MDB_dbi dbi_nodes;
	MDB_dbi dbi_node_time;
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

void print_error(char *err)
{
	fprintf(stderr, "%s\n", err);
}

void print_error_and_exit(char *err)
{
	print_error(err);
	exit(1);
}

void print_mdb_error(int rc)
{
	fprintf(stderr, "%d, %s\n", rc, mdb_strerror(rc));
}

void print_mdb_error_and_exit(int rc)
{
	print_mdb_error(rc);
	exit(rc);
}

static void insert_uint32(uint8_t *dst, const uint32_t src)
{
	uint32_t be = htonl(src);
	memcpy(dst, &be, 4);
}

static void insert_uint64(uint8_t *dst, const uint64_t src)
{
	uint32_t upper = (uint32_t) (src >> 32);
	insert_uint32(dst, upper);
	uint32_t lower = (uint32_t) src & 0xFFFFFFFF;
	insert_uint32(dst + 4, lower);
}

int trlmdb_is_put_op(uint8_t *time)
{
	return *(time + 19) & 1;
}

static int cmp_time(uint8_t *time1, uint8_t *time2)
{
	return memcmp(time1, time2, 20);
}

static int trlmdb_node_put_time_all_nodes(TRLMDB_txn *txn, uint8_t *time)
{
	MDB_cursor *cursor;
	int rc = mdb_cursor_open(txn->mdb_txn, txn->env->dbi_nodes, &cursor);
	if (rc) return rc;

	size_t node_time_size = 100;
	uint8_t *node_time = malloc(node_time_size);
	if (!node_time) return ENOMEM;
	
	MDB_val node_val, data;
	MDB_val node_time_data = {2, "ff"};
	while ((rc = mdb_cursor_get(cursor, &node_val, &data, MDB_NEXT)) == 0) {
		if (node_val.mv_size > node_time_size - TIME_LENGTH) {
			node_time_size = node_val.mv_size + TIME_LENGTH;
			node_time = realloc(node_time, node_time_size);
			if (!node_time) goto cleanup_node_time;
		}
		memcpy(node_time, node_val.mv_data, node_val.mv_size);
		memcpy(node_time + node_val.mv_size, time, TIME_LENGTH);
		MDB_val node_time_key = {node_val.mv_size + TIME_LENGTH, node_time};
		rc = mdb_put(txn->mdb_txn, txn->env->dbi_node_time, &node_time_key, &node_time_data, 0);
		if (rc) break;
	}

cleanup_node_time:
	free(node_time);

	return rc == MDB_NOTFOUND ? 0 : rc;
}

void print_mdb_val(MDB_val *val)
{
	printf("size = %zu, data = ", val->mv_size);
	for (size_t i = 0; i < val->mv_size; i++) {
		printf("%02x", *(uint8_t *)(val->mv_data + i));
	}
	printf("\n");
}

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *db_env = calloc(1, sizeof *db_env);
	if (!db_env) return ENOMEM;

	uint32_t random = arc4random();
	insert_uint32(db_env->time_component, random);
	
	int rc = mdb_env_create(&(db_env->mdb_env));
	if (rc) free(db_env);

	mdb_env_set_maxdbs(db_env->mdb_env, 5);
	
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

	rc = mdb_dbi_open(txn, DB_NODE_TIME, MDB_CREATE, &env->dbi_node_time);
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

	if (!trlmdb_is_put_op(time_val.mv_data)) return MDB_NOTFOUND;

	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, data);
}

int trlmdb_insert_time_key_data(TRLMDB_txn *txn, uint8_t *time, MDB_val *key, MDB_val *data)
{
	int rc = 0;

	MDB_val time_val = {20, time};
	
	TRLMDB_txn *child_txn;
	rc = trlmdb_txn_begin(txn->env, txn, 0, &child_txn);
	if (rc) return rc;

	rc = mdb_put(child_txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, key, 0);
	if (rc) goto cleanup_child_txn;

	if (trlmdb_is_put_op(time)) {
		rc = mdb_put(child_txn->mdb_txn, txn->env->dbi_time_to_data, &time_val,data, 0);
		if (rc) goto cleanup_child_txn;
	}

	int is_time_most_recent = 1;
	MDB_val current_time_val;
	rc = mdb_get(child_txn->mdb_txn, txn->env->dbi_key_to_time, key, &current_time_val);
	if (!rc) {
		is_time_most_recent = cmp_time(time, current_time_val.mv_data) > 0;
	}

	if (is_time_most_recent) {
		rc = mdb_put(child_txn->mdb_txn, txn->env->dbi_key_to_time, key, &time_val, 0);
		if (rc) goto cleanup_child_txn;
	}

	rc = trlmdb_node_put_time_all_nodes(child_txn, time);
	if (rc) goto cleanup_child_txn;
	
	rc = trlmdb_txn_commit(child_txn);
	goto out;
	
cleanup_child_txn:
	trlmdb_txn_abort(child_txn);
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

	if (!trlmdb_is_put_op(time_val.mv_data)) return MDB_NOTFOUND;

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

int trlmdb_cursor_get(TRLMDB_cursor *cursor, MDB_val *key, MDB_val *data, int *is_deleted, MDB_cursor_op op)
{
	MDB_val time_val;
	
	int rc = mdb_cursor_get(cursor->mdb_cursor, key, &time_val, op);
	if (rc) return rc;

	if (trlmdb_is_put_op(time_val.mv_data)) {
		*is_deleted = 0;
		return mdb_get(cursor->txn->mdb_txn, cursor->txn->env->dbi_time_to_data, &time_val, data);
	} else {
		*is_deleted = 1;
		data->mv_size = 0;
		data->mv_data = NULL;
		return 0;
	}
}

static int trlmdb_node_put_all_times(TRLMDB_txn *txn, char *node_name)
{
	MDB_cursor *cursor;
	int rc = mdb_cursor_open(txn->mdb_txn, txn->env->dbi_time_to_key, &cursor);
	if (rc) return rc;

	size_t node_name_len = strlen(node_name);
	uint8_t *node_time = malloc(node_name_len + 20);  // 20 is the length of time.
	if (!node_time) return ENOMEM;

	memcpy(node_time, node_name, node_name_len);

	MDB_val node_time_key = {node_name_len + 20, node_time};
	MDB_val node_time_data = {2, "ff"};
	MDB_val time_val, data;
	while ((rc = mdb_cursor_get(cursor, &time_val, &data, MDB_NEXT)) == 0) {
		memcpy(node_time + node_name_len,time_val.mv_data, 20);
		rc = mdb_put(txn->mdb_txn, txn->env->dbi_node_time, &node_time_key, &node_time_data, 0);
		if (rc) break;
	}
	free(node_time);
	mdb_cursor_close(cursor);
	return rc == MDB_NOTFOUND ? 0 : rc;
}

int trlmdb_node_add(TRLMDB_txn *txn, char *node_name)
{
	MDB_val key = {strlen(node_name), node_name};
	MDB_val data = {0, ""};
	int rc = mdb_put(txn->mdb_txn, txn->env->dbi_nodes, &key, &data, 0);
	if (rc) return rc;

	return trlmdb_node_put_all_times(txn, node_name);
}

int trlmdb_node_time_update(TRLMDB_txn *txn, char *node_name, uint8_t *time, uint8_t* flag)
{
	size_t node_name_len = strlen(node_name);
	uint8_t *node_time = malloc(node_name_len + 20);  // 20 is the length of time.
	if (!node_time) return ENOMEM;

	memcpy(node_time, node_name, node_name_len);
	memcpy(node_time + node_name_len,time, 20);
	MDB_val node_time_key = {node_name_len + 20, node_time};
	int rc;
	if (memcmp(flag, "tt", 2) == 0) {
		rc = mdb_del(txn->mdb_txn, txn->env->dbi_node_time, &node_time_key, NULL);
	} else {
		MDB_val node_time_data = {2, flag}; 
		rc = mdb_put(txn->mdb_txn, txn->env->dbi_node_time, &node_time_key, &node_time_data, 0);
	}

	free(node_time);
	return rc;
}

int trlmdb_node_del(TRLMDB_txn *txn, char *node_name)
{
	size_t node_name_len = strlen(node_name);
	MDB_val key = {node_name_len, node_name};
	int rc = mdb_del(txn->mdb_txn, txn->env->dbi_nodes, &key, NULL);
	if (rc) return rc;

	MDB_cursor *cursor;
	rc = mdb_cursor_open(txn->mdb_txn, txn->env->dbi_node_time, &cursor);
	if (rc) return rc;

	MDB_val node_time_val = {node_name_len, node_name};
	MDB_val data;
	rc = mdb_cursor_get(cursor, &node_time_val, &data, MDB_SET_RANGE);
	while (!rc && node_time_val.mv_size >= node_name_len && memcmp(node_time_val.mv_data, node_name, node_name_len) == 0) {
		rc = mdb_cursor_del(cursor, 0);
		if (rc) break;
		rc = mdb_cursor_get(cursor, &node_time_val, &data, MDB_NEXT);
	}

	return rc == MDB_NOTFOUND ? 0 : rc;
}

int trlmdb_get_key(TRLMDB_txn *txn, uint8_t *time, MDB_val *key)
{
	MDB_val time_val = {20, time};
	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, key);
}

/* Messages */

struct message {
	uint8_t *buffer;
	uint64_t size;
	uint64_t capacity;
};

static uint64_t decode_length(uint8_t *buffer)
{
	uint64_t upper = (uint64_t) ntohl(*(uint32_t*) buffer);
	uint64_t lower = (uint64_t) ntohl(*(uint32_t*) (buffer + 4));

	return (upper << 32) + lower;
}

struct message *message_alloc_init()
{
	struct message *msg = malloc(sizeof *msg);
	if (!msg) return NULL;

	msg->buffer = malloc(256);
	if (!msg->buffer) {
		free(msg);
		return NULL;
	}

	msg->capacity = 256;
	msg->size = 0;

	return msg;
}

void message_free(struct message *msg)
{
	free(msg->buffer);
	free(msg);
}

struct message *message_from_prefix_buffer(uint8_t *buffer)
{
	struct message *msg = malloc(sizeof *msg);
	if (!msg) return NULL;

	uint64_t size = decode_length(buffer);
	msg->buffer = buffer + 8;
	msg->size = size;
	msg->capacity = size;

	return msg;
}

int message_append(struct message *msg, uint8_t *data, uint64_t size)
{
	uint64_t new_capacity = msg->size + 8 + size;
	if (new_capacity > msg->capacity) {
		uint8_t *realloc_buffer = realloc(msg->buffer, new_capacity);
		if (!realloc_buffer) return 1;
		msg->buffer = realloc_buffer;
		msg->capacity = new_capacity;
	}

	insert_uint64(msg->buffer + msg->size, size);
	msg->size += 8;

	memcpy(msg->buffer + msg->size, data, size);
	msg->size += size;
	
	return 0;
}

uint64_t message_get_count(struct message *msg)
{
	uint8_t *buffer = msg->buffer;
	uint64_t remaining = msg->size;
	uint64_t count = 0;
	while (remaining >= 8) {
		uint64_t length = decode_length(buffer);
		if (remaining < 8 + length) return count;
		count++;
		remaining -= 8 + length;
		buffer += 8 + length;
	}
	return count;
}

int message_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size)
{
	uint8_t *buffer = msg->buffer;
	uint64_t remaining = msg->size;
	uint64_t count = 0;
	while (remaining >= 8) {
		uint64_t length = decode_length(buffer);
		if (remaining < 8 + length) return 1;
		if (index == count) {
			*data = buffer + 8;
			*size = length;
			return 0;
		}
		count++;
		remaining -= 8 + length;
		buffer += 8 + length;
	}	
	return 1;
}

/* node name message */

char *read_node_name_message(struct message *msg)
{
	uint64_t count = message_get_count(msg);
	if (count != 2) return NULL;

	uint8_t *data;
	uint64_t size;
	
	int rc = message_get_elem(msg, 0, &data, & size);
	if (rc) return NULL;

	if (size != 4 || memcmp(data, "node", 4) != 0) return NULL;

	rc = message_get_elem(msg, 1, &data, &size);
	if (rc) return NULL;

	char *node_name = malloc(size + 1);
	if (!node_name) return NULL;

	memcpy(node_name, data, size);
	node_name[size] = '\0';

	return node_name;
}

int write_node_name_message(struct message *msg, char *node_name)
{
	msg->size = 0;

	int rc = message_append(msg, (uint8_t*) "node", 4); 
	if (rc) return rc;

	rc = message_append(msg, (uint8_t*)node_name, strlen(node_name));

	return rc;
}

/* time message */

int read_time_message(TRLMDB_txn *txn, char *remote_node_name, struct message *msg)
{
	uint64_t count = message_get_count(msg);
	if (count < 3 || count > 5) return 1;

	uint8_t *data;
	uint64_t size;
	
	int rc = message_get_elem(msg, 0, &data, & size);
	if (rc) return 1;

	if (size != 4 || memcmp(data, "time", 4) != 0) return 1;

	uint8_t *flag;
	rc = message_get_elem(msg, 1, &flag, &size);
	if (rc) return 1;
	if (size != 2) return 1;
	if (flag[0] != 'f' && flag[0] != 'f') return 1;
	if (flag[1] != 'f' && flag[1] != 'f') return 1;
	
	uint8_t *time;
	
	rc = message_get_elem(msg, 2, &time, & size);
	if (rc || size != 20) return 1;

	int is_put = trlmdb_is_put_op(time);
	
	MDB_val key;
	int key_absent = trlmdb_get_key(txn, time, &key);

	if (key_absent && count > 3) {
		uint8_t *key;
		uint64_t key_size;
		rc = message_get_elem(msg, 3, &key, &key_size); 
		if (rc) return 1;

		MDB_val key_val = {key_size, key};
		
		if (is_put && count == 5) {
			uint8_t *data;
			uint64_t data_size;
			rc = message_get_elem(msg, 4, &data, &data_size); 
			if (rc) return 1;
			MDB_val data_val = {data_size, data};
			trlmdb_put(txn, &key_val, &data_val);
		} else {
			trlmdb_del(txn, &key_val);
		}
	}
	
	return trlmdb_node_time_update(txn, remote_node_name, time, flag);	
}

int write_time_message(TRLMDB_txn *txn, MDB_cursor *cursor, char *node_name, int should_reset_cursor, struct message *msg)
{
	MDB_val node_time_val;
	MDB_val flag_val;
	int rc;
	
	if (should_reset_cursor) {
		node_time_val.mv_size = strlen(node_name);
		node_time_val.mv_data = node_name;
		rc = mdb_cursor_get(cursor, &node_time_val, &flag_val, MDB_SET_RANGE);
	} else {
		rc = mdb_cursor_get(cursor, &node_time_val, &flag_val, MDB_NEXT);
	}
	if (rc) return rc;	

	uint8_t *time = node_time_val.mv_data + (node_time_val.mv_size - 20);
	MDB_val time_val = {20, time};

	MDB_val key;
	int key_known = mdb_get(txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, &key);

	uint8_t out_flag[2];
	out_flag[0] = key_known ? 't' : 'f';
	out_flag[1] = *(uint8_t*)flag_val.mv_data;

	msg->size = 0;

	rc = message_append(msg, (uint8_t*) "time", 4); 
	if (rc) return rc;

	rc = message_append(msg, out_flag, 2);
	if (rc) return rc;

	rc = message_append(msg, time, 20);
	if (rc) return rc;

	if (out_flag[1] == 't' || !key_known) return 0;

	rc = message_append(msg, (uint8_t*)key.mv_data, key.mv_size);
	if (rc) return rc;
	
	if (!trlmdb_is_put_op(time)) return 0;

	MDB_val data;
	rc = mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, &data);
	if (rc) return rc;
	
	rc = message_append(msg, (uint8_t*)data.mv_data, data.mv_size);

	return rc;
}

/*
 *  Conf file
 *
 * A conf file is read by the replicator. The format is
 * a specification of a port to listen on and a number of
 * nodes to connect to. All lines are optional. 
 * 
 * path: directory or path to the trlmdb database. 
 * node: the name of this node 
 * port: listening port 
 * remote: internet address of remote nodes 
 *
 * Example:
 *
 * path: user-database 
 * node: node-1
 * port: 8000
 * remote: 192.168.0.1:8000
 * remote: 192.168.0.2:9000
 */

struct conf_info {
	char *path;
	char *node;
	char *port;
	int nremote;
	char **remote;
};

/* trim removes leading and trailing whitespace and returns the trimmed string. The argument string is modified. str must have a null terminator */
char *trim(char *str)
{
	char *start = str;
	while (isspace(*start)) start++;

	if (*start == '\0') return start;

	char *end = start + strlen(start) - 1;
	while (isspace(*end)) end--;

	*(end + 1) = '\0';

	return start;
}

struct conf_info parse_conf_file(const char *conf_file, char **err)
{
	struct conf_info conf_info = {NULL, NULL, NULL, 0, NULL};

	/* This allocation is not checked, since there is no way of communicating an error if the
	 * error itself can not be allocated. It will never fail in practice.
	 */
	*err = malloc(100);
	
	FILE *file;
	if ((file = fopen(conf_file, "r")) == NULL) {
		sprintf(*err, "The conf file %s could not be opened", conf_file);
		return conf_info;
	}

	char line[1024];
	for (;;) {
		if (fgets(line, sizeof line, file) == NULL) break;
		if (strlen(line) >= sizeof line - 1) {
			sprintf(*err, "The conf file has too long lines");
			goto out;
		}

		char *right = line;
		char *left = strsep(&right, ":");
		if (right == NULL) continue;

		left = trim(left);
		right = trim(right);

		if (strcmp(left, "path") == 0) {
			conf_info.path = strdup(right);
		} else if (strcmp(left, "node") == 0) {
			conf_info.node = strdup(right);
		} else if (strcmp(left, "port") == 0) {
			conf_info.port = strdup(right);
		} else if (strcmp(left, "remote") == 0) {
			conf_info.nremote++;
			/* Allocation should not fail this early */ 
			conf_info.remote = realloc(conf_info.remote, conf_info.nremote);
		}
	}

	if (!feof(file)) {
		sprintf(*err, "There was a problem reading the conf file");
		goto out;
	}

	if (!conf_info.path) {
		sprintf(*err, "There is no path in the conf file");
		goto out;
	}

	if (!conf_info.node) {
		sprintf(*err, "There is no node name in the conf file");
		goto out;
	}

	if (!conf_info.port && conf_info.nremote == 0) {
		sprintf(*err, "There is no port and no remote internet addresses in the conf file");
		goto out;
	}
	
	free(*err);
	*err = NULL;

out:
	fclose(file);
	return conf_info;
}

/* Replicator state */

struct replicator_state {
	char *node;
	int socket_fd;
	TRLMDB_env *env;
};

/* Network code */

int create_listener(const int ai_family, const char *hostname, const char *servname, struct sockaddr *socket_addr)
{
	struct addrinfo hints;

	memset(&hints, 0, sizeof hints);
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = ai_family;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;
	int status;
	
	if ((status = getaddrinfo(hostname, servname, &hints, &res)) != 0) {
		fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
		return -1;
	}

	int listen_fd = -1;
	struct addrinfo *addrinfo;
	for (addrinfo = res; addrinfo != NULL; addrinfo = addrinfo->ai_next) {
		if ((listen_fd = socket(addrinfo->ai_family, addrinfo->ai_socktype, addrinfo->ai_protocol)) == -1) {
			perror("socket: ");
			continue;
		}

		int socket_option_value = 1;
		setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &socket_option_value, sizeof(int));
		
		if (bind(listen_fd, addrinfo->ai_addr, addrinfo->ai_addrlen) == -1) {
			close(listen_fd);
			perror("bind: ");
			continue;
		}

		break;
	}

	if (addrinfo == NULL) {
		fprintf(stderr, "Check that the host and port are correct, that the port is not used by another process and that the process has the right permission\n");
		return -1;
	}

	if (socket_addr != NULL) {
		*socket_addr = *addrinfo->ai_addr;
	}

	freeaddrinfo(res);
	
	int backlog = 20;
	if (listen(listen_fd, backlog) == -1) {
		perror("Error in listen(): ");
		return -1;
	}

	return listen_fd;
}

void *replicator_connection_handler(void *replicator_state);

void accept_loop(int listen_fd, TRLMDB_env *env, char *node)
{
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	for (;;) {
		struct sockaddr_storage remote_addr;
		socklen_t remote_addr_len = sizeof remote_addr;

		printf("ready to accept\n");
		int accepted_fd = accept(listen_fd, (struct sockaddr *) &remote_addr, &remote_addr_len);
		printf("accepted = %d\n", accepted_fd);
		if (accepted_fd == -1) continue;

		struct replicator_state *replicator_state = malloc(sizeof replicator_state);
		if (!replicator_state) continue;
		replicator_state->node = node;
		replicator_state->socket_fd = accepted_fd;
		replicator_state->env = env;
		
		pthread_t thread;
		if (pthread_create(&thread, &attr, replicator_connection_handler, replicator_state) != 0) {
			printf("hello\n");
			close(accepted_fd);
			free(replicator_state);
		}
	}
}

/* The replicator server */

void replicator(struct conf_info conf_info)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) print_error_and_exit("The lmdb environment could not be created"); 

	rc = trlmdb_env_open(env, conf_info.path, 0, 0644);
	if (rc) print_error_and_exit("The database could not be opened");


	
	if (conf_info.port) {
		int listen_fd = create_listener(PF_INET, "localhost", conf_info.port, NULL);
		if (listen_fd != -1) accept_loop(listen_fd, env, conf_info.node);
	}
}

/* Length prefixed messages */

struct lmessage {
	struct message *msg;
	uint8_t length_msg[8];
	uint64_t written;
	int done;
};

struct lmessage *lmessage_init(struct lmessage *lmsg, struct message *msg)
{
	lmsg->msg = msg;
	insert_uint64(lmsg->length_msg, msg->size);
	lmsg->written = 0;
	lmsg->done = 0;
	return lmsg;
}

ssize_t lmessage_write_blocking(struct lmessage *lmsg, int fd)
{
	while (lmsg->written < 8 + lmsg.msg.size) {
		if (lmsg->written < 8) {
			ssize_t written = write(fd, lmsg->length_msg + lmsg->written, 8 - lmsg->written);
			lmsg->written += written;
		} else {
			ssize_t written = write(fd, lmsg->msg + lmsg->written - 8, lmsg->msg.size - lmsg->written + 8);
			lmsg->written += written;
		}
}

/* The replicator thread start routine */

void *replicator_connection_handler(void *replicator_state)
{
	char *node = ((struct replicator_state*) replicator_state)->node;
	int socket_fd = ((struct replicator_state*) replicator_state)->socket_fd;
	TRLMDB_env *env = ((struct replicator_state*) replicator_state)->env;
	free(replicator_state);

	struct message msg;

	int rc = write_node_name_message(&msg, node);
	if (rc) goto close;

	int nwritten = 0;
	while (nwritten < msg.size) {
		int nbytes = write(socket_fd, msg.buffer + nwritten, msg.size - nwritten);
		nwritten += nbytes;
	}
	
	sleep(1);

close:
	close(socket_fd);
	return NULL;
}
