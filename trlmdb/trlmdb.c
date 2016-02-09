#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>

#include "trlmdb.h"

#define DB_TIME_TO_KEY "db_time_to_key"
#define DB_TIME_TO_DATA "db_time_to_data"
#define DB_KEY_TO_TIME "db_key_to_time"
#define DB_NODES "db_nodes"
#define DB_NODE_TIME "db_node_time"

/* Structs */

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

struct conf_info {
	char *database;
	char *node;
	char *port;
	int naccept;
	char **accept_node;
	int nconnect;
	char **connect_node;
	char **connect_address;
};

struct message {
	uint8_t *buf;
	uint64_t size;
	uint64_t cap;
};

struct time {
	uint8_t seconds[4];
	uint8_t fraction[4];
	uint8_t id[4];
	uint64_t counter;
};

/* Replicator state */
struct rstate {
	char *node;
	TRLMDB_env *env;
	int connector;
	int socket_fd;
	char *connect_node;
	char *connect_hostname;
	char *connect_servname;
	int naccept;
	char **accept_node;
	int node_msg_sent;
	int node_msg_received;
	char *remote_node;
	uint8_t *read_buf;
	uint64_t read_buf_cap;
	uint64_t read_buf_size;
	int read_buf_loaded;
	struct message *write_msg;
	uint64_t write_msg_nwritten;
	int write_msg_loaded;
	uint8_t write_time[20];
	int end_of_write_loop;
	int socket_readable;
	int socket_writable;
};

/* Logging and printing */

int log_stdout(const char * restrict format, ...)
{
	va_list ap;

	va_start(ap, format);
	int rc = vprintf(format, ap);
	va_end(ap);
	return rc;
}

int log_stderr(const char * restrict format, ...)
{
	va_list ap;

	va_start(ap, format);
	int rc = vfprintf(stderr, format, ap);
	va_end(ap);
	return rc;
}

void log_fatal_err(const char * restrict format, ...)
{
 	va_list ap;

	va_start(ap, format);
	int rc = vfprintf(stderr, format, ap);
	va_end(ap);

	exit(1);
}

void log_mdb_err(int rc)
{
	log_fatal_err("lmdb error: %d, %s\n", rc, mdb_strerror(rc));
}

void log_enomem()
{
	log_fatal_err("malloc failed\n");
}

void print_buf(uint8_t *buf, uint64_t size)
{
	printf("size = %llu\n", size);
	for (uint64_t i = 0; i < size; i++) {
		printf("%02x", *(uint8_t *)(buf + i));
	}
	printf("\n");
}

void print_mdb_val(MDB_val *val)
{
	printf("size = %zu, data = ", val->mv_size);
	for (size_t i = 0; i < val->mv_size; i++) {
		printf("%02x", *(uint8_t *)(val->mv_data + i));
	}
	printf("\n");
}

uint64_t msg_get_count(struct message *msg);
int msg_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size);
void print_message(struct message *msg)
{
	uint64_t count = msg_get_count(msg);
	printf("count = %llu\n", count);
	for (uint64_t i = 0; i < count; i++) {
		uint8_t *data;
		uint64_t size;
		msg_get_elem(msg, i, &data, &size);
		printf("index = %llu\n", i);
		print_buf(data, size);
	}
}

void print_rstate(struct rstate *rs)
{
	printf("node = %s\n", rs->node);
	printf("connector = %d\n", rs->connector);
	printf("socket_fd = %d\n", rs->socket_fd);
	printf("connect_node = %s\n", rs->connect_node);
	printf("connect_hostname = %s\n", rs->connect_hostname);
	printf("connect_servname = %s\n", rs->connect_servname);
	printf("naccept = %d\n", rs->naccept);
	for (int i = 0; i < rs->naccept; i++) {
		printf("accept_node = %s\n", rs->accept_node[i]);
	}
	printf("node_msg_sent = %d\n", rs->node_msg_sent);
	printf("node_msg_received = %d\n", rs->node_msg_received);
	printf("remote_node = %s\n", rs->remote_node);
	printf("read_buf_size = %llu\n", rs->read_buf_size);
	print_buf(rs->read_buf, rs->read_buf_size);
	printf("read_buf_cap = %llu\n", rs->read_buf_cap);
	printf("read_buf_loaded = %d\n", rs->read_buf_loaded);
	printf("write_msg_nwritten = %llu\n", rs->write_msg_nwritten);
	printf("write_msg_loaded = %d\n", rs->write_msg_loaded);
	printf("write_msg\n");
	print_message(rs->write_msg);
	printf("write_time\n");
	print_buf(rs->write_time, 20);
	printf("end_of_write_loop = %d\n", rs->end_of_write_loop);
	printf("socket_readable = %d\n", rs->socket_readable);
	printf("socket_writable = %d\n", rs->socket_writable);
}

/* malloc */

/* tr_malloc logs and exits if malloc fails */
void *tr_malloc(size_t size)
{
	void *mem = malloc(size);
	if (!mem)
		log_enomem();

	return mem;
}

void *tr_realloc(void *ptr, size_t size)
{
	void *mem = realloc(ptr, size);
	if (!mem)
		log_enomem();

	return mem;
}

/* Util */

/* trim removes leading and trailing whitespace and returns the trimmed string. The argument string is modified. str must have a null terminator */
static char *trim(char *str)
{
	char *start = str;
	while (isspace(*start)) start++;

	if (*start == '\0') return start;

	char *end = start + strlen(start) - 1;
	while (isspace(*end)) end--;

	*(end + 1) = '\0';

	return start;
}

/* inserts big-endian uint32 in dst */
static void encode_uint32(uint8_t *dst, const uint32_t src)
{
	uint32_t be = htonl(src);
	memcpy(dst, &be, 4);
}

/* inserts big-endian uint64 in dst */
static void encode_uint64(uint8_t *dst, const uint64_t src)
{
	uint32_t upper = (uint32_t) (src >> 32);
	encode_uint32(dst, upper);
	uint32_t lower = (uint32_t) src & 0xFFFFFFFF;
	encode_uint32(dst + 4, lower);
}

static uint64_t decode_uint64(uint8_t *buf)
{
	uint64_t upper = (uint64_t) ntohl(*(uint32_t*) buf);
	uint64_t lower = (uint64_t) ntohl(*(uint32_t*) (buf + 4));

	return (upper << 32) + lower;
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

/* parse_conf_file logs an error and exits if the conf file is invalid */
struct conf_info *parse_conf_file(const char *conf_file)
{
	struct conf_info *conf_info = tr_malloc(sizeof *conf_info);
	*conf_info = (struct conf_info){0};

	FILE *file;
	if ((file = fopen(conf_file, "r")) == NULL)
		log_fatal_err("The conf file %s could not be opened", conf_file);

	char line[1024];
	for (;;) {
		if (fgets(line, sizeof line, file) == NULL) break;
		if (strlen(line) >= sizeof line - 1)
			log_fatal_err("The conf file %s has too long lines", conf_file);

		char *right = line;
		char *left = strsep(&right, "=");
		if (right == NULL) continue;

		left = trim(left);
		right = trim(right);

		if (strcmp(left, "database") == 0) {
			conf_info->database = strdup(right);
		} else if (strcmp(left, "node") == 0) {
			conf_info->node = strdup(right);
		} else if (strcmp(left, "port") == 0) {
			conf_info->port = strdup(right);
		} else if (strcmp(left, "accept") == 0) {
			conf_info->naccept++;
			conf_info->accept_node = tr_realloc(conf_info->accept_node, conf_info->naccept);
			conf_info->accept_node[conf_info->naccept - 1] = strdup(right);
		} else if (strcmp(left, "connect") == 0) {
			conf_info->nconnect++;
			char *address = right;
			char *node = strsep(&address, " ");
			address = trim(address);
			node = trim(node);
			conf_info->connect_node = tr_realloc(conf_info->connect_node, conf_info->nconnect);
			conf_info->connect_address = tr_realloc(conf_info->connect_address, conf_info->nconnect);
			conf_info->connect_node[conf_info->nconnect - 1] = strdup(node);
			conf_info->connect_address[conf_info->nconnect - 1] = strdup(address);
		}
	}

	if (!feof(file))
		log_fatal_err("There was a problem reading the conf file");

	if (!conf_info->database)
		log_fatal_err("There is no database path in the conf file");

	if (!conf_info->node)
		log_fatal_err("There is no node name in the conf file");

	if (conf_info->naccept != 0 && !conf_info->port)
		log_fatal_err("There is no tcp port for listening");
	
	if (conf_info->naccept == 0 && conf_info->nconnect == 0)
		log_fatal_err("There is no accept or connect nodes in the conf file");

	fclose(file);
	return conf_info;
}

/* Messages */

struct message *msg_alloc_init(uint64_t cap)
{
	cap = cap < 8 ? 8 : cap;
	struct message *msg = tr_malloc(sizeof *msg);
	msg->buf = tr_malloc(cap);
	msg->cap = cap;
	encode_uint64(msg->buf, 8);
	msg->size = 8;

	return msg;
}

void msg_free(struct message *msg)
{
	free(msg->buf);
	free(msg);
}

void msg_reset(struct message *msg)
{
	encode_uint64(msg->buf, 8);
	msg->size = 8;
}

struct message *msg_from_buf(uint8_t *buf, uint64_t buf_size)
{
	if (buf_size < 8) return NULL;
	uint64_t size = decode_uint64(buf);
	if (buf_size < size) return NULL;

	struct message *msg = msg_alloc_init(size);
	msg->size = size;
	memcpy(msg->buf, buf, size);

	return msg;
}

int msg_append(struct message *msg, uint8_t *data, uint64_t size)
{
	uint64_t new_cap = msg->size + 8 + size;
	if (new_cap > msg->cap) {
		uint8_t *realloc_buf = realloc(msg->buf, new_cap);
		if (!realloc_buf) return 1;
		msg->buf = realloc_buf;
		msg->cap = new_cap;
	}

	encode_uint64(msg->buf + msg->size, size);
	msg->size += 8;

	memcpy(msg->buf + msg->size, data, size);
	msg->size += size;

	encode_uint64(msg->buf, msg->size);

	return 0;
}

uint64_t msg_get_count(struct message *msg)
{
	uint8_t *buf = msg->buf + 8;
	uint64_t remaining = msg->size - 8;
	uint64_t count = 0;
	while (remaining >= 8) {
		uint64_t length = decode_uint64(buf);
		if (remaining < 8 + length) return count;
		count++;
		remaining -= 8 + length;
		buf += 8 + length;
	}
	return count;
}

int msg_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size)
{
	uint8_t *buf = msg->buf + 8;
	uint64_t remaining = msg->size - 8;
	uint64_t count = 0;
	while (remaining >= 8) {
		uint64_t length = decode_uint64(buf);
		if (remaining < 8 + length) return 1;
		if (index == count) {
			*data = buf + 8;
			*size = length;
			return 0;
		}
		count++;
		remaining -= 8 + length;
		buf += 8 + length;
	}	
	return 1;
}

/* replicator state */

struct rstate *rstate_alloc_init(TRLMDB_env *env, struct conf_info *conf_info, int connector)
{
	struct rstate *rs = tr_malloc(sizeof *rs);
	*rs = (struct rstate) {0};
	
	rs->node = conf_info->node;
	rs->env = env;
	rs->connector = connector;
	rs->socket_fd = -1;
	rs->naccept = conf_info->naccept;
	rs->accept_node = conf_info->accept_node;
	rs->read_buf_cap = 10000; 
	rs->read_buf = tr_malloc(rs->read_buf_cap);
	rs->write_msg = msg_alloc_init(256);
	
	return rs;
}

void rstate_free(struct rstate *rs)
{
	free(rs->connect_node);
	free(rs->connect_hostname);
	free(rs->connect_servname);
	msg_free(rs->write_msg);
	free(rs->read_buf);
	free(rs);
}

/* time stamps Operations, put or del, have time stamps.  A time stamp, called time here, is 20
 * bytes long.  Time is composed as follows.  
 *
 * 4 bytes: number of seconds since the unix epoch. The time is fixed at the beginning of the
 *  transaction
 * 
 * 4 bytes: fration second after the unix epoch. The fraction is in steps of 1/ 2^32.
 *
 * 4 bytes: A random id for each environment. This number will break ties in the unlikely event
 * of two simultaneous transactions on separate nodes.
 *
 * 8 bytes: A counter within each transaction. The counter increaes by two, and the last bit is 1
 * for a put operation and 0 for a del operation.
 *
 * Node-time is the concatenation of a node name and time. The concatenation uses '\0' as the
 * separator.  The node name must not contain the null byte.
 */

struct time *time_gettimeofday(struct time *time)
{
	struct timeval tv;
	int rc = gettimeofday(&tv, NULL);
	if (rc)
		log_fatal_err("gettimeofday failed");
	
	encode_uint32(time->seconds, (uint32_t) tv.tv_sec);
	uint64_t usec = (uint64_t) tv.tv_usec;
	uint64_t frac_sec = (usec << 32) / 1000000;
	encode_uint32(time->fraction, (uint32_t) frac_sec);

	return time;
}

struct time *time_id(struct time *time)
{
        uint32_t rnd = arc4random();
	encode_uint32(time->id, rnd);
	return time;
}

struct time *time_inc(struct time *time)
{
	time->counter += 2;
	return time;
}

struct time *time_prepare(struct time *time)
{
	time_gettimeofday(time);
	time->counter = 0;
	return time;
}

uint8_t *time_encode(struct time *time, int is_put)
{
	uint8_t *encoded = tr_malloc(20);
	memcpy(encoded, time->seconds, 4);
	memcpy(encoded + 4, time->fraction, 4);
	memcpy(encoded + 8, time->id, 4);

	uint64_t counter = time->counter + (is_put ? 1 : 0);
	encode_uint64(encoded, counter);
	
	return encoded;
}

int time_is_put(uint8_t *time)
{
	return *(time + 19) & 1;
}

static int time_cmp(uint8_t *time1, uint8_t *time2)
{
	return memcmp(time1, time2, 20);
}

uint8_t *encode_node_time(char *node, uint8_t *time)
{
	size_t len = strlen(node);
	uint8_t *node_time = tr_malloc(len + 1 + 20);
	memcpy(node_time, node, len + 1);
	memcpy(node_time + len + 1, time, 20);

	return node_time;
}

/* node name message */
char *read_node_name(struct message *msg)
{
	uint64_t count = msg_get_count(msg);
	if (count != 2) return NULL;

	uint8_t *data;
	uint64_t size;
	
	int rc = msg_get_elem(msg, 0, &data, & size);
	if (rc) return NULL;

	if (size != 4 || memcmp(data, "node", 4) != 0) return NULL;

	rc = msg_get_elem(msg, 1, &data, &size);
	if (rc) return NULL;

	char *node_name = tr_malloc(size + 1);

	memcpy(node_name, data, size);
	node_name[size] = '\0';

	return node_name;
}

void write_node_name(struct message *msg, const char *node_name)
{
	msg_reset(msg);
	msg_append(msg, (uint8_t*) "node", 4); 
	msg_append(msg, (uint8_t*)node_name, strlen(node_name));
}





























/* Managing the lmdb environment with the special databases */  

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
		if (node_val.mv_size > node_time_size - 20) {
			node_time_size = node_val.mv_size + 20;
			node_time = realloc(node_time, node_time_size);
			if (!node_time) goto cleanup_node_time;
		}
		memcpy(node_time, node_val.mv_data, node_val.mv_size);
		memcpy(node_time + node_val.mv_size, time, 20);
		MDB_val node_time_key = {node_val.mv_size + 20, node_time};
		rc = mdb_put(txn->mdb_txn, txn->env->dbi_node_time, &node_time_key, &node_time_data, 0);
		if (rc) break;
	}

cleanup_node_time:
	free(node_time);

	return rc == MDB_NOTFOUND ? 0 : rc;
}

int trlmdb_env_create(TRLMDB_env **env)
{
	TRLMDB_env *db_env = calloc(1, sizeof *db_env);
	if (!db_env) return ENOMEM;

	uint32_t random = arc4random();
	encode_uint32(db_env->time_component, random);
	
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
		encode_uint32(db_txn->time, (uint32_t) tv.tv_sec);
		uint64_t usec = (uint64_t) tv.tv_usec;
		uint64_t frac_sec = (usec << 32) / 1000000;
		encode_uint32(db_txn->time + 4, (uint32_t) frac_sec);
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

	if (!time_is_put(time_val.mv_data)) return MDB_NOTFOUND;

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

	if (time_is_put(time)) {
		rc = mdb_put(child_txn->mdb_txn, txn->env->dbi_time_to_data, &time_val,data, 0);
		if (rc) goto cleanup_child_txn;
	}

	int is_time_most_recent = 1;
	MDB_val current_time_val;
	rc = mdb_get(child_txn->mdb_txn, txn->env->dbi_key_to_time, key, &current_time_val);
	if (!rc) {
		is_time_most_recent = time_cmp(time, current_time_val.mv_data) > 0;
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
	encode_uint64(time + 12, counter);
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

	if (!time_is_put(time_val.mv_data)) return MDB_NOTFOUND;

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

	if (time_is_put(time_val.mv_data)) {
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
	int rc = mdb_put(txn->mdb_txn, txn->env->dbi_nodes, &key, &data, MDB_NOOVERWRITE);
	if (rc == MDB_KEYEXIST) return 0;
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

/* returns 1 if node exists, 0 if the node does not exist, and -1 for an error */
int trlmdb_node_exists(TRLMDB_env *env, char *node_name)
{
	MDB_val key = {strlen(node_name), node_name};
	MDB_val data = {0, ""};

	MDB_txn *txn;
	int rc = mdb_txn_begin(env->mdb_env, NULL, MDB_RDONLY, &txn);
	if (rc) return -1;

	int found = mdb_get(txn, env->dbi_nodes, &key, &data);

	mdb_txn_commit(txn);

	return found == MDB_NOTFOUND ? 0 : 1;
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



/* time message */

int read_time_msg(TRLMDB_txn *txn, char *remote_node_name, struct message *msg)
{
	uint64_t count = msg_get_count(msg);
	if (count < 3 || count > 5) return 1;

	uint8_t *data;
	uint64_t size;
	
	int rc = msg_get_elem(msg, 0, &data, &size);
	if (rc) return 1;

	if (size != 4 || memcmp(data, "time", 4) != 0) return 1;

	uint8_t *flag;
	rc = msg_get_elem(msg, 1, &flag, &size);
	if (rc) return 1;
	if (size != 2) return 1;
	if (flag[0] != 't' && flag[0] != 'f') return 1;
	if (flag[1] != 't' && flag[1] != 'f') return 1;
	
	uint8_t *time;
	
	rc = msg_get_elem(msg, 2, &time, &size);
	if (rc || size != 20) return 1;

	int is_put = time_is_put(time);
	
	MDB_val key;
	int key_absent = trlmdb_get_key(txn, time, &key);
	
	if (key_absent && count > 3) {
		uint8_t *key;
		uint64_t key_size;
		rc = msg_get_elem(msg, 3, &key, &key_size); 
		if (rc) return 1;

		MDB_val key_val = {key_size, key};
		
		if (is_put && count == 5) {
			uint8_t *data;
			uint64_t data_size;
			rc = msg_get_elem(msg, 4, &data, &data_size); 
			if (rc) return 1;
			MDB_val data_val = {data_size, data};
			trlmdb_insert_time_key_data(txn, time, &key_val, &data_val);
		} else {
			trlmdb_insert_time_key_data(txn, time, &key_val, NULL);
		}
	}
	
	return trlmdb_node_time_update(txn, remote_node_name, time, flag);	
}

/* returns MDB_NOTFOUND if time is the last entry in node_time for that node */ 
int write_time_message(TRLMDB_txn *txn, uint8_t *time, char *node, struct message *msg)
{
	size_t node_len = strlen(node);
	
	uint8_t *node_time = malloc(node_len + 20);
	if (!node_time) return ENOMEM;
	memcpy(node_time, node, node_len);
	memcpy(node_time + node_len, time, 20);
	MDB_val node_time_val = {node_len + 20, node_time};
	
	MDB_cursor *cursor;
	int rc = mdb_cursor_open(txn->mdb_txn, txn->env->dbi_node_time, &cursor);
	if (rc) {
		free(node_time);
		return rc;
	}
	
	MDB_val flag_val;
	rc = mdb_cursor_get(cursor, &node_time_val, &flag_val, MDB_SET_RANGE);
	if (rc) {
		free(node_time);
		return rc;
	}

	if (node_time_val.mv_size == node_len + 20 && memcmp(node_time_val.mv_data, node_time, node_len + 20) == 0) {
		rc = mdb_cursor_get(cursor, &node_time_val, &flag_val, MDB_NEXT);
		if (rc) {
			free(node_time);
			return rc;
		}
	}

	free(node_time);
	mdb_cursor_close(cursor);
	
	if (node_time_val.mv_size != node_len + 20 || memcmp(node_time_val.mv_data, node, node_len) != 0) return MDB_NOTFOUND;

	memcpy(time, node_time_val.mv_data + node_len, 20);

	MDB_val time_val = {20, time};
	
	MDB_val key;
	int key_known = !mdb_get(txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, &key);

	uint8_t out_flag[2];
	out_flag[0] = key_known ? 't' : 'f';
	out_flag[1] = *(uint8_t*)flag_val.mv_data;

	msg_reset(msg);

	rc = msg_append(msg, (uint8_t*) "time", 4);
	if (rc) return rc;

	rc = msg_append(msg, out_flag, 2);
	if (rc) return rc;

	rc = msg_append(msg, time, 20);
	if (rc) return rc;

	if (out_flag[1] == 'f' && key_known) {
		rc = msg_append(msg, (uint8_t*)key.mv_data, key.mv_size);
		if (rc) return rc;
	
		if (time_is_put(time)) {
			MDB_val data;
			rc = mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, &data);
			if (rc) return rc;
			rc = msg_append(msg, (uint8_t*)data.mv_data, data.mv_size);
		}
	}

	if (out_flag[0] == 't' && out_flag[1] == 't') {
		mdb_del(txn->mdb_txn, txn->env->dbi_node_time, &node_time_val, NULL); 
	}

	return 0;
}



/* Network code */

int create_listener(const int ai_family, const char *hostname, const char *servname, struct sockaddr *socket_addr)
{
	struct addrinfo hints = {0};

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

void *replicator_loop(void *arg);

void accept_loop(int listen_fd, TRLMDB_env *env, struct conf_info *conf_info)
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

		int on = 1;
		int rc = setsockopt(accepted_fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on);
		
		struct rstate *rs = rstate_alloc_init(env, conf_info, 0);
		if(!rs) continue;
		rs->socket_fd = accepted_fd;
		
		pthread_t thread;
		if (pthread_create(&thread, &attr, replicator_loop, rs) != 0) {
			printf("error creating thread\n");
			close(accepted_fd);
			rstate_free(rs);
		}
	}
}

int create_connection(const int ai_family, const char *hostname, const char *servname, struct sockaddr *socket_addr)
{
	struct addrinfo hints = {0};

	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = ai_family;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;
	int rc;

	printf("hostname = %s, servname = %s\n", hostname, servname);
	
	if ((rc = getaddrinfo(hostname, servname, &hints, &res)) != 0) {
		fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(rc));
		return -1;
	}

	int socket_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (socket_fd == -1) {
		freeaddrinfo(res);
		return -1;
	}

	int on = 1;
	rc = setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on);
	
	rc = connect(socket_fd, res->ai_addr, res->ai_addrlen);
	if (rc == -1) {
		freeaddrinfo(res);
		fprintf(stderr, "connect error to %s:%s. Trying again later\n", hostname, servname);
		return -1;
	}

	printf("connected to %s:%s\n", hostname, servname);

	freeaddrinfo(res);
	
	return socket_fd;
}

/* The replicator server */

void replicator(struct conf_info *conf_info)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) {
		log_stderr("The lmdb environment could not be created");
		exit(1);
	}

	rc = trlmdb_env_open(env, conf_info->database, 0, 0644);
	if (rc) {
		log_stderr("The database could not be opened");
		exit(1);
	}

	pthread_t *threads;
	if (conf_info->nconnect > 0) {
		threads = calloc(conf_info->nconnect, sizeof threads);
	}

	for (int i = 0; i < conf_info->nconnect; i++) {
		char *node = conf_info->connect_node[i];

		TRLMDB_txn *txn;
		int rc = trlmdb_txn_begin(env, NULL, 0, &txn);
		if (rc)
			log_mdb_err(rc);
		
		rc = trlmdb_node_add(txn, node);
		if (rc)
			log_mdb_err(rc);

		rc = trlmdb_txn_commit(txn);
		if (rc)
			log_mdb_err(rc);
		
		struct rstate *rs = rstate_alloc_init(env, conf_info, 1);
		if (!rs)
			log_enomem();

		rs->connect_node = node;
		char *address = conf_info->connect_address[i];
		char *colon = strchr(address, ':');

		if (colon) {
			rs->connect_hostname = strndup(address, colon - address);
			rs->connect_servname = strndup(colon + 1, address + strlen(address) - colon - 1);
		} else {
			rs->connect_hostname = strdup(address);
			rs->connect_servname = strdup("80");
		}

		pthread_attr_t attr;
		pthread_attr_init(&attr);
		pthread_t thread;

		if (pthread_create(&thread, &attr, replicator_loop, rs) != 0) {
			printf("error creating thread\n");
			rstate_free(rs);
			exit(1);
		}

		threads[i] = thread;
	}

	for (int i = 0; i < conf_info->naccept; i++) {
		char *node = conf_info->accept_node[i];
		
		TRLMDB_txn *txn;
		int rc = trlmdb_txn_begin(env, NULL, 0, &txn);
		if (rc)
			log_mdb_err(rc);

		rc = trlmdb_node_add(txn, node);
		if (rc)
			log_mdb_err(rc);

		rc = trlmdb_txn_commit(txn);
		if (rc)
			log_mdb_err(rc);
	}
	
	if (conf_info->port) {
		int listen_fd = create_listener(PF_INET, "localhost", conf_info->port, NULL);
		if (listen_fd != -1) accept_loop(listen_fd, env, conf_info);
	}

	for (int i = 0; i < conf_info->nconnect; i++) {
		pthread_join(threads[i], NULL);
	}
}

void connect_to_remote(struct rstate *rs)
{
	rs->node_msg_sent = 0;
	rs->node_msg_received = 0;
	
	rs->socket_fd = create_connection(PF_INET, rs->connect_hostname, rs->connect_servname, NULL);
	printf("socket_fd = %d\n", rs->socket_fd); 
	
	if (rs->socket_fd == -1) {
		printf("Connection could not be created\n");
		sleep(100);
	}
}

void send_node_msg(struct rstate *rs)
{
	struct message *msg = rs->write_msg;
	
	write_node_name(msg, rs->node);

	uint64_t nwritten = 0;
	while (nwritten < msg->size) {
		ssize_t nbytes = write(rs->socket_fd, msg->buf + nwritten, msg->size - nwritten);
		if (nbytes > 0) {
			nwritten += nbytes;
		} else {
			rs->socket_fd = -1;
			return;
		}
	}

	rs->node_msg_sent = 1;
	msg_reset(rs->write_msg);
}

void receive_node_msg(struct rstate *rs)
{
	struct message *msg = NULL;

	while (!msg) {
		if (rs->read_buf_size == rs->read_buf_cap) {
			uint8_t *realloced = (uint8_t*) realloc(rs->read_buf, 2 * rs->read_buf_cap);
			if (!realloced) {
				rs->socket_fd = -1;
				return;
			}
			rs->read_buf = realloced;
			rs->read_buf_cap *= 2;
		}
		ssize_t nread = read(rs->socket_fd, rs->read_buf + rs->read_buf_size, rs->read_buf_cap - rs->read_buf_size);
		if (nread < 1) {
			rs->socket_fd = -1;
			return;
		}
		rs->read_buf_size += nread;

		msg = msg_from_buf(rs->read_buf, rs->read_buf_size);
	}

	memmove(rs->read_buf, rs->read_buf + msg->size, rs->read_buf_size - msg->size);
	rs->read_buf_size -= msg->size;

	char *remote_node = read_node_name(msg);

	
	printf("hello\n");
	print_message(msg);
	printf("%s\n", remote_node);
	printf("hello\n");
	
	int acceptable = 0;
	if (rs->connect_node) {
		if (rs->connect_node && strcmp(rs->connect_node, remote_node) == 0) acceptable = 1;
	} else {
		for (int i = 0; i < rs->naccept; i++) {
			if (strcmp(remote_node, rs->accept_node[i]) == 0) {
				acceptable = 1;
				break;
			}
		}
	}

	rs->node_msg_received = 1;
	
	if (acceptable) {
		rs->remote_node = remote_node;
	} else {
		rs->socket_fd = -1;
		fprintf(stderr, "The remote node name is not acceptable\n");
	}
}

void read_messages_from_buf(struct rstate *rs)
{
	struct message *msg;
	uint64_t msg_index = 0;

	TRLMDB_txn *txn;
	int rc = trlmdb_txn_begin(rs->env, NULL, 0, &txn);
	if (rc) return;

	while (msg_index < rs->read_buf_size && ((msg = msg_from_buf(rs->read_buf + msg_index, rs->read_buf_size - msg_index)) != NULL)) {
		int rc = read_time_msg(txn, rs->remote_node, msg);
		msg_index += msg->size;
	}

	trlmdb_txn_commit(txn);

	if (msg_index > 0) {
		memmove(rs->read_buf, rs->read_buf + msg_index, rs->read_buf_size - msg_index);
		rs->read_buf_size -= msg_index;
	}
	rs->read_buf_loaded = 0;
}

void read_from_socket(struct rstate *rs)
{
	if (rs->read_buf_size == rs->read_buf_cap) {
		uint8_t *realloced = (uint8_t*) realloc(rs->read_buf, 2 * rs->read_buf_cap);
		if (!realloced) {
			rs->socket_fd = -1;
			return;
		}
		rs->read_buf = realloced;
		rs->read_buf_cap *= 2;
	}

	ssize_t nread = read(rs->socket_fd, rs->read_buf + rs->read_buf_size, rs->read_buf_cap - rs->read_buf_size);
	if (nread < 1) {
		rs->socket_fd = -1;
		return;
	}

	rs->read_buf_size += nread;
	rs->socket_readable = 0;
	rs->read_buf_loaded = 1;
	printf("nread = %zu\n", nread);
}

void load_write_msg(struct rstate *rs)
{
	TRLMDB_txn *txn;
	int rc = trlmdb_txn_begin(rs->env, NULL, 0, &txn);
	if (rc)
		log_mdb_err(rc);

	rc = write_time_message(txn, rs->write_time, rs->remote_node, rs->write_msg);
	if (rc)
		log_mdb_err(rc);
	
	if (rc == 0) {
		rs->write_msg_loaded = 1;
	} else if (rc == MDB_NOTFOUND) {
		rs->write_msg_loaded = 0;
		rs->end_of_write_loop = 1;
		memset(rs->write_time, 0, 20);
	}

	rc = trlmdb_txn_commit(txn);
	if (rc)
		log_mdb_err(rc);
}

void write_to_socket(struct rstate *rs)
{
	ssize_t nwritten = write(rs->socket_fd, rs->write_msg->buf, rs->write_msg->size - rs->write_msg_nwritten);
	if (nwritten < 1) {
		rs->socket_fd = -1;
		return;
	}

	rs->write_msg_nwritten += nwritten;

	if (rs->write_msg->size == rs->write_msg_nwritten) {
		rs->write_msg_nwritten = 0;
		msg_reset(rs->write_msg);
		rs->write_msg_loaded = 0;
	}

	rs->socket_writable = 0;
}

void poll_socket(struct rstate *rs)
{
	int timeout = 1;
	
	struct pollfd pollfd;
	pollfd.fd = rs->socket_fd;
	if (rs->write_msg_loaded) {
		pollfd.events = POLLRDNORM | POLLWRNORM;
	} else {
		pollfd.events = POLLRDNORM;
	}
	int rc = poll(&pollfd, 1, timeout);
	if (rc == 0) {
		printf("POLL timeout\n");
		rs->end_of_write_loop = 0;
	} else if (rc == 1) {
		if (pollfd.revents & (POLLHUP | POLLNVAL)) {
			printf("POLLHUP\n");
			close(rs->socket_fd);
			rs->socket_fd = -1;
			return;
		}
		if (pollfd.revents & POLLRDNORM) {
			printf("POLLRDNORM\n");
			rs->end_of_write_loop = 0;
			rs->socket_readable = 1;
		}
		if (pollfd.revents & POLLWRNORM) {
			printf("POLLWRNORM\n");
			rs->socket_writable = 1;
		}
	}
}

void replicator_iteration(struct rstate *rs)
{
	printf("\n\n\nIteration\n");
	print_rstate(rs);

	if (rs->socket_fd == -1 && rs->connector) {
		printf("connect to remote\n");
		connect_to_remote(rs);
	} else if (rs->socket_fd == -1) {
		printf("acceptor exits\n");
		rstate_free(rs);
		pthread_exit(NULL);
	} else if (!rs->node_msg_sent) {
		printf("send_node_msg\n");
		send_node_msg(rs);
	} else if (!rs->node_msg_received) {
		printf("receive_node_msg\n");
		receive_node_msg(rs);
	} else if (rs->read_buf_loaded) {
		printf("Read messages from buffer\n");
		read_messages_from_buf(rs);
	} else if (rs->socket_readable) {
		printf("Read from socket\n");
		read_from_socket(rs);
	} else if (!rs->write_msg_loaded && !rs->end_of_write_loop) {
		printf("Load write msg\n");
		load_write_msg(rs);
	} else if (rs->write_msg_loaded && rs->socket_writable) {
		printf("Write to socket\n");
		write_to_socket(rs);
	} else {
		printf("Poll\n");
		poll_socket(rs);
	}

	sleep(5);
}

/* The replicator thread start routine */
void *replicator_loop(void *arg)
{
	struct rstate *rs = (struct rstate*) arg;

	for (;;) {

			/* printf("rs->socket_fd = %d\n", rs->socket_fd); */
			/* if (rs->socket_fd != -1) { */
				/* close(rs->socket_fd); */
				/* rs->socket_fd = -1; */
			/* } */
			/* /\* if (!rs->connect_node || (rs->node_msg_received && !rs->remote_node)) { *\/ */
			/* 	rstate_free(rs); */
			/* 	printf("Terminate thread\n"); */
			/* 	return NULL; */
			/* } */
		/* } */
		replicator_iteration(rs);
	}
}
