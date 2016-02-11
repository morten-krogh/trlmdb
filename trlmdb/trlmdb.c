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

#include "lmdb.h"

#define DB_TIME_TO_KEY "db_time_to_key"
#define DB_TIME_TO_DATA "db_time_to_data"
#define DB_KEY_TO_TIME "db_key_to_time"
#define DB_NODES "db_nodes"
#define DB_NODE_TIME "db_node_time"

/* Structs */

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

struct trlmdb_env {
	MDB_env *mdb_env;
	uint8_t time_id[4];
	MDB_dbi dbi_time_to_key;
	MDB_dbi dbi_time_to_data;
	MDB_dbi dbi_key_to_time;
	MDB_dbi dbi_nodes;
	MDB_dbi dbi_node_time;
};

struct trlmdb_txn {
	MDB_txn *mdb_txn;
	struct trlmdb_env *env;
	unsigned int flags;
	struct time *time;
};

struct trlmdb_cursor {
	struct trlmdb_txn *txn;
	MDB_cursor *mdb_cursor;
	char *table;
};

/* Replicator state */
struct rstate {
	char *node;
	struct trlmdb_env *env;
	int socket_fd;
	int connect_now;
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

static int log_stdout(const char * restrict format, ...)
{
	va_list ap;

	va_start(ap, format);
	int rc = vprintf(format, ap);
	va_end(ap);
	return rc;
}

static int log_stderr(const char * restrict format, ...)
{
	va_list ap;

	va_start(ap, format);
	int rc = vfprintf(stderr, format, ap);
	va_end(ap);
	return rc;
}

static void log_fatal_err(const char * restrict format, ...)
{
 	va_list ap;

	va_start(ap, format);
	int rc = vfprintf(stderr, format, ap);
	va_end(ap);

	exit(1);
}

static void log_mdb_err(int rc)
{
	log_fatal_err("lmdb error: %d, %s\n", rc, mdb_strerror(rc));
}

static void log_enomem()
{
	log_fatal_err("malloc failed\n");
}

static void print_buf(uint8_t *buf, uint64_t size)
{
	printf("size = %llu\n", size);
	for (uint64_t i = 0; i < size; i++) {
		printf("%02x", *(uint8_t *)(buf + i));
	}
	printf("\n");
}

static uint8_t *encode_time(struct time *time, int is_put);
static void print_struct_time(struct time *time)
{
	uint8_t *encoded_time = encode_time(time, 0);
	printf("encoded time = ");
	for (size_t i = 0; i < 20; i++) {
		printf("%02x", encoded_time[i]);
	}
	printf("\n");
}

static void print_mdb_val(MDB_val *val)
{
	printf("size = %zu, data = ", val->mv_size);
	for (size_t i = 0; i < val->mv_size; i++) {
		printf("%02x", *(uint8_t *)(val->mv_data + i));
	}
	printf("\n");
}

static uint64_t msg_get_count(struct message *msg);
static int msg_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size);
static void print_message(struct message *msg)
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

static void print_rstate(struct rstate *rs)
{
	printf("node = %s\n", rs->node);
	printf("socket_fd = %d\n", rs->socket_fd);
	printf("connect_now = %d\n", rs->connect_now);
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
static void *tr_malloc(size_t size)
{
	void *mem = malloc(size);
	if (!mem)
		log_enomem();

	return mem;
}

static void *tr_realloc(void *ptr, size_t size)
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

static struct message *msg_alloc_init(uint64_t cap)
{
	cap = cap < 8 ? 8 : cap;
	struct message *msg = tr_malloc(sizeof *msg);
	msg->buf = tr_malloc(cap);
	msg->cap = cap;
	encode_uint64(msg->buf, 8);
	msg->size = 8;

	return msg;
}

static void msg_free(struct message *msg)
{
	free(msg->buf);
	free(msg);
}

static void msg_reset(struct message *msg)
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

static int msg_append(struct message *msg, uint8_t *data, uint64_t size)
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

static uint64_t msg_get_count(struct message *msg)
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

static int msg_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size)
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
	return MDB_NOTFOUND;
}

/* replicator state */

static struct rstate *rstate_alloc_init(struct trlmdb_env *env, struct conf_info *conf_info)
{
	struct rstate *rs = tr_malloc(sizeof *rs);
	*rs = (struct rstate) {0};
	
	rs->node = conf_info->node;
	rs->env = env;
	rs->socket_fd = -1;
	rs->naccept = conf_info->naccept;
	rs->accept_node = conf_info->accept_node;
	rs->read_buf_cap = 10000; 
	rs->read_buf = tr_malloc(rs->read_buf_cap);
	rs->write_msg = msg_alloc_init(256);
	
	return rs;
}

static void rstate_free(struct rstate *rs)
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
 * Node-time is the concatenation of a node name and time.
 */

static struct time *time_gettimeofday(struct time *time)
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

static struct time *time_inc(struct time *time)
{
	time->counter += 2;
	return time;
}

static struct time *time_prepare(struct time *time)
{
	time_gettimeofday(time);
	time->counter = 0;
	return time;
}

static uint8_t *encode_time(struct time *time, int is_put)
{
	uint8_t *encoded = malloc(20);
	if (!encoded)
		return NULL;

	memcpy(encoded, time->seconds, 4);
	memcpy(encoded + 4, time->fraction, 4);
	memcpy(encoded + 8, time->id, 4);

	uint64_t counter = time->counter + (is_put ? 1 : 0);
	encode_uint64(encoded + 12, counter);
	
	return encoded;
}

static int time_is_put(uint8_t *time)
{
	return *(time + 19) & 1;
}

static int time_cmp(uint8_t *time1, uint8_t *time2)
{
	return memcmp(time1, time2, 20);
}

static uint8_t *encode_node_time(char *node, size_t node_size, uint8_t *time)
{
	uint8_t *node_time = malloc(node_size + 20);
	if (!node_time)
		return NULL;
	memcpy(node_time, node, node_size);
	memcpy(node_time + node_size, time, 20);

	return node_time;
}

/* Network code */

/* create_listener returns a valid fd. It exists if there are errors. */
static int create_listener(const char *hostname, const char *servname)
{
	struct addrinfo hints = {0};

	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = PF_INET;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;
	int status = getaddrinfo(hostname, servname, &hints, &res);
	if (status)
		log_fatal_err("getaddrinfo error: %s\n", gai_strerror(status));

	int listen_fd = -1;
	struct addrinfo *addrinfo;
	for (addrinfo = res; addrinfo != NULL; addrinfo = addrinfo->ai_next) {
		listen_fd = socket(addrinfo->ai_family, addrinfo->ai_socktype, addrinfo->ai_protocol);
		if (listen_fd == -1)
			continue;

		int socket_option_value = 1;
		setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &socket_option_value, sizeof(int));
		
		if (bind(listen_fd, addrinfo->ai_addr, addrinfo->ai_addrlen) == -1)
			log_fatal_err("bind error: %s\n", strerror(errno));

		break;
	}

	if (addrinfo == NULL) {
		fprintf(stderr, "Check that the host and port are correct, that the port is not used by another process and that the process has the right permission\n");
		return -1;
	}

	freeaddrinfo(res);
	
	int backlog = 20;
	if (listen(listen_fd, backlog) == -1)
		log_fatal_err("listen error: %s\n", strerror(errno));

	return listen_fd;
}

static void accept_loop(int listen_fd, struct trlmdb_env *env, struct conf_info *conf_info, void *(*handler)(void*))
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
		setsockopt(accepted_fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on);
		
		struct rstate *rs = rstate_alloc_init(env, conf_info);
		rs->socket_fd = accepted_fd;
		
		pthread_t thread;
		if (pthread_create(&thread, &attr,handler, rs))
			log_fatal_err("error creatng a new thread\n");
	}
}

static int create_connection(const char *hostname, const char *servname)
{
	struct addrinfo hints = {0};

	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = PF_INET;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;
	int rc;

	printf("hostname = %s, servname = %s\n", hostname, servname);
	
	if ((rc = getaddrinfo(hostname, servname, &hints, &res))) {
		log_stderr("getaddrinfo error: %s\n", gai_strerror(rc));
		return -1;
	}

	int socket_fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (socket_fd == -1) {
		freeaddrinfo(res);
		return -1;
	}

	int on = 1;
	setsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof on);
	
	rc = connect(socket_fd, res->ai_addr, res->ai_addrlen);
	if (rc == -1) {
		freeaddrinfo(res);
		log_stderr("connect error to %s:%s. Trying again later\n", hostname, servname);
		return -1;
	}

	printf("connected to %s:%s\n", hostname, servname);

	freeaddrinfo(res);
	
	return socket_fd;
}

/* node message */
static char *read_node(struct message *msg)
{
	uint64_t count = msg_get_count(msg);
	if (count != 2)
		return NULL;

	uint8_t *data;
	uint64_t size;
	
	msg_get_elem(msg, 0, &data, &size);
	if (size != 4 || memcmp(data, "node", 4) != 0)
		return NULL;

	msg_get_elem(msg, 1, &data, &size);

	char *node = tr_malloc(size + 1);

	memcpy(node, data, size);
	node[size] = '\0';

	return node;
}

static void write_node(struct message *msg, const char *node)
{
	msg_reset(msg);
	msg_append(msg, (uint8_t*) "node", 4); 
	msg_append(msg, (uint8_t*)node, strlen(node));
}

/* The trlmdb functions. trlmdb is a wrapper around the lmdb functions. trlmdb contrls the lmdb
 * database, and all dataase access should go throught these functions.
 */  

int trlmdb_env_create(struct trlmdb_env **env)
{
	*env = malloc(sizeof **env);
	if (!*env) return ENOMEM;

	**env = (struct trlmdb_env) {0}; 
	
	encode_uint32((*env)->time_id, arc4random());
	
	int rc = mdb_env_create(&((*env)->mdb_env));
	if (rc) {
		free(*env);
		return rc;
	}

	mdb_env_set_maxdbs((*env)->mdb_env, 5);

	return rc;
}

int trlmdb_env_open(struct trlmdb_env *env, const char *path, unsigned int flags, mdb_mode_t mode)
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

void trlmdb_env_close(struct trlmdb_env *env)
{
	mdb_env_close(env->mdb_env);
	free(env);
}


MDB_env *trlmdb_mdb_env(struct trlmdb_env *env)
{
	return env->mdb_env;
}

int trlmdb_txn_begin(struct trlmdb_env *env, unsigned int flags, struct trlmdb_txn **txn)
{
	*txn = malloc(sizeof **txn);
	if (!*txn)
		return ENOMEM;

	**txn = (struct trlmdb_txn) {0};
	
	(*txn)->env = env;
	(*txn)->flags = flags;
	
	(*txn)->time = malloc(sizeof *(*txn)->time);
	if (!(*txn)->time) {
		free(*txn);
		return ENOMEM;
	}

	memcpy((*txn)->time->id, env->time_id, 4);
	time_gettimeofday((*txn)->time);
	(*txn)->time->counter = 0;

	int rc = mdb_txn_begin(env->mdb_env, NULL, flags, &((*txn)->mdb_txn));
	if (rc) {
		free((*txn)->time);
		free(*txn);
	}

	return rc;
}

int trlmdb_txn_commit(struct trlmdb_txn *txn)
{
	int rc = 0;

	rc = mdb_txn_commit(txn->mdb_txn);
	free(txn->time);
	free(txn);

	return rc;
}

void trlmdb_txn_abort(struct trlmdb_txn *txn)
{
	mdb_txn_abort(txn->mdb_txn);
	free(txn->time);
	free(txn);
}

static int trlmdb_node_put_time_all_nodes(struct trlmdb_env *env, MDB_txn *txn, uint8_t *time)
{
	MDB_cursor *cursor;
	int rc = mdb_cursor_open(txn, env->dbi_nodes, &cursor);
	if (rc)
		return rc;

	MDB_val node_val;
	MDB_val empty_val;
	MDB_val node_time_val = {2, "ff"};

	while ((rc = mdb_cursor_get(cursor, &node_val, &empty_val, MDB_NEXT)) == 0) {
		uint8_t *node_time = encode_node_time(node_val.mv_data, node_val.mv_size, time);
		if (!node_time) {
			rc = ENOMEM;
			break;
		}
		MDB_val node_time_key = {node_val.mv_size + 20, node_time};
		rc = mdb_put(txn, env->dbi_node_time, &node_time_key, &node_time_val, 0);
		free(node_time);
		if (rc)
			break;
	}

	mdb_cursor_close(cursor);

	return rc == MDB_NOTFOUND ? 0 : rc;
}

static int trlmdb_insert_time_key_data(struct trlmdb_env *env, MDB_txn *txn, uint8_t *time, MDB_val *key, MDB_val *data)
{
	MDB_txn *child_txn;
	int rc = mdb_txn_begin(env->mdb_env, txn, 0, &child_txn);
	if (rc)
		return rc;	

	MDB_val time_val = {20, time};
	
	rc = mdb_put(child_txn, env->dbi_time_to_key, &time_val, key, 0);
	if (rc)
		goto abort_child_txn;

	if (time_is_put(time)) {
		rc = mdb_put(child_txn, env->dbi_time_to_data, &time_val,data, 0);
		if (rc)
			goto abort_child_txn;
	}

	int is_time_most_recent = 1;
	MDB_val existing_time_val;
	rc = mdb_get(child_txn, env->dbi_key_to_time, key, &existing_time_val);
	if (!rc) {
		is_time_most_recent = time_cmp(time, existing_time_val.mv_data) > 0;
	}

	if (is_time_most_recent) {
		rc = mdb_put(child_txn, env->dbi_key_to_time, key, &time_val, 0);
		if (rc)
			goto abort_child_txn;
	}

	rc = trlmdb_node_put_time_all_nodes(env, child_txn, time);
	if (rc)
		goto abort_child_txn;
	
	rc = mdb_txn_commit(child_txn);
	goto out;
	
abort_child_txn:
	mdb_txn_abort(child_txn);
out:	
	return rc;
}	

static int trlmdb_single_get(struct trlmdb_txn *txn, MDB_val *key, MDB_val *data)
{
	MDB_val time_val;
	int rc = mdb_get(txn->mdb_txn, txn->env->dbi_key_to_time, key, &time_val);
	if (rc)
		return rc;

	if (!time_is_put(time_val.mv_data)) return MDB_NOTFOUND;

	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, data);
}

static int trlmdb_single_put_del(struct trlmdb_txn *txn, MDB_val *key, MDB_val *data)
{
	int is_put = data != NULL;
	uint8_t *time = encode_time(txn->time, is_put);
	if (!time)
		return ENOMEM;
	
	time_inc(txn->time);
	
	return trlmdb_insert_time_key_data(txn->env, txn->mdb_txn, time, key, data);
}

static int trlmdb_single_put(struct trlmdb_txn *txn, MDB_val *key, MDB_val *data)
{
	return trlmdb_single_put_del(txn, key, data);
}

static int trlmdb_single_del(struct trlmdb_txn *txn, MDB_val *key)
{
	MDB_val time_val;
	int rc = mdb_get(txn->mdb_txn, txn->env->dbi_key_to_time, key, &time_val);
	if (rc)
		return rc;

	if (!time_is_put(time_val.mv_data)) return MDB_NOTFOUND;

	return trlmdb_single_put_del(txn, key, NULL);
}

static int trlmdb_node_put_all_times(struct trlmdb_env *env, MDB_txn *txn, char *node)
{
	size_t node_len = strlen(node);
	MDB_val node_time_data = {2, "ff"};
	
	MDB_cursor *cursor;
	int rc = mdb_cursor_open(txn, env->dbi_time_to_key, &cursor);
	if (rc)
		return rc;

	MDB_val time_val, data;
	while ((rc = mdb_cursor_get(cursor, &time_val, &data, MDB_NEXT)) == 0) {
		uint8_t *node_time = encode_node_time(node, node_len, time_val.mv_data);
		if (!node_time)
			return ENOMEM;

		MDB_val node_time_key = {node_len + 20, node_time};
		rc = mdb_put(txn, env->dbi_node_time, &node_time_key, &node_time_data, 0);
		free(node_time);
		if (rc)
			return rc;
	}

	mdb_cursor_close(cursor);
	return rc == MDB_NOTFOUND ? 0 : rc;
}

static int trlmdb_node_add(struct trlmdb_env *env, char *node)
{
	MDB_txn *txn;
	int rc = mdb_txn_begin(env->mdb_env, NULL, 0, &txn);
	if (rc)
		return rc;

	MDB_val node_val = {strlen(node), node};
	MDB_val data = {0, ""};
	rc = mdb_put(txn, env->dbi_nodes, &node_val, &data, MDB_NOOVERWRITE);
	if (rc) {
		mdb_txn_commit(txn);
		return rc == MDB_KEYEXIST ? 0 : rc;
	}

	rc = trlmdb_node_put_all_times(env, txn, node);
	if (rc) {
		mdb_txn_abort(txn);
		return rc;
	}

	rc = mdb_txn_commit(txn);
	return rc;	
}

static int trlmdb_node_del(struct trlmdb_env *env, char *node)
{
	MDB_txn *txn;
	int rc = mdb_txn_begin(env->mdb_env, NULL, 0, &txn);
	if (rc)
		return rc;

	size_t node_len = strlen(node);
	MDB_val node_val = {node_len, node};
	rc = mdb_del(txn, env->dbi_nodes, &node_val, NULL);
	if (rc) {
		mdb_txn_abort(txn);
		return rc;
	}

	MDB_cursor *cursor;
	rc = mdb_cursor_open(txn, env->dbi_node_time, &cursor);
	if (rc) {
		mdb_txn_abort(txn);
		return rc;
	}

	MDB_val node_time_val = {node_len, node};
	MDB_val data;
	rc = mdb_cursor_get(cursor, &node_time_val, &data, MDB_SET_RANGE);
	for (;;) {
		if (rc)
			break;

		if (node_time_val.mv_size >= node_len && memcmp(node_time_val.mv_data, node, node_len) == 0)
			mdb_cursor_del(cursor, 0);

		rc = mdb_cursor_get(cursor, &node_time_val, &data, MDB_NEXT);
	}

	return mdb_txn_commit(txn);
}

static int trlmdb_node_time_update(struct trlmdb_txn *txn, char *node, uint8_t *time, uint8_t* flag)
{
	size_t node_len = strlen(node);
	uint8_t *node_time = encode_node_time(node, node_len, time);
	if (!node_time)
		return ENOMEM;

	MDB_val node_time_key = {node_len + 20, node_time};
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
static int trlmdb_node_exists(struct trlmdb_env *env, char *node)
{
	MDB_val key = {strlen(node), node};
	MDB_val data = {0, ""};

	MDB_txn *txn;
	int rc = mdb_txn_begin(env->mdb_env, NULL, MDB_RDONLY, &txn);
	if (rc)
		return -1;

	rc = mdb_get(txn, env->dbi_nodes, &key, &data);

	mdb_txn_commit(txn);

	return rc == MDB_NOTFOUND ? 0 : 1;
}

static int trlmdb_get_key_for_time(struct trlmdb_txn *txn, uint8_t *time, MDB_val *key)
{
	MDB_val time_val = {20, time};
	return mdb_get(txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, key);
}

/* table_key encoding */

static MDB_val *encode_table_key(char *table, MDB_val *key)
{
	size_t table_len = strlen(table);

	MDB_val *table_key = malloc(sizeof *table_key);
	if (!table_key)
		return NULL;

	table_key->mv_size = table_len + 1 + key->mv_size;
	table_key->mv_data = malloc(table_key->mv_size);
	if (!table_key->mv_data) {
		free(table_key);
		return NULL;
	}

	memcpy(table_key->mv_data, table, table_len);
	memset(table_key->mv_data + table_len, 0, 1);
	memcpy(table_key->mv_data + 1 + table_len, key->mv_data, key->mv_size);

	return table_key;
}

static void free_table_key(MDB_val *table_key)
{
	free(table_key->mv_data);
	free(table_key);
}

static int remove_table_prefix(MDB_val *table_key, MDB_val *key)
{
	size_t table_len = strnlen(table_key->mv_data, table_key->mv_size);
	if (table_len == table_key->mv_size)
		return EINVAL;
	
	key->mv_size = table_key->mv_size - table_len - 1;
	key->mv_data = table_key->mv_data + table_len + 1;

	return 0;
}

/* public functions for accessing the database */

/* trlmdb_get looks up the key in the table.
 * The table name must be null terminated.
 * The return value is
 * 0 at success and value will contain the result.
 * MDB_NOTFOUND if the key was absent.
 * ENOMEM if the operation failed due to memory allocation failure.
 */

int trlmdb_get(struct trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value)
{
	MDB_val *table_key = encode_table_key(table, key);
	if (!table_key)
		return ENOMEM;

        int rc = trlmdb_single_get(txn, table_key, value);
	free_table_key(table_key);

	return rc;
}

int trlmdb_put(struct trlmdb_txn *txn, char *table, MDB_val *key, MDB_val *value)
{
	MDB_val *table_key = encode_table_key(table, key);
	if (!table_key)
		return ENOMEM;

        int rc = trlmdb_single_put(txn, table_key, value);
	free_table_key(table_key);

	return rc;
}

int trlmdb_del(struct trlmdb_txn *txn, char *table, MDB_val *key)
{
	MDB_val *table_key = encode_table_key(table, key);
	if (!table_key)
		return ENOMEM;

        int rc = trlmdb_single_del(txn, table_key);
	free_table_key(table_key);

	return rc;
}

int trlmdb_cursor_open(struct trlmdb_txn *txn, char *table, struct trlmdb_cursor **cursor)
{
	*cursor = malloc(sizeof **cursor);
	if (!*cursor) return ENOMEM; 

	(*cursor)->txn = txn;
	(*cursor)->table = strdup(table);
	return mdb_cursor_open(txn->mdb_txn, txn->env->dbi_key_to_time, &((*cursor)->mdb_cursor));
}

void trlmdb_cursor_close(struct trlmdb_cursor *cursor){
	mdb_cursor_close(cursor->mdb_cursor);
	free(cursor->table);
	free(cursor);
}

int trlmdb_cursor_first(struct trlmdb_cursor *cursor)
{
	size_t table_len = strlen(cursor->table);
	MDB_val key = {table_len, cursor->table};
	MDB_val time_val;
	
	int rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_SET_RANGE);
	while (!rc && !time_is_put(time_val.mv_data)) {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_NEXT);
	}
		
	if (rc)
		return rc;

	if (table_len + 1 > key.mv_size || memcmp(cursor->table, key.mv_data, table_len + 1))
		return MDB_NOTFOUND;

	return 0;
}

int trlmdb_cursor_last(struct trlmdb_cursor *cursor)
{
	size_t table_len = strlen(cursor->table);
	uint8_t *table_successor = malloc(table_len + 1);
	if (!table_successor)
		return ENOMEM;

	memcpy(table_successor, cursor->table, table_len);
	table_successor[table_len] = 1;

	MDB_val key = {table_len + 1, table_successor};
	MDB_val time_val;
	
	int rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_SET_RANGE);
	if (rc) {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_LAST);
		if (rc)
			return rc;
	} else {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_PREV);
		if (rc)
			return rc;
	}
		
	while (!rc && !time_is_put(time_val.mv_data)) {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_PREV);
	}
		
	if (rc)
		return rc;

	if (table_len + 1 > key.mv_size || memcmp(cursor->table, key.mv_data, table_len + 1))
		return MDB_NOTFOUND;

	return 0;
}

int trlmdb_cursor_next(struct trlmdb_cursor *cursor)
{
	size_t table_len = strlen(cursor->table);
	MDB_val key;
	MDB_val time_val;

	int rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_NEXT);
	while (!rc && !time_is_put(time_val.mv_data)) {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_NEXT);
	}
		
	if (rc)
		return rc;

	if (table_len + 1 > key.mv_size || memcmp(cursor->table, key.mv_data, table_len + 1))
		return MDB_NOTFOUND;

	return 0;
}

int trlmdb_cursor_prev(struct trlmdb_cursor *cursor)
{
	size_t table_len = strlen(cursor->table);
	MDB_val key;
	MDB_val time_val;

	int rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_PREV);
	while (!rc && !time_is_put(time_val.mv_data)) {
		rc = mdb_cursor_get(cursor->mdb_cursor, &key, &time_val, MDB_PREV);
	}
		
	if (rc)
		return rc;

	if (table_len + 1 > key.mv_size || memcmp(cursor->table, key.mv_data, table_len + 1))
		return MDB_NOTFOUND;

	return 0;
}

int trlmdb_cursor_get(struct trlmdb_cursor *cursor, MDB_val *key, MDB_val *val)
{
	MDB_val table_key, time_val;
	int rc = mdb_cursor_get(cursor->mdb_cursor, &table_key, &time_val, MDB_GET_CURRENT);
	if (rc || !time_is_put(time_val.mv_data))
		return MDB_NOTFOUND;

	rc = remove_table_prefix(&table_key, key);
	if (rc)
		return rc;
	
	return mdb_get(cursor->txn->mdb_txn, cursor->txn->env->dbi_time_to_data, &time_val, val);
}

/* time message */

/* read_time_msg reads the msg, verifies that it is a time msg, and inserts the information in the database */
static int read_time_msg(struct trlmdb_txn *txn, char *remote_node, struct message *msg)
{
	uint64_t count = msg_get_count(msg);
	if (count < 3 || count > 5)
		return EINVAL;

	uint8_t *data;
	uint64_t size;
	
	msg_get_elem(msg, 0, &data, &size);

	if (size != 4 || memcmp(data, "time", 4) != 0)
		return EINVAL;

	uint8_t *flag;
	msg_get_elem(msg, 1, &flag, &size);
	if (size != 2 || (flag[0] != 't' && flag[0] != 'f') || (flag[1] != 't' && flag[1] != 'f'))
		return EINVAL;
	
	uint8_t *time;
	msg_get_elem(msg, 2, &time, &size);
	if (size != 20)
		return EINVAL;

	int is_put = time_is_put(time);
	
	MDB_val key;
	int key_absent = trlmdb_get_key_for_time(txn, time, &key);
	
	if (key_absent) {
		if (count < 4)
			return EINVAL;

		uint8_t *key;
		uint64_t key_size;
		msg_get_elem(msg, 3, &key, &key_size); 
		MDB_val key_val = {key_size, key};

		if (is_put && count == 5) {
			uint8_t *data;
			uint64_t data_size;
			msg_get_elem(msg, 4, &data, &data_size); 

			MDB_val data_val = {data_size, data};
			trlmdb_insert_time_key_data(txn->env, txn->mdb_txn, time, &key_val, &data_val);
		} else {
			trlmdb_insert_time_key_data(txn->env, txn->mdb_txn, time, &key_val, NULL);
		}
	}
	
	return trlmdb_node_time_update(txn, remote_node, time, flag);	
}

/* load_time_message reads from the database and writes a new message that can be sent on the network
 * It finds the next time to send to node.
 * It returns 0 if a msg is loaded, MDB_NOTFOUND if time is the last entry in node_time for that node 
 * and ENOMEM if there was a memory problem.
 */ 
static int load_time_msg(struct trlmdb_txn *txn, uint8_t *time, char *node, struct message *msg)
{
	size_t node_len = strlen(node);

	uint8_t *node_time = encode_node_time(node, node_len, time);
	if (!node_time)
		return ENOMEM;

	MDB_val node_time_val = {node_len + 20, node_time};
	MDB_cursor *cursor;
        mdb_cursor_open(txn->mdb_txn, txn->env->dbi_node_time, &cursor);

	MDB_val flag_val;
	int rc = mdb_cursor_get(cursor, &node_time_val, &flag_val, MDB_SET_RANGE);
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
	
	if (node_time_val.mv_size != node_len + 20 || memcmp(node_time_val.mv_data, node, node_len) != 0)
		return MDB_NOTFOUND;

	memcpy(time, node_time_val.mv_data + node_len, 20);

	MDB_val time_val = {20, time};
	
	MDB_val key;
	int key_known = !mdb_get(txn->mdb_txn, txn->env->dbi_time_to_key, &time_val, &key);

	uint8_t out_flag[2];
	out_flag[0] = key_known ? 't' : 'f';
	out_flag[1] = *(uint8_t*)flag_val.mv_data;

	msg_reset(msg);

	rc = msg_append(msg, (uint8_t*) "time", 4);
	if (rc)
		return rc;

	rc = msg_append(msg, out_flag, 2);
	if (rc)
		return rc;

	rc = msg_append(msg, time, 20);
	if (rc)
		return rc;

	if (out_flag[1] == 'f' && key_known) {
		rc = msg_append(msg, (uint8_t*)key.mv_data, key.mv_size);
		if (rc)
			return rc;
	
		if (time_is_put(time)) {
			MDB_val data;
			rc = mdb_get(txn->mdb_txn, txn->env->dbi_time_to_data, &time_val, &data);
			if (rc)
				return rc;

			rc = msg_append(msg, (uint8_t*)data.mv_data, data.mv_size);
			if (rc)
				return rc;
		}
	}

	if (out_flag[0] == 't' && out_flag[1] == 't') {
		mdb_del(txn->mdb_txn, txn->env->dbi_node_time, &node_time_val, NULL); 
	}

	return 0;
}

/* The replicator server 
 * replicator(struct conf_info*) is called by main to start the replicator
*/

static void *replicator_loop(void *arg);

void replicator(struct conf_info *conf_info)
{
	int rc = 0;
	struct trlmdb_env *env;

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

	for (int i = 0; i < conf_info->naccept; i++) {
		char *node = conf_info->accept_node[i];

		int rc = trlmdb_node_add(env, node);
		if (rc)
			log_mdb_err(rc);
	}

	pthread_t *threads;
	if (conf_info->nconnect > 0) {
		threads = calloc(conf_info->nconnect, sizeof threads);
	}

	for (int i = 0; i < conf_info->nconnect; i++) {
		char *node = conf_info->connect_node[i];

		int rc = trlmdb_node_add(env, node);
		if (rc)
			log_mdb_err(rc);
		
		struct rstate *rs = rstate_alloc_init(env, conf_info);
		if (!rs)
			log_enomem();

		rs->connect_now = 1;
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

	if (conf_info->port) {
		int listen_fd = create_listener("localhost", conf_info->port);
		printf("listen_fd = %d\n", listen_fd);
		if (listen_fd != -1)
			accept_loop(listen_fd, env, conf_info, replicator_loop);
	}

	for (int i = 0; i < conf_info->nconnect; i++) {
		pthread_join(threads[i], NULL);
	}
}

/* event handlers in the replicator loop */

static void connect_to_remote(struct rstate *rs)
{
	rs->node_msg_sent = 0;
	rs->node_msg_received = 0;
	rs->connect_now = 0;
	rs->write_msg_loaded = 0;
	rs->read_buf_size = 0;
	rs->read_buf_loaded = 0;
	
	rs->socket_fd = create_connection(rs->connect_hostname, rs->connect_servname);
}

static void send_node_msg(struct rstate *rs)
{
	struct message *msg = rs->write_msg;
	
	write_node(rs->write_msg, rs->node);

	rs->node_msg_sent = 1;
	rs->write_msg_loaded = 1;
}

static void read_node_msg_from_buf(struct rstate *rs)
{
	struct message *msg = msg_from_buf(rs->read_buf, rs->read_buf_size);
	if (!msg) {
		rs->read_buf_loaded = 0;
		return;
	}

	memmove(rs->read_buf, rs->read_buf + msg->size, rs->read_buf_size - msg->size);
	rs->read_buf_size -= msg->size;
	rs->read_buf_loaded = rs->read_buf_size > 0;

	char *remote_node = read_node(msg);
	if (!remote_node)
		return;

	int acceptable = 0;
	if (rs->connect_node && strcmp(rs->connect_node, remote_node) == 0) {
		acceptable = 1;
	} else if (!rs->connect_node) {
		for (int i = 0; i < rs->naccept; i++) {
			if (strcmp(remote_node, rs->accept_node[i]) == 0) {
				acceptable = 1;
				break;
			}
		}
	}
	
	if (acceptable) {
		rs->node_msg_received = 1;
		rs->remote_node = remote_node;
	} else {
		log_fatal_err("The remote node name is not acceptable\n");
	}
}

static void read_time_msg_from_buf(struct rstate *rs)
{
	struct message *msg;
	uint64_t msg_index = 0;

	struct trlmdb_txn *txn;
	int rc = trlmdb_txn_begin(rs->env, 0, &txn);
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

static void read_from_socket(struct rstate *rs)
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

static void load_write_msg(struct rstate *rs)
{
	struct trlmdb_txn *txn;
	int rc = trlmdb_txn_begin(rs->env, 0, &txn);
	if (rc)
		log_mdb_err(rc);

	rc = load_time_msg(txn, rs->write_time, rs->remote_node, rs->write_msg);
	if (rc == ENOMEM)
		log_mdb_err(rc);
	
	if (rc == 0)
		rs->write_msg_loaded = 1;

	if (rc == MDB_NOTFOUND) {
		rs->write_msg_loaded = 0;
		rs->end_of_write_loop = 1;
		memset(rs->write_time, 0, 20);
	}

	rc = trlmdb_txn_commit(txn);
	if (rc)
		log_mdb_err(rc);
}

static void write_to_socket(struct rstate *rs)
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

static void poll_socket(struct rstate *rs)
{
	int timeout = 1000; /* milliseconds */
	
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

static void replicator_iteration(struct rstate *rs)
{
	printf("\n\n\nIteration\n");
	
	if (rs->socket_fd == -1 && rs->connect_node && rs->connect_now) {
		printf("connect to remote\n");
		connect_to_remote(rs);
	} else if (rs->socket_fd == -1 && rs->connect_node) {
		printf("sleeping before connecting again\n");
		rs->connect_now = 1;
		sleep(10);
	} else if (rs->socket_fd == -1) {
		printf("acceptor exits\n");
		rstate_free(rs);
		pthread_exit(NULL);
	} else if (!rs->node_msg_sent) {
		printf("send_node_msg\n");
		send_node_msg(rs);
	} else if (rs->read_buf_loaded && !rs->node_msg_received) {
		printf("Read node message from buffer\n");
		read_node_msg_from_buf(rs);
	} else if (rs->read_buf_loaded) {
		printf("Read time message from buffer\n");
		read_time_msg_from_buf(rs);
	} else if (rs->socket_readable) {
		printf("Read from socket\n");
		read_from_socket(rs);
	} else if (!rs->write_msg_loaded && !rs->end_of_write_loop && rs->remote_node) {
		printf("Load write msg\n");
		load_write_msg(rs);
	} else if (rs->write_msg_loaded && rs->socket_writable) {
		printf("Write to socket\n");
		write_to_socket(rs);
	} else {
		printf("Poll\n");
		poll_socket(rs);
	}

	print_rstate(rs);
	/* sleep(1); */
}

/* The replicator thread start routine */
static void *replicator_loop(void *arg)
{
	struct rstate *rs = (struct rstate*) arg;

	for (;;) {
		replicator_iteration(rs);
	}
}
