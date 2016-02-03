#include <string.h>
#include <stdlib.h>

#include "trlmdb.h"
#include "message.h"
#include "handle_message.h"

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
	
	int key_known = mdb_get(txn->mdb_txn, txn->env->dbi_time_key, &time_val, &key);

	




	return rc;
}
