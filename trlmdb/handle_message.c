#include <string.h>

#include "trlmdb.h"
#include "message.h"
#include "handle_message.h"

int handle_time_message(TRLMDB_txn *txn, char *remote_node_name, struct message *msg)
{
	uint64_t count = message_get_count(msg);
	if (count < 3 || count > 5) return 1;

	uint8_t *data;
	uint64_t size;
	
	int rc = message_get_elem(msg, 0, &data, & size);
	if (rc) return 1;

	if (size != 4 ||  memcmp(data, "time", 4) != 0) return 1;

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
