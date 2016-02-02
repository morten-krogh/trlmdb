#include <string.h>

#include "trlmdb.h"
#include "message.h"


int handle_message(TRLMDB_txn *txn, struct message *msg)
{
	uint64_t count = message_get_count(msg);
	if (count < 4 || count > 6) return 1;

	uint8_t *data;
	uint64_t size;
	
	int rc = message_get_elem(msg, 0, &data, & size);
	if (rc) return 1;

	if (size != 4 ||  memcmp(data, "time", 4) != 0) return 1;

	rc = message_get_elem(msg, 1, &data, & size);
	if (rc) return 1;

	int local_has_key_value = 0;
	if (size == 4 && memcmp(data, "true", 4) == 0) {
		local_has_key_value = 1;
	}

	rc = message_get_elem(msg, 2, &data, & size);
	if (rc) return 1;

	int local_knows_that_remote_has_key_value = 0;
	if (size == 4 && memcmp(data, "true", 4) == 0) {
		local_knows_that_remote_has_key_value = 1;
	}

	uint8_t *time;
	
	rc = message_get_elem(msg, 2, &time, & size);
	if (rc || size != 20) return 1;

	
	
	
	
	
	
	
	




	return 0;
}
