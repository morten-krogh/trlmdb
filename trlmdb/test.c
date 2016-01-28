#include <stdio.h>
#include <stdlib.h>

#include "trlmdb.h"



int main (void)
{
	MDB_env *env;

	mdb_env_create(&env);

	mdb_env_set_maxdbs(env, 1);
	
	int rc = mdb_env_open(env, "./testdb", 0, 0644);
	if (rc != 0) {
		fprintf(stderr, "%s\n", mdb_strerror(rc));
		abort();
	}

	MDB_txn *txn;
	mdb_txn_begin(env, NULL, 0, &txn);

	MDB_dbi dbi;
        rc = mdb_dbi_open(txn, "table5", MDB_CREATE, &dbi);
	if (rc != 0) {
		fprintf(stderr, "%s\n", mdb_strerror(rc));
		abort();
	}

	mdb_txn_commit(txn);

	mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	rc = mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi);
	if (rc != 0) {
		fprintf(stderr, "%s\n", mdb_strerror(rc));
		abort();
	}

	MDB_cursor *cursor;
	
	rc = mdb_cursor_open(txn, dbi, &cursor);

	MDB_val key, data;

	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		printf("key: %p %.*s, data: %p %.*s\n",
		       key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
		       data.mv_data, (int) data.mv_size, (char *) data.mv_data);
	}

	

	mdb_cursor_close(cursor);
	
	mdb_txn_commit(txn);
	

	mdb_env_close(env);
}
