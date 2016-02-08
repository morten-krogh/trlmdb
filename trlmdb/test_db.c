#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "trlmdb.c"

void populate_db_1 (void)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) print_mdb_error(rc); 
	
	rc = trlmdb_env_open(env, "./trlmdb-1", 0, 0644);
	if (rc) print_mdb_error(rc);

	TRLMDB_txn *txn;
	
	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_mdb_error(rc);

	MDB_val key_1 = {5, "key_1"};
	MDB_val val_1 = {5, "val_1"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	if (rc) print_mdb_error(rc);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_mdb_error(rc);
}

int main (void)
{
	populate_db_1();

	return 0;
}
