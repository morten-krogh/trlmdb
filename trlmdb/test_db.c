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

	MDB_val key_1 = {5, "key_3"};
	MDB_val val_1 = {5, "val_3"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	/* rc = trlmdb_del(txn, &key_1); */
	if (rc) print_mdb_error(rc);

	MDB_val key_2 = {5, "key_4"};
	MDB_val val_2 = {6, "val_41"};
	rc = trlmdb_put(txn, &key_2, &val_2);
	/* rc = trlmdb_del(txn, &key_2); */
	
	rc = trlmdb_txn_commit(txn);
	if (rc) print_mdb_error(rc);
}

void populate_db_2 (void)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) print_mdb_error(rc); 
	
	rc = trlmdb_env_open(env, "./trlmdb-2", 0, 0644);
	if (rc) print_mdb_error(rc);

	TRLMDB_txn *txn;
	
	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_mdb_error(rc);

	MDB_val key_1 = {5, "key_4"};
	MDB_val val_1 = {6, "val_42"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	/* rc = trlmdb_del(txn, &key_1); */
	if (rc) print_mdb_error(rc);

	MDB_val key_2 = {5, "key_5"};
	MDB_val val_2 = {5, "val_5"};
	rc = trlmdb_put(txn, &key_2, &val_2);
	/* rc = trlmdb_del(txn, &key_2); */
	
	rc = trlmdb_txn_commit(txn);
	if (rc) print_mdb_error(rc);
}

int main (void)
{
	populate_db_1();
	populate_db_2();

	return 0;
}
