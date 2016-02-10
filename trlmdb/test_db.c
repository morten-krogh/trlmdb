#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "trlmdb.h"

void populate_db_1 (void)
{
	int rc = 0;
	trlmdb_env *env;

	rc = trlmdb_env_create(&env);
	rc = trlmdb_env_open(env, "./trlmdb-1", 0, 0644);

	trlmdb_txn *txn;
	
	rc = trlmdb_txn_begin(env, 0, &txn);

	MDB_val key_1 = {5, "key_1"};
	MDB_val val_1 = {5, "val_1"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	/* rc = trlmdb_del(txn, &key_1); */

	MDB_val key_2 = {5, "key_4"};
	MDB_val val_2 = {6, "val_41"};
	/* rc = trlmdb_put(txn, &key_2, &val_2); */
	/* rc = trlmdb_del(txn, &key_2); */
	
	rc = trlmdb_txn_commit(txn);
}

void populate_db_2 (void)
{
	int rc = 0;
	trlmdb_env *env;

	rc = trlmdb_env_create(&env);
	rc = trlmdb_env_open(env, "./trlmdb-2", 0, 0644);

	trlmdb_txn *txn;
	
	rc = trlmdb_txn_begin(env, 0, &txn);

	MDB_val key_1 = {5, "key_4"};
	MDB_val val_1 = {6, "val_42"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	/* rc = trlmdb_del(txn, &key_1); */

	MDB_val key_2 = {5, "key_5"};
	MDB_val val_2 = {5, "val_5"};
	rc = trlmdb_put(txn, &key_2, &val_2);
	/* rc = trlmdb_del(txn, &key_2); */
	
	rc = trlmdb_txn_commit(txn);
}

int main (void)
{
	populate_db_1();
	/* populate_db_2(); */

	return 0;
}
