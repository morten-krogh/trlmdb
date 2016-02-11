#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "trlmdb.h"

void test(void);

int main (void)
{
	test();
	printf("All tests passed\n");
	return 0;
}

void test(void)
{
	int rc = 0;

	trlmdb_env *env_1;
	rc = trlmdb_env_create(&env_1);
	assert(!rc);

	rc = trlmdb_env_open(env_1, "./trlmdb-1", 0, 0644);
	assert(!rc);

	trlmdb_txn *txn;
	rc = trlmdb_txn_begin(env_1, 0, &txn);
	assert(!rc);

	char *table = "table-1";
	
	MDB_val key_1 = {1, "key_1"};
	MDB_val val_1 = {2, "val_1"};
	
	rc = trlmdb_put(txn, table, &key_1, &val_1);
	assert(!rc);

	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	trlmdb_env_close(env_1);
}

/* void trlmdb_test (void) */
/* { */
/* 	int rc = 0; */
/* 	TRLMDB_env *env; */

/* 	rc = trlmdb_env_create(&env); */
/* 	if (rc) print_error(rc);  */
	
/* 	rc = trlmdb_env_open(env, "./testdb", 0, 0644); */
/* 	if (rc) print_error(rc); */

/* 	TRLMDB_txn *txn; */
	
/* 	/\* add nodes *\/ */
/* 	rc = trlmdb_txn_begin(env, NULL, 0, &txn); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_node_add(txn, "node-1"); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_node_add(txn, "mm"); */
/* 	if (rc) print_error(rc); */
	
/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */
	
/* 	/\* add keys/values *\/ */
	
/* 	rc = trlmdb_txn_begin(env, NULL, 0, &txn); */
/* 	if (rc) print_error(rc); */

/* 	MDB_val key_1 = {1, "a"}; */
/* 	MDB_val val_1 = {2, "aa"}; */
	
/* 	rc = trlmdb_put(txn, &key_1, &val_1); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */

/* 	MDB_val key_2 = {1, "a"}; */
/* 	MDB_val val_2; */
/* 	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn); */
/* 	rc = trlmdb_get(txn, &key_2, &val_2); */
/* 	if (rc) print_error(rc); */
/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */
/* 	print_mdb_val(&val_2); */
	
/* 	rc = trlmdb_txn_begin(env, NULL, 0, &txn); */
/* 	if (rc) print_error(rc); */

/* 	MDB_val key_3 = {1, "c"}; */
/* 	rc = trlmdb_del(txn, &key_3); */
/* 	if (rc && rc != MDB_NOTFOUND) print_error(rc); */

/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn); */
/* 	rc = trlmdb_get(txn, &key_2, &val_2); */
/* 	if (rc) print_error(rc); */
/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */
/* 	print_mdb_val(&val_2); */
	
/* 	trlmdb_env_close(env); */

/* 	/\* nested transaction *\/ */

/* 	rc = trlmdb_env_create(&env); */
/* 	if (rc) print_error(rc); */
	
/* 	rc = trlmdb_env_open(env, "./testdb", 0, 0644); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_txn_begin(env, NULL, 0, &txn); */
/* 	if (rc) print_error(rc); */

/* 	key_1.mv_data = "e"; */
/* 	val_1.mv_data = "ee"; */
/* 	rc = trlmdb_put(txn, &key_1, &val_1); */
/* 	if (rc) print_error(rc); */

/* 	TRLMDB_txn *txn_2; */
/* 	rc = trlmdb_txn_begin(env, txn, 0, &txn_2); */

/* 	key_1.mv_data = "f"; */
/* 	val_1.mv_data = "ff"; */
/* 	rc = trlmdb_put(txn_2, &key_1, &val_1); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_txn_commit(txn_2); */
/* 	if (rc) print_error(rc); */

/* 	key_1.mv_data = "g"; */
/* 	val_1.mv_data = "gg"; */
/* 	rc = trlmdb_put(txn, &key_1, &val_1); */
/* 	if (rc) print_error(rc); */

/* 	key_1.mv_data = "e"; */
/* 	val_1.mv_data = "ee"; */
/* 	rc = trlmdb_del(txn, &key_1); */
/* 	if (rc) print_error(rc); */
	
/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */
	
/* 	/\* cursor *\/ */
/* 	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn); */
/* 	if (rc) print_error(rc); */

/* 	TRLMDB_cursor *cursor; */
/* 	rc = trlmdb_cursor_open(txn, &cursor); */
/* 	if (rc) print_error(rc); */

/* 	MDB_val key, val; */
	
/* 	int is_deleted = 0; */
/* 	while ((rc = trlmdb_cursor_get(cursor, &key, &val, &is_deleted, MDB_NEXT)) == 0) { */
/* 		printf("key = %.*s, is_deleted = %d, data = %.*s\n", (int) key.mv_size,  (char *) key.mv_data, is_deleted, (int) val.mv_size, (char *) val.mv_data); */
/* 	} */

/* 	trlmdb_cursor_close(cursor); */

/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */

/* 	/\* delete node *\/ */
/* 	rc = trlmdb_txn_begin(env, NULL, 0, &txn); */
/* 	if (rc) print_error(rc); */

/* 	rc = trlmdb_node_del(txn, "mm"); */
/* 	if (rc) print_error(rc); */

/* 	if (rc) print_error(rc); */
	
/* 	rc = trlmdb_txn_commit(txn); */
/* 	if (rc) print_error(rc); */

/* 	trlmdb_env_close(env); */
/* } */
