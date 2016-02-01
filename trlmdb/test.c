#include <stdio.h>
#include <stdlib.h>

#include "trlmdb.h"

void print_error(int rc)
{
	fprintf(stderr, "%d, %s\n", rc, mdb_strerror(rc));
	exit(rc);
}

void print_mdb_val(MDB_val *val)
{
	printf("size = %zu, data = %.*s\n", val->mv_size, (int) val->mv_size, val->mv_data);
}

int main (void)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) print_error(rc); 
	
	rc = trlmdb_env_open(env, "./testdb", 0, 0644);
	if (rc) print_error(rc);

	TRLMDB_txn *txn;
	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_error(rc);

	MDB_val key_1 = {1, "a"};
	MDB_val val_1 = {2, "aa"};
	
	rc = trlmdb_put(txn, &key_1, &val_1);
	if (rc) print_error(rc);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);

	MDB_val key_2 = {1, "a"};
	MDB_val val_2;
	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	rc = trlmdb_get(txn, &key_2, &val_2);
	if (rc) print_error(rc);
	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);
	print_mdb_val(&val_2);
	
	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_error(rc);

	MDB_val key_3 = {1, "c"};
	rc = trlmdb_del(txn, &key_3);
	//if (rc) print_error(rc);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);

	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	rc = trlmdb_get(txn, &key_2, &val_2);
	if (rc) print_error(rc);
	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);
	print_mdb_val(&val_2);
	
	trlmdb_env_close(env);

	/* nested transaction */

	rc = trlmdb_env_create(&env);
	if (rc) print_error(rc); 
	
	rc = trlmdb_env_open(env, "./testdb", 0, 0644);
	if (rc) print_error(rc);

	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_error(rc);

	key_1.mv_data = "e";
	val_1.mv_data = "ee";
	rc = trlmdb_put(txn, &key_1, &val_1);
	if (rc) print_error(rc);

	TRLMDB_txn *txn_2;
	rc = trlmdb_txn_begin(env, txn, 0, &txn_2);

	key_1.mv_data = "f";
	val_1.mv_data = "ff";
	rc = trlmdb_put(txn_2, &key_1, &val_1);
	if (rc) print_error(rc);

	rc = trlmdb_txn_commit(txn_2);
	if (rc) print_error(rc);

	key_1.mv_data = "g";
	val_1.mv_data = "gg";
	rc = trlmdb_put(txn, &key_1, &val_1);
	if (rc) print_error(rc);

	key_1.mv_data = "e";
	val_1.mv_data = "ee";
	rc = trlmdb_del(txn, &key_1);
	if (rc) print_error(rc);
	
	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);
	
	/* cursor */
	rc = trlmdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	rc = trlmdb_get(txn, &key_2, &val_2);
	if (rc) print_error(rc);

	TRLMDB_cursor *cursor;
	rc = trlmdb_cursor_open(txn, &cursor);
	if (rc) print_error(rc);

	MDB_val key, val;
	
	int is_deleted = 0;
	while ((rc = trlmdb_cursor_get(cursor, &key, &val, &is_deleted, MDB_NEXT)) == 0) {
		printf("key = %.*s, is_deleted = %d, data = %.*s\n", (int) key.mv_size,  (char *) key.mv_data, is_deleted, (int) val.mv_size, (char *) val.mv_data);
	}

	trlmdb_cursor_close(cursor);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);
}
