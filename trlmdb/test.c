#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "trlmdb.h"

void test(void);

int main (void)
{
	test();
	printf("All tests passed\n");
	return 0;
}

void print_mdb_val(MDB_val *val)
{
	printf("size = %zu, data = ", val->mv_size);
	for (size_t i = 0; i < val->mv_size; i++) {
		printf("%02x", *(uint8_t *)(val->mv_data + i));
	}
	printf("\n");
}

void print_table(trlmdb_txn *txn, char *table)
{
	printf("\n%s:\n", table);
	
	trlmdb_cursor *cursor;
	int rc = trlmdb_cursor_open(txn, table, &cursor);
	if (rc)
		return;
		
	rc = trlmdb_cursor_first(cursor);
	if (rc)
		return;

	MDB_val key, val;
	do {
		trlmdb_cursor_get(cursor, &key, &val);
		printf("key: ");
		print_mdb_val(&key);
		printf("val: ");
		print_mdb_val(&val);
	} while (!trlmdb_cursor_next(cursor));
}

void print_table_backwards(trlmdb_txn *txn, char *table)
{
	printf("\n%s backwards:\n", table);
	
	trlmdb_cursor *cursor;
	int rc = trlmdb_cursor_open(txn, table, &cursor);
	if (rc)
		return;
		
	rc = trlmdb_cursor_last(cursor);
	if (rc)
		return;

	MDB_val key, val;
	do {
		trlmdb_cursor_get(cursor, &key, &val);
		printf("key: ");
		print_mdb_val(&key);
		printf("val: ");
		print_mdb_val(&val);
	} while (!trlmdb_cursor_prev(cursor));
}

void print_error(int rc)
{
	fprintf(stderr, "%d, %s\n", rc, mdb_strerror(rc));
}

int cmp_mdb_val(MDB_val *val_1, MDB_val *val_2)
{
	size_t min_size = val_1->mv_size < val_2->mv_size ? val_1->mv_size : val_2->mv_size;
	int res = memcmp(val_1->mv_data, val_2->mv_data, min_size);
	if (res)
		return res;

	if (val_1->mv_size == val_2->mv_size)
		return 0;
	
	return val_1->mv_size > val_2->mv_size ? 1 : -1;
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

	char *table_1 = "table-1";
	
	MDB_val key_1 = {5, "key_1"};
	MDB_val val_1 = {5, "val_1"};
	
	rc = trlmdb_put(txn, table_1, &key_1, &val_1);
	assert(!rc);

	MDB_val val_2;
	rc = trlmdb_get(txn, table_1, &key_1, &val_2);
	assert(!rc);
	assert(!cmp_mdb_val(&val_1, &val_2));

	rc = trlmdb_put(txn, table_1, &key_1, &val_1);
	assert(!rc);

	rc = trlmdb_get(txn, table_1, &key_1, &val_2);
	assert(!rc);
	assert(!cmp_mdb_val(&val_1, &val_2));
	
	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	rc = trlmdb_txn_begin(env_1, 0, &txn);
	assert(!rc);

	rc = trlmdb_del(txn, table_1, &key_1);
	assert(!rc);

	rc = trlmdb_get(txn, table_1, &key_1, &val_2);
	assert(rc == MDB_NOTFOUND);
	print_mdb_val(&val_2);

	char *table_2 = "table-2";
	MDB_val key_3 = {5, "key_3"};
	MDB_val val_3 = {5, "val_3"};

	rc = trlmdb_put(txn, table_2, &key_3, &val_3);
	assert(!rc);

	MDB_val key_4 = {5, "key_4"};
	MDB_val val_4 = {5, "val_4"};

	rc = trlmdb_put(txn, table_2, &key_4, &val_4);
	assert(!rc);
	
	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	trlmdb_env_close(env_1);

	rc = trlmdb_env_create(&env_1);
	assert(!rc);
	
	rc = trlmdb_env_open(env_1, "./trlmdb-1", 0, 0644);
	assert(!rc);

	rc = trlmdb_txn_begin(env_1, 0, &txn);
	assert(!rc);

	rc = trlmdb_get(txn, table_2, &key_3, &val_2);
	assert(!rc);
	assert(!cmp_mdb_val(&val_3, &val_2));

	trlmdb_cursor *cursor;
	rc = trlmdb_cursor_open(txn, table_2, &cursor);
	assert(!rc);

	MDB_val key, val;
	
	rc = trlmdb_cursor_first(cursor);
	assert(!rc);

	rc = trlmdb_cursor_get(cursor, &key, &val);
	assert(!rc);
	assert(!cmp_mdb_val(&key, &key_3));
	assert(!cmp_mdb_val(&val, &val_3));

	rc = trlmdb_cursor_next(cursor);
	assert(!rc);

	rc = trlmdb_cursor_get(cursor, &key, &val);
	assert(!rc);
	assert(!cmp_mdb_val(&key, &key_4));
	assert(!cmp_mdb_val(&val, &val_4));

	rc = trlmdb_cursor_prev(cursor);
	assert(!rc);

	rc = trlmdb_cursor_get(cursor, &key, &val);
	assert(!rc);
	assert(!cmp_mdb_val(&key, &key_3));
	assert(!cmp_mdb_val(&val, &val_3));

	
	rc = trlmdb_cursor_next(cursor);
	assert(!rc);

	rc = trlmdb_cursor_get(cursor, &key, &val);
	assert(!rc);
	assert(!cmp_mdb_val(&key, &key_4));
	assert(!cmp_mdb_val(&val, &val_4));

	rc = trlmdb_cursor_next(cursor);
	assert(rc == MDB_NOTFOUND);
	
	trlmdb_cursor_close(cursor);

	MDB_val key_0 = {5, "key_0"};
	MDB_val val_0 = {5, "val_0"};

	rc = trlmdb_put(txn, table_2, &key_0, &val_0);
	assert(!rc);

	rc = trlmdb_del(txn, table_2, &key_3);
	assert(!rc);

	MDB_val key_no = {6, "key_no"};
	rc = trlmdb_del(txn, table_2, &key_no);
	assert(rc == MDB_NOTFOUND);

	rc = trlmdb_cursor_open(txn, table_2, &cursor);
	assert(!rc);

	rc = trlmdb_cursor_last(cursor);
	assert(!rc);

	rc = trlmdb_cursor_get(cursor, &key, &val);
	assert(!rc);
	assert(!cmp_mdb_val(&key, &key_4));
	assert(!cmp_mdb_val(&val, &val_4));

	trlmdb_cursor_close(cursor);

	rc = trlmdb_cursor_open(txn, table_1, &cursor);
	assert(!rc);

	rc = trlmdb_cursor_last(cursor);
	assert(rc = MDB_NOTFOUND);

	trlmdb_cursor_close(cursor);

	MDB_val key_5 = {5, "key_5"};
	MDB_val val_5 = {5, "val_5"};

	rc = trlmdb_put(txn, table_1, &key_5, &val_5);
	assert(!rc);
	
	print_table(txn, table_1);
	print_table(txn, table_2);
	
	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	
	rc = trlmdb_txn_begin(env_1, 0, &txn);
	assert(!rc);

	print_table(txn, table_1);
	
	MDB_val key_6 = {5, "key_6"};
	MDB_val val_6 = {5, "val_6"};

	rc = trlmdb_put(txn, table_1, &key_6, &val_6);
	assert(!rc);

	trlmdb_txn_abort(txn);
	
	rc = trlmdb_txn_begin(env_1, MDB_RDONLY, &txn);
	assert(!rc);

	print_table(txn, table_1);
		
	rc = trlmdb_get(txn, table_1, &key_6, &val);
	assert(rc == MDB_NOTFOUND);
	
	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	rc = trlmdb_txn_begin(env_1, 0, &txn);
	assert(!rc);

	MDB_val key_11 = {5, "key_1"};
	MDB_val val_11 = {5, "val_1"};

	rc = trlmdb_put(txn, table_1, &key_11, &val_11);
	assert(!rc);

	rc = trlmdb_get(txn, table_1, &key_11, &val_2);
	assert(!rc);
	assert(!cmp_mdb_val(&val_11, &val_2));

	MDB_val key_111 = {5, "key_1"};
	MDB_val val_111 = {5, "val_1"};

	rc = trlmdb_put(txn, table_1, &key_111, &val_111);
	assert(!rc);

	rc = trlmdb_get(txn, table_1, &key_111, &val_2);
	assert(!rc);
	assert(!cmp_mdb_val(&val_111, &val_2));

	print_table(txn, table_1);
	print_table_backwards(txn, table_1);

	print_table(txn, table_2);
	print_table_backwards(txn, table_2);
	
	char *table_3 = "table-3";
	print_table(txn, table_3);
	print_table_backwards(txn, table_3);
	
	rc = trlmdb_txn_commit(txn);
	assert(!rc);
	
	trlmdb_env_close(env_1);
}
