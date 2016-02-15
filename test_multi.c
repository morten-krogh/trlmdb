#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "trlmdb.h"

#define DB_1 "./databases/trlmdb-1"
#define DB_2 "./databases/trlmdb-2"
#define DB_3 "./databases/trlmdb-3"

#define N 100000

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
		printf("%02x", *((uint8_t *)val->mv_data + i));
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

char *make_key_val(const char *base, int i, char *key_val)
{
	sprintf(key_val, "%s-%d", base, i);
	return key_val;
}

void test(void)
{
	int rc = 0;

	char key[20];
	char val[20];
	
	trlmdb_env *env_1;
	rc = trlmdb_env_create(&env_1);
	assert(!rc);

	rc = trlmdb_env_set_mapsize(env_1, 4096 * 100000);
	assert(!rc);
	
	rc = trlmdb_env_open(env_1, DB_1, 0, 0644);
	assert(!rc);

	trlmdb_env *env_2;
	rc = trlmdb_env_create(&env_2);
	assert(!rc);

	rc = trlmdb_env_set_mapsize(env_2, 4096 * 100000);
	assert(!rc);
	
	rc = trlmdb_env_open(env_2, DB_2, 0, 0644);
	assert(!rc);

	trlmdb_env *env_3;
	rc = trlmdb_env_create(&env_3);
	assert(!rc);

	rc = trlmdb_env_set_mapsize(env_3, 4096 * 100000);
	assert(!rc);
	
	rc = trlmdb_env_open(env_3, DB_3, 0, 0644);
	assert(!rc);

	trlmdb_txn *txn;

	char *table = "table";

	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		trlmdb_env *env = i % 3 == 0 ? env_1 : (i % 3 == 1 ? env_2 : env_3);
		rc = trlmdb_txn_begin(env, 0, &txn);
		assert(!rc);
		
		rc = trlmdb_put(txn, table, &mdb_key, &mdb_val);
		assert(!rc);

		rc = trlmdb_txn_commit(txn);
		if (rc)
			print_error(rc);
		assert(!rc);
	}

	sleep(5 + N / 20000);

	trlmdb_txn_begin(env_1, MDB_RDONLY, &txn);
	assert(!rc);
	
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		MDB_val mdb_actual;
		rc = trlmdb_get(txn, table, &mdb_key, &mdb_actual);
		assert(!rc);
		assert(!cmp_mdb_val(&mdb_val, &mdb_actual));

	}

	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	trlmdb_txn_begin(env_2, MDB_RDONLY, &txn);
	assert(!rc);
	
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		MDB_val mdb_actual;
		rc = trlmdb_get(txn, table, &mdb_key, &mdb_actual);
		assert(!rc);
		assert(!cmp_mdb_val(&mdb_val, &mdb_actual));

	}

	rc = trlmdb_txn_commit(txn);
	assert(!rc);

	trlmdb_txn_begin(env_3, MDB_RDONLY, &txn);
	assert(!rc);
	
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		MDB_val mdb_actual;
		rc = trlmdb_get(txn, table, &mdb_key, &mdb_actual);
		assert(!rc);
		assert(!cmp_mdb_val(&mdb_val, &mdb_actual));

	}

	rc = trlmdb_txn_commit(txn);
	assert(!rc);
}
