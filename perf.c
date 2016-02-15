#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>

#include "trlmdb.h"

#define TRLMDB_DB "./databases/trlmdb-perf"
#define LMDB_DB "./databases/lmdb-perf"

#define N 1000

void run_trlmdb(void);

int main(void)
{
	run_trlmdb();
	return 0;
}

void print_error(int rc)
{
	fprintf(stderr, "%d, %s\n", rc, mdb_strerror(rc));
}

char *make_key_val(const char *base, int i, char *key_val)
{
	sprintf(key_val, "%s-%d", base, i);
	return key_val;
}

struct timeval calculate_duration(struct timeval start, struct timeval end)
{
	struct timeval duration;

	duration.tv_sec = end.tv_sec - start.tv_sec;
	
	if (start.tv_usec < end.tv_usec) {
		duration.tv_usec = end.tv_usec - start.tv_usec;
	} else {
		duration.tv_sec--;
		duration.tv_usec = end.tv_usec + (1000000 - start.tv_usec);
	}
	return duration;
}

float tvtof(struct timeval tv)
{
	return (float)tv.tv_sec + (float)tv.tv_usec / 1000000.0;
}

void run_trlmdb(void)
{
	int rc = 0;

	char key[20];
	char val[20];
	
	trlmdb_env *env;
	rc = trlmdb_env_create(&env);
	assert(!rc);

	rc = trlmdb_env_open(env, TRLMDB_DB, 0, 0644);
	assert(!rc);

	char *table = "table";

	trlmdb_txn *txn;

	struct timeval start, end;

	gettimeofday(&start, NULL);
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);

		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		rc = trlmdb_txn_begin(env, 0, &txn);
		assert(!rc);

		rc = trlmdb_put(txn, table, &mdb_key, &mdb_val);
		assert(!rc);

		rc = trlmdb_txn_commit(txn);
		assert(!rc);
	}
	gettimeofday(&end, NULL);
	printf("trlmdb, %d insertions, one transaction per insertion, duration = %f seconds\n", N, tvtof(calculate_duration(start, end)));

	gettimeofday(&start, NULL);
	rc = trlmdb_txn_begin(env, 0, &txn);
	assert(!rc);
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
		make_key_val("val", i, val);

		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val = {strlen(val), val};

		rc = trlmdb_put(txn, table, &mdb_key, &mdb_val);
		assert(!rc);

	}
	rc = trlmdb_txn_commit(txn);
	assert(!rc);
	gettimeofday(&end, NULL);
	printf("trlmdb, %d insertions, one transaction in total, duration = %f seconds\n", N, tvtof(calculate_duration(start, end)));

	gettimeofday(&start, NULL);
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val;

		rc = trlmdb_txn_begin(env, MDB_RDONLY, &txn);
		assert(!rc);

		rc = trlmdb_get(txn, table, &mdb_key, &mdb_val);
		assert(!rc);

		rc = trlmdb_txn_commit(txn);
		assert(!rc);
	}
	gettimeofday(&end, NULL);
	printf("trlmdb, %d reads, one transaction per read, duration = %f seconds\n", N, tvtof(calculate_duration(start, end)));

	gettimeofday(&start, NULL);
	rc = trlmdb_txn_begin(env, MDB_RDONLY, &txn);
	assert(!rc);
	for (int i = 0; i < N; i++) {
		make_key_val("key", i, key);
	
		MDB_val mdb_key = {strlen(key), key};
		MDB_val mdb_val;

		rc = trlmdb_get(txn, table, &mdb_key, &mdb_val);
		assert(!rc);
	}
	rc = trlmdb_txn_commit(txn);
	assert(!rc);
	gettimeofday(&end, NULL);
	printf("trlmdb, %d reads, one transaction in total, duration = %f seconds\n", N, tvtof(calculate_duration(start, end)));























}
