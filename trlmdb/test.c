#include <stdio.h>
#include <stdlib.h>

#include "trlmdb.h"

void print_error(int rc)
{
	fprintf(stderr, "%d, %s\n", rc, mdb_strerror(rc));
	exit(rc);
}

int main (void)
{
	int rc = 0;
	TRLMDB_env *env;

	rc = trlmdb_env_create(&env);
	if (rc) print_error(rc); 
	
	mdb_env_set_maxdbs(trlmdb_mdb_env(env), 10);
	
	rc = trlmdb_env_open(env, "./testdb", 0, 0644);
	if (rc) print_error(rc);

	TRLMDB_txn *txn;
	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_error(rc);

	TRLMDB_dbi *dbi;
	rc = trlmdb_dbi_open(txn, "a", MDB_CREATE, &dbi);
	if (rc) print_error(rc);

	MDB_val key_1 = {1, "c"};
	MDB_val val_1 = {2, "cc"};
	
	rc = trlmdb_put(txn, dbi, &key_1, &val_1);
	if (rc) print_error(rc);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);

	rc = trlmdb_txn_begin(env, NULL, 0, &txn);
	if (rc) print_error(rc);

	MDB_val key_2 = {1, "c"};
	
	rc = trlmdb_del(txn, dbi, &key_2);
	if (rc) print_error(rc);

	rc = trlmdb_txn_commit(txn);
	if (rc) print_error(rc);

	
	
        /* rc = mdb_dbi_open(txn, "table5", MDB_CREATE, &dbi); */
	/* if (rc != 0) { */
	/* 	fprintf(stderr, "%s\n", mdb_strerror(rc)); */
	/* 	abort(); */
	/* } */

	/* mdb_txn_commit(txn); */

	/* mdb_txn_begin(env, NULL, MDB_RDONLY, &txn); */
	/* rc = mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi); */
	/* if (rc != 0) { */
	/* 	fprintf(stderr, "%s\n", mdb_strerror(rc)); */
	/* 	abort(); */
	/* } */

	/* MDB_cursor *cursor; */
	
	/* rc = mdb_cursor_open(txn, dbi, &cursor); */

	/* MDB_val key, data; */

	/* while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) { */
	/* 	printf("key: %p %.*s, data: %p %.*s\n", */
	/* 	       key.mv_data,  (int) key.mv_size,  (char *) key.mv_data, */
	/* 	       data.mv_data, (int) data.mv_size, (char *) data.mv_data); */
	/* } */

	

	/* mdb_cursor_close(cursor); */
	
	
	trlmdb_env_close(env);
}
