

all: test

mdb.o: mdb.c lmdb.h midl.h
	cc -O0 -c mdb.c

midl.o: midl.c midl.h
	cc -O0 -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc -O0 -c trlmdb.c 

replicator: replicator.c trlmdb.c midl.o mdb.o
	cc -O0 midl.o mdb.o replicator.c -o replicator

test_db: test_db.c trlmdb.c
	cc -O0 midl.o mdb.o test_db.c -o test_db

test: test.c trlmdb.c mdb.o midl.o
	cc -O0 midl.o mdb.o test.c -o test

.PHONY: clean

clean:
	@- rm test
	@- rm testdb/*.mdb
	@- rm *.o
