cflags := -std=c99 -Wpedantic -O0

all: test replicator

mdb.o: mdb.c lmdb.h midl.h
	cc $(cflags) -c mdb.c

midl.o: midl.c midl.h
	cc $(cflags) -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc $(cflags) -c trlmdb.c 

replicator: replicator.c trlmdb.c midl.o mdb.o
	cc $(cflags) midl.o mdb.o replicator.c -o replicator

test_single: test_single.c trlmdb.o mdb.o midl.o
	cc $(cflags) midl.o mdb.o trlmdb.o test.c -o test

.PHONY: clean

clean:
	@- rm mdb.o
	@- rm midl.o
	@- rm trlmdb.o
	@- rm replicator
	@- rm test_single
	@- rm testdb/*.mdb
	@- rm *.o
