cflags := -std=c99 -Wpedantic -O0

all: replicator test_single test_multi perf

mdb.o: mdb.c lmdb.h midl.h
	cc $(cflags) -c mdb.c

midl.o: midl.c midl.h
	cc $(cflags) -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc $(cflags) -c trlmdb.c

replicator: replicator.c trlmdb.o midl.o mdb.o
	cc $(cflags) midl.o mdb.o trlmdb.o replicator.c -o replicator

test_single: test_single.c trlmdb.o mdb.o midl.o
	cc $(cflags) midl.o mdb.o trlmdb.o test_single.c -o test_single

test_multi: test_multi.c trlmdb.o mdb.o midl.o
	cc $(cflags) midl.o mdb.o trlmdb.o test_multi.c -o test_multi

perf: perf.c trlmdb.o mdb.o midl.o
	cc $(clfags) midl.o mdb.o trlmdb.o perf.c -o perf

.PHONY: clean

clean:
	@- rm mdb.o
	@- rm midl.o
	@- rm trlmdb.o
	@- rm replicator
	@- rm test_single
