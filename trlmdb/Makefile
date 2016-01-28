

all: test

mdb.o: mdb.c lmdb.h midl.h
	cc -O0 -c mdb.c

midl.o: midl.c midl.h
	cc -O0 -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc -O0 -c trlmdb.c 

test: test.c trlmdb.o mdb.o midl.o
	cc -O0 trlmdb.o midl.o mdb.o test.c -o test

.PHONY: clean

clean:
	@- rm test
	@- rm testdb/*.mdb
