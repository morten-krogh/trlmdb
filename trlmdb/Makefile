
trlmdb.o: trlmdb.h trlmdb.c
	cc -O0 -c trlmdb.c 

test: lmdb.h mdb.c midl.c midl.h trlmdb.c trlmdb.h test.c
	cc -O0 midl.c mdb.c test.c -o test

.PHONY: clean

clean:
	@- rm test
	@- rm testdb/*.mdb
