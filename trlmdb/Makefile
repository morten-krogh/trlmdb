

all: test

mdb.o: mdb.c lmdb.h midl.h
	cc -O0 -c mdb.c

midl.o: midl.c midl.h
	cc -O0 -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc -O0 -c trlmdb.c 

message_coder.o: message_coder.h message_coder.c
	cc -O0 -c message_coder.c

test: test.c trlmdb.o mdb.o midl.o message_coder.o
	cc -O0 trlmdb.o midl.o mdb.o message_coder.o test.c -o test

.PHONY: clean

clean:
	@- rm test
	@- rm testdb/*.mdb
