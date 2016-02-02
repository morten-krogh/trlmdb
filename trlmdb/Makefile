

all: test

mdb.o: mdb.c lmdb.h midl.h
	cc -O0 -c mdb.c

midl.o: midl.c midl.h
	cc -O0 -c midl.c

trlmdb.o: trlmdb.h trlmdb.c
	cc -O0 -c trlmdb.c 

message.o: message.h message.c
	cc -O0 -c message.c

handle_message.o: handle_message.h handle_message.c
	cc -O0 -c handle_message.c

test: test.c trlmdb.o mdb.o midl.o message.o
	cc -O0 trlmdb.o midl.o mdb.o message.o test.c -o test

.PHONY: clean

clean:
	@- rm test
	@- rm testdb/*.mdb
