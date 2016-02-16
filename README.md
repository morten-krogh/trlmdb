# Trlmdb(time replicating lightning memory database)

Trlmdb (time replicating lightning memory datadase) is a database built on top of LMDB (lightning memory database).
Trlmdb is a key-value store where eack put or get operation is time stamped and replicated to remote nodes.

## Programming language 
Trlmdb is written in C.

## Two parts
Trlmdb consists of two parts: An API that applications must use to access the database and an executable program, the replicator, that replocates the databasse to remote nodes.

## Platform
Trlmdb runs on Posix compliant platforms such as Linux, Unix, and Mac OS X. Trlmdb has not been tested on Windows.
Only minor changes shopuld be necessary to make trlmdb work on Windows.

## Dependencies
The only dependency, besides a Poix platform, is LMDB. The relevant LMDB files are included in the repository, so 
no additional installations are needed. 

## License
The license is the MIT license described in the file LICENSE. LMDB comes with itw own license which is 
described in the file LMDB_LICENSE. 

##Intallation
Clone the git repository and run 

```
make
```

To use the API, the file trlmdb.h must be included in the application and the application must be linked to
mdb.o, midl.o, trlmdb.o. The files mdb.o and midl.o originate from LMDB.

The replicator is started with 

```
./replicator conf-file 
```
The details are described below.

Check the files test_single.c and test_multi.c for examples of how to use trlmdb.

## Brief overview of trlmdb
A trlmdb system consists of one or more nodes. A node consists of
 
 * LMDB database with a fixed set of tables(LMDB dbi).
 * The replicator executable running in its own process. 
 * An application, or several, reading and writing to the LMDB database through the trlmdb API. 

The replicators establish TCP connections to each other and send messages back and forth in order to synchronize the nodes. Some of the tables in the LMDB database are used internally by the replicator to store information about the known state of remote nodes. The replicator can be stopped and restarted at any time.

Every put or delete to trlmdb is automatically time stamped. The value of a key is the determined by the latest time stamp. Applications can use distinct nodes simultaneously. Any conflicting updates to the database will be resolved using the time stamps. Trlmdb is eventually consistent. There is an unknown time interval during which a read from one node can give an old result. If the replicators are running, the delay will be short in practice. 

There can be more than one replicator and application using the same LMDB database simultaneously. It does not make sense to have more than one replicator per node, but it is possible. The replicator is multi-threaded internally, which is equivalent to having several replicators attached to the same node. Consistency is guaranteed by the LMDB transaction system. 

Further details are described below.





## Time stamps

Key-value store replication using time stamps.
