#ifndef HANDLE_MESSAGE_H
#define HANDLE_MESSAGE_H

int handle_read_time_message(TRLMDB_txn *txn, char *remote_node_name, struct message *msg);

#endif
