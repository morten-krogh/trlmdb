#ifndef MESSAGE_H
#define MESSAGE_H

struct message {
	uint8_t *buffer;
	uint64_t size;
	uint64_t capacity;
};

struct message *message_alloc_init();
void message_free(struct message *msg);
struct message *message_from_prefix_buffer(uint8_t *buffer);
int message_append(struct message *msg, uint8_t *data, uint64_t size);
uint64_t message_get_count(struct message *msg);
int message_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size);
	
#endif
