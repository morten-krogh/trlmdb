#include <stdlib.h>
#include <string.h>

#include "message.h"

/*
struct message {
	uint8_t *buffer;
	uint64_t capacity;
	uint64_t size;
};
*/

static void insert_uint32(uint8_t *dst, const uint32_t src)
{
	uint32_t be = htonl(src);
	memcpy(dst, &be, 4);
}

static void insert_uint64(uint8_t *dst, const uint64_t src)
{
	uint32_t upper = (uint32_t) (src >> 32);
	insert_uint32(dst, upper);
	uint32_t lower = (uint32_t) src & 0xFFFFFFFF;
	insert_uint32(dst + 4, lower);
}

static uint64_t decode_length(uint8_t *buffer)
{
	uint64_t upper = (uint64_t) ntohl((uint32_t) buffer);
	uint64_t lower = (uint64_t) ntohl((uint32_t) (buffer + 4));

	return (upper << 32) + lower;
}

struct message *message_alloc_init()
{
	struct message *msg = malloc(sizeof *msg);
	if (!msg) return NULL;

	msg->buffer = malloc(256);
	if (!msg->buffer) {
		free(msg);
		return NULL;
	}

	msg->capacity = 256;
	msg->size = 0;

	return msg;
}

void message_free(struct message *msg)
{
	free(msg->buffer);
	free(msg);
}


int message_append(struct message *msg, uint8_t *data, uint64_t size)
{
	uint64_t new_capacity = msg->size + 8 + size;
	if (new_capacity > msg->capacity) {
		uint8_t *realloc_buffer = realloc(msg->buffer, new_capacity);
		if (!realloc_buffer) return 1;
		msg->buffer = realloc_buffer;
		msg->capacity = new_capacity;
	}

	insert_uint64(msg->buffer + msg->size, size);
	msg->size += 8;

	memcpy(msg->buffer + msg->size, data, size);
	
	return 0;
}

uint64_t message_get_count(struct message *msg)
{
	uint8_t buffer = msg->buffer;
	uint64_t remaining = msg->size;
	uint64_t count = 0;
	while (remaining >= 8) {
		uint64_t length = decode_length(buffer);
		if (remaining < 8 + length) return count;
		count++;
		remaining -= 8 + length;
		buffer += 8 + length;
	}
	return count;
}

int message_get_elem(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size)
{






}










/*

size_t encode_length(uint64_t number, void* buffer)
{
	int has_started = 0;
	size_t index = 0;

	if (number >> 63) {
		has_started = 1;
		* (uint8_t*) buffer = (uint8_t) 129;
		index++;
	}

	for (int i = 8; i >= 0; i--) {
		uint8_t byte = (uint8_t)((number >> 7 * i) & 0x7f);
		if (byte || i == 0) has_started = 1;
		if (has_started) {
			if (i > 0) byte |= 0x80;
			*(uint8_t*) (buffer + index) = byte;
			index++;
		}
	}
	return index;
}

int decode_length(void *buffer, size_t buffer_size, uint64_t *number)
{
	*number = 0;
	for (size_t i = 0; i < buffer_size; i++) {
		if (i == 11) return 1;
		uint8_t byte = *(uint8_t*) (buffer + i);
		if (i < 10) {
			*number <<= 7;
			*number += byte & 0x7f;
		} else {
			*number <<= 1;
			*number += byte &01;
		}
		if (!(byte >> 7)) {
			return 0;
		}
	}
	return 1;
}

*/
