#ifndef MESSAGE_H
#define MESSAGE_H

struct message {
	uint8_t *buffer;
	uint64_t capacity;
	uint64_t size;
};

struct message *message_alloc_init();
void message_free(struct message *msg);
int message_append(struct message *msg, uint8_t *data, uint64_t size);
int message_get_comp(struct message *msg, uint64_t index, uint8_t **data, uint64_t *size);










/* The encoding of an unsigned integer is a series of bytes where the most significant bit
 * is one except for the last byte where it is zero. The value of an encoded series of bytes is
 * given by concatenating the 7 least significant bits of all bytes and intepreting them as a binary number
 * Examples: 
 *
 * 0 <= x <= 127: 0xxxxxxx
 * 200 : 10000001 01001000
 */

/* The number is encoded into the buffer which must be long enough.
 * A buffer length of 10 is alway enough.
 * Return value: The length of the encoded buffer 
 */
// size_t encode_length(uint64_t number, void* buffer);

/* The buffer should contain an encoding of a number
 * Return value: 0 on succes, 1 on overflow.
 */
// int decode_length(void *buffer, size_t buffer_size, uint64_t *number);

	
#endif
