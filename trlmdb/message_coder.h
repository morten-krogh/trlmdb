#ifndef MESSAGE_CODER_H
#define MESSAGE_CODER_H

/* The encoding of an unsigned integer is a series of bytes where the most significant bit
 * is one except for the last byte where it is zero. The value of an encoded series of bytes is
 * given by concatenating the 7 least significant bits of all bytes and intepreting them as a binary number
 * Examples: 
 *
 * 0 <= x <= 127: 0xxxxxxx
 * 200 : 10000001 01001000
 */

/* The number is encoded into the buffer which must be long enough.
 * Return value: The length of the encoded buffer 
 */
int encode_length(uint64_t number, void* buffer);

/* The buffer should contain an encoding of a number
 * Return value: 0 on succes, 1 on overflow.
 */
int decode_length(void *buffer, size_t buffer_size; uint64_t *number);

	
#endif
