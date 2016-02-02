#include <stdlib.h>

int encode_length(uint64_t number, void* buffer)
{
	int has_started = 0;
	int index = 0;

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
	for (int i = 0; i < buffer_size; i++) {
		if (i == 11) return 1;
		uint8_t byte = *(uint8_t*) (buffer + i);
		if (i < 10) {
			*number <<= 7;
			number += byte & 0x7f;
		} else {
			*number <<= 1;
			number += byte &01;
		}
		if (!(byte >> 7)) {
			return 0;
		}
	}
	return 1;
}
