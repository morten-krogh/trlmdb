#include <stdio.h>

#include "lmdb.h"
#include "trlmdb.c"

int main(int argc, char **argv)
{
	if (argc != 2) {
		fprintf(stderr, "Usage: %s conf-file\n", argv[0]);
		return 1;
	}

	char err[100];
	struct conf_info *conf_info = parse_conf_file(argv[1], err);

	if (strcmp(err, "") != 0) {
		fprintf(stderr, "%s\n", err);
		return 1;
	}

	replicator(conf_info);
}
