#include <stdio.h>

struct conf_info *parse_conf_file(const char *conf_file);
void replicator(struct conf_info *conf_info);
	
int main(int argc, char **argv)
{
	if (argc != 2) {
		fprintf(stderr, "Usage: %s conf-file\n", argv[0]);
		return 1;
	}

	/* parse_conf_info logs and exits if there is a parse error */
	struct conf_info *conf_info = parse_conf_file(argv[1]);

	replicator(conf_info);
}
