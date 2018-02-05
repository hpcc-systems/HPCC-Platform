/*
 * Copyright 2014 Luke Dashjr
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the standard MIT license.  See COPYING for more details.
 */

#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <gcrypt.h>

#include "libbase58.h"

static
bool my_sha256(void *digest, const void *data, size_t datasz)
{
	gcry_md_hash_buffer(GCRY_MD_SHA256, digest, data, datasz);
	return true;
}

static
void usage(const char *prog)
{
	fprintf(stderr, "Usage: %s [-c] [-d] [data]\n", prog);
	fprintf(stderr, "\t-c         Use base58check (default: raw base58)\n");
	fprintf(stderr, "\t-d <size>  Decode <size> bytes\n");
	exit(1);
}

int main(int argc, char **argv)
{
	bool b58c = false;
	size_t decode = 0;
	int opt;
	while ( (opt = getopt(argc, argv, "cd:h")) != -1)
	{
		switch (opt)
		{
			case 'c':
				b58c = true;
				b58_sha256_impl = my_sha256;
				break;
			case 'd':
			{
				int i = atoi(optarg);
				if (i < 0 || (uintmax_t)i >= SIZE_MAX)
					usage(argv[0]);
				decode = (size_t)i;
				break;
			}
			default:
				usage(argv[0]);
		}
	}
	
	size_t rt;
	union {
		uint8_t *b;
		char *s;
	} r;
	if (optind >= argc)
	{
		rt = 0;
		r.b = NULL;
		while (!feof(stdin))
		{
			r.b = realloc(r.b, rt + 0x100);
			rt += fread(&r.b[rt], 1, 0x100, stdin);
		}
		if (decode)
			while (isspace(r.s[rt-1]))
				--rt;
	}
	else
	{
		r.s = argv[optind];
		rt = strlen(argv[optind]);
	}
	
	if (decode)
	{
		uint8_t bin[decode];
		size_t ssz = decode;
		if (!b58tobin(bin, &ssz, r.s, rt))
			return 2;
		if (b58c)
		{
			int chk = b58check(bin, decode, r.s, rt);
			if (chk < 0)
				return chk;
			if (fwrite(bin, decode, 1, stdout) != 1)
				return 3;
		}
		else
		{
			// Raw base58 doesn't check length match
			uint8_t cbin[ssz];
			if (ssz > decode)
			{
				size_t zeros = ssz - decode;
				memset(cbin, 0, zeros);
				memcpy(&cbin[zeros], bin, decode);
			}
			else
				memcpy(cbin, &bin[decode - ssz], ssz);
			
			if (fwrite(cbin, ssz, 1, stdout) != 1)
				return 3;
		}
	}
	else
	{
		size_t ssz = rt * 2;
		char s[ssz];
		bool rv;
		if (b58c)
			rv = rt && b58check_enc(s, &ssz, r.b[0], &r.b[1], rt-1);
		else
			rv = b58enc(s, &ssz, r.b, rt);
		if (!rv)
			return 2;
		puts(s);
	}
}
