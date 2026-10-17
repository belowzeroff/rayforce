/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_SYM_H
#define RAY_BLOCK_SYM_H
#include <rayforce.h>

/* Generation-local, immutable STRL vocabulary. This disk-size cap does not
 * bound the domain's atom/index allocations or total runtime memory. */
#define RAY_BLOCK_SYM_MAX_BYTES (64u * 1024u * 1024u)
ray_err_t ray_block_sym_checksum(const char* path, uint64_t* count, uint32_t* crc);
#endif
