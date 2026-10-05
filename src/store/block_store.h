/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_STORE_H
#define RAY_BLOCK_STORE_H
#include <rayforce.h>

typedef struct {
    uint32_t block_bytes;
    uint8_t codec; /* 0 raw blocks, 1 Snappy with raw fallback */
    bool durable;
} ray_block_store_options_t;

/* Explicit whole-table major-2 save; existing save APIs remain raw.
 * Accepts rectangular, nonempty-schema tables with BOOL..STR columns only.
 * STR uses independent lengths/bytes blocks; a string longer than block_bytes
 * minus its 4-byte length prefix returns range before filesystem mutation.
 * SYM is re-encoded as uint64 positions over an immutable generation-local
 * .sym dictionary (STRL), capped at 64 MiB on disk. Vocabulary memory grows
 * with distinct symbols; row translation uses one block of extra scratch.
 * Rejects column attrs except HAS_NULLS and SYM width bits (views/indexes/links/sorted)
 * rather than silently dropping metadata. Input must remain unchanged during
 * the call. Options/types/names/shape are checked before filesystem mutation.
 * Always stages, including first save; .current is the publication point.
 * Pre-publication errors leave the previous table readable. As with splay,
 * a post-rename fsync failure may report IO although the new generation is
 * already visible. Failed initial saves can leave empty infrastructure dirs.
 * Requires runtime heap/symbol table. Caller owns the input table. */
ray_err_t ray_block_store_save(ray_t* table, const char* root,
                               const ray_block_store_options_t* options);
#endif
