/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_STORE_H
#define RAY_BLOCK_STORE_H
#include <rayforce.h>

typedef struct ray_block_store_options_s {
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
/* Streaming variant of the same format and publication. begin stages a new
 * generation of root for the columns, names and types of schema_table (its
 * rows are ignored); append writes a chunk with exactly that schema, in any
 * number of calls; finish seals the generation dictionary and publishes.
 * Output does not depend on how rows are chunked: each column carries one
 * partial block between appends. Memory per column is one block buffer plus
 * the codec workspace; SYM vocabulary grows with distinct symbols, capped as
 * for save. Validation is per chunk, after staging: a failed append makes the
 * writer terminal, and abort (or a failed finish) discards the generation and
 * leaves the previous one published. One thread at a time. */
/* {block-bytes: I64 codec: none|snappy [durable: BOOL]}, durable by default. */
ray_err_t ray_block_store_parse_options(ray_t* value, ray_block_store_options_t* out);
typedef struct ray_block_table_writer_s ray_block_table_writer_t;
ray_err_t ray_block_table_begin(const char* root, ray_t* schema_table,
                                const ray_block_store_options_t* options,
                                ray_block_table_writer_t** out);
ray_err_t ray_block_table_append(ray_block_table_writer_t* writer, ray_t* chunk);
/* Always releases the writer and clears *writer. */
ray_err_t ray_block_table_finish(ray_block_table_writer_t** writer);
void ray_block_table_abort(ray_block_table_writer_t** writer);
#endif
