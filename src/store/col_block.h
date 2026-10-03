/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_COL_BLOCK_H
#define RAY_COL_BLOCK_H

#include <rayforce.h>
#include <stdio.h>

#define RAY_COL_BLOCK_MAX (64u * 1024u * 1024u)
#define RAY_COL_BLOCK_HEADER 128u
#define RAY_COL_BLOCK_ENTRY 64u
#define RAY_COL_BLOCK_FOOTER 32u

/* Experimental major-2 fixed-width container. No attrs, SYM/STR or indexes.
 * Payload is little-endian; callers supply native values on little-endian
 * hosts only. The mapped bytes must remain immutable and alive until the
 * reader is discarded. Opening checks all metadata, not payload CRCs. */
typedef struct {
    const uint8_t* data;
    size_t size;
    uint64_t rows, blocks, directory, generation;
    uint32_t block_bytes;
    uint8_t type;
} ray_col_block_reader_t;

ray_err_t ray_col_block_open(ray_col_block_reader_t* reader,
                             const void* data, size_t size);
/* Decode exactly one block into caller-owned output. No allocations.
 * Output must not overlap the mapped bytes. On error output may be partially
 * written; row outputs change only on success. Reader must come from open. */
ray_err_t ray_col_block_read(const ray_col_block_reader_t* reader, uint64_t block,
                             void* output, size_t capacity,
                             uint64_t* row_start, uint64_t* row_count);

/* Range [start, start + count). Empty ranges at EOF are valid. Reports exact
 * output bytes and scratch required for partial boundary blocks. No allocation
 * or payload access. All APIs require an unchanged reader returned by open. */
ray_err_t ray_col_block_range_size(const ray_col_block_reader_t* reader,
                                  uint64_t start, uint64_t count,
                                  size_t* output_bytes, size_t* scratch_bytes);
/* Buffers must not overlap each other or the mapping. Capacities are checked
 * before any output write. CRC/decode errors may leave partial output. */
ray_err_t ray_col_block_read_range(const ray_col_block_reader_t* reader,
                                  uint64_t start, uint64_t count,
                                  void* output, size_t capacity,
                                  void* scratch, size_t scratch_capacity);
/* Explicit native-vector adapter; requires initialized runtime heap. Limit
 * applies to logical output payload, NOT allocator rounding/header or RSS.
 * Scratch is caller-owned and separately bounded. No implicit full-column
 * materialization, mapping ownership, or change to existing load/mmap APIs. */
ray_t* ray_col_block_materialize(const ray_col_block_reader_t* reader,
                                uint64_t start, uint64_t count,
                                size_t payload_limit, void* scratch,
                                size_t scratch_capacity);

typedef struct {
    FILE *output, *directory;
    void* scratch;
    uint64_t rows, blocks, offset, generation;
    uint32_t block_bytes;
    uint8_t type, codec;
    ray_err_t error;
} ray_col_block_writer_t;

/* output must be a new, empty, seekable binary file. Caller owns output and
 * publication (fsync/rename); this API never publishes files. Do not access
 * output independently while a writer is active. codec: 0 raw,
 * 1 Snappy with raw fallback. Memory is bounded by Snappy bound(block_bytes)
 * plus workspace, independent of row count; directory is spooled to tmpfile.
 * Each append ends its last block; partial blocks are not buffered across calls.
 * Begin only on an inactive writer. Finish always releases writer resources;
 * abort is also valid after failed begin/finish. Failed output must be discarded. */
ray_err_t ray_col_block_begin(ray_col_block_writer_t* writer, FILE* output,
                              uint8_t type, uint32_t block_bytes,
                              uint8_t codec, uint64_t generation);
ray_err_t ray_col_block_append(ray_col_block_writer_t* writer,
                               const void* values, uint64_t rows);
ray_err_t ray_col_block_finish(ray_col_block_writer_t* writer);
void ray_col_block_abort(ray_col_block_writer_t* writer);

#endif
