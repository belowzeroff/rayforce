/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_COL_BLOCK_H
#define RAY_COL_BLOCK_H

#include <rayforce.h>
#include <stdio.h>

#define RAY_COL_BLOCK_MAX (64u * 1024u * 1024u)
#define RAY_COL_BLOCK_HEADER 128u
#define RAY_COL_BLOCK_ENTRY 64u
#define RAY_COL_BLOCK_FOOTER 32u

/* Experimental major-2 block container. No semantic attrs or indexes.
 * STR blocks contain repeated [u32 byte length, bytes], with zero length
 * representing the canonical empty/null string. No runtime descriptors persist.
 * SYM payloads are uint64 positions in a generation-local dictionary, bound
 * by count and file CRC. They are never process-local symbol identifiers.
 * Payload is little-endian; callers supply native values on little-endian
 * hosts only. The mapped bytes must remain immutable and alive until the
 * reader is discarded. Opening checks all metadata, not payload CRCs. */
typedef struct {
    const uint8_t* data;
    size_t size;
    uint64_t rows, blocks, directory, generation;
    uint32_t block_bytes;
    uint8_t type;
    uint64_t sym_count;
    uint32_t sym_crc;
} ray_col_block_reader_t;

ray_err_t ray_col_block_open(ray_col_block_reader_t* reader,
                             const void* data, size_t size);
/* Decode exactly one block into caller-owned output. No allocations.
 * Output must not overlap the mapped bytes. On error output may be partially
 * written; row outputs change only on success. Reader must come from open. */
ray_err_t ray_col_block_read(const ray_col_block_reader_t* reader, uint64_t block,
                             void* output, size_t capacity,
                             uint64_t* row_start, uint64_t* row_count);

/* Range [start, start + count). Empty ranges at EOF are valid. Fixed width:
 * exact output bytes and scratch for partial boundary blocks. STR: minimum
 * descriptor bytes (16 per row), scratch for the largest selected block;
 * payload_size below adds the exact pool bytes by decoding selected blocks.
 * No allocation
 * or payload access. All APIs require an unchanged reader returned by open. */
ray_err_t ray_col_block_range_size(const ray_col_block_reader_t* reader,
                                  uint64_t start, uint64_t count,
                                  size_t* output_bytes, size_t* scratch_bytes);
/* Half-open block interval covering a row range; empty ranges return [0,0). */
ray_err_t ray_col_block_range_blocks(const ray_col_block_reader_t* reader,
                                    uint64_t start, uint64_t count,
                                    uint64_t* first, uint64_t* end);
/* Exact native payload (STR descriptors + pooled bytes, fixed width otherwise).
 * STR validates/decodes selected blocks with caller scratch, without allocation.
 * Pool > UINT32_MAX returns range, matching the native STR representation. */
ray_err_t ray_col_block_payload_size(const ray_col_block_reader_t* reader,
                                    uint64_t start, uint64_t count,
                                    void* scratch, size_t scratch_capacity,
                                    size_t* payload_bytes);
/* Buffers must not overlap each other or the mapping. Capacities are checked
 * before any output write. CRC/decode errors may leave partial output.
 * Fixed-width only; STR uses materialization below. */
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
/* SYM requires an explicitly supplied dictionary already verified against
 * sym_crc by the caller. Decoded positions are bounds checked. Output retains
 * the domain; its vocabulary allocation is outside the batch payload budget.
 * The ordinary materialize/load_range APIs reject SYM without a domain. */
ray_t* ray_col_block_materialize_dom(const ray_col_block_reader_t* reader,
                                    uint64_t start, uint64_t count,
                                    size_t payload_limit, void* scratch,
                                    size_t scratch_capacity,
                                    struct ray_sym_domain_s* domain);
/* STR-only second pass after payload_size on the same immutable range.
 * measured_bytes includes descriptors and pool; caller checks its budget.
 * A mismatched size returns an error without publishing a partial vector. */
ray_t* ray_col_block_materialize_str(const ray_col_block_reader_t* reader,
                                    uint64_t start, uint64_t count,
                                    size_t measured_bytes, void* scratch,
                                    size_t scratch_capacity);

/* Owning file handle. Open requires *out == NULL; failures leave it NULL.
 * Close clears the caller's pointer and accepts NULL. The borrowed reader is
 * valid until close; do not copy it past the handle lifetime or mutate it.
 * Uses the platform file mapper. The inode must remain immutable: this is NOT
 * a generation lease or protection against concurrent in-place truncation.
 * Callers synchronize close with all reads. No runtime heap is needed here. */
typedef struct ray_col_block_file_s ray_col_block_file_t;
ray_err_t ray_col_block_file_open(const char* path, ray_col_block_file_t** out);
const ray_col_block_reader_t* ray_col_block_file_reader(const ray_col_block_file_t* file);
void ray_col_block_file_close(ray_col_block_file_t** file);

/* Explicit major-2-only path load, with owned output. Opens/maps once and
 * always closes before returning, including on errors. Budgets cover logical
 * output payload and scratch separately, not mappings/allocator overhead/RSS.
 * Requires initialized runtime heap. Legacy files return a version error;
 * existing raw load/mmap dispatch is deliberately unchanged. */
ray_t* ray_col_block_load_range(const char* path, uint64_t start, uint64_t count,
                               size_t payload_limit, size_t scratch_limit);

typedef struct {
    FILE *output, *directory;
    void* scratch;
    uint64_t rows, blocks, offset, generation;
    uint32_t block_bytes;
    uint8_t type, codec;
    ray_err_t error;
    uint64_t sym_count;
    uint32_t sym_crc;
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
/* SYM-only variant; count includes the reserved empty symbol at position 0.
 * Dictionary bytes must be persisted before publishing the column. */
ray_err_t ray_col_block_begin_sym(ray_col_block_writer_t* writer, FILE* output,
                                  uint32_t block_bytes, uint8_t codec,
                                  uint64_t generation, uint64_t sym_count,
                                  uint32_t sym_crc);
ray_err_t ray_col_block_append(ray_col_block_writer_t* writer,
                               const void* values, uint64_t rows);
/* Append one pre-encoded STR block. Validates lengths and exact consumption;
 * encoded_size <= block_bytes. Each string stays in a single block. */
ray_err_t ray_col_block_append_str(ray_col_block_writer_t* writer,
                                   const void* encoded, size_t encoded_size,
                                   uint64_t rows);
/* Native STR adapter. Uses one block buffer; caller retains the stable vector.
 * A single string larger than block_bytes - 4 returns range. */
ray_err_t ray_col_block_append_str_vec(ray_col_block_writer_t* writer, ray_t* strings);
ray_err_t ray_col_block_finish(ray_col_block_writer_t* writer);
void ray_col_block_abort(ray_col_block_writer_t* writer);

#endif
