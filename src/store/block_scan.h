/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_SCAN_H
#define RAY_BLOCK_SCAN_H
#include <rayforce.h>
#include "col_block.h"

#define RAY_BLOCK_SCAN_MAX_COLUMNS 1024u
#define RAY_BLOCK_SCAN_SCHEMA_MAX_BYTES (1024u * 1024u)

typedef struct ray_block_scan_s ray_block_scan_t;
typedef struct {
    uint64_t start, count; /* count == UINT64_MAX means through EOF */
    uint32_t batch_rows;  /* nonzero; a batch that exceeds a budget fails */
    size_t payload_limit, scratch_limit;
    size_t work_limit; /* 0 disables the combined logical-work cap */
} ray_block_scan_options_t;
/* Work done so far. A decode pass counts every block it touches, so STR
 * batches, which are sized by one pass and filled by another, count twice. */
typedef struct {
    uint64_t blocks_decoded; /* compressed blocks, summed over columns and passes */
    uint64_t raw_rows;       /* rows copied from raw columns, summed over columns */
} ray_block_scan_stats_t;
/* Where a root's data lives, beyond its own generation. */
typedef struct {
    const char* sym_path; /* symfile for raw SYM columns, e.g. a parted root's */
    bool immutable;       /* admit a flat root without generations or a reader lease */
} ray_block_scan_source_t;
typedef enum {
    RAY_BLOCK_SCAN_EQ, RAY_BLOCK_SCAN_NE, RAY_BLOCK_SCAN_LT,
    RAY_BLOCK_SCAN_LE, RAY_BLOCK_SCAN_GT, RAY_BLOCK_SCAN_GE
} ray_block_scan_cmp_t;

/* Internal named-column scan over mixed major-0 raw and major-2 block files.
 * Explicit projection: 1..1024 unique, safe column filenames declared by .d.
 * Validates the complete name schema but not unselected column files. Schema
 * is limited to 1 MiB on disk, 1024 entries, and 255 bytes per name. Selected
 * columns must have equal row counts; compressed columns must also have equal
 * generation tokens. Acquires a generation lease before mapping. Raw columns
 * remain mmap-backed inside the scan, but returned batches copy selected rows
 * and do not retain the mappings.
 * SYM uses the generation's .sym vocabulary, verified by count and CRC before
 * attaching its retained domain to each output vector. Selected SYM columns
 * must reference the same dictionary; numeric-only projections do not open it.
 * open_source: a nonnull sym_path instead supplies the vocabulary of raw SYM columns, as a
 * partition's table shares its parted root's .sym; compressed SYM columns keep
 * their generation dictionary. A root without a published generation is
 * nyi unless immutable is set: then the flat directory is read without a
 * lease, and the caller vouches that no writer replaces or prunes its files
 * while the scan is open. source and sym_path are not retained after open.
 * Requires initialized runtime heap/symbol table. *out must initially be NULL.
 * Names are interned; caller may discard options/name strings after open.
 * Limits cover logical payload of one batch and temporary decode scratch,
 * including STR descriptors and pooled bytes. A nonzero work_limit covers
 * payload + scratch + caller-reserved live bytes. STR decodes selected blocks
 * twice: once to validate/size the whole batch, once to fill owned output.
 * Limits exclude mappings, metadata, allocator capacity, symbol vocabulary,
 * and retained batches unless the caller includes them through reserve().
 * The dictionary file is capped at 64 MiB; this is not a vocabulary heap cap. */
ray_err_t ray_block_scan_open(const char* root, const char* const* columns,
                              size_t count, const ray_block_scan_options_t* options,
                              ray_block_scan_t** out);
/* As open, with an optional source description (NULL: generation-local).
 * columns NULL with count 0 selects every column of the schema pinned by this
 * open, in schema order; ray_block_scan_schema then names them. */
ray_err_t ray_block_scan_open_source(const char* root, const ray_block_scan_source_t* source,
                                     const char* const* columns, size_t count,
                                     const ray_block_scan_options_t* options,
                                     ray_block_scan_t** out);
/* Owned zero-row table describing the projection, including for empty scans.
 * No payload reads or cursor advancement. Allocator overhead is not budgeted. */
ray_t* ray_block_scan_schema(const ray_block_scan_t* scan);
/* Membership in the full pinned name schema, including unprojected columns.
 * The bounded decoded name schema is retained as metadata until close. */
bool ray_block_scan_has_column(const ray_block_scan_t* scan, const char* name);
/* Borrowed full name schema (STR vector) of the pinned generation, valid
 * until close; retain it to keep it longer. */
ray_t* ray_block_scan_names(const ray_block_scan_t* scan);
/* Attributes persisted with a selected raw column (SORTED, HAS_NULLS); a
 * compressed column persists none. Batches never carry SORTED. */
uint8_t ray_block_scan_column_attrs(const ray_block_scan_t* scan, size_t column);
/* Configured row-range cardinality, before filtering; stable across next(). */
uint64_t ray_block_scan_rows(const ray_block_scan_t* scan);
/* Inspect the next batch range without reading payload or moving the cursor. */
ray_err_t ray_block_scan_next_range(const ray_block_scan_t* scan,
                                    uint64_t* start, uint64_t* rows);
/* Conservative metadata probe for one projected column and row range. Raw,
 * legacy, and unsupported columns report true. No payload bytes are read. */
ray_err_t ray_block_scan_range_may_match(const ray_block_scan_t* scan,
                                        size_t column, uint64_t start, uint64_t rows,
                                        ray_block_scan_cmp_t op, int64_t value,
                                        bool value_is_null, bool* may_match);
/* Advance over exactly the next batch without reading payload. */
ray_err_t ray_block_scan_skip_next(ray_block_scan_t* scan, uint64_t* row_start);
/* Owned table, NULL at EOF, or owned runtime error. Batches outlive scan.
 * On any error scan is terminal; close it rather than retrying. No row cursor
 * advancement until a complete batch succeeds. Cancel is checked at next(),
 * not mid-decode. All calls, including cancel/close, require caller serialization. */
ray_t* ray_block_scan_next(ray_block_scan_t* scan);
/* Subset indices refer to the original open projection, in requested order.
 * NULL indices selects its first count columns. count must be nonzero; indices
 * must be distinct and in range. The subset alone consumes the payload/scratch
 * budgets; other mapped columns' payloads are not read or checked.
 * Advances the shared row cursor exactly as next(); optional row_start receives
 * the absolute first row only on success (unchanged on EOF or error).
 * No ownership of indices or row_start is retained. */
ray_t* ray_block_scan_next_columns(ray_block_scan_t* scan, const size_t* indices,
                                   size_t count, uint64_t* row_start);
/* Read from the same pinned generation without moving the shared row cursor,
 * including after EOF. Range must lie within the configured start/count, with
 * rows <= batch_rows. Zero rows returns an owned typed empty table without
 * payload reads. Same subset rules, budgets, cancellation and terminal errors
 * as next_columns(). Used for late materialization after a filter-only batch;
 * repeated small ranges may decode the same boundary block again. */
ray_t* ray_block_scan_read_columns(ray_block_scan_t* scan, const size_t* indices,
                                   size_t count, uint64_t start, uint64_t rows);
/* Set logical bytes retained by the caller while the next batch is produced.
 * The reservation persists until replaced and is only charged by work_limit. */
ray_err_t ray_block_scan_reserve(ray_block_scan_t* scan, size_t bytes);
void ray_block_scan_stats(const ray_block_scan_t* scan, ray_block_scan_stats_t* out);
/* Storage of one selected column of the pinned generation, from metadata. */
typedef struct {
    bool compressed;
    int8_t type;
    uint64_t rows;
    uint64_t file_bytes;           /* the column file on disk */
    uint32_t block_bytes;          /* compressed only */
    ray_col_block_summary_t blocks; /* compressed only, else zero */
} ray_block_scan_column_info_t;
ray_err_t ray_block_scan_column_info(const ray_block_scan_t* scan, size_t column,
                                     ray_block_scan_column_info_t* out);
/* Decode and check every block of a compressed column (ray_col_block_verify).
 * Raw columns carry no checksums: RAY_OK with *checked = 0. */
ray_err_t ray_block_scan_verify_column(const ray_block_scan_t* scan, size_t column,
                                       uint64_t* checked, uint64_t* bad_block);
void ray_block_scan_cancel(ray_block_scan_t* scan);
void ray_block_scan_close(ray_block_scan_t** scan);
#endif
