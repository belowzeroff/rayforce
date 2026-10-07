/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_SCAN_H
#define RAY_BLOCK_SCAN_H
#include <rayforce.h>

#define RAY_BLOCK_SCAN_MAX_COLUMNS 1024u
#define RAY_BLOCK_SCAN_SCHEMA_MAX_BYTES (1024u * 1024u)

typedef struct ray_block_scan_s ray_block_scan_t;
typedef struct {
    uint64_t start, count; /* count == UINT64_MAX means through EOF */
    uint32_t batch_rows;  /* nonzero; a batch that exceeds a budget fails */
    size_t payload_limit, scratch_limit;
} ray_block_scan_options_t;

/* Internal major-2 named-column scan, not yet a query source.
 * Explicit projection: 1..1024 unique, safe column filenames declared by .d.
 * Validates the complete name schema but not unselected column files. Schema
 * is limited to 1 MiB on disk, 1024 entries, and 255 bytes per name. Selected
 * columns must have equal row
 * counts and generation tokens. Acquires a generation lease before mapping.
 * SYM uses the generation's .sym vocabulary, verified by count and CRC before
 * attaching its retained domain to each output vector. Selected SYM columns
 * must reference the same dictionary; numeric-only projections do not open it.
 * Requires initialized runtime heap/symbol table. *out must initially be NULL.
 * Names are interned; caller may discard options/name strings after open.
 * Limits cover logical payload of one batch and temporary decode scratch,
 * including STR descriptors and pooled bytes. STR decodes selected blocks
 * twice: once to validate/size the whole batch, once to fill owned output.
 * not mapping/metadata/allocator overhead, symbol vocabulary, or retained batches.
 * The dictionary file is capped at 64 MiB; this is not a vocabulary heap cap. */
ray_err_t ray_block_scan_open(const char* root, const char* const* columns,
                              size_t count, const ray_block_scan_options_t* options,
                              ray_block_scan_t** out);
/* Owned zero-row table describing the projection, including for empty scans.
 * No payload reads or cursor advancement. Allocator overhead is not budgeted. */
ray_t* ray_block_scan_schema(const ray_block_scan_t* scan);
/* Membership in the full pinned name schema, including unprojected columns.
 * The bounded decoded name schema is retained as metadata until close. */
bool ray_block_scan_has_column(const ray_block_scan_t* scan, const char* name);
/* Configured row-range cardinality, before filtering; stable across next(). */
uint64_t ray_block_scan_rows(const ray_block_scan_t* scan);
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
void ray_block_scan_cancel(ray_block_scan_t* scan);
void ray_block_scan_close(ray_block_scan_t** scan);
#endif
