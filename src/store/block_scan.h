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

/* Internal fixed-width major-2 named-column scan, not yet a query source.
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
 * not mapping/metadata/allocator overhead, symbol vocabulary, or retained batches.
 * The dictionary file is capped at 64 MiB; this is not a vocabulary heap cap. */
ray_err_t ray_block_scan_open(const char* root, const char* const* columns,
                              size_t count, const ray_block_scan_options_t* options,
                              ray_block_scan_t** out);
/* Owned zero-row table describing the projection, including for empty scans.
 * No payload reads or cursor advancement. Allocator overhead is not budgeted. */
ray_t* ray_block_scan_schema(const ray_block_scan_t* scan);
/* Owned table, NULL at EOF, or owned runtime error. Batches outlive scan.
 * On any error scan is terminal; close it rather than retrying. No row cursor
 * advancement until a complete batch succeeds. Cancel is checked at next(),
 * not mid-decode. All calls, including cancel/close, require caller serialization. */
ray_t* ray_block_scan_next(ray_block_scan_t* scan);
void ray_block_scan_cancel(ray_block_scan_t* scan);
void ray_block_scan_close(ray_block_scan_t** scan);
#endif
