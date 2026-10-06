/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_QUERY_H
#define RAY_BLOCK_QUERY_H
#include "store/block_scan.h"

#define RAY_BLOCK_QUERY_MAX_PREDICATES 32u
typedef struct ray_block_query_s ray_block_query_t;
typedef struct {
    const char* column;
    uint16_t opcode; /* OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE from ops.h */
    ray_t* value;    /* same-type integer/temporal atom; copied by open */
} ray_block_predicate_t;

/* Internal C adapter, not a Rayfall select source. Explicit output projection
 * plus 0..32 conjunctive comparisons on BOOL/U8/I16/I32/I64/DATE/TIME/TIMESTAMP.
 * Uses native graph execution, including native null semantics. No promotion,
 * arbitrary expressions, aggregates, ordering, or hidden full materialization.
 * One scan pins the union of filter and output columns. Filter-only batches
 * precede output-only contiguous matching ranges, in original row order.
 * Output batch sizes may vary. Every returned table owns its data.
 * options bounds each scan read, NOT total query RSS: graph intermediates are
 * additional, bounded by batch_rows and predicate count; metadata, vocabulary,
 * allocator overhead and caller-retained results are also outside these caps.
 * Names/options/literals may be discarded or mutated after open. *out must be
 * NULL. All calls require caller serialization and initialized heap/symbols. */
ray_err_t ray_block_query_open(const char* root, const char* const* columns,
                             size_t count, const ray_block_predicate_t* predicates,
                             size_t n_predicates, const ray_block_scan_options_t* options,
                             ray_block_query_t** out);
/* Owned zero-row output schema, including for an empty/no-match query. */
ray_t* ray_block_query_schema(const ray_block_query_t* query);
/* Owned table/error, NULL at EOF. Errors and cancellation are terminal. Cancel
 * is checked between calls and filter batches, not during synchronous decode.
 * Repeated small matching ranges may re-decode boundary blocks. */
ray_t* ray_block_query_next(ray_block_query_t* query);
void ray_block_query_cancel(ray_block_query_t* query);
void ray_block_query_close(ray_block_query_t** query);
#endif
