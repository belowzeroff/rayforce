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
 * Conjunctive predicates use compressed integer/temporal zone metadata to
 * skip provably nonmatching batches; raw, legacy, and unsupported data remain
 * conservative and are evaluated normally.
 * Output batch sizes may vary. Every returned table owns its data.
 * payload/scratch options bound each scan read independently. A nonzero
 * work_limit additionally combines decoded payload, scratch, logical predicate
 * masks, and bytes reserved by the caller. Metadata, vocabulary, allocator
 * capacity, and graph node objects remain outside these logical-byte caps.
 * Names/options/literals may be discarded or mutated after open. *out must be
 * NULL. All calls require caller serialization and initialized heap/symbols. */
ray_err_t ray_block_query_open(const char* root, const char* const* columns,
                             size_t count, const ray_block_predicate_t* predicates,
                             size_t n_predicates, const ray_block_scan_options_t* options,
                             ray_block_query_t** out);
/* As open, reading root as described by source (NULL: generation-local). */
ray_err_t ray_block_query_open_source(const char* root, const ray_block_scan_source_t* source,
                                      const char* const* columns, size_t count,
                                      const ray_block_predicate_t* predicates,
                                      size_t n_predicates,
                                      const ray_block_scan_options_t* options,
                                      ray_block_query_t** out);
/* Partitioned variant over <root>/<partition>/<table>. Partitions are listed
 * once at open, in the key order of .db.parted.get, and read one at a time:
 * each holds its own generation lease (or, with immutable, is a flat
 * directory) only while it is read, so there is no snapshot across
 * partitions. The virtual key column is "date" for YYYY.MM.DD partitions,
 * else "part" (I64 or SYM); it may be projected and compared. Predicates on
 * the key prune whole partitions before any of their files are opened. The
 * first partition, pruned or not, is opened at open: like .db.parted.get it
 * defines the name schema, and later partitions must have a prefix of it with
 * every selected column of the same type. A projection needs one stored
 * column. SYM columns are re-interned into the runtime symbol table, since
 * each compressed generation has its own dictionary; raw SYM columns use
 * <root>/.sym. Limits apply per partition read as in open; the key and SYM
 * copies are charged to work_limit per batch row. */
ray_err_t ray_block_query_open_parted(const char* root, const char* table, bool immutable,
                                      const char* const* columns, size_t count,
                                      const ray_block_predicate_t* predicates,
                                      size_t n_predicates,
                                      const ray_block_scan_options_t* options,
                                      ray_block_query_t** out);
/* Whether the source holds no rows at all, before filtering. A parted query
 * may open the metadata of partitions it has not read, one at a time. */
ray_err_t ray_block_query_source_empty(ray_block_query_t* query, bool* empty);
/* Owned zero-row output schema, including for an empty/no-match query. */
ray_t* ray_block_query_schema(const ray_block_query_t* query);
bool ray_block_query_has_column(const ray_block_query_t* query, const char* name);
/* Rows before filtering; for a parted query, of the partitions read so far. */
uint64_t ray_block_query_rows(const ray_block_query_t* query);
/* Logical bytes retained by the caller while query_next produces a batch.
 * Combined with decoded payload, scratch, and predicate masks by work_limit. */
ray_err_t ray_block_query_reserve(ray_block_query_t* query, size_t bytes);
/* Owned table/error, NULL at EOF. Errors and cancellation are terminal. Cancel
 * and runtime interrupts are checked between calls and filter batches, not
 * during synchronous decode.
 * Repeated small matching ranges may re-decode boundary blocks. */
ray_t* ray_block_query_next(ray_block_query_t* query);
/* Scan work so far, plus filter batches skipped by zone maps and, for a
 * parted query, partitions opened for reading and pruned by key predicates.
 * The first partition's schema open is not a read unless it is read. */
typedef struct {
    ray_block_scan_stats_t scan;
    uint64_t batches_skipped;
    uint64_t partitions_read, partitions_pruned;
} ray_block_query_stats_t;
void ray_block_query_stats(const ray_block_query_t* query, ray_block_query_stats_t* out);
void ray_block_query_cancel(ray_block_query_t* query);
void ray_block_query_close(ray_block_query_t** query);
#endif
