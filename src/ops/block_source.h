/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_SOURCE_H
#define RAY_BLOCK_SOURCE_H
#include <rayforce.h>
#include "store/block_scan.h"
/* Experimental (.db.block.scan path result-bytes [options]) data-only descriptor.
 * Opening occurs at select time, not construction. Result limit covers logical
 * vector bytes and STR pools, not total RSS, vocabulary, or allocator capacity.
 * A leased generation may mix legacy raw and compressed block columns; scan
 * batches own copied raw ranges and remain valid after the lease is released.
 * Results grow in singly-owned column buffers; the table is built once at EOF.
 * Options: {batch-rows: I64 payload-bytes: I64 scratch-bytes: I64
 *           work-bytes: I64 immutable: BOOL}.
 * immutable: true admits a flat directory without generations or a reader
 * lease; the caller vouches that nothing rewrites it during the select.
 * Each key is optional. Defaults: rows sized to half the payload limit (at
 * most 1Mi), 64 MiB payload, 8 MiB scratch;
 * whole blocks of a read decode in parallel on the native pool.
 * work-bytes has no default and, when present, bounds logical result retained
 * by select plus the current payload, scratch, predicate masks, and aggregate
 * workspace. It must be positive.
 * batch-rows is in [1, UINT32_MAX]; byte limits are nonnegative size_t values.
 * Metadata, symbol vocabularies and allocator capacity remain outside limits.
 * Unknown/duplicate keys and invalid values fail before opening files; select
 * revalidates descriptor options. Large batches do not grow aggregate gids.
 * Select admits explicit distinct bare-column projections/aliases and typed comparisons
 * with AND; unsupported plans fail, never silently materialize the full source.
 * Alternatively, all outputs may be global count(column), integer sum/avg,
 * or integer/temporal min/max. Repeated aggregate inputs are read once per
 * batch. Count includes null rows and currently decodes its input column.
 * Aggregate result limits cover the scalar output, not the streamed input.
 * Avg plans admit at most INT32_MAX matching rows (including nulls), guarding
 * native accumulator counters. Empty physical sources preserve select's empty
 * output; a nonempty source with no matches emits one aggregate row.
 * An integer sum wrapping exactly to INT64_MIN fails with range: ordinary
 * select and registry kernels disagree on its null hint in this engine version.
 * Ordinary .db.splayed.get/set are unchanged. */
ray_t* ray_block_source_fn(ray_t** args, int64_t n);
/* Experimental (.db.block.parted root table result-bytes [options]) descriptor
 * over <root>/<partition>/<table>; see ray_block_query_open_parted. Accepts the
 * same options and select subset as .db.block.scan, plus the virtual partition
 * key column ("date" or "part") in projections, aggregates and comparisons. */
ray_t* ray_block_parted_fn(ray_t** args, int64_t n);
/* (.db.block.convert src dest block-options [read-options]): rewrite a raw or
 * compressed table as a new block generation of dest; returns the row count.
 * read-options {immutable: BOOL sym: STR} describe the source as for scans;
 * a partition path <root>/<partition>/<table> defaults sym to <root>/.sym.
 * See ray_block_store_convert: dest never overlaps src.
 * (.db.block.convert src-root 'table dest-root block-options [read-options])
 * converts every partition in key order (raw SYM defaulting to src-root's
 * .sym) and returns partition/rows; a failure names its partition and leaves
 * the partitions before it published. */
ray_t* ray_block_convert_fn(ray_t** args, int64_t n);
/* (.db.storage.info path [read-options]): column, type, format (raw|block),
 * codec (none|snappy|mixed), rows, blocks, block_bytes, stored (file bytes),
 * logical (decoded block bytes; file bytes for raw) and ratio, without
 * decoding data. (.db.storage.verify path [read-options]): decodes every block
 * of compressed columns, checking both CRCs, string framing and symbol bounds;
 * returns blocks checked per column (0 for raw columns, which have no
 * checksums) or a corrupt error naming the column and block. Both pin one
 * generation; read options as for .db.block.convert. With root 'table they
 * cover every partition <root>/<p>/<table> (raw SYM defaulting to <root>/.sym)
 * and add a leading partition column; errors also name the partition. */
ray_t* ray_storage_info_fn(ray_t** args, int64_t n);
/* Every row of the table at dir, raw or compressed (see block scans), into
 * an in-memory table whose logical size may not exceed limit bytes. */
ray_t* ray_block_materialize(const char* dir, const ray_block_scan_source_t* source, size_t limit);
ray_t* ray_storage_verify_fn(ray_t** args, int64_t n);
/* NULL means not our descriptor; otherwise owned final result/error. */
ray_t* ray_block_select_source(ray_t* source, ray_t* query);
#endif
