/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_BLOCK_SOURCE_H
#define RAY_BLOCK_SOURCE_H
#include <rayforce.h>
/* Experimental (.db.block.scan path result-bytes [options]) data-only descriptor.
 * Opening occurs at select time, not construction. Result limit covers logical
 * vector bytes and STR pools, not total RSS, vocabulary, or allocator capacity.
 * Results grow in singly-owned column buffers; the table is built once at EOF.
 * Options: {batch-rows: I64 payload-bytes: I64 scratch-bytes: I64}.
 * Each key is optional. Defaults: 4096 rows, 8 MiB payload, 8 MiB scratch.
 * batch-rows is in [1, UINT32_MAX]; byte limits are nonnegative size_t values.
 * Limits apply per read and do not include graph masks/states/metadata or
 * caller-retained results. They are independent, not a shared query RAM cap.
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
/* NULL means not our descriptor; otherwise owned final result/error. */
ray_t* ray_block_select_source(ray_t* source, ray_t* query);
#endif
