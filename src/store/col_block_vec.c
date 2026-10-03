/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "mem/heap.h"

static ray_t* block_error(ray_err_t err) {
    switch (err) {
    case RAY_ERR_RANGE: return ray_error("range", "column row range is out of bounds");
    case RAY_ERR_LIMIT: return ray_error("limit", "column materialization exceeds buffer budget");
    case RAY_ERR_TYPE: return ray_error("type", "unsupported block column type");
    case RAY_ERR_DOMAIN: return ray_error("domain", "invalid block reader or scratch buffer");
    default: return ray_error("corrupt", "invalid column block payload");
    }
}

ray_t* ray_col_block_materialize(const ray_col_block_reader_t* r,
                                uint64_t start, uint64_t count,
                                size_t payload_limit, void* scratch,
                                size_t scratch_capacity) {
    size_t bytes, needed;
    ray_err_t err = ray_col_block_range_size(r, start, count, &bytes, &needed);
    if (err) return block_error(err);
    if (bytes > payload_limit || needed > scratch_capacity) return block_error(RAY_ERR_LIMIT);
    if (needed && !scratch) return block_error(RAY_ERR_DOMAIN);
    ray_t* result = ray_vec_new((int8_t)r->type, (int64_t)count);
    if (RAY_IS_ERR(result)) return result;
    err = ray_col_block_read_range(r, start, count, ray_data(result), bytes, scratch, scratch_capacity);
    if (err) { ray_release(result); return block_error(err); }
    result->len = (int64_t)count;
    /* A conservative null hint is safe; omitting it hides integer/GUID nulls.
     * BOOL/U8 cannot carry nulls. No persisted indexes or sorted hints exist. */
    if (count && r->type != RAY_BOOL && r->type != RAY_U8)
        result->attrs |= RAY_ATTR_HAS_NULLS;
    return result;
}
