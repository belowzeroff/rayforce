/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "mem/heap.h"
#include "table/domain.h"
#include <stdlib.h>

static ray_t* block_error(ray_err_t err) {
    switch (err) {
    case RAY_ERR_RANGE: return ray_error("range", "column row range is out of bounds");
    case RAY_ERR_LIMIT: return ray_error("limit", "column materialization exceeds buffer budget");
    case RAY_ERR_TYPE: return ray_error("type", "unsupported block column type");
    case RAY_ERR_DOMAIN: return ray_error("domain", "invalid block column argument");
    case RAY_ERR_IO: return ray_error("io", "cannot map block column file");
    case RAY_ERR_VERSION: return ray_error("version", "expected major-2 block column");
    case RAY_ERR_OOM: return ray_error("oom", "cannot allocate block column workspace");
    case RAY_ERR_NYI: return ray_error("nyi", "block columns require a little-endian host");
    default: return ray_error("corrupt", "invalid column block payload");
    }
}

ray_t* ray_col_block_materialize_dom(const ray_col_block_reader_t* r,
                                uint64_t start, uint64_t count,
                                size_t payload_limit, void* scratch,
                                size_t scratch_capacity, ray_sym_domain_t* domain) {
    if (!r) return block_error(RAY_ERR_DOMAIN);
    if (r->type == RAY_SYM && (!domain ||
        domain == ray_sym_runtime_domain())) return block_error(RAY_ERR_DOMAIN);
    if (r->type == RAY_SYM &&
        (uint64_t)ray_sym_domain_count(domain) != r->sym_count) return block_error(RAY_ERR_CORRUPT);
    size_t bytes, needed;
    ray_err_t err = ray_col_block_range_size(r, start, count, &bytes, &needed);
    if (err) return block_error(err);
    if (bytes > payload_limit || needed > scratch_capacity) return block_error(RAY_ERR_LIMIT);
    if (needed && !scratch) return block_error(RAY_ERR_DOMAIN);
    if (r->type == RAY_STR) {
        err = ray_col_block_payload_size(r, start, count, scratch, scratch_capacity, &bytes);
        if (err) return block_error(err);
        if (bytes > payload_limit) return block_error(RAY_ERR_LIMIT);
        return ray_col_block_materialize_str(r, start, count, bytes, scratch, scratch_capacity);
    }
    ray_t* result = r->type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, (int64_t)count) :
        ray_vec_new((int8_t)r->type, (int64_t)count);
    if (!result) return block_error(RAY_ERR_OOM);
    if (RAY_IS_ERR(result)) return result;
    err = ray_col_block_read_range(r, start, count, ray_data(result), bytes, scratch, scratch_capacity);
    if (err) { ray_release(result); return block_error(err); }
    result->len = (int64_t)count;
    if (r->type == RAY_SYM) {
        const uint64_t* positions = ray_data(result);
        for (uint64_t i = 0; i < count; i++) {
            if (positions[i] >= r->sym_count) {
                ray_release(result); return block_error(RAY_ERR_CORRUPT);
            }
        }
        result->sym_domain = domain;
        ray_sym_domain_retain(domain);
    }
    /* A conservative null hint is safe; omitting it hides integer/GUID nulls.
     * BOOL/U8 cannot carry nulls. No persisted indexes or sorted hints exist. */
    if (count && r->type != RAY_BOOL && r->type != RAY_U8)
        result->attrs |= RAY_ATTR_HAS_NULLS;
    return result;
}

ray_t* ray_col_block_materialize(const ray_col_block_reader_t* r,
                                uint64_t start, uint64_t count,
                                size_t payload_limit, void* scratch,
                                size_t scratch_capacity) {
    return ray_col_block_materialize_dom(r, start, count, payload_limit,
                                         scratch, scratch_capacity, NULL);
}

ray_t* ray_col_block_load_range(const char* path, uint64_t start, uint64_t count,
                               size_t payload_limit, size_t scratch_limit) {
    ray_col_block_file_t* file = NULL;
    ray_err_t err = ray_col_block_file_open(path, &file);
    if (err) return block_error(err);
    const ray_col_block_reader_t* r = ray_col_block_file_reader(file);
    size_t bytes, needed;
    err = ray_col_block_range_size(r, start, count, &bytes, &needed);
    if (!err && (bytes > payload_limit || needed > scratch_limit)) err = RAY_ERR_LIMIT;
    void* scratch = NULL;
    if (!err && needed) {
        scratch = malloc(needed);
        if (!scratch) err = RAY_ERR_OOM;
    }
    ray_t* result = err ? block_error(err) :
        ray_col_block_materialize(r, start, count, payload_limit, scratch, needed);
    free(scratch);
    ray_col_block_file_close(&file);
    return result;
}
