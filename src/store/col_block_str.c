/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "mem/heap.h"
#include "vec/str.h"
#include <stdlib.h>
#include <string.h>

_Static_assert(sizeof(ray_str_t) == 16, "block STR sizing assumes 16-byte descriptors");

static uint32_t string_length(const uint8_t* p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

ray_err_t ray_col_block_append_str_vec(ray_col_block_writer_t* w, ray_t* strings) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    if (w->error) return w->error;
    if (w->type != RAY_STR || !strings || RAY_IS_ERR(strings) || strings->type != RAY_STR)
        return w->error = RAY_ERR_TYPE;
    if (strings->len < 0 || (uint64_t)strings->len > (uint64_t)INT64_MAX - w->rows)
        return w->error = RAY_ERR_RANGE;
    if (!strings->len) return RAY_OK;
    uint8_t* block = malloc(w->block_bytes);
    if (!block) return w->error = RAY_ERR_OOM;
    size_t used = 0;
    uint64_t rows = 0;
    for (int64_t i = 0; !w->error && i < strings->len; i++) {
        size_t len;
        const char* s = ray_str_vec_get(strings, i, &len);
        if (!s) { w->error = RAY_ERR_CORRUPT; break; }
        if (len > w->block_bytes - 4) { w->error = RAY_ERR_RANGE; break; }
        if (len + 4 > w->block_bytes - used) {
            if (ray_col_block_append_str(w, block, used, rows)) break;
            used = 0; rows = 0;
        }
        for (unsigned j = 0; j < 4; j++) block[used + j] = (uint8_t)(len >> (j * 8));
        memcpy(block + used + 4, s, len);
        used += len + 4; rows++;
    }
    if (!w->error && rows) (void)ray_col_block_append_str(w, block, used, rows);
    free(block);
    return w->error;
}

/* First pass validates selected blocks and counts only the selected strings'
 * native pool bytes. Second pass fills an exactly sized owned vector using
 * the same single scratch buffer, with no retained block pointers. */
static uint64_t entry_u64(const ray_col_block_reader_t* r, uint64_t block, unsigned at, unsigned n) {
    const uint8_t* e = r->data + r->directory + block * RAY_COL_BLOCK_ENTRY + at;
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++) v |= (uint64_t)e[i] << (8 * i);
    return v;
}

/* With arena, blocks were already decoded and validated there in order by
 * ray_col_block_decode_arena; otherwise each is decoded into scratch. */
static ray_err_t walk_strings(const ray_col_block_reader_t* r, uint64_t start,
                               uint64_t count, void* scratch, size_t capacity,
                               const uint8_t* arena, ray_t* output, size_t* pool_bytes) {
    uint64_t first, end;
    ray_err_t err = ray_col_block_range_blocks(r, start, count, &first, &end);
    if (err) return err;
    size_t used = 0, at = 0;
    for (uint64_t b = first; b < end; b++) {
        uint64_t row, rows;
        const uint8_t* p;
        if (arena) {
            row = entry_u64(r, b, 0, 8);
            rows = entry_u64(r, b, 8, 8);
            p = arena + at;
            at += (size_t)entry_u64(r, b, 28, 4);
        } else {
            err = ray_col_block_read(r, b, scratch, capacity, &row, &rows);
            if (err) return err;
            p = scratch;
        }
        for (uint64_t i = 0; i < rows; i++) {
            uint32_t len = string_length(p);
            p += 4;
            if (row + i >= start && row + i < start + count) {
                bool pooled = len > RAY_STR_INLINE_MAX;
                if (pooled && len > UINT32_MAX - used) return RAY_ERR_RANGE;
                if (output) {
                    ray_str_t* d = &((ray_str_t*)ray_data(output))[row + i - start];
                    memset(d, 0, sizeof(*d));
                    d->len = len;
                    if (pooled) {
                        if (!output->str_pool || used > (size_t)output->str_pool->len ||
                            len > (size_t)output->str_pool->len - used) return RAY_ERR_CORRUPT;
                        char* pool = ray_data(output->str_pool);
                        memcpy(pool + used, p, len);
                        d->pool_off = (uint32_t)used;
                        memcpy(d->prefix, p, 4);
                        ray_str_t_cache_hash(d, pool);
                    } else if (len) memcpy(d->data, p, len);
                    else output->attrs |= RAY_ATTR_HAS_NULLS;
                }
                if (pooled) used += len;
            }
            p += len;
        }
    }
    *pool_bytes = used;
    return RAY_OK;
}

ray_err_t ray_col_block_payload_size(const ray_col_block_reader_t* r,
                                    uint64_t start, uint64_t count,
                                    void* scratch, size_t capacity, size_t* bytes) {
    if (!bytes) return RAY_ERR_DOMAIN;
    size_t base, needed;
    ray_err_t err = ray_col_block_range_size(r, start, count, &base, &needed);
    if (err) return err;
    if (r->type != RAY_STR) { *bytes = base; return RAY_OK; }
    if (capacity < needed) return RAY_ERR_LIMIT;
    if (needed && !scratch) return RAY_ERR_DOMAIN;
    size_t pool;
    err = walk_strings(r, start, count, scratch, capacity, NULL, NULL, &pool);
    if (err) return err;
    if (pool > SIZE_MAX - base) return RAY_ERR_LIMIT;
    *bytes = base + pool;
    return RAY_OK;
}

ray_err_t ray_col_block_payload_size_arena(const ray_col_block_reader_t* r,
                                          uint64_t start, uint64_t count,
                                          const uint8_t* arena, size_t* bytes) {
    if (!r || !bytes) return RAY_ERR_DOMAIN;
    if (r->type != RAY_STR) return RAY_ERR_TYPE;
    size_t base, needed, pool;
    ray_err_t err = ray_col_block_range_size(r, start, count, &base, &needed);
    if (err) return err;
    if (count && !arena) return RAY_ERR_DOMAIN;
    err = walk_strings(r, start, count, NULL, 0, arena, NULL, &pool);
    if (err) return err;
    if (pool > SIZE_MAX - base) return RAY_ERR_LIMIT;
    *bytes = base + pool;
    return RAY_OK;
}

static ray_t* materialize_str(const ray_col_block_reader_t* r, uint64_t start, uint64_t count,
                              size_t measured_bytes, void* scratch, size_t capacity,
                              const uint8_t* arena);

ray_t* ray_col_block_materialize_str_arena(const ray_col_block_reader_t* r,
                                          uint64_t start, uint64_t count,
                                          size_t measured_bytes, const uint8_t* arena) {
    if (count && !arena) return ray_error("domain", "STR arena required");
    return materialize_str(r, start, count, measured_bytes, NULL, 0, arena);
}

ray_t* ray_col_block_materialize_str(const ray_col_block_reader_t* r,
                                    uint64_t start, uint64_t count,
                                    size_t measured_bytes, void* scratch, size_t capacity) {
    return materialize_str(r, start, count, measured_bytes, scratch, capacity, NULL);
}

static ray_t* materialize_str(const ray_col_block_reader_t* r, uint64_t start, uint64_t count,
                              size_t measured_bytes, void* scratch, size_t capacity,
                              const uint8_t* arena) {
    if (!r || r->type != RAY_STR) return ray_error("type", "expected STR block column");
    size_t base, needed;
    ray_err_t err = ray_col_block_range_size(r, start, count, &base, &needed);
    if (!err && measured_bytes < base) err = RAY_ERR_LIMIT;
    if (!err && !arena && capacity < needed) err = RAY_ERR_LIMIT;
    if (!err && !arena && needed && !scratch) err = RAY_ERR_DOMAIN;
    if (err) return ray_error(ray_err_code_str(err), "invalid STR materialization range");
    size_t pool = measured_bytes - base;
    if (pool > UINT32_MAX || pool > SIZE_MAX - sizeof(ray_t))
        return ray_error("range", "STR pool exceeds native offset limit");
    ray_t* v = ray_vec_new(RAY_STR, (int64_t)count);
    if (!v) return ray_error("oom", "cannot allocate STR output");
    if (RAY_IS_ERR(v)) return v;
    if (pool) {
        ray_t* p = ray_vec_new(RAY_U8, (int64_t)pool);
        if (!p || RAY_IS_ERR(p)) {
            ray_release(v);
            return p ? p : ray_error("oom", "cannot allocate STR pool");
        }
        p->len = (int64_t)pool;
        v->str_pool = p;
    }
    size_t used;
    err = walk_strings(r, start, count, scratch, capacity, arena, v, &used);
    if (!err && used != pool) err = RAY_ERR_CORRUPT;
    if (err) { ray_release(v); return ray_error(ray_err_code_str(err), "invalid STR block payload"); }
    v->len = (int64_t)count;
    return v;
}
