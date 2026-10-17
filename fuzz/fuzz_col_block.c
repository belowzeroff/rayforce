/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "store/col_block.h"
#include "core/crc32.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static uint64_t get(const uint8_t* p) {
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
/* Row predicate as zone maps define it: a null is below every value. */
static bool row_matches(bool null, int64_t v, ray_col_block_cmp_t op, int64_t value) {
    if (null) return op == RAY_COL_BLOCK_NE || op == RAY_COL_BLOCK_LT || op == RAY_COL_BLOCK_LE;
    switch (op) {
    case RAY_COL_BLOCK_EQ: return v == value;
    case RAY_COL_BLOCK_NE: return v != value;
    case RAY_COL_BLOCK_LT: return v < value;
    case RAY_COL_BLOCK_LE: return v <= value;
    case RAY_COL_BLOCK_GT: return v > value;
    default: return v >= value;
    }
}
/* Decoded value i of a zone-mapped type, with its null flag. */
static bool zone_row(const uint8_t* p, uint64_t i, uint8_t type, int64_t* v) {
    switch (type) {
    case RAY_BOOL: case RAY_U8: *v = p[i]; return false;
    case RAY_I16: { int16_t x; memcpy(&x, p + i * 2, 2); *v = x; return x == INT16_MIN; }
    case RAY_I32: case RAY_DATE: case RAY_TIME: { int32_t x; memcpy(&x, p + i * 4, 4); *v = x; return x == INT32_MIN; }
    default: { int64_t x; memcpy(&x, p + i * 8, 8); *v = x; return x == INT64_MIN; }
    }
}
static void put_crc(uint8_t* p, uint32_t crc) {
    for (unsigned i = 0; i < 4; i++) { p[i] = (uint8_t)crc; crc >>= 8; }
}
static void exercise(const uint8_t* data, size_t size) {
    ray_col_block_reader_t r;
    if (ray_col_block_open(&r, data, size) != RAY_OK || r.block_bytes > 1024 * 1024)
        return;
    void* output = malloc(r.block_bytes);
    if (!output) abort();
    for (uint64_t b = 0; b < r.blocks && b < 8; b++) {
        uint64_t row, count;
        (void)ray_col_block_read(&r, b, output, r.block_bytes, &row, &count);
        (void)ray_col_block_read(&r, b, output, 0, &row, &count);
    }
    uint64_t start = r.rows ? get(data + size - 8) % r.rows : 0;
    uint64_t count = r.rows - start < 128 ? r.rows - start : 128;
    uint8_t range[128 * 16];
    size_t bytes, scratch;
    if (ray_col_block_range_size(&r, start, count, &bytes, &scratch) != RAY_OK ||
        bytes > sizeof(range) || scratch > r.block_bytes) abort();
    ray_err_t range_err = ray_col_block_read_range(&r, start, count, range, bytes, output, scratch);
    (void)ray_col_block_read_range(&r, start, UINT64_MAX, range, bytes, output, scratch);
    ray_col_block_summary_t summary;
    if (ray_col_block_summary(&r, &summary) != RAY_OK || summary.blocks != r.blocks ||
        summary.snappy_blocks > r.blocks || summary.zoned_blocks > r.blocks) abort();
    uint64_t bad = UINT64_MAX;
    ray_err_t verified = ray_col_block_verify(&r, output, r.block_bytes, &bad);
    if (verified == RAY_ERR_CORRUPT && bad >= r.blocks) abort();
    /* On a file verify accepts, zone maps may only exclude a range none of
     * whose decoded rows match. (Re-signed metadata can lie; verify finds it.) */
    bool fixed = r.type == RAY_BOOL || r.type == RAY_U8 || r.type == RAY_I16 ||
        r.type == RAY_I32 || r.type == RAY_I64 || r.type == RAY_DATE ||
        r.type == RAY_TIME || r.type == RAY_TIMESTAMP;
    int64_t value = (int64_t)get(data + size - 16);
    if (r.type == RAY_I16) value = (int16_t)value;
    else if (r.type == RAY_I32 || r.type == RAY_DATE || r.type == RAY_TIME) value = (int32_t)value;
    else if (r.type == RAY_BOOL || r.type == RAY_U8) value = (uint8_t)value;
    for (int op = RAY_COL_BLOCK_EQ; op <= RAY_COL_BLOCK_GE; op++) {
        bool may = true;
        if (ray_col_block_range_may_match(&r, start, count, (ray_col_block_cmp_t)op, value,
                                          false, &may) != RAY_OK || may || !fixed || range_err ||
            verified != RAY_OK)
            continue;
        for (uint64_t i = 0; i < count; i++) {
            int64_t v;
            bool null = zone_row(range, i, r.type, &v);
            if (row_matches(null, v, (ray_col_block_cmp_t)op, value)) abort();
        }
    }
    free(output);
}
int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 65536) return 0;
    exercise(data, size);
    if (size < RAY_COL_BLOCK_HEADER + RAY_COL_BLOCK_FOOTER) return 0;
    uint8_t* copy = malloc(size);
    if (!copy) abort();
    memcpy(copy, data, size);
    uint64_t dir = get(copy + 56), blocks = get(copy + 48);
    /* Re-sign mutated metadata to reach structural validation beyond CRCs. */
    if (dir >= RAY_COL_BLOCK_HEADER && dir <= size - RAY_COL_BLOCK_FOOTER &&
        blocks <= (size - RAY_COL_BLOCK_FOOTER - dir) / RAY_COL_BLOCK_ENTRY) {
        put_crc(copy + 80, ray_crc32(0, copy + dir, (size_t)blocks * RAY_COL_BLOCK_ENTRY));
        put_crc(copy + 84, 0);
        put_crc(copy + 84, ray_crc32(0, copy, RAY_COL_BLOCK_HEADER));
        exercise(copy, size);
        /* Reach STR length validation even when mutations change raw payloads.
         * Open first so every offset/length used for CRC repair is bounded. */
        ray_col_block_reader_t r;
        if (ray_col_block_open(&r, copy, size) == RAY_OK && r.type == RAY_STR) {
            for (uint64_t b = 0; b < r.blocks; b++) {
                uint8_t* e = copy + r.directory + b * RAY_COL_BLOCK_ENTRY;
                if (e[32]) continue;
                size_t n = (size_t)(get(e + 24) & UINT32_MAX);
                uint32_t crc = ray_crc32(0, copy + get(e + 16), n);
                put_crc(e + 40, crc); put_crc(e + 44, crc);
            }
            put_crc(copy + 80, ray_crc32(0, copy + dir, (size_t)blocks * RAY_COL_BLOCK_ENTRY));
            put_crc(copy + 84, 0);
            put_crc(copy + 84, ray_crc32(0, copy, RAY_COL_BLOCK_HEADER));
            exercise(copy, size);
        }
    }
    free(copy);
    return 0;
}
