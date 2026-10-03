/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "store/col_block.h"
#include "core/crc32.h"
#include <stdlib.h>
#include <string.h>

static uint64_t get(const uint8_t* p) {
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
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
    }
    free(copy);
    return 0;
}
