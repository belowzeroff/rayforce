/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_sym.h"
#include "core/crc32.h"
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>

ray_err_t ray_block_sym_checksum(const char* path, uint64_t* count, uint32_t* crc) {
    if (!path || !count || !crc) return RAY_ERR_DOMAIN;
    struct stat st;
    if (stat(path, &st)) return RAY_ERR_IO;
    if (!S_ISREG(st.st_mode) || st.st_size < 16) return RAY_ERR_CORRUPT;
    if ((uint64_t)st.st_size > RAY_BLOCK_SYM_MAX_BYTES) return RAY_ERR_LIMIT;
    FILE* f = fopen(path, "rb");
    if (!f) return RAY_ERR_IO;
    uint8_t buffer[8192];
    size_t left = (size_t)st.st_size;
    uint32_t sum = 0;
    ray_err_t err = RAY_OK;
    uint64_t entries = 0;
    while (left) {
        size_t n = left < sizeof(buffer) ? left : sizeof(buffer);
        if (fread(buffer, 1, n, f) != n) { err = RAY_ERR_IO; break; }
        if (left == (size_t)st.st_size) {
            for (unsigned i = 0; i < 8; i++) entries |= (uint64_t)buffer[4 + i] << (8 * i);
            /* Bound the existing domain parser's per-entry allocations before
             * it maps the file. Every STRL entry needs at least a u32 length. */
            if (memcmp(buffer, "STRL", 4) || !entries || entries > (left - 12) / 4) {
                err = RAY_ERR_CORRUPT; break;
            }
        }
        sum = ray_crc32(sum, buffer, n);
        left -= n;
    }
    if (!err && (fgetc(f) != EOF || ferror(f))) err = RAY_ERR_IO;
    if (fclose(f) && !err) err = RAY_ERR_IO;
    if (!err) { *crc = sum; *count = entries; }
    return err;
}
