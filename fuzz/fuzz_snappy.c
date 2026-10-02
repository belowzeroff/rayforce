/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "core/snappy.h"
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 65536) return 0;
    ray_snappy_workspace_t workspace;
    size_t bound = ray_snappy_compress_bound(size);
    uint8_t* encoded = malloc(bound);
    uint8_t* decoded = malloc(size ? size : 1);
    if (!encoded || !decoded) abort();
    size_t n = ray_snappy_compress(data, size, encoded, bound, &workspace);
    if (!n || !ray_snappy_decompress(encoded, n, decoded, size) ||
        memcmp(data, decoded, size)) abort();
    if (size) {
        encoded[data[0] % n] ^= data[size - 1];
        (void)ray_snappy_decompress(encoded, n, decoded, size);
    }
    free(encoded); free(decoded);

    /* Decode arbitrary streams too; never allocate an unbounded forged size. */
    uint64_t output = 0;
    for (size_t i = 0; i < size && i < 5; i++) {
        output |= (uint64_t)(data[i] & 127) << (i * 7);
        if (!(data[i] & 128)) {
            if (output <= 1024 * 1024) {
                decoded = malloc(output ? (size_t)output : 1);
                if (!decoded) abort();
                (void)ray_snappy_decompress(data, size, decoded, (size_t)output);
                free(decoded);
            }
            break;
        }
    }
    return 0;
}
