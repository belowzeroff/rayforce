/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE.
 * Format: github.com/google/snappy/blob/main/format_description.txt.
 * Native implementation; no third-party code or libraries.
 */
#include "snappy.h"
#include <string.h>

static uint32_t snappy_u32(const uint8_t* p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 |
           (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

size_t ray_snappy_compress_bound(size_t size) {
    if (size > UINT32_MAX || size > SIZE_MAX - 32 - size / 6) return 0;
    return 32 + size + size / 6;
}

static bool snappy_literal(const uint8_t* src, size_t n, uint8_t* dst,
                            size_t cap, size_t* at) {
    if (!n) return true;
    uint32_t last = (uint32_t)(n - 1);
    unsigned extra = 0;
    if (n > 60) {
        uint32_t v = last;
        do { extra++; v >>= 8; } while (v);
    }
    size_t header = 1 + extra;
    if (header > cap - *at || n > cap - *at - header) return false;
    dst[(*at)++] = (uint8_t)((extra ? 59 + extra : last) << 2);
    for (unsigned i = 0; i < extra; i++) {
        dst[(*at)++] = (uint8_t)last;
        last >>= 8;
    }
    memcpy(dst + *at, src, n);
    *at += n;
    return true;
}

static bool snappy_copy(size_t offset, size_t n, uint8_t* dst,
                         size_t cap, size_t* at) {
    size_t tags = 1 + (n - 1) / 64;
    if (tags > (cap - *at) / 3) return false;
    while (n) {
        size_t count = n < 64 ? n : 64;
        dst[(*at)++] = (uint8_t)(((count - 1) << 2) | 2);
        dst[(*at)++] = (uint8_t)offset;
        dst[(*at)++] = (uint8_t)(offset >> 8);
        n -= count;
    }
    return true;
}

size_t ray_snappy_compress(const uint8_t* src, size_t size, uint8_t* dst,
                           size_t dst_cap, ray_snappy_workspace_t* workspace) {
    size_t bound = ray_snappy_compress_bound(size);
    if (!bound || dst_cap < bound || !dst || !workspace || (!src && size))
        return 0;

    size_t out = 0;
    uint32_t remaining = (uint32_t)size;
    do {
        uint8_t b = (uint8_t)(remaining & 127);
        remaining >>= 7;
        dst[out++] = (uint8_t)(b | (remaining ? 128 : 0));
    } while (remaining);
    if (!size) return out;

    memset(workspace, 0, sizeof(*workspace));
    size_t at = 0, literal = 0;
    while (size - at >= 4) {
        uint32_t word = snappy_u32(src + at);
        uint32_t hash = (word * UINT32_C(2654435761)) >> 18;
        uint32_t previous = workspace->positions[hash];
        workspace->positions[hash] = (uint32_t)at + 1;
        if (!previous || at - (previous - 1) > UINT16_MAX ||
            snappy_u32(src + previous - 1) != word) {
            at++;
            continue;
        }

        size_t candidate = previous - 1, count = 4;
        while (count < size - at && src[candidate + count] == src[at + count])
            count++;
        if (!snappy_literal(src + literal, at - literal, dst, dst_cap, &out) ||
            !snappy_copy(at - candidate, count, dst, dst_cap, &out))
            return 0;
        at += count;
        literal = at;
        /* Seed the match's last position so adjacent runs can reference it. */
        if (size - at >= 3) {
            hash = (snappy_u32(src + at - 1) * UINT32_C(2654435761)) >> 18;
            workspace->positions[hash] = (uint32_t)at;
        }
    }
    if (!snappy_literal(src + literal, size - literal, dst, dst_cap, &out))
        return 0;
    return out;
}

bool ray_snappy_decompress(const uint8_t* src, size_t len,
                           uint8_t* dst, size_t size) {
    if (!src || !len || (!dst && size) || size > UINT32_MAX) return false;
    size_t in = 0, at = 0;
    uint32_t expected = 0;
    for (unsigned shift = 0;; shift += 7) {
        if (in == len || shift > 28) return false;
        uint8_t b = src[in++];
        if (shift == 28 && b > 15) return false;
        expected |= (uint32_t)(b & 127) << shift;
        if (!(b & 128)) break;
    }
    if (expected != size) return false;
    while (in < len && at < size) {
        uint8_t tag = src[in++];
        if (!(tag & 3)) {
            uint64_t n = tag >> 2;
            if (n < 60) n++;
            else {
                unsigned bytes = (unsigned)n - 59;
                if (bytes > len - in) return false;
                uint32_t last = 0;
                for (unsigned i = 0; i < bytes; i++)
                    last |= (uint32_t)src[in++] << (8 * i);
                n = (uint64_t)last + 1;
            }
            if (n > size - at || n > len - in) return false;
            memcpy(dst + at, src + in, (size_t)n);
            in += (size_t)n;
            at += (size_t)n;
        } else {
            size_t n, off;
            if ((tag & 3) == 1) {
                if (in == len) return false;
                n = 4 + ((tag >> 2) & 7);
                off = ((size_t)(tag & 224) << 3) | src[in++];
            } else {
                unsigned bytes = (tag & 3) == 2 ? 2 : 4;
                if (bytes > len - in) return false;
                off = bytes == 2 ? (size_t)src[in] | (size_t)src[in + 1] << 8
                                 : snappy_u32(src + in);
                in += bytes;
                n = 1 + (tag >> 2);
            }
            if (!off || off > at || n > size - at) return false;
            /* Grow nonoverlapping spans to decode overlapping backreferences. */
            size_t copied = 0;
            while (copied < n) {
                size_t step = n - copied < off ? n - copied : off;
                memcpy(dst + at + copied, dst + at + copied - off, step);
                copied += step;
                if (copied >= off) off += off;
            }
            at += n;
        }
    }
    return at == size && in == len;
}
