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

/* Unaligned 8-byte copy; the compiler emits one load and one store. */
static inline void snappy_copy8(uint8_t* dst, const uint8_t* src) {
    uint64_t v;
    memcpy(&v, src, 8);
    memcpy(dst, &v, 8);
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
    /* An empty block is only its length; dst may be NULL, so no pointer
     * arithmetic on it. */
    if (!size) return in == len;
    /* Fast loop while both buffers have slack for unconditional wide moves:
     * 20 input bytes cover a tag, its operands and a 16-byte literal move;
     * 80 output bytes cover a 64-byte copy plus a trailing 8-byte move. Long
     * literals and 4-byte offsets fall through to the exact loop below. */
    const uint8_t* ip = src + in;
    const uint8_t* const end = src + len;
    uint8_t* op = dst + at;
    while (end - ip >= 20 && (dst + size) - op >= 80) {
        uint8_t tag = *ip++;
        size_t n, off;
        if (!(tag & 3)) {
            n = (size_t)(tag >> 2) + 1;
            if (n > 60) { ip--; break; }
            if (n > (size_t)(end - ip)) return false;
            snappy_copy8(op, ip);
            snappy_copy8(op + 8, ip + 8);
            if (n > 16) memcpy(op + 16, ip + 16, n - 16);
            op += n; ip += n;
            continue;
        }
        if ((tag & 3) == 1) {
            n = 4 + ((tag >> 2) & 7);
            off = ((size_t)(tag & 224) << 3) | *ip++;
        } else if ((tag & 3) == 2) {
            n = 1 + (tag >> 2);
            off = (size_t)ip[0] | (size_t)ip[1] << 8;
            ip += 2;
        } else { ip--; break; }
        if (!off || off > (size_t)(op - dst)) return false;
        if (off >= 8) {
            /* Sources end at or before their destinations: already decoded. */
            snappy_copy8(op, op - off);
            snappy_copy8(op + 8, op + 8 - off);
            for (size_t i = 16; i < n; i += 8) snappy_copy8(op + i, op + i - off);
        } else {
            const uint8_t* from = op - off;
            for (size_t i = 0; i < n; i++) op[i] = from[i];
        }
        op += n;
    }
    in = (size_t)(ip - src);
    at = (size_t)(op - dst);
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
            /* Short literals with 16 bytes of slack on both sides: two wide
             * copies, the excess output rewritten by later tags. */
            if (n <= 16 && len - in >= 16 && size - at >= 16) {
                snappy_copy8(dst + at, src + in);
                snappy_copy8(dst + at + 8, src + in + 8);
            } else memcpy(dst + at, src + in, (size_t)n);
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
            if (off >= 8 && size - at - n >= 16) {
                /* Each 8-byte source ends at or before its destination, so it
                 * is already decoded; writes past n stay inside the slack.
                 * Copy tags of 1-byte offsets hold at most 11 bytes: the
                 * common case is two unconditional moves. */
                snappy_copy8(dst + at, dst + at - off);
                snappy_copy8(dst + at + 8, dst + at + 8 - off);
                for (size_t i = 16; i < n; i += 8)
                    snappy_copy8(dst + at + i, dst + at + i - off);
                at += n;
                continue;
            }
            if (off == 1) {
                memset(dst + at, dst[at - 1], n);
                at += n;
                continue;
            }
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
