/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#ifndef RAY_SNAPPY_H
#define RAY_SNAPPY_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Raw Snappy blocks, including the uncompressed-length varint (not framed
 * Snappy). No allocation or shared state; input and output must not overlap.
 * The caller owns this 64 KiB workspace and may reuse it between calls, but
 * concurrent encoders need separate workspaces. No initialization required. */
typedef struct {
    uint32_t positions[1u << 14];
} ray_snappy_workspace_t;

/* Zero means the input exceeds Snappy's uint32 length or the size_t bound. */
size_t ray_snappy_compress_bound(size_t size);

/* dst_cap must be at least compress_bound(size). Returns bytes written, or
 * zero for invalid arguments/capacity. An empty block occupies one byte.
 * src may be NULL only for size == 0. workspace and dst must be non-NULL. */
size_t ray_snappy_compress(const uint8_t* src, size_t size, uint8_t* dst,
                           size_t dst_cap, ray_snappy_workspace_t* workspace);

/* Requires exact input/output lengths; rejects trailing bytes. dst may be
 * NULL only for size == 0. On failure dst may contain a partial decode. */
bool ray_snappy_decompress(const uint8_t* src, size_t len,
                           uint8_t* dst, size_t size);

#endif
