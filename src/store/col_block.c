/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "core/crc32.h"
#include "core/snappy.h"
#include <stdlib.h>
#include <string.h>

/* All integers are little-endian. Header: compatibility prefix [0,32),
 * magic[32,40), header size@40, block limit@44, block count@48, directory@56,
 * file size@64, generation@72, directory CRC@80, header CRC@84 (zeroed when
 * computing), reserved[88,128). Prefix: major@17, type@18, row count@24.
 * Entry: row@0, count@8, payload offset@16, stored size@24, decoded size@28,
 * codec@32, encoding@33 (0), reserved[34,40), stored CRC@40, decoded CRC@44,
 * reserved[48,64). Footer: magic[0,8), blocks@8, directory@16, file size@24.
 * Regions are canonical and contiguous: header, blocks, directory, footer.
 * Empty columns have no blocks. Reserved bytes MUST be zero. */
static uint64_t get_le(const uint8_t* p, unsigned n) {
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
static void put_le(uint8_t* p, uint64_t v, unsigned n) {
    for (unsigned i = 0; i < n; i++) { p[i] = (uint8_t)v; v >>= 8; }
}
static bool zero(const uint8_t* p, size_t n) {
    for (size_t i = 0; i < n; i++) if (p[i]) return false;
    return true;
}
static bool little_endian(void) {
    const uint16_t v = 1;
    return *(const uint8_t*)&v == 1;
}
static unsigned width(uint8_t type) {
    switch (type) {
    case RAY_BOOL: case RAY_U8: return 1;
    case RAY_I16: return 2;
    case RAY_I32: case RAY_F32: case RAY_DATE: case RAY_TIME: return 4;
    case RAY_I64: case RAY_F64: case RAY_TIMESTAMP: return 8;
    case RAY_GUID: return 16;
    default: return 0;
    }
}
static uint32_t header_crc(const uint8_t* h) {
    const uint8_t zeros[4] = {0};
    uint32_t crc = ray_crc32(0, h, 84);
    crc = ray_crc32(crc, zeros, 4);
    return ray_crc32(crc, h + 88, 40);
}

ray_err_t ray_col_block_open(ray_col_block_reader_t* r, const void* data, size_t size) {
    if (!r) return RAY_ERR_DOMAIN;
    memset(r, 0, sizeof(*r));
    if (!data || size < RAY_COL_BLOCK_HEADER + RAY_COL_BLOCK_FOOTER)
        return RAY_ERR_CORRUPT;
    const uint8_t* h = data;
    if (h[17] != 2) return RAY_ERR_VERSION;
    if (!little_endian()) return RAY_ERR_NYI;
    unsigned w = width(h[18]);
    uint32_t limit = (uint32_t)get_le(h + 44, 4);
    uint64_t rows = get_le(h + 24, 8), blocks = get_le(h + 48, 8);
    uint64_t dir = get_le(h + 56, 8);
    if (memcmp(h + 32, "RAYHDB2", 8) || !zero(h, 17) ||
        !zero(h + 19, 5) || !zero(h + 88, 40) ||
        get_le(h + 40, 4) != RAY_COL_BLOCK_HEADER ||
        get_le(h + 84, 4) != header_crc(h) || !w ||
        limit < w || limit > RAY_COL_BLOCK_MAX || limit % w ||
        rows > INT64_MAX || get_le(h + 64, 8) != size ||
        dir < RAY_COL_BLOCK_HEADER || dir > size - RAY_COL_BLOCK_FOOTER ||
        blocks > (size - RAY_COL_BLOCK_FOOTER - dir) / RAY_COL_BLOCK_ENTRY ||
        blocks * RAY_COL_BLOCK_ENTRY != size - RAY_COL_BLOCK_FOOTER - dir)
        return RAY_ERR_CORRUPT;
    const uint8_t* footer = h + size - RAY_COL_BLOCK_FOOTER;
    if (memcmp(footer, "RAYEND2", 8) || get_le(footer + 8, 8) != blocks ||
        get_le(footer + 16, 8) != dir || get_le(footer + 24, 8) != size ||
        get_le(h + 80, 4) != ray_crc32(0, h + dir, (size_t)(blocks * RAY_COL_BLOCK_ENTRY)))
        return RAY_ERR_CORRUPT;
    uint64_t row = 0, offset = RAY_COL_BLOCK_HEADER;
    for (uint64_t i = 0; i < blocks; i++) {
        const uint8_t* e = h + dir + i * RAY_COL_BLOCK_ENTRY;
        uint64_t count = get_le(e + 8, 8);
        uint32_t stored = (uint32_t)get_le(e + 24, 4);
        uint32_t decoded = (uint32_t)get_le(e + 28, 4);
        if (get_le(e, 8) != row || !count || count > (rows - row) ||
            count > limit / w || decoded != count * w ||
            get_le(e + 16, 8) != offset || !stored || stored > dir - offset ||
            e[32] > 1 || !zero(e + 33, 7) || !zero(e + 48, 16) ||
            (e[32] == 0 ? stored != decoded : stored >= decoded))
            return RAY_ERR_CORRUPT;
        row += count; offset += stored;
    }
    if (row != rows || offset != dir) return RAY_ERR_CORRUPT;
    *r = (ray_col_block_reader_t){h, size, rows, blocks, dir,
        get_le(h + 72, 8), limit, h[18]};
    return RAY_OK;
}

ray_err_t ray_col_block_read(const ray_col_block_reader_t* r, uint64_t block,
                             void* output, size_t capacity,
                             uint64_t* row_start, uint64_t* row_count) {
    if (!r || !r->data || !output || !row_start || !row_count) return RAY_ERR_DOMAIN;
    if (block >= r->blocks) return RAY_ERR_RANGE;
    const uint8_t* e = r->data + r->directory + block * RAY_COL_BLOCK_ENTRY;
    size_t stored = (size_t)get_le(e + 24, 4), decoded = (size_t)get_le(e + 28, 4);
    if (capacity < decoded) return RAY_ERR_LIMIT;
    const uint8_t* payload = r->data + get_le(e + 16, 8);
    if (ray_crc32(0, payload, stored) != get_le(e + 40, 4)) return RAY_ERR_CORRUPT;
    if (!e[32]) memcpy(output, payload, decoded);
    else if (!ray_snappy_decompress(payload, stored, output, decoded)) return RAY_ERR_CORRUPT;
    if (ray_crc32(0, output, decoded) != get_le(e + 44, 4)) return RAY_ERR_CORRUPT;
    *row_start = get_le(e, 8); *row_count = get_le(e + 8, 8);
    return RAY_OK;
}

/* Find the block containing an existing row (row < reader->rows). */
static uint64_t block_for_row(const ray_col_block_reader_t* r, uint64_t row) {
    uint64_t lo = 0, hi = r->blocks;
    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;
        const uint8_t* e = r->data + r->directory + mid * RAY_COL_BLOCK_ENTRY;
        if (get_le(e, 8) <= row) lo = mid + 1;
        else hi = mid;
    }
    return lo - 1;
}

ray_err_t ray_col_block_range_size(const ray_col_block_reader_t* r,
                                  uint64_t start, uint64_t count,
                                  size_t* output_bytes, size_t* scratch_bytes) {
    if (!r || !r->data || !output_bytes || !scratch_bytes) return RAY_ERR_DOMAIN;
    unsigned esz = width(r->type);
    if (!esz) return RAY_ERR_TYPE;
    if (start > r->rows || count > r->rows - start) return RAY_ERR_RANGE;
    if (count > SIZE_MAX / esz) return RAY_ERR_LIMIT;
    size_t scratch = 0;
    if (count) {
        uint64_t boundary[2] = {block_for_row(r, start), block_for_row(r, start + count - 1)};
        for (unsigned i = 0; i < 2; i++) {
            const uint8_t* e = r->data + r->directory + boundary[i] * RAY_COL_BLOCK_ENTRY;
            uint64_t row = get_le(e, 8), rows = get_le(e + 8, 8);
            if (start > row || start + count < row + rows) {
                size_t decoded = (size_t)get_le(e + 28, 4);
                if (scratch < decoded) scratch = decoded;
            }
        }
    }
    *output_bytes = (size_t)count * esz;
    *scratch_bytes = scratch;
    return RAY_OK;
}

ray_err_t ray_col_block_read_range(const ray_col_block_reader_t* r,
                                  uint64_t start, uint64_t count,
                                  void* output, size_t capacity,
                                  void* scratch, size_t scratch_capacity) {
    size_t bytes, needed;
    ray_err_t err = ray_col_block_range_size(r, start, count, &bytes, &needed);
    if (err) return err;
    if (capacity < bytes || scratch_capacity < needed) return RAY_ERR_LIMIT;
    if ((bytes && !output) || (needed && !scratch)) return RAY_ERR_DOMAIN;
    if (!count) return RAY_OK;
    unsigned esz = width(r->type);
    uint64_t block = block_for_row(r, start), end = start + count;
    uint8_t* dst = output;
    while (start < end) {
        const uint8_t* e = r->data + r->directory + block * RAY_COL_BLOCK_ENTRY;
        uint64_t row = get_le(e, 8), rows = get_le(e + 8, 8);
        uint64_t stop = row + rows < end ? row + rows : end;
        bool whole = start == row && stop == row + rows;
        size_t n = (size_t)(stop - start) * esz;
        uint64_t ignored_row, ignored_count;
        err = ray_col_block_read(r, block, whole ? dst : scratch,
            whole ? n : scratch_capacity, &ignored_row, &ignored_count);
        if (err) return err;
        if (!whole) memcpy(dst, (uint8_t*)scratch + (size_t)(start - row) * esz, n);
        dst += n; start = stop; block++;
    }
    return RAY_OK;
}

void ray_col_block_abort(ray_col_block_writer_t* w) {
    if (!w) return;
    if (w->directory) fclose(w->directory);
    free(w->scratch);
    memset(w, 0, sizeof(*w));
}

ray_err_t ray_col_block_begin(ray_col_block_writer_t* w, FILE* output,
                              uint8_t type, uint32_t block_bytes,
                              uint8_t codec, uint64_t generation) {
    if (!w) return RAY_ERR_DOMAIN;
    memset(w, 0, sizeof(*w));
    if (!output || codec > 1) return RAY_ERR_DOMAIN;
    unsigned esz = width(type);
    if (!esz) return RAY_ERR_TYPE;
    if (block_bytes < esz || block_bytes > RAY_COL_BLOCK_MAX || block_bytes % esz)
        return RAY_ERR_RANGE;
    if (!little_endian()) return RAY_ERR_NYI;
    if (fseek(output, 0, SEEK_END) || ftell(output) != 0) return RAY_ERR_IO;
    w->directory = tmpfile();
    if (!w->directory) return RAY_ERR_IO;
    if (codec) {
        w->scratch = malloc(sizeof(ray_snappy_workspace_t) + ray_snappy_compress_bound(block_bytes));
        if (!w->scratch) { ray_col_block_abort(w); return RAY_ERR_OOM; }
    }
    w->output = output; w->type = type; w->codec = codec;
    w->block_bytes = block_bytes; w->generation = generation;
    w->offset = RAY_COL_BLOCK_HEADER;
    const uint8_t header[RAY_COL_BLOCK_HEADER] = {0};
    if (fwrite(header, 1, sizeof(header), output) != sizeof(header)) {
        ray_col_block_abort(w); return RAY_ERR_IO;
    }
    return RAY_OK;
}

ray_err_t ray_col_block_append(ray_col_block_writer_t* w, const void* values, uint64_t rows) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    if (w->error) return w->error;
    unsigned esz = width(w->type);
    if (!esz) return w->error = RAY_ERR_TYPE;
    if ((!values && rows) || rows > (uint64_t)INT64_MAX - w->rows || rows > SIZE_MAX / esz)
        return w->error = RAY_ERR_RANGE;
    const uint8_t* p = values;
    while (rows) {
        uint64_t count = rows < w->block_bytes / esz ? rows : w->block_bytes / esz;
        size_t bytes = (size_t)count * esz, stored = bytes;
        const uint8_t* payload = p;
        uint8_t codec = 0;
        if (w->codec) {
            ray_snappy_workspace_t* workspace = w->scratch;
            uint8_t* compressed = (uint8_t*)(workspace + 1);
            size_t n = ray_snappy_compress(p, bytes, compressed,
                ray_snappy_compress_bound(w->block_bytes), workspace);
            if (!n) return w->error = RAY_ERR_CORRUPT;
            if (n < bytes) { stored = n; payload = compressed; codec = 1; }
        }
        /* Reserve space for this entry and footer before advancing the file. */
        if (w->blocks >= (UINT64_MAX - RAY_COL_BLOCK_FOOTER) / RAY_COL_BLOCK_ENTRY ||
            w->offset > UINT64_MAX - RAY_COL_BLOCK_FOOTER -
                (w->blocks + 1) * RAY_COL_BLOCK_ENTRY ||
            stored > UINT64_MAX - RAY_COL_BLOCK_FOOTER -
                (w->blocks + 1) * RAY_COL_BLOCK_ENTRY - w->offset)
            return w->error = RAY_ERR_LIMIT;
        uint8_t e[RAY_COL_BLOCK_ENTRY] = {0};
        put_le(e, w->rows, 8); put_le(e + 8, count, 8); put_le(e + 16, w->offset, 8);
        put_le(e + 24, stored, 4); put_le(e + 28, bytes, 4); e[32] = codec;
        put_le(e + 40, ray_crc32(0, payload, stored), 4);
        put_le(e + 44, ray_crc32(0, p, bytes), 4);
        if (fwrite(payload, 1, stored, w->output) != stored ||
            fwrite(e, 1, sizeof(e), w->directory) != sizeof(e))
            return w->error = RAY_ERR_IO;
        w->rows += count; w->blocks++; w->offset += stored;
        p += bytes; rows -= count;
    }
    return RAY_OK;
}

ray_err_t ray_col_block_finish(ray_col_block_writer_t* w) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    ray_err_t err = w->error;
    if (err) { ray_col_block_abort(w); return err; }
    uint8_t buffer[4096];
    uint32_t crc = 0;
    if (fseek(w->directory, 0, SEEK_SET)) err = RAY_ERR_IO;
    uint64_t remaining = w->blocks * RAY_COL_BLOCK_ENTRY;
    while (!err && remaining) {
        size_t n = remaining < sizeof(buffer) ? (size_t)remaining : sizeof(buffer);
        if (fread(buffer, 1, n, w->directory) != n || fwrite(buffer, 1, n, w->output) != n)
            err = RAY_ERR_IO;
        else { crc = ray_crc32(crc, buffer, n); remaining -= n; }
    }
    uint64_t size = w->offset + w->blocks * RAY_COL_BLOCK_ENTRY + RAY_COL_BLOCK_FOOTER;
    uint8_t footer[RAY_COL_BLOCK_FOOTER] = {0}, header[RAY_COL_BLOCK_HEADER] = {0};
    memcpy(footer, "RAYEND2", 8); put_le(footer + 8, w->blocks, 8);
    put_le(footer + 16, w->offset, 8); put_le(footer + 24, size, 8);
    header[17] = 2; header[18] = w->type; put_le(header + 24, w->rows, 8);
    memcpy(header + 32, "RAYHDB2", 8); put_le(header + 40, sizeof(header), 4);
    put_le(header + 44, w->block_bytes, 4); put_le(header + 48, w->blocks, 8);
    put_le(header + 56, w->offset, 8); put_le(header + 64, size, 8);
    put_le(header + 72, w->generation, 8); put_le(header + 80, crc, 4);
    put_le(header + 84, header_crc(header), 4);
    if (!err && (fwrite(footer, 1, sizeof(footer), w->output) != sizeof(footer) ||
        fseek(w->output, 0, SEEK_SET) ||
        fwrite(header, 1, sizeof(header), w->output) != sizeof(header) || fflush(w->output)))
        err = RAY_ERR_IO;
    ray_col_block_abort(w);
    return err;
}
