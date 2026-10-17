/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "core/crc32.h"
#include "core/snappy.h"
#include "core/pool.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* All integers are little-endian. Header: compatibility prefix [0,32),
 * magic[32,40), header size@40, block limit@44, block count@48, directory@56,
 * file size@64, generation@72, directory CRC@80, header CRC@84 (zeroed when
 * computing), SYM dictionary count@88, dictionary CRC@96, reserved[100,128).
 * Dictionary fields must be zero for non-SYM. Prefix: major@17, type@18, row count@24.
 * Entry: row@0, count@8, payload offset@16, stored size@24, decoded size@28,
 * codec@32, encoding@33 (0 fixed, 1 STR lengths/bytes), zone flags@34,
 * reserved[35,40), stored CRC@40, decoded CRC@44, zone min@48, zone max@56.
 * Zone flags: bit0 present, bit1 has null, bit2 has non-null values.
 * Regions are canonical and contiguous: header, blocks, directory, footer.
 * Empty columns have no blocks. Reserved bytes MUST be zero. */
static uint64_t get_le(const uint8_t* p, unsigned n) {
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
static int64_t get_i64_le(const uint8_t* p) {
    uint64_t bits = get_le(p, 8);
    int64_t value;
    memcpy(&value, &bits, sizeof(value));
    return value;
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
    case RAY_I64: case RAY_F64: case RAY_TIMESTAMP: case RAY_SYM: return 8;
    case RAY_GUID: return 16;
    default: return 0;
    }
}
static bool zone_type(uint8_t type) {
    return type == RAY_BOOL || type == RAY_U8 || type == RAY_I16 ||
        type == RAY_I32 || type == RAY_I64 || type == RAY_DATE ||
        type == RAY_TIME || type == RAY_TIMESTAMP;
}
static int64_t zone_value(const uint8_t* p, uint64_t i, uint8_t type) {
    switch (type) {
    case RAY_BOOL: case RAY_U8: return p[i];
    case RAY_I16: return (int16_t)get_le(p + i * 2, 2);
    case RAY_I32: case RAY_DATE: case RAY_TIME:
        return (int32_t)get_le(p + i * 4, 4);
    default: return get_i64_le(p + i * 8);
    }
}
static bool zone_null(int64_t value, uint8_t type) {
    return (type == RAY_I16 && value == NULL_I16) ||
        ((type == RAY_I32 || type == RAY_DATE || type == RAY_TIME) && value == NULL_I32) ||
        ((type == RAY_I64 || type == RAY_TIMESTAMP) && value == NULL_I64);
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
    if (!data || size < 32) return RAY_ERR_CORRUPT;
    const uint8_t* h = data;
    if (h[17] != 2) return RAY_ERR_VERSION;
    if (size < RAY_COL_BLOCK_HEADER + RAY_COL_BLOCK_FOOTER) return RAY_ERR_CORRUPT;
    if (!little_endian()) return RAY_ERR_NYI;
    bool strings = h[18] == RAY_STR;
    unsigned w = strings ? 4 : width(h[18]);
    uint32_t limit = (uint32_t)get_le(h + 44, 4);
    uint64_t rows = get_le(h + 24, 8), blocks = get_le(h + 48, 8);
    uint64_t dir = get_le(h + 56, 8);
    uint64_t sym_count = get_le(h + 88, 8);
    uint32_t sym_crc = (uint32_t)get_le(h + 96, 4);
    if (memcmp(h + 32, "RAYHDB2", 8) || !zero(h, 17) ||
        !zero(h + 19, 5) ||
        (h[18] == RAY_SYM ? (!sym_count || sym_count > INT64_MAX) : (sym_count || sym_crc)) ||
        get_le(h + 40, 4) != RAY_COL_BLOCK_HEADER ||
        get_le(h + 84, 4) != header_crc(h) || !w ||
        limit < w || limit > RAY_COL_BLOCK_MAX || (!strings && limit % w) ||
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
    /* Checksummed metadata that this reader cannot interpret: a newer file,
     * not a damaged one. */
    if ((uint32_t)get_le(h + 100, 4) & ~(uint32_t)RAY_COL_BLOCK_FEATURES) return RAY_ERR_NYI;
    /* Optional features add metadata an older reader may ignore, in the
     * reserved header and entry bytes; without any, those stay zero. */
    bool optional = get_le(h + 104, 4) != 0;
    if (!optional && !zero(h + 108, 20)) return RAY_ERR_CORRUPT;
    uint64_t row = 0, offset = RAY_COL_BLOCK_HEADER;
    for (uint64_t i = 0; i < blocks; i++) {
        const uint8_t* e = h + dir + i * RAY_COL_BLOCK_ENTRY;
        /* Unknown codec, encoding or zone flags: written by a newer writer. */
        if (e[32] > 1 || e[33] > 1 || (e[34] & ~7u)) return RAY_ERR_NYI;
        uint64_t count = get_le(e + 8, 8);
        uint32_t stored = (uint32_t)get_le(e + 24, 4);
        uint32_t decoded = (uint32_t)get_le(e + 28, 4);
        if (get_le(e, 8) != row || !count || count > (rows - row) ||
            (strings ? (decoded > limit || count > decoded / 4) :
                (count > limit / w || decoded != count * w)) ||
            get_le(e + 16, 8) != offset || !stored || stored > dir - offset ||
            e[32] > 1 || e[33] != (strings ? 1 : 0) ||
            (e[32] == 0 && get_le(e + 40, 4) != get_le(e + 44, 4)) ||
            (!optional && !zero(e + 35, 5)) ||
            (e[32] == 0 ? stored != decoded : stored >= decoded))
            return RAY_ERR_CORRUPT;
        uint8_t zone = e[34];
        if ((!zone && !zero(e + 48, 16)) ||
            (zone && (!(zone & 1) || !zone_type(h[18]) || !(zone & 6) ||
                (!(zone & 4) && !zero(e + 48, 16)) ||
                ((zone & 4) && get_i64_le(e + 48) > get_i64_le(e + 56)))))
            return RAY_ERR_CORRUPT;
        row += count; offset += stored;
    }
    if (row != rows || offset != dir) return RAY_ERR_CORRUPT;
    *r = (ray_col_block_reader_t){h, size, rows, blocks, dir,
        get_le(h + 72, 8), limit, h[18], sym_count, sym_crc, NULL, NULL};
    return RAY_OK;
}

static bool strings_valid(const uint8_t* data, size_t size, uint64_t rows) {
    if (rows > size / 4) return false;
    size_t offset = 0;
    for (uint64_t i = 0; i < rows; i++) {
        if (size - offset < 4) return false;
        uint32_t len = (uint32_t)get_le(data + offset, 4);
        offset += 4;
        if (len > size - offset) return false;
        offset += len;
    }
    return offset == size;
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
    /* Parallel range reads share the bitmap: bits are set atomically. */
    bool checked = r->verified &&
        (__atomic_load_n(&r->verified[block >> 3], __ATOMIC_RELAXED) >> (block & 7) & 1);
    /* Uncompressed payload is the decoded block: open requires equal CRCs. */
    if (!checked && ray_crc32(0, payload, stored) != get_le(e + 40, 4)) return RAY_ERR_CORRUPT;
    if (!e[32]) memcpy(output, payload, decoded);
    else if (!ray_snappy_decompress(payload, stored, output, decoded)) return RAY_ERR_CORRUPT;
    if (!checked && e[32] && ray_crc32(0, output, decoded) != get_le(e + 44, 4)) return RAY_ERR_CORRUPT;
    if (r->type == RAY_STR && !strings_valid(output, decoded, get_le(e + 8, 8)))
        return RAY_ERR_CORRUPT;
    if (r->verified)
        __atomic_fetch_or(&r->verified[block >> 3], (uint8_t)(1u << (block & 7)), __ATOMIC_RELAXED);
    *row_start = get_le(e, 8); *row_count = get_le(e + 8, 8);
    return RAY_OK;
}

ray_err_t ray_col_block_summary(const ray_col_block_reader_t* r,
                                ray_col_block_summary_t* out) {
    if (!r || !r->data || !out) return RAY_ERR_DOMAIN;
    memset(out, 0, sizeof(*out));
    out->blocks = r->blocks;
    for (uint64_t b = 0; b < r->blocks; b++) {
        const uint8_t* e = r->data + r->directory + b * RAY_COL_BLOCK_ENTRY;
        out->stored_bytes += get_le(e + 24, 4);
        out->decoded_bytes += get_le(e + 28, 4);
        out->snappy_blocks += e[32];
        out->zoned_blocks += e[34] & 1;
    }
    return RAY_OK;
}

ray_err_t ray_col_block_verify(const ray_col_block_reader_t* reader, void* scratch,
                               size_t capacity, uint64_t* bad_block) {
    if (!reader || !reader->data || !scratch) return RAY_ERR_DOMAIN;
    if (capacity < reader->block_bytes) return RAY_ERR_LIMIT;
    ray_col_block_reader_t copy = *reader;
    copy.verified = NULL;
    const ray_col_block_reader_t* r = &copy;
    for (uint64_t b = 0; b < r->blocks; b++) {
        uint64_t row, rows;
        ray_err_t err = ray_col_block_read(r, b, scratch, capacity, &row, &rows);
        if (!err && r->type == RAY_SYM) {
            for (uint64_t i = 0; i < rows; i++)
                if (get_le((const uint8_t*)scratch + i * 8, 8) >= r->sym_count) {
                    err = RAY_ERR_CORRUPT; break;
                }
        }
        /* Statistics drive pruning: they must describe the decoded rows. */
        const uint8_t* e = r->data + r->directory + b * RAY_COL_BLOCK_ENTRY;
        if (!err && (e[34] & 1)) {
            bool has_null = false, has_value = false;
            int64_t min = 0, max = 0;
            for (uint64_t i = 0; i < rows; i++) {
                int64_t value = zone_value(scratch, i, r->type);
                if (zone_null(value, r->type)) { has_null = true; continue; }
                if (!has_value) min = max = value;
                else { if (value < min) min = value; if (value > max) max = value; }
                has_value = true;
            }
            if (has_null != !!(e[34] & 2) || has_value != !!(e[34] & 4) ||
                (has_value && (get_i64_le(e + 48) != min || get_i64_le(e + 56) != max)))
                err = RAY_ERR_CORRUPT;
        }
        if (err) {
            if (bad_block) *bad_block = b;
            return err;
        }
    }
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

ray_err_t ray_col_block_range_blocks(const ray_col_block_reader_t* r,
                                    uint64_t start, uint64_t count,
                                    uint64_t* first, uint64_t* end) {
    if (!r || !r->data || !first || !end) return RAY_ERR_DOMAIN;
    if (start > r->rows || count > r->rows - start) return RAY_ERR_RANGE;
    *first = count ? block_for_row(r, start) : 0;
    *end = count ? block_for_row(r, start + count - 1) + 1 : 0;
    return RAY_OK;
}

static bool zone_may_match(const uint8_t* e, ray_col_block_cmp_t op,
                           int64_t value, bool value_is_null) {
    uint8_t flags = e[34];
    if (!(flags & 1)) return true;
    bool has_null = flags & 2, has_value = flags & 4;
    int64_t min = get_i64_le(e + 48), max = get_i64_le(e + 56);
    if (value_is_null) {
        switch (op) {
        case RAY_COL_BLOCK_EQ: return has_null;
        case RAY_COL_BLOCK_NE: return has_value;
        case RAY_COL_BLOCK_LT: return false;
        case RAY_COL_BLOCK_LE: return has_null;
        case RAY_COL_BLOCK_GT: return has_value;
        case RAY_COL_BLOCK_GE: return has_null || has_value;
        }
    }
    switch (op) {
    case RAY_COL_BLOCK_EQ: return has_value && min <= value && value <= max;
    case RAY_COL_BLOCK_NE: return has_null || (has_value && (min != value || max != value));
    case RAY_COL_BLOCK_LT: return has_null || (has_value && min < value);
    case RAY_COL_BLOCK_LE: return has_null || (has_value && min <= value);
    case RAY_COL_BLOCK_GT: return has_value && max > value;
    case RAY_COL_BLOCK_GE: return has_value && max >= value;
    }
    return true;
}

ray_err_t ray_col_block_range_may_match(const ray_col_block_reader_t* r,
                                       uint64_t start, uint64_t count,
                                       ray_col_block_cmp_t op, int64_t value,
                                       bool value_is_null, bool* may_match) {
    if (!may_match || op > RAY_COL_BLOCK_GE) return RAY_ERR_DOMAIN;
    uint64_t first, end;
    ray_err_t err = ray_col_block_range_blocks(r, start, count, &first, &end);
    if (err) return err;
    *may_match = false;
    for (uint64_t b = first; b < end; b++) {
        const uint8_t* e = r->data + r->directory + b * RAY_COL_BLOCK_ENTRY;
        if (zone_may_match(e, op, value, value_is_null)) { *may_match = true; break; }
    }
    return RAY_OK;
}

ray_err_t ray_col_block_range_size(const ray_col_block_reader_t* r,
                                  uint64_t start, uint64_t count,
                                  size_t* output_bytes, size_t* scratch_bytes) {
    if (!r || !r->data || !output_bytes || !scratch_bytes) return RAY_ERR_DOMAIN;
    unsigned esz = r->type == RAY_STR ? 16 : width(r->type);
    if (!esz) return RAY_ERR_TYPE;
    if (start > r->rows || count > r->rows - start) return RAY_ERR_RANGE;
    if (count > SIZE_MAX / esz) return RAY_ERR_LIMIT;
    size_t scratch = 0;
    if (count && r->type == RAY_STR) {
        uint64_t first = block_for_row(r, start), end = block_for_row(r, start + count - 1) + 1;
        for (uint64_t b = first; b < end; b++) {
            const uint8_t* e = r->data + r->directory + b * RAY_COL_BLOCK_ENTRY;
            size_t decoded = (size_t)get_le(e + 28, 4);
            if (scratch < decoded) scratch = decoded;
        }
    } else if (count) {
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

/* Whole blocks [first, end) decode into disjoint slices of one output. */
typedef struct {
    const ray_col_block_reader_t* r;
    uint8_t* output;
    uint64_t first, row;
    unsigned width;
    _Atomic int err;
} range_run_t;

static ray_err_t range_block(const range_run_t* run, uint64_t block) {
    const uint8_t* e = run->r->data + run->r->directory + block * RAY_COL_BLOCK_ENTRY;
    uint64_t row = get_le(e, 8), rows = get_le(e + 8, 8), ignored_row, ignored_count;
    return ray_col_block_read(run->r, block, run->output + (size_t)(row - run->row) * run->width,
                              (size_t)rows * run->width, &ignored_row, &ignored_count);
}

static void range_task(void* ctx, uint32_t worker, int64_t start, int64_t end) {
    (void)worker;
    range_run_t* run = ctx;
    for (int64_t i = start; i < end; i++) {
        if (atomic_load_explicit(&run->err, memory_order_relaxed)) return;
        ray_err_t err = range_block(run, run->first + (uint64_t)i);
        if (err) {
            int ok = RAY_OK;
            atomic_compare_exchange_strong_explicit(&run->err, &ok, (int)err,
                                                    memory_order_relaxed, memory_order_relaxed);
            return;
        }
    }
}

/* Runs of whole blocks totalling at least this many decoded bytes are
 * decoded on the native pool, one block per task. */
#define RANGE_PARALLEL_BYTES (256u * 1024u)

static ray_err_t range_whole(const ray_col_block_reader_t* r, uint64_t first, uint64_t end,
                             uint8_t* output, uint64_t row, unsigned width, size_t bytes) {
    range_run_t run = {r, output, first, row, width, RAY_OK};
    ray_pool_t* pool = ray_pool_get();
    if (end - first >= 2 && bytes >= RANGE_PARALLEL_BYTES &&
        ray_pool_par_dispatch_ok(pool, (int64_t)(end - first), 2)) {
        ray_pool_dispatch_n(pool, range_task, &run, (uint32_t)(end - first));
        return (ray_err_t)atomic_load(&run.err);
    }
    for (uint64_t b = first; b < end; b++) {
        ray_err_t err = range_block(&run, b);
        if (err) return err;
    }
    return RAY_OK;
}

ray_err_t ray_col_block_range_decoded(const ray_col_block_reader_t* r, uint64_t start,
                                     uint64_t count, size_t* bytes) {
    if (!bytes) return RAY_ERR_DOMAIN;
    uint64_t first, end;
    ray_err_t err = ray_col_block_range_blocks(r, start, count, &first, &end);
    if (err) return err;
    size_t total = 0;
    for (uint64_t b = first; b < end; b++) {
        size_t n = (size_t)get_le(r->data + r->directory + b * RAY_COL_BLOCK_ENTRY + 28, 4);
        if (n > SIZE_MAX - total) return RAY_ERR_LIMIT;
        total += n;
    }
    *bytes = total;
    return RAY_OK;
}

/* Blocks [first, first + n) decode into one arena at prefix-sum offsets. */
typedef struct {
    const ray_col_block_reader_t* r;
    uint8_t* arena;
    const size_t* offsets;
    uint64_t first;
    _Atomic int err;
} arena_run_t;

static ray_err_t arena_block(const arena_run_t* run, uint64_t i) {
    uint64_t row, rows;
    return ray_col_block_read(run->r, run->first + i, run->arena + run->offsets[i],
                              run->offsets[i + 1] - run->offsets[i], &row, &rows);
}

static void arena_task(void* ctx, uint32_t worker, int64_t start, int64_t end) {
    (void)worker;
    arena_run_t* run = ctx;
    for (int64_t i = start; i < end; i++) {
        if (atomic_load_explicit(&run->err, memory_order_relaxed)) return;
        ray_err_t err = arena_block(run, (uint64_t)i);
        if (err) {
            int ok = RAY_OK;
            atomic_compare_exchange_strong_explicit(&run->err, &ok, (int)err,
                                                    memory_order_relaxed, memory_order_relaxed);
            return;
        }
    }
}

ray_err_t ray_col_block_decode_arena(const ray_col_block_reader_t* r, uint64_t start,
                                    uint64_t count, uint8_t* arena, size_t capacity) {
    uint64_t first, end;
    size_t bytes;
    ray_err_t err = ray_col_block_range_blocks(r, start, count, &first, &end);
    if (!err) err = ray_col_block_range_decoded(r, start, count, &bytes);
    if (err) return err;
    if (capacity < bytes) return RAY_ERR_LIMIT;
    if (bytes && !arena) return RAY_ERR_DOMAIN;
    uint64_t n = end - first;
    if (!n) return RAY_OK;
    size_t* offsets = malloc((size_t)(n + 1) * sizeof(*offsets));
    if (!offsets) return RAY_ERR_OOM;
    offsets[0] = 0;
    for (uint64_t i = 0; i < n; i++)
        offsets[i + 1] = offsets[i] +
            (size_t)get_le(r->data + r->directory + (first + i) * RAY_COL_BLOCK_ENTRY + 28, 4);
    arena_run_t run = {r, arena, offsets, first, RAY_OK};
    ray_pool_t* pool = ray_pool_get();
    if (n >= 2 && bytes >= RANGE_PARALLEL_BYTES && ray_pool_par_dispatch_ok(pool, (int64_t)n, 2)) {
        ray_pool_dispatch_n(pool, arena_task, &run, (uint32_t)n);
        err = (ray_err_t)atomic_load(&run.err);
    } else {
        for (uint64_t i = 0; !err && i < n; i++) err = arena_block(&run, i);
    }
    free(offsets);
    return err;
}

ray_err_t ray_col_block_read_range(const ray_col_block_reader_t* r,
                                  uint64_t start, uint64_t count,
                                  void* output, size_t capacity,
                                  void* scratch, size_t scratch_capacity) {
    if (r && r->type == RAY_STR) return RAY_ERR_TYPE;
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
        ray_col_block_cache_t* cache = r->cache;
        if (!whole && cache && !cache->data) cache->data = malloc(r->block_bytes);
        const uint8_t* src;
        if (whole) {
            /* Only the range's first and last blocks can be partial. */
            uint64_t last = block_for_row(r, end - 1);
            const uint8_t* le = r->data + r->directory + last * RAY_COL_BLOCK_ENTRY;
            uint64_t run_end = get_le(le, 8) + get_le(le + 8, 8) == end ? last + 1 : last;
            uint64_t run_stop = run_end == last + 1 ? end : get_le(le, 8);
            n = (size_t)(run_stop - start) * esz;
            err = range_whole(r, block, run_end, dst, row, esz, n);
            if (err) return err;
            dst += n; start = run_stop; block = run_end;
            continue;
        } else if (cache && cache->data && cache->block == block) {
            src = cache->data;
        } else if (cache && cache->data) {
            cache->block = UINT64_MAX;
            err = ray_col_block_read(r, block, cache->data, r->block_bytes, &ignored_row, &ignored_count);
            if (!err) cache->block = block;
            src = cache->data;
        } else {
            err = ray_col_block_read(r, block, scratch, scratch_capacity, &ignored_row, &ignored_count);
            src = scratch;
        }
        if (err) return err;
        if (!whole) memcpy(dst, src + (size_t)(start - row) * esz, n);
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

static ray_err_t block_begin(ray_col_block_writer_t* w, FILE* output,
                              uint8_t type, uint32_t block_bytes,
                              uint8_t codec, uint64_t generation) {
    if (!w) return RAY_ERR_DOMAIN;
    memset(w, 0, sizeof(*w));
    if (!output || codec > 1) return RAY_ERR_DOMAIN;
    unsigned esz = type == RAY_STR ? 4 : width(type);
    if (!esz) return RAY_ERR_TYPE;
    if (block_bytes < esz || block_bytes > RAY_COL_BLOCK_MAX ||
        (type != RAY_STR && block_bytes % esz))
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

ray_err_t ray_col_block_begin(ray_col_block_writer_t* w, FILE* output,
                              uint8_t type, uint32_t block_bytes,
                              uint8_t codec, uint64_t generation) {
    if (type == RAY_SYM) {
        if (w) memset(w, 0, sizeof(*w));
        return RAY_ERR_TYPE;
    }
    return block_begin(w, output, type, block_bytes, codec, generation);
}

ray_err_t ray_col_block_begin_sym(ray_col_block_writer_t* w, FILE* output,
                                  uint32_t block_bytes, uint8_t codec,
                                  uint64_t generation, uint64_t sym_count,
                                  uint32_t sym_crc) {
    if (!w) return RAY_ERR_DOMAIN;
    if (!sym_count || sym_count > INT64_MAX) {
        memset(w, 0, sizeof(*w));
        return RAY_ERR_RANGE;
    }
    ray_err_t err = block_begin(w, output, RAY_SYM, block_bytes, codec, generation);
    if (!err) { w->sym_count = sym_count; w->sym_crc = sym_crc; }
    return err;
}

static ray_err_t append_block(ray_col_block_writer_t* w, const uint8_t* p,
                               size_t bytes, uint64_t count) {
    size_t stored = bytes;
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
    e[33] = w->type == RAY_STR ? 1 : 0;
    if (zone_type(w->type)) {
        bool has_null = false, has_value = false;
        int64_t min = 0, max = 0;
        for (uint64_t i = 0; i < count; i++) {
            int64_t value = zone_value(p, i, w->type);
            if (zone_null(value, w->type)) { has_null = true; continue; }
            if (!has_value) min = max = value;
            else { if (value < min) min = value; if (value > max) max = value; }
            has_value = true;
        }
        e[34] = 1u | (has_null ? 2u : 0u) | (has_value ? 4u : 0u);
        if (has_value) { put_le(e + 48, (uint64_t)min, 8); put_le(e + 56, (uint64_t)max, 8); }
    }
    put_le(e + 40, ray_crc32(0, payload, stored), 4);
    put_le(e + 44, ray_crc32(0, p, bytes), 4);
    if (fwrite(payload, 1, stored, w->output) != stored ||
        fwrite(e, 1, sizeof(e), w->directory) != sizeof(e))
        return w->error = RAY_ERR_IO;
    w->rows += count; w->blocks++; w->offset += stored;
    return RAY_OK;
}

ray_err_t ray_col_block_append_str(ray_col_block_writer_t* w, const void* encoded,
                                   size_t bytes, uint64_t rows) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    if (w->error) return w->error;
    if (w->type != RAY_STR) return w->error = RAY_ERR_TYPE;
    if (rows > (uint64_t)INT64_MAX - w->rows || bytes > w->block_bytes)
        return w->error = RAY_ERR_RANGE;
    if ((!encoded && bytes) || !strings_valid(encoded, bytes, rows))
        return w->error = RAY_ERR_CORRUPT;
    return rows ? append_block(w, encoded, bytes, rows) : RAY_OK;
}

ray_err_t ray_col_block_append(ray_col_block_writer_t* w, const void* values, uint64_t rows) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    if (w->error) return w->error;
    unsigned esz = width(w->type);
    if (!esz) return w->error = RAY_ERR_TYPE;
    if ((!values && rows) || rows > (uint64_t)INT64_MAX - w->rows || rows > SIZE_MAX / esz)
        return w->error = RAY_ERR_RANGE;
    const uint8_t* p = values;
    if (w->type == RAY_SYM) {
        for (uint64_t i = 0; i < rows; i++) {
            uint64_t position = get_le(p + i * 8, 8);
            if (position >= w->sym_count) return w->error = RAY_ERR_CORRUPT;
            if (position >= w->sym_used) w->sym_used = position + 1;
        }
    }
    while (rows) {
        uint64_t count = rows < w->block_bytes / esz ? rows : w->block_bytes / esz;
        size_t bytes = (size_t)count * esz;
        ray_err_t err = append_block(w, p, bytes, count);
        if (err) return err;
        p += bytes; rows -= count;
    }
    return RAY_OK;
}

ray_err_t ray_col_block_set_sym(ray_col_block_writer_t* w, uint64_t sym_count,
                                uint32_t sym_crc) {
    if (!w || !w->output) return RAY_ERR_DOMAIN;
    if (w->error) return w->error;
    if (w->type != RAY_SYM) return w->error = RAY_ERR_TYPE;
    if (!sym_count || sym_count > INT64_MAX) return w->error = RAY_ERR_RANGE;
    if (sym_count < w->sym_used) return w->error = RAY_ERR_CORRUPT;
    w->sym_count = sym_count; w->sym_crc = sym_crc;
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
    put_le(header + 88, w->sym_count, 8); put_le(header + 96, w->sym_crc, 4);
    put_le(header + 84, header_crc(header), 4);
    if (!err && (fwrite(footer, 1, sizeof(footer), w->output) != sizeof(footer) ||
        fseek(w->output, 0, SEEK_SET) ||
        fwrite(header, 1, sizeof(header), w->output) != sizeof(header) || fflush(w->output)))
        err = RAY_ERR_IO;
    ray_col_block_abort(w);
    return err;
}
