/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/col_block.h"
#include "store/col.h"
#include "core/crc32.h"
#include "mem/heap.h"
#include "mem/sys.h"
#include "table/sym.h"
#include "table/domain.h"
#include <stdlib.h>
#include <unistd.h>

static FILE* file;
static char path[64];
static ray_col_block_writer_t writer;
static uint8_t bytes[65536], copy[65536], input[16384], output[16384];
static size_t file_size;
static ray_col_block_file_t* owned_file;

static void setup(void) {
    ray_heap_init(); ray_sym_init();
    strcpy(path, "/tmp/ray-col-block-XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) ray_test_fatal("create column fixture");
    file = fdopen(fd, "w+b");
    if (!file) { close(fd); unlink(path); ray_test_fatal("open column fixture"); }
    memset(&writer, 0, sizeof(writer));
}
static void teardown(void) {
    ray_col_block_file_close(&owned_file);
    ray_col_block_abort(&writer);
    if (file) fclose(file);
    file = NULL; unlink(path);
    ray_sym_destroy(); ray_heap_destroy();
}
static uint64_t get(const uint8_t* p, unsigned n) {
    uint64_t v = 0;
    for (unsigned i = 0; i < n; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}
static void put(uint8_t* p, uint64_t v, unsigned n) {
    for (unsigned i = 0; i < n; i++) { p[i] = (uint8_t)v; v >>= 8; }
}
static void snapshot(void) {
    if (fseek(file, 0, SEEK_END)) ray_test_fatal("seek fixture");
    long n = ftell(file);
    if (n < 0 || (size_t)n > sizeof(bytes)) ray_test_fatal("fixture size");
    file_size = (size_t)n;
    rewind(file);
    if (fread(bytes, 1, file_size, file) != file_size) ray_test_fatal("read fixture");
}
static void reset_file(void) {
    if (fflush(file) || ftruncate(fileno(file), 0)) ray_test_fatal("reset fixture");
    rewind(file);
}
static void rechecksum(uint8_t* data) {
    size_t dir = (size_t)get(data + 56, 8);
    size_t length = (size_t)get(data + 48, 8) * RAY_COL_BLOCK_ENTRY;
    put(data + 80, ray_crc32(0, data + dir, length), 4);
    put(data + 84, 0, 4);
    put(data + 84, ray_crc32(0, data, RAY_COL_BLOCK_HEADER), 4);
}

static test_result_t roundtrip(void) {
    const unsigned widths[] = {0, 1, 1, 2, 4, 8, 4, 8, 4, 4, 8, 16};
    for (uint8_t type = RAY_BOOL; type <= RAY_GUID; type++) {
        unsigned width = widths[type];
        for (uint8_t codec = 0; codec <= 1; codec++) {
            reset_file();
            for (size_t i = 0; i < sizeof(input); i++) input[i] = (uint8_t)(i % 2);
            TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, type, 64, codec, 987), RAY_OK);
            TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input, 3), RAY_OK);
            TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input + 3 * width, 510), RAY_OK);
            TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
            snapshot();
            ray_col_block_reader_t r;
            TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
            TEST_ASSERT_EQ_I(r.rows, 513); TEST_ASSERT_EQ_I(r.generation, 987);
            TEST_ASSERT_EQ_I(r.type, type);
            uint64_t total = 0;
            for (uint64_t b = r.blocks; b-- > 0;) {
                uint64_t row, count;
                TEST_ASSERT_EQ_I(ray_col_block_read(&r, b, output, sizeof(output), &row, &count), RAY_OK);
                TEST_ASSERT_TRUE(memcmp(output, input + row * width, count * width) == 0);
                total += count;
            }
            TEST_ASSERT_EQ_I(total, 513);
            uint8_t scratch[64];
            ray_t* vec = ray_col_block_materialize(&r, 2, 509, 509 * width, scratch, sizeof(scratch));
            TEST_ASSERT_FALSE(RAY_IS_ERR(vec));
            TEST_ASSERT_EQ_I(vec->type, type); TEST_ASSERT_EQ_I(vec->len, 509);
            TEST_ASSERT_TRUE(memcmp(ray_data(vec), input + 2 * width, 509 * width) == 0);
            ray_release(vec);
            /* Existing loaders must not interpret compressed bytes as vectors. */
            ray_t* old = ray_col_load(path);
            TEST_ASSERT_TRUE(RAY_IS_ERR(old));
            TEST_ASSERT_STR_EQ(ray_err_code(old), "version"); ray_release(old);
            old = ray_col_mmap(path);
            TEST_ASSERT_TRUE(RAY_IS_ERR(old));
            TEST_ASSERT_STR_EQ(ray_err_code(old), "version"); ray_release(old);
        }
    }
    PASS();
}

static test_result_t empty_and_bounds(void) {
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_SYM, 64, 1, 0), RAY_ERR_TYPE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_STR, 64, 1, 0), RAY_ERR_TYPE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 63, 1, 0), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 0, 1, 0), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, RAY_COL_BLOCK_MAX + 8, 1, 0), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 2, 0), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, NULL, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    TEST_ASSERT_EQ_I(file_size, RAY_COL_BLOCK_HEADER + RAY_COL_BLOCK_FOOTER);
    ray_col_block_reader_t r;
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    TEST_ASSERT_EQ_I(r.rows, 0); TEST_ASSERT_EQ_I(r.blocks, 0);
    TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 0, 0, NULL, 0, NULL, 0), RAY_OK);
    ray_t* empty = ray_col_block_materialize(&r, 0, 0, 0, NULL, 0);
    TEST_ASSERT_FALSE(RAY_IS_ERR(empty)); TEST_ASSERT_EQ_I(empty->len, 0);
    TEST_ASSERT_EQ_I(empty->type, RAY_I64); ray_release(empty);
    uint64_t row = 99, count = 99;
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 0, output, sizeof(output), &row, &count), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 0, 0), RAY_ERR_IO);
    reset_file();
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 0, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input, UINT64_MAX), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input, 1), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_ERR_RANGE);
    TEST_ASSERT_TRUE(writer.output == NULL && writer.directory == NULL && writer.scratch == NULL);
    snapshot();
    TEST_ASSERT_TRUE(ray_col_block_open(&r, bytes, file_size) != RAY_OK);
    TEST_ASSERT_TRUE(r.data == NULL);
    PASS();
}

static test_result_t corruption(void) {
    memset(input, 0, sizeof(input));
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 5), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input, 24), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    ray_col_block_reader_t r;
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    size_t dir = (size_t)r.directory;
    TEST_ASSERT_EQ_I(bytes[dir + 32], 1);
    for (size_t n = 0; n < file_size; n++)
        TEST_ASSERT_TRUE(ray_col_block_open(&r, bytes, n) != RAY_OK);
    bytes[file_size] = 0;
    TEST_ASSERT_TRUE(ray_col_block_open(&r, bytes, file_size + 1) != RAY_OK);
    for (size_t i = 0; i < file_size; i++) {
        if (i >= RAY_COL_BLOCK_HEADER && i < dir) continue;
        memcpy(copy, bytes, file_size); copy[i] ^= 1;
        TEST_ASSERT_TRUE(ray_col_block_open(&r, copy, file_size) != RAY_OK);
    }
    /* Repair metadata CRCs to exercise structural checks, not only checksums. */
    const size_t offsets[] = {0, 19, 40, 47, 88,
        dir, dir + 8, dir + 16, dir + 24, dir + 28,
        dir + 32, dir + 33, dir + 34, dir + 48, dir + 64};
    for (size_t i = 0; i < sizeof(offsets) / sizeof(*offsets); i++) {
        memcpy(copy, bytes, file_size); copy[offsets[i]] ^= 0x80;
        rechecksum(copy);
        TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_ERR_CORRUPT);
    }
    memcpy(copy, bytes, file_size); copy[RAY_COL_BLOCK_HEADER] ^= 1;
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_OK);
    uint64_t row = 99, count = 99;
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 1, output, 64, &row, &count), RAY_OK);
    TEST_ASSERT_EQ_I(row, 8); TEST_ASSERT_EQ_I(count, 8);
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 0, output, 64, &row, &count), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(row, 8);
    memcpy(copy, bytes, file_size); copy[dir + 44] ^= 1; rechecksum(copy);
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 0, output, 64, &row, &count), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 0, output, 63, &row, &count), RAY_ERR_LIMIT);
    memcpy(copy, bytes, file_size); copy[RAY_COL_BLOCK_HEADER] = 0;
    put(copy + dir + 40, ray_crc32(0, copy + RAY_COL_BLOCK_HEADER,
        (size_t)get(copy + dir + 24, 4)), 4);
    rechecksum(copy);
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_read(&r, 0, output, 64, &row, &count), RAY_ERR_CORRUPT);
    PASS();
}

static test_result_t io_failure(void) {
    TEST_ASSERT_EQ_I(fclose(file), 0);
    file = fopen(path, "rb");
    TEST_ASSERT_NOT_NULL(file);
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 0), RAY_ERR_IO);
    TEST_ASSERT_TRUE(writer.output == NULL && writer.directory == NULL && writer.scratch == NULL);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, input, 1), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_ERR_DOMAIN);
    ray_col_block_abort(&writer);
    PASS();
}

static test_result_t fallback_and_bits(void) {
    /* Arbitrary bits preserve float NaNs, signed zero and null sentinels
     * without numeric conversion, regardless of the selected codec. */
    const uint64_t values[] = {UINT64_C(0x8000000000000000), UINT64_C(0x7ff8000000000042),
        UINT64_C(0xfff0000000000000), UINT64_MAX};
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_F64, 8, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 4), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    ray_col_block_reader_t r;
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    for (uint64_t i = 0; i < 4; i++) {
        uint64_t row, count;
        TEST_ASSERT_EQ_I(ray_col_block_read(&r, i, output, sizeof(output), &row, &count), RAY_OK);
        TEST_ASSERT_EQ_I(row, i); TEST_ASSERT_EQ_I(count, 1);
        TEST_ASSERT_TRUE(memcmp(output, values + i, 8) == 0);
    }
    ray_t* vec = ray_col_block_materialize(&r, 0, 4, sizeof(values), NULL, 0);
    TEST_ASSERT_FALSE(RAY_IS_ERR(vec));
    TEST_ASSERT_TRUE(memcmp(ray_data(vec), values, sizeof(values)) == 0);
    TEST_ASSERT_FALSE(ray_vec_is_null(vec, 0)); TEST_ASSERT_TRUE(ray_vec_is_null(vec, 1));
    ray_release(vec);
    reset_file();
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_U8, 1, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 8), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    for (uint64_t i = 0; i < r.blocks; i++)
        TEST_ASSERT_EQ_I(bytes[r.directory + i * RAY_COL_BLOCK_ENTRY + 32], 0);
    PASS();
}

static test_result_t ranges(void) {
    int64_t values[24];
    uint8_t scratch[65];
    for (unsigned i = 0; i < 24; i++) values[i] = i * 100;
    for (uint8_t codec = 0; codec < 2; codec++) {
        reset_file();
        TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, codec, 0), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 3), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values + 3, 21), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
        snapshot();
        ray_col_block_reader_t r;
        TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
        for (uint64_t start = 0; start <= 24; start++) {
            for (uint64_t count = 0; count <= 24 - start; count++) {
                size_t out_bytes, scratch_bytes;
                TEST_ASSERT_EQ_I(ray_col_block_range_size(&r, start, count, &out_bytes, &scratch_bytes), RAY_OK);
                TEST_ASSERT_EQ_I(out_bytes, count * 8);
                TEST_ASSERT_TRUE(scratch_bytes <= 64);
                memset(output, 0xa5, sizeof(output)); memset(scratch, 0xa5, sizeof(scratch));
                TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, start, count, output, out_bytes,
                    scratch_bytes ? scratch : NULL, scratch_bytes), RAY_OK);
                TEST_ASSERT_TRUE(memcmp(output, values + start, out_bytes) == 0);
                TEST_ASSERT_EQ_I(output[out_bytes], 0xa5);
                TEST_ASSERT_EQ_I(scratch[scratch_bytes], 0xa5);
            }
        }
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 0, 24, output, 192, NULL, 0), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 24, 0, NULL, 0, NULL, 0), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 25, 0, NULL, 0, NULL, 0), RAY_ERR_RANGE);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 1, UINT64_MAX, output, sizeof(output), scratch, 64), RAY_ERR_RANGE);
        memset(output, 0xa5, sizeof(output));
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 2, 16, output, sizeof(output), scratch, 63), RAY_ERR_LIMIT);
        TEST_ASSERT_EQ_I(output[0], 0xa5);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 2, 16, output, 127, scratch, 64), RAY_ERR_LIMIT);
        TEST_ASSERT_EQ_I(output[0], 0xa5);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 2, 16, output, 128, NULL, 64), RAY_ERR_DOMAIN);
        /* A damaged unselected block must not be touched, even at its boundary. */
        bytes[RAY_COL_BLOCK_HEADER] ^= 1;
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 3, 8, output, 64, NULL, 0), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_read_range(&r, 2, 1, output, 8, scratch, 64), RAY_ERR_CORRUPT);
    }
    PASS();
}

static test_result_t materialize(void) {
    const int64_t values[] = {1, INT64_MIN, 3, 4, 5, 6, 7, 8};
    uint8_t scratch[64];
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 8), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    ray_col_block_reader_t r;
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    ray_t* v = ray_col_block_materialize(&r, 1, 2, 15, scratch, sizeof(scratch));
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "limit"); ray_error_free(v);
    v = ray_col_block_materialize(&r, 1, 2, 16, scratch, 63);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "limit"); ray_error_free(v);
    v = ray_col_block_materialize(&r, 1, 2, 16, NULL, 64);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "domain"); ray_error_free(v);
    v = ray_col_block_materialize(&r, 9, 0, 0, NULL, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "range"); ray_error_free(v);
    v = ray_col_block_materialize(&r, 8, 0, 0, NULL, 0);
    TEST_ASSERT_FALSE(RAY_IS_ERR(v)); TEST_ASSERT_EQ_I(v->len, 0); ray_release(v);
    v = ray_col_block_materialize(&r, 1, 2, 16, scratch, 64);
    TEST_ASSERT_FALSE(RAY_IS_ERR(v));
    TEST_ASSERT_TRUE(ray_vec_is_null(v, 0)); TEST_ASSERT_FALSE(ray_vec_is_null(v, 1));
    memset(bytes, 0, file_size); memset(scratch, 0, sizeof(scratch));
    TEST_ASSERT_TRUE(memcmp(ray_data(v), values + 1, 16) == 0);
    ray_release(v);
    snapshot();
    TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
    bytes[RAY_COL_BLOCK_HEADER] ^= 1;
    v = ray_col_block_materialize(&r, 0, 8, 64, NULL, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "corrupt"); ray_error_free(v);
    PASS();
}

static int64_t mapped_bytes(void) {
    int64_t current, peak;
    ray_sys_get_mapped(&current, &peak);
    return current;
}

static test_result_t file_lifetime(void) {
    const int64_t values[] = {1, INT64_MIN, 3, 4, 5, 6, 7, 8};
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 8), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    snapshot();
    int64_t baseline = mapped_bytes();
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &owned_file), RAY_OK);
    TEST_ASSERT_EQ_I(mapped_bytes(), baseline + (int64_t)file_size);
    const ray_col_block_reader_t* r = ray_col_block_file_reader(owned_file);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_EQ_I(r->rows, 8);
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &owned_file), RAY_ERR_DOMAIN);
    TEST_ASSERT_TRUE(ray_col_block_file_reader(owned_file) == r);
    ray_t* v = ray_col_block_load_range(path, 1, 2, 16, 64);
    TEST_ASSERT_FALSE(RAY_IS_ERR(v)); TEST_ASSERT_TRUE(ray_vec_is_null(v, 0));
    TEST_ASSERT_EQ_I(mapped_bytes(), baseline + (int64_t)file_size);
    ray_col_block_file_close(&owned_file);
    TEST_ASSERT_TRUE(owned_file == NULL);
    TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    ray_col_block_file_close(&owned_file); ray_col_block_file_close(NULL);
    TEST_ASSERT_TRUE(ray_col_block_file_reader(NULL) == NULL);
    TEST_ASSERT_TRUE(memcmp(ray_data(v), values + 1, 16) == 0); ray_release(v);

    /* On POSIX an unlinked inode stays alive through its mapping. A new file
     * at the same path must not change a previously opened handle's data. */
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &owned_file), RAY_OK);
    TEST_ASSERT_EQ_I(unlink(path), 0);
    TEST_ASSERT_EQ_I(fclose(file), 0); file = fopen(path, "w+b");
    TEST_ASSERT_NOT_NULL(file);
    int64_t replacement[8] = {0};
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 1), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, replacement, 8), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    r = ray_col_block_file_reader(owned_file);
    v = ray_col_block_materialize(r, 0, 8, 64, NULL, 0);
    TEST_ASSERT_FALSE(RAY_IS_ERR(v)); TEST_ASSERT_TRUE(memcmp(ray_data(v), values, 64) == 0);
    ray_release(v);
    v = ray_col_block_load_range(path, 0, 8, 64, 0);
    TEST_ASSERT_FALSE(RAY_IS_ERR(v)); TEST_ASSERT_TRUE(memcmp(ray_data(v), replacement, 64) == 0);
    ray_release(v); ray_col_block_file_close(&owned_file);
    TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    PASS();
}

static test_result_t file_errors(void) {
    int64_t baseline = mapped_bytes();
    TEST_ASSERT_EQ_I(ray_col_block_file_open(NULL, &owned_file), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_col_block_file_open("", &owned_file), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, NULL), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &owned_file), RAY_ERR_IO); /* empty file */
    const int64_t values[8] = {0};
    TEST_ASSERT_EQ_I(ray_col_block_begin(&writer, file, RAY_I64, 64, 1, 0), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 8), RAY_OK);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
    const uint64_t starts[] = {0, 1, 9};
    const size_t payloads[] = {7, 8, 8}, scratches[] = {64, 63, 64};
    const char* errors[] = {"limit", "limit", "range"};
    for (unsigned i = 0; i < 3; i++) {
        ray_t* v = ray_col_block_load_range(path, starts[i], 1, payloads[i], scratches[i]);
        TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), errors[i]);
        ray_error_free(v); TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    }
    /* Valid metadata, damaged compressed data: load must release its mapping. */
    TEST_ASSERT_EQ_I(fseek(file, RAY_COL_BLOCK_HEADER, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(0, file) != EOF); TEST_ASSERT_EQ_I(fflush(file), 0);
    ray_t* v = ray_col_block_load_range(path, 0, 8, 64, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "corrupt");
    ray_error_free(v); TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    TEST_ASSERT_EQ_I(fseek(file, 0, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(1, file) != EOF); TEST_ASSERT_EQ_I(fflush(file), 0);
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &owned_file), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(owned_file == NULL); TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    ray_t* raw = ray_vec_new(RAY_I64, 8);
    TEST_ASSERT_FALSE(RAY_IS_ERR(raw)); raw->len = 8; memcpy(ray_data(raw), values, 64);
    TEST_ASSERT_EQ_I(ray_col_save(raw, path), RAY_OK); ray_release(raw);
    v = ray_col_block_load_range(path, 0, 8, 64, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "version");
    ray_error_free(v); TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    TEST_ASSERT_EQ_I(unlink(path), 0);
    v = ray_col_block_load_range(path, 0, 1, 8, 64);
    TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "io");
    ray_error_free(v); TEST_ASSERT_EQ_I(mapped_bytes(), baseline);
    PASS();
}

static test_result_t symbols(void) {
    ray_sym_domain_t* dom = ray_sym_domain_new();
    TEST_ASSERT_NOT_NULL(dom);
    TEST_ASSERT_EQ_I(ray_sym_domain_intern(dom, "value", 5), 1);
    uint64_t values[24];
    for (unsigned i = 0; i < 24; i++) values[i] = i % 3 ? 1 : 0;
    uint8_t scratch[64];
    ray_col_block_reader_t r;
    TEST_ASSERT_EQ_I(ray_col_block_begin_sym(&writer, file, 64, 1, 7, 0, 99), RAY_ERR_RANGE);
    for (uint8_t codec = 0; codec <= 1; codec++) {
        reset_file();
        TEST_ASSERT_EQ_I(ray_col_block_begin_sym(&writer, file, 64, codec, 7, 2, 99), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 24), RAY_OK);
        TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_OK);
        snapshot();
        TEST_ASSERT_EQ_I(ray_col_block_open(&r, bytes, file_size), RAY_OK);
        TEST_ASSERT_EQ_I(r.sym_count, 2); TEST_ASSERT_EQ_I(r.sym_crc, 99);
        ray_t* v = ray_col_block_materialize(&r, 2, 13, 104, scratch, 64);
        TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "domain"); ray_release(v);
        v = ray_col_block_materialize_dom(&r, 2, 13, 104, scratch, 64, dom);
        TEST_ASSERT_NOT_NULL(v); TEST_ASSERT_FALSE(RAY_IS_ERR(v));
        TEST_ASSERT_TRUE(ray_sym_vec_domain(v) == dom);
        TEST_ASSERT_EQ_I(v->attrs & RAY_SYM_W_MASK, RAY_SYM_W64);
        for (int64_t i = 0; i < v->len; i++) {
            TEST_ASSERT_EQ_I(ray_read_sym(ray_data(v), i, RAY_SYM, v->attrs), values[i + 2]);
            TEST_ASSERT_EQ_I(ray_str_len(ray_sym_vec_cell(v, i)), values[i + 2] ? 5 : 0);
        }
        ray_release(v);
        /* Metadata must bind a nonempty vocabulary even for an empty range. */
        memcpy(copy, bytes, file_size); put(copy + 88, 0, 8); rechecksum(copy);
        TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_ERR_CORRUPT);
        if (!codec) {
            /* Valid CRCs cannot make an out-of-domain cell safe to materialize. */
            memcpy(copy, bytes, file_size); put(copy + RAY_COL_BLOCK_HEADER, 2, 8);
            uint8_t* e = copy + get(copy + 56, 8);
            uint32_t crc = ray_crc32(0, copy + RAY_COL_BLOCK_HEADER, 64);
            put(e + 40, crc, 4); put(e + 44, crc, 4); rechecksum(copy);
            TEST_ASSERT_EQ_I(ray_col_block_open(&r, copy, file_size), RAY_OK);
            v = ray_col_block_materialize_dom(&r, 0, 8, 64, NULL, 0, dom);
            TEST_ASSERT_TRUE(RAY_IS_ERR(v)); TEST_ASSERT_STR_EQ(ray_err_code(v), "corrupt"); ray_release(v);
        }
    }
    reset_file();
    TEST_ASSERT_EQ_I(ray_col_block_begin_sym(&writer, file, 64, 1, 7, 2, 99), RAY_OK);
    values[0] = UINT64_MAX;
    TEST_ASSERT_EQ_I(ray_col_block_append(&writer, values, 24), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(ray_col_block_finish(&writer), RAY_ERR_CORRUPT);
    ray_sym_domain_release(dom);
    PASS();
}

const test_entry_t col_block_entries[] = {
    {"col_block/symbols", symbols, setup, teardown},
    {"col_block/roundtrip", roundtrip, setup, teardown},
    {"col_block/empty_and_bounds", empty_and_bounds, setup, teardown},
    {"col_block/corruption", corruption, setup, teardown},
    {"col_block/fallback_and_bits", fallback_and_bits, setup, teardown},
    {"col_block/io_failure", io_failure, setup, teardown},
    {"col_block/ranges", ranges, setup, teardown},
    {"col_block/materialize", materialize, setup, teardown},
    {"col_block/file_lifetime", file_lifetime, setup, teardown},
    {"col_block/file_errors", file_errors, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
