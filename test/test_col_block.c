/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/col_block.h"
#include "store/col.h"
#include "core/crc32.h"
#include "mem/heap.h"
#include "table/sym.h"
#include <stdlib.h>
#include <unistd.h>

static FILE* file;
static char path[64];
static ray_col_block_writer_t writer;
static uint8_t bytes[65536], copy[65536], input[16384], output[16384];
static size_t file_size;

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

const test_entry_t col_block_entries[] = {
    {"col_block/roundtrip", roundtrip, setup, teardown},
    {"col_block/empty_and_bounds", empty_and_bounds, setup, teardown},
    {"col_block/corruption", corruption, setup, teardown},
    {"col_block/fallback_and_bits", fallback_and_bits, setup, teardown},
    {"col_block/io_failure", io_failure, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
