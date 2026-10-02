/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "test.h"
#include "core/snappy.h"
#include <stdlib.h>

static ray_snappy_workspace_t* workspace;
static uint8_t *input, *encoded, *decoded;
#define SNAPPY_TEST_SIZE (1024u * 1024u)

static void snappy_setup(void) {
    workspace = malloc(sizeof(*workspace));
    input = malloc(SNAPPY_TEST_SIZE);
    encoded = malloc(ray_snappy_compress_bound(SNAPPY_TEST_SIZE) + 1);
    decoded = malloc(SNAPPY_TEST_SIZE + 1);
    if (!workspace || !input || !encoded || !decoded)
        ray_test_fatal("snappy test allocation failed");
}

static void snappy_teardown(void) {
    free(workspace); free(input); free(encoded); free(decoded);
    workspace = NULL; input = encoded = decoded = NULL;
}

static test_result_t test_snappy_bounds(void) {
    size_t bound = ray_snappy_compress_bound(1);
    TEST_ASSERT_TRUE(bound > 1);
    TEST_ASSERT_EQ_I(ray_snappy_compress_bound(SIZE_MAX), 0);
#if SIZE_MAX > UINT32_MAX
    TEST_ASSERT_TRUE(ray_snappy_compress_bound(UINT32_MAX) > UINT32_MAX);
    TEST_ASSERT_EQ_I(ray_snappy_compress_bound((size_t)UINT32_MAX + 1), 0);
#else
    TEST_ASSERT_EQ_I(ray_snappy_compress_bound(UINT32_MAX), 0);
#endif
    input[0] = 42;
    memset(encoded, 0xa5, bound);
    TEST_ASSERT_EQ_I(ray_snappy_compress(input, 1, encoded, bound - 1, workspace), 0);
    for (size_t i = 0; i < bound; i++) TEST_ASSERT_EQ_I(encoded[i], 0xa5);
    TEST_ASSERT_EQ_I(ray_snappy_compress(NULL, 1, encoded, bound, workspace), 0);
    TEST_ASSERT_EQ_I(ray_snappy_compress(input, 1, NULL, bound, workspace), 0);
    TEST_ASSERT_EQ_I(ray_snappy_compress(input, 1, encoded, bound, NULL), 0);
    TEST_ASSERT_EQ_I(ray_snappy_compress(input, SIZE_MAX, encoded, bound, workspace), 0);
    TEST_ASSERT_EQ_I(ray_snappy_compress(NULL, 0, encoded, bound, workspace), 1);
    TEST_ASSERT_EQ_I(encoded[0], 0);
    TEST_ASSERT_TRUE(ray_snappy_decompress(encoded, 1, NULL, 0));
    TEST_ASSERT_FALSE(ray_snappy_decompress(NULL, 0, NULL, 0));
    TEST_ASSERT_FALSE(ray_snappy_decompress(encoded, 0, decoded, 0));
    TEST_ASSERT_FALSE(ray_snappy_decompress(encoded, 1, NULL, 1));
    TEST_ASSERT_FALSE(ray_snappy_decompress(encoded, 1, decoded, SIZE_MAX));
    PASS();
}

static uint32_t snappy_random(uint32_t* state) {
    *state ^= *state << 13; *state ^= *state >> 17; *state ^= *state << 5;
    return *state;
}

static test_result_t test_snappy_roundtrip(void) {
    const size_t lengths[] = {0, 1, 2, 3, 4, 5, 59, 60, 61, 63, 64, 65,
        127, 128, 255, 256, 257, 16383, 16384, 65535, 65536, 65537,
        262144, SNAPPY_TEST_SIZE};
    for (unsigned pattern = 0; pattern < 5; pattern++) {
        uint32_t rng = 0x12345678;
        for (size_t i = 0; i < SNAPPY_TEST_SIZE; i++) {
            if (pattern == 0) input[i] = 0;
            else if (pattern == 1) input[i] = (uint8_t)(i % 7);
            else if (pattern == 2) input[i] = (uint8_t)snappy_random(&rng);
            else if (pattern == 3) input[i] = (uint8_t)((uint64_t)(i / 8) >> ((i % 8) * 8));
            else input[i] = (i % 256 < 128) ? (uint8_t)snappy_random(&rng) : 0;
        }
        for (size_t k = 0; k < sizeof(lengths) / sizeof(*lengths); k++) {
            size_t n = lengths[k], bound = ray_snappy_compress_bound(n);
            encoded[bound] = decoded[n] = 0xa5;
            size_t written = ray_snappy_compress(input, n, encoded, bound, workspace);
            TEST_ASSERT_TRUE(written > 0 && written <= bound);
            TEST_ASSERT_EQ_I(encoded[bound], 0xa5);
            TEST_ASSERT_TRUE(ray_snappy_decompress(encoded, written, decoded, n));
            TEST_ASSERT_TRUE(memcmp(input, decoded, n) == 0);
            TEST_ASSERT_EQ_I(decoded[n], 0xa5);
            if (pattern < 2 && n >= 1024) TEST_ASSERT_TRUE(written < n / 4);
            TEST_ASSERT_FALSE(ray_snappy_decompress(encoded, written, decoded, n + 1));
            encoded[written] = 0;
            TEST_ASSERT_FALSE(ray_snappy_decompress(encoded, written + 1, decoded, n));
        }
    }
    PASS();
}

static test_result_t test_snappy_malformed(void) {
    const uint8_t good[] = {20, 4, 'a', 'b', 1, 2, 14, 2, 0, 39, 2, 0, 0, 0};
    TEST_ASSERT_TRUE(ray_snappy_decompress(good, sizeof(good), decoded, 20));
    for (size_t i = 0; i < sizeof(good); i++)
        TEST_ASSERT_FALSE(ray_snappy_decompress(good, i, decoded, 20));
    const uint8_t bad[][12] = {
        {0x80, 0x80, 0x80, 0x80, 0x10}, /* length > uint32 */
        {0x80, 0x80, 0x80, 0x80, 0x80, 0}, /* overlong preamble */
        {4, 1, 0}, /* copy before any output / zero offset */
        {4, 0, 'a', 2, 2, 0}, /* backreference before start */
        {4, 0, 'a', 14, 1, 0}, /* copy exceeds output */
        {4, 16, 'a', 'b', 'c', 'd', 'e'}, /* literal exceeds output */
        {4, 252, 255, 255, 255, 255}, /* literal length 2^32 */
        {4, 240}, {4, 244, 0}, {4, 248, 0, 0}, {4, 252, 0, 0, 0},
        {4, 0, 'a', 3, 1, 0, 0}, /* truncated COPY_4 */
    };
    const size_t lengths[] = {5, 6, 3, 6, 6, 7, 6, 2, 3, 4, 5, 7};
    for (size_t i = 0; i < sizeof(lengths) / sizeof(*lengths); i++)
        TEST_ASSERT_FALSE(ray_snappy_decompress(bad[i], lengths[i], decoded, 4));
    /* All extended literal-length widths, even when a small value fits. */
    for (unsigned width = 1; width <= 4; width++) {
        uint8_t literal[10] = {4, (uint8_t)((59 + width) << 2), 3, 0, 0, 0};
        memcpy(literal + 2 + width, "abcd", 4);
        TEST_ASSERT_TRUE(ray_snappy_decompress(literal, 6 + width, decoded, 4));
        TEST_ASSERT_TRUE(memcmp(decoded, "abcd", 4) == 0);
    }
    PASS();
}

static test_result_t test_snappy_large_offset(void) {
    /* A conforming decoder must accept COPY_4 beyond the encoder's window. */
    size_t n = 70004, at = 0;
    uint32_t v = (uint32_t)n;
    do {
        uint8_t b = (uint8_t)(v & 127); v >>= 7;
        encoded[at++] = (uint8_t)(b | (v ? 128 : 0));
    } while (v);
    encoded[at++] = 248; /* literal of 70000 bytes, 3-byte length-minus-one */
    v = 69999;
    for (unsigned i = 0; i < 3; i++) { encoded[at++] = (uint8_t)v; v >>= 8; }
    for (size_t i = 0; i < 70000; i++) encoded[at++] = (uint8_t)(i * 17);
    encoded[at++] = 15; /* COPY_4, four bytes, offset 70000 */
    v = 70000;
    for (unsigned i = 0; i < 4; i++) { encoded[at++] = (uint8_t)v; v >>= 8; }
    TEST_ASSERT_TRUE(ray_snappy_decompress(encoded, at, decoded, n));
    for (size_t i = 0; i < 70000; i++) TEST_ASSERT_EQ_I(decoded[i], (uint8_t)(i * 17));
    TEST_ASSERT_TRUE(memcmp(decoded, decoded + 70000, 4) == 0);
    PASS();
}

const test_entry_t snappy_entries[] = {
    {"snappy/bounds", test_snappy_bounds, snappy_setup, snappy_teardown},
    {"snappy/roundtrip", test_snappy_roundtrip, snappy_setup, snappy_teardown},
    {"snappy/malformed", test_snappy_malformed, snappy_setup, snappy_teardown},
    {"snappy/large_offset", test_snappy_large_offset, snappy_setup, snappy_teardown},
    {NULL, NULL, NULL, NULL},
};
