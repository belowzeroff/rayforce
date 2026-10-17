/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
/* The table the golden block files in test/data/col_block were written from,
 * and a value-by-value column comparison. */
#ifndef RAY_TEST_GOLDEN_TABLE_H
#define RAY_TEST_GOLDEN_TABLE_H
#include "test.h"
#include "table/sym.h"
#include <string.h>

static inline ray_t* golden_table(void) {
    const int64_t n = 16;
    static const char* const syms[] = {"AAPL", "MSFT", "IBM"};
    ray_t* cols[13];
    int8_t types[] = {RAY_BOOL, RAY_U8, RAY_I16, RAY_I32, RAY_I64, RAY_F32, RAY_F64,
                      RAY_DATE, RAY_TIME, RAY_TIMESTAMP, RAY_GUID};
    for (int c = 0; c < 11; c++) { cols[c] = ray_vec_new(types[c], n); cols[c]->len = n; }
    cols[11] = ray_sym_vec_new(RAY_SYM_W64, n); cols[11]->len = n;
    cols[12] = ray_vec_new(RAY_STR, n);
    for (int64_t i = 0; i < n; i++) {
        ((uint8_t*)ray_data(cols[0]))[i] = i % 3 != 0;
        ((uint8_t*)ray_data(cols[1]))[i] = (uint8_t)(i * 37);
        ((int16_t*)ray_data(cols[2]))[i] = (int16_t)((i - 8) * 300);
        ((int32_t*)ray_data(cols[3]))[i] = (int32_t)((i - 8) * 70000);
        ((int64_t*)ray_data(cols[4]))[i] = (i - 8) * 625000000LL;
        ((float*)ray_data(cols[5]))[i] = (float)(i - 8) * 0.75f;
        ((double*)ray_data(cols[6]))[i] = (double)(i - 8) * 1.25;
        ((int32_t*)ray_data(cols[7]))[i] = (int32_t)(8766 + i * 3);
        ((int32_t*)ray_data(cols[8]))[i] = (int32_t)(i * 3600000 + i);
        ((int64_t*)ray_data(cols[9]))[i] = 1000 * i - 8000;
        uint8_t* g = (uint8_t*)ray_data(cols[10]) + 16 * i;
        for (int k = 0; k < 16; k++) g[k] = (uint8_t)(i * 16 + k);
        ((int64_t*)ray_data(cols[11]))[i] = ray_sym_intern(syms[i % 3], strlen(syms[i % 3]));
        char text[16];
        memset(text, 'a' + (int)i, sizeof(text));
        cols[12] = ray_str_vec_append(cols[12], text, (size_t)(i % 4 ? i : 0));
    }
    static const char* const names[] = {"b", "u", "h", "i", "l", "e", "f", "d", "tm", "p", "g", "s", "c"};
    ray_t* t = ray_table_new(13);
    for (int c = 0; c < 13; c++) {
        t = ray_table_add_col(t, ray_sym_intern(names[c], strlen(names[c])), cols[c]);
        ray_release(cols[c]);
    }
    return t;
}

/* Every value of a against e, which has the same type and length. */
static inline test_result_t golden_column_equal(ray_t* a, ray_t* e) {
    TEST_ASSERT_NOT_NULL(a); TEST_ASSERT_FALSE(RAY_IS_ERR(a));
    TEST_ASSERT_EQ_I(a->type, e->type);
    TEST_ASSERT_EQ_I(a->len, e->len);
    for (int64_t i = 0; i < e->len; i++) {
        if (e->type == RAY_STR) {
            size_t al, el;
            const char* as = ray_str_vec_get(a, i, &al);
            const char* es = ray_str_vec_get(e, i, &el);
            TEST_ASSERT_EQ_I(al, el);
            TEST_ASSERT_TRUE(!el || !memcmp(as, es, el));
        } else if (e->type == RAY_SYM) {
            ray_t* ac = ray_sym_vec_cell(a, i);
            ray_t* ec = ray_sym_vec_cell(e, i);
            TEST_ASSERT_TRUE(ac && ec);
            TEST_ASSERT_EQ_I(ray_str_len(ac), ray_str_len(ec));
            TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(ac), ray_str_ptr(ec), ray_str_len(ec)));
        } else {
            size_t w = ray_elem_size(e->type);
            TEST_ASSERT_TRUE(!memcmp((uint8_t*)ray_data(a) + i * w, (uint8_t*)ray_data(e) + i * w, w));
        }
    }
    PASS();
}

/* Every column of expected, by name, in actual. */
static inline test_result_t golden_columns_equal(ray_t* actual, ray_t* expected) {
    TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
    for (int64_t c = 0; c < ray_table_ncols(expected); c++) {
        ray_t* a = ray_table_get_col(actual, ray_table_col_name(expected, c));
        test_result_t r = golden_column_equal(a, ray_table_get_col_idx(expected, c));
        if (r.status != TEST_PASS) return r;
    }
    PASS();
}
#endif
