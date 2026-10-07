/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/block_store.h"
#include "store/splay.h"
#include "store/col_block.h"
#include "lang/eval.h"
#include "lang/env.h"
#include "ops/block_query.h"
#include "table/sym.h"
#include "table/domain.h"
#include <stdlib.h>
#include <unistd.h>

static ray_runtime_t* runtime;
static char root[64];
static void setup(void) {
    runtime = ray_runtime_create(0, NULL);
    strcpy(root, "/tmp/ray-block-source-XXXXXX");
    if (!runtime || !mkdtemp(root)) ray_test_fatal("block source setup");
}
static void teardown(void) {
    ray_eval_set_restricted(false);
    ray_runtime_destroy(runtime);
    ray_test_rm_rf(root);
}
static void fixture(unsigned rows, bool shadow) {
    ray_t* t = ray_table_new(4);
    ray_t* id = ray_vec_new(RAY_I64, rows); id->len = rows;
    ray_t* x = ray_vec_new(RAY_I64, rows); x->len = rows;
    ray_t* sy = ray_sym_vec_new(RAY_SYM_W64, rows); sy->len = rows;
    ray_t* str = ray_vec_new(RAY_STR, rows);
    for (unsigned i = 0; i < rows; i++) {
        ((int64_t*)ray_data(id))[i] = i;
        ((int64_t*)ray_data(x))[i] = i % 5;
        ((int64_t*)ray_data(sy))[i] = ray_sym_intern(i % 2 ? "alpha" : "beta", i % 2 ? 5 : 4);
        const char* text = i % 3 ? "abcdefghijklm" : "abc\0efghijklm";
        str = ray_str_vec_append(str, i % 2 ? text : "", i % 2 ? 13 : 0);
    }
    t = ray_table_add_col(t, ray_sym_intern("id", 2), id);
    t = ray_table_add_col(t, ray_sym_intern("x", 1), x);
    t = ray_table_add_col(t, ray_sym_intern("s", 1), str);
    t = ray_table_add_col(t, ray_sym_intern("sy", 2), sy);
    if (shadow) t = ray_table_add_col(t, ray_sym_intern(">", 1), x);
    ray_release(id); ray_release(x); ray_release(str); ray_release(sy);
    ray_block_store_options_t options = {256, 1, false};
    if (ray_block_store_save(t, root, &options)) ray_test_fatal("block source save");
    if (ray_env_set(ray_sym_intern("raw", 3), t)) ray_test_fatal("block source bind");
    ray_release(t);
    char command[256];
    snprintf(command, sizeof(command), "(set source (.db.block.scan \"%s\" 1048576))", root);
    ray_t* r = ray_eval_str(command);
    if (!r || RAY_IS_ERR(r)) ray_test_fatal("block source descriptor");
    ray_release(r);
}

static test_result_t select_parity(void) {
    fixture(4103, false);
    const char* clauses[] = {
        "out: id text: s symbol: sy",
        "where: (== x 2) out: id text: s symbol: sy",
        "where: (and (> x 1) (< x 4)) out: id text: s symbol: sy",
        "where: (== x 100) out: id text: s symbol: sy",
        "where: (!= x 0Nl) out: id text: s symbol: sy"
    };
    for (unsigned c = 0; c < sizeof(clauses) / sizeof(*clauses); c++) {
        char command[512];
        snprintf(command, sizeof(command), "(select {from: raw %s})", clauses[c]);
        ray_t* expected = ray_eval_str(command);
        snprintf(command, sizeof(command), "(select {from: source %s})", clauses[c]);
        ray_t* actual = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
        TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
        TEST_ASSERT_EQ_I(ray_table_nrows(actual), ray_table_nrows(expected));
        TEST_ASSERT_EQ_I(ray_table_ncols(actual), 3);
        for (int64_t j = 0; j < 3; j++) {
            TEST_ASSERT_EQ_I(ray_table_col_name(actual, j), ray_table_col_name(expected, j));
            ray_t* a = ray_table_get_col_idx(actual, j); ray_t* e = ray_table_get_col_idx(expected, j);
            for (int64_t i = 0; i < a->len; i++) {
                if (!j) TEST_ASSERT_EQ_I(((int64_t*)ray_data(a))[i], ((int64_t*)ray_data(e))[i]);
                else if (j == 1) {
                    size_t na, ne; const char* sa = ray_str_vec_get(a, i, &na);
                    const char* se = ray_str_vec_get(e, i, &ne);
                    TEST_ASSERT_EQ_I(na, ne); TEST_ASSERT_TRUE(!memcmp(sa, se, na));
                } else {
                    ray_t* sa = ray_sym_vec_cell(a, i); ray_t* se = ray_sym_vec_cell(e, i);
                    TEST_ASSERT_EQ_I(ray_str_len(sa), ray_str_len(se));
                    TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(sa), ray_str_ptr(se), ray_str_len(sa)));
                }
            }
        }
        ray_release(expected); ray_release(actual);
    }
    PASS();
}

static test_result_t select_rejections(void) {
    fixture(16, false);
    const char* commands[] = {
        "(select {from: source})", "(select {from: source n: (count id) id: id})",
        "(select {from: source id: id take: 2})", "(select {from: source id: id by: x})",
        "(select {from: source id: id asc: id})", "(select {from: source id: id where: (> x (+ 1 1))})",
        "(select {from: source id: id where: (or (> x 1) (< x 4))})",
        "(select {from: source id: id where: (> x 1.5)})",
        "(select {from: source id: id where: (> 1 x)})",
        "(select {from: source id: id where: (> x missing)})"
    };
    for (unsigned i = 0; i < sizeof(commands) / sizeof(*commands); i++) {
        ray_t* r = ray_eval_str(commands[i]);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    ray_t* saved = ray_env_get(ray_sym_intern(">", 1)); ray_retain(saved);
    ray_t* replacement = ray_env_get(ray_sym_intern("<", 1));
    TEST_ASSERT_EQ_I(ray_env_set(ray_sym_intern(">", 1), replacement), RAY_OK);
    ray_t* r = ray_eval_str("(select {from: source id: id where: (> x 1)})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    TEST_ASSERT_EQ_I(ray_env_set(ray_sym_intern(">", 1), saved), RAY_OK); ray_release(saved);
    ray_eval_set_restricted(true);
    r = ray_eval_str("(select {from: source id: id})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "access"); ray_release(r);
    ray_eval_set_restricted(false);
    fixture(16, true);
    r = ray_eval_str("(select {from: source id: id where: (> x 1)})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    PASS();
}

static test_result_t select_limits(void) {
    fixture(16, false);
    for (unsigned i = 0; i < 2; i++) {
        char command[256];
        snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" %u) id: id})", root, 128 - i);
        ray_t* r = ray_eval_str(command);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 16); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    /* A no-match query must not decode a damaged output payload. */
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/id", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER, SEEK_SET), 0);
    int b = fgetc(f); TEST_ASSERT_TRUE(b != EOF);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(b ^ 1, f) != EOF); TEST_ASSERT_EQ_I(fclose(f), 0);
    ray_t* r = ray_eval_str("(select {from: source id: id where: (== x 999)})");
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 0); ray_release(r);
    r = ray_eval_str("(select {from: source id: id})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "corrupt"); ray_release(r);
    PASS();
}

static test_result_t descriptor_validation(void) {
    fixture(16, false);
    const char* invalid[] = {
        "(.db.block.scan \"path\")", "(.db.block.scan \"\" 1)",
        "(.db.block.scan \"path\" 0)", "(.db.block.scan \"path\" -1)",
        "(.db.block.scan \"path\" 1.0)", "(.db.block.scan 1 1024)",
        "(select {from: {.hdb.block.source: \"path\"} x: x})",
        "(select {from: {.hdb.block.source: \"path\" .hdb.block.limit: -1} x: x})"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        ray_t* r = ray_eval_str(invalid[i]); TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    ray_t* r = ray_eval_str("(select {from: (.db.block.scan \"/missing-hdb-source\" 1) n: (sum (+ x 1))})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    ray_eval_set_restricted(true);
    r = ray_eval_str("(.db.block.scan \"path\" 100)");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "access"); ray_release(r);
    ray_eval_set_restricted(false);
    /* Eight pooled strings and sixteen descriptors: exact logical boundary. */
    for (unsigned i = 0; i < 2; i++) {
        char command[256];
        snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" %u) text: s})", root, 360 - i);
        r = ray_eval_str(command);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 16); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 40, 256};
    const char* output[] = {"id"}; ray_block_query_t* q = NULL;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &q), RAY_OK);
    ray_eval_request_interrupt();
    r = ray_block_query_next(q);
    ray_eval_clear_interrupt();
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "cancel"); ray_release(r);
    r = ray_block_query_next(q);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "domain"); ray_release(r);
    ray_block_query_close(&q);
    PASS();
}

static test_result_t builder_growth(void) {
    fixture(8207, false);
    ray_t* raw = ray_env_get(ray_sym_intern("raw", 3));
    ray_vec_set_null(ray_table_get_col_idx(raw, 0), 4096, true);
    ray_vec_set_null(ray_table_get_col_idx(raw, 3), 4097, true);
    ray_block_store_options_t options = {256, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(raw, root, &options), RAY_OK);
    ray_t* r = ray_eval_str("(select {from: source number: id text: s symbol: sy})");
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(ray_table_nrows(r), 8207);
    ray_t* numbers = ray_table_get_col_idx(r, 0);
    ray_t* strings = ray_table_get_col_idx(r, 1);
    ray_t* symbols = ray_table_get_col_idx(r, 2);
    TEST_ASSERT_TRUE(ray_vec_is_null(numbers, 4096));
    TEST_ASSERT_TRUE(ray_vec_is_null(symbols, 4097));
    TEST_ASSERT_TRUE(ray_sym_vec_domain(symbols) != ray_sym_runtime_domain());
    TEST_ASSERT_EQ_I(ray_sym_elem_size(symbols->type, symbols->attrs), 8);
    /* Returned vectors must survive publication/GC, not just query close. */
    for (unsigned i = 0; i < 3; i++) fixture(0, false);
    for (int64_t i = 0; i < 8207; i++) {
        TEST_ASSERT_EQ_I(((int64_t*)ray_data(numbers))[i], i == 4096 ? NULL_I64 : i);
        size_t n; const char* text = ray_str_vec_get(strings, i, &n);
        TEST_ASSERT_EQ_I(n, i % 2 ? 13 : 0);
        TEST_ASSERT_TRUE(!memcmp(text, i % 3 ? "abcdefghijklm" : "abc\0efghijklm", n));
        ray_t* symbol = ray_sym_vec_cell(symbols, i);
        const char* expected = i == 4097 ? "" : i % 2 ? "alpha" : "beta";
        TEST_ASSERT_EQ_I(ray_str_len(symbol), strlen(expected));
        TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(symbol), expected, strlen(expected)));
    }
    ray_release(r);
    r = ray_eval_str("(select {from: source number: id text: s symbol: sy})");
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(ray_table_nrows(r), 0);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(r, 0)->type, RAY_I64);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(r, 1)->type, RAY_STR);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(r, 2)->type, RAY_SYM);
    ray_release(r);
    PASS();
}

static test_result_t builder_partial_limit(void) {
    fixture(8207, false);
    /* First full batch fits; fail after it has been appended. Also exercise
     * cleanup after many single-row matching ranges, then exact total budget. */
    const unsigned total = 8207 * 32 + 4103 * 13;
    const unsigned limits[] = {4096 * 32 + 2048 * 13, 1000, total - 1, total};
    for (unsigned i = 0; i < 4; i++) {
        char command[512];
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" %u) %s number: id text: s symbol: sy})",
            root, limits[i], i == 1 ? "where: (== x 2)" : "");
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (i < 3) { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        else { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 8207); }
        ray_release(r);
    }
    PASS();
}

static test_result_t aggregate_parity(void) {
    fixture(8207, false);
    const char* clauses[] = {
        "n: (count id) total: (sum id) mean: (avg id) lo: (min id) hi: (max id) ns: (count s) ny: (count sy)",
        "where: (== x 2) n: (count id) total: (sum id) mean: (avg id) lo: (min id) hi: (max id)",
        "where: (and (> id 4090) (< id 8194)) n: (count id) total: (sum id) mean: (avg id)",
        "where: (> x 99) n: (count id) total: (sum id) mean: (avg id) lo: (min id) hi: (max id)",
        "id: (sum id) n: (count id) x: (avg x)",
        "n: (count id)"
    };
    for (unsigned mode = 0; mode < 5; mode++) {
        ray_t* raw = ray_env_get(ray_sym_intern("raw", 3));
        ray_t* col = ray_table_get_col_idx(raw, 0);
        if (mode == 1) {
            for (int64_t i = 0; i < col->len; i++) ((int64_t*)ray_data(col))[i] =
                i % 3 == 0 ? INT64_MAX : i % 3 == 1 ? INT64_MAX - 1 : -INT64_MAX;
            ray_vec_set_null(col, 4096, true);
        } else if (mode == 2) {
            memset(ray_data(col), 0, (size_t)col->len * 8);
            ((int64_t*)ray_data(col))[0] = INT64_MAX;
            ((int64_t*)ray_data(col))[4096] = 1;
        } else if (mode == 3) {
            for (int64_t i = 0; i < col->len; i++) ray_vec_set_null(col, i, true);
        } else if (mode == 4) fixture(0, false);
        if (mode < 4) {
            ray_block_store_options_t write = {256, 1, false};
            TEST_ASSERT_EQ_I(ray_block_store_save(raw, root, &write), RAY_OK);
        }
        for (unsigned c = 0; c < sizeof(clauses) / sizeof(*clauses); c++) {
            char command[768];
            snprintf(command, sizeof(command), "(select {from: raw %s})", clauses[c]);
            ray_t* expected = ray_eval_str(command);
            snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" 56) %s})", root, clauses[c]);
            ray_t* actual = ray_eval_str(command);
            TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
            if (mode == 2 && (c == 0 || c == 4)) {
                TEST_ASSERT_TRUE(RAY_IS_ERR(actual)); TEST_ASSERT_STR_EQ(ray_err_code(actual), "range");
                ray_release(actual); ray_release(expected); continue;
            }
            TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
            TEST_ASSERT_EQ_I(ray_table_nrows(actual), ray_table_nrows(expected));
            TEST_ASSERT_EQ_I(ray_table_ncols(actual), ray_table_ncols(expected));
            for (int64_t a = 0; a < ray_table_ncols(actual); a++) {
                ray_t* v = ray_table_get_col_idx(actual, a); ray_t* e = ray_table_get_col_idx(expected, a);
                TEST_ASSERT_EQ_I(ray_table_col_name(actual, a), ray_table_col_name(expected, a));
                TEST_ASSERT_EQ_I(v->type, e->type);
                TEST_ASSERT_EQ_I(v->len, e->len);
                if (!v->len) continue;
                TEST_ASSERT_EQ_I(ray_vec_is_null(v, 0), ray_vec_is_null(e, 0));
                if (!ray_vec_is_null(e, 0))
                    TEST_ASSERT_TRUE(!memcmp(ray_data(v), ray_data(e), ray_type_sizes[(uint8_t)e->type]));
            }
            ray_release(actual); ray_release(expected);
        }
    }
    PASS();
}

static test_result_t aggregate_types(void) {
    fixture(4103, false);
    const int8_t types[] = {RAY_BOOL, RAY_U8, RAY_I16, RAY_I32, RAY_DATE, RAY_TIME, RAY_TIMESTAMP};
    for (unsigned t = 0; t < sizeof(types); t++) {
        int8_t type = types[t];
        for (unsigned empty = 0; empty < 3; empty++) {
            ray_t* col = ray_vec_new(type, empty == 1 ? 0 : 4103); col->len = empty == 1 ? 0 : 4103;
            for (int64_t i = 0; i < col->len; i++) {
                switch (type) {
                case RAY_BOOL: ((uint8_t*)ray_data(col))[i] = i % 2; break;
                case RAY_U8: ((uint8_t*)ray_data(col))[i] = i % 201; break;
                case RAY_I16: ((int16_t*)ray_data(col))[i] = (int16_t)(i % 99 - 40); break;
                case RAY_I32: case RAY_DATE: case RAY_TIME:
                    ((int32_t*)ray_data(col))[i] = (int32_t)(i % 99 - 40); break;
                default: ((int64_t*)ray_data(col))[i] = i % 99 - 40; break;
                }
            }
            if (!empty && type != RAY_BOOL && type != RAY_U8) ray_vec_set_null(col, 4096, true);
            if (empty == 2 && type != RAY_BOOL && type != RAY_U8)
                for (int64_t i = 0; i < col->len; i++) ray_vec_set_null(col, i, true);
            ray_t* table = ray_table_new(1);
            table = ray_table_add_col(table, ray_sym_intern("v", 1), col); ray_release(col);
            TEST_ASSERT_EQ_I(ray_env_set(ray_sym_intern("raw", 3), table), RAY_OK);
            ray_block_store_options_t write = {256, 1, false};
            TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
            const char* extra = type == RAY_I16 || type == RAY_I32 ? "s: (sum v) a: (avg v)" : "";
            char command[512];
            snprintf(command, sizeof(command), "(select {from: raw n: (count v) lo: (min v) hi: (max v) %s})", extra);
            ray_t* expected = ray_eval_str(command);
            snprintf(command, sizeof(command), "(select {from: source n: (count v) lo: (min v) hi: (max v) %s})", extra);
            ray_t* actual = ray_eval_str(command);
            TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
            TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
            TEST_ASSERT_EQ_I(ray_table_nrows(actual), ray_table_nrows(expected));
            TEST_ASSERT_EQ_I(ray_table_ncols(actual), ray_table_ncols(expected));
            for (int64_t a = 0; a < ray_table_ncols(actual); a++) {
                ray_t* v = ray_table_get_col_idx(actual, a); ray_t* e = ray_table_get_col_idx(expected, a);
                TEST_ASSERT_EQ_I(v->type, e->type);
                TEST_ASSERT_EQ_I(v->len, e->len);
                if (!v->len) continue;
                TEST_ASSERT_EQ_I(ray_vec_is_null(v, 0), ray_vec_is_null(e, 0));
                if (!ray_vec_is_null(e, 0))
                    TEST_ASSERT_TRUE(!memcmp(ray_data(v), ray_data(e), ray_type_sizes[(uint8_t)e->type]));
            }
            ray_release(expected); ray_release(actual);
        }
    }
    PASS();
}

static test_result_t aggregate_admission(void) {
    fixture(16, false);
    const char* invalid[] = {
        "(select {from: source n: (sum s)})", "(select {from: source n: (min sy)})",
        "(select {from: source n: (avg s)})", "(select {from: source n: (sum (+ id 1))})",
        "(select {from: source id: id n: (count id)})", "(select {from: source n: (count id) by: x})"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        ray_t* r = ray_eval_str(invalid[i]);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    }
    for (unsigned i = 0; i < 2; i++) {
        char command[256];
        snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" %u) n: (count id) s: (sum id)})", root, 16 - i);
        ray_t* r = ray_eval_str(command);
        if (i) { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        else { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 1); }
        ray_release(r);
    }
    int64_t sum = ray_sym_intern("sum", 3);
    ray_t* saved = ray_env_get(sum); ray_retain(saved);
    TEST_ASSERT_EQ_I(ray_env_set(sum, ray_env_get(ray_sym_intern("count", 5))), RAY_OK);
    ray_t* r = ray_eval_str("(select {from: source s: (sum id)})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    TEST_ASSERT_EQ_I(ray_env_set(sum, saved), RAY_OK); ray_release(saved);
    ray_t* raw = ray_env_get(ray_sym_intern("raw", 3)); ray_retain(raw);
    raw = ray_table_add_col(raw, sum, ray_table_get_col_idx(raw, 0));
    ray_block_store_options_t write = {256, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(raw, root, &write), RAY_OK); ray_release(raw);
    r = ray_eval_str("(select {from: source s: (sum id)})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    PASS();
}

static test_result_t aggregate_corruption(void) {
    fixture(16, false);
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/id", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER, SEEK_SET), 0);
    int b = fgetc(f); TEST_ASSERT_TRUE(b != EOF);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(b ^ 1, f) != EOF); TEST_ASSERT_EQ_I(fclose(f), 0);
    ray_t* r = ray_eval_str("(select {from: source where: (> x 99) n: (count id) s: (sum id) a: (avg id)})");
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 0)))[0], 0);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 1)))[0], 0);
    TEST_ASSERT_TRUE(ray_vec_is_null(ray_table_get_col_idx(r, 2), 0)); ray_release(r);
    r = ray_eval_str("(select {from: source n: (count id)})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "corrupt"); ray_release(r);
    PASS();
}

const test_entry_t block_source_entries[] = {
    {"block_source/aggregate_corruption", aggregate_corruption, setup, teardown},
    {"block_source/aggregate_parity", aggregate_parity, setup, teardown},
    {"block_source/aggregate_types", aggregate_types, setup, teardown},
    {"block_source/aggregate_admission", aggregate_admission, setup, teardown},
    {"block_source/builder_growth", builder_growth, setup, teardown},
    {"block_source/builder_partial_limit", builder_partial_limit, setup, teardown},
    {"block_source/select_parity", select_parity, setup, teardown},
    {"block_source/select_rejections", select_rejections, setup, teardown},
    {"block_source/select_limits", select_limits, setup, teardown},
    {"block_source/descriptor_validation", descriptor_validation, setup, teardown},
    {NULL, NULL, NULL, NULL}
};
