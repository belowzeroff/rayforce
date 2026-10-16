/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/block_store.h"
#include "store/splay.h"
#include "store/col.h"
#include "store/col_block.h"
#include "lang/eval.h"
#include "lang/env.h"
#include "ops/block_query.h"
#include "ops/block_source.h"
#include "table/sym.h"
#include "table/domain.h"
#include "store/part.h"
#include "io/csv.h"
#include "core/runtime.h"
#include "mem/heap.h"
#include "ops/ops.h"
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

static void make_column_raw(const char* name) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("block source resolve");
    int n = snprintf(path, sizeof(path), "%s/%s", dir, name);
    if (n < 0 || (size_t)n >= sizeof(path)) ray_test_fatal("block source column path");
    ray_t* table = ray_env_get(ray_sym_intern("raw", 3));
    ray_t* col = ray_table_get_col(table, ray_sym_intern(name, strlen(name)));
    ray_err_t err;
    if (col->type == RAY_SYM) {
        char sym_path[1100];
        n = snprintf(sym_path, sizeof(sym_path), "%s/.sym", dir);
        if (n < 0 || (size_t)n >= sizeof(sym_path)) ray_test_fatal("block source sym path");
        ray_sym_domain_t* dom = ray_sym_domain_open(sym_path);
        if (!dom) ray_test_fatal("block source sym domain");
        err = ray_col_save_sym_encoded(col, path, dom, false);
        ray_sym_domain_release(dom);
    } else err = ray_col_save(col, path);
    if (err) ray_test_fatal("block source raw column");
}

static test_result_t mixed_format_parity(void) {
    fixture(4103, false);
    make_column_raw("x");
    make_column_raw("s");
    make_column_raw("sy");
    ray_t* expected = ray_eval_str(
        "(select {from: raw where: (and (> x 1) (< x 4)) out: id text: s symbol: sy})");
    ray_t* actual = ray_eval_str(
        "(select {from: source where: (and (> x 1) (< x 4)) out: id text: s symbol: sy})");
    TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
    TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
    TEST_ASSERT_EQ_I(ray_table_nrows(actual), ray_table_nrows(expected));
    for (int64_t c = 0; c < 3; c++) {
        ray_t* a = ray_table_get_col_idx(actual, c);
        ray_t* e = ray_table_get_col_idx(expected, c);
        TEST_ASSERT_EQ_I(a->type, e->type); TEST_ASSERT_EQ_I(a->len, e->len);
        for (int64_t i = 0; i < a->len; i++) {
            if (c == 0) TEST_ASSERT_EQ_I(((int64_t*)ray_data(a))[i], ((int64_t*)ray_data(e))[i]);
            else if (c == 1) {
                size_t an, en; const char* ap = ray_str_vec_get(a, i, &an);
                const char* ep = ray_str_vec_get(e, i, &en);
                TEST_ASSERT_EQ_I(an, en); TEST_ASSERT_TRUE(!memcmp(ap, ep, an));
            } else {
                ray_t* as = ray_sym_vec_cell(a, i); ray_t* es = ray_sym_vec_cell(e, i);
                TEST_ASSERT_EQ_I(ray_str_len(as), ray_str_len(es));
                TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(as), ray_str_ptr(es), ray_str_len(as)));
            }
        }
    }
    ray_release(expected); ray_release(actual);

    char command[512];
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 1048576 {batch-rows: 1 payload-bytes: 37 scratch-bytes: 0}) where: (== x 2) text: s symbol: sy})",
        root);
    actual = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual)); ray_release(actual);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 1048576 {batch-rows: 1 payload-bytes: 36 scratch-bytes: 0}) where: (== x 2) text: s symbol: sy})",
        root);
    actual = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(actual)); TEST_ASSERT_STR_EQ(ray_err_code(actual), "limit");
    ray_release(actual);

    expected = ray_eval_str(
        "(select {from: raw where: (>= x 2) n: (count id) total: (sum id) mean: (avg id)})");
    actual = ray_eval_str(
        "(select {from: source where: (>= x 2) n: (count id) total: (sum id) mean: (avg id)})");
    TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
    TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
    for (int64_t c = 0; c < 3; c++) {
        ray_t* a = ray_table_get_col_idx(actual, c); ray_t* e = ray_table_get_col_idx(expected, c);
        TEST_ASSERT_EQ_I(a->type, e->type);
        TEST_ASSERT_TRUE(!memcmp(ray_data(a), ray_data(e), ray_type_sizes[(uint8_t)a->type]));
    }
    ray_release(expected); ray_release(actual);
    PASS();
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
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 40, 256, 0};
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

static test_result_t block_store_api(void) {
    fixture(17, false);
    char target[128], command[512];
    snprintf(target, sizeof(target), "%s/api", root);
    snprintf(command, sizeof(command),
        "(.db.block.set \"%s\" raw {block-bytes: 256 codec: 'snappy durable: false})",
        target);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(r->type, RAY_TABLE); TEST_ASSERT_EQ_I(ray_table_nrows(r), 17);
    ray_release(r);

    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 4096) where: (== x 2) id: id text: s symbol: sy})",
        target);
    r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(ray_table_nrows(r), 3);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 0)))[0], 2);
    ray_release(r);

    snprintf(command, sizeof(command),
        "(.db.block.set \"%s/none\" raw {codec: 'none block-bytes: 256})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s/none\" 4096) n: (count id)})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(*(int64_t*)ray_data(ray_table_get_col_idx(r, 0)), 17);
    ray_release(r);

    const char* invalid[] = {
        "(.db.block.set 1 raw {block-bytes: 256 codec: 'snappy})",
        "(.db.block.set \"%s/bad\" 1 {block-bytes: 256 codec: 'snappy})",
        "(.db.block.set \"%s/bad\" raw {block-bytes: 0 codec: 'snappy})",
        "(.db.block.set \"%s/bad\" raw {block-bytes: 256 codec: 'other})",
        "(.db.block.set \"%s/bad\" raw {block-bytes: 256 codec: \"snappy\"})",
        "(.db.block.set \"%s/bad\" raw {block-bytes: 256 codec: 'none extra: 1})",
        "(.db.block.set \"%s/bad\" raw {block-bytes: 256 codec: 'none durable: 1})"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        snprintf(command, sizeof(command), invalid[i], root);
        r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    char bad[128]; snprintf(bad, sizeof(bad), "%s/bad", root);
    TEST_ASSERT_EQ_I(access(bad, F_OK), -1);

    ray_eval_set_restricted(true);
    snprintf(command, sizeof(command),
        "(.db.block.set \"%s/restricted\" raw {block-bytes: 256 codec: 'none})", root);
    r = ray_eval_str(command);
    ray_eval_set_restricted(false);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "access"); ray_release(r);
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

static test_result_t scan_work_limits(void) {
    fixture(64, false);
    const char* opts[] = {
        "{batch-rows: 1 payload-bytes: 45 scratch-bytes: 256}",
        "{batch-rows: 1 payload-bytes: 44 scratch-bytes: 256}",
        "{batch-rows: 1 payload-bytes: 45 scratch-bytes: 255}",
        "{batch-rows: 2 payload-bytes: 45 scratch-bytes: 256}",
        "{batch-rows: 1 payload-bytes: 7 scratch-bytes: 256}"
    };
    for (unsigned i = 0; i < sizeof(opts) / sizeof(*opts); i++) {
        char command[512];
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 8192 %s) where: (>= x 0) number: id text: s symbol: sy})",
            root, opts[i]);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (i) { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        else {
            TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 64);
            ray_t* id = ray_table_get_col_idx(r, 0);
            for (int64_t j = 0; j < id->len; j++) TEST_ASSERT_EQ_I(((int64_t*)ray_data(id))[j], j);
        }
        ray_release(r);
    }
    fixture(0, false);
    char command[512];
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 1 {payload-bytes: 0 scratch-bytes: 0}) id: id})", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 0);
    ray_release(r);
    PASS();
}

static test_result_t query_work_budget(void) {
    fixture(64, false);
    char command[512];
    for (unsigned i = 0; i < 2; i++) {
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 8192 {batch-rows: 2 payload-bytes: 16 scratch-bytes: 256 work-bytes: %u}) where: (and (>= x 4) (< x 1)) id: id})",
            root, 278 - i);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 0); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    for (unsigned i = 0; i < 2; i++) {
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 8192 {batch-rows: 1 payload-bytes: 45 scratch-bytes: 256 work-bytes: %u}) where: (>= x 0) number: id text: s symbol: sy})",
            root, 2721 - i);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 64); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    make_column_raw("id");
    for (unsigned i = 0; i < 2; i++) {
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 8192 {batch-rows: 1 payload-bytes: 8 scratch-bytes: 0 work-bytes: %u}) id: id})",
            root, 520 - i);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 64); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    for (unsigned i = 0; i < 2; i++) {
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 16 {batch-rows: 1 payload-bytes: 8 scratch-bytes: 0 work-bytes: %u}) n: (count id) m: (count id)})",
            root, 36 - i);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r);
        if (!i) { TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 1); }
        else { TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); }
        ray_release(r);
    }
    PASS();
}

static test_result_t scan_options_admission(void) {
    const char* invalid[] = {
        "42", "{batch-rows: 0}", "{batch-rows: -1}", "{batch-rows: 4294967296}",
        "{payload-bytes: -1}", "{scratch-bytes: 0Nl}", "{work-bytes: 0}",
        "{work-bytes: -1}", "{work-bytes: \"1\"}", "{batch-rows: 2.0}",
        "{batch-rows: true}", "{unknown: 1}", "{payload-bytes: \"1\"}"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        char command[512];
        snprintf(command, sizeof(command), "(.db.block.scan \"/missing-hdb-options\" 1024 %s)", invalid[i]);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_TRUE(strcmp(ray_err_code(r), "io") != 0); ray_release(r);
        snprintf(command, sizeof(command),
            "(select {from: {.hdb.block.source: \"/missing-hdb-options\" .hdb.block.limit: 1024 .hdb.block.options: %s} id: id})",
            invalid[i]);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_TRUE(strcmp(ray_err_code(r), "io") != 0); ray_release(r);
    }
    fixture(16, false);
    char command[512];
    snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" 128 {}) id: id})", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 16); ray_release(r);
    /* External descriptors can contain duplicate keys that the parser's
     * dictionary builder would otherwise merge. Reject them at both gates. */
    int64_t duplicate[] = {ray_sym_intern("batch-rows", 10), ray_sym_intern("batch-rows", 10)};
    ray_t* keys = ray_vec_from_raw(RAY_SYM, duplicate, 2);
    ray_t* vals = ray_list_new(2);
    ((ray_t**)ray_data(vals))[0] = ray_i64(1); ((ray_t**)ray_data(vals))[1] = ray_i64(2); vals->len = 2;
    ray_t* bad = ray_dict_new(keys, vals);
    ray_t* args[] = {ray_str(root, strlen(root)), ray_i64(128), bad};
    r = ray_block_source_fn(args, 3);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "domain"); ray_release(r);
    ray_t* descriptor = ray_block_source_fn(args, 2);
    TEST_ASSERT_FALSE(RAY_IS_ERR(descriptor));
    ray_t* option_key = ray_sym(ray_sym_intern(".hdb.block.options", 18));
    descriptor = ray_dict_upsert(descriptor, option_key, bad); ray_release(option_key);
    TEST_ASSERT_EQ_I(ray_env_set(ray_sym_intern("forged", 6), descriptor), RAY_OK);
    ray_release(descriptor);
    r = ray_eval_str("(select {from: forged id: id})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "domain"); ray_release(r);
    for (unsigned i = 0; i < 3; i++) ray_release(args[i]);
    PASS();
}

static test_result_t aggregate_batch_options(void) {
    fixture(8207, false);
    ray_t* raw = ray_env_get(ray_sym_intern("raw", 3));
    ray_t* id = ray_table_get_col_idx(raw, 0);
    for (int64_t i = 4096; i < 8192; i++) ray_vec_set_null(id, i, true);
    ray_block_store_options_t write = {256, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(raw, root, &write), RAY_OK);
    ray_t* expected = ray_eval_str("(select {from: raw n: (count id) total: (sum id) mean: (avg id) lo: (min id) hi: (max id) ns: (count s)})");
    TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
    const uint32_t sizes[] = {1, 31, 4096, 8192, UINT32_MAX};
    for (unsigned i = 0; i < sizeof(sizes) / sizeof(*sizes); i++) {
        char command[512];
        snprintf(command, sizeof(command),
            "(select {from: (.db.block.scan \"%s\" 48 {batch-rows: %u}) n: (count id) total: (sum id) mean: (avg id) lo: (min id) hi: (max id) ns: (count s)})", root, sizes[i]);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_NOT_NULL(r); TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 1);
        for (int64_t c = 0; c < 6; c++) {
            ray_t* a = ray_table_get_col_idx(r, c); ray_t* e = ray_table_get_col_idx(expected, c);
            TEST_ASSERT_EQ_I(a->type, e->type);
            TEST_ASSERT_TRUE(!memcmp(ray_data(a), ray_data(e), ray_type_sizes[(uint8_t)a->type]));
        }
        ray_release(r);
    }
    ray_release(expected);
    PASS();
}

static int64_t storage_cell(ray_t* table, const char* field, int64_t row);
static bool storage_sym(ray_t* table, const char* field, int64_t row, const char* expected);

/* ---- Partitioned block sources ---- */

/* Partition p of a small HDB: its own vocabulary plus one shared symbol, so
 * compressed partitions carry different generation dictionaries. */
static ray_t* parted_table(unsigned p, unsigned rows, int8_t x_type) {
    ray_t* id = ray_vec_new(RAY_I64, rows);
    ray_t* x = ray_vec_new(x_type, rows);
    ray_t* sy = ray_sym_vec_new(RAY_SYM_W64, rows);
    ray_t* s = ray_vec_new(RAY_STR, rows);
    for (unsigned i = 0; i < rows; i++) {
        ((int64_t*)ray_data(id))[i] = p * 1000 + i;
        if (x_type == RAY_I64) ((int64_t*)ray_data(x))[i] = i % 7;
        else ((int32_t*)ray_data(x))[i] = (int32_t)(i % 7);
        char name[32];
        int n = i % 5 ? snprintf(name, sizeof(name), "p%u_%u", p, i % 5) :
            snprintf(name, sizeof(name), "common");
        ((int64_t*)ray_data(sy))[i] = ray_sym_intern(name, (size_t)n);
        const char* text = i % 3 ? "a longer pooled string" : "short";
        s = ray_str_vec_append(s, text, strlen(text));
    }
    id->len = x->len = sy->len = (int64_t)rows;
    ray_t* t = ray_table_new(4);
    t = ray_table_add_col(t, ray_sym_intern("id", 2), id);
    t = ray_table_add_col(t, ray_sym_intern("x", 1), x);
    t = ray_table_add_col(t, ray_sym_intern("sy", 2), sy);
    t = ray_table_add_col(t, ray_sym_intern("s", 1), s);
    ray_release(id); ray_release(x); ray_release(sy); ray_release(s);
    return t;
}

static const char* const parted_days[] = {"2024.01.01", "2024.01.02", "2024.01.03", "2024.01.04"};

/* Save one partition flat and raw (ordinary .db.splayed.set layout, root
 * .sym) or as a compressed block generation. */
static void parted_save(const char* db, unsigned p, ray_t* t, bool block) {
    char dir[256], sym[256];
    snprintf(dir, sizeof(dir), "%s/%s/t", db, parted_days[p]);
    snprintf(sym, sizeof(sym), "%s/.sym", db);
    ray_block_store_options_t options = {256, 1, false};
    ray_err_t err = block ? ray_block_store_save(t, dir, &options) : ray_splay_save(t, dir, sym);
    if (err) ray_test_fatal("parted partition save");
}

static void parted_fixture(const char* name, unsigned block_mask) {
    char db[128];
    snprintf(db, sizeof(db), "%s/%s", root, name);
    for (unsigned p = 0; p < 4; p++) {
        ray_t* t = parted_table(p, 300 + 97 * p, RAY_I64);
        parted_save(db, p, t, block_mask & (1u << p));
        ray_release(t);
    }
}

static bool cells_equal(ray_t* a, ray_t* e, int64_t i) {
    if (a->type == RAY_SYM) {
        ray_t* x = ray_sym_vec_cell(a, i); ray_t* y = ray_sym_vec_cell(e, i);
        return x && y && ray_str_len(x) == ray_str_len(y) &&
            !memcmp(ray_str_ptr(x), ray_str_ptr(y), ray_str_len(x));
    }
    if (a->type == RAY_STR) {
        size_t n, m; const char* x = ray_str_vec_get(a, i, &n);
        const char* y = ray_str_vec_get(e, i, &m);
        return n == m && (!n || !memcmp(x, y, n));
    }
    size_t w = ray_type_sizes[(uint8_t)a->type];
    return !memcmp((const char*)ray_data(a) + i * w, (const char*)ray_data(e) + i * w, w);
}

static test_result_t tables_equal(ray_t* actual, ray_t* expected) {
    TEST_ASSERT_NOT_NULL(expected); TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
    TEST_ASSERT_NOT_NULL(actual); TEST_ASSERT_FALSE(RAY_IS_ERR(actual));
    TEST_ASSERT_EQ_I(ray_table_ncols(actual), ray_table_ncols(expected));
    TEST_ASSERT_EQ_I(ray_table_nrows(actual), ray_table_nrows(expected));
    for (int64_t c = 0; c < ray_table_ncols(actual); c++) {
        TEST_ASSERT_EQ_I(ray_table_col_name(actual, c), ray_table_col_name(expected, c));
        ray_t* a = ray_table_get_col_idx(actual, c); ray_t* e = ray_table_get_col_idx(expected, c);
        TEST_ASSERT_EQ_I(a->type, e->type); TEST_ASSERT_EQ_I(a->len, e->len);
        for (int64_t i = 0; i < a->len; i++) TEST_ASSERT_TRUE(cells_equal(a, e, i));
    }
    PASS();
}

static ray_t* parted_select(const char* source, const char* clauses) {
    char command[768];
    snprintf(command, sizeof(command), "(select {from: %s %s})", source, clauses);
    return ray_eval_str(command);
}

static test_result_t parted_parity(void) {
    parted_fixture("raw", 0);
    parted_fixture("mixed", 0x6); /* 01 and 04 flat raw, 02 and 03 compressed */
    char raw[256], mixed[256];
    snprintf(raw, sizeof(raw), "(.db.parted.get \"%s/raw\" (quote t))", root);
    snprintf(mixed, sizeof(mixed),
        "(.db.block.parted \"%s/mixed\" (quote t) 100000000 {batch-rows: 64 immutable: true})", root);
    const char* clauses[] = {
        "d: date i: id k: sy t: s",
        "where: (== date 2024.01.02) i: id k: sy",
        "where: (and (>= date 2024.01.02) (> x 3)) d: date i: id t: s",
        "where: (and (< date 2024.01.03) (== x 2)) k: sy i: id",
        "where: (> date 2030.01.01) i: id",
        "where: (> x 3) n: (count id) total: (sum id) lo: (min date) hi: (max date) m: (avg x)",
        "where: (> date 2030.01.01) n: (count id) total: (sum id) lo: (min date)",
        "where: (!= date 2024.01.03) n: (count sy) top: (max id)"
    };
    for (unsigned c = 0; c < sizeof(clauses) / sizeof(*clauses); c++) {
        ray_t* expected = parted_select(raw, clauses[c]);
        ray_t* actual = parted_select(mixed, clauses[c]);
        test_result_t r = tables_equal(actual, expected);
        ray_release(expected); ray_release(actual);
        if (r.status != TEST_PASS) return r;
    }
    /* SYM batches from different dictionaries share the runtime domain. */
    ray_t* t = parted_select(mixed, "k: sy");
    TEST_ASSERT_FALSE(RAY_IS_ERR(t));
    TEST_ASSERT_TRUE(ray_sym_vec_domain(ray_table_get_col_idx(t, 0)) == ray_sym_runtime_domain());
    ray_release(t);
    PASS();
}

static void parted_damage_schema(const char* db, unsigned p) {
    char table[256], dir[1024], path[1100];
    snprintf(table, sizeof(table), "%s/%s/t", db, parted_days[p]);
    if (ray_splay_resolve_dir(table, dir, sizeof(dir))) ray_test_fatal("parted resolve");
    snprintf(path, sizeof(path), "%s/.d", dir);
    if (truncate(path, 0)) ray_test_fatal("parted damage");
}

static test_result_t parted_pruning(void) {
    /* 01 compressed, 02 flat raw, 03 compressed but damaged, 04 compressed. */
    parted_fixture("db", 0xd);
    char db[128], leased[256], flat[256];
    snprintf(db, sizeof(db), "%s/db", root);
    parted_damage_schema(db, 2);
    snprintf(leased, sizeof(leased), "(.db.block.parted \"%s\" (quote t) 100000000)", db);
    snprintf(flat, sizeof(flat), "(.db.block.parted \"%s\" (quote t) 100000000 {immutable: true})", db);
    /* Pruned partitions are never opened: neither the damaged one nor the
     * flat one that has no lease. */
    ray_t* r = parted_select(leased, "where: (and (!= date 2024.01.02) (!= date 2024.01.03)) n: (count id) lo: (min date) hi: (max date)");
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 0)))[0], 300 + 591);
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(r, 1)))[0],
                     ray_parted_dir_key("2024.01.01", RAY_MC_DATE));
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(r, 2)))[0],
                     ray_parted_dir_key("2024.01.04", RAY_MC_DATE));
    ray_release(r);
    r = parted_select(leased, "where: (< date 2024.01.03) i: id");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    r = parted_select(flat, "where: (< date 2024.01.03) i: id");
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 300 + 397); ray_release(r);
    r = parted_select(flat, "i: id");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "corrupt"); ray_release(r);

    /* Stats: two partitions read, two pruned without opening. */
    const char* output[] = {"id"};
    ray_t* day = ray_date(ray_parted_dir_key("2024.01.03", RAY_MC_DATE));
    ray_t* other = ray_date(ray_parted_dir_key("2024.01.02", RAY_MC_DATE));
    ray_block_predicate_t pred[] = {{"date", OP_NE, day}, {"date", OP_NE, other}};
    ray_block_scan_options_t read = {0, UINT64_MAX, 64, 1u << 20, 1u << 20, 0};
    ray_block_query_t* q = NULL;
    TEST_ASSERT_EQ_I(ray_block_query_open_parted(db, "t", false, output, 1, pred, 2, &read, &q), RAY_OK);
    ray_release(day); ray_release(other);
    uint64_t rows = 0;
    for (ray_t* b; (b = ray_block_query_next(q)); ray_release(b)) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(b));
        rows += (uint64_t)ray_table_nrows(b);
    }
    ray_block_query_stats_t stats;
    ray_block_query_stats(q, &stats);
    ray_block_query_close(&q);
    TEST_ASSERT_EQ_I(rows, 300 + 591);
    TEST_ASSERT_EQ_I(stats.partitions_read, 2);
    TEST_ASSERT_EQ_I(stats.partitions_pruned, 2);
    TEST_ASSERT_EQ_I(stats.scan.raw_rows, 0);
    TEST_ASSERT_TRUE(stats.scan.blocks_decoded > 0);
    PASS();
}

static test_result_t parted_rejections(void) {
    char db[128], source[256];
    snprintf(db, sizeof(db), "%s/db", root);
    for (unsigned p = 0; p < 3; p++) {
        ray_t* t = parted_table(p, 40, p == 2 ? RAY_I32 : RAY_I64);
        parted_save(db, p, t, true);
        ray_release(t);
    }
    snprintf(source, sizeof(source), "(.db.block.parted \"%s\" (quote t) 100000000)", db);
    /* A later partition with another type for a selected column. */
    ray_t* r = parted_select(source, "i: id v: x");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "schema"); ray_release(r);
    r = parted_select(source, "where: (< date 2024.01.03) i: id v: x");
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 80); ray_release(r);
    const char* invalid[] = {
        "d: date",                                  /* no stored column */
        "where: (== date 1) i: id",                 /* key literal type */
        "where: (== date 0Nd) i: id"                /* null key literal */
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        r = parted_select(source, invalid[i]);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    const char* descriptors[] = {
        "(.db.block.parted \"%s\" \"t\" 100)", "(.db.block.parted \"%s\" (quote .t) 100)",
        "(.db.block.parted \"%s\" (quote t) 0)", "(.db.block.parted \"%s\" (quote t) 100 {immutable: 1})",
        "(.db.block.parted \"%s\" (quote t))"
    };
    for (unsigned i = 0; i < sizeof(descriptors) / sizeof(*descriptors); i++) {
        char command[256];
        snprintf(command, sizeof(command), descriptors[i], db);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    char command[512];
    snprintf(command, sizeof(command), "(select {from: (.db.block.parted \"%s/none\" (quote t) 100) i: id})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    /* The stored schema may not shadow the partition key. */
    ray_t* t = parted_table(0, 8, RAY_I64);
    ray_t* date = ray_vec_new(RAY_DATE, 8); date->len = 8;
    memset(ray_data(date), 0, 8 * sizeof(int32_t));
    t = ray_table_add_col(t, ray_sym_intern("date", 4), date); ray_release(date);
    char other[128];
    snprintf(other, sizeof(other), "%s/keyed", root);
    parted_save(other, 0, t, true); ray_release(t);
    snprintf(command, sizeof(command), "(select {from: (.db.block.parted \"%s\" (quote t) 100000) i: id})", other);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    ray_eval_set_restricted(true);
    r = parted_select(source, "i: id");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "access"); ray_release(r);
    ray_eval_set_restricted(false);
    /* Result and work limits apply across partitions. */
    snprintf(command, sizeof(command), "(select {from: (.db.block.parted \"%s\" (quote t) 640) where: (< date 2024.01.03) i: id})", db);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(ray_table_nrows(r), 80); ray_release(r);
    snprintf(command, sizeof(command), "(select {from: (.db.block.parted \"%s\" (quote t) 639) where: (< date 2024.01.03) i: id})", db);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); ray_release(r);
    snprintf(command, sizeof(command), "(select {from: (.db.block.parted \"%s\" (quote t) 100000 {work-bytes: 64}) where: (< date 2024.01.03) d: date i: id})", db);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); ray_release(r);
    PASS();
}

/* Every column of a Parquet import read back through a block scan. */
static ray_t* parquet_block_readback(const char* target, ray_t* like) {
    char command[2048];
    int n = snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" 100000000)", target);
    for (int64_t c = 0; c < ray_table_ncols(like); c++) {
        ray_t* name = ray_sym_str(ray_table_col_name(like, c));
        n += snprintf(command + n, sizeof(command) - (size_t)n, " %.*s: %.*s",
                      (int)ray_str_len(name), ray_str_ptr(name), (int)ray_str_len(name), ray_str_ptr(name));
    }
    snprintf(command + n, sizeof(command) - (size_t)n, "})");
    return ray_eval_str(command);
}

static test_result_t parquet_block(void) {
    const char* files[] = {"flat-1-v2", "flat-0-v1", "all-null", "unix", "bool"};
    for (unsigned f = 0; f < sizeof(files) / sizeof(*files); f++) {
        char command[512], target[128];
        snprintf(target, sizeof(target), "%s/pq%u", root, f);
        snprintf(command, sizeof(command), "(.parquet.read \"test/data/parquet/%s.parquet\")", files[f]);
        ray_t* expected = ray_eval_str(command);
        TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
        snprintf(command, sizeof(command),
            "(.parquet.block \"test/data/parquet/%s.parquet\" \"%s\" {block-bytes: 64 codec: snappy durable: false})",
            files[f], target);
        ray_t* rows = ray_eval_str(command);
        TEST_ASSERT_FALSE(RAY_IS_ERR(rows));
        TEST_ASSERT_EQ_I(rows->i64, ray_table_nrows(expected)); ray_release(rows);
        ray_t* actual = parquet_block_readback(target, expected);
        test_result_t r = tables_equal(actual, expected);
        ray_release(actual); ray_release(expected);
        if (r.status != TEST_PASS) return r;
    }
    /* Several 64Ki-row batches stream into one generation. */
    char command[512];
    snprintf(command, sizeof(command),
        "(.parquet.block \"test/data/parquet/parallel.parquet\" \"%s/big\" {block-bytes: 65536 codec: snappy durable: false})", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(r->i64, 70003); ray_release(r);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s/big\" 1000) n: (count x) s: (sum x) lo: (min y)})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 0)))[0], 70003);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 1)))[0], 2450175003);
    ray_release(r);
    /* Read options select columns; the published schema follows them. */
    snprintf(command, sizeof(command),
        "(.parquet.block [x] \"test/data/parquet/parallel.parquet\" \"%s/narrow\" {block-bytes: 4096 codec: none durable: false})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s/narrow\" 1000) y: y})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    /* An empty file publishes its schema with no rows. */
    snprintf(command, sizeof(command),
        "(.parquet.block \"test/data/parquet/empty.parquet\" \"%s/empty\" {block-bytes: 64 codec: snappy})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(r->i64, 0); ray_release(r);
    snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s/empty\" 1000) n: (count x)})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    /* Invalid block options and missing inputs publish nothing. */
    const char* invalid[] = {
        "(.parquet.block \"test/data/parquet/bool.parquet\" \"%s/bad\" {codec: zstd})",
        "(.parquet.block \"test/data/parquet/bool.parquet\" \"%s/bad\" {block-bytes: 0 codec: snappy})",
        "(.parquet.block \"test/data/parquet/missing.parquet\" \"%s/bad\" {block-bytes: 64 codec: snappy})"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        snprintf(command, sizeof(command), invalid[i], root);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    snprintf(command, sizeof(command), "%s/bad/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    ray_eval_set_restricted(true);
    r = ray_eval_str("(.parquet.block \"test/data/parquet/bool.parquet\" \"/tmp/never\" {block-bytes: 64 codec: snappy})");
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    ray_eval_set_restricted(false);
    PASS();
}

static void csv_fixture(const char* path, unsigned rows) {
    FILE* f = fopen(path, "w");
    if (!f) ray_test_fatal("csv fixture");
    fprintf(f, "id,px,venue,day,note\n");
    for (unsigned i = 0; i < rows; i++)
        fprintf(f, "%u,%u.25,%s,2024.01.%02u,\"%s, %u\"\n", i, i * 3, i % 3 ? "XNAS" : "ARCX",
                1 + i % 28, i % 4 ? "plain" : "with \"\"quotes\"\" and a longer tail", i);
    if (fclose(f)) ray_test_fatal("csv fixture close");
}

static test_result_t csv_block(void) {
    char csv[128], command[1024], target[128];
    snprintf(csv, sizeof(csv), "%s/trades.csv", root);
    csv_fixture(csv, 500);
    snprintf(command, sizeof(command), "(.csv.read \"%s\")", csv);
    ray_t* expected = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(expected));
    snprintf(target, sizeof(target), "%s/csv1", root);
    snprintf(command, sizeof(command), "(.csv.block \"%s\" \"%s\" {block-bytes: 256 codec: snappy durable: false})", csv, target);
    ray_t* rows = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(rows)); TEST_ASSERT_EQ_I(rows->i64, 500); ray_release(rows);
    ray_t* actual = parquet_block_readback(target, expected);
    test_result_t r = tables_equal(actual, expected);
    ray_release(actual);
    if (r.status != TEST_PASS) { ray_release(expected); return r; }

    /* Seven-row chunks give the same table as one chunk. */
    ray_block_store_options_t options = {256, 1, false};
    int64_t n = 0;
    snprintf(target, sizeof(target), "%s/csv7", root);
    TEST_ASSERT_EQ_I(ray_csv_save_block(csv, ',', true, NULL, 0, NULL, 0, target, 7, &options, &n), RAY_OK);
    TEST_ASSERT_EQ_I(n, 500);
    actual = parquet_block_readback(target, expected);
    r = tables_equal(actual, expected);
    ray_release(actual); ray_release(expected);
    if (r.status != TEST_PASS) return r;

    /* Explicit names and types, with SYM for the venue. */
    snprintf(target, sizeof(target), "%s/typed", root);
    snprintf(command, sizeof(command),
        "(.csv.block [id px venue day note] [I64 F64 SYM DATE STR] \"%s\" \"%s\" {block-bytes: 512 codec: none})", csv, target);
    rows = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(rows)); TEST_ASSERT_EQ_I(rows->i64, 500); ray_release(rows);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 1000000) where: (== day 2024.01.05) v: venue i: id})", target);
    ray_t* t = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(t));
    TEST_ASSERT_EQ_I(ray_table_nrows(t), 18);
    ray_t* venue = ray_table_get_col_idx(t, 0);
    TEST_ASSERT_EQ_I(venue->type, RAY_SYM);
    ray_t* first = ray_sym_vec_cell(venue, 0);
    TEST_ASSERT_TRUE(ray_str_len(first) == 4 && !memcmp(ray_str_ptr(first), "XNAS", 4));
    ray_release(t);

    /* Failures publish nothing. */
    const char* invalid[] = {
        "(.csv.block \"%s\" \"%s/bad\" {codec: zstd})",
        "(.csv.block \"%s/missing.csv\" \"%s/bad\" {block-bytes: 64 codec: snappy})",
        "(.csv.block \"%s\" \"%s/bad\" {block-bytes: 16 codec: snappy})"   /* note > 12 bytes */
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        if (i == 1) snprintf(command, sizeof(command), invalid[i], root, root);
        else snprintf(command, sizeof(command), invalid[i], csv, root);
        ray_t* e = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(e)); ray_release(e);
    }
    snprintf(command, sizeof(command), "%s/bad/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    PASS();
}

static test_result_t block_convert(void) {
    parted_fixture("raw", 0);
    char command[1024];
    for (unsigned p = 0; p < 4; p++) {
        snprintf(command, sizeof(command),
            "(.db.block.convert \"%s/raw/%s/t\" \"%s/conv/%s/t\" {block-bytes: 256 codec: snappy durable: false} {immutable: true})",
            root, parted_days[p], root, parted_days[p]);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(storage_cell(r, "rows", 0), 300 + 97 * p); ray_release(r);
    }
    char raw[256], conv[256];
    snprintf(raw, sizeof(raw), "(.db.parted.get \"%s/raw\" (quote t))", root);
    snprintf(conv, sizeof(conv), "(.db.block.parted \"%s/conv\" (quote t) 100000000)", root);
    const char* clauses[] = {
        "d: date i: id v: x k: sy t: s",
        "where: (and (>= date 2024.01.02) (> x 3)) i: id k: sy",
        "where: (< x 2) n: (count sy) total: (sum id) hi: (max date)"
    };
    for (unsigned c = 0; c < sizeof(clauses) / sizeof(*clauses); c++) {
        ray_t* expected = parted_select(raw, clauses[c]);
        ray_t* actual = parted_select(conv, clauses[c]);
        test_result_t r = tables_equal(actual, expected);
        ray_release(expected); ray_release(actual);
        if (r.status != TEST_PASS) return r;
    }
    /* A flat source is read only when declared immutable. */
    snprintf(command, sizeof(command),
        "(.db.block.convert \"%s/raw/2024.01.01/t\" \"%s/other\" {block-bytes: 256 codec: snappy})", root, root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    /* Compressed to compressed: a repack with another block size. */
    snprintf(command, sizeof(command),
        "(.db.block.convert \"%s/conv/2024.01.02/t\" \"%s/repack\" {block-bytes: 4096 codec: none durable: false})", root, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(storage_cell(r, "rows", 0), 397); ray_release(r);
    char repack[256];
    snprintf(repack, sizeof(repack), "(.db.block.scan \"%s/repack\" 100000000)", root);
    ray_t* expected = parted_select(conv, "where: (== date 2024.01.02) i: id v: x k: sy t: s");
    ray_t* actual = parted_select(repack, "i: id v: x k: sy t: s");
    test_result_t tr = tables_equal(actual, expected);
    ray_release(expected); ray_release(actual);
    if (tr.status != TEST_PASS) return tr;
    /* Destinations overlapping the source are refused; the source is intact. */
    const char* overlap[] = {"%s/raw/2024.01.01/t", "%s/raw/2024.01.01/t/sub", "%s/raw", "%s/raw/2024.01.01/t/"};
    for (unsigned i = 0; i < sizeof(overlap) / sizeof(*overlap); i++) {
        char dest[256];
        snprintf(dest, sizeof(dest), overlap[i], root);
        snprintf(command, sizeof(command),
            "(.db.block.convert \"%s/raw/2024.01.01/t\" \"%s\" {block-bytes: 256 codec: snappy} {immutable: true})", root, dest);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "domain"); ray_release(r);
    }
    snprintf(command, sizeof(command), "%s/raw/2024.01.01/t/.d", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), 0);
    snprintf(command, sizeof(command), "%s/raw/2024.01.01/t/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    /* A standalone flat table with its own .sym. */
    ray_t* t = parted_table(9, 50, RAY_I64);
    char flat[256];
    snprintf(flat, sizeof(flat), "%s/flat", root);
    snprintf(command, sizeof(command), "%s/flat/.sym", root);
    TEST_ASSERT_EQ_I(ray_splay_save(t, flat, command), RAY_OK);
    if (ray_env_set(ray_sym_intern("flat-src", 8), t)) ray_test_fatal("convert bind");
    ray_release(t);
    snprintf(command, sizeof(command),
        "(.db.block.convert \"%s\" \"%s/flat-block\" {block-bytes: 128 codec: snappy} {immutable: true})", flat, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(storage_cell(r, "rows", 0), 50); ray_release(r);
    snprintf(repack, sizeof(repack), "(.db.block.scan \"%s/flat-block\" 100000000)", root);
    expected = parted_select("flat-src", "i: id v: x k: sy t: s");
    actual = parted_select(repack, "i: id v: x k: sy t: s");
    tr = tables_equal(actual, expected);
    ray_release(expected); ray_release(actual);
    if (tr.status != TEST_PASS) return tr;
    const char* invalid[] = {
        "(.db.block.convert \"%s/flat\" \"%s/x\" {block-bytes: 128 codec: snappy} {immutable: 1})",
        "(.db.block.convert \"%s/flat\" \"%s/x\" {block-bytes: 128 codec: snappy} {other: true})",
        "(.db.block.convert \"%s/flat\" \"%s/x\" {codec: zstd})",
        "(.db.block.convert \"%s/missing\" \"%s/x\" {block-bytes: 128 codec: snappy} {immutable: true})"
    };
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        snprintf(command, sizeof(command), invalid[i], root, root);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    snprintf(command, sizeof(command), "%s/x/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    /* A persisted SORTED column cannot be represented: refused, not dropped. */
    t = parted_table(8, 40, RAY_I64);
    ray_table_get_col_idx(t, 0)->attrs |= RAY_ATTR_SORTED;
    char sorted[256];
    snprintf(sorted, sizeof(sorted), "%s/sorted", root);
    snprintf(command, sizeof(command), "%s/sorted/.sym", root);
    TEST_ASSERT_EQ_I(ray_splay_save(t, sorted, command), RAY_OK);
    ray_release(t);
    snprintf(command, sizeof(command),
        "(.db.block.convert \"%s\" \"%s/sorted-block\" {block-bytes: 128 codec: snappy} {immutable: true})", sorted, root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "nyi"); ray_release(r);
    snprintf(command, sizeof(command), "%s/sorted-block/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    PASS();
}

static int64_t storage_cell(ray_t* table, const char* field, int64_t row) {
    ray_t* col = ray_table_get_col(table, ray_sym_intern(field, strlen(field)));
    return col ? ((int64_t*)ray_data(col))[row] : -1;
}

static bool storage_sym(ray_t* table, const char* field, int64_t row, const char* expected) {
    ray_t* col = ray_table_get_col(table, ray_sym_intern(field, strlen(field)));
    ray_t* s = col ? ray_sym_vec_cell(col, row) : NULL;
    return s && ray_str_len(s) == strlen(expected) && !memcmp(ray_str_ptr(s), expected, ray_str_len(s));
}

static test_result_t storage_info_verify(void) {
    fixture(4103, false);   /* id x s sy, 256-byte Snappy blocks */
    make_column_raw("x");
    char command[512];
    snprintf(command, sizeof(command), "(.db.storage.info \"%s\")", root);
    ray_t* info = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(info));
    TEST_ASSERT_EQ_I(ray_table_nrows(info), 4); TEST_ASSERT_EQ_I(ray_table_ncols(info), 10);
    TEST_ASSERT_TRUE(storage_sym(info, "column", 0, "id"));
    TEST_ASSERT_TRUE(storage_sym(info, "format", 0, "block"));
    TEST_ASSERT_TRUE(storage_sym(info, "codec", 0, "snappy"));
    TEST_ASSERT_TRUE(storage_sym(info, "type", 0, "I64"));
    TEST_ASSERT_EQ_I(storage_cell(info, "rows", 0), 4103);
    TEST_ASSERT_EQ_I(storage_cell(info, "blocks", 0), (4103 + 31) / 32);
    TEST_ASSERT_EQ_I(storage_cell(info, "block_bytes", 0), 256);
    TEST_ASSERT_EQ_I(storage_cell(info, "logical", 0), 4103 * 8);
    TEST_ASSERT_TRUE(storage_sym(info, "format", 1, "raw"));
    TEST_ASSERT_TRUE(storage_sym(info, "codec", 1, "none"));
    TEST_ASSERT_EQ_I(storage_cell(info, "blocks", 1), 0);
    TEST_ASSERT_TRUE(storage_cell(info, "stored", 1) >= 4103 * 8);
    double ratio = ((double*)ray_data(ray_table_get_col(info, ray_sym_intern("ratio", 5))))[0];
    TEST_ASSERT_TRUE(ratio > 1.0); /* sequential ids compress */
    ray_release(info);

    snprintf(command, sizeof(command), "(.db.storage.verify \"%s\")", root);
    ray_t* verify = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(verify));
    TEST_ASSERT_EQ_I(storage_cell(verify, "blocks", 0), (4103 + 31) / 32);
    TEST_ASSERT_EQ_I(storage_cell(verify, "blocks", 1), 0);
    ray_release(verify);

    /* A payload byte flip passes metadata checks; only verify reports it. */
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/id", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER + 3, SEEK_SET), 0);
    int b = fgetc(f); TEST_ASSERT_TRUE(b != EOF);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER + 3, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(b ^ 0x10, f) != EOF); TEST_ASSERT_EQ_I(fclose(f), 0);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s\")", root);
    info = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(info)); ray_release(info);
    snprintf(command, sizeof(command), "(.db.storage.verify \"%s\")", root);
    verify = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(verify)); TEST_ASSERT_STR_EQ(ray_err_code(verify), "corrupt");
    ray_release(verify);

    /* Flat raw tables need immutable; unknown options and restricted fail. */
    ray_t* t = parted_table(1, 20, RAY_I64);
    char flat[256], sym[256];
    snprintf(flat, sizeof(flat), "%s/flat", root);
    snprintf(sym, sizeof(sym), "%s/flat/.sym", root);
    TEST_ASSERT_EQ_I(ray_splay_save(t, flat, sym), RAY_OK); ray_release(t);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s\")", flat);
    info = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(info)); TEST_ASSERT_STR_EQ(ray_err_code(info), "nyi"); ray_release(info);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s\" {immutable: true})", flat);
    info = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(info)); TEST_ASSERT_EQ_I(ray_table_nrows(info), 4);
    TEST_ASSERT_TRUE(storage_sym(info, "format", 2, "raw"));
    TEST_ASSERT_TRUE(storage_sym(info, "type", 2, "SYM"));
    ray_release(info);
    const char* invalid[] = {"(.db.storage.info)", "(.db.storage.info \"\")",
                             "(.db.storage.info \"%s\" {other: 1})", "(.db.storage.verify \"%s\" 1)"};
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        snprintf(command, sizeof(command), invalid[i], flat);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    ray_eval_set_restricted(true);
    snprintf(command, sizeof(command), "(.db.storage.verify \"%s\" {immutable: true})", flat);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "access"); ray_release(r);
    ray_eval_set_restricted(false);
    PASS();
}

/* Large reads decode their whole blocks on the pool; values and errors are
 * the same as serial decoding. */
static test_result_t parallel_decode(void) {
    const int64_t rows = 1 << 20;
    ray_t* v = ray_vec_new(RAY_I64, rows);
    for (int64_t i = 0; i < rows; i++) ((int64_t*)ray_data(v))[i] = (i * 2654435761) % 1000003;
    v->len = rows;
    ray_t* t = ray_table_new(1);
    t = ray_table_add_col(t, ray_sym_intern("v", 1), v);
    ray_block_store_options_t store = {4096, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
    ray_release(t);
    char command[256];
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 100000000 {batch-rows: 262144}) x: v})", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    ray_t* x = ray_table_get_col_idx(r, 0);
    TEST_ASSERT_EQ_I(x->len, rows);
    TEST_ASSERT_TRUE(!memcmp(ray_data(x), ray_data(v), (size_t)rows * 8));
    ray_release(r);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 100000000 {batch-rows: 262144}) s: (sum v) n: (count v)})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    int64_t sum = 0;
    for (int64_t i = 0; i < rows; i++) sum += ((int64_t*)ray_data(v))[i];
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(r, 0)))[0], sum);
    ray_release(r);
    /* A damaged block in the middle of a parallel run fails the read. */
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/v", dir);
    ray_col_block_file_t* file = NULL;
    TEST_ASSERT_EQ_I(ray_col_block_file_open(path, &file), RAY_OK);
    const ray_col_block_reader_t* reader = ray_col_block_file_reader(file);
    const uint8_t* e = reader->data + reader->directory + 100 * RAY_COL_BLOCK_ENTRY;
    uint64_t offset = 0;
    for (unsigned i = 0; i < 8; i++) offset |= (uint64_t)e[16 + i] << (8 * i);
    ray_col_block_file_close(&file);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, (long)offset + 5, SEEK_SET), 0);
    int b = fgetc(f); TEST_ASSERT_TRUE(b != EOF);
    TEST_ASSERT_EQ_I(fseek(f, (long)offset + 5, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(b ^ 0x40, f) != EOF); TEST_ASSERT_EQ_I(fclose(f), 0);
    snprintf(command, sizeof(command),
        "(select {from: (.db.block.scan \"%s\" 100000000 {batch-rows: 262144}) x: v})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "corrupt"); ray_release(r);
    ray_release(v);
    PASS();
}

static test_result_t storage_parted(void) {
    parted_fixture("mix", 0x6); /* 01 and 04 flat raw, 02 and 03 compressed */
    char command[512];
    snprintf(command, sizeof(command), "(.db.storage.info \"%s/mix\" (quote t) {immutable: true})", root);
    ray_t* info = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(info));
    TEST_ASSERT_EQ_I(ray_table_nrows(info), 16); TEST_ASSERT_EQ_I(ray_table_ncols(info), 11);
    TEST_ASSERT_TRUE(storage_sym(info, "partition", 0, "2024.01.01"));
    TEST_ASSERT_TRUE(storage_sym(info, "format", 0, "raw"));
    TEST_ASSERT_TRUE(storage_sym(info, "partition", 4, "2024.01.02"));
    TEST_ASSERT_TRUE(storage_sym(info, "format", 4, "block"));
    TEST_ASSERT_TRUE(storage_sym(info, "column", 4, "id"));
    TEST_ASSERT_EQ_I(storage_cell(info, "rows", 4), 397);
    TEST_ASSERT_TRUE(storage_sym(info, "type", 6, "SYM"));
    ray_release(info);
    snprintf(command, sizeof(command), "(.db.storage.verify \"%s/mix\" (quote t) {immutable: true})", root);
    ray_t* verify = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(verify)); TEST_ASSERT_EQ_I(ray_table_nrows(verify), 16);
    TEST_ASSERT_EQ_I(storage_cell(verify, "blocks", 0), 0);
    TEST_ASSERT_TRUE(storage_cell(verify, "blocks", 4) > 0);
    ray_release(verify);
    /* Flat partitions need immutable; a damaged block names its partition. */
    snprintf(command, sizeof(command), "(.db.storage.verify \"%s/mix\" (quote t))", root);
    verify = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(verify)); TEST_ASSERT_STR_EQ(ray_err_code(verify), "nyi"); ray_release(verify);
    char table[256], dir[1024], path[1100];
    snprintf(table, sizeof(table), "%s/mix/2024.01.03/t", root);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(table, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/x", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER + 2, SEEK_SET), 0);
    int b = fgetc(f); TEST_ASSERT_TRUE(b != EOF);
    TEST_ASSERT_EQ_I(fseek(f, RAY_COL_BLOCK_HEADER + 2, SEEK_SET), 0);
    TEST_ASSERT_TRUE(fputc(b ^ 0x20, f) != EOF); TEST_ASSERT_EQ_I(fclose(f), 0);
    snprintf(command, sizeof(command), "(.db.storage.verify \"%s/mix\" (quote t) {immutable: true})", root);
    verify = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(verify)); TEST_ASSERT_STR_EQ(ray_err_code(verify), "corrupt"); ray_release(verify);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s/mix\" (quote t) {immutable: true})", root);
    info = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(info)); ray_release(info);
    const char* invalid[] = {"(.db.storage.info \"%s/mix\" (quote .t))", "(.db.storage.info \"%s/none\" (quote t))",
                             "(.db.storage.info \"%s/mix\" (quote t) {immutable: true} 1)"};
    for (unsigned i = 0; i < sizeof(invalid) / sizeof(*invalid); i++) {
        snprintf(command, sizeof(command), invalid[i], root);
        ray_t* r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    PASS();
}

/* STR columns decode once into an arena when it fits scratch-bytes, else
 * twice through one block of scratch: the same rows either way. */
static test_result_t string_arena_fallback(void) {
    fixture(4103, false);
    const char* clauses[] = {"text: s i: id", "where: (== x 2) text: s i: id"};
    const char* options[] = {"", "{scratch-bytes: 300}", "{batch-rows: 777}", "{batch-rows: 777 scratch-bytes: 300}"};
    for (unsigned c = 0; c < 2; c++) {
        char command[512];
        snprintf(command, sizeof(command), "(select {from: raw %s})", clauses[c]);
        ray_t* expected = ray_eval_str(command);
        for (unsigned o = 0; o < 4; o++) {
            snprintf(command, sizeof(command), "(select {from: (.db.block.scan \"%s\" 100000000 %s) %s})",
                     root, options[o], clauses[c]);
            ray_t* actual = ray_eval_str(command);
            test_result_t r = tables_equal(actual, expected);
            ray_release(actual);
            if (r.status != TEST_PASS) { ray_release(expected); return r; }
        }
        ray_release(expected);
    }
    /* Arena and fallback count their STR decodes: once versus twice. */
    const char* output[] = {"s"};
    ray_block_scan_options_t read = {0, UINT64_MAX, 8192, 1u << 20, 1u << 20, 0};
    uint64_t decoded[2];
    for (unsigned o = 0; o < 2; o++) {
        read.scratch_limit = o ? 300 : 1u << 20;
        ray_block_query_t* q = NULL;
        TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &q), RAY_OK);
        for (ray_t* b; (b = ray_block_query_next(q)); ray_release(b)) TEST_ASSERT_FALSE(RAY_IS_ERR(b));
        ray_block_query_stats_t stats;
        ray_block_query_stats(q, &stats);
        decoded[o] = stats.scan.blocks_decoded;
        ray_block_query_close(&q);
    }
    TEST_ASSERT_EQ_I(decoded[1], decoded[0] * 2);
    PASS();
}

static test_result_t block_convert_parted(void) {
    parted_fixture("raw", 0);
    char command[512];
    snprintf(command, sizeof(command),
        "(.db.block.convert \"%s/raw\" (quote t) \"%s/conv\" {block-bytes: 256 codec: snappy durable: false} {immutable: true})",
        root, root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(ray_table_nrows(r), 4);
    TEST_ASSERT_TRUE(storage_sym(r, "partition", 0, "2024.01.01"));
    for (int64_t p = 0; p < 4; p++) TEST_ASSERT_EQ_I(storage_cell(r, "rows", p), 300 + 97 * p);
    ray_release(r);
    char raw[256], conv[256];
    snprintf(raw, sizeof(raw), "(.db.parted.get \"%s/raw\" (quote t))", root);
    snprintf(conv, sizeof(conv), "(.db.block.parted \"%s/conv\" (quote t) 100000000)", root);
    ray_t* expected = parted_select(raw, "d: date i: id v: x k: sy t: s");
    ray_t* actual = parted_select(conv, "d: date i: id v: x k: sy t: s");
    test_result_t tr = tables_equal(actual, expected);
    ray_release(expected); ray_release(actual);
    if (tr.status != TEST_PASS) return tr;
    const char* invalid[] = {
        "(.db.block.convert \"%s/raw\" (quote t) \"%s/raw/sub\" {block-bytes: 256 codec: snappy} {immutable: true})",
        "(.db.block.convert \"%s/raw\" (quote t) \"%s/other\" {block-bytes: 256 codec: snappy})",
        "(.db.block.convert \"%s/raw\" (quote .t) \"%s/other\" {block-bytes: 256 codec: snappy})",
        "(.db.block.convert \"%s/missing\" (quote t) \"%s/other\" {block-bytes: 256 codec: snappy})"
    };
    const char* codes[] = {"domain", "nyi", "domain", "io"};
    for (unsigned i = 0; i < 4; i++) {
        snprintf(command, sizeof(command), invalid[i], root, root);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), codes[i]); ray_release(r);
    }
    snprintf(command, sizeof(command), "%s/raw/2024.01.01/t/.current", root);
    TEST_ASSERT_EQ_I(access(command, F_OK), -1);
    PASS();
}

/* Without options the block builtins write 64 KiB Snappy blocks and say so. */
static test_result_t block_defaults(void) {
    fixture(500, false);
    char command[512];
    snprintf(command, sizeof(command), "(.db.block.set \"%s/set\" raw)", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s/set\")", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(storage_cell(r, "block_bytes", 0), RAY_BLOCK_STORE_DEFAULT_BYTES);
    ray_release(r);
    snprintf(command, sizeof(command), "(.db.block.convert \"%s/set\" \"%s/conv\")", root, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(storage_cell(r, "rows", 0), 500);
    TEST_ASSERT_TRUE(storage_sym(r, "codec", 0, "snappy"));
    TEST_ASSERT_EQ_I(storage_cell(r, "block_bytes", 0), RAY_BLOCK_STORE_DEFAULT_BYTES);
    ray_release(r);
    snprintf(command, sizeof(command), "(.db.block.convert \"%s/set\" \"%s/none\" {codec: none} {})", root, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_TRUE(storage_sym(r, "codec", 0, "none")); ray_release(r);
    snprintf(command, sizeof(command), "(.db.block.convert \"%s/set\" \"%s/empty\" {})", root, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_TRUE(storage_sym(r, "codec", 0, "snappy")); ray_release(r);
    snprintf(command, sizeof(command), "(.parquet.block \"test/data/parquet/bool.parquet\" \"%s/pq\")", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command), "(.parquet.block [b] \"test/data/parquet/bool.parquet\" \"%s/pq2\")", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r) || r->i64 >= 0); ray_release(r);
    char csv[128];
    snprintf(csv, sizeof(csv), "%s/d.csv", root);
    csv_fixture(csv, 20);
    snprintf(command, sizeof(command), "(.csv.block \"%s\" \"%s/csv\")", csv, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); TEST_ASSERT_EQ_I(r->i64, 20); ray_release(r);
    snprintf(command, sizeof(command), "(.db.storage.info \"%s/csv\")", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r));
    TEST_ASSERT_EQ_I(storage_cell(r, "block_bytes", 0), RAY_BLOCK_STORE_DEFAULT_BYTES);
    ray_release(r);
    PASS();
}

/* Existing loaders name the block format; materialize and splayed.set
 * options reach it explicitly. */
static test_result_t splayed_integration(void) {
    fixture(300, false);   /* root holds a block generation of raw's rows */
    char command[512];
    snprintf(command, sizeof(command), "(.db.splayed.get \"%s\")", root);
    ray_t* r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "version");
    const char* msg = ray_error_msg();
    TEST_ASSERT_TRUE(msg && strstr(msg, ".db.block.scan") && strstr(msg, "materialize"));
    ray_release(r);
    snprintf(command, sizeof(command), "(.db.splayed.get \"%s\" {materialize: 100000000})", root);
    ray_t* loaded = ray_eval_str(command);
    ray_t* expected = ray_eval_str("(select {from: raw id: id x: x s: s sy: sy})");
    test_result_t tr = tables_equal(loaded, expected);
    ray_release(loaded);
    if (tr.status != TEST_PASS) { ray_release(expected); return tr; }
    snprintf(command, sizeof(command), "(.db.splayed.get \"%s\" {materialize: 100})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); TEST_ASSERT_STR_EQ(ray_err_code(r), "limit"); ray_release(r);
    const char* invalid[] = {"(.db.splayed.get \"%s\" {materialize: 0})", "(.db.splayed.get \"%s\" {other: 1})",
                             "(.db.splayed.get \"%s\" {materialize: 1.5})"};
    for (unsigned i = 0; i < 3; i++) {
        snprintf(command, sizeof(command), invalid[i], root);
        r = ray_eval_str(command);
        TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    }
    /* .db.splayed.set with options writes blocks; without, raw as before. */
    snprintf(command, sizeof(command), "(.db.splayed.set \"%s/viaset\" raw {codec: none})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command), "(.db.splayed.set \"%s/viaset2\" raw \"%s/unused.sym\" {})", root, root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    for (unsigned i = 0; i < 2; i++) {
        snprintf(command, sizeof(command), "(.db.storage.info \"%s/viaset%s\")", root, i ? "2" : "");
        r = ray_eval_str(command);
        TEST_ASSERT_FALSE(RAY_IS_ERR(r));
        TEST_ASSERT_TRUE(storage_sym(r, "format", 0, "block"));
        TEST_ASSERT_TRUE(storage_sym(r, "codec", 0, i ? "snappy" : "none"));
        ray_release(r);
    }
    snprintf(command, sizeof(command), "(.db.splayed.set \"%s/flat/\" raw)", root);
    r = ray_eval_str(command);
    TEST_ASSERT_FALSE(RAY_IS_ERR(r)); ray_release(r);
    snprintf(command, sizeof(command), "(.db.splayed.get \"%s/flat\" {materialize: 100000000})", root);
    loaded = ray_eval_str(command);
    tr = tables_equal(loaded, expected);
    ray_release(loaded); ray_release(expected);
    if (tr.status != TEST_PASS) return tr;
    snprintf(command, sizeof(command), "(.db.splayed.set \"%s/x\" raw 1 {})", root);
    r = ray_eval_str(command);
    TEST_ASSERT_TRUE(RAY_IS_ERR(r)); ray_release(r);
    PASS();
}

const test_entry_t block_source_entries[] = {
    {"block_source/splayed_integration", splayed_integration, setup, teardown},
    {"block_source/block_defaults", block_defaults, setup, teardown},
    {"block_source/block_convert_parted", block_convert_parted, setup, teardown},
    {"block_source/string_arena_fallback", string_arena_fallback, setup, teardown},
    {"block_source/storage_parted", storage_parted, setup, teardown},
    {"block_source/parallel_decode", parallel_decode, setup, teardown},
    {"block_source/storage_info_verify", storage_info_verify, setup, teardown},
    {"block_source/block_convert", block_convert, setup, teardown},
    {"block_source/csv_block", csv_block, setup, teardown},
    {"block_source/parquet_block", parquet_block, setup, teardown},
    {"block_source/parted_parity", parted_parity, setup, teardown},
    {"block_source/parted_pruning", parted_pruning, setup, teardown},
    {"block_source/parted_rejections", parted_rejections, setup, teardown},
    {"block_source/block_store_api", block_store_api, setup, teardown},
    {"block_source/mixed_format_parity", mixed_format_parity, setup, teardown},
    {"block_source/scan_work_limits", scan_work_limits, setup, teardown},
    {"block_source/query_work_budget", query_work_budget, setup, teardown},
    {"block_source/scan_options_admission", scan_options_admission, setup, teardown},
    {"block_source/aggregate_batch_options", aggregate_batch_options, setup, teardown},
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
