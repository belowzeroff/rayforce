/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/block_scan.h"
#include "store/col_block.h"
#include "store/splay.h"
#include "mem/heap.h"
#include "mem/sys.h"
#include "table/sym.h"
#include <stdlib.h>
#include <unistd.h>

static char root[64];
static bool seeded;
static ray_block_scan_t* scan;
static const char* const names[] = {"x", "y"};

static void setup(void) {
    ray_heap_init(); ray_sym_init();
    strcpy(root, "/tmp/ray-block-scan-XXXXXX");
    if (!mkdtemp(root)) ray_test_fatal("scan test directory");
    seeded = false;
}
static void teardown(void) {
    ray_block_scan_close(&scan);
    ray_test_rm_rf(root);
    ray_sym_destroy(); ray_heap_destroy();
}
static int64_t mapped(void) {
    int64_t current, peak; ray_sys_get_mapped(&current, &peak); return current;
}
static void publish(unsigned epoch, unsigned rows, unsigned y_rows, unsigned y_epoch) {
    int64_t x[32]; int32_t y[32];
    for (unsigned i = 0; i < 32; i++) { x[i] = epoch * 100 + i; y[i] = epoch * 1000 + i; }
    ray_t* t = ray_table_new(2);
    for (unsigned c = 0; c < 2; c++) {
        ray_t* v = ray_vec_new(c ? RAY_I32 : RAY_I64, rows);
        if (!v || RAY_IS_ERR(v)) ray_test_fatal("scan test vector");
        memcpy(ray_data(v), c ? (void*)y : (void*)x, rows * (c ? 4 : 8)); v->len = rows;
        t = ray_table_add_col(t, ray_sym_intern(names[c], 1), v); ray_release(v);
        if (!t || RAY_IS_ERR(t)) ray_test_fatal("scan test table");
    }
    if (!seeded) {
        if (ray_splay_save(t, root, NULL)) ray_test_fatal("scan test seed");
        seeded = true;
    }
    ray_splay_write_t write;
    if (ray_splay_write_begin(root, &write)) ray_test_fatal("scan test stage");
    ray_err_t err = ray_splay_write_table(t, write.dir, NULL, false);
    ray_release(t);
    for (unsigned c = 0; !err && c < 2; c++) {
        char path[1100]; snprintf(path, sizeof(path), "%s/%s", write.dir, names[c]);
        FILE* f = fopen(path, "w+b");
        if (!f) { err = RAY_ERR_IO; break; }
        ray_col_block_writer_t w;
        err = ray_col_block_begin(&w, f, c ? RAY_I32 : RAY_I64, c ? 12 : 64, 1, c ? y_epoch : epoch);
        if (!err) err = ray_col_block_append(&w, c ? (void*)y : (void*)x, c ? y_rows : rows);
        if (!err) err = ray_col_block_finish(&w);
        else ray_col_block_abort(&w);
        if (fclose(f) && !err) err = RAY_ERR_IO;
    }
    err = ray_splay_write_finish(&write, err, false);
    if (err) ray_test_fatal("scan test publish");
}
static void damage(const char* name, long offset) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("scan test resolve");
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "r+b");
    if (!f || fseek(f, offset, SEEK_SET)) ray_test_fatal("scan test damage seek");
    int value = fgetc(f);
    if (value == EOF || fseek(f, offset, SEEK_SET) || fputc(value ^ 1, f) == EOF || fclose(f))
        ray_test_fatal("scan test damage");
}

static test_result_t batches(void) {
    publish(1, 16, 16, 1);
    char pinned[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    int64_t baseline = mapped();
    const char* const projection[] = {"y", "x"};
    ray_block_scan_options_t options = {2, 12, 5, 60, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &options, &scan), RAY_OK);
    uint64_t offset = 2;
    ray_t* retained = NULL;
    for (unsigned batch = 0; batch < 3; batch++) {
        ray_t* t = ray_block_scan_next(scan);
        TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t));
        int64_t n = batch == 2 ? 2 : 5;
        TEST_ASSERT_EQ_I(ray_table_nrows(t), n); TEST_ASSERT_EQ_I(ray_table_ncols(t), 2);
        TEST_ASSERT_EQ_I(ray_table_col_name(t, 0), ray_sym_intern("y", 1));
        ray_t* y = ray_table_get_col_idx(t, 0); ray_t* x = ray_table_get_col_idx(t, 1);
        for (int64_t i = 0; i < n; i++) {
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(y))[i], 1000 + offset + i);
            TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[i], 100 + offset + i);
        }
        offset += n;
        if (!batch) {
            retained = t;
            for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
            TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
        } else ray_release(t);
    }
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL);
    ray_block_scan_close(&scan); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(5, 16, 16, 5);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[0], 102);
    ray_release(retained);
    PASS();
}

static test_result_t failures(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    const char* const invalid[] = {"x", "../y"};
    const char* const duplicates[] = {"x", "x"};
    const char* const missing[] = {"x", "missing"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, NULL), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, NULL, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 0, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1025, &options, &scan), RAY_ERR_LIMIT);
    options.batch_rows = 0;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_DOMAIN);
    options.batch_rows = 5;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, invalid, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, duplicates, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, missing, 2, &options, &scan), RAY_ERR_IO);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    for (unsigned kind = 0; kind < 3; kind++) {
        options.payload_limit = kind == 0 ? 59 : 60;
        options.scratch_limit = kind == 1 ? 63 : 64;
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        if (kind == 2) ray_block_scan_cancel(scan);
        ray_t* err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err));
        TEST_ASSERT_STR_EQ(ray_err_code(err), kind == 2 ? "cancel" : "limit"); ray_error_free(err);
        err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    publish(2, 16, 15, 2);
    char failed_generation[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, failed_generation, sizeof(failed_generation)), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_SCHEMA);
    publish(3, 16, 16, 4);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(4, 0, 0, 4);
    TEST_ASSERT_EQ_I(access(failed_generation, F_OK), -1); /* No leaked lease after open failure. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* schema = ray_block_scan_schema(scan);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); TEST_ASSERT_EQ_I(ray_table_ncols(schema), 2);
    TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_I64);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 1)->type, RAY_I32); ray_release(schema);
    options.start = 1;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t cancel_after_batch(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_DOMAIN);
    ray_t* t = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t));
    ray_block_scan_cancel(scan);
    ray_t* err = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "cancel");
    ray_error_free(err); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(t, 0)))[0], 100);
    ray_release(t);
    ray_block_scan_cancel(NULL); ray_block_scan_close(NULL);
    err = ray_block_scan_next(NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); ray_error_free(err);
    PASS();
}

static test_result_t projection(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    damage("y", 0); /* A corrupt unselected column must never be opened. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_t* t = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t)); ray_release(t);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(2, 16, 16, 2);
    damage("y", RAY_COL_BLOCK_HEADER); /* Failure after x has been materialized. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    t = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(t)); TEST_ASSERT_STR_EQ(ray_err_code(t), "corrupt"); ray_error_free(t);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

const test_entry_t block_scan_entries[] = {
    {"block_scan/batches", batches, setup, teardown},
    {"block_scan/failures", failures, setup, teardown},
    {"block_scan/projection", projection, setup, teardown},
    {"block_scan/cancel_after_batch", cancel_after_batch, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
