/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/block_scan.h"
#include "store/block_store.h"
#include "store/block_sym.h"
#include "store/col_block.h"
#include "store/col.h"
#include "store/splay.h"
#include "mem/heap.h"
#include "mem/sys.h"
#include "table/sym.h"
#include "table/domain.h"
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
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, missing, 2, &options, &scan), RAY_ERR_SCHEMA);
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

static test_result_t subset_ranges(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    char pinned[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    ray_block_scan_options_t options = {2, 9, 5, 60, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    size_t y_only[] = {1};
    uint64_t start = UINT64_MAX;
    ray_t* filter = ray_block_scan_next_columns(scan, y_only, 1, &start);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(start, 2); TEST_ASSERT_EQ_I(ray_table_nrows(filter), 5);
    TEST_ASSERT_EQ_I(ray_table_ncols(filter), 1);
    TEST_ASSERT_EQ_I(ray_table_col_name(filter, 0), ray_sym_intern("y", 1));
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(filter, 0)))[4], 1006);
    ray_release(filter);
    for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
    size_t reverse[] = {1, 0};
    ray_t* retained = ray_block_scan_read_columns(scan, reverse, 2, 4, 3);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_nrows(retained), 3);
    TEST_ASSERT_EQ_I(ray_table_col_name(retained, 0), ray_sym_intern("y", 1));
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(retained, 0)))[0], 1004);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[2], 106);
    filter = ray_block_scan_next_columns(scan, NULL, 1, &start);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(start, 7); TEST_ASSERT_EQ_I(ray_table_nrows(filter), 4);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(filter, 0)))[3], 110);
    ray_release(filter);
    start = UINT64_MAX;
    TEST_ASSERT_TRUE(ray_block_scan_next_columns(scan, NULL, 1, &start) == NULL);
    TEST_ASSERT_TRUE(start == UINT64_MAX);
    filter = ray_block_scan_read_columns(scan, NULL, 1, 2, 1);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(filter, 0)))[0], 102);
    ray_release(filter);
    filter = ray_block_scan_read_columns(scan, reverse, 2, 11, 0);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(ray_table_nrows(filter), 0);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(filter, 0)->type, RAY_I32);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(filter, 1)->type, RAY_I64);
    ray_release(filter);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(5, 16, 16, 5); TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[0], 104);
    ray_release(retained);
    PASS();
}

static test_result_t subset_failures(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {2, 9, 5, 60, 64};
    const size_t invalid[] = {SIZE_MAX}, duplicate[] = {1, 1};
    for (unsigned which = 0; which < 14; which++) {
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        uint64_t start = UINT64_MAX;
        ray_t* err = NULL;
        const char* code = "range";
        switch (which) {
        case 0: err = ray_block_scan_next_columns(scan, NULL, 0, &start); code = "domain"; break;
        case 1: err = ray_block_scan_next_columns(scan, NULL, 3, &start); code = "domain"; break;
        case 2: err = ray_block_scan_next_columns(scan, invalid, 1, &start); break;
        case 3: err = ray_block_scan_next_columns(scan, duplicate, 2, &start); code = "domain"; break;
        case 4: err = ray_block_scan_read_columns(scan, NULL, 1, 1, 1); break;
        case 5: err = ray_block_scan_read_columns(scan, NULL, 1, 12, 0); break;
        case 6: err = ray_block_scan_read_columns(scan, NULL, 1, 10, 2); break;
        case 7: err = ray_block_scan_read_columns(scan, NULL, 1, 2, 6); break;
        case 8: err = ray_block_scan_read_columns(scan, NULL, 1, UINT64_MAX, 1); break;
        case 9: err = ray_block_scan_read_columns(scan, NULL, 1, 2, UINT64_MAX); break;
        case 10: err = ray_block_scan_read_columns(scan, duplicate, 2, 2, 1); code = "domain"; break;
        case 11: err = ray_block_scan_read_columns(scan, invalid, 1, 2, 1); break;
        case 12:
            ray_block_scan_cancel(scan);
            err = ray_block_scan_read_columns(scan, NULL, 1, 2, 0); code = "cancel"; break;
        case 13:
            ray_block_scan_cancel(scan);
            err = ray_block_scan_next_columns(scan, NULL, 1, &start); code = "cancel"; break;
        }
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), code);
        TEST_ASSERT_TRUE(start == UINT64_MAX); ray_error_free(err);
        err = ray_block_scan_read_columns(scan, NULL, 1, 2, 1);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    ray_t* err = ray_block_scan_read_columns(NULL, NULL, 1, 0, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
    err = ray_block_scan_next_columns(NULL, NULL, 1, NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
    PASS();
}

static test_result_t subset_payloads(void) {
    publish(1, 16, 16, 1);
    /* Open validates y's metadata, but filter-only x reads must not decode y. */
    damage("y", RAY_COL_BLOCK_HEADER);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, 5, 5, 40, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next_columns(scan, NULL, 1, NULL);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    size_t y_only[] = {1};
    batch = ray_block_scan_read_columns(scan, y_only, 1, 0, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    batch = ray_block_scan_read_columns(scan, y_only, 1, 0, 1);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt"); ray_error_free(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(2, 16, 16, 2);
    for (unsigned which = 0; which < 3; which++) {
        options.payload_limit = which == 0 ? 39 : 40;
        options.scratch_limit = which == 1 ? 63 : 64;
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        batch = ray_block_scan_read_columns(scan, NULL, which == 2 ? 2 : 1, 0, 5);
        TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_error_free(batch);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    PASS();
}

static void schema_path(char* path, size_t capacity) {
    char dir[1024];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("schema fixture resolve");
    int n = snprintf(path, capacity, "%s/.d", dir);
    if (n < 0 || (size_t)n >= capacity) ray_test_fatal("schema fixture path");
}
static void write_schema(ray_t* schema) {
    if (!schema || RAY_IS_ERR(schema)) ray_test_fatal("schema fixture allocation");
    char path[1100]; schema_path(path, sizeof(path));
    ray_err_t err = ray_col_save(schema, path);
    ray_release(schema);
    if (err) ray_test_fatal("schema fixture save");
}

static test_result_t schema_membership(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    int64_t baseline = mapped();
    char dir[1024], from[1100], to[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(from, sizeof(from), "%s/x", dir); snprintf(to, sizeof(to), "%s/rogue", dir);
    TEST_ASSERT_EQ_I(rename(from, to), 0);
    const char* const rogue[] = {"rogue"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, rogue, 1, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(rename(to, from), 0);
    /* Declaration without a selected data file is an I/O failure, not a schema failure. */
    const char* schema_names[] = {"x", "ghost"};
    const uint32_t lengths[] = {1, 5};
    write_schema(ray_str_vec_from_parts(schema_names, lengths, NULL, 2));
    const char* const ghost[] = {"ghost"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, ghost, 1, &options, &scan), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    /* No failed open retained a lease: this generation may be pruned. */
    publish(2, 16, 16, 2); publish(3, 16, 16, 3);
    TEST_ASSERT_EQ_I(access(dir, F_OK), -1);
    PASS();
}

static test_result_t schema_corruption(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    int64_t baseline = mapped();
    const char* bad[] = {"", ".hidden", "../y", "a/b", "a\\b", "y\0tail", "x"};
    const uint32_t sizes[] = {0, 7, 4, 3, 3, 6, 1};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); i++) {
        const char* entries[] = {"x", bad[i]};
        uint32_t lengths[] = {1, sizes[i]};
        write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
        TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    write_schema(ray_vec_new(RAY_I64, 0));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    write_schema(ray_vec_new(RAY_STR, 0));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_SCHEMA);
    char path[1100]; schema_path(path, sizeof(path));
    TEST_ASSERT_EQ_I(unlink(path), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_IO);
    FILE* f = fopen(path, "wb"); TEST_ASSERT_NOT_NULL(f); TEST_ASSERT_EQ_I(fclose(f), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    f = fopen(path, "wb"); TEST_ASSERT_NOT_NULL(f);
    int truncated = ftruncate(fileno(f), RAY_BLOCK_SCAN_SCHEMA_MAX_BYTES + 1);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_LIMIT);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t schema_limits(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64};
    int64_t baseline = mapped();
    ray_t* schema = ray_vec_new(RAY_STR, RAY_BLOCK_SCAN_MAX_COLUMNS);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    for (unsigned i = 0; i < RAY_BLOCK_SCAN_MAX_COLUMNS; i++) {
        char name[32];
        int n = i ? snprintf(name, sizeof(name), "column%u", i) : snprintf(name, sizeof(name), "x");
        schema = ray_str_vec_append(schema, name, (size_t)n);
        TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    }
    char path[1100]; schema_path(path, sizeof(path));
    TEST_ASSERT_EQ_I(ray_col_save(schema, path), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    schema = ray_str_vec_append(schema, "extra", 5);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); write_schema(schema);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_LIMIT);
    char long_name[256]; memset(long_name, 'a', sizeof(long_name));
    const char* entries[] = {"x", long_name};
    uint32_t lengths[] = {1, 255};
    write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    lengths[1] = 256;
    write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    /* Truncated serialized schema must fail in the existing column decoder. */
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    int truncated = ftruncate(fileno(f), 31);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static ray_t* store_table(unsigned epoch, unsigned rows) {
    ray_t* t = ray_table_new(2);
    for (unsigned c = 0; c < 2; c++) {
        ray_t* col = ray_vec_new(c ? RAY_I32 : RAY_I64, rows);
        if (!col || RAY_IS_ERR(col)) ray_test_fatal("store table allocation");
        for (unsigned i = 0; i < rows; i++) {
            if (c) ((int32_t*)ray_data(col))[i] = epoch * 1000 + i;
            else ((int64_t*)ray_data(col))[i] = i == 1 ? INT64_MIN : epoch * 100 + i;
        }
        col->len = rows;
        if (!c) col->attrs |= RAY_ATTR_HAS_NULLS;
        t = ray_table_add_col(t, ray_sym_intern(names[c], 1), col); ray_release(col);
        if (!t || RAY_IS_ERR(t)) ray_test_fatal("store table assembly");
    }
    return t;
}

static test_result_t store_roundtrip(void) {
    char target[128], path[1300], pinned[1024];
    snprintf(target, sizeof(target), "%s/new/table", root);
    ray_block_store_options_t options = {64, 1, true};
    ray_t* table = store_table(1, 16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    ray_release(table);
    snprintf(path, sizeof(path), "%s/.d", target); TEST_ASSERT_EQ_I(access(path, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, pinned, sizeof(pinned)), RAY_OK);
    TEST_ASSERT_TRUE(strcmp(pinned, target) != 0);
    ray_block_scan_options_t scan_options = {0, UINT64_MAX, 5, 60, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    ray_t* first = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(first); TEST_ASSERT_FALSE(RAY_IS_ERR(first));
    TEST_ASSERT_TRUE(ray_vec_is_null(ray_table_get_col_idx(first, 0), 1));
    for (unsigned e = 2; e <= 4; e++) {
        table = store_table(e, 16);
        options.codec = e % 2; options.durable = false;
        TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
        ray_release(table);
    }
    TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
    unsigned row = 5;
    ray_t* batch;
    while ((batch = ray_block_scan_next(scan))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
        ray_t* x = ray_table_get_col_idx(batch, 0);
        for (int64_t i = 0; i < x->len; i++)
            TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[i], 100 + row + i);
        row += (unsigned)x->len; ray_release(batch);
    }
    TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(first, 0)))[0], 100);
    ray_release(first);
    table = store_table(5, 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

static test_result_t store_preflight(void) {
    char target[128]; snprintf(target, sizeof(target), "%s/preflight", root);
    ray_block_store_options_t options = {64, 1, false};
    ray_t* table = store_table(1, 16);
    ray_t* x = ray_table_get_col_idx(table, 0);
    ray_t* y = ray_table_get_col_idx(table, 1);
    y->len--;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_LENGTH); y->len++;
    x->attrs |= RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_NYI);
    x->attrs &= (uint8_t)~RAY_ATTR_SORTED;
    ray_table_set_col_name(table, 1, ray_sym_intern("../bad", 6));
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    ray_table_set_col_name(table, 1, ray_sym_intern("x", 1));
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    ray_table_set_col_name(table, 1, ray_sym_intern("y", 1));
    options.block_bytes = 7;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_RANGE);
    options.block_bytes = 64; options.codec = 2;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    options.codec = 1;
    TEST_ASSERT_EQ_I(ray_block_store_save(x, target, &options), RAY_ERR_TYPE);
    ray_t* unsupported = ray_table_new(1);
    ray_t* str = ray_vec_new(RAY_LIST, 0);
    unsupported = ray_table_add_col(unsupported, ray_sym_intern("s", 1), str); ray_release(str);
    TEST_ASSERT_EQ_I(ray_block_store_save(unsupported, target, &options), RAY_ERR_TYPE);
    ray_release(unsupported);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    char before[1024], after[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, before, sizeof(before)), RAY_OK);
    x->attrs |= RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_NYI);
    x->attrs &= (uint8_t)~RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(before, after); ray_release(table);
    PASS();
}

static test_result_t store_rollback(void) {
    char target[128], manifest[140], staged[1024];
    snprintf(target, sizeof(target), "%s/rollback", root);
    snprintf(manifest, sizeof(manifest), "%s/.current", target);
    ray_splay_write_t write;
    TEST_ASSERT_EQ_I(ray_splay_write_begin_staged(target, &write), RAY_OK);
    TEST_ASSERT_TRUE(write.staged); snprintf(staged, sizeof(staged), "%s", write.dir);
    TEST_ASSERT_EQ_I(access(manifest, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_write_finish(&write, RAY_ERR_IO, false), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(access(staged, F_OK), -1); TEST_ASSERT_EQ_I(access(manifest, F_OK), -1);
    /* Obstruct the final rename, after the whole new generation is written. */
    TEST_ASSERT_EQ_I(ray_test_mkdir_p(manifest), 0);
    ray_t* table = store_table(1, 16);
    ray_block_store_options_t options = {64, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(rmdir(manifest), 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    char before[1024], after[1024], column[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, before, sizeof(before)), RAY_OK);
    TEST_ASSERT_EQ_I(ray_splay_write_begin_staged(target, &write), RAY_OK);
    snprintf(staged, sizeof(staged), "%s", write.dir);
    snprintf(column, sizeof(column), "%s/x", write.dir);
    TEST_ASSERT_EQ_I(ray_col_save(ray_table_get_col_idx(table, 0), column), RAY_OK);
    TEST_ASSERT_EQ_I(ray_splay_write_finish(&write, RAY_ERR_IO, false), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(access(staged, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(before, after);
    ray_block_scan_options_t scan_options = {0, UINT64_MAX, 5, 60, 64};
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); ray_release(table);
    PASS();
}

/* Reversed local dictionaries deliberately give equal strings different IDs. */
static ray_t* symbol_table(uint8_t width, unsigned rows) {
    ray_t* table = store_table(1, rows);
    ray_sym_domain_t* dom = ray_sym_domain_new();
    if (!dom || ray_sym_domain_intern(dom, "beta", 4) != 1 ||
        ray_sym_domain_intern(dom, "alpha", 5) != 2) ray_test_fatal("symbol fixture domain");
    const char* text[] = {"", "alpha", "beta"};
    for (unsigned c = 0; c < 2; c++) {
        ray_t* v = ray_sym_vec_new(width, rows);
        if (!v || RAY_IS_ERR(v)) ray_test_fatal("symbol fixture vector");
        if (c) { v->sym_domain = dom; ray_sym_domain_retain(dom); }
        for (unsigned i = 0; i < rows; i++) {
            unsigned which = i % 3;
            int64_t pos = c ? (which ? 3 - which : 0) : ray_sym_intern(text[which], strlen(text[which]));
            ray_write_sym(ray_data(v), i, (uint64_t)pos, RAY_SYM, width);
        }
        v->len = rows; v->attrs |= RAY_ATTR_HAS_NULLS;
        table = ray_table_add_col(table, ray_sym_intern(c ? "local" : "global", c ? 5 : 6), v);
        ray_release(v);
        if (!table || RAY_IS_ERR(table)) ray_test_fatal("symbol fixture table");
    }
    ray_sym_domain_release(dom);
    return table;
}

static test_result_t store_symbols(void) {
    const char* projection[] = {"global", "local", "x"};
    const char* text[] = {"", "alpha", "beta"};
    ray_block_store_options_t options = {64, 1, true};
    ray_block_scan_options_t read = {1, 15, 5, 120, 64};
    for (uint8_t width = RAY_SYM_W8; width <= RAY_SYM_W64; width++) {
        ray_t* table = symbol_table(width, 16);
        options.codec = width % 2;
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
        ray_t* schema = ray_block_scan_schema(scan);
        TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
        TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_SYM);
        TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0); ray_release(schema);
        unsigned row = 1;
        ray_t* batch;
        while ((batch = ray_block_scan_next(scan))) {
            TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
            ray_t* a = ray_table_get_col_idx(batch, 0);
            ray_t* b = ray_table_get_col_idx(batch, 1);
            TEST_ASSERT_TRUE(ray_sym_vec_domain(a) == ray_sym_vec_domain(b));
            TEST_ASSERT_TRUE(ray_sym_vec_domain(a) != ray_sym_runtime_domain());
            for (int64_t j = 0; j < a->len; j++) {
                const char* expected = text[(row + j) % 3];
                ray_t* sa = ray_sym_vec_cell(a, j); ray_t* sb = ray_sym_vec_cell(b, j);
                TEST_ASSERT_NOT_NULL(sa); TEST_ASSERT_NOT_NULL(sb);
                TEST_ASSERT_EQ_I(ray_str_len(sa), strlen(expected));
                TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(sa), expected, strlen(expected)));
                TEST_ASSERT_TRUE(sa == sb);
                TEST_ASSERT_EQ_I(ray_vec_is_null(a, j), (row + j) % 3 == 0);
            }
            row += (unsigned)a->len; ray_release(batch);
        }
        TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    }
    /* Release all inputs, then rebuild runtime IDs in a different order. */
    ray_sym_destroy(); ray_sym_init();
    TEST_ASSERT_TRUE(ray_sym_intern("unrelated", 9) >= 0);
    TEST_ASSERT_TRUE(ray_sym_intern("beta", 4) >= 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
    ray_t* retained = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    ray_block_scan_close(&scan);
    char pinned[1024]; TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    for (unsigned i = 0; i < 3; i++) {
        ray_t* replacement = store_table(i, 0);
        TEST_ASSERT_EQ_I(ray_block_store_save(replacement, root, &options), RAY_OK); ray_release(replacement);
    }
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    ray_t* v = ray_table_get_col_idx(retained, 0);
    ray_t* s = ray_sym_vec_cell(v, 0);
    TEST_ASSERT_EQ_I(ray_str_len(s), 5); TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(s), "alpha", 5));
    char other[128]; snprintf(other, sizeof(other), "%s/resave", root);
    TEST_ASSERT_EQ_I(ray_block_store_save(retained, other, &options), RAY_OK);
    ray_release(retained);
    read.start = 0; read.count = UINT64_MAX;
    ray_t* empty = symbol_table(RAY_SYM_W8, 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(empty, root, &options), RAY_OK); ray_release(empty);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

static test_result_t symbol_failures(void) {
    const char* projection[] = {"global", "local"};
    ray_block_store_options_t options = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 80, 64};
    ray_t* table = symbol_table(RAY_SYM_W64, 16);
    ray_t* col = ray_table_get_col_idx(table, 2);
    ((uint64_t*)ray_data(col))[0] = UINT64_MAX;
    char target[128]; snprintf(target, sizeof(target), "%s/invalid", root);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    ((uint64_t*)ray_data(col))[0] = 0;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    int64_t baseline = mapped();
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    ray_t* error = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(error)); TEST_ASSERT_STR_EQ(ray_err_code(error), "limit"); ray_release(error);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline); read.payload_limit++;
    damage(".sym", 20);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    /* A numeric projection is independent of an unselected symbol dictionary. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &read, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/.sym", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, 4, SEEK_SET), 0);
    uint64_t forged_count = UINT64_MAX, checked_count;
    uint32_t checked_crc;
    TEST_ASSERT_EQ_I(fwrite(&forged_count, 8, 1, f), 1);
    TEST_ASSERT_EQ_I(fflush(f), 0);
    TEST_ASSERT_EQ_I(ray_block_sym_checksum(path, &checked_count, &checked_crc), RAY_ERR_CORRUPT);
    int truncated = ftruncate(fileno(f), RAY_BLOCK_SYM_MAX_BYTES + 1);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_LIMIT);
    TEST_ASSERT_EQ_I(unlink(path), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_IO);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static const char* string_parts[] = {
    "", "abcdefghijkl", "abcdefghijklm", "a\0b",
    "012345678901234567890123456789012345678901234567890123456789"
};
static const uint32_t string_lengths[] = {0, 12, 13, 3, 60};

static ray_t* string_table(unsigned rows) {
    const char* parts[16]; uint32_t lengths[16];
    for (unsigned i = 0; i < rows; i++) {
        parts[i] = string_parts[i % 5]; lengths[i] = string_lengths[i % 5];
    }
    ray_t* table = symbol_table(RAY_SYM_W8, rows);
    ray_t* col = ray_str_vec_from_parts(parts, lengths, NULL, rows);
    if (!col || RAY_IS_ERR(col)) ray_test_fatal("string fixture vector");
    table = ray_table_add_col(table, ray_sym_intern("s", 1), col);
    if (!table || RAY_IS_ERR(table)) ray_test_fatal("string fixture table");
    table = ray_table_add_col(table, ray_sym_intern("t", 1), col); ray_release(col);
    if (!table || RAY_IS_ERR(table)) ray_test_fatal("string fixture table");
    return table;
}

static test_result_t subset_variable_width(void) {
    const char* projection[] = {"s", "local", "x", "t"};
    const size_t numeric[] = {2}, result[] = {1, 0};
    ray_block_store_options_t options = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 109, 64};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    /* Neither reading x nor materializing local/s should decode the unused t. */
    damage("t", RAY_COL_BLOCK_HEADER);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    uint64_t start = UINT64_MAX;
    ray_t* batch = ray_block_scan_next_columns(scan, numeric, 1, &start);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(start, 0); ray_release(batch);
    /* 4 symbol codes + 4 STR descriptors + 13 pooled bytes = 109 bytes. */
    ray_t* retained = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_ncols(retained), 2);
    batch = ray_block_scan_read_columns(scan, result, 2, 16, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(ray_table_nrows(batch), 0);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(batch, 0)->type, RAY_SYM);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(batch, 1)->type, RAY_STR); ray_release(batch);
    for (unsigned i = 0; i < 3; i++) {
        table = string_table(0);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    }
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    const char* symbols[] = {"", "alpha", "beta", ""};
    for (int64_t i = 0; i < 4; i++) {
        ray_t* symbol = ray_sym_vec_cell(ray_table_get_col_idx(retained, 0), i);
        TEST_ASSERT_NOT_NULL(symbol);
        TEST_ASSERT_EQ_I(ray_str_len(symbol), strlen(symbols[i]));
        TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(symbol), symbols[i], strlen(symbols[i])));
        size_t len;
        const char* str = ray_str_vec_get(ray_table_get_col_idx(retained, 1), i, &len);
        TEST_ASSERT_EQ_I(len, string_lengths[i]);
        TEST_ASSERT_TRUE(!memcmp(str, string_parts[i], len));
    }
    ray_release(retained);
    table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    int64_t baseline = mapped();
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_error_free(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit = 0; read.scratch_limit = 0;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t store_strings(void) {
    const char* projection[] = {"s", "local", "x", "t"};
    ray_block_store_options_t options = {64, 1, true};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 1024, 64};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    ray_t* schema = ray_block_scan_schema(scan);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_STR); ray_release(schema);
    ray_t* first = NULL;
    unsigned row = 0;
    ray_t* batch;
    while ((batch = ray_block_scan_next(scan))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
        ray_t* col = ray_table_get_col_idx(batch, 0);
        ray_t* other = ray_table_get_col_idx(batch, 3);
        TEST_ASSERT_EQ_I(col->len, 4);
        for (int64_t j = 0; j < col->len; j++) {
            unsigned which = (row + j) % 5;
            size_t len; const char* s = ray_str_vec_get(col, j, &len);
            TEST_ASSERT_EQ_I(len, string_lengths[which]);
            TEST_ASSERT_TRUE(!memcmp(s, string_parts[which], len));
            size_t n; const char* t = ray_str_vec_get(other, j, &n);
            TEST_ASSERT_EQ_I(n, len); TEST_ASSERT_TRUE(!memcmp(s, t, len));
            TEST_ASSERT_EQ_I(ray_vec_is_null(col, j), which == 0);
        }
        if (!row) {
            first = batch;
            for (unsigned i = 0; i < 3; i++) {
                table = string_table(0); options.codec = 0; options.durable = false;
                TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
            }
        } else ray_release(batch);
        row += 4;
    }
    TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    size_t len; const char* s = ray_str_vec_get(ray_table_get_col_idx(first, 0), 2, &len);
    TEST_ASSERT_EQ_I(len, 13); TEST_ASSERT_TRUE(!memcmp(s, string_parts[2], len));
    TEST_ASSERT_EQ_I(ray_block_store_save(first, root, &options), RAY_OK); ray_release(first);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    table = string_table(0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

static test_result_t string_limits(void) {
    const char* projection[] = {"s", "t"};
    ray_block_store_options_t options = {64, 0, false};
    ray_t* table = string_table(16);
    char target[128]; snprintf(target, sizeof(target), "%s/too-long", root);
    options.block_bytes = 56;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    options.block_bytes = 64;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    ray_block_scan_options_t read = {0, 4, 4, 154, 44};
    int64_t baseline = mapped();
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_release(batch);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "domain"); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit++; read.scratch_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_release(batch);
    ray_block_scan_close(&scan);
    read.scratch_limit++; damage("s", RAY_COL_BLOCK_HEADER);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt"); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

const test_entry_t block_scan_entries[] = {
    {"block_scan/subset_variable_width", subset_variable_width, setup, teardown},
    {"block_scan/subset_ranges", subset_ranges, setup, teardown},
    {"block_scan/subset_failures", subset_failures, setup, teardown},
    {"block_scan/subset_payloads", subset_payloads, setup, teardown},
    {"block_scan/store_strings", store_strings, setup, teardown},
    {"block_scan/string_limits", string_limits, setup, teardown},
    {"block_scan/store_symbols", store_symbols, setup, teardown},
    {"block_scan/symbol_failures", symbol_failures, setup, teardown},
    {"block_scan/batches", batches, setup, teardown},
    {"block_scan/failures", failures, setup, teardown},
    {"block_scan/projection", projection, setup, teardown},
    {"block_scan/cancel_after_batch", cancel_after_batch, setup, teardown},
    {"block_scan/schema_membership", schema_membership, setup, teardown},
    {"block_scan/schema_corruption", schema_corruption, setup, teardown},
    {"block_scan/schema_limits", schema_limits, setup, teardown},
    {"block_scan/store_roundtrip", store_roundtrip, setup, teardown},
    {"block_scan/store_preflight", store_preflight, setup, teardown},
    {"block_scan/store_rollback", store_rollback, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
