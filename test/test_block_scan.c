/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _POSIX_C_SOURCE 200809L
#include "test.h"
#include "store/block_scan.h"
#include "store/block_store.h"
#include "store/col_block.h"
#include "store/col.h"
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
    ray_t* str = ray_vec_new(RAY_STR, 0);
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

const test_entry_t block_scan_entries[] = {
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
