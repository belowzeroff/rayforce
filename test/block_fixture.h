/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
/* Shared fixtures of the block scan and block query tests: one table root per
 * test, and small tables published raw, as block columns or mixed. */
#ifndef RAY_TEST_BLOCK_FIXTURE_H
#define RAY_TEST_BLOCK_FIXTURE_H
#include "test.h"
#include "store/block_scan.h"
#include "store/block_store.h"
#include "store/col_block.h"
#include "store/splay.h"
#include "mem/heap.h"
#include "mem/sys.h"
#include "ops/ops.h"
#include "table/sym.h"
#include "table/domain.h"
#include <stdio.h>
#include <string.h>

static char root[64];
static bool seeded;
static ray_block_scan_t* scan;
static const char* const names[] = {"x", "y"};

static inline int64_t mapped(void) {
    int64_t current, peak; ray_sys_get_mapped(&current, &peak); return current;
}
static inline void publish(unsigned epoch, unsigned rows, unsigned y_rows, unsigned y_epoch) {
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
static inline void damage(const char* name, long offset) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("scan test resolve");
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "r+b");
    if (!f || fseek(f, offset, SEEK_SET)) ray_test_fatal("scan test damage seek");
    int value = fgetc(f);
    if (value == EOF || fseek(f, offset, SEEK_SET) || fputc(value ^ 1, f) == EOF || fclose(f))
        ray_test_fatal("scan test damage");
}

static inline ray_t* store_table(unsigned epoch, unsigned rows) {
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

static inline ray_t* symbol_table(uint8_t width, unsigned rows) {
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

static const char* string_parts[] = {
    "", "abcdefghijkl", "abcdefghijklm", "a\0b",
    "012345678901234567890123456789012345678901234567890123456789"
};
static const uint32_t string_lengths[] = {0, 12, 13, 3, 60};

static inline ray_t* string_table(unsigned rows) {
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

#endif
