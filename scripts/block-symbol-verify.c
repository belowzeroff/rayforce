/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
/* Link against the engine objects, then run write ROOT and read ROOT as
 * separate processes. The reader deliberately interns symbols in a different
 * order. This also verifies that output vectors retain their dictionary. */
#include "store/block_scan.h"
#include "store/block_store.h"
#include "mem/heap.h"
#include "table/sym.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char** argv) {
    if (argc != 3 || (strcmp(argv[1], "write") && strcmp(argv[1], "read"))) {
        fprintf(stderr, "usage: %s write|read ROOT\n", argv[0]);
        return 2;
    }
    ray_heap_init(); ray_sym_init();
    const char* values[] = {"", "ALPHA", "BETA", "ALPHA", "", "BETA"};
    if (!strcmp(argv[1], "write")) {
        assert(ray_sym_intern("writer-only", 11) >= 0);
        ray_t* v = ray_sym_vec_new(RAY_SYM_W64, 6);
        assert(v && !RAY_IS_ERR(v));
        for (int64_t i = 0; i < 6; i++) {
            int64_t id = ray_sym_intern(values[i], strlen(values[i]));
            assert(id >= 0);
            ray_write_sym(ray_data(v), i, (uint64_t)id, RAY_SYM, v->attrs);
        }
        v->len = 6;
        ray_t* table = ray_table_new(1);
        assert(table && !RAY_IS_ERR(table));
        table = ray_table_add_col(table, ray_sym_intern("s", 1), v);
        ray_release(v);
        assert(table && !RAY_IS_ERR(table));
        ray_block_store_options_t options = {32, 1, true};
        assert(ray_block_store_save(table, argv[2], &options) == RAY_OK);
        ray_release(table);
    } else {
        assert(ray_sym_intern("BETA", 4) == 1);
        assert(ray_sym_intern("reader-only", 11) >= 0);
        const char* columns[] = {"s"};
        ray_block_scan_options_t options = {0, UINT64_MAX, 6, 48, 32};
        ray_block_scan_t* scan = NULL;
        assert(ray_block_scan_open(argv[2], columns, 1, &options, &scan) == RAY_OK);
        ray_t* batch = ray_block_scan_next(scan);
        assert(batch && !RAY_IS_ERR(batch));
        assert(ray_block_scan_next(scan) == NULL);
        ray_block_scan_close(&scan);
        ray_t* v = ray_table_get_col_idx(batch, 0);
        assert(v->len == 6 && v->type == RAY_SYM);
        for (int64_t i = 0; i < 6; i++) {
            ray_t* s = ray_sym_vec_cell(v, i);
            assert(s && ray_str_len(s) == strlen(values[i]));
            assert(!memcmp(ray_str_ptr(s), values[i], strlen(values[i])));
        }
        ray_release(batch);
    }
    ray_sym_destroy(); ray_heap_destroy();
    printf("%s: ok\n", argv[1]);
    return 0;
}
