/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _DEFAULT_SOURCE   /* mkdtemp, strdup */
#include "block_fixture.h"
#include "ops/block_query.h"
#include "ops/ops.h"
#include "mem/heap.h"
#include <stdlib.h>
#include <unistd.h>

static ray_block_query_t* query;

static void setup(void) {
    ray_heap_init(); ray_sym_init();
    strcpy(root, "/tmp/ray-block-query-XXXXXX");
    if (!mkdtemp(root)) ray_test_fatal("query test directory");
    seeded = false;
}
static void teardown(void) {
    ray_block_query_close(&query);
    ray_block_scan_close(&scan);
    ray_test_rm_rf(root);
    ray_sym_destroy(); ray_heap_destroy();
}

static test_result_t query_late_materialization(void) {
    const char* output[] = {"s", "local"};
    ray_block_store_options_t write = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 256, 64, 0};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    damage("s", RAY_COL_BLOCK_HEADER);
    ray_t* value = ray_i64(INT64_MAX);
    ray_block_predicate_t pred = {"x", OP_EQ, value};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_t* schema = ray_block_query_schema(query);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0);
    TEST_ASSERT_EQ_I(ray_table_ncols(schema), 2); ray_release(schema);
    ray_block_query_close(&query);
    pred.opcode = OP_LT;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    ray_release(value);
    ray_t* err = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "corrupt"); ray_release(err);
    err = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_release(err);
    ray_block_query_close(&query);
    table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    value = ray_i64(INT64_MAX); pred.value = value;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    value->i64 = INT64_MIN; ray_release(value); /* Open owns a copy, not an alias. */
    ray_t* retained = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_nrows(retained), 4);
    for (unsigned i = 0; i < 3; i++) {
        table = string_table(0);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    }
    unsigned rows = 4;
    ray_t* batch;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); rows += ray_table_nrows(batch); ray_release(batch);
    }
    TEST_ASSERT_EQ_I(rows, 16);
    ray_block_query_close(&query);
    size_t len;
    const char* str = ray_str_vec_get(ray_table_get_col_idx(retained, 0), 2, &len);
    TEST_ASSERT_EQ_I(len, 13); TEST_ASSERT_TRUE(!memcmp(str, string_parts[2], len));
    ray_t* sym = ray_sym_vec_cell(ray_table_get_col_idx(retained, 1), 1);
    TEST_ASSERT_EQ_I(ray_str_len(sym), 5); TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(sym), "alpha", 5));
    ray_release(retained);
    PASS();
}

static test_result_t query_conjunction(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {2, 12, 5, 60, 64, 0};
    ray_t* lo = ray_i64(103); ray_t* hi = ray_i64(112); ray_t* hole = ray_i64(105);
    ray_t* y_lo = ray_i32(1002);
    ray_block_predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    for (unsigned i = 0; i < RAY_BLOCK_QUERY_MAX_PREDICATES; i++)
        predicates[i] = (ray_block_predicate_t){"x", OP_GE, lo};
    predicates[1] = (ray_block_predicate_t){"x", OP_LE, hi};
    predicates[2] = (ray_block_predicate_t){"x", OP_NE, hole};
    predicates[3] = (ray_block_predicate_t){"y", OP_GT, y_lo};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, predicates,
                     RAY_BLOCK_QUERY_MAX_PREDICATES, &read, &query), RAY_OK);
    ray_release(lo); ray_release(hi); ray_release(hole); ray_release(y_lo);
    unsigned expected = 1003, rows = 0;
    ray_t* batch;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); TEST_ASSERT_EQ_I(ray_table_ncols(batch), 1);
        ray_t* col = ray_table_get_col_idx(batch, 0);
        for (int64_t i = 0; i < col->len; i++) {
            if (expected == 1005) expected++;
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], expected++); rows++;
        }
        ray_release(batch);
        if (rows == 2) for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
    }
    TEST_ASSERT_EQ_I(rows, 9); TEST_ASSERT_EQ_I(expected, 1013);
    PASS();
}

static test_result_t query_native_parity(void) {
    const int8_t types[] = {RAY_BOOL, RAY_U8, RAY_I16, RAY_I32, RAY_I64,
                           RAY_DATE, RAY_TIME, RAY_TIMESTAMP};
    const uint16_t ops[] = {OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE};
    const char* output[] = {"id"};
    ray_block_store_options_t write = {64, 1, false};
    ray_block_scan_options_t read = {1, 15, 5, 80, 64, 0};
    for (unsigned t = 0; t < sizeof(types); t++) {
        int8_t type = types[t];
        ray_t* col = ray_vec_new(type, 16); col->len = 16;
        int64_t ids[16];
        for (int64_t i = 0; i < 16; i++) {
            ids[i] = i;
            int64_t v = i % 3;
            switch (type) {
            case RAY_BOOL: ((uint8_t*)ray_data(col))[i] = v != 0; break;
            case RAY_U8: ((uint8_t*)ray_data(col))[i] = (uint8_t)v; break;
            case RAY_I16: ((int16_t*)ray_data(col))[i] = (int16_t)v; break;
            case RAY_I32: case RAY_DATE: case RAY_TIME: ((int32_t*)ray_data(col))[i] = (int32_t)v; break;
            default: ((int64_t*)ray_data(col))[i] = v; break;
            }
        }
        if (type != RAY_BOOL && type != RAY_U8) ray_vec_set_null(col, 7, true);
        ray_t* id = ray_vec_from_raw(RAY_I64, ids, 16);
        ray_t* table = ray_table_new(2);
        table = ray_table_add_col(table, ray_sym_intern("v", 1), col);
        table = ray_table_add_col(table, ray_sym_intern("id", 2), id);
        ray_release(col); ray_release(id);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK);
        for (unsigned null = 0; null < 2; null++) {
            ray_t* atom = null ? ray_typed_null(-type) : ray_i64(1);
            if (!null) atom->type = -type;
            for (unsigned op = 0; op < sizeof(ops) / sizeof(*ops); op++) {
                ray_graph_t* g = ray_graph_new(table);
                ray_op_t* source = ray_scan(g, "v");
                ray_op_t* literal = ray_const_atom(g, atom);
                ray_t* reference = ray_execute(g, ray_binop(g, ops[op], source, literal));
                TEST_ASSERT_NOT_NULL(reference); TEST_ASSERT_FALSE(RAY_IS_ERR(reference));
                TEST_ASSERT_EQ_I(reference->type, RAY_BOOL);
                ray_graph_free(g);
                ray_block_predicate_t pred = {"v", ops[op], atom};
                TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
                unsigned expected = 1;
                ray_t* batch;
                while ((batch = ray_block_query_next(query))) {
                    TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
                    ray_t* values = ray_table_get_col_idx(batch, 0);
                    for (int64_t i = 0; i < values->len; i++) {
                        while (expected < 16 && !((uint8_t*)ray_data(reference))[expected]) expected++;
                        TEST_ASSERT_TRUE(expected < 16);
                        TEST_ASSERT_EQ_I(((int64_t*)ray_data(values))[i], expected++);
                    }
                    ray_release(batch);
                }
                while (expected < 16 && !((uint8_t*)ray_data(reference))[expected]) expected++;
                TEST_ASSERT_EQ_I(expected, 16);
                ray_release(reference); ray_block_query_close(&query);
            }
            ray_release(atom);
        }
        ray_release(table);
    }
    PASS();
}

static test_result_t query_zone_pruning(void) {
    publish(1, 16, 16, 1);
    damage("x", RAY_COL_BLOCK_HEADER);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {0, 16, 8, 64, 64, 0};
    ray_t* atom = ray_i64(108);
    ray_block_predicate_t pred = {"x", OP_GE, atom};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_t* batch = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(ray_table_nrows(batch), 8);
    ray_t* y = ray_table_get_col_idx(batch, 0);
    for (int64_t i = 0; i < 8; i++)
        TEST_ASSERT_EQ_I(((int32_t*)ray_data(y))[i], 1008 + i);
    ray_release(batch);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_block_query_close(&query);

    pred.opcode = OP_LT;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(atom);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt");
    ray_release(batch);
    PASS();
}

/* Release gate 2: on a sorted column, a narrow range decodes only the blocks
 * it overlaps; every excluded block stays undecoded, in the filter column and
 * in the late-materialized output column. */
static test_result_t gate_range_pruning(void) {
    struct { uint32_t rows, block_bytes, batch_rows, lo, hi; uint64_t blocks; } cases[] = {
        {4096, 512, 64, 1000, 1064, 4},               /* batch == I64 block */
        {4096, 512, 4096, 1000, 1064, 4},             /* one read: trimmed to the range's blocks */
        /* Default batch, 64 KiB blocks: batches are finer than blocks, so the
         * next batch inside the matching t block decodes that block again. */
        {524288, 65536, 4096, 100000, 100100, 3}
    };
    for (unsigned c = 0; c < sizeof(cases) / sizeof(*cases); c++) {
        uint32_t rows = cases[c].rows;
        ray_t* t = ray_vec_new(RAY_I64, rows);
        ray_t* v = ray_vec_new(RAY_I32, rows);
        for (uint32_t i = 0; i < rows; i++) {
            ((int64_t*)ray_data(t))[i] = i;
            ((int32_t*)ray_data(v))[i] = (int32_t)(i * 3);
        }
        t->len = v->len = rows;
        ray_t* table = ray_table_new(2);
        table = ray_table_add_col(table, ray_sym_intern("t", 1), t);
        table = ray_table_add_col(table, ray_sym_intern("v", 1), v);
        ray_release(t); ray_release(v);
        ray_block_store_options_t store = {cases[c].block_bytes, 1, false};
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &store), RAY_OK);
        ray_release(table);
        uint64_t total_blocks = (rows * 8ull + cases[c].block_bytes - 1) / cases[c].block_bytes +
                                (rows * 4ull + cases[c].block_bytes - 1) / cases[c].block_bytes;
        const char* output[] = {"v"};
        ray_t* lo = ray_i64(cases[c].lo); ray_t* hi = ray_i64(cases[c].hi);
        ray_block_predicate_t pred[] = {{"t", OP_GE, lo}, {"t", OP_LT, hi}};
        ray_block_scan_options_t read = {0, UINT64_MAX, cases[c].batch_rows, 1u << 20, 1u << 20, 0};
        TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, pred, 2, &read, &query), RAY_OK);
        ray_release(lo); ray_release(hi);
        uint64_t matched = 0;
        for (ray_t* b; (b = ray_block_query_next(query)); ray_release(b)) {
            TEST_ASSERT_FALSE(RAY_IS_ERR(b));
            ray_t* col = ray_table_get_col_idx(b, 0);
            for (int64_t i = 0; i < col->len; i++, matched++)
                TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], (int32_t)((cases[c].lo + matched) * 3));
        }
        TEST_ASSERT_EQ_I(matched, cases[c].hi - cases[c].lo);
        ray_block_query_stats_t stats;
        ray_block_query_stats(query, &stats);
        TEST_ASSERT_EQ_I(stats.scan.blocks_decoded, cases[c].blocks);
        TEST_ASSERT_EQ_I(stats.scan.raw_rows, 0);
        uint64_t batches = (rows + cases[c].batch_rows - 1) / cases[c].batch_rows;
        TEST_ASSERT_TRUE(batches <= 2 || stats.batches_skipped >= batches - 2);
        TEST_ASSERT_TRUE(stats.scan.blocks_decoded * 10 <= total_blocks);
        ray_block_query_close(&query);
    }
    PASS();
}

/* Scattered matches: every output block is decoded once, not once per run. */
static test_result_t query_scattered_matches(void) {
    const uint32_t rows = 4096;
    ray_t* x = ray_vec_new(RAY_I64, rows);
    ray_t* v = ray_vec_new(RAY_I32, rows);
    for (uint32_t i = 0; i < rows; i++) {
        ((int64_t*)ray_data(x))[i] = i % 7;
        ((int32_t*)ray_data(v))[i] = (int32_t)i;
    }
    x->len = v->len = rows;
    ray_t* table = ray_table_new(2);
    table = ray_table_add_col(table, ray_sym_intern("x", 1), x);
    table = ray_table_add_col(table, ray_sym_intern("v", 1), v);
    ray_release(x); ray_release(v);
    ray_block_store_options_t store = {512, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &store), RAY_OK);
    ray_release(table);
    const char* output[] = {"v"};
    ray_t* three = ray_i64(3);
    ray_block_predicate_t pred = {"x", OP_EQ, three};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4096, 1u << 20, 1u << 20, 0};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(three);
    int64_t seen = 0;
    for (ray_t* b; (b = ray_block_query_next(query)); ray_release(b)) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(b));
        ray_t* col = ray_table_get_col_idx(b, 0);
        for (int64_t i = 0; i < col->len; i++, seen++)
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], (int32_t)(3 + 7 * seen));
    }
    TEST_ASSERT_EQ_I(seen, (rows - 3 + 6) / 7);
    ray_block_query_stats_t stats;
    ray_block_query_stats(query, &stats);
    /* 64 x blocks for the filter, 32 v blocks for the output. */
    TEST_ASSERT_EQ_I(stats.scan.blocks_decoded, 64 + 32);
    PASS();
}

static test_result_t query_admission(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 40, 64, 0};
    ray_t* atom = ray_i64(101);
    ray_block_predicate_t pred = {"x", OP_EQ, atom};
    int64_t baseline = mapped();
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 1, &read, &query), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 33, &read, &query), RAY_ERR_LIMIT);
    pred.opcode = OP_ADD;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_NYI);
    pred.opcode = OP_EQ; pred.column = "y";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_NYI);
    pred.column = "missing";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_SCHEMA);
    pred.column = "../x";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_DOMAIN);
    pred.column = "x"; atom->type = -RAY_F64;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_NYI);
    atom->type = -RAY_I64;
    TEST_ASSERT_TRUE(query == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(atom);
    ray_t* batch = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_query_cancel(query);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "cancel"); ray_release(batch);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "domain"); ray_release(batch);
    ray_block_query_close(&query); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &query), RAY_OK);
    unsigned rows = 0;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); rows += ray_table_nrows(batch); ray_release(batch);
    }
    TEST_ASSERT_EQ_I(rows, 16); ray_block_query_close(&query);
    read.count = 0;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &query), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_block_query_close(&query); ray_block_query_close(&query);
    ray_block_query_close(NULL); ray_block_query_cancel(NULL);
    batch = ray_block_query_next(NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); ray_release(batch);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t query_budgets_and_errors(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"x"};
    char filter_name[] = "y";
    ray_t* atom = ray_i32(999);
    ray_block_predicate_t pred = {filter_name, OP_GT, atom};
    int64_t baseline = mapped();
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 19, 64, 0};
    for (unsigned which = 0; which < 4; which++) {
        read.payload_limit = which == 0 ? 19 : which == 1 ? 39 : 40;
        read.scratch_limit = which == 2 ? 63 : 64;
        if (which == 3) damage("y", RAY_COL_BLOCK_HEADER);
        TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
        filter_name[0] = 'z';
        ray_t* err = ray_block_query_next(query);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err));
        TEST_ASSERT_STR_EQ(ray_err_code(err), which == 3 ? "corrupt" : "limit"); ray_release(err);
        err = ray_block_query_next(query);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_release(err);
        ray_block_query_close(&query); TEST_ASSERT_EQ_I(mapped(), baseline);
        filter_name[0] = 'y';
    }
    ray_release(atom);
    PASS();
}

const test_entry_t block_query_entries[] = {
    {"block_query/query_budgets_and_errors", query_budgets_and_errors, setup, teardown},
    {"block_query/query_late_materialization", query_late_materialization, setup, teardown},
    {"block_query/query_conjunction", query_conjunction, setup, teardown},
    {"block_query/query_native_parity", query_native_parity, setup, teardown},
    {"block_query/query_zone_pruning", query_zone_pruning, setup, teardown},
    {"block_query/gate_range_pruning", gate_range_pruning, setup, teardown},
    {"block_query/query_scattered_matches", query_scattered_matches, setup, teardown},
    {"block_query/query_admission", query_admission, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
