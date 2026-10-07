/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_source.h"
#include "block_query.h"
#include "ops.h"
#include "lang/eval.h"
#include "lang/env.h"
#include "lang/internal.h"
#include <string.h>

static bool dictionary(ray_t* d) {
    if (!d || d->type != RAY_DICT) return false;
    ray_t* k = ray_dict_keys(d); ray_t* v = ray_dict_vals(d);
    return k && v && k->type == RAY_SYM && v->type == RAY_LIST && k->len == v->len;
}

static ray_t* field(ray_t* d, const char* name) {
    if (!dictionary(d)) return NULL;
    ray_t* k = ray_dict_keys(d); ray_t* v = ray_dict_vals(d);
    for (int64_t i = 0; i < k->len; i++) {
        ray_t* s = ray_sym_vec_cell(k, i);
        if (s && ray_str_len(s) == strlen(name) && !memcmp(ray_str_ptr(s), name, strlen(name)))
            return ray_list_get(v, i);
    }
    return NULL;
}

static const char* string(ray_t* s) {
    if (!s || s->type != -RAY_STR) return NULL;
    const char* p = ray_str_ptr(s); size_t n = ray_str_len(s);
    return p && n && !memchr(p, 0, n) ? p : NULL;
}

static const char* name(ray_t* s) {
    return s && s->type == -RAY_SYM && !(s->attrs & ATTR_QUOTED) ?
        string(ray_sym_str(s->i64)) : NULL;
}

ray_t* ray_block_source_fn(ray_t** args, int64_t n) {
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    if (n != 2) return ray_error("arity", ".db.block.scan: path result-bytes");
    if (!string(args[0]) || args[1]->type != -RAY_I64 || args[1]->i64 <= 0 ||
        (uint64_t)args[1]->i64 > SIZE_MAX)
        return ray_error("domain", ".db.block.scan: nonempty path and positive I64 result limit required");
    int64_t names[] = {ray_sym_intern(".hdb.block.source", 17), ray_sym_intern(".hdb.block.limit", 16)};
    if (names[0] < 0 || names[1] < 0) return ray_error("oom", NULL);
    ray_t* keys = ray_vec_from_raw(RAY_SYM, names, 2);
    ray_t* vals = ray_list_new(2);
    if (!keys || RAY_IS_ERR(keys) || !vals || RAY_IS_ERR(vals)) {
        ray_release(keys); ray_release(vals); return ray_error("oom", NULL);
    }
    for (int i = 0; i < 2; i++) { ray_retain(args[i]); ((ray_t**)ray_data(vals))[i] = args[i]; }
    vals->len = 2;
    return ray_dict_new(keys, vals);
}

typedef struct {
    const char* name;
    uint16_t opcode;
    ray_binary_fn fn;
} comparison_t;
static const comparison_t comparisons[] = {
    {"==", OP_EQ, ray_eq_fn}, {"!=", OP_NE, ray_neq_fn},
    {"<", OP_LT, ray_lt_fn}, {"<=", OP_LE, ray_lte_fn},
    {">", OP_GT, ray_gt_fn}, {">=", OP_GE, ray_gte_fn}
};

static bool predicates(ray_t* expr, ray_block_predicate_t* out, size_t* n, unsigned depth) {
    if (!expr || expr->type != RAY_LIST || expr->len != 3 || depth > 32) return false;
    ray_t* head = ray_list_get(expr, 0);
    const char* op = name(head);
    ray_t* fn = op ? ray_env_get(head->i64) : NULL;
    if (!fn) return false;
    if (!strcmp(op, "and")) {
        if (fn->type != RAY_VARY || (ray_vary_fn)(uintptr_t)fn->i64 != ray_and_vary_fn) return false;
        return predicates(ray_list_get(expr, 1), out, n, depth + 1) &&
               predicates(ray_list_get(expr, 2), out, n, depth + 1);
    }
    if (*n == RAY_BLOCK_QUERY_MAX_PREDICATES || fn->type != RAY_BINARY) return false;
    for (size_t i = 0; i < sizeof(comparisons) / sizeof(*comparisons); i++) {
        const comparison_t* c = &comparisons[i];
        if (strcmp(op, c->name) || (ray_binary_fn)(uintptr_t)fn->i64 != c->fn) continue;
        const char* col = name(ray_list_get(expr, 1));
        if (!col) return false;
        out[(*n)++] = (ray_block_predicate_t){col, c->opcode, ray_list_get(expr, 2)};
        return true;
    }
    return false;
}

/* Consume only logical result capacity; a batch and concat temporaries remain
 * separately live. Count SYM as W64 even if concat chooses a narrower width. */
static bool charge(ray_t* table, size_t* remaining) {
    for (int64_t c = 0; c < ray_table_ncols(table); c++) {
        ray_t* col = ray_table_get_col_idx(table, c);
        size_t width = col->type == RAY_STR ? 16 : col->type == RAY_SYM ? 8 : ray_type_sizes[(uint8_t)col->type];
        if (!width || (uint64_t)col->len > *remaining / width) return false;
        *remaining -= (size_t)col->len * width;
        if (col->type == RAY_STR) for (int64_t i = 0; i < col->len; i++) {
            size_t len; ray_str_vec_get(col, i, &len);
            if (len > 12) {
                if (len > *remaining) return false;
                *remaining -= len;
            }
        }
    }
    return true;
}

ray_t* ray_block_select_source(ray_t* source, ray_t* query) {
    ray_t* path = field(source, ".hdb.block.source");
    if (!path) return NULL;
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    ray_t* limit = field(source, ".hdb.block.limit");
    if (ray_dict_len(source) != 2 || !string(path) || !limit || limit->type != -RAY_I64 ||
        limit->i64 <= 0 || (uint64_t)limit->i64 > SIZE_MAX)
        return ray_error("domain", "invalid block source descriptor");
    if (!dictionary(query)) return ray_error("type", "invalid block query");
    const char* columns[RAY_BLOCK_SCAN_MAX_COLUMNS];
    int64_t aliases[RAY_BLOCK_SCAN_MAX_COLUMNS];
    size_t count = 0;
    ray_t* keys = ray_dict_keys(query); ray_t* vals = ray_dict_vals(query);
    for (int64_t i = 0; i < keys->len; i++) {
        ray_t* key = ray_sym_vec_cell(keys, i);
        const char* k = string(key);
        if (!k) return ray_error("domain", "invalid block query key");
        if (!strcmp(k, "from") || !strcmp(k, "where")) continue;
        if (!strcmp(k, "by") || !strcmp(k, "take") || !strcmp(k, "asc") ||
            !strcmp(k, "desc") || !strcmp(k, "nearest"))
            return ray_error("nyi", "block select: grouping, ordering and take are not supported");
        if (count == RAY_BLOCK_SCAN_MAX_COLUMNS) return ray_error("limit", "too many output columns");
        columns[count] = name(ray_list_get(vals, i));
        if (!columns[count]) return ray_error("nyi", "block select requires bare-column projections");
        aliases[count] = ray_sym_intern(k, strlen(k));
        if (aliases[count] < 0) return ray_error("oom", NULL);
        count++;
    }
    if (!count) return ray_error("nyi", "block select requires explicit projection");
    ray_block_predicate_t pred[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t n_pred = 0;
    ray_t* where = field(query, "where");
    if (where && !predicates(where, pred, &n_pred, 0))
        return ray_error("nyi", "block select requires builtin comparisons and binary AND with typed literals");
    ray_block_scan_options_t options = {0, UINT64_MAX, 4096, 8u * 1024 * 1024, 8u * 1024 * 1024};
    ray_block_query_t* cursor = NULL;
    ray_err_t err = ray_block_query_open(string(path), columns, count, pred, n_pred, &options, &cursor);
    if (err) return ray_error(ray_err_code_str(err), "block select: source or predicate admission failed");
    ray_t* result = NULL;
    /* Query scope can shadow call heads with even an unprojected column.
     * Use the full name schema from the SAME pinned generation. */
    if (where) {
        bool shadowed = ray_block_query_has_column(cursor, "and");
        for (size_t i = 0; i < sizeof(comparisons) / sizeof(*comparisons); i++)
            shadowed |= ray_block_query_has_column(cursor, comparisons[i].name);
        if (shadowed) { result = ray_error("nyi", "block select: operator-named columns are not supported"); goto done; }
    }
    result = ray_block_query_schema(cursor);
    size_t remaining = (size_t)limit->i64;
    while (result && !RAY_IS_ERR(result)) {
        ray_t* batch = ray_block_query_next(cursor);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { ray_release(result); result = batch; break; }
        if (!charge(batch, &remaining)) {
            ray_release(batch); ray_release(result);
            result = ray_error("limit", "block select: result exceeds explicit byte limit"); break;
        }
        ray_t* joined = ray_concat_fn(result, batch);
        ray_release(result); ray_release(batch);
        result = joined;
    }
    if (result && !RAY_IS_ERR(result)) {
        ray_t* renamed = ray_table_new((int64_t)count);
        for (size_t i = 0; renamed && !RAY_IS_ERR(renamed) && i < count; i++)
            renamed = ray_table_add_col(renamed, aliases[i], ray_table_get_col_idx(result, (int64_t)i));
        ray_release(result); result = renamed;
    }
done:
    ray_block_query_close(&cursor);
    return result ? result : ray_error("oom", NULL);
}
