/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_source.h"
#include "block_query.h"
#include "ops.h"
#include "agg_registry.h"
#include "lang/eval.h"
#include "lang/env.h"
#include "lang/internal.h"
#include "table/sym.h"
#include "vec/vec.h"
#include <stdlib.h>
#include <string.h>

#define BLOCK_SOURCE_BATCH_ROWS 4096u

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

/* Consume only logical result capacity; batch buffers and geometric growth
 * capacity are separate. The collector preserves SYM's on-disk W64 domain. */
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

static ray_t* collect(ray_block_query_t* cursor, const int64_t* aliases,
                      size_t count, size_t remaining) {
    ray_t** vectors = calloc(count, sizeof(*vectors));
    if (!vectors) return ray_error("oom", NULL);
    ray_t* schema = ray_block_query_schema(cursor);
    ray_t* result = NULL;
    if (!schema || RAY_IS_ERR(schema)) { result = schema; schema = NULL; goto done; }
    for (size_t c = 0; c < count; c++) {
        ray_t* col = ray_table_get_col_idx(schema, (int64_t)c);
        ray_t* v = col->type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, 0) : ray_vec_new(col->type, 0);
        if (!v || RAY_IS_ERR(v)) { result = v; goto done; }
        vectors[c] = v;
        if (col->type == RAY_SYM) ray_sym_vec_adopt_domain(v, col);
    }
    ray_release(schema); schema = NULL;
    for (;;) {
        ray_t* batch = ray_block_query_next(cursor);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { result = batch; goto done; }
        if (!charge(batch, &remaining)) {
            ray_release(batch);
            result = ray_error("limit", "block select: result exceeds explicit byte limit"); goto done;
        }
        /* These vectors have a single owner until final table construction, so
         * append can grow geometrically instead of copying via table concat. */
        for (size_t c = 0; c < count; c++) {
            ray_t* src = ray_table_get_col_idx(batch, (int64_t)c);
            if (src->type != vectors[c]->type || (src->type == RAY_SYM &&
                (ray_sym_elem_size(src->type, src->attrs) != 8 ||
                 ray_sym_vec_domain(src) != ray_sym_vec_domain(vectors[c])))) {
                result = ray_error("schema", "block select: inconsistent batch column"); break;
            }
            if (src->type == RAY_STR) {
                for (int64_t i = 0; i < src->len; i++) {
                    size_t len; const char* s = ray_str_vec_get(src, i, &len);
                    ray_t* v = ray_str_vec_append(vectors[c], s, len);
                    if (!v || RAY_IS_ERR(v)) { result = v ? v : ray_error("oom", NULL); break; }
                    vectors[c] = v;
                }
            } else {
                ray_t* v = ray_vec_append_raw(vectors[c], ray_data(src), src->len);
                if (!v || RAY_IS_ERR(v)) result = v ? v : ray_error("oom", NULL);
                else vectors[c] = v;
            }
            if (result) break;
            vectors[c]->attrs |= src->attrs & RAY_ATTR_HAS_NULLS;
        }
        ray_release(batch);
        if (result) goto done;
    }
    result = ray_table_new((int64_t)count);
    for (size_t c = 0; result && !RAY_IS_ERR(result) && c < count; c++)
        result = ray_table_add_col(result, aliases[c], vectors[c]);
done:
    ray_release(schema);
    for (size_t c = 0; c < count; c++) ray_release(vectors[c]);
    free(vectors);
    return result ? result : ray_error("oom", NULL);
}

typedef struct {
    const char* name;
    uint16_t opcode;
    ray_unary_fn fn;
} aggregate_t;
static const aggregate_t aggregates[] = {
    {"count", OP_COUNT, ray_count_fn}, {"sum", OP_SUM, ray_sum_fn},
    {"avg", OP_AVG, ray_avg_fn}, {"min", OP_MIN, ray_min_fn}, {"max", OP_MAX, ray_max_fn}
};

static const aggregate_t* aggregate_kind(ray_t* expr) {
    if (!expr || expr->type != RAY_LIST || expr->len != 2) return NULL;
    ray_t* head = ray_list_get(expr, 0);
    const char* op = name(head);
    ray_t* fn = op ? ray_env_get(head->i64) : NULL;
    if (!fn || fn->type != RAY_UNARY) return NULL;
    for (size_t i = 0; i < sizeof(aggregates) / sizeof(*aggregates); i++)
        if (!strcmp(op, aggregates[i].name) && (ray_unary_fn)(uintptr_t)fn->i64 == aggregates[i].fn)
            return &aggregates[i];
    return NULL;
}

static bool aggregate_type(uint16_t op, int8_t type) {
    if (op == OP_COUNT) return true;
    if (op == OP_SUM || op == OP_AVG)
        return type == RAY_I16 || type == RAY_I32 || type == RAY_I64;
    return type == RAY_BOOL || type == RAY_U8 || type == RAY_I16 || type == RAY_I32 ||
        type == RAY_I64 || type == RAY_DATE || type == RAY_TIME || type == RAY_TIMESTAMP;
}

typedef struct {
    const agg_vtable_t* kernel;
    void* state;
} aggregate_state_t;

static ray_t* aggregate_result(ray_block_query_t* cursor, const aggregate_t* const* kinds,
                               const size_t* positions, const int64_t* aliases,
                               size_t count, size_t remaining) {
    aggregate_state_t* states = calloc(count, sizeof(*states));
    uint32_t* gids = calloc(BLOCK_SOURCE_BATCH_ROWS, sizeof(*gids));
    ray_t* schema = NULL;
    ray_t* result = NULL;
    if (!states || !gids) goto done;
    schema = ray_block_query_schema(cursor);
    if (!schema || RAY_IS_ERR(schema)) { result = schema; schema = NULL; goto done; }
    bool has_avg = false;
    /* Match select's empty-source projection rule, distinct from a WHERE
     * that filters all rows of a nonempty source (one aggregate row). */
    int64_t output_rows = ray_block_query_rows(cursor) ? 1 : 0;
    for (size_t a = 0; a < count; a++) {
        int8_t type = ray_table_get_col_idx(schema, (int64_t)positions[a])->type;
        uint16_t op = kinds[a]->opcode;
        if (ray_block_query_has_column(cursor, kinds[a]->name)) {
            result = ray_error("nyi", "block select: aggregate name is shadowed by a column"); goto done;
        }
        const agg_vtable_t* k = aggregate_type(op, type) ? agg_resolve(op, type) : NULL;
        if (!k || k->kind != ACC_STREAMING || !k->init || !k->update_batch ||
            !k->finalize_value || k->destroy) {
            result = ray_error("nyi", "block select: unsupported streaming aggregate type"); goto done;
        }
        size_t bytes = output_rows ? ray_type_sizes[(uint8_t)k->out_type] : 0;
        if (bytes > remaining) {
            result = ray_error("limit", "block select: aggregate result exceeds explicit byte limit"); goto done;
        }
        remaining -= bytes;
        states[a].kernel = k;
        has_avg |= op == OP_AVG;
    }
    ray_release(schema); schema = NULL;
    for (size_t a = 0; a < count; a++) {
        states[a].state = malloc(states[a].kernel->state_size);
        if (!states[a].state) goto done;
        states[a].kernel->init(states[a].state);
    }
    uint64_t total = 0;
    for (;;) {
        ray_t* batch = ray_block_query_next(cursor);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { result = batch; goto done; }
        uint64_t rows = (uint64_t)ray_table_nrows(batch);
        /* Narrow integer avg needs fewer than 2^31 values; I64's packed
         * counter allows more, but use one conservative cap for all avg plans.
         * Count includes nulls, so this also bounds every live-value counter. */
        uint64_t cap = has_avg ? INT32_MAX : INT64_MAX;
        if (rows > BLOCK_SOURCE_BATCH_ROWS || rows > cap - total) {
            ray_release(batch); result = ray_error("limit", "block select: aggregate row limit exceeded"); goto done;
        }
        total += rows;
        for (size_t a = 0; a < count; a++) {
            ray_t* col = ray_table_get_col_idx(batch, (int64_t)positions[a]);
            ray_valid_t valid = {ray_data(col), col->type, ray_vec_may_have_nulls(col)};
            const agg_vtable_t* k = states[a].kernel;
            k->update_batch(states[a].state, k->state_size, gids, ray_data(col), &valid, (int64_t)rows, NULL);
        }
        ray_release(batch);
    }
    result = ray_table_new((int64_t)count);
    for (size_t a = 0; result && !RAY_IS_ERR(result) && a < count; a++) {
        const agg_vtable_t* k = states[a].kernel;
        ray_t* col = ray_vec_new(k->out_type, output_rows);
        if (!col || RAY_IS_ERR(col)) { ray_release(result); result = col; break; }
        col->len = output_rows;
        bool is_null = output_rows && k->finalize_value(states[a].state, ray_data(col));
        /* Some ordinary select paths lose the null hint on a wrapped sum at
         * INT64_MIN. Fail closed until that engine-wide contract is unified. */
        if (is_null && kinds[a]->opcode == OP_SUM) {
            ray_release(col); ray_release(result);
            result = ray_error("range", "block select: sum reached the reserved null sentinel"); break;
        }
        if (is_null) col->attrs |= RAY_ATTR_HAS_NULLS;
        result = ray_table_add_col(result, aliases[a], col);
        ray_release(col);
    }
done:
    ray_release(schema);
    if (states) for (size_t a = 0; a < count; a++) free(states[a].state);
    free(states); free(gids);
    return result ? result : ray_error("oom", NULL);
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
    const aggregate_t* kinds[RAY_BLOCK_SCAN_MAX_COLUMNS];
    size_t positions[RAY_BLOCK_SCAN_MAX_COLUMNS];
    size_t count = 0, n_columns = 0;
    bool aggregate = false;
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
        ray_t* expr = ray_list_get(vals, i);
        kinds[count] = aggregate_kind(expr);
        bool is_aggregate = kinds[count] != NULL;
        const char* col = name(is_aggregate ? ray_list_get(expr, 1) : expr);
        if (!col || (count && aggregate != is_aggregate))
            return ray_error("nyi", "block select requires either bare columns or supported global aggregates");
        aggregate = is_aggregate;
        size_t pos = 0;
        while (pos < n_columns && strcmp(columns[pos], col)) pos++;
        if (pos < n_columns && !aggregate)
            return ray_error("domain", "block select: repeated projection column");
        if (pos == n_columns) columns[n_columns++] = col;
        positions[count] = pos;
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
    ray_block_scan_options_t options = {0, UINT64_MAX, BLOCK_SOURCE_BATCH_ROWS, 8u * 1024 * 1024, 8u * 1024 * 1024};
    ray_block_query_t* cursor = NULL;
    ray_err_t err = ray_block_query_open(string(path), columns, n_columns, pred, n_pred, &options, &cursor);
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
    result = aggregate ? aggregate_result(cursor, kinds, positions, aliases, count, (size_t)limit->i64) :
                         collect(cursor, aliases, count, (size_t)limit->i64);
done:
    ray_block_query_close(&cursor);
    return result ? result : ray_error("oom", NULL);
}
