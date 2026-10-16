/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_query.h"
#include "ops.h"
#include "lang/eval.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char column[256];
    uint16_t opcode;
    size_t scan_index;
    ray_t* value;
} predicate_t;

struct ray_block_query_s {
    ray_block_scan_t* scan;
    predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t filter_indices[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t count, n_predicates, n_filters;
    ray_t* schema;
    ray_t* mask;
    uint64_t start, position, remaining;
    size_t external_reserve;
    bool failed, cancelled;
};

void ray_block_query_close(ray_block_query_t** query) {
    if (!query || !*query) return;
    ray_block_query_t* q = *query;
    ray_block_scan_close(&q->scan);
    for (size_t i = 0; i < q->n_predicates; i++) ray_release(q->predicates[i].value);
    ray_release(q->schema);
    ray_release(q->mask);
    free(q);
    *query = NULL;
}

static bool query_name_safe(const char* name) {
    return name && *name && strlen(name) <= 255 && *name != '.' &&
        !strchr(name, '/') && !strchr(name, '\\');
}

static bool query_atom_admitted(const ray_t* v) {
    if (!v || RAY_IS_NULL(v)) return false;
    switch (v->type) {
    case -RAY_BOOL: case -RAY_U8: case -RAY_I16: case -RAY_I32:
    case -RAY_I64: case -RAY_DATE: case -RAY_TIME: case -RAY_TIMESTAMP: return true;
    default: return false;
    }
}

static bool query_opcode_admitted(uint16_t op) {
    return op == OP_EQ || op == OP_NE || op == OP_LT || op == OP_LE || op == OP_GT || op == OP_GE;
}

ray_err_t ray_block_query_open(const char* root, const char* const* columns,
                             size_t count, const ray_block_predicate_t* predicates,
                             size_t n_predicates, const ray_block_scan_options_t* options,
                             ray_block_query_t** out) {
    if (!out || *out || !columns || !count || !options || (n_predicates && !predicates))
        return RAY_ERR_DOMAIN;
    if (count > RAY_BLOCK_SCAN_MAX_COLUMNS || n_predicates > RAY_BLOCK_QUERY_MAX_PREDICATES)
        return RAY_ERR_LIMIT;
    const char* names[RAY_BLOCK_SCAN_MAX_COLUMNS];
    size_t n = count;
    for (size_t i = 0; i < count; i++) {
        if (!query_name_safe(columns[i])) return RAY_ERR_DOMAIN;
        for (size_t j = 0; j < i; j++)
            if (!strcmp(columns[i], columns[j])) return RAY_ERR_DOMAIN;
        names[i] = columns[i];
    }
    for (size_t i = 0; i < n_predicates; i++) {
        if (!query_name_safe(predicates[i].column)) return RAY_ERR_DOMAIN;
        if (!query_opcode_admitted(predicates[i].opcode)) return RAY_ERR_NYI;
        if (!query_atom_admitted(predicates[i].value)) return RAY_ERR_TYPE;
    }
    ray_block_query_t* q = calloc(1, sizeof(*q));
    if (!q) return RAY_ERR_OOM;
    q->count = count; q->n_predicates = n_predicates;
    ray_err_t err = RAY_OK;
    for (size_t i = 0; i < n_predicates; i++) {
        predicate_t* p = &q->predicates[i];
        size_t column_len = strlen(predicates[i].column);
        memcpy(p->column, predicates[i].column, column_len + 1);
        p->opcode = predicates[i].opcode;
        p->value = ray_i64(0);
        if (!p->value || RAY_IS_ERR(p->value)) { err = RAY_ERR_OOM; break; }
        /* Admitted atoms contain only inline scalar data, never pointers. */
        p->value->type = predicates[i].value->type;
        memcpy(&p->value->i64, &predicates[i].value->i64, sizeof(int64_t));
        p->value->aux[0] = predicates[i].value->aux[0] & 1;
        size_t index = 0;
        while (index < n && strcmp(names[index], p->column) != 0) index++;
        if (index == n) {
            if (n == RAY_BLOCK_SCAN_MAX_COLUMNS) { err = RAY_ERR_LIMIT; break; }
            names[n++] = p->column;
        }
        p->scan_index = index;
        size_t j = 0;
        while (j < q->n_filters && q->filter_indices[j] != index) j++;
        if (j == q->n_filters) q->filter_indices[q->n_filters++] = index;
    }
    if (!err) err = ray_block_scan_open(root, names, n, options, &q->scan);
    ray_t* schema = NULL;
    if (!err) {
        schema = ray_block_scan_schema(q->scan);
        if (!schema || RAY_IS_ERR(schema)) err = RAY_ERR_OOM;
    }
    for (size_t i = 0; !err && i < n_predicates; i++) {
        ray_t* col = ray_table_get_col(schema,
            ray_sym_intern(q->predicates[i].column, strlen(q->predicates[i].column)));
        if (!col || col->type != -q->predicates[i].value->type) err = RAY_ERR_TYPE;
    }
    if (!err) {
        q->remaining = ray_block_scan_rows(q->scan);
        q->schema = ray_table_new((int64_t)count);
        if (!q->schema || RAY_IS_ERR(q->schema)) err = RAY_ERR_OOM;
        for (size_t i = 0; !err && i < count; i++) {
            q->schema = ray_table_add_col(q->schema, ray_table_col_name(schema, (int64_t)i),
                                         ray_table_get_col_idx(schema, (int64_t)i));
            if (!q->schema || RAY_IS_ERR(q->schema)) err = RAY_ERR_OOM;
        }
    }
    ray_release(schema);
    if (err) { ray_block_query_close(&q); return err; }
    *out = q;
    return RAY_OK;
}

uint64_t ray_block_query_rows(const ray_block_query_t* q) {
    return q ? ray_block_scan_rows(q->scan) : 0;
}

ray_err_t ray_block_query_reserve(ray_block_query_t* q, size_t bytes) {
    if (!q || q->failed) return RAY_ERR_DOMAIN;
    size_t mask_bytes = q->mask ? (size_t)q->mask->len : 0;
    if (bytes > SIZE_MAX - mask_bytes) return RAY_ERR_LIMIT;
    ray_err_t err = ray_block_scan_reserve(q->scan, bytes + mask_bytes);
    if (err) return err;
    q->external_reserve = bytes;
    return RAY_OK;
}

bool ray_block_query_has_column(const ray_block_query_t* q, const char* name) {
    return q && ray_block_scan_has_column(q->scan, name);
}

ray_t* ray_block_query_schema(const ray_block_query_t* q) {
    if (!q) return ray_error("domain", "null block query");
    ray_retain(q->schema);
    return q->schema;
}

void ray_block_query_cancel(ray_block_query_t* q) {
    if (q) { q->cancelled = true; ray_block_scan_cancel(q->scan); }
}

static ray_t* query_mask(ray_block_query_t* q, ray_t* batch) {
    ray_graph_t* g = ray_graph_new(batch);
    if (!g) return ray_error("oom", "block query graph allocation failed");
    uint32_t root = 0;
    for (size_t i = 0; i < q->n_predicates; i++) {
        predicate_t* p = &q->predicates[i];
        ray_op_t* col = ray_scan(g, p->column);
        if (!col) goto oom;
        uint32_t col_id = col->id;
        ray_op_t* literal = ray_const_atom(g, p->value);
        if (!literal) goto oom;
        ray_op_t* cmp = ray_binop(g, p->opcode, &g->nodes[col_id], literal);
        if (!cmp) goto oom;
        if (i) cmp = ray_and(g, &g->nodes[root], cmp);
        if (!cmp) goto oom;
        root = cmp->id;
    }
    ray_t* mask = ray_execute(g, &g->nodes[root]);
    ray_graph_free(g);
    return mask;
oom:
    ray_graph_free(g);
    return ray_error("oom", "block query graph allocation failed");
}

static ray_err_t query_scan_reserve(ray_block_query_t* q, size_t query_bytes) {
    if (query_bytes > SIZE_MAX - q->external_reserve) return RAY_ERR_LIMIT;
    return ray_block_scan_reserve(q->scan, q->external_reserve + query_bytes);
}

static ray_t* query_reserve_error(ray_block_query_t* q, size_t bytes) {
    ray_err_t err = query_scan_reserve(q, bytes);
    return err ? ray_error(ray_err_code_str(err), "block query working set exceeds limit") : NULL;
}

static ray_block_scan_cmp_t query_scan_op(uint16_t op) {
    switch (op) {
    case OP_EQ: return RAY_BLOCK_SCAN_EQ;
    case OP_NE: return RAY_BLOCK_SCAN_NE;
    case OP_LT: return RAY_BLOCK_SCAN_LT;
    case OP_LE: return RAY_BLOCK_SCAN_LE;
    case OP_GT: return RAY_BLOCK_SCAN_GT;
    default: return RAY_BLOCK_SCAN_GE;
    }
}

static int64_t query_atom_value(const ray_t* value) {
    switch (-value->type) {
    case RAY_BOOL: return value->b8;
    case RAY_U8: return value->u8;
    case RAY_I16: return value->i16;
    case RAY_I32: case RAY_DATE: case RAY_TIME: return value->i32;
    default: return value->i64;
    }
}

static ray_err_t query_batch_may_match(ray_block_query_t* q, uint64_t start,
                                       uint64_t rows, bool* may_match) {
    *may_match = true;
    for (size_t i = 0; i < q->n_predicates; i++) {
        predicate_t* p = &q->predicates[i];
        bool predicate_may_match;
        ray_err_t err = ray_block_scan_range_may_match(q->scan, p->scan_index,
            start, rows, query_scan_op(p->opcode), query_atom_value(p->value),
            RAY_ATOM_IS_NULL(p->value), &predicate_may_match);
        if (err) return err;
        if (!predicate_may_match) { *may_match = false; break; }
    }
    return RAY_OK;
}

ray_t* ray_block_query_next(ray_block_query_t* q) {
    if (!q) return ray_error("domain", "null block query");
    if (q->failed) return ray_error("domain", "block query is terminal; close it");
    ray_t* result;
    for (;;) {
        if (q->cancelled || ray_eval_is_interrupted()) {
            result = ray_error("cancel", "block query cancelled"); break;
        }
        if (!q->n_predicates) {
            result = query_reserve_error(q, 0);
            if (!result) result = ray_block_scan_next(q->scan);
            break;
        }
        if (q->mask) {
            const uint8_t* mask = ray_data(q->mask);
            uint64_t rows = (uint64_t)q->mask->len;
            while (q->position < rows && !mask[q->position]) q->position++;
            uint64_t begin = q->position;
            while (q->position < rows && mask[q->position]) q->position++;
            if (q->position > begin) {
                result = query_reserve_error(q, (size_t)q->mask->len);
                if (!result) result = ray_block_scan_read_columns(q->scan, NULL, q->count,
                                                        q->start + begin, q->position - begin);
                break;
            }
            ray_release(q->mask); q->mask = NULL;
        }
        uint64_t batch_start, scan_rows;
        ray_err_t range_err = ray_block_scan_next_range(q->scan, &batch_start, &scan_rows);
        if (range_err) { result = ray_error(ray_err_code_str(range_err), "block query range failed"); break; }
        if (!scan_rows) { result = NULL; break; }
        bool may_match;
        range_err = query_batch_may_match(q, batch_start, scan_rows, &may_match);
        if (range_err) { result = ray_error(ray_err_code_str(range_err), "block query statistics failed"); break; }
        if (!may_match) {
            range_err = ray_block_scan_skip_next(q->scan, &q->start);
            if (range_err) { result = ray_error(ray_err_code_str(range_err), "block query skip failed"); break; }
            q->remaining -= scan_rows;
            continue;
        }
        size_t mask_vectors = q->n_predicates * 2 - 1;
        if (scan_rows > SIZE_MAX / mask_vectors) {
            result = ray_error("limit", "block query mask workspace exceeds size limit"); break;
        }
        result = query_reserve_error(q, (size_t)scan_rows * mask_vectors);
        if (result) break;
        ray_t* batch = ray_block_scan_next_columns(q->scan, q->filter_indices,
                                                   q->n_filters, &q->start);
        if (!batch || RAY_IS_ERR(batch)) { result = batch; break; }
        int64_t rows = ray_table_nrows(batch);
        q->remaining -= (uint64_t)rows;
        ray_t* mask = query_mask(q, batch);
        ray_release(batch);
        if (!mask) { result = ray_error("oom", "block query mask allocation failed"); break; }
        if (RAY_IS_ERR(mask)) { result = mask; break; }
        if (mask->type != RAY_BOOL || mask->len != rows) {
            ray_release(mask);
            result = ray_error("type", "block query requires a row-aligned boolean mask"); break;
        }
        q->mask = mask; q->position = 0;
    }
    if (RAY_IS_ERR(result)) q->failed = true;
    return result;
}
