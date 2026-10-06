/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_query.h"
#include "ops.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char column[256];
    uint16_t opcode;
    ray_t* value;
} predicate_t;

struct ray_block_query_s {
    ray_block_scan_t* scan;
    predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t filter_indices[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t count, n_predicates, n_filters;
    ray_t* schema;
    ray_t* mask;
    uint64_t start, position;
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
        strcpy(p->column, predicates[i].column);
        p->opcode = predicates[i].opcode;
        p->value = ray_i64(0);
        if (!p->value || RAY_IS_ERR(p->value)) { err = RAY_ERR_OOM; break; }
        /* Admitted atoms contain only inline scalar data, never pointers. */
        p->value->type = predicates[i].value->type;
        memcpy(&p->value->i64, &predicates[i].value->i64, sizeof(int64_t));
        p->value->aux[0] = predicates[i].value->aux[0] & 1;
        size_t index = 0;
        while (index < n && strcmp(names[index], p->column)) index++;
        if (index == n) {
            if (n == RAY_BLOCK_SCAN_MAX_COLUMNS) { err = RAY_ERR_LIMIT; break; }
            names[n++] = p->column;
        }
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

ray_t* ray_block_query_next(ray_block_query_t* q) {
    if (!q) return ray_error("domain", "null block query");
    if (q->failed) return ray_error("domain", "block query is terminal; close it");
    ray_t* result;
    for (;;) {
        if (q->cancelled) { result = ray_error("cancel", "block query cancelled"); break; }
        if (!q->n_predicates) { result = ray_block_scan_next(q->scan); break; }
        if (q->mask) {
            const uint8_t* mask = ray_data(q->mask);
            uint64_t rows = (uint64_t)q->mask->len;
            while (q->position < rows && !mask[q->position]) q->position++;
            uint64_t begin = q->position;
            while (q->position < rows && mask[q->position]) q->position++;
            if (q->position > begin) {
                result = ray_block_scan_read_columns(q->scan, NULL, q->count,
                                                    q->start + begin, q->position - begin);
                break;
            }
            ray_release(q->mask); q->mask = NULL;
        }
        ray_t* batch = ray_block_scan_next_columns(q->scan, q->filter_indices,
                                                   q->n_filters, &q->start);
        if (!batch || RAY_IS_ERR(batch)) { result = batch; break; }
        int64_t rows = ray_table_nrows(batch);
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
