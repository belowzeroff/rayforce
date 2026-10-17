/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_query.h"
#include "ops.h"
#include "lang/eval.h"
#include "store/part.h"
#include "mem/heap.h"
#include "vec/str.h"
#include "table/sym.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {
    char column[256];
    uint16_t opcode;
    size_t scan_index;
    ray_t* value;
} predicate_t;

typedef struct block_parted_s block_parted_t;
static void parted_free(block_parted_t* p);
static ray_err_t parted_reserve(block_parted_t* p);
static ray_err_t parted_set_reserve(block_parted_t* p, size_t bytes);
static ray_err_t parted_source_empty(block_parted_t* p, bool* empty);
static bool parted_has_column(const block_parted_t* p, const char* name);
static void parted_cancel(block_parted_t* p);
static uint64_t parted_rows(const block_parted_t* p);
static uint32_t parted_batch_rows(const block_parted_t* p);
static void parted_stats(const block_parted_t* p, ray_block_query_stats_t* out);
static ray_t* parted_next(ray_block_query_t* q);

struct ray_block_query_s {
    block_parted_t* parted; /* set for a partitioned query, which has no scan */
    ray_block_scan_t* scan;
    predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t filter_indices[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t count, n_predicates, n_filters;
    ray_t* schema;
    ray_t* mask;
    uint64_t start, position, remaining;
    uint64_t mask_base;   /* absolute row of mask[0] */
    uint64_t probe_rows;  /* finest zone-map block among filter columns, 0: none */
    size_t external_reserve;
    uint64_t batches_skipped;
    bool failed, cancelled;
};

void ray_block_query_close(ray_block_query_t** query) {
    if (!query || !*query) return;
    ray_block_query_t* q = *query;
    ray_block_scan_close(&q->scan);
    for (size_t i = 0; i < q->n_predicates; i++) ray_release(q->predicates[i].value);
    ray_release(q->schema);
    ray_release(q->mask);
    parted_free(q->parted);
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
    case -RAY_I64: case -RAY_DATE: case -RAY_TIME: case -RAY_TIMESTAMP: case -RAY_SYM: return true;
    default: return false;
    }
}

/* Symbols compare for equality only: their order is no data order. */
static bool query_pair_admitted(uint16_t op, const ray_t* v) {
    return v->type != -RAY_SYM || op == OP_EQ || op == OP_NE;
}

static bool query_opcode_admitted(uint16_t op) {
    return op == OP_EQ || op == OP_NE || op == OP_LT || op == OP_LE || op == OP_GT || op == OP_GE;
}

ray_err_t ray_block_query_open(const char* root, const char* const* columns,
                             size_t count, const ray_block_predicate_t* predicates,
                             size_t n_predicates, const ray_block_scan_options_t* options,
                             ray_block_query_t** out) {
    return ray_block_query_open_source(root, NULL, columns, count, predicates, n_predicates,
                                       options, out);
}

ray_err_t ray_block_query_open_source(const char* root, const ray_block_scan_source_t* source,
                                      const char* const* columns, size_t count,
                                      const ray_block_predicate_t* predicates,
                                      size_t n_predicates,
                                      const ray_block_scan_options_t* options,
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
        /* Unsupported, not wrong: native select may accept these. */
        if (!query_atom_admitted(predicates[i].value)) return RAY_ERR_NYI;
        if (!query_pair_admitted(predicates[i].opcode, predicates[i].value)) return RAY_ERR_NYI;
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
    if (!err) err = ray_block_scan_open_source(root, source, names, n, options, &q->scan);
    ray_t* schema = NULL;
    if (!err) {
        schema = ray_block_scan_schema(q->scan);
        if (!schema || RAY_IS_ERR(schema)) err = RAY_ERR_OOM;
    }
    for (size_t i = 0; !err && i < n_predicates; i++) {
        ray_t* col = ray_table_get_col(schema,
            ray_sym_intern(q->predicates[i].column, strlen(q->predicates[i].column)));
        if (!col) err = RAY_ERR_SCHEMA;
        else if (col->type != -q->predicates[i].value->type) err = RAY_ERR_NYI;
    }
    for (size_t i = 0; !err && i < n_predicates; i++) {
        uint64_t g = ray_block_scan_zone_rows(q->scan, q->predicates[i].scan_index);
        if (g && (!q->probe_rows || g < q->probe_rows)) q->probe_rows = g;
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
    if (!q) return 0;
    return q->parted ? parted_rows(q->parted) : ray_block_scan_rows(q->scan);
}

uint32_t ray_block_query_batch_rows(const ray_block_query_t* q) {
    if (!q) return 0;
    if (q->parted) return parted_batch_rows(q->parted);
    return ray_block_scan_batch_rows(q->scan);
}

ray_err_t ray_block_query_source_empty(ray_block_query_t* q, bool* empty) {
    if (!q || !empty || q->failed) return RAY_ERR_DOMAIN;
    if (q->parted) return parted_source_empty(q->parted, empty);
    *empty = !ray_block_scan_rows(q->scan);
    return RAY_OK;
}

ray_err_t ray_block_query_reserve(ray_block_query_t* q, size_t bytes) {
    if (!q || q->failed) return RAY_ERR_DOMAIN;
    if (q->parted) return parted_set_reserve(q->parted, bytes);
    size_t mask_bytes = q->mask ? (size_t)q->mask->len : 0;
    if (bytes > SIZE_MAX - mask_bytes) return RAY_ERR_LIMIT;
    ray_err_t err = ray_block_scan_reserve(q->scan, bytes + mask_bytes);
    if (err) return err;
    q->external_reserve = bytes;
    return RAY_OK;
}

bool ray_block_query_has_column(const ray_block_query_t* q, const char* name) {
    if (!q) return false;
    return q->parted ? parted_has_column(q->parted, name) : ray_block_scan_has_column(q->scan, name);
}

ray_t* ray_block_query_schema(const ray_block_query_t* q) {
    if (!q) return ray_error("domain", "null block query");
    ray_retain(q->schema);
    return q->schema;
}

void ray_block_query_stats(const ray_block_query_t* q, ray_block_query_stats_t* out) {
    if (!out) return;
    *out = (ray_block_query_stats_t){{0, 0}, 0, 0, 0};
    if (!q) return;
    if (q->parted) { parted_stats(q->parted, out); return; }
    ray_block_scan_stats(q->scan, &out->scan);
    out->batches_skipped = q->batches_skipped;
}

void ray_block_query_cancel(ray_block_query_t* q) {
    if (!q) return;
    q->cancelled = true;
    if (q->parted) parted_cancel(q->parted);
    else ray_block_scan_cancel(q->scan);
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
    if (q->parted) {
        result = parted_next(q);
        if (RAY_IS_ERR(result)) q->failed = true;
        return result;
    }
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
            /* Runs a short gap apart share blocks: read them as one window
             * and compact, instead of decoding those blocks once per run. */
            bool fragmented = false;
            for (;;) {
                uint64_t next = q->position;
                while (next < rows && !mask[next]) next++;
                if (next == rows || next - q->position > RAY_BLOCK_QUERY_COALESCE_ROWS) break;
                q->position = next;
                while (q->position < rows && mask[q->position]) q->position++;
                fragmented = true;
            }
            if (q->position > begin) {
                result = query_reserve_error(q, (size_t)q->mask->len);
                if (!result) result = fragmented ?
                    ray_block_scan_read_masked(q->scan, NULL, q->count, q->mask_base + begin,
                                               q->position - begin, mask + begin) :
                    ray_block_scan_read_columns(q->scan, NULL, q->count,
                                                q->mask_base + begin, q->position - begin);
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
        /* Trim blocks at either end of the read that zone maps exclude, so a
         * narrow range decodes only its own blocks of a large read. */
        uint64_t lo = 0, hi = scan_rows;
        for (uint64_t g = q->probe_rows; may_match && g && g < scan_rows && lo < hi;) {
            uint64_t s = batch_start + lo, e = (s / g + 1) * g;
            if (e > batch_start + hi) e = batch_start + hi;
            bool m;
            range_err = query_batch_may_match(q, s, e - s, &m);
            if (range_err || m) break;
            lo = e - batch_start;
        }
        for (uint64_t g = q->probe_rows; !range_err && may_match && g && g < scan_rows && lo < hi;) {
            uint64_t e = batch_start + hi, s = (e - 1) / g * g;
            if (s < batch_start + lo) s = batch_start + lo;
            bool m;
            range_err = query_batch_may_match(q, s, e - s, &m);
            if (range_err || m) break;
            hi = s - batch_start;
        }
        if (range_err) { result = ray_error(ray_err_code_str(range_err), "block query statistics failed"); break; }
        if (lo == hi) may_match = false;
        if (!may_match) {
            range_err = ray_block_scan_skip_next(q->scan, &q->start);
            if (range_err) { result = ray_error(ray_err_code_str(range_err), "block query skip failed"); break; }
            q->batches_skipped++;
            q->remaining -= scan_rows;
            continue;
        }
        size_t mask_vectors = q->n_predicates * 2 - 1;
        if (scan_rows > SIZE_MAX / mask_vectors) {
            result = ray_error("limit", "block query mask workspace exceeds size limit"); break;
        }
        result = query_reserve_error(q, (size_t)(hi - lo) * mask_vectors);
        if (result) break;
        ray_t* batch;
        if (lo == 0 && hi == scan_rows) {
            batch = ray_block_scan_next_columns(q->scan, q->filter_indices, q->n_filters, &q->start);
        } else {
            batch = ray_block_scan_read_columns(q->scan, q->filter_indices, q->n_filters,
                                                batch_start + lo, hi - lo);
            range_err = batch && !RAY_IS_ERR(batch) ? ray_block_scan_skip_next(q->scan, &q->start) : RAY_OK;
            if (range_err) { ray_release(batch); result = ray_error(ray_err_code_str(range_err), "block query skip failed"); break; }
        }
        if (!batch || RAY_IS_ERR(batch)) { result = batch; break; }
        q->mask_base = batch_start + lo;
        int64_t rows = ray_table_nrows(batch);
        q->remaining -= scan_rows;
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

/* ---- Partitioned queries ------------------------------------------------ */

typedef struct {
    int64_t name;  /* output column name */
    bool key;
    size_t data;   /* index into the partition projection */
} parted_output_t;

typedef struct {
    uint16_t opcode;
    int64_t value;
} key_predicate_t;

struct block_parted_s {
    char root[1024], table[256], sym_path[1100];
    bool has_sym, immutable, cancelled;
    char** dirs;
    int64_t n_dirs, next, reading;
    uint8_t mc_type;
    int8_t key_type;
    int64_t key_name;
    parted_output_t* outputs;
    size_t n_outputs;
    char (*data)[256];
    const char* data_names[RAY_BLOCK_SCAN_MAX_COLUMNS];
    int8_t* data_types;
    size_t n_data;
    predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t n_predicates;
    key_predicate_t keys[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t n_keys;
    ray_block_scan_options_t options;
    ray_t* names;        /* first partition's full name schema */
    ray_block_query_t* current;
    uint64_t first_rows, rows_read;
    ray_block_query_stats_t done; /* closed partitions */
    size_t row_bytes;    /* key and SYM copies per output row */
    size_t external_reserve;
};

/* Fold the current partition's work into the totals before closing it. */
static void parted_close_current(block_parted_t* p) {
    if (!p->current) return;
    ray_block_query_stats_t s;
    ray_block_query_stats(p->current, &s);
    p->done.scan.blocks_decoded += s.scan.blocks_decoded;
    p->done.scan.raw_rows += s.scan.raw_rows;
    p->done.batches_skipped += s.batches_skipped;
    ray_block_query_close(&p->current);
}

static void parted_free(block_parted_t* p) {
    if (!p) return;
    ray_block_query_close(&p->current);
    for (size_t i = 0; i < p->n_predicates; i++) ray_release(p->predicates[i].value);
    ray_release(p->names);
    ray_parted_dirs_free(p->dirs, p->n_dirs);
    free(p->outputs); free(p->data); free(p->data_types);
    free(p);
}

static bool key_match(const key_predicate_t* k, int64_t key) {
    switch (k->opcode) {
    case OP_EQ: return key == k->value;
    case OP_NE: return key != k->value;
    case OP_LT: return key < k->value;
    case OP_LE: return key <= k->value;
    case OP_GT: return key > k->value;
    default: return key >= k->value;
    }
}

static bool parted_pruned(const block_parted_t* p, int64_t partition) {
    if (!p->n_keys) return false;
    int64_t key = ray_parted_dir_key(p->dirs[partition], p->mc_type);
    for (size_t i = 0; i < p->n_keys; i++)
        if (!key_match(&p->keys[i], key)) return true;
    return false;
}

static ray_err_t parted_path(const block_parted_t* p, int64_t partition, char* path, size_t size) {
    int n = snprintf(path, size, "%s/%s/%s", p->root, p->dirs[partition], p->table);
    return n < 0 || (size_t)n >= size ? RAY_ERR_RANGE : RAY_OK;
}

static ray_err_t parted_open_partition(block_parted_t* p, int64_t partition,
                                       const char* const* columns, size_t count,
                                       bool predicates, ray_block_query_t** out) {
    char path[1300];
    ray_err_t err = parted_path(p, partition, path, sizeof(path));
    if (err) return err;
    ray_block_predicate_t pred[RAY_BLOCK_QUERY_MAX_PREDICATES];
    size_t n = predicates ? p->n_predicates : 0;
    for (size_t i = 0; i < n; i++)
        pred[i] = (ray_block_predicate_t){p->predicates[i].column, p->predicates[i].opcode,
                                          p->predicates[i].value};
    ray_block_scan_source_t source = {p->has_sym ? p->sym_path : NULL, p->immutable};
    return ray_block_query_open_source(path, &source, columns, count, pred, n, &p->options, out);
}

/* Name schema of a later partition: a prefix of the first partition's. */
static bool parted_names_prefix(const block_parted_t* p, const ray_t* names) {
    if (!names || names->len > p->names->len) return false;
    for (int64_t i = 0; i < names->len; i++) {
        size_t a, b;
        const char* x = ray_str_vec_get((ray_t*)names, i, &a);
        const char* y = ray_str_vec_get(p->names, i, &b);
        if (a != b || memcmp(x, y, a)) return false;
    }
    return true;
}

static ray_err_t parted_check_schema(const block_parted_t* p, ray_block_query_t* q) {
    ray_block_scan_t* scan = q->scan;
    if (!parted_names_prefix(p, ray_block_scan_names(scan))) return RAY_ERR_SCHEMA;
    for (size_t i = 0; i < p->n_data; i++) {
        ray_t* col = ray_table_get_col_idx(q->schema, (int64_t)i);
        if (!col || col->type != p->data_types[i]) return RAY_ERR_SCHEMA;
    }
    return RAY_OK;
}

ray_err_t ray_block_query_open_parted(const char* root, const char* table, bool immutable,
                                      const char* const* columns, size_t count,
                                      const ray_block_predicate_t* predicates,
                                      size_t n_predicates,
                                      const ray_block_scan_options_t* options,
                                      ray_block_query_t** out) {
    if (!out || *out || !root || !*root || !query_name_safe(table) || !columns || !count ||
        !options || (n_predicates && !predicates)) return RAY_ERR_DOMAIN;
    if (count > RAY_BLOCK_SCAN_MAX_COLUMNS || n_predicates > RAY_BLOCK_QUERY_MAX_PREDICATES)
        return RAY_ERR_LIMIT;
    if (strlen(root) >= sizeof(((block_parted_t*)0)->root)) return RAY_ERR_RANGE;
    for (size_t i = 0; i < count; i++) {
        if (!query_name_safe(columns[i])) return RAY_ERR_DOMAIN;
        for (size_t j = 0; j < i; j++)
            if (!strcmp(columns[i], columns[j])) return RAY_ERR_DOMAIN;
    }
    for (size_t i = 0; i < n_predicates; i++) {
        if (!query_name_safe(predicates[i].column)) return RAY_ERR_DOMAIN;
        if (!query_opcode_admitted(predicates[i].opcode)) return RAY_ERR_NYI;
        /* Unsupported, not wrong: native select may accept these. */
        if (!query_atom_admitted(predicates[i].value)) return RAY_ERR_NYI;
        if (!query_pair_admitted(predicates[i].opcode, predicates[i].value)) return RAY_ERR_NYI;
    }
    block_parted_t* p = calloc(1, sizeof(*p));
    ray_block_query_t* q = calloc(1, sizeof(*q));
    if (!p || !q) { free(p); free(q); return RAY_ERR_OOM; }
    q->parted = p;
    ray_err_t err = RAY_OK;
    memcpy(p->root, root, strlen(root) + 1);
    memcpy(p->table, table, strlen(table) + 1);
    p->immutable = immutable;
    p->options = *options;
    p->outputs = calloc(count, sizeof(*p->outputs));
    p->data = calloc(count, sizeof(*p->data));
    p->data_types = calloc(count, sizeof(*p->data_types));
    if (!p->outputs || !p->data || !p->data_types) err = RAY_ERR_OOM;
    struct stat st;
    int n = snprintf(p->sym_path, sizeof(p->sym_path), "%s/.sym", root);
    if (!err && (n < 0 || (size_t)n >= sizeof(p->sym_path))) err = RAY_ERR_RANGE;
    p->has_sym = !err && stat(p->sym_path, &st) == 0;
    ray_t* listed = err ? NULL : ray_parted_dirs(root, &p->dirs, &p->n_dirs, &p->mc_type);
    if (listed) { err = ray_err_from_obj(listed); ray_error_free(listed); }
    if (!err && !p->n_dirs) err = RAY_ERR_IO;
    if (!err) {
        const char* key = p->mc_type == RAY_MC_DATE ? "date" : "part";
        p->key_type = p->mc_type == RAY_MC_DATE ? RAY_DATE :
            p->mc_type == RAY_MC_I64 ? RAY_I64 : RAY_SYM;
        p->key_name = ray_sym_intern(key, strlen(key));
        if (p->key_name < 0) err = RAY_ERR_OOM;
    }
    const char* key_column = p->mc_type == RAY_MC_DATE ? "date" : "part";
    for (size_t i = 0; !err && i < count; i++) {
        parted_output_t* o = &p->outputs[i];
        o->name = ray_sym_intern(columns[i], strlen(columns[i]));
        if (o->name < 0) { err = RAY_ERR_OOM; break; }
        o->key = !strcmp(columns[i], key_column);
        if (o->key) { p->row_bytes += p->key_type == RAY_DATE ? 4 : 8; continue; }
        o->data = p->n_data;
        memcpy(p->data[p->n_data], columns[i], strlen(columns[i]) + 1);
        p->data_names[p->n_data] = p->data[p->n_data];
        p->n_data++;
    }
    p->n_outputs = count;
    if (!err && !p->n_data) err = RAY_ERR_NYI;
    for (size_t i = 0; !err && i < n_predicates; i++) {
        const ray_t* v = predicates[i].value;
        if (strcmp(predicates[i].column, key_column)) {
            predicate_t* d = &p->predicates[p->n_predicates];
            memcpy(d->column, predicates[i].column, strlen(predicates[i].column) + 1);
            d->opcode = predicates[i].opcode;
            d->value = ray_i64(0);
            if (!d->value || RAY_IS_ERR(d->value)) { d->value = NULL; err = RAY_ERR_OOM; break; }
            /* Admitted atoms contain only inline scalar data, never pointers. */
            d->value->type = v->type;
            memcpy(&d->value->i64, &v->i64, sizeof(int64_t));
            d->value->aux[0] = v->aux[0] & 1;
            p->n_predicates++;
            continue;
        }
        if (v->type != -p->key_type) { err = RAY_ERR_NYI; break; }
        if (RAY_ATOM_IS_NULL(v)) { err = RAY_ERR_NYI; break; }
        p->keys[p->n_keys++] = (key_predicate_t){predicates[i].opcode, query_atom_value(v)};
    }
    /* The first partition defines the schema, as for .db.parted.get. */
    ray_block_query_t* first = NULL;
    if (!err) err = parted_open_partition(p, 0, p->data_names, p->n_data, true, &first);
    if (!err && ray_block_scan_has_column(first->scan, key_column)) err = RAY_ERR_SCHEMA;
    if (!err) {
        p->names = ray_block_scan_names(first->scan);
        ray_retain(p->names);
        p->first_rows = ray_block_query_rows(first);
        q->schema = ray_table_new((int64_t)count);
        if (!q->schema || RAY_IS_ERR(q->schema)) { q->schema = NULL; err = RAY_ERR_OOM; }
    }
    for (size_t i = 0; !err && i < count; i++) {
        parted_output_t* o = &p->outputs[i];
        int8_t type = o->key ? p->key_type :
            ray_table_get_col_idx(first->schema, (int64_t)o->data)->type;
        if (!o->key) p->data_types[o->data] = type;
        if (!o->key && type == RAY_SYM) p->row_bytes += 8;
        ray_t* col = type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, 0) : ray_vec_new(type, 0);
        if (!col || RAY_IS_ERR(col)) { err = RAY_ERR_OOM; break; }
        q->schema = ray_table_add_col(q->schema, o->name, col);
        ray_release(col);
        if (!q->schema || RAY_IS_ERR(q->schema)) { q->schema = NULL; err = RAY_ERR_OOM; }
    }
    q->count = count;
    if (!err && !parted_pruned(p, 0)) {
        p->current = first; first = NULL;
        p->reading = 0;
        p->rows_read = p->first_rows;
        p->done.partitions_read = 1;
    } else if (!err) p->done.partitions_pruned = 1;
    p->next = 1;
    ray_block_query_close(&first);
    if (err) { ray_block_query_close(&q); return err; }
    *out = q;
    return RAY_OK;
}

static ray_err_t parted_reserve(block_parted_t* p) {
    if (!p->current) return RAY_OK;
    size_t extra = (size_t)ray_block_query_batch_rows(p->current);
    if (p->row_bytes && extra > SIZE_MAX / p->row_bytes) return RAY_ERR_LIMIT;
    extra *= p->row_bytes;
    if (extra > SIZE_MAX - p->external_reserve) return RAY_ERR_LIMIT;
    return ray_block_query_reserve(p->current, p->external_reserve + extra);
}

static ray_t* parted_key_vector(const block_parted_t* p, int64_t rows) {
    int64_t key = ray_parted_dir_key(p->dirs[p->reading], p->mc_type);
    if (key < 0 && p->key_type == RAY_SYM) return ray_error("oom", NULL);
    ray_t* v = p->key_type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, rows) :
        ray_vec_new(p->key_type, rows);
    if (!v || RAY_IS_ERR(v)) return v;
    for (int64_t i = 0; i < rows; i++) {
        if (p->key_type == RAY_DATE) ((int32_t*)ray_data(v))[i] = (int32_t)key;
        else ((int64_t*)ray_data(v))[i] = key;
    }
    v->len = rows;
    return v;
}

/* Generation dictionaries differ between partitions: intern into the
 * runtime table so every batch of one column shares a single domain. */
static ray_t* parted_global_symbols(ray_t* src) {
    ray_t* v = ray_sym_vec_new(RAY_SYM_W64, src->len);
    if (!v || RAY_IS_ERR(v)) return v;
    int64_t last_pos = -1, last_id = 0;
    for (int64_t i = 0; i < src->len; i++) {
        int64_t pos = ray_read_sym(ray_data(src), i, RAY_SYM, src->attrs);
        if (pos != last_pos) {
            ray_t* s = ray_sym_vec_cell(src, i);
            if (!s) { ray_release(v); return ray_error("corrupt", "symbol outside its dictionary"); }
            last_id = ray_sym_intern(ray_str_ptr(s), ray_str_len(s));
            if (last_id < 0) { ray_release(v); return ray_error("oom", NULL); }
            last_pos = pos;
        }
        ((int64_t*)ray_data(v))[i] = last_id;
    }
    v->len = src->len;
    v->attrs |= src->attrs & RAY_ATTR_HAS_NULLS;
    return v;
}

static ray_t* parted_batch(const block_parted_t* p, ray_t* batch) {
    int64_t rows = ray_table_nrows(batch);
    ray_t* out = ray_table_new((int64_t)p->n_outputs);
    for (size_t c = 0; out && !RAY_IS_ERR(out) && c < p->n_outputs; c++) {
        const parted_output_t* o = &p->outputs[c];
        ray_t* col;
        if (o->key) col = parted_key_vector(p, rows);
        else {
            col = ray_table_get_col_idx(batch, (int64_t)o->data);
            if (col->type == RAY_SYM) col = parted_global_symbols(col);
            else ray_retain(col);
        }
        if (!col || RAY_IS_ERR(col)) { ray_release(out); out = col; break; }
        out = ray_table_add_col(out, o->name, col);
        ray_release(col);
    }
    return out ? out : ray_error("oom", NULL);
}

static ray_t* parted_error(const block_parted_t* p, ray_err_t err, int64_t partition,
                           const char* what) {
    return ray_error(ray_err_code_str(err), "block parted %s: partition %s/%s: %s",
                     p->root, p->dirs[partition], p->table, what);
}

static ray_t* parted_next(ray_block_query_t* q) {
    block_parted_t* p = q->parted;
    for (;;) {
        if (p->cancelled || ray_eval_is_interrupted())
            return ray_error("cancel", "block query cancelled");
        if (!p->current) {
            while (p->next < p->n_dirs && parted_pruned(p, p->next)) {
                p->next++;
                p->done.partitions_pruned++;
            }
            if (p->next == p->n_dirs) return NULL;
            int64_t partition = p->next++;
            ray_err_t err = parted_open_partition(p, partition, p->data_names, p->n_data,
                                                  true, &p->current);
            if (!err) err = parted_check_schema(p, p->current);
            if (err) {
                ray_block_query_close(&p->current);
                return parted_error(p, err, partition, "open failed");
            }
            p->reading = partition;
            p->rows_read += ray_block_query_rows(p->current);
            p->done.partitions_read++;
        }
        ray_err_t err = parted_reserve(p);
        if (err) return ray_error(ray_err_code_str(err), "block query working set exceeds limit");
        ray_t* batch = ray_block_query_next(p->current);
        if (!batch) { parted_close_current(p); continue; }
        if (RAY_IS_ERR(batch)) return batch;
        ray_t* out = parted_batch(p, batch);
        ray_release(batch);
        return out;
    }
}

static ray_err_t parted_source_empty(block_parted_t* p, bool* empty) {
    *empty = false;
    if (p->first_rows || p->rows_read) return RAY_OK;
    for (int64_t i = 1; i < p->n_dirs; i++) {
        ray_block_query_t* metadata = NULL;
        ray_err_t err = parted_open_partition(p, i, p->data_names, 1, false, &metadata);
        uint64_t rows = err ? 0 : ray_block_query_rows(metadata);
        ray_block_query_close(&metadata);
        if (err) return err;
        if (rows) return RAY_OK;
    }
    *empty = true;
    return RAY_OK;
}

static bool parted_has_column(const block_parted_t* p, const char* name) {
    if (!name) return false;
    if (!strcmp(name, p->mc_type == RAY_MC_DATE ? "date" : "part")) return true;
    size_t len = strlen(name);
    for (int64_t i = 0; i < p->names->len; i++) {
        size_t n;
        const char* s = ray_str_vec_get(p->names, i, &n);
        if (n == len && !memcmp(s, name, n)) return true;
    }
    return false;
}

static void parted_cancel(block_parted_t* p) {
    p->cancelled = true;
    ray_block_query_cancel(p->current);
}

static uint64_t parted_rows(const block_parted_t* p) {
    return p->rows_read;
}

static void parted_stats(const block_parted_t* p, ray_block_query_stats_t* out) {
    *out = p->done;
    if (!p->current) return;
    ray_block_query_stats_t s;
    ray_block_query_stats(p->current, &s);
    out->scan.blocks_decoded += s.scan.blocks_decoded;
    out->scan.raw_rows += s.scan.raw_rows;
    out->batches_skipped += s.batches_skipped;
}

static ray_err_t parted_set_reserve(block_parted_t* p, size_t bytes) {
    if (p->options.work_limit && bytes > p->options.work_limit) return RAY_ERR_LIMIT;
    size_t previous = p->external_reserve;
    p->external_reserve = bytes;
    ray_err_t err = parted_reserve(p);
    if (err) p->external_reserve = previous;
    return err;
}

static uint32_t parted_batch_rows(const block_parted_t* p) {
    return p->current ? ray_block_query_batch_rows(p->current) : 0;
}
