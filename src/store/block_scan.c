/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_scan.h"
#include "col_block.h"
#include "splay.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    ray_col_block_file_t* file;
    int64_t name;
} scan_column_t;

struct ray_block_scan_s {
    ray_splay_lease_t* lease;
    scan_column_t* columns;
    size_t count;
    ray_block_scan_options_t options;
    uint64_t cursor, end;
    bool failed, cancelled;
};

void ray_block_scan_close(ray_block_scan_t** scan) {
    if (!scan || !*scan) return;
    ray_block_scan_t* s = *scan;
    for (size_t i = 0; i < s->count; i++) ray_col_block_file_close(&s->columns[i].file);
    free(s->columns);
    ray_splay_lease_release(&s->lease);
    free(s);
    *scan = NULL;
}

ray_err_t ray_block_scan_open(const char* root, const char* const* columns,
                              size_t count, const ray_block_scan_options_t* options,
                              ray_block_scan_t** out) {
    if (!out || *out || !options || !options->batch_rows || !columns || !count)
        return RAY_ERR_DOMAIN;
    if (count > 1024) return RAY_ERR_LIMIT;
    for (size_t i = 0; i < count; i++) {
        if (!columns[i] || !*columns[i] || columns[i][0] == '.' ||
            strchr(columns[i], '/') || strchr(columns[i], '\\')) return RAY_ERR_DOMAIN;
        if (strlen(columns[i]) > 255) return RAY_ERR_RANGE;
        for (size_t j = 0; j < i; j++)
            if (!strcmp(columns[i], columns[j])) return RAY_ERR_DOMAIN;
    }
    ray_block_scan_t* s = calloc(1, sizeof(*s));
    if (!s) return RAY_ERR_OOM;
    s->options = *options;
    s->columns = calloc(count, sizeof(*s->columns));
    if (!s->columns) { free(s); return RAY_ERR_OOM; }
    s->count = count;
    ray_err_t err = ray_splay_lease_acquire(root, &s->lease);
    uint64_t rows = 0, generation = 0;
    for (size_t i = 0; !err && i < count; i++) {
        char path[1300];
        int n = snprintf(path, sizeof(path), "%s/%s", ray_splay_lease_dir(s->lease), columns[i]);
        if (n < 0 || (size_t)n >= sizeof(path)) { err = RAY_ERR_RANGE; break; }
        err = ray_col_block_file_open(path, &s->columns[i].file);
        if (err) break;
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        if (!i) { rows = r->rows; generation = r->generation; }
        else if (r->rows != rows || r->generation != generation) { err = RAY_ERR_SCHEMA; break; }
        s->columns[i].name = ray_sym_intern(columns[i], strlen(columns[i]));
        if (s->columns[i].name < 0) err = RAY_ERR_OOM;
    }
    if (!err) {
        if (options->start > rows ||
            (options->count != UINT64_MAX && options->count > rows - options->start)) err = RAY_ERR_RANGE;
        else {
            s->cursor = options->start;
            s->end = options->count == UINT64_MAX ? rows : s->cursor + options->count;
        }
    }
    if (err) { ray_block_scan_close(&s); return err; }
    *out = s;
    return RAY_OK;
}

ray_t* ray_block_scan_schema(const ray_block_scan_t* s) {
    if (!s) return ray_error("domain", "null block scan");
    ray_t* schema = ray_table_new((int64_t)s->count);
    if (!schema) return ray_error("oom", "block scan schema allocation failed");
    for (size_t i = 0; !RAY_IS_ERR(schema) && i < s->count; i++) {
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        ray_t* col = ray_col_block_materialize(r, 0, 0, 0, NULL, 0);
        if (RAY_IS_ERR(col)) { ray_release(schema); return col; }
        schema = ray_table_add_col(schema, s->columns[i].name, col);
        ray_release(col);
        if (!schema) return ray_error("oom", "block scan schema allocation failed");
    }
    return schema;
}

void ray_block_scan_cancel(ray_block_scan_t* s) {
    if (s) s->cancelled = true;
}

static ray_t* scan_error(ray_block_scan_t* s, ray_err_t err) {
    s->failed = true;
    return ray_error(ray_err_code_str(err), "block scan failed");
}

ray_t* ray_block_scan_next(ray_block_scan_t* s) {
    if (!s) return ray_error("domain", "null block scan");
    if (s->failed) return ray_error("domain", "block scan is terminal; close it");
    if (s->cancelled) return scan_error(s, RAY_ERR_CANCEL);
    if (s->cursor == s->end) return NULL;
    uint64_t rows = s->end - s->cursor;
    if (rows > s->options.batch_rows) rows = s->options.batch_rows;
    size_t total = 0, scratch_bytes = 0;
    for (size_t i = 0; i < s->count; i++) {
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        size_t bytes, scratch;
        ray_err_t err = ray_col_block_range_size(r, s->cursor, rows, &bytes, &scratch);
        if (err) return scan_error(s, err);
        if (bytes > s->options.payload_limit - total || scratch > s->options.scratch_limit)
            return scan_error(s, RAY_ERR_LIMIT);
        total += bytes;
        if (scratch_bytes < scratch) scratch_bytes = scratch;
    }
    void* scratch = scratch_bytes ? malloc(scratch_bytes) : NULL;
    if (scratch_bytes && !scratch) return scan_error(s, RAY_ERR_OOM);
    ray_t* batch = ray_table_new((int64_t)s->count);
    if (!batch) { free(scratch); return scan_error(s, RAY_ERR_OOM); }
    for (size_t i = 0; !RAY_IS_ERR(batch) && i < s->count; i++) {
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        ray_t* col = ray_col_block_materialize(r, s->cursor, rows,
            s->options.payload_limit, scratch, scratch_bytes);
        if (RAY_IS_ERR(col)) { ray_release(batch); batch = col; break; }
        batch = ray_table_add_col(batch, s->columns[i].name, col);
        ray_release(col);
        if (!batch) { batch = ray_error("oom", "block scan batch allocation failed"); break; }
    }
    free(scratch);
    if (RAY_IS_ERR(batch)) s->failed = true;
    else s->cursor += rows;
    return batch;
}
