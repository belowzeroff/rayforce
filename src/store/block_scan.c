/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_scan.h"
#include "col_block.h"
#include "col.h"
#include "splay.h"
#include "block_sym.h"
#include "table/domain.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

typedef struct {
    ray_col_block_file_t* file;
    int64_t name;
    size_t payload_bytes;
} scan_column_t;

struct ray_block_scan_s {
    ray_splay_lease_t* lease;
    ray_sym_domain_t* domain;
    ray_t* names;
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
    ray_release(s->names);
    if (s->domain) ray_sym_domain_release(s->domain);
    ray_splay_lease_release(&s->lease);
    free(s);
    *scan = NULL;
}

static bool scan_name_safe(const char* name, size_t len) {
    return name && len && len <= 255 && name[0] != '.' &&
        !memchr(name, '/', len) && !memchr(name, '\\', len) && !memchr(name, '\0', len);
}

/* The lease prevents prune across stat/load; immutability is a writer contract.
 * Use the normal .d decoder, with an on-disk cap before allocating vectors. */
static ray_err_t scan_check_schema(const char* dir, const char* const* columns, size_t count,
                                   ray_t** names) {
    char path[1100];
    int n = snprintf(path, sizeof(path), "%s/.d", dir);
    if (n < 0 || (size_t)n >= sizeof(path)) return RAY_ERR_RANGE;
    struct stat st;
    if (stat(path, &st) != 0) return RAY_ERR_IO;
    if (!S_ISREG(st.st_mode) || st.st_size <= 0) return RAY_ERR_CORRUPT;
    if ((uint64_t)st.st_size > RAY_BLOCK_SCAN_SCHEMA_MAX_BYTES) return RAY_ERR_LIMIT;
    ray_t* schema = ray_col_load(path);
    if (!schema) return RAY_ERR_OOM;
    if (RAY_IS_ERR(schema)) {
        const char* code = ray_err_code(schema);
        ray_err_t err = !strcmp(code, "oom") ? RAY_ERR_OOM :
            !strcmp(code, "io") ? RAY_ERR_IO :
            !strcmp(code, "version") ? RAY_ERR_VERSION : RAY_ERR_CORRUPT;
        ray_error_free(schema);
        return err;
    }
    ray_err_t err = RAY_OK;
    if (schema->type != RAY_STR || schema->len < 0) err = RAY_ERR_CORRUPT;
    else if ((uint64_t)schema->len > RAY_BLOCK_SCAN_MAX_COLUMNS) err = RAY_ERR_LIMIT;
    bool found[RAY_BLOCK_SCAN_MAX_COLUMNS] = {false};
    for (int64_t i = 0; !err && i < schema->len; i++) {
        size_t len = 0;
        const char* name = ray_str_vec_get(schema, i, &len);
        if (!scan_name_safe(name, len)) { err = RAY_ERR_CORRUPT; break; }
        for (int64_t j = 0; j < i; j++) {
            size_t previous_len = 0;
            const char* previous = ray_str_vec_get(schema, j, &previous_len);
            if (previous_len == len && !memcmp(previous, name, len)) {
                err = RAY_ERR_CORRUPT; break;
            }
        }
        for (size_t j = 0; j < count; j++)
            if (strlen(columns[j]) == len && !memcmp(columns[j], name, len)) found[j] = true;
    }
    for (size_t j = 0; !err && j < count; j++)
        if (!found[j]) err = RAY_ERR_SCHEMA;
    if (err) ray_release(schema);
    else *names = schema;
    return err;
}

ray_err_t ray_block_scan_open(const char* root, const char* const* columns,
                              size_t count, const ray_block_scan_options_t* options,
                              ray_block_scan_t** out) {
    if (!out || *out || !options || !options->batch_rows || !columns || !count)
        return RAY_ERR_DOMAIN;
    if (count > RAY_BLOCK_SCAN_MAX_COLUMNS) return RAY_ERR_LIMIT;
    for (size_t i = 0; i < count; i++) {
        if (!columns[i]) return RAY_ERR_DOMAIN;
        if (strlen(columns[i]) > 255) return RAY_ERR_RANGE;
        if (!scan_name_safe(columns[i], strlen(columns[i]))) return RAY_ERR_DOMAIN;
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
    if (!err) err = scan_check_schema(ray_splay_lease_dir(s->lease), columns, count, &s->names);
    uint64_t rows = 0, generation = 0, sym_count = 0;
    uint32_t sym_crc = 0;
    for (size_t i = 0; !err && i < count; i++) {
        char path[1300];
        int n = snprintf(path, sizeof(path), "%s/%s", ray_splay_lease_dir(s->lease), columns[i]);
        if (n < 0 || (size_t)n >= sizeof(path)) { err = RAY_ERR_RANGE; break; }
        err = ray_col_block_file_open(path, &s->columns[i].file);
        if (err) break;
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        if (!i) { rows = r->rows; generation = r->generation; }
        else if (r->rows != rows || r->generation != generation) { err = RAY_ERR_SCHEMA; break; }
        if (r->type == RAY_SYM) {
            if (sym_count && (sym_count != r->sym_count || sym_crc != r->sym_crc)) {
                err = RAY_ERR_SCHEMA; break;
            }
            sym_count = r->sym_count; sym_crc = r->sym_crc;
        }
        s->columns[i].name = ray_sym_intern(columns[i], strlen(columns[i]));
        if (s->columns[i].name < 0) err = RAY_ERR_OOM;
    }
    if (!err && sym_count) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/.sym", ray_splay_lease_dir(s->lease));
        uint64_t actual_count;
        uint32_t actual_crc;
        err = ray_block_sym_checksum(path, &actual_count, &actual_crc);
        if (!err && (actual_count != sym_count || actual_crc != sym_crc)) err = RAY_ERR_CORRUPT;
        if (!err) {
            s->domain = ray_sym_domain_open(path);
            if (!s->domain || (uint64_t)ray_sym_domain_count(s->domain) != sym_count)
                err = RAY_ERR_CORRUPT;
        }
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

uint64_t ray_block_scan_rows(const ray_block_scan_t* s) {
    return s ? s->end - s->options.start : 0;
}

bool ray_block_scan_has_column(const ray_block_scan_t* s, const char* name) {
    if (!s || !name) return false;
    size_t len = strlen(name);
    for (int64_t i = 0; i < s->names->len; i++) {
        size_t n;
        const char* p = ray_str_vec_get(s->names, i, &n);
        if (n == len && !memcmp(p, name, n)) return true;
    }
    return false;
}

ray_t* ray_block_scan_schema(const ray_block_scan_t* s) {
    if (!s) return ray_error("domain", "null block scan");
    ray_t* schema = ray_table_new((int64_t)s->count);
    if (!schema) return ray_error("oom", "block scan schema allocation failed");
    for (size_t i = 0; !RAY_IS_ERR(schema) && i < s->count; i++) {
        const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
        ray_t* col = ray_col_block_materialize_dom(r, 0, 0, 0, NULL, 0, s->domain);
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

static ray_err_t scan_validate_columns(const ray_block_scan_t* s,
                                      const size_t* indices, size_t count) {
    if (!count || count > s->count) return RAY_ERR_DOMAIN;
    if (!indices) return RAY_OK;
    bool seen[RAY_BLOCK_SCAN_MAX_COLUMNS] = {false};
    for (size_t i = 0; i < count; i++) {
        size_t index = indices[i];
        if (index >= s->count) return RAY_ERR_RANGE;
        if (seen[index]) return RAY_ERR_DOMAIN;
        seen[index] = true;
    }
    return RAY_OK;
}

static ray_t* scan_materialize(ray_block_scan_t* s, const size_t* indices,
                              size_t count, uint64_t start, uint64_t rows) {
    size_t total = 0, scratch_bytes = 0;
    for (size_t i = 0; i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        const ray_col_block_reader_t* r = ray_col_block_file_reader(column->file);
        size_t bytes, scratch;
        ray_err_t err = ray_col_block_range_size(r, start, rows, &bytes, &scratch);
        if (err) return scan_error(s, err);
        if (bytes > s->options.payload_limit - total || scratch > s->options.scratch_limit)
            return scan_error(s, RAY_ERR_LIMIT);
        column->payload_bytes = bytes;
        total += bytes;
        if (scratch_bytes < scratch) scratch_bytes = scratch;
    }
    void* scratch = scratch_bytes ? malloc(scratch_bytes) : NULL;
    if (scratch_bytes && !scratch) return scan_error(s, RAY_ERR_OOM);
    /* STR sizes depend on decoded lengths. Check the entire batch's exact
     * descriptor/pool sum before allocating any output column. */
    total = 0;
    for (size_t i = 0; i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        const ray_col_block_reader_t* r = ray_col_block_file_reader(column->file);
        size_t bytes = column->payload_bytes;
        ray_err_t err = RAY_OK;
        if (r->type == RAY_STR)
            err = ray_col_block_payload_size(r, start, rows, scratch, scratch_bytes, &bytes);
        if (!err && bytes > s->options.payload_limit - total) err = RAY_ERR_LIMIT;
        if (err) { free(scratch); return scan_error(s, err); }
        column->payload_bytes = bytes;
        total += bytes;
    }
    ray_t* batch = ray_table_new((int64_t)count);
    if (!batch) { free(scratch); return scan_error(s, RAY_ERR_OOM); }
    for (size_t i = 0; !RAY_IS_ERR(batch) && i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        const ray_col_block_reader_t* r = ray_col_block_file_reader(column->file);
        ray_t* col = r->type == RAY_STR ?
            ray_col_block_materialize_str(r, start, rows,
                column->payload_bytes, scratch, scratch_bytes) :
            ray_col_block_materialize_dom(r, start, rows,
                s->options.payload_limit, scratch, scratch_bytes, s->domain);
        if (RAY_IS_ERR(col)) { ray_release(batch); batch = col; break; }
        batch = ray_table_add_col(batch, column->name, col);
        ray_release(col);
        if (!batch) { batch = ray_error("oom", "block scan batch allocation failed"); break; }
    }
    free(scratch);
    if (RAY_IS_ERR(batch)) s->failed = true;
    return batch;
}

ray_t* ray_block_scan_read_columns(ray_block_scan_t* s, const size_t* indices,
                                   size_t count, uint64_t start, uint64_t rows) {
    if (!s) return ray_error("domain", "null block scan");
    if (s->failed) return ray_error("domain", "block scan is terminal; close it");
    if (s->cancelled) return scan_error(s, RAY_ERR_CANCEL);
    ray_err_t err = scan_validate_columns(s, indices, count);
    if (err) return scan_error(s, err);
    if (start < s->options.start || start > s->end || rows > s->end - start ||
        rows > s->options.batch_rows) return scan_error(s, RAY_ERR_RANGE);
    return scan_materialize(s, indices, count, start, rows);
}

ray_t* ray_block_scan_next_columns(ray_block_scan_t* s, const size_t* indices,
                                   size_t count, uint64_t* row_start) {
    if (!s) return ray_error("domain", "null block scan");
    if (s->failed) return ray_error("domain", "block scan is terminal; close it");
    if (s->cancelled) return scan_error(s, RAY_ERR_CANCEL);
    ray_err_t err = scan_validate_columns(s, indices, count);
    if (err) return scan_error(s, err);
    if (s->cursor == s->end) return NULL;
    uint64_t rows = s->end - s->cursor;
    if (rows > s->options.batch_rows) rows = s->options.batch_rows;
    ray_t* batch = scan_materialize(s, indices, count, s->cursor, rows);
    if (!RAY_IS_ERR(batch)) {
        if (row_start) *row_start = s->cursor;
        s->cursor += rows;
    }
    return batch;
}

ray_t* ray_block_scan_next(ray_block_scan_t* s) {
    return ray_block_scan_next_columns(s, NULL, s ? s->count : 0, NULL);
}
