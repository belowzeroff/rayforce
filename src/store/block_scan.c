/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_scan.h"
#include "col_block.h"
#include "col.h"
#include "splay.h"
#include "block_sym.h"
#include "core/types.h"
#include "mem/heap.h"
#include "table/sym.h"
#include "table/domain.h"
#include "vec/str.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

typedef struct {
    ray_col_block_file_t* file;
    ray_t* raw;
    int64_t name;
    size_t payload_bytes;
    uint8_t* arena;     /* STR: the batch range decoded once, while reading */
    size_t arena_bytes;
} scan_column_t;

struct ray_block_scan_s {
    ray_splay_lease_t* lease;
    char dir[1024];
    ray_sym_domain_t* domain;
    ray_sym_domain_t* raw_domain; /* source->sym_path, when given */
    ray_t* names;
    scan_column_t* columns;
    size_t count;
    uint64_t domain_count;
    uint32_t domain_crc;
    size_t reserved_bytes;
    ray_block_scan_stats_t stats;
    ray_block_scan_options_t options;
    uint64_t cursor, end;
    bool failed, cancelled;
};

static uint32_t scan_auto_rows(const ray_block_scan_t* s);

void ray_block_scan_close(ray_block_scan_t** scan) {
    if (!scan || !*scan) return;
    ray_block_scan_t* s = *scan;
    for (size_t i = 0; i < s->count; i++) {
        ray_col_block_file_close(&s->columns[i].file);
        ray_release(s->columns[i].raw);
    }
    free(s->columns);
    ray_release(s->names);
    if (s->domain) ray_sym_domain_release(s->domain);
    if (s->raw_domain) ray_sym_domain_release(s->raw_domain);
    ray_splay_lease_release(&s->lease);
    free(s);
    *scan = NULL;
}

static ray_err_t scan_value_error(ray_t* value) {
    if (!value) return RAY_ERR_OOM;
    if (!RAY_IS_ERR(value)) return RAY_OK;
    ray_err_t err = ray_err_from_obj(value);
    return err == RAY_ERR_DOMAIN && strcmp(ray_err_code(value), "domain") ?
        RAY_ERR_CORRUPT : err;
}

static ray_err_t scan_open_domain(ray_block_scan_t* s, const char* dir,
                                  uint64_t* count, uint32_t* crc) {
    if (!s->domain) {
        char path[1100];
        int n = snprintf(path, sizeof(path), "%s/.sym", dir);
        if (n < 0 || (size_t)n >= sizeof(path)) return RAY_ERR_RANGE;
        ray_err_t err = ray_block_sym_checksum(path, &s->domain_count, &s->domain_crc);
        if (err) return err;
        s->domain = ray_sym_domain_open(path);
        if (!s->domain || (uint64_t)ray_sym_domain_count(s->domain) != s->domain_count)
            return RAY_ERR_CORRUPT;
    }
    *count = s->domain_count;
    *crc = s->domain_crc;
    return RAY_OK;
}

/* Raw SYM columns use the table's own symbol file: <root>/.sym, where raw
 * writers put it whether or not the table has generations; a flat immutable
 * root is its own directory. Block columns use the generation's dictionary. */
static const char* scan_root_sym(const char* root, const char* dir, char* buf, size_t size) {
    size_t len = strlen(root);
    while (len > 1 && root[len - 1] == '/') len--;
    int n = snprintf(buf, size, "%.*s/.sym", (int)len, root);
    struct stat st;
    if (n > 0 && (size_t)n < size && stat(buf, &st) == 0) return buf;
    n = snprintf(buf, size, "%s/.sym", dir);
    return n > 0 && (size_t)n < size ? buf : "";
}

static ray_err_t scan_open_raw_domain(ray_block_scan_t* s, const char* path) {
    struct stat st;
    if (stat(path, &st) != 0) return RAY_ERR_IO;
    s->raw_domain = ray_sym_domain_open(path);
    return s->raw_domain ? RAY_OK : RAY_ERR_CORRUPT;
}

/* An immutable flat root is the table directory itself. A root that has
 * published generations is never read this way: its columns live below. */
static ray_err_t scan_flat_dir(const char* root, char* dir, size_t size) {
    size_t len = strlen(root);
    while (len > 1 && root[len - 1] == '/') len--;
    if (len >= size) return RAY_ERR_RANGE;
    memcpy(dir, root, len);
    dir[len] = '\0';
    char path[1100];
    int n = snprintf(path, sizeof(path), "%s/.current", dir);
    if (n < 0 || (size_t)n >= sizeof(path)) return RAY_ERR_RANGE;
    struct stat st;
    if (stat(path, &st) == 0) return RAY_ERR_NYI;
    if (stat(dir, &st) != 0) return RAY_ERR_IO;
    return S_ISDIR(st.st_mode) ? RAY_OK : RAY_ERR_IO;
}

static int8_t scan_column_type(const scan_column_t* column) {
    if (column->raw) return column->raw->type;
    return (int8_t)ray_col_block_file_reader(column->file)->type;
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
    return ray_block_scan_open_source(root, NULL, columns, count, options, out);
}

ray_err_t ray_block_scan_open_source(const char* root, const ray_block_scan_source_t* source,
                                     const char* const* columns, size_t count,
                                     const ray_block_scan_options_t* options,
                                     ray_block_scan_t** out) {
    const char* sym_path = source ? source->sym_path : NULL;
    bool all = !columns && !count;
    if (!out || *out || !options || !options->batch_rows || (!all && (!columns || !count)))
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
    if (!all) {
        s->columns = calloc(count, sizeof(*s->columns));
        if (!s->columns) { free(s); return RAY_ERR_OOM; }
        s->count = count;
    }
    ray_err_t err = ray_splay_lease_acquire(root, &s->lease);
    if (err == RAY_ERR_NYI && source && source->immutable)
        err = scan_flat_dir(root, s->dir, sizeof(s->dir));
    else if (!err) {
        const char* dir = ray_splay_lease_dir(s->lease);
        if (strlen(dir) >= sizeof(s->dir)) err = RAY_ERR_RANGE;
        else memcpy(s->dir, dir, strlen(dir) + 1);
    }
    if (!err) err = scan_check_schema(s->dir, columns, count, &s->names);
    /* Every column of the schema read under this lease, in its order. */
    char (*all_names)[256] = NULL;
    const char** all_list = NULL;
    if (!err && all) {
        count = (size_t)s->names->len;
        if (!count) err = RAY_ERR_CORRUPT;
        else {
            all_names = malloc(count * sizeof(*all_names));
            all_list = malloc(count * sizeof(*all_list));
            s->columns = calloc(count, sizeof(*s->columns));
            if (!all_names || !all_list || !s->columns) err = RAY_ERR_OOM;
        }
        for (size_t i = 0; !err && i < count; i++) {
            size_t len;
            const char* name = ray_str_vec_get(s->names, (int64_t)i, &len);
            memcpy(all_names[i], name, len);
            all_names[i][len] = '\0';
            all_list[i] = all_names[i];
        }
        if (!err) { s->count = count; columns = all_list; }
    }
    uint64_t rows = 0, generation = 0, sym_count = 0;
    uint64_t actual_sym_count = 0;
    uint32_t sym_crc = 0, actual_sym_crc = 0;
    bool have_generation = false;
    for (size_t i = 0; !err && i < count; i++) {
        char path[1300];
        int n = snprintf(path, sizeof(path), "%s/%s", s->dir, columns[i]);
        if (n < 0 || (size_t)n >= sizeof(path)) { err = RAY_ERR_RANGE; break; }
        err = ray_col_block_file_open(path, &s->columns[i].file);
        uint64_t column_rows;
        if (err == RAY_ERR_VERSION) {
            err = RAY_OK;
            ray_t* raw = ray_col_mmap_splayed_dom(path, s->raw_domain);
            if (RAY_IS_ERR(raw) && !strcmp(ray_err_code(raw), "sym") && !s->raw_domain) {
                ray_error_free(raw);
                char sym[1100];
                err = scan_open_raw_domain(s, sym_path ? sym_path :
                                           scan_root_sym(root, s->dir, sym, sizeof(sym)));
                if (!err) raw = ray_col_mmap_splayed_dom(path, s->raw_domain);
            }
            if (!err) {
                err = scan_value_error(raw);
                if (err) ray_error_free(raw);
                else {
                    s->columns[i].raw = raw;
                    column_rows = (uint64_t)raw->len;
                }
            }
        } else if (!err) {
            const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[i].file);
            column_rows = r->rows;
            if (!have_generation) { generation = r->generation; have_generation = true; }
            else if (r->generation != generation) err = RAY_ERR_SCHEMA;
            if (!err && r->type == RAY_SYM) {
                if (sym_count && (sym_count != r->sym_count || sym_crc != r->sym_crc))
                    err = RAY_ERR_SCHEMA;
                else { sym_count = r->sym_count; sym_crc = r->sym_crc; }
            }
        }
        if (err) break;
        if (!i) rows = column_rows;
        else if (column_rows != rows) { err = RAY_ERR_SCHEMA; break; }
        s->columns[i].name = ray_sym_intern(columns[i], strlen(columns[i]));
        if (s->columns[i].name < 0) err = RAY_ERR_OOM;
    }
    if (!err && sym_count) {
        err = scan_open_domain(s, s->dir, &actual_sym_count, &actual_sym_crc);
        if (!err && (actual_sym_count != sym_count || actual_sym_crc != sym_crc))
            err = RAY_ERR_CORRUPT;
    }
    if (!err) {
        if (options->start > rows ||
            (options->count != UINT64_MAX && options->count > rows - options->start)) err = RAY_ERR_RANGE;
        else {
            s->cursor = options->start;
            s->end = options->count == UINT64_MAX ? rows : s->cursor + options->count;
        }
    }
    free(all_names); free(all_list);
    if (!err && s->options.batch_rows == RAY_BLOCK_SCAN_AUTO_ROWS)
        s->options.batch_rows = scan_auto_rows(s);
    if (err) { ray_block_scan_close(&s); return err; }
    *out = s;
    return RAY_OK;
}

uint64_t ray_block_scan_rows(const ray_block_scan_t* s) {
    return s ? s->end - s->options.start : 0;
}

ray_err_t ray_block_scan_next_range(const ray_block_scan_t* s,
                                    uint64_t* start, uint64_t* rows) {
    if (!s || !start || !rows || s->failed) return RAY_ERR_DOMAIN;
    *start = s->cursor;
    *rows = s->end - s->cursor;
    if (*rows > s->options.batch_rows) *rows = s->options.batch_rows;
    return RAY_OK;
}

static ray_col_block_cmp_t scan_col_cmp(ray_block_scan_cmp_t op) {
    switch (op) {
    case RAY_BLOCK_SCAN_EQ: return RAY_COL_BLOCK_EQ;
    case RAY_BLOCK_SCAN_NE: return RAY_COL_BLOCK_NE;
    case RAY_BLOCK_SCAN_LT: return RAY_COL_BLOCK_LT;
    case RAY_BLOCK_SCAN_LE: return RAY_COL_BLOCK_LE;
    case RAY_BLOCK_SCAN_GT: return RAY_COL_BLOCK_GT;
    default: return RAY_COL_BLOCK_GE;
    }
}

ray_err_t ray_block_scan_range_may_match(const ray_block_scan_t* s,
                                        size_t column, uint64_t start, uint64_t rows,
                                        ray_block_scan_cmp_t op, int64_t value,
                                        bool value_is_null, bool* may_match) {
    if (!s || !may_match || s->failed || column >= s->count || op > RAY_BLOCK_SCAN_GE)
        return RAY_ERR_DOMAIN;
    if (start < s->options.start || start > s->end || rows > s->end - start)
        return RAY_ERR_RANGE;
    scan_column_t* c = &s->columns[column];
    if (c->raw) { *may_match = true; return RAY_OK; }
    return ray_col_block_range_may_match(ray_col_block_file_reader(c->file), start, rows,
        scan_col_cmp(op), value, value_is_null, may_match);
}

uint64_t ray_block_scan_zone_rows(const ray_block_scan_t* s, size_t column) {
    if (!s || column >= s->count || s->columns[column].raw) return 0;
    const ray_col_block_reader_t* r = ray_col_block_file_reader(s->columns[column].file);
    switch (r->type) {
    case RAY_BOOL: case RAY_U8: case RAY_I16: case RAY_I32: case RAY_I64:
    case RAY_DATE: case RAY_TIME: case RAY_TIMESTAMP:
        return r->block_bytes / ray_elem_size((int8_t)r->type);
    default: return 0;
    }
}

ray_err_t ray_block_scan_skip_next(ray_block_scan_t* s, uint64_t* row_start) {
    if (!s || s->failed) return RAY_ERR_DOMAIN;
    if (s->cancelled) return RAY_ERR_CANCEL;
    uint64_t rows = s->end - s->cursor;
    if (rows > s->options.batch_rows) rows = s->options.batch_rows;
    if (!rows) return RAY_OK;
    if (row_start) *row_start = s->cursor;
    s->cursor += rows;
    return RAY_OK;
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

ray_err_t ray_block_scan_column_info(const ray_block_scan_t* s, size_t column,
                                     ray_block_scan_column_info_t* out) {
    if (!s || !out || column >= s->count) return RAY_ERR_DOMAIN;
    memset(out, 0, sizeof(*out));
    const scan_column_t* c = &s->columns[column];
    out->type = scan_column_type(c);
    if (!c->raw) {
        const ray_col_block_reader_t* r = ray_col_block_file_reader(c->file);
        out->compressed = true;
        out->rows = r->rows;
        out->file_bytes = r->size;
        out->block_bytes = r->block_bytes;
        return ray_col_block_summary(r, &out->blocks);
    }
    out->rows = (uint64_t)c->raw->len;
    ray_t* name = ray_sym_str(c->name);
    if (!name || RAY_IS_ERR(name)) return RAY_ERR_CORRUPT;
    char path[1300];
    int n = snprintf(path, sizeof(path), "%s/%.*s", s->dir, (int)ray_str_len(name), ray_str_ptr(name));
    if (n < 0 || (size_t)n >= sizeof(path)) return RAY_ERR_RANGE;
    struct stat st;
    if (stat(path, &st) != 0) return RAY_ERR_IO;
    out->file_bytes = (uint64_t)st.st_size;
    return RAY_OK;
}

ray_err_t ray_block_scan_verify_column(const ray_block_scan_t* s, size_t column,
                                       uint64_t* checked, uint64_t* bad_block) {
    if (!s || !checked || column >= s->count) return RAY_ERR_DOMAIN;
    *checked = 0;
    const scan_column_t* c = &s->columns[column];
    if (c->raw) return RAY_OK;
    const ray_col_block_reader_t* r = ray_col_block_file_reader(c->file);
    void* scratch = malloc(r->block_bytes ? r->block_bytes : 1);
    if (!scratch) return RAY_ERR_OOM;
    ray_err_t err = ray_col_block_verify(r, scratch, r->block_bytes, bad_block);
    free(scratch);
    if (!err) *checked = r->blocks;
    return err;
}

uint32_t ray_block_scan_batch_rows(const ray_block_scan_t* s) {
    return s ? s->options.batch_rows : 0;
}

/* Logical bytes per output row: fixed widths, SYM as W64, STR as its
 * descriptor plus the average decoded string of the column. */
static uint32_t scan_auto_rows(const ray_block_scan_t* s) {
    double width = 0, decoded = 0;  /* decoded: STR arena bytes per row */
    for (size_t i = 0; i < s->count; i++) {
        const scan_column_t* c = &s->columns[i];
        int8_t type = scan_column_type(c);
        if (type == RAY_SYM) width += 8;
        else if (type != RAY_STR) width += ray_elem_size(type);
        else if (c->raw) {
            /* Descriptor plus the column's average pooled (non-inline) bytes,
             * which scan_raw_range_size charges against the payload. */
            const ray_t* raw = c->raw->attrs & RAY_ATTR_SLICE ? c->raw->slice_parent : c->raw;
            const ray_t* pool = raw->str_pool;
            width += 16 + (raw->len > 0 && pool && !RAY_IS_ERR(pool) && pool->len > 0 ?
                (double)pool->len / (double)raw->len : 0);
        } else {
            const ray_col_block_reader_t* r = ray_col_block_file_reader(c->file);
            ray_col_block_summary_t sum;
            double avg = r->rows && !ray_col_block_summary(r, &sum) ?
                (double)sum.decoded_bytes / (double)r->rows : 0;
            width += 12 + avg;
            decoded += avg;
        }
    }
    size_t budget = s->options.payload_limit / 2;
    if (s->options.work_limit && s->options.work_limit / 4 < budget) budget = s->options.work_limit / 4;
    double rows = (double)budget / (width > 1 ? width : 1);
    /* Keep string arenas within scratch, so strings decode once per read;
     * a quarter is left for the whole boundary blocks a range also covers. */
    if (decoded > 0 && 0.75 * (double)s->options.scratch_limit / decoded < rows)
        rows = 0.75 * (double)s->options.scratch_limit / decoded;
    if (rows > RAY_BLOCK_SCAN_AUTO_MAX) rows = RAY_BLOCK_SCAN_AUTO_MAX;
    return rows < 1 ? 1 : (uint32_t)rows;
}

uint8_t ray_block_scan_column_attrs(const ray_block_scan_t* s, size_t column) {
    if (!s || column >= s->count || !s->columns[column].raw) return 0;
    return s->columns[column].raw->attrs & (RAY_ATTR_SORTED | RAY_ATTR_HAS_NULLS);
}

ray_t* ray_block_scan_names(const ray_block_scan_t* s) {
    return s ? s->names : NULL;
}

ray_t* ray_block_scan_schema(const ray_block_scan_t* s) {
    if (!s) return ray_error("domain", "null block scan");
    ray_t* schema = ray_table_new((int64_t)s->count);
    if (!schema) return ray_error("oom", "block scan schema allocation failed");
    for (size_t i = 0; !RAY_IS_ERR(schema) && i < s->count; i++) {
        scan_column_t* column = &s->columns[i];
        ray_t* col = column->raw ? ray_vec_new(scan_column_type(column), 0) :
            ray_col_block_materialize_dom(ray_col_block_file_reader(column->file),
                                          0, 0, 0, NULL, 0, s->domain);
        if (column->raw && col && !RAY_IS_ERR(col) && column->raw->type == RAY_SYM)
            ray_sym_vec_adopt_domain(col, column->raw);
        if (RAY_IS_ERR(col)) { ray_release(schema); return col; }
        schema = ray_table_add_col(schema, s->columns[i].name, col);
        ray_release(col);
        if (!schema) return ray_error("oom", "block scan schema allocation failed");
    }
    return schema;
}

static ray_err_t scan_raw_range_size(const ray_t* raw, uint64_t start, uint64_t rows,
                                     size_t* bytes) {
    if (!raw || raw->len < 0 || start > (uint64_t)raw->len ||
        rows > (uint64_t)raw->len - start) return RAY_ERR_RANGE;
    if (raw->type == RAY_STR) {
        if (rows > SIZE_MAX / sizeof(ray_str_t)) return RAY_ERR_LIMIT;
        size_t total = (size_t)rows * sizeof(ray_str_t);
        for (uint64_t i = 0; i < rows; i++) {
            size_t len = 0;
            if (!ray_str_vec_get((ray_t*)raw, (int64_t)(start + i), &len))
                return RAY_ERR_CORRUPT;
            if (len > RAY_STR_INLINE_MAX) {
                if (len > SIZE_MAX - total) return RAY_ERR_LIMIT;
                total += len;
            }
        }
        *bytes = total;
        return RAY_OK;
    }
    size_t width = raw->type == RAY_SYM ? sizeof(int64_t) : ray_elem_size(raw->type);
    if (!width || rows > SIZE_MAX / width) return RAY_ERR_LIMIT;
    *bytes = (size_t)rows * width;
    return RAY_OK;
}

static ray_t* scan_raw_materialize(ray_t* raw, uint64_t start, uint64_t rows) {
    if (rows > INT64_MAX) return ray_error("range", "raw block range exceeds vector length");
    if (raw->type == RAY_STR) {
        ray_t* out = ray_vec_new(RAY_STR, (int64_t)rows);
        if (!out || RAY_IS_ERR(out)) return out;
        for (uint64_t i = 0; i < rows; i++) {
            size_t len = 0;
            const char* str = ray_str_vec_get(raw, (int64_t)(start + i), &len);
            if (!str && len) { ray_release(out); return ray_error("corrupt", "invalid raw string"); }
            ray_t* next = ray_str_vec_append(out, str, len);
            if (!next || RAY_IS_ERR(next)) { ray_release(out); return next; }
            out = next;
        }
        return out;
    }
    if (raw->type == RAY_SYM) {
        ray_t* out = ray_sym_vec_new(RAY_SYM_W64, (int64_t)rows);
        if (!out || RAY_IS_ERR(out)) return out;
        for (uint64_t i = 0; i < rows; i++)
            ray_write_sym(ray_data(out), (int64_t)i,
                (uint64_t)ray_read_sym(ray_data(raw), (int64_t)(start + i), RAY_SYM, raw->attrs),
                RAY_SYM, out->attrs);
        out->len = (int64_t)rows;
        out->attrs |= raw->attrs & RAY_ATTR_HAS_NULLS;
        ray_sym_vec_adopt_domain(out, raw);
        return out;
    }
    size_t width = ray_elem_size(raw->type);
    const uint8_t* data = (const uint8_t*)ray_data(raw) + (size_t)start * width;
    ray_t* out = ray_vec_from_raw(raw->type, data, (int64_t)rows);
    if (out && !RAY_IS_ERR(out)) out->attrs |= raw->attrs & RAY_ATTR_HAS_NULLS;
    return out;
}

void ray_block_scan_stats(const ray_block_scan_t* s, ray_block_scan_stats_t* out) {
    if (!out) return;
    if (s) *out = s->stats;
    else *out = (ray_block_scan_stats_t){0, 0};
}

static void scan_count_blocks(ray_block_scan_t* s, const ray_col_block_reader_t* r,
                              uint64_t start, uint64_t rows) {
    uint64_t first, end;
    if (!ray_col_block_range_blocks(r, start, rows, &first, &end))
        s->stats.blocks_decoded += end - first;
}

void ray_block_scan_cancel(ray_block_scan_t* s) {
    if (s) s->cancelled = true;
}

ray_err_t ray_block_scan_reserve(ray_block_scan_t* s, size_t bytes) {
    if (!s || s->failed) return RAY_ERR_DOMAIN;
    if (s->options.work_limit && bytes > s->options.work_limit) return RAY_ERR_LIMIT;
    s->reserved_bytes = bytes;
    return RAY_OK;
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

static bool scan_work_exceeds(const ray_block_scan_t* s, size_t payload, size_t scratch) {
    if (!s->options.work_limit) return false;
    if (s->reserved_bytes > s->options.work_limit) return true;
    size_t remaining = s->options.work_limit - s->reserved_bytes;
    return payload > remaining || scratch > remaining - payload;
}

static void scan_free_arenas(ray_block_scan_t* s, const size_t* indices, size_t count) {
    for (size_t i = 0; i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        free(column->arena);
        column->arena = NULL;
        column->arena_bytes = 0;
    }
}

/* STR columns decode their whole range once into an arena when every arena
 * of the batch fits the scratch limit (and the work limit with the payload);
 * otherwise each STR block is decoded twice through one block of scratch:
 * once to measure, once to fill. Either way the exact batch size is known
 * before any output column is allocated. */
/* Rows of col whose mask byte is set, as a fresh owned vector. SYM keeps its
 * dictionary domain and width, STR copies its strings into a new pool. */
static ray_t* scan_compact(ray_t* src, const uint8_t* mask, int64_t kept) {
    int64_t rows = src->len;
    ray_t* v;
    if (src->type == RAY_STR) {
        v = ray_vec_new(RAY_STR, kept);
        for (int64_t i = 0; v && !RAY_IS_ERR(v) && i < rows; i++) {
            if (!mask[i]) continue;
            size_t len;
            const char* str = ray_str_vec_get(src, i, &len);
            v = ray_str_vec_append(v, str, len);
        }
    } else {
        /* Fixed widths compact in place, within the column's own payload:
         * every materialized column is a fresh copy this scan owns. */
        size_t width = ray_sym_elem_size(src->type, src->attrs);
        uint8_t* data = ray_data(src);
        int64_t to = 0;
        for (int64_t i = 0; i < rows; i++)
            if (mask[i]) {
                if (to != i) memmove(data + (size_t)to * width, data + (size_t)i * width, width);
                to++;
            }
        src->len = kept;
        ray_retain(src);
        return src;
    }
    if (!v) return ray_error("oom", NULL);
    if (!RAY_IS_ERR(v)) v->attrs |= src->attrs & RAY_ATTR_HAS_NULLS;
    return v;
}

/* mask (optional, one byte per row) keeps only marked rows. String columns
 * decoded into an arena materialize just those; others are compacted. */
static ray_t* scan_materialize(ray_block_scan_t* s, const size_t* indices,
                              size_t count, uint64_t start, uint64_t rows,
                              const uint8_t* mask) {
    int64_t kept = (int64_t)rows;
    if (mask) { kept = 0; for (uint64_t i = 0; i < rows; i++) kept += mask[i] != 0; }
    size_t total = 0, scratch_bytes = 0, scratch_fixed = 0, arena_total = 0;
    bool strings = false, arenas = false;
    void* scratch = NULL;
    ray_err_t err = RAY_OK;
    for (size_t i = 0; !err && i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        size_t bytes, needed = 0;
        if (column->raw) err = scan_raw_range_size(column->raw, start, rows, &bytes);
        else {
            const ray_col_block_reader_t* r = ray_col_block_file_reader(column->file);
            err = ray_col_block_range_size(r, start, rows, &bytes, &needed);
            if (!err && r->type == RAY_STR) {
                strings = true;
                err = ray_col_block_range_decoded(r, start, rows, &column->arena_bytes);
                if (!err) arena_total = column->arena_bytes > SIZE_MAX - arena_total ?
                    SIZE_MAX : arena_total + column->arena_bytes;
            }
        }
        if (!err && (bytes > s->options.payload_limit - total || needed > s->options.scratch_limit))
            err = RAY_ERR_LIMIT;
        if (err) break;
        column->payload_bytes = bytes;
        total += bytes;
        if (scratch_bytes < needed) scratch_bytes = needed;
        if (!column->arena_bytes && scratch_fixed < needed) scratch_fixed = needed;
    }
    if (!err && scan_work_exceeds(s, total, scratch_bytes)) err = RAY_ERR_LIMIT;
    /* A pool never exceeds its decoded bytes: admit arenas only if the batch
     * fits with that bound, so tight limits keep the exact two-pass path. */
    /* Arenas and the fixed scratch are live together: both within scratch. */
    if (!err && strings && arena_total <= SIZE_MAX - scratch_fixed &&
        scratch_fixed + arena_total <= s->options.scratch_limit && arena_total <= SIZE_MAX - total &&
        total + arena_total <= s->options.payload_limit &&
        !scan_work_exceeds(s, total + arena_total, scratch_fixed + arena_total)) {
        arenas = true;
        for (size_t i = 0; arenas && i < count; i++) {
            scan_column_t* column = &s->columns[indices ? indices[i] : i];
            if (!column->raw && ray_col_block_file_reader(column->file)->type == RAY_STR) {
                column->arena = malloc(column->arena_bytes ? column->arena_bytes : 1);
                if (!column->arena) arenas = false;
            }
        }
        if (!arenas) scan_free_arenas(s, indices, count);
        else scratch_bytes = scratch_fixed;
    }
    if (!err && scratch_bytes) {
        scratch = malloc(scratch_bytes);
        if (!scratch) err = RAY_ERR_OOM;
    }
    /* STR sizes depend on decoded lengths. Check the entire batch's exact
     * descriptor/pool sum before allocating any output column. */
    total = 0;
    for (size_t i = 0; !err && i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        size_t bytes = column->payload_bytes;
        const ray_col_block_reader_t* r = column->raw ? NULL :
            ray_col_block_file_reader(column->file);
        if (r && r->type == RAY_STR) {
            scan_count_blocks(s, r, start, rows);
            if (column->arena) {
                err = ray_col_block_decode_arena(r, start, rows, column->arena, column->arena_bytes);
                if (!err) err = ray_col_block_payload_size_arena(r, start, rows, column->arena, mask, &bytes);
            } else err = ray_col_block_payload_size(r, start, rows, scratch, scratch_bytes, &bytes);
        }
        if (!err && bytes > s->options.payload_limit - total) err = RAY_ERR_LIMIT;
        if (err) break;
        column->payload_bytes = bytes;
        total += bytes;
    }
    /* A masked string column without an arena is compacted into a copy. */
    size_t compact = 0;
    for (size_t i = 0; !err && mask && i < count; i++) {
        const scan_column_t* column = &s->columns[indices ? indices[i] : i];
        bool str = column->raw ? column->raw->type == RAY_STR :
            ray_col_block_file_reader(column->file)->type == RAY_STR;
        if (str && !column->arena && compact < column->payload_bytes) compact = column->payload_bytes;
    }
    if (!err && (compact > SIZE_MAX - total ||
                 scan_work_exceeds(s, total + compact, scratch_bytes + (arenas ? arena_total : 0))))
        err = RAY_ERR_LIMIT;
    if (err) {
        free(scratch);
        scan_free_arenas(s, indices, count);
        return scan_error(s, err);
    }
    ray_t* batch = ray_table_new((int64_t)count);
    if (!batch) batch = ray_error("oom", "block scan batch allocation failed");
    for (size_t i = 0; !RAY_IS_ERR(batch) && i < count; i++) {
        scan_column_t* column = &s->columns[indices ? indices[i] : i];
        const ray_col_block_reader_t* r = column->raw ? NULL :
            ray_col_block_file_reader(column->file);
        if (!r) s->stats.raw_rows += rows;
        else if (!column->arena) scan_count_blocks(s, r, start, rows);
        ray_t* col = column->raw ? scan_raw_materialize(column->raw, start, rows) :
            r->type == RAY_STR ?
            (column->arena ?
                ray_col_block_materialize_str_arena(r, start, rows, mask, column->payload_bytes, column->arena) :
                ray_col_block_materialize_str(r, start, rows, column->payload_bytes, scratch, scratch_bytes)) :
            ray_col_block_materialize_dom(r, start, rows,
                s->options.payload_limit, scratch, scratch_bytes, s->domain);
        if (!RAY_IS_ERR(col) && mask && col->len != kept) {
            ray_t* compact = scan_compact(col, mask, kept);
            ray_release(col);
            col = compact;
        }
        if (RAY_IS_ERR(col)) { ray_release(batch); batch = col; break; }
        batch = ray_table_add_col(batch, column->name, col);
        ray_release(col);
        if (!batch) { batch = ray_error("oom", "block scan batch allocation failed"); break; }
    }
    free(scratch);
    scan_free_arenas(s, indices, count);
    if (RAY_IS_ERR(batch)) s->failed = true;
    return batch;
}

ray_t* ray_block_scan_read_masked(ray_block_scan_t* s, const size_t* indices, size_t count,
                                  uint64_t start, uint64_t rows, const uint8_t* mask) {
    if (!s) return ray_error("domain", "null block scan");
    if (s->failed) return ray_error("domain", "block scan is terminal; close it");
    if (s->cancelled) return scan_error(s, RAY_ERR_CANCEL);
    if (!mask) return scan_error(s, RAY_ERR_DOMAIN);
    ray_err_t err = scan_validate_columns(s, indices, count);
    if (err) return scan_error(s, err);
    if (start < s->options.start || start > s->end || rows > s->end - start ||
        rows > s->options.batch_rows) return scan_error(s, RAY_ERR_RANGE);
    return scan_materialize(s, indices, count, start, rows, mask);
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
    return scan_materialize(s, indices, count, start, rows, NULL);
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
    ray_t* batch = scan_materialize(s, indices, count, s->cursor, rows, NULL);
    if (!RAY_IS_ERR(batch)) {
        if (row_start) *row_start = s->cursor;
        s->cursor += rows;
    }
    return batch;
}

ray_t* ray_block_scan_next(ray_block_scan_t* s) {
    return ray_block_scan_next_columns(s, NULL, s ? s->count : 0, NULL);
}
