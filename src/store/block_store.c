/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#if !defined(_WIN32) && !defined(__APPLE__)
#define _DEFAULT_SOURCE /* realpath */
#endif
#include "block_store.h"
#include "block_scan.h"
#include "block_sym.h"
#include "col_block.h"
#include "col.h"
#include "splay.h"
#include "core/types.h"
#include "mem/heap.h"
#include "ops/hash.h"
#include "table/sym.h"
#include "table/domain.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

static ray_err_t sync_file(const char* path) {
    ray_fd_t fd = ray_file_open(path, RAY_OPEN_READ | RAY_OPEN_WRITE);
    if (fd == RAY_FD_INVALID) return RAY_ERR_IO;
    ray_err_t err = ray_file_sync(fd);
    ray_file_close(fd);
    return err;
}

/* mkdir_p may have created several ancestors. Persist their directory entries
 * before publishing a durable generation, from the table root outward up to
 * the shallowest directory begin created; directories that existed before are
 * not touched, so unreadable ancestors do not fail the write. */
static ray_err_t sync_parents(const char* root, const char* created) {
    char path[1024];
    memcpy(path, root, strlen(root) + 1); /* write.root is bounded by this size. */
    for (;;) {
        ray_err_t err = ray_file_sync_dir(path); /* syncs path's parent */
        if (err) return err;
        if (!*created || !strcmp(path, created)) return RAY_OK;
        char* slash = strrchr(path, '/');
        if (!slash || slash == path) return RAY_OK;
        *slash = '\0';
    }
}

/* ---- Streaming table writer --------------------------------------------- */

typedef struct {
    FILE* file;
    ray_col_block_writer_t writer;
    int64_t name;
    int8_t type;
    unsigned width;
    uint8_t* carry;       /* the partial last block, so chunking never shows */
    size_t used;
    uint64_t carry_rows;
    int64_t last_source;  /* SYM: last source position and its dictionary slot */
    ray_sym_domain_t* last_domain;
    uint64_t last_position;
    bool begun;
} store_column_t;

struct ray_block_table_writer_s {
    ray_splay_write_t write;
    ray_block_store_options_t options;
    ray_t* schema;
    store_column_t* columns;
    int64_t count;
    ray_sym_domain_t* domain;
    size_t sym_file_bytes;
    uint64_t rows;
    bool staged, failed;
};

static ray_err_t store_schema_options(const ray_block_store_options_t* o) {
    if (!o || o->codec > 1) return RAY_ERR_DOMAIN;
    if (!o->block_bytes || o->block_bytes > RAY_COL_BLOCK_MAX) return RAY_ERR_RANGE;
    return RAY_OK;
}

/* Names, types, attrs and options: everything a schema can be judged on. */
static ray_err_t store_schema(ray_t* table, const ray_block_store_options_t* o,
                              ray_t** schema_out) {
    if (!o || o->codec > 1) return RAY_ERR_DOMAIN;
    if (!o->block_bytes || o->block_bytes > RAY_COL_BLOCK_MAX) return RAY_ERR_RANGE;
    if (!table || RAY_IS_ERR(table) || table->type != RAY_TABLE) return RAY_ERR_TYPE;
    int64_t count = ray_table_ncols(table);
    if (count <= 0 || count > RAY_BLOCK_SCAN_MAX_COLUMNS) return RAY_ERR_LIMIT;
    const uint16_t endian = 1;
    if (*(const uint8_t*)&endian != 1) return RAY_ERR_NYI;
    const char* names[RAY_BLOCK_SCAN_MAX_COLUMNS];
    uint32_t lengths[RAY_BLOCK_SCAN_MAX_COLUMNS];
    for (int64_t i = 0; i < count; i++) {
        ray_t* col = ray_table_get_col_idx(table, i);
        if (!col || RAY_IS_ERR(col) || col->type < RAY_BOOL || col->type > RAY_STR)
            return RAY_ERR_TYPE;
        unsigned width = col->type == RAY_STR ? 4 : col->type == RAY_SYM ? 8 : ray_elem_size(col->type);
        if (o->block_bytes < width || (col->type != RAY_STR && o->block_bytes % width))
            return RAY_ERR_RANGE;
        ray_t* name = ray_sym_str(ray_table_col_name(table, i));
        if (!name || RAY_IS_ERR(name)) return RAY_ERR_DOMAIN;
        size_t len = ray_str_len(name);
        const char* p = ray_str_ptr(name);
        if (!len || len > 255 || p[0] == '.' || memchr(p, '/', len) ||
            memchr(p, '\\', len) || memchr(p, '\0', len)) return RAY_ERR_DOMAIN;
        names[i] = p; lengths[i] = (uint32_t)len;
        for (int64_t j = 0; j < i; j++)
            if (ray_table_col_name(table, j) == ray_table_col_name(table, i)) return RAY_ERR_DOMAIN;
    }
    if (!schema_out) return RAY_OK;
    ray_t* schema = ray_str_vec_from_parts(names, lengths, NULL, count);
    if (!schema || RAY_IS_ERR(schema)) { ray_error_free(schema); return RAY_ERR_OOM; }
    *schema_out = schema;
    return RAY_OK;
}

static ray_err_t store_chunk_columns(const ray_block_table_writer_t* w, ray_t* chunk,
                                     int64_t* rows) {
    if (!chunk || RAY_IS_ERR(chunk) || chunk->type != RAY_TABLE) return RAY_ERR_TYPE;
    if (ray_table_ncols(chunk) != w->count) return RAY_ERR_SCHEMA;
    *rows = ray_table_nrows(chunk);
    if (*rows < 0) return RAY_ERR_RANGE;
    for (int64_t i = 0; i < w->count; i++) {
        ray_t* col = ray_table_get_col_idx(chunk, i);
        if (ray_table_col_name(chunk, i) != w->columns[i].name || !col || RAY_IS_ERR(col) ||
            col->type != w->columns[i].type) return RAY_ERR_SCHEMA;
        uint8_t allowed = RAY_ATTR_HAS_NULLS | (col->type == RAY_SYM ? RAY_SYM_W_MASK : 0);
        if (col->attrs & ~allowed) return RAY_ERR_NYI;
        if (col->len != *rows) return RAY_ERR_LENGTH;
    }
    return RAY_OK;
}

static ray_err_t store_flush(store_column_t* c) {
    if (!c->carry_rows) return RAY_OK;
    ray_err_t err = c->type == RAY_STR ?
        ray_col_block_append_str(&c->writer, c->carry, c->used, c->carry_rows) :
        ray_col_block_append(&c->writer, c->carry, c->carry_rows);
    c->used = 0; c->carry_rows = 0;
    return err;
}

static ray_err_t store_fixed(store_column_t* c, const uint8_t* data, int64_t rows,
                             uint32_t block_bytes) {
    for (int64_t i = 0; i < rows;) {
        int64_t room = (int64_t)((block_bytes - c->used) / c->width);
        int64_t n = rows - i < room ? rows - i : room;
        memcpy(c->carry + c->used, data + (size_t)i * c->width, (size_t)n * c->width);
        c->used += (size_t)n * c->width; c->carry_rows += (uint64_t)n; i += n;
        if (c->used == block_bytes) {
            ray_err_t err = store_flush(c);
            if (err) return err;
        }
    }
    return RAY_OK;
}

/* Only the vocabulary grows with the number of distinct symbols. */
static ray_err_t store_symbol(ray_block_table_writer_t* w, store_column_t* c, ray_t* col,
                              int64_t row, uint64_t* position) {
    int64_t source = ray_read_sym(ray_data(col), row, RAY_SYM, col->attrs);
    ray_sym_domain_t* domain = ray_sym_vec_domain(col);
    if (source == c->last_source && domain == c->last_domain) {
        *position = c->last_position;
        return RAY_OK;
    }
    ray_t* s = ray_sym_vec_cell(col, row);
    if (!s) return RAY_ERR_CORRUPT;
    size_t len = ray_str_len(s);
    if (len > RAY_BLOCK_SYM_MAX_BYTES - 20) return RAY_ERR_LIMIT;
    int64_t pos = ray_sym_domain_find(w->domain, ray_str_ptr(s), len);
    if (pos < 0) {
        if (len + 4 > RAY_BLOCK_SYM_MAX_BYTES - w->sym_file_bytes) return RAY_ERR_LIMIT;
        pos = ray_sym_domain_intern(w->domain, ray_str_ptr(s), len);
        if (pos < 0) return RAY_ERR_OOM;
        w->sym_file_bytes += len + 4;
    }
    c->last_source = source; c->last_domain = domain; c->last_position = (uint64_t)pos;
    *position = (uint64_t)pos;
    return RAY_OK;
}

static ray_err_t store_symbols_chunk(ray_block_table_writer_t* w, store_column_t* c,
                                     ray_t* col, uint32_t block_bytes) {
    for (int64_t i = 0; i < col->len; i++) {
        uint64_t position;
        ray_err_t err = store_symbol(w, c, col, i, &position);
        if (err) return err;
        memcpy(c->carry + c->used, &position, 8);
        c->used += 8; c->carry_rows++;
        if (c->used == block_bytes && (err = store_flush(c))) return err;
    }
    return RAY_OK;
}

static ray_err_t store_strings(store_column_t* c, ray_t* col, uint32_t block_bytes) {
    for (int64_t i = 0; i < col->len; i++) {
        size_t len;
        const char* s = ray_str_vec_get(col, i, &len);
        if (!s && len) return RAY_ERR_CORRUPT;
        if (len > block_bytes - 4) return RAY_ERR_RANGE;
        if (len + 4 > block_bytes - c->used) {
            ray_err_t err = store_flush(c);
            if (err) return err;
        }
        for (unsigned j = 0; j < 4; j++) c->carry[c->used + j] = (uint8_t)(len >> (j * 8));
        if (len) memcpy(c->carry + c->used + 4, s, len);
        c->used += len + 4; c->carry_rows++;
    }
    return RAY_OK;
}

void ray_block_table_abort(ray_block_table_writer_t** writer) {
    if (!writer || !*writer) return;
    ray_block_table_writer_t* w = *writer;
    for (int64_t i = 0; w->columns && i < w->count; i++) {
        store_column_t* c = &w->columns[i];
        if (c->begun) ray_col_block_abort(&c->writer);
        if (c->file) fclose(c->file);
        free(c->carry);
    }
    free(w->columns);
    ray_release(w->schema);
    if (w->domain) ray_sym_domain_release(w->domain);
    if (w->staged) (void)ray_splay_write_finish(&w->write, RAY_ERR_CANCEL, false);
    free(w);
    *writer = NULL;
}

ray_err_t ray_block_table_begin(const char* root, ray_t* schema_table,
                                const ray_block_store_options_t* options,
                                ray_block_table_writer_t** out) {
    if (!root || !*root || !out || *out) return RAY_ERR_DOMAIN;
    ray_t* schema = NULL;
    ray_err_t err = store_schema(schema_table, options, &schema);
    if (err) return err;
    ray_block_table_writer_t* w = calloc(1, sizeof(*w));
    if (!w) { ray_release(schema); return RAY_ERR_OOM; }
    w->schema = schema;
    w->options = *options;
    w->count = ray_table_ncols(schema_table);
    w->sym_file_bytes = 16; /* STRL header and the reserved empty entry */
    w->columns = calloc((size_t)w->count, sizeof(*w->columns));
    if (!w->columns) err = RAY_ERR_OOM;
    if (!err) err = ray_splay_write_begin_staged(root, &w->write);
    if (!err) w->staged = true;
    /* A consistency token shared by these columns, not a globally unique ID.
     * Directory ownership/lease provides the actual snapshot identity. */
    uint64_t token = err ? 0 : ray_hash_bytes(w->write.generation, strlen(w->write.generation));
    for (int64_t i = 0; !err && i < w->count; i++) {
        store_column_t* c = &w->columns[i];
        ray_t* col = ray_table_get_col_idx(schema_table, i);
        c->name = ray_table_col_name(schema_table, i);
        c->type = col->type;
        c->width = col->type == RAY_STR ? 1 : col->type == RAY_SYM ? 8 : ray_elem_size(col->type);
        c->last_source = -1;
        if (col->type == RAY_SYM && !w->domain) {
            char path[1100];
            snprintf(path, sizeof(path), "%s/.sym", w->write.dir);
            w->domain = ray_sym_domain_open_or_create(path);
            if (!w->domain) { err = RAY_ERR_IO; break; }
            if (ray_sym_domain_intern(w->domain, "", 0) != 0) { err = RAY_ERR_OOM; break; }
        }
        size_t name_len;
        const char* name = ray_str_vec_get(schema, i, &name_len);
        char path[1300];
        int n = snprintf(path, sizeof(path), "%s/%.*s", w->write.dir, (int)name_len, name);
        if (n < 0 || (size_t)n >= sizeof(path)) { err = RAY_ERR_RANGE; break; }
        c->carry = malloc(options->block_bytes);
        c->file = c->carry ? fopen(path, "w+b") : NULL;
        if (!c->carry) { err = RAY_ERR_OOM; break; }
        if (!c->file) { err = RAY_ERR_IO; break; }
        /* The dictionary still grows: seal its identity at finish. */
        err = col->type == RAY_SYM ?
            ray_col_block_begin_sym(&c->writer, c->file, options->block_bytes, options->codec,
                                    token, INT64_MAX, 0) :
            ray_col_block_begin(&c->writer, c->file, (uint8_t)col->type, options->block_bytes,
                                options->codec, token);
        c->begun = !err;
    }
    if (err) { ray_block_table_abort(&w); return err; }
    *out = w;
    return RAY_OK;
}

ray_err_t ray_block_table_append(ray_block_table_writer_t* w, ray_t* chunk) {
    if (!w || w->failed) return RAY_ERR_DOMAIN;
    int64_t rows;
    ray_err_t err = store_chunk_columns(w, chunk, &rows);
    if (!err && (uint64_t)rows > (uint64_t)INT64_MAX - w->rows) err = RAY_ERR_RANGE;
    for (int64_t i = 0; !err && i < w->count; i++) {
        store_column_t* c = &w->columns[i];
        ray_t* col = ray_table_get_col_idx(chunk, i);
        if (c->type == RAY_STR) err = store_strings(c, col, w->options.block_bytes);
        else if (c->type == RAY_SYM) err = store_symbols_chunk(w, c, col, w->options.block_bytes);
        else err = store_fixed(c, ray_data(col), rows, w->options.block_bytes);
    }
    if (err) { w->failed = true; return err; }
    w->rows += (uint64_t)rows;
    return RAY_OK;
}

ray_err_t ray_block_table_finish(ray_block_table_writer_t** writer) {
    if (!writer || !*writer) return RAY_ERR_DOMAIN;
    ray_block_table_writer_t* w = *writer;
    if (w->failed) { ray_block_table_abort(writer); return RAY_ERR_DOMAIN; }
    bool durable = w->options.durable;
    ray_err_t err = RAY_OK;
    uint64_t sym_count = 0;
    uint32_t sym_crc = 0;
    if (w->domain) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/.sym", w->write.dir);
        err = ray_sym_domain_flush(w->domain, durable);
        if (!err) err = ray_block_sym_checksum(path, &sym_count, &sym_crc);
        if (!err && sym_count != (uint64_t)ray_sym_domain_count(w->domain)) err = RAY_ERR_CORRUPT;
    }
    for (int64_t i = 0; !err && i < w->count; i++) {
        store_column_t* c = &w->columns[i];
        err = store_flush(c);
        if (!err && c->type == RAY_SYM) err = ray_col_block_set_sym(&c->writer, sym_count, sym_crc);
        c->begun = false;
        if (!err) err = ray_col_block_finish(&c->writer);
        else ray_col_block_abort(&c->writer);
        if (fclose(c->file) && !err) err = RAY_ERR_IO;
        c->file = NULL;
        if (!err && durable) {
            size_t name_len;
            const char* name = ray_str_vec_get(w->schema, i, &name_len);
            char path[1300];
            snprintf(path, sizeof(path), "%s/%.*s", w->write.dir, (int)name_len, name);
            err = sync_file(path);
        }
    }
    if (!err) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/.d", w->write.dir);
        err = durable ? ray_col_save(w->schema, path) : ray_col_save_bulk(w->schema, path);
        if (!err && durable) {
            snprintf(path, sizeof(path), "%s/.lease", w->write.dir);
            err = sync_file(path);
            if (!err) err = sync_parents(w->write.root, w->write.created);
        }
    }
    w->staged = false;
    err = ray_splay_write_finish(&w->write, err, durable);
    ray_block_table_abort(writer);
    return err;
}

/* Whole-table validation before any filesystem mutation: the schema plus
 * every row that a streamed append would only reject after staging. */
static ray_err_t store_preflight(ray_t* table, const ray_block_store_options_t* o) {
    ray_err_t err = store_schema(table, o, NULL);
    if (err) return err;
    int64_t rows = ray_table_nrows(table);
    if (rows < 0) return RAY_ERR_RANGE;
    for (int64_t i = 0; i < ray_table_ncols(table); i++) {
        ray_t* col = ray_table_get_col_idx(table, i);
        uint8_t allowed = RAY_ATTR_HAS_NULLS | (col->type == RAY_SYM ? RAY_SYM_W_MASK : 0);
        if (col->attrs & ~allowed) return RAY_ERR_NYI;
        if (col->len != rows) return RAY_ERR_LENGTH;
        unsigned width = col->type == RAY_STR ? 16 : col->type == RAY_SYM ? 8 : ray_elem_size(col->type);
        if (col->type == RAY_STR) {
            if (o->block_bytes < 4) return RAY_ERR_RANGE;
            for (int64_t j = 0; j < rows; j++) {
                size_t len;
                if (!ray_str_vec_get(col, j, &len)) return RAY_ERR_CORRUPT;
                if (len > o->block_bytes - 4) return RAY_ERR_RANGE;
            }
        }
        if ((uint64_t)rows > SIZE_MAX / width) return RAY_ERR_LIMIT;
        if (col->type == RAY_SYM) {
            uint64_t symbols = (uint64_t)ray_sym_domain_count(ray_sym_vec_domain(col));
            for (int64_t j = 0; j < rows; j++)
                if ((uint64_t)ray_read_sym(ray_data(col), j, RAY_SYM, col->attrs) >= symbols)
                    return RAY_ERR_CORRUPT;
        }
    }
    return RAY_OK;
}

ray_err_t ray_block_store_save(ray_t* table, const char* root,
                               const ray_block_store_options_t* options) {
    if (!root || !*root) return RAY_ERR_DOMAIN;
    ray_err_t err = store_preflight(table, options);
    if (err) return err;
    ray_block_table_writer_t* w = NULL;
    err = ray_block_table_begin(root, table, options, &w);
    if (!err) err = ray_block_table_append(w, table);
    if (!err) return ray_block_table_finish(&w);
    ray_block_table_abort(&w);
    return err;
}

/* Canonical form of a path that may not exist yet: the resolved path of its
 * deepest existing ancestor plus the remaining components. */
static bool convert_resolve(const char* path, char* out, size_t size) {
#ifdef _WIN32
    return _fullpath(out, path, size) != NULL;
#else
    return size >= PATH_MAX && realpath(path, out) != NULL;
#endif
}

/* Missing components are appended unresolved. mkdir_p resolves ".." in them,
 * so a ".." there could escape the comparison: refuse it. */
static ray_err_t convert_canonical(const char* path, char* out, size_t size) {
    char buf[PATH_MAX], tail[PATH_MAX] = "", resolved[PATH_MAX];
    size_t len = strlen(path);
    if (!len || len >= sizeof(buf)) return RAY_ERR_RANGE;
    memcpy(buf, path, len + 1);
    while (len > 1 && buf[len - 1] == '/') buf[--len] = '\0';
    while (!convert_resolve(buf, resolved, sizeof(resolved))) {
        if (!strcmp(buf, ".") || !strcmp(buf, "/")) return RAY_ERR_IO;
        char* slash = strrchr(buf, '/');
        const char* part = slash ? slash + 1 : buf;
        if (!strcmp(part, "..")) return RAY_ERR_DOMAIN;
        char next[PATH_MAX];
        int n = *part && strcmp(part, ".") ?
            snprintf(next, sizeof(next), "/%s%s", part, tail) :
            snprintf(next, sizeof(next), "%s", tail);
        if (n < 0 || (size_t)n >= sizeof(next)) return RAY_ERR_RANGE;
        memcpy(tail, next, (size_t)n + 1);
        if (!slash) strcpy(buf, ".");
        else if (slash == buf) strcpy(buf, "/");
        else *slash = '\0';
    }
    int n = snprintf(out, size, "%s%s", strcmp(resolved, "/") ? resolved : "", tail);
    return n > 0 && (size_t)n < size ? RAY_OK : RAY_ERR_RANGE;
}

static bool convert_nested(const char* a, const char* b) {
    size_t n = strlen(a);
    return !n || (!strncmp(a, b, n) && (b[n] == '\0' || b[n] == '/'));
}

ray_err_t ray_block_store_paths_overlap(const char* a, const char* b, bool* overlap) {
    if (!a || !*a || !b || !*b || !overlap) return RAY_ERR_DOMAIN;
    char x[PATH_MAX + 256], y[PATH_MAX + 256];
    ray_err_t err = convert_canonical(a, x, sizeof(x));
    if (!err) err = convert_canonical(b, y, sizeof(y));
    if (err) return err;
    *overlap = convert_nested(x, y) || convert_nested(y, x);
    return RAY_OK;
}

ray_err_t ray_block_store_convert(const char* src, const ray_block_scan_source_t* source,
                                  const char* dest, const ray_block_store_options_t* options,
                                  uint64_t* rows_out) {
    if (!src || !*src || !dest || !*dest || !options) return RAY_ERR_DOMAIN;
    char a[PATH_MAX + 256], b[PATH_MAX + 256];
    ray_err_t err = convert_canonical(src, a, sizeof(a));
    if (!err) err = convert_canonical(dest, b, sizeof(b));
    if (err) return err;
    /* Publishing over a flat table retires its schema: never write into,
     * above or below the source. */
    if (convert_nested(a, b) || convert_nested(b, a)) return RAY_ERR_DOMAIN;
    err = store_schema_options(options);
    if (err) return err;
    ray_block_scan_options_t read = {0, UINT64_MAX, RAY_BLOCK_CONVERT_BATCH_ROWS,
                                     RAY_BLOCK_CONVERT_BATCH_BYTES, RAY_BLOCK_CONVERT_BATCH_BYTES, 0};
    ray_block_scan_t* scan = NULL;
    err = ray_block_scan_open_source(src, source, NULL, 0, &read, &scan);
    if (err) return err;
    /* The block format has no place for SORTED: refuse rather than drop it. */
    ray_t* names = ray_block_scan_names(scan);
    for (int64_t i = 0; !err && names && i < names->len; i++)
        if (ray_block_scan_column_attrs(scan, (size_t)i) & RAY_ATTR_SORTED) err = RAY_ERR_NYI;
    if (err) { ray_block_scan_close(&scan); return err; }
    ray_block_table_writer_t* w = NULL;
    uint64_t rows = 0;
    for (;;) {
        ray_t* batch = ray_block_scan_next(scan);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { err = ray_err_from_obj(batch); ray_error_free(batch); break; }
        err = w ? RAY_OK : ray_block_table_begin(dest, batch, options, &w);
        if (!err) err = ray_block_table_append(w, batch);
        rows += (uint64_t)ray_table_nrows(batch);
        ray_release(batch);
        if (err) break;
    }
    if (!err && !w) {
        ray_t* schema = ray_block_scan_schema(scan);
        if (!schema || RAY_IS_ERR(schema)) {
            err = schema ? ray_err_from_obj(schema) : RAY_ERR_OOM;
            if (schema) ray_error_free(schema);
        } else {
            err = ray_block_table_begin(dest, schema, options, &w);
            ray_release(schema);
        }
    }
    ray_block_scan_close(&scan);
    if (err) { ray_block_table_abort(&w); return err; }
    err = ray_block_table_finish(&w);
    if (!err && rows_out) *rows_out = rows;
    return err;
}
