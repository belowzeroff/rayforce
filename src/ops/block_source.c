/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_source.h"
#include "block_query.h"
#include "ops.h"
#include "agg_registry.h"
#include "lang/eval.h"
#include "lang/env.h"
#include "lang/internal.h"
#include "lang/format.h"
#include "table/sym.h"
#include "vec/vec.h"
#include "store/block_store.h"
#include "store/part.h"
#include "core/pool.h"
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>

#define BLOCK_SOURCE_BATCH_ROWS 4096u   /* aggregate chunk: bounds group-id workspace */
/* Reads span many blocks so whole blocks decode in parallel and per-batch
 * barriers stay rare: the scan sizes batches from the payload limit. */
#define BLOCK_SOURCE_READ_ROWS RAY_BLOCK_SCAN_AUTO_ROWS
#define BLOCK_SOURCE_PAYLOAD_BYTES (64u * 1024 * 1024)

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

static ray_err_t scan_options(ray_t* opts, ray_block_scan_options_t* out, bool* immutable) {
    *out = (ray_block_scan_options_t){0, UINT64_MAX, BLOCK_SOURCE_READ_ROWS,
                                    BLOCK_SOURCE_PAYLOAD_BYTES, 8u * 1024 * 1024, 0};
    *immutable = false;
    if (!opts) return RAY_OK;
    /* The parser represents {} using empty LIST keys and values. */
    ray_t* empty_keys = ray_dict_keys(opts); ray_t* empty_vals = ray_dict_vals(opts);
    if (empty_keys && empty_vals && empty_keys->type == RAY_LIST && empty_vals->type == RAY_LIST &&
        !empty_keys->len && !empty_vals->len) return RAY_OK;
    if (!dictionary(opts)) return RAY_ERR_TYPE;
    ray_t* keys = ray_dict_keys(opts); ray_t* vals = ray_dict_vals(opts);
    if (keys->len > 5) return RAY_ERR_DOMAIN;
    unsigned seen = 0;
    for (int64_t i = 0; i < keys->len; i++) {
        const char* key = string(ray_sym_vec_cell(keys, i));
        unsigned bit = !key ? 0 : !strcmp(key, "batch-rows") ? 1 :
            !strcmp(key, "payload-bytes") ? 2 : !strcmp(key, "scratch-bytes") ? 4 :
            !strcmp(key, "work-bytes") ? 8 : !strcmp(key, "immutable") ? 16 : 0;
        if (!bit || (seen & bit)) return RAY_ERR_DOMAIN;
        seen |= bit;
        ray_t* value = ray_list_get(vals, i);
        if (bit == 16) {
            if (!value || value->type != -RAY_BOOL) return RAY_ERR_TYPE;
            *immutable = value->b8;
            continue;
        }
        if (!value || value->type != -RAY_I64) return RAY_ERR_TYPE;
        if (value->i64 < 0 || (uint64_t)value->i64 > SIZE_MAX) return RAY_ERR_RANGE;
        if (bit == 1) {
            if (!value->i64 || (uint64_t)value->i64 > UINT32_MAX) return RAY_ERR_RANGE;
            out->batch_rows = (uint32_t)value->i64;
        } else if (bit == 2) out->payload_limit = (size_t)value->i64;
        else if (bit == 4) out->scratch_limit = (size_t)value->i64;
        else {
            if (!value->i64) return RAY_ERR_RANGE;
            out->work_limit = (size_t)value->i64;
        }
    }
    return RAY_OK;
}

static const char* table_name(ray_t* s) {
    if (!s || s->type != -RAY_SYM) return NULL;
    const char* p = string(ray_sym_str(s->i64));
    return p && *p != '.' && !strchr(p, '/') && !strchr(p, '\\') && strlen(p) <= 255 ? p : NULL;
}

static ray_t* descriptor(const char* const* fields, ray_t** args, int64_t n) {
    int64_t names[4];
    for (int64_t i = 0; i < n; i++) {
        names[i] = ray_sym_intern(fields[i], strlen(fields[i]));
        if (names[i] < 0) return ray_error("oom", NULL);
    }
    ray_t* keys = ray_vec_from_raw(RAY_SYM, names, n);
    ray_t* vals = ray_list_new(n);
    if (!keys || RAY_IS_ERR(keys) || !vals || RAY_IS_ERR(vals)) {
        ray_release(keys); ray_release(vals); return ray_error("oom", NULL);
    }
    for (int64_t i = 0; i < n; i++) { ray_retain(args[i]); ((ray_t**)ray_data(vals))[i] = args[i]; }
    vals->len = n;
    return ray_dict_new(keys, vals);
}

ray_t* ray_block_source_fn(ray_t** args, int64_t n) {
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    if (n != 2 && n != 3) return ray_error("arity", ".db.block.scan: path result-bytes [options]");
    if (!string(args[0]) || args[1]->type != -RAY_I64 || args[1]->i64 <= 0 ||
        (uint64_t)args[1]->i64 > SIZE_MAX)
        return ray_error("domain", ".db.block.scan: nonempty path and positive I64 result limit required");
    ray_block_scan_options_t options;
    bool immutable;
    ray_err_t err = scan_options(n == 3 ? args[2] : NULL, &options, &immutable);
    if (err) return ray_error(ray_err_code_str(err), "invalid block scan options");
    static const char* const keys[] = {".hdb.block.source", ".hdb.block.limit", ".hdb.block.options"};
    return descriptor(keys, args, n);
}

/* (.db.block.parted root table result-bytes [options]) */
ray_t* ray_block_parted_fn(ray_t** args, int64_t n) {
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    if (n != 3 && n != 4) return ray_error("arity", ".db.block.parted: root table result-bytes [options]");
    if (!string(args[0]) || !table_name(args[1]) || args[2]->type != -RAY_I64 ||
        args[2]->i64 <= 0 || (uint64_t)args[2]->i64 > SIZE_MAX)
        return ray_error("domain", ".db.block.parted: nonempty root, table symbol and positive I64 result limit required");
    ray_block_scan_options_t options;
    bool immutable;
    ray_err_t err = scan_options(n == 4 ? args[3] : NULL, &options, &immutable);
    if (err) return ray_error(ray_err_code_str(err), "invalid block scan options");
    static const char* const keys[] = {".hdb.block.parted", ".hdb.block.table", ".hdb.block.limit",
                                       ".hdb.block.options"};
    return descriptor(keys, args, n);
}

/* A partition table <root>/<digits and dots>/<table> shares <root>/.sym. */
static const char* convert_default_sym(const char* src, char* buf, size_t size) {
    size_t len = strlen(src);
    while (len > 1 && src[len - 1] == '/') len--;
    size_t table = len;
    while (table > 0 && src[table - 1] != '/') table--;
    if (table < 2) return NULL;
    size_t part_end = table - 1, part = part_end;
    while (part > 0 && src[part - 1] != '/') part--;
    if (part == part_end || part < 1) return NULL;
    for (size_t i = part; i < part_end; i++)
        if (src[i] != '.' && (src[i] < '0' || src[i] > '9')) return NULL;
    int n = snprintf(buf, size, "%.*s/.sym", (int)(part - 1), src);
    struct stat st;
    return n > 0 && (size_t)n < size && stat(buf, &st) == 0 ? buf : NULL;
}

/* Read options {immutable: BOOL sym: STR} of a stored table; sym defaults to
 * the parted root's .sym for a partition path. NULL or an owned error. */
static ray_t* read_source(const char* who, const char* path, ray_t* opts,
                          ray_block_scan_source_t* source, char* sym, size_t size) {
    *source = (ray_block_scan_source_t){NULL, false};
    if (opts) {
        ray_t* keys = ray_dict_keys(opts); ray_t* vals = ray_dict_vals(opts);
        bool empty = keys && vals && keys->type == RAY_LIST && vals->type == RAY_LIST && !keys->len;
        if (!empty && !dictionary(opts)) return ray_error("type", "%s: read options must be a dictionary", who);
        for (int64_t i = 0; !empty && i < keys->len; i++) {
            const char* key = string(ray_sym_vec_cell(keys, i));
            ray_t* value = ray_list_get(vals, i);
            if (key && !strcmp(key, "immutable") && value && value->type == -RAY_BOOL)
                source->immutable = value->b8;
            else if (key && !strcmp(key, "sym") && string(value)) source->sym_path = string(value);
            else return ray_error("domain", "%s: read options are immutable: BOOL and sym: STR", who);
        }
    }
    if (!source->sym_path) source->sym_path = convert_default_sym(path, sym, size);
    return NULL;
}

/* Storage reports over one table, or every partition <root>/<p>/<table>:
 * (fn path [read-options]) or (fn root 'table [read-options]). */
typedef struct {
    const char* who;
    const char* path;   /* table, or parted root */
    const char* table;  /* NULL for a single table */
    ray_block_scan_source_t source;
    char sym[1100];
    char** dirs;
    int64_t n_dirs;
} storage_target_t;

static ray_t* storage_target(const char* who, ray_t** args, int64_t n, storage_target_t* t) {
    memset(t, 0, sizeof(*t));
    t->who = who;
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    if (n < 1 || n > 3) return ray_error("arity", "%s: path [read-options] or root 'table [read-options]", who);
    t->path = string(args[0]);
    if (!t->path) return ray_error("domain", "%s: nonempty path required", who);
    int64_t next = 1;
    if (n > 1 && args[1]->type == -RAY_SYM) {
        t->table = table_name(args[1]);
        if (!t->table) return ray_error("domain", "%s: invalid table name", who);
        next = 2;
    }
    if (n > next + 1) return ray_error("arity", "%s: path [read-options] or root 'table [read-options]", who);
    ray_t* opts = n > next ? args[next] : NULL;
    if (!t->table) return read_source(who, t->path, opts, &t->source, t->sym, sizeof(t->sym));
    /* Partitions share the root's symbol file unless options name another. */
    ray_t* bad = read_source(who, t->path, opts, &t->source, t->sym, sizeof(t->sym));
    if (bad) return bad;
    if (!t->source.sym_path) {
        struct stat st;
        int len = snprintf(t->sym, sizeof(t->sym), "%s/.sym", t->path);
        if (len > 0 && (size_t)len < sizeof(t->sym) && stat(t->sym, &st) == 0) t->source.sym_path = t->sym;
    }
    ray_t* listed = ray_parted_dirs(t->path, &t->dirs, &t->n_dirs, &(uint8_t){0});
    if (listed) return listed;
    if (!t->n_dirs) return ray_error("io", "%s: %s has no partition directories", who, t->path);
    return NULL;
}

static void storage_target_free(storage_target_t* t) {
    ray_parted_dirs_free(t->dirs, t->n_dirs);
    t->dirs = NULL; t->n_dirs = 0;
}

static int64_t storage_count(const storage_target_t* t) { return t->table ? t->n_dirs : 1; }

/* Open target i: the table itself or one partition's table. */
static ray_t* storage_open(storage_target_t* t, int64_t i, ray_block_scan_t** scan) {
    char path[1300];
    const char* where = t->path;
    if (t->table) {
        int len = snprintf(path, sizeof(path), "%s/%s/%s", t->path, t->dirs[i], t->table);
        if (len < 0 || (size_t)len >= sizeof(path)) return ray_error("range", "%s: path too long", t->who);
        where = path;
    }
    ray_block_scan_options_t read = {0, UINT64_MAX, 1, 0, 0, 0};
    ray_err_t err = ray_block_scan_open_source(where, &t->source, NULL, 0, &read, scan);
    return err ? ray_error(ray_err_code_str(err), "%s: cannot open %s", t->who, where) : NULL;
}

/* Result columns grow one row at a time: [partition] then the report's own. */
typedef struct {
    ray_t* cols[11];
    int8_t types[11];
    const char* names[11];
    size_t count;
} storage_rows_t;

static ray_err_t storage_rows_init(storage_rows_t* r, const storage_target_t* t,
                                   const char* const* names, const int8_t* types, size_t count) {
    memset(r, 0, sizeof(*r));
    if (t->table) { r->names[0] = "partition"; r->types[0] = RAY_SYM; r->count = 1; }
    for (size_t i = 0; i < count; i++) {
        r->names[r->count] = names[i];
        r->types[r->count++] = types[i];
    }
    for (size_t i = 0; i < r->count; i++) {
        r->cols[i] = r->types[i] == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, 0) : ray_vec_new(r->types[i], 0);
        if (!r->cols[i] || RAY_IS_ERR(r->cols[i])) { r->cols[i] = NULL; return RAY_ERR_OOM; }
    }
    return RAY_OK;
}

static ray_err_t storage_rows_push(storage_rows_t* r, size_t i, const void* value) {
    ray_t* v = ray_vec_append_raw(r->cols[i], value, 1);
    if (!v || RAY_IS_ERR(v)) return RAY_ERR_OOM;
    r->cols[i] = v;
    return RAY_OK;
}

static void storage_rows_free(storage_rows_t* r) {
    for (size_t i = 0; i < r->count; i++) ray_release(r->cols[i]);
}

static ray_t* storage_rows_table(storage_rows_t* r) {
    ray_t* t = ray_table_new((int64_t)r->count);
    for (size_t i = 0; t && !RAY_IS_ERR(t) && i < r->count; i++)
        t = ray_table_add_col(t, ray_sym_intern(r->names[i], strlen(r->names[i])), r->cols[i]);
    storage_rows_free(r);
    return t ? t : ray_error("oom", NULL);
}

static int64_t storage_symbol(const char* s) { return ray_sym_intern(s, strlen(s)); }

/* (.db.storage.info path|root 'table [read-options]): metadata only. */
ray_t* ray_storage_info_fn(ray_t** args, int64_t n) {
    storage_target_t t;
    ray_t* result = storage_target(".db.storage.info", args, n, &t);
    if (result) { storage_target_free(&t); return result; }
    static const char* const fields[] = {"column", "type", "format", "codec", "rows", "blocks",
                                         "block_bytes", "stored", "logical", "ratio"};
    static const int8_t types[] = {RAY_SYM, RAY_SYM, RAY_SYM, RAY_SYM, RAY_I64, RAY_I64,
                                   RAY_I64, RAY_I64, RAY_I64, RAY_F64};
    storage_rows_t rows;
    ray_err_t err = storage_rows_init(&rows, &t, fields, types, 10);
    size_t base = t.table ? 1 : 0;
    for (int64_t p = 0; !err && !result && p < storage_count(&t); p++) {
        ray_block_scan_t* scan = NULL;
        result = storage_open(&t, p, &scan);
        if (result) break;
        ray_t* names = ray_block_scan_names(scan);
        int64_t partition = t.table ? storage_symbol(t.dirs[p]) : 0;
        for (int64_t c = 0; !err && c < names->len; c++) {
            ray_block_scan_column_info_t info;
            err = ray_block_scan_column_info(scan, (size_t)c, &info);
            if (err) break;
            size_t len;
            const char* name = ray_str_vec_get(names, c, &len);
            uint64_t logical = info.compressed ? info.blocks.decoded_bytes : info.file_bytes;
            const char* codec = !info.compressed || !info.blocks.snappy_blocks ? "none" :
                info.blocks.snappy_blocks == info.blocks.blocks ? "snappy" : "mixed";
            int64_t values[] = {ray_sym_intern(name, len), storage_symbol(ray_type_name(info.type)),
                                storage_symbol(info.compressed ? "block" : "raw"), storage_symbol(codec),
                                (int64_t)info.rows, (int64_t)info.blocks.blocks, info.block_bytes,
                                (int64_t)info.file_bytes, (int64_t)logical};
            double ratio = info.file_bytes ? (double)logical / (double)info.file_bytes : 0.0;
            if (t.table) err = storage_rows_push(&rows, 0, &partition);
            for (size_t i = 0; !err && i < 9; i++) err = storage_rows_push(&rows, base + i, &values[i]);
            if (!err) err = storage_rows_push(&rows, base + 9, &ratio);
        }
        ray_block_scan_close(&scan);
    }
    storage_target_free(&t);
    if (!result && err) result = ray_error(ray_err_code_str(err), ".db.storage.info: metadata unavailable");
    if (result) { storage_rows_free(&rows); return result; }
    return storage_rows_table(&rows);
}

/* (.db.storage.verify path|root 'table [read-options]): decode and check every
 * block of every compressed column; corruption names partition, column, block. */
ray_t* ray_storage_verify_fn(ray_t** args, int64_t n) {
    storage_target_t t;
    ray_t* result = storage_target(".db.storage.verify", args, n, &t);
    if (result) { storage_target_free(&t); return result; }
    static const char* const fields[] = {"column", "format", "blocks"};
    static const int8_t types[] = {RAY_SYM, RAY_SYM, RAY_I64};
    storage_rows_t rows;
    ray_err_t err = storage_rows_init(&rows, &t, fields, types, 3);
    size_t base = t.table ? 1 : 0;
    for (int64_t p = 0; !err && !result && p < storage_count(&t); p++) {
        ray_block_scan_t* scan = NULL;
        result = storage_open(&t, p, &scan);
        if (result) break;
        ray_t* names = ray_block_scan_names(scan);
        int64_t partition = t.table ? storage_symbol(t.dirs[p]) : 0;
        for (int64_t c = 0; !err && !result && c < names->len; c++) {
            if (ray_eval_is_interrupted()) { result = ray_error("cancel", ".db.storage.verify: interrupted"); break; }
            size_t len;
            const char* name = ray_str_vec_get(names, c, &len);
            ray_block_scan_column_info_t info;
            uint64_t checked = 0, block = 0;
            ray_err_t bad = ray_block_scan_column_info(scan, (size_t)c, &info);
            if (!bad) bad = ray_block_scan_verify_column(scan, (size_t)c, &checked, &block);
            if (bad) {
                result = t.table ?
                    ray_error(ray_err_code_str(bad), ".db.storage.verify: %s partition %s column %.*s block %llu",
                              t.path, t.dirs[p], (int)len, name, (unsigned long long)block) :
                    ray_error(ray_err_code_str(bad), ".db.storage.verify: %s column %.*s block %llu",
                              t.path, (int)len, name, (unsigned long long)block);
                break;
            }
            int64_t values[] = {ray_sym_intern(name, len), storage_symbol(info.compressed ? "block" : "raw"),
                                (int64_t)checked};
            if (t.table) err = storage_rows_push(&rows, 0, &partition);
            for (size_t i = 0; !err && i < 3; i++) err = storage_rows_push(&rows, base + i, &values[i]);
        }
        ray_block_scan_close(&scan);
    }
    storage_target_free(&t);
    if (!result && err) result = ray_error(ray_err_code_str(err), ".db.storage.verify: report unavailable");
    if (result) { storage_rows_free(&rows); return result; }
    return storage_rows_table(&rows);
}

/* (.db.block.convert src dest block-options [{immutable: BOOL sym: STR}]) */
/* (.db.block.convert src-root 'table dest-root block-options [read-options]):
 * every partition src-root/<p>/table into dest-root/<p>/table, in key order. */
/* Append the effective codec and block size, so defaults are visible. */
static ray_t* convert_settings(ray_t* t, const ray_block_store_options_t* o, int64_t rows) {
    ray_t* codec = ray_sym_vec_new(RAY_SYM_W64, rows);
    ray_t* bytes = ray_vec_new(RAY_I64, rows);
    if (!t || RAY_IS_ERR(t) || !codec || RAY_IS_ERR(codec) || !bytes || RAY_IS_ERR(bytes)) {
        if (t && !RAY_IS_ERR(t)) ray_release(t);
        if (codec && !RAY_IS_ERR(codec)) ray_release(codec);
        if (bytes && !RAY_IS_ERR(bytes)) ray_release(bytes);
        return ray_error("oom", NULL);
    }
    int64_t name = o->codec ? ray_sym_intern("snappy", 6) : ray_sym_intern("none", 4);
    for (int64_t i = 0; i < rows; i++) {
        ((int64_t*)ray_data(codec))[i] = name;
        ((int64_t*)ray_data(bytes))[i] = o->block_bytes;
    }
    codec->len = bytes->len = rows;
    t = ray_table_add_col(t, ray_sym_intern("codec", 5), codec);
    t = ray_table_add_col(t, ray_sym_intern("block_bytes", 11), bytes);
    ray_release(codec); ray_release(bytes);
    return t ? t : ray_error("oom", NULL);
}

static ray_t* convert_parted(ray_t** args, int64_t n) {
    const char* src = string(args[0]);
    const char* table = table_name(args[1]);
    const char* dest = string(args[2]);
    if (!src || !table || !dest)
        return ray_error("domain", ".db.block.convert: nonempty roots and a table symbol required");
    ray_block_store_options_t options;
    ray_err_t err = ray_block_store_parse_options(n > 3 ? args[3] : NULL, &options);
    if (err) return ray_error(ray_err_code_str(err), ".db.block.convert: invalid block options");
    bool overlap;
    err = ray_block_store_paths_overlap(src, dest, &overlap);
    if (err) return ray_error(ray_err_code_str(err), ".db.block.convert: invalid paths");
    if (overlap) return ray_error("domain", ".db.block.convert: %s and %s overlap", src, dest);
    ray_block_scan_source_t source;
    char sym[1100];
    ray_t* bad = read_source(".db.block.convert", src, n == 5 ? args[4] : NULL, &source, sym, sizeof(sym));
    if (bad) return bad;
    if (!source.sym_path) {
        struct stat st;
        int len = snprintf(sym, sizeof(sym), "%s/.sym", src);
        if (len > 0 && (size_t)len < sizeof(sym) && stat(sym, &st) == 0) source.sym_path = sym;
    }
    char** dirs = NULL;
    int64_t count = 0;
    bad = ray_parted_dirs(src, &dirs, &count, &(uint8_t){0});
    if (bad) return bad;
    if (!count) return ray_error("io", ".db.block.convert: %s has no partition directories", src);
    ray_t* parts = ray_sym_vec_new(RAY_SYM_W64, 0);
    ray_t* rows = ray_vec_new(RAY_I64, 0);
    ray_t* result = !parts || RAY_IS_ERR(parts) || !rows || RAY_IS_ERR(rows) ? ray_error("oom", NULL) : NULL;
    for (int64_t p = 0; !result && p < count; p++) {
        if (ray_eval_is_interrupted()) { result = ray_error("cancel", ".db.block.convert: interrupted"); break; }
        char from[1300], to[1300];
        int a = snprintf(from, sizeof(from), "%s/%s/%s", src, dirs[p], table);
        int b = snprintf(to, sizeof(to), "%s/%s/%s", dest, dirs[p], table);
        if (a < 0 || (size_t)a >= sizeof(from) || b < 0 || (size_t)b >= sizeof(to)) {
            result = ray_error("range", ".db.block.convert: partition path too long"); break;
        }
        uint64_t converted = 0;
        err = ray_block_store_convert(from, &source, to, &options, &converted);
        if (err) {
            result = ray_error(ray_err_code_str(err), ".db.block.convert: partition %s failed; earlier partitions are published", dirs[p]);
            break;
        }
        int64_t name = ray_sym_intern(dirs[p], strlen(dirs[p]));
        int64_t value = (int64_t)converted;
        ray_t* np = ray_vec_append_raw(parts, &name, 1);
        if (np && !RAY_IS_ERR(np)) parts = np;
        ray_t* nr = np && !RAY_IS_ERR(np) ? ray_vec_append_raw(rows, &value, 1) : NULL;
        if (nr && !RAY_IS_ERR(nr)) rows = nr;
        else result = ray_error("oom", NULL);
    }
    ray_parted_dirs_free(dirs, count);
    if (result) { ray_release(parts); ray_release(rows); return result; }
    ray_t* t = ray_table_new(4);
    t = ray_table_add_col(t, ray_sym_intern("partition", 9), parts);
    t = ray_table_add_col(t, ray_sym_intern("rows", 4), rows);
    ray_release(parts); ray_release(rows);
    return convert_settings(t, &options, count);
}

ray_t* ray_block_convert_fn(ray_t** args, int64_t n) {
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    if (n >= 3 && n <= 5 && args[1] && args[1]->type == -RAY_SYM) return convert_parted(args, n);
    if (n < 2 || n > 4) return ray_error("arity", ".db.block.convert: src dest [block-options [read-options]]");
    const char* src = string(args[0]);
    const char* dest = string(args[1]);
    if (!src || !dest) return ray_error("domain", ".db.block.convert: nonempty source and destination paths required");
    ray_block_store_options_t options;
    ray_err_t err = ray_block_store_parse_options(n > 2 ? args[2] : NULL, &options);
    if (err) return ray_error(ray_err_code_str(err), ".db.block.convert: invalid block options");
    ray_block_scan_source_t source;
    char sym[1100];
    ray_t* bad = read_source(".db.block.convert", src, n == 4 ? args[3] : NULL, &source, sym, sizeof(sym));
    if (bad) return bad;
    uint64_t rows = 0;
    err = ray_block_store_convert(src, &source, dest, &options, &rows);
    if (err) return ray_error(ray_err_code_str(err), ".db.block.convert: %s -> %s failed", src, dest);
    ray_t* count = ray_vec_new(RAY_I64, 1);
    if (!count || RAY_IS_ERR(count)) return ray_error("oom", NULL);
    ((int64_t*)ray_data(count))[0] = (int64_t)rows;
    count->len = 1;
    ray_t* t = ray_table_new(3);
    t = ray_table_add_col(t, ray_sym_intern("rows", 4), count);
    ray_release(count);
    return convert_settings(t, &options, 1);
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
        ray_t* value = ray_list_get(expr, 2);
        /* The query is unevaluated: an unquoted name is a column or variable
         * reference, not the symbol it spells. */
        if (!col || (value && value->type == -RAY_SYM && !(value->attrs & ATTR_QUOTED)))
            return false;
        out[(*n)++] = (ray_block_predicate_t){col, c->opcode, value};
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

/* Result vectors shaped like schema's columns; SYM keeps its domain. */
static ray_t* collect_vectors(ray_t* schema, ray_t** vectors, size_t count) {
    for (size_t c = 0; c < count; c++) {
        ray_t* col = ray_table_get_col_idx(schema, (int64_t)c);
        ray_t* v = col->type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, 0) : ray_vec_new(col->type, 0);
        if (!v || RAY_IS_ERR(v)) return v ? v : ray_error("oom", NULL);
        vectors[c] = v;
        if (col->type == RAY_SYM) ray_sym_vec_adopt_domain(v, col);
    }
    return NULL;
}

/* Append a batch to singly owned vectors, so they grow geometrically instead
 * of being copied by table concatenation. NULL or an owned error. */
static ray_t* collect_append(ray_t** vectors, size_t count, ray_t* batch) {
    for (size_t c = 0; c < count; c++) {
        ray_t* src = ray_table_get_col_idx(batch, (int64_t)c);
        if (src->type != vectors[c]->type || (src->type == RAY_SYM &&
            (ray_sym_elem_size(src->type, src->attrs) != 8 ||
             ray_sym_vec_domain(src) != ray_sym_vec_domain(vectors[c]))))
            return ray_error("schema", "block select: inconsistent batch column");
        if (src->type == RAY_STR) {
            for (int64_t i = 0; i < src->len; i++) {
                size_t len; const char* s = ray_str_vec_get(src, i, &len);
                ray_t* v = ray_str_vec_append(vectors[c], s, len);
                if (!v || RAY_IS_ERR(v)) return v ? v : ray_error("oom", NULL);
                vectors[c] = v;
            }
        } else {
            ray_t* v = ray_vec_append_raw(vectors[c], ray_data(src), src->len);
            if (!v || RAY_IS_ERR(v)) return v ? v : ray_error("oom", NULL);
            vectors[c] = v;
        }
        vectors[c]->attrs |= src->attrs & RAY_ATTR_HAS_NULLS;
    }
    return NULL;
}

static ray_t* collect(ray_block_query_t* cursor, const int64_t* aliases,
                      size_t count, size_t remaining) {
    ray_t** vectors = calloc(count, sizeof(*vectors));
    if (!vectors) return ray_error("oom", NULL);
    ray_t* schema = ray_block_query_schema(cursor);
    ray_t* result = NULL;
    if (!schema || RAY_IS_ERR(schema)) { result = schema; schema = NULL; goto done; }
    result = collect_vectors(schema, vectors, count);
    if (result) goto done;
    ray_release(schema); schema = NULL;
    size_t budget = remaining;
    for (;;) {
        ray_err_t reserve_err = ray_block_query_reserve(cursor, budget - remaining);
        if (reserve_err) {
            result = ray_error(ray_err_code_str(reserve_err), "block select: working set exceeds limit");
            goto done;
        }
        ray_t* batch = ray_block_query_next(cursor);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { result = batch; goto done; }
        size_t before = remaining;
        if (!charge(batch, &remaining)) {
            ray_release(batch);
            result = ray_error("limit", "block select: result exceeds explicit byte limit"); goto done;
        }
        size_t retained = budget - remaining;
        size_t batch_bytes = before - remaining;
        if (batch_bytes > SIZE_MAX - retained) {
            ray_release(batch);
            result = ray_error("limit", "block select: copy working set exceeds size limit");
            goto done;
        }
        reserve_err = ray_block_query_reserve(cursor, retained + batch_bytes);
        if (reserve_err) {
            ray_release(batch);
            result = ray_error(ray_err_code_str(reserve_err), "block select: copy working set exceeds limit");
            goto done;
        }
        result = collect_append(vectors, count, batch);
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
        return type == RAY_I16 || type == RAY_I32 || type == RAY_I64 || type == RAY_F64;
    return type == RAY_BOOL || type == RAY_U8 || type == RAY_I16 || type == RAY_I32 ||
        type == RAY_I64 || type == RAY_F64 || type == RAY_DATE || type == RAY_TIME ||
        type == RAY_TIMESTAMP;
}

typedef struct {
    const agg_vtable_t* kernel;
    void* state;
} aggregate_state_t;

/* One batch's aggregate inputs, updated chunk by chunk on the native pool:
 * worker w accumulates into its own state copy (slot 0 is the result). */
typedef struct {
    const aggregate_state_t* states;
    void** slots;           /* [workers * count], slot 0 unused */
    size_t count;
    ray_t* const* cols;
    const uint32_t* gids;
    uint64_t rows;
    uint32_t chunk_rows;
} aggregate_batch_t;

static void aggregate_chunk(const aggregate_batch_t* b, uint32_t worker, uint64_t chunk) {
    uint64_t offset = chunk * b->chunk_rows, n = b->rows - offset;
    if (n > b->chunk_rows) n = b->chunk_rows;
    for (size_t a = 0; a < b->count; a++) {
        ray_t* col = b->cols[a];
        const agg_vtable_t* k = b->states[a].kernel;
        void* state = worker ? b->slots[worker * b->count + a] : b->states[a].state;
        size_t width = ray_sym_elem_size(col->type, col->attrs);
        /* Validity must use the same offset as vals. */
        const uint8_t* data = (const uint8_t*)ray_data(col) + (size_t)offset * width;
        ray_valid_t valid = {data, col->type, ray_vec_may_have_nulls(col)};
        k->update_batch(state, k->state_size, b->gids, data, &valid, (int64_t)n, NULL);
    }
}

static void aggregate_task(void* ctx, uint32_t worker, int64_t start, int64_t end) {
    for (int64_t i = start; i < end; i++) aggregate_chunk(ctx, worker, (uint64_t)i);
}

static ray_t* aggregate_result(ray_block_query_t* cursor, const aggregate_t* const* kinds,
                               const size_t* positions, const int64_t* aliases,
                               size_t count, size_t remaining, uint32_t batch_rows) {
    aggregate_state_t* states = calloc(count, sizeof(*states));
    uint32_t chunk_rows = batch_rows < BLOCK_SOURCE_BATCH_ROWS ? batch_rows : BLOCK_SOURCE_BATCH_ROWS;
    uint32_t* gids = NULL;
    void** slots = NULL;
    uint32_t workers = 1;
    ray_t* schema = NULL;
    ray_t* result = NULL;
    if (!states) goto done;
    schema = ray_block_query_schema(cursor);
    if (!schema || RAY_IS_ERR(schema)) { result = schema; schema = NULL; goto done; }
    bool has_avg = false;
    size_t output_bytes = 0;
    /* Match select's empty-source projection rule, distinct from a WHERE
     * that filters all rows of a nonempty source (one aggregate row). */
    bool empty_source;
    ray_err_t empty_err = ray_block_query_source_empty(cursor, &empty_source);
    if (empty_err) {
        result = ray_error(ray_err_code_str(empty_err), "block select: source size unavailable"); goto done;
    }
    int64_t output_rows = empty_source ? 0 : 1;
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
        output_bytes += bytes;
        states[a].kernel = k;
        has_avg |= op == OP_AVG;
    }
    ray_release(schema); schema = NULL;
    size_t workspace = (size_t)chunk_rows * sizeof(*gids);
    for (size_t a = 0; a < count; a++) {
        if (states[a].kernel->state_size > SIZE_MAX - workspace) {
            result = ray_error("limit", "block select: aggregate workspace exceeds size limit");
            goto done;
        }
        workspace += states[a].kernel->state_size;
    }
    ray_err_t reserve_err = ray_block_query_reserve(cursor, workspace);
    if (reserve_err) {
        result = ray_error(ray_err_code_str(reserve_err), "block select: working set exceeds limit");
        goto done;
    }
    gids = calloc(chunk_rows, sizeof(*gids));
    if (!gids) goto done;
    for (size_t a = 0; a < count; a++) {
        states[a].state = malloc(states[a].kernel->state_size);
        if (!states[a].state) goto done;
        states[a].kernel->init(states[a].state);
    }
    /* Mergeable kernels also accumulate per pool worker; merged at EOF. */
    ray_pool_t* pool = ray_pool_get();
    workers = pool ? ray_pool_total_workers(pool) : 1;
    for (size_t a = 0; a < count && workers > 1; a++)
        if (!states[a].kernel->merge) workers = 1;
    if (workers > 1) {
        size_t extra = 0;
        for (size_t a = 0; a < count; a++) extra += states[a].kernel->state_size;
        if (extra > (SIZE_MAX - workspace) / (workers - 1)) workers = 1;
        else if (ray_block_query_reserve(cursor, workspace + extra * (workers - 1))) workers = 1;
        else workspace += extra * (workers - 1);
    }
    if (workers > 1) {
        slots = calloc((size_t)workers * count, sizeof(*slots));
        if (!slots) goto done;
        for (uint32_t w = 1; w < workers; w++)
            for (size_t a = 0; a < count; a++) {
                void* state = malloc(states[a].kernel->state_size);
                if (!state) goto done;
                states[a].kernel->init(state);
                slots[w * count + a] = state;
            }
    }
    ray_t* cols[RAY_BLOCK_SCAN_MAX_COLUMNS];
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
        if (rows > batch_rows || rows > cap - total) {
            ray_release(batch); result = ray_error("limit", "block select: aggregate row limit exceeded"); goto done;
        }
        total += rows;
        if (ray_eval_is_interrupted()) {
            ray_release(batch); result = ray_error("cancel", "block aggregate interrupted"); goto done;
        }
        for (size_t a = 0; a < count; a++) cols[a] = ray_table_get_col_idx(batch, (int64_t)positions[a]);
        /* The group-id workspace stays one chunk, whatever the input batch. */
        aggregate_batch_t work = {states, slots, count, cols, gids, rows, chunk_rows};
        uint64_t chunks = (rows + chunk_rows - 1) / chunk_rows;
        if (slots && chunks >= 2 && ray_pool_par_dispatch_ok(pool, (int64_t)chunks, 2))
            ray_pool_dispatch_n(pool, aggregate_task, &work, (uint32_t)chunks);
        else
            for (uint64_t c = 0; c < chunks; c++) aggregate_chunk(&work, 0, c);
        ray_release(batch);
    }
    for (uint32_t w = 1; slots && w < workers; w++)
        for (size_t a = 0; a < count; a++)
            states[a].kernel->merge(states[a].state, slots[w * count + a], NULL);
    if (output_bytes > SIZE_MAX - workspace) {
        result = ray_error("limit", "block select: aggregate working set exceeds size limit"); goto done;
    }
    reserve_err = ray_block_query_reserve(cursor, workspace + output_bytes);
    if (reserve_err) {
        result = ray_error(ray_err_code_str(reserve_err), "block select: working set exceeds limit");
        goto done;
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
        if (is_null && kinds[a]->opcode == OP_SUM && k->out_type != RAY_F64) {
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
    for (size_t i = 0; slots && i < (size_t)workers * count; i++) free(slots[i]);
    free(slots); free(states); free(gids);
    return result ? result : ray_error("oom", NULL);
}

ray_t* ray_block_select_source(ray_t* source, ray_t* query) {
    ray_t* path = field(source, ".hdb.block.source");
    ray_t* parted = path ? NULL : field(source, ".hdb.block.parted");
    if (!path && !parted) return NULL;
    if (ray_eval_get_restricted()) return ray_error("access", "restricted");
    ray_t* table = parted ? field(source, ".hdb.block.table") : NULL;
    ray_t* limit = field(source, ".hdb.block.limit");
    ray_t* opts = field(source, ".hdb.block.options");
    if (ray_dict_len(source) != (opts ? 3 : 2) + (parted ? 1 : 0) || !string(path ? path : parted) ||
        (parted && !table_name(table)) || !limit || limit->type != -RAY_I64 ||
        limit->i64 <= 0 || (uint64_t)limit->i64 > SIZE_MAX)
        return ray_error("domain", "invalid block source descriptor");
    ray_block_scan_options_t options;
    bool immutable;
    ray_err_t option_err = scan_options(opts, &options, &immutable);
    if (option_err) return ray_error(ray_err_code_str(option_err), "invalid block scan options");
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
    ray_block_query_t* cursor = NULL;
    ray_block_scan_source_t flat = {NULL, immutable};
    ray_err_t err = parted ?
        ray_block_query_open_parted(string(parted), table_name(table), immutable, columns, n_columns,
                                    pred, n_pred, &options, &cursor) :
        ray_block_query_open_source(string(path), &flat, columns, n_columns, pred, n_pred,
                                    &options, &cursor);
    if (err && parted && err == RAY_ERR_NYI)
        return ray_error("nyi", "block select: %s/%s: first partition is flat (immutable: true reads it) "
                         "or the projection has no stored column", string(parted), table_name(table));
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
    result = aggregate ? aggregate_result(cursor, kinds, positions, aliases, count, (size_t)limit->i64, options.batch_rows) :
                         collect(cursor, aliases, count, (size_t)limit->i64);
done:
    ray_block_query_close(&cursor);
    return result ? result : ray_error("oom", NULL);
}

ray_t* ray_block_materialize(const char* dir, const ray_block_scan_source_t* source, size_t limit) {
    ray_block_scan_options_t read = {0, UINT64_MAX, RAY_BLOCK_SCAN_AUTO_ROWS,
                                     BLOCK_SOURCE_PAYLOAD_BYTES, 8u * 1024 * 1024, 0};
    ray_block_scan_t* scan = NULL;
    ray_err_t err = ray_block_scan_open_source(dir, source, NULL, 0, &read, &scan);
    if (err) return ray_error(ray_err_code_str(err), "materialize: cannot open %s", dir);
    ray_t* schema = ray_block_scan_schema(scan);
    size_t count = schema && !RAY_IS_ERR(schema) ? (size_t)ray_table_ncols(schema) : 0;
    ray_t** vectors = count ? calloc(count, sizeof(*vectors)) : NULL;
    ray_t* result = !schema || RAY_IS_ERR(schema) ? (schema ? schema : ray_error("oom", NULL)) :
        !vectors ? ray_error("oom", NULL) : collect_vectors(schema, vectors, count);
    if (schema && RAY_IS_ERR(schema)) schema = NULL;
    size_t remaining = limit;
    while (!result) {
        if (ray_eval_is_interrupted()) { result = ray_error("cancel", "materialize: interrupted"); break; }
        ray_t* batch = ray_block_scan_next(scan);
        if (!batch) break;
        if (RAY_IS_ERR(batch)) { result = batch; break; }
        if (!charge(batch, &remaining))
            result = ray_error("limit", "materialize: %s exceeds %zu bytes", dir, limit);
        else result = collect_append(vectors, count, batch);
        ray_release(batch);
    }
    if (!result) {
        result = ray_table_new((int64_t)count);
        for (size_t c = 0; result && !RAY_IS_ERR(result) && c < count; c++)
            result = ray_table_add_col(result, ray_table_col_name(schema, (int64_t)c), vectors[c]);
        if (!result) result = ray_error("oom", NULL);
    }
    for (size_t c = 0; vectors && c < count; c++) ray_release(vectors[c]);
    free(vectors);
    if (schema) ray_release(schema);
    ray_block_scan_close(&scan);
    return result;
}
