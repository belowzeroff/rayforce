/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "block_store.h"
#include "block_scan.h"
#include "col_block.h"
#include "col.h"
#include "splay.h"
#include "core/types.h"
#include "mem/heap.h"
#include "ops/hash.h"
#include <stdio.h>
#include <string.h>

static ray_err_t store_preflight(ray_t* table, const ray_block_store_options_t* o,
                                 ray_t** schema_out) {
    if (!o || o->codec > 1) return RAY_ERR_DOMAIN;
    if (!o->block_bytes || o->block_bytes > RAY_COL_BLOCK_MAX) return RAY_ERR_RANGE;
    if (!table || RAY_IS_ERR(table) || table->type != RAY_TABLE) return RAY_ERR_TYPE;
    int64_t count = ray_table_ncols(table), rows = ray_table_nrows(table);
    if (count <= 0 || count > RAY_BLOCK_SCAN_MAX_COLUMNS) return RAY_ERR_LIMIT;
    if (rows < 0) return RAY_ERR_RANGE;
    const uint16_t endian = 1;
    if (*(const uint8_t*)&endian != 1) return RAY_ERR_NYI;
    const char* names[RAY_BLOCK_SCAN_MAX_COLUMNS];
    uint32_t lengths[RAY_BLOCK_SCAN_MAX_COLUMNS];
    /* Validation precedes schema allocation as well as any output mutation. */
    for (int64_t i = 0; i < count; i++) {
        ray_t* col = ray_table_get_col_idx(table, i);
        if (!col || RAY_IS_ERR(col) || col->type < RAY_BOOL || col->type > RAY_GUID)
            return RAY_ERR_TYPE;
        if (col->attrs & ~RAY_ATTR_HAS_NULLS) return RAY_ERR_NYI;
        if (col->len != rows) return RAY_ERR_LENGTH;
        unsigned width = ray_elem_size(col->type);
        if (o->block_bytes < width || o->block_bytes % width) return RAY_ERR_RANGE;
        if ((uint64_t)rows > SIZE_MAX / width) return RAY_ERR_LIMIT;
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
    ray_t* schema = ray_str_vec_from_parts(names, lengths, NULL, count);
    if (!schema || RAY_IS_ERR(schema)) { ray_error_free(schema); return RAY_ERR_OOM; }
    *schema_out = schema;
    return RAY_OK;
}

static ray_err_t sync_file(const char* path) {
    ray_fd_t fd = ray_file_open(path, RAY_OPEN_READ | RAY_OPEN_WRITE);
    if (fd == RAY_FD_INVALID) return RAY_ERR_IO;
    ray_err_t err = ray_file_sync(fd);
    ray_file_close(fd);
    return err;
}

/* mkdir_p may have created several ancestors. Persist their directory entries
 * before publishing a durable generation, from the table root outward. */
static ray_err_t sync_parents(const char* root) {
    char path[1024];
    memcpy(path, root, strlen(root) + 1); /* write.root is bounded by this size. */
    for (;;) {
        ray_err_t err = ray_file_sync_dir(path);
        if (err) return err;
        char* slash = strrchr(path, '/');
        if (!slash || slash == path) return RAY_OK;
        *slash = '\0';
    }
}

ray_err_t ray_block_store_save(ray_t* table, const char* root,
                               const ray_block_store_options_t* options) {
    if (!root || !*root) return RAY_ERR_DOMAIN;
    ray_t* schema = NULL;
    ray_err_t err = store_preflight(table, options, &schema);
    if (err) return err;
    ray_splay_write_t write;
    err = ray_splay_write_begin_staged(root, &write);
    if (err) { ray_release(schema); return err; }
    /* A consistency token shared by these columns, not a globally unique ID.
     * Directory ownership/lease provides the actual snapshot identity. */
    uint64_t token = ray_hash_bytes(write.generation, strlen(write.generation));
    for (int64_t i = 0; !err && i < schema->len; i++) {
        size_t name_len;
        const char* name = ray_str_vec_get(schema, i, &name_len);
        char path[1300];
        int n = snprintf(path, sizeof(path), "%s/%.*s", write.dir, (int)name_len, name);
        if (n < 0 || (size_t)n >= sizeof(path)) { err = RAY_ERR_RANGE; break; }
        FILE* f = fopen(path, "w+b");
        if (!f) { err = RAY_ERR_IO; break; }
        ray_t* col = ray_table_get_col_idx(table, i);
        ray_col_block_writer_t w;
        err = ray_col_block_begin(&w, f, (uint8_t)col->type, options->block_bytes, options->codec, token);
        if (!err) err = ray_col_block_append(&w, ray_data(col), (uint64_t)col->len);
        if (!err) err = ray_col_block_finish(&w);
        else ray_col_block_abort(&w);
        if (fclose(f) && !err) err = RAY_ERR_IO;
        if (!err && options->durable) err = sync_file(path);
    }
    if (!err) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/.d", write.dir);
        err = options->durable ? ray_col_save(schema, path) : ray_col_save_bulk(schema, path);
        if (!err && options->durable) {
            snprintf(path, sizeof(path), "%s/.lease", write.dir);
            err = sync_file(path);
            if (!err) err = sync_parents(write.root);
        }
    }
    ray_release(schema);
    return ray_splay_write_finish(&write, err, options->durable);
}
