/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#include "col_block.h"
#include "core/platform.h"
#include <stdlib.h>

struct ray_col_block_file_s {
    ray_col_block_reader_t reader;
    ray_col_block_cache_t cache;
    void* mapping;
    size_t size;
};

ray_err_t ray_col_block_file_open(const char* path, ray_col_block_file_t** out) {
    if (!out || *out || !path || !*path) return RAY_ERR_DOMAIN;
    ray_col_block_file_t* file = calloc(1, sizeof(*file));
    if (!file) return RAY_ERR_OOM;
    file->mapping = ray_vm_map_file(path, &file->size);
    if (!file->mapping) { free(file); return RAY_ERR_IO; }
    ray_err_t err = ray_col_block_open(&file->reader, file->mapping, file->size);
    if (!err) {
        file->reader.verified = calloc((size_t)(file->reader.blocks / 8 + 1), 1);
        if (!file->reader.verified) err = RAY_ERR_OOM;
        file->cache.block = UINT64_MAX;
        file->reader.cache = &file->cache;
    }
    if (err) {
        ray_vm_unmap_file(file->mapping, file->size);
        free(file);
        return err;
    }
    *out = file;
    return RAY_OK;
}

const ray_col_block_reader_t* ray_col_block_file_reader(const ray_col_block_file_t* file) {
    return file ? &file->reader : NULL;
}

void ray_col_block_file_close(ray_col_block_file_t** file) {
    if (!file || !*file) return;
    ray_vm_unmap_file((*file)->mapping, (*file)->size);
    free((*file)->reader.verified);
    free((*file)->cache.data);
    free(*file);
    *file = NULL;
}
