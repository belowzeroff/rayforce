/* Copyright (c) 2026 Anton Kundenko. MIT license; see LICENSE. */
#define _DEFAULT_SOURCE   /* mkdtemp, strdup */
#include "test.h"
#include "store/block_scan.h"
#include "store/block_store.h"
#include "store/block_sym.h"
#include "ops/block_query.h"
#include "ops/ops.h"
#include "store/col_block.h"
#include "store/col.h"
#include "store/splay.h"
#include "mem/heap.h"
#include "mem/sys.h"
#include "table/sym.h"
#include "table/domain.h"
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <sys/wait.h>
#include <time.h>

static char root[64];
static bool seeded;
static ray_block_scan_t* scan;
static ray_block_query_t* query;
static const char* const names[] = {"x", "y"};

static void setup(void) {
    ray_heap_init(); ray_sym_init();
    strcpy(root, "/tmp/ray-block-scan-XXXXXX");
    if (!mkdtemp(root)) ray_test_fatal("scan test directory");
    seeded = false;
}
static void teardown(void) {
    ray_block_query_close(&query);
    ray_block_scan_close(&scan);
    ray_test_rm_rf(root);
    ray_sym_destroy(); ray_heap_destroy();
}
static int64_t mapped(void) {
    int64_t current, peak; ray_sys_get_mapped(&current, &peak); return current;
}
static void publish(unsigned epoch, unsigned rows, unsigned y_rows, unsigned y_epoch) {
    int64_t x[32]; int32_t y[32];
    for (unsigned i = 0; i < 32; i++) { x[i] = epoch * 100 + i; y[i] = epoch * 1000 + i; }
    ray_t* t = ray_table_new(2);
    for (unsigned c = 0; c < 2; c++) {
        ray_t* v = ray_vec_new(c ? RAY_I32 : RAY_I64, rows);
        if (!v || RAY_IS_ERR(v)) ray_test_fatal("scan test vector");
        memcpy(ray_data(v), c ? (void*)y : (void*)x, rows * (c ? 4 : 8)); v->len = rows;
        t = ray_table_add_col(t, ray_sym_intern(names[c], 1), v); ray_release(v);
        if (!t || RAY_IS_ERR(t)) ray_test_fatal("scan test table");
    }
    if (!seeded) {
        if (ray_splay_save(t, root, NULL)) ray_test_fatal("scan test seed");
        seeded = true;
    }
    ray_splay_write_t write;
    if (ray_splay_write_begin(root, &write)) ray_test_fatal("scan test stage");
    ray_err_t err = ray_splay_write_table(t, write.dir, NULL, false);
    ray_release(t);
    for (unsigned c = 0; !err && c < 2; c++) {
        char path[1100]; snprintf(path, sizeof(path), "%s/%s", write.dir, names[c]);
        FILE* f = fopen(path, "w+b");
        if (!f) { err = RAY_ERR_IO; break; }
        ray_col_block_writer_t w;
        err = ray_col_block_begin(&w, f, c ? RAY_I32 : RAY_I64, c ? 12 : 64, 1, c ? y_epoch : epoch);
        if (!err) err = ray_col_block_append(&w, c ? (void*)y : (void*)x, c ? y_rows : rows);
        if (!err) err = ray_col_block_finish(&w);
        else ray_col_block_abort(&w);
        if (fclose(f) && !err) err = RAY_ERR_IO;
    }
    err = ray_splay_write_finish(&write, err, false);
    if (err) ray_test_fatal("scan test publish");
}
static void damage(const char* name, long offset) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("scan test resolve");
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "r+b");
    if (!f || fseek(f, offset, SEEK_SET)) ray_test_fatal("scan test damage seek");
    int value = fgetc(f);
    if (value == EOF || fseek(f, offset, SEEK_SET) || fputc(value ^ 1, f) == EOF || fclose(f))
        ray_test_fatal("scan test damage");
}

static void replace_x_raw(unsigned epoch, unsigned rows) {
    int64_t values[32];
    for (unsigned i = 0; i < 32; i++) values[i] = epoch * 100 + i;
    ray_t* col = ray_vec_from_raw(RAY_I64, values, rows);
    if (!col || RAY_IS_ERR(col)) ray_test_fatal("scan raw vector");
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("scan raw resolve");
    int n = snprintf(path, sizeof(path), "%s/x", dir);
    if (n < 0 || (size_t)n >= sizeof(path)) ray_test_fatal("scan raw path");
    ray_err_t err = ray_col_save(col, path);
    ray_release(col);
    if (err) ray_test_fatal("scan raw save");
}

static void truncate_x_raw(void) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("scan raw resolve");
    int n = snprintf(path, sizeof(path), "%s/x", dir);
    if (n < 0 || (size_t)n >= sizeof(path) || truncate(path, 1))
        ray_test_fatal("scan raw truncate");
}

static test_result_t mixed_raw_scan(void) {
    publish(1, 16, 16, 1);
    replace_x_raw(1, 16);
    int64_t baseline = mapped();
    char pinned[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    ray_block_scan_options_t options = {2, 12, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* retained = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_nrows(retained), 5);
    ray_t* x = ray_table_get_col_idx(retained, 0);
    ray_t* y = ray_table_get_col_idx(retained, 1);
    for (int64_t i = 0; i < 5; i++) {
        TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[i], 102 + i);
        TEST_ASSERT_EQ_I(((int32_t*)ray_data(y))[i], 1002 + i);
    }
    for (unsigned epoch = 2; epoch <= 4; epoch++) publish(epoch, 16, 16, epoch);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(5, 16, 16, 5);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[0], 102);
    ray_release(retained);

    publish(6, 16, 16, 6);
    replace_x_raw(6, 16);
    options.start = 0; options.count = 5; options.payload_limit = 59;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* error = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(error)); TEST_ASSERT_STR_EQ(ray_err_code(error), "limit");
    ray_release(error); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);

    publish(7, 16, 16, 7);
    replace_x_raw(7, 15);
    options.payload_limit = 60;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);

    publish(8, 16, 16, 8);
    replace_x_raw(8, 16);
    truncate_x_raw();
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t flat_immutable(void) {
    static const char* const symbols[] = {"a", "bb", "a", "ccc", "bb", "a"};
    ray_t* x = ray_vec_new(RAY_I64, 6);
    ray_t* s = ray_sym_vec_new(RAY_SYM_W64, 6);
    for (int64_t i = 0; i < 6; i++) {
        ((int64_t*)ray_data(x))[i] = 10 + i;
        ((int64_t*)ray_data(s))[i] = ray_sym_intern(symbols[i], strlen(symbols[i]));
    }
    x->len = s->len = 6;
    ray_t* t = ray_table_new(2);
    t = ray_table_add_col(t, ray_sym_intern("x", 1), x);
    t = ray_table_add_col(t, ray_sym_intern("s", 1), s);
    ray_release(x); ray_release(s);
    char part[128], sym[128], missing[128];
    snprintf(part, sizeof(part), "%s/2024.01.01/t", root);
    snprintf(sym, sizeof(sym), "%s/.sym", root);
    snprintf(missing, sizeof(missing), "%s/2024.01.02/t", root);
    TEST_ASSERT_EQ_I(ray_splay_save(t, part, sym), RAY_OK);
    ray_release(t);
    int64_t baseline = mapped();
    const char* const projection[] = {"x", "s"};
    ray_block_scan_options_t options = {0, UINT64_MAX, 4, 4096, 4096, 0};
    ray_block_scan_source_t source = {NULL, false};
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(part, &source, projection, 2, &options, &scan),
                     RAY_ERR_NYI);
    source.immutable = true;
    /* The partition has no .sym of its own; numeric columns need none. */
    TEST_ASSERT_TRUE(ray_block_scan_open_source(part, &source, projection, 2, &options, &scan) != RAY_OK);
    TEST_ASSERT_TRUE(scan == NULL);
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(part, &source, projection, 1, &options, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(missing, &source, projection, 1, &options, &scan), RAY_ERR_IO);
    source.sym_path = missing;
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(part, &source, projection, 2, &options, &scan), RAY_ERR_IO);
    char sym_copy[128];
    snprintf(sym_copy, sizeof(sym_copy), "%s", sym);
    source.sym_path = sym_copy;
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(part, &source, projection, 2, &options, &scan), RAY_OK);
    memset(sym_copy, 0, sizeof(sym_copy)); /* not retained after open */
    int64_t row = 0;
    for (ray_t* batch; (batch = ray_block_scan_next(scan)); ray_release(batch)) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
        ray_t* xs = ray_table_get_col_idx(batch, 0);
        ray_t* ss = ray_table_get_col_idx(batch, 1);
        for (int64_t i = 0; i < xs->len; i++, row++) {
            TEST_ASSERT_EQ_I(((int64_t*)ray_data(xs))[i], 10 + row);
            ray_t* cell = ray_sym_vec_cell(ss, i);
            TEST_ASSERT_EQ_I(ray_str_len(cell), strlen(symbols[row]));
            TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(cell), symbols[row], ray_str_len(cell)));
        }
    }
    TEST_ASSERT_EQ_I(row, 6);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);

    /* A root with a generation manifest is never read as a flat directory. */
    source.sym_path = NULL;
    char current[1100];
    snprintf(current, sizeof(current), "%s/.current", part);
    FILE* f = fopen(current, "wb");
    TEST_ASSERT_NOT_NULL(f); fclose(f);
    TEST_ASSERT_TRUE(ray_block_scan_open_source(part, &source, projection, 1, &options, &scan) != RAY_OK);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t auto_batch_rows(void) {
    publish(1, 16, 16, 1);   /* x I64, y I32: 12 logical bytes per row */
    ray_block_scan_options_t options = {0, UINT64_MAX, RAY_BLOCK_SCAN_AUTO_ROWS, 1u << 20, 4096, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_batch_rows(scan), (1u << 19) / 12);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); TEST_ASSERT_EQ_I(ray_table_nrows(batch), 16);
    ray_release(batch); ray_block_scan_close(&scan);
    options.work_limit = 1200;   /* a quarter of the work limit is smaller */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_batch_rows(scan), 300 / 12);
    ray_block_scan_close(&scan);
    options.work_limit = 0; options.payload_limit = (size_t)1 << 40;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_batch_rows(scan), RAY_BLOCK_SCAN_AUTO_MAX);
    ray_block_scan_close(&scan);
    options.batch_rows = 7;      /* explicit sizes are kept */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_batch_rows(scan), 7);
    PASS();
}

/* Raw STR columns are costed by their pooled bytes, so automatic batches of
 * long strings stay within the payload limit. */
static test_result_t auto_batch_raw_strings(void) {
    const int64_t rows = 4000;
    char text[201];
    memset(text, 'q', 200); text[200] = '\0';
    ray_t* s = ray_vec_new(RAY_STR, rows);
    for (int64_t i = 0; i < rows; i++) {
        text[0] = (char)('a' + i % 26);
        s = ray_str_vec_append(s, text, 200);
    }
    ray_t* t = ray_table_new(1);
    t = ray_table_add_col(t, ray_sym_intern("s", 1), s);
    ray_release(s);
    char flat[128];
    snprintf(flat, sizeof(flat), "%s/flat", root);
    TEST_ASSERT_EQ_I(ray_splay_save(t, flat, NULL), RAY_OK);
    ray_release(t);
    const char* const projection[] = {"s"};
    ray_block_scan_options_t options = {0, UINT64_MAX, RAY_BLOCK_SCAN_AUTO_ROWS, 1u << 18, 4096, 0};
    ray_block_scan_source_t source = {NULL, true};
    TEST_ASSERT_EQ_I(ray_block_scan_open_source(flat, &source, projection, 1, &options, &scan), RAY_OK);
    /* Half the payload over a 16-byte descriptor plus 200 pooled bytes. */
    uint32_t batch = ray_block_scan_batch_rows(scan);
    TEST_ASSERT_TRUE(batch <= (1u << 17) / 216 && batch >= (1u << 17) / 232);
    int64_t seen = 0;
    for (ray_t* b; (b = ray_block_scan_next(scan)); ray_release(b)) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(b));
        seen += ray_table_nrows(b);
    }
    TEST_ASSERT_EQ_I(seen, rows);
    PASS();
}

static void sleep_us(long us) {
    struct timespec ts = {us / 1000000, (us % 1000000) * 1000};
    nanosleep(&ts, NULL);
}

/* Generation e of a crash/concurrency table: x = y = e * 1000000 + row. */
static ray_t* epoch_table(int64_t epoch, int64_t rows) {
    ray_t* x = ray_vec_new(RAY_I64, rows);
    ray_t* y = ray_vec_new(RAY_I64, rows);
    for (int64_t i = 0; i < rows; i++)
        ((int64_t*)ray_data(x))[i] = ((int64_t*)ray_data(y))[i] = epoch * 1000000 + i;
    x->len = y->len = rows;
    ray_t* t = ray_table_new(2);
    t = ray_table_add_col(t, ray_sym_intern("x", 1), x);
    t = ray_table_add_col(t, ray_sym_intern("y", 1), y);
    ray_release(x); ray_release(y);
    return t;
}

/* Read the whole table: 0 and *epoch when every row belongs to one complete
 * generation, else -1. */
static int epoch_read(int64_t rows, int64_t* epoch) {
    ray_block_scan_t* sc = NULL;
    ray_block_scan_options_t read = {0, UINT64_MAX, 1000, 1u << 20, 1u << 20, 0};
    if (ray_block_scan_open(root, names, 2, &read, &sc)) return -1;
    int64_t seen = 0, e = -1;
    int bad = 0;
    for (ray_t* b; !bad && (b = ray_block_scan_next(sc)); ray_release(b)) {
        if (RAY_IS_ERR(b)) { bad = 1; break; }
        const int64_t* x = ray_data(ray_table_get_col_idx(b, 0));
        const int64_t* y = ray_data(ray_table_get_col_idx(b, 1));
        for (int64_t i = 0; i < ray_table_nrows(b); i++, seen++) {
            if (e < 0) e = x[i] / 1000000;
            if (x[i] != y[i] || x[i] != e * 1000000 + seen) { bad = 1; break; }
        }
    }
    ray_block_scan_close(&sc);
    if (bad || seen != rows) return -1;
    *epoch = e;
    return 0;
}

/* A writer killed at any point publishes all of a generation or none of it. */
static test_result_t crash_publication(void) {
    const int64_t rows = 60000;
    ray_block_store_options_t store = {4096, 1, true};
    ray_t* t = epoch_table(1, rows);
    long save_us = 0;
    for (int i = 0; i < 3; i++) {
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        long us = (long)((t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_nsec - t0.tv_nsec) / 1000);
        if (us > save_us) save_us = us;
    }
    ray_release(t);
    /* Kill points sweep 0..150% of the slowest save: staging, sync,
     * publication and after it. */
    long window = save_us + save_us / 2 + 1;
    /* A reader scans throughout, through kills inside rename and GC. */
    fflush(stdout); fflush(stderr);
    pid_t reader = fork();
    TEST_ASSERT_TRUE(reader >= 0);
    if (!reader) {
        for (;;) {
            int64_t e;
            if (epoch_read(rows, &e)) _exit(1);
            sleep_us(200);
        }
    }
    int64_t current = 1;
    int published = 0, kept = 0;
    /* A forked writer can run slower than the timed saves: widen the sweep
     * until kills land after publication too, narrow it if never before. */
    for (int64_t attempt = 2, round = 0; round < 3 && !(published && kept); round++) {
        if (round && !published) window *= 2;
        else if (round && !kept) window /= 2;
        for (int k = 0; k < 40; k++, attempt++) {
            t = epoch_table(attempt, rows);
            fflush(stdout); fflush(stderr);
            pid_t pid = fork();
            TEST_ASSERT_TRUE(pid >= 0);
            if (!pid) _exit(ray_block_store_save(t, root, &store) ? 1 : 0);
            sleep_us(window * k / 40);
            kill(pid, SIGKILL);
            int status = 0;
            TEST_ASSERT_EQ_I(waitpid(pid, &status, 0), pid);
            ray_release(t);
            int64_t epoch = 0;
            TEST_ASSERT_EQ_I(epoch_read(rows, &epoch), 0);
            TEST_ASSERT_TRUE(epoch == current || epoch == attempt);
            if (epoch == attempt) published++; else kept++;
            current = epoch;
        }
    }
    int reader_status = 0;
    pid_t reader_done = waitpid(reader, &reader_status, WNOHANG);
    if (!reader_done) {
        kill(reader, SIGKILL);
        TEST_ASSERT_EQ_I(waitpid(reader, &reader_status, 0), reader);
    }
    /* Still scanning when stopped: no read saw a partial generation. */
    TEST_ASSERT_TRUE(WIFSIGNALED(reader_status));
    /* Both outcomes occurred, so kills landed on both sides of publication. */
    TEST_ASSERT_TRUE(published > 0 && kept > 0);
    /* A later writer succeeds over whatever the killed ones left. */
    t = epoch_table(99, rows);
    TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
    ray_release(t);
    int64_t epoch = 0;
    TEST_ASSERT_EQ_I(epoch_read(rows, &epoch), 0);
    TEST_ASSERT_EQ_I(epoch, 99);
    PASS();
}

/* Readers in other processes see one whole generation per scan while a
 * writer publishes and collects old ones; collection keeps up. */
static test_result_t concurrent_readers(void) {
    const int64_t rows = 30000;
    ray_block_store_options_t store = {4096, 1, false};
    ray_t* t = epoch_table(1, rows);
    TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
    ray_release(t);
    pid_t readers[3];
    fflush(stdout); fflush(stderr);
    for (int r = 0; r < 3; r++) {
        readers[r] = fork();
        TEST_ASSERT_TRUE(readers[r] >= 0);
        if (!readers[r]) {
            int scans = 0;
            for (int i = 0; i < 400; i++) {
                int64_t epoch;
                if (epoch_read(rows, &epoch)) _exit(1);
                scans++;
                sleep_us(500);
            }
            _exit(scans ? 0 : 2);
        }
    }
    for (int64_t epoch = 2; epoch <= 31; epoch++) {
        t = epoch_table(epoch, rows);
        TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
        ray_release(t);
        sleep_us(5000);
    }
    for (int r = 0; r < 3; r++) {
        int status = 0;
        TEST_ASSERT_EQ_I(waitpid(readers[r], &status, 0), readers[r]);
        TEST_ASSERT_TRUE(WIFEXITED(status));
        TEST_ASSERT_EQ_I(WEXITSTATUS(status), 0);
    }
    /* With readers gone, one more publication prunes to current + previous. */
    t = epoch_table(32, rows);
    TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
    ray_release(t);
    char gens[128];
    snprintf(gens, sizeof(gens), "%s/.generations", root);
    DIR* d = opendir(gens);
    TEST_ASSERT_NOT_NULL(d);
    int count = 0;
    for (struct dirent* e; (e = readdir(d));) if (e->d_name[0] != '.') count++;
    closedir(d);
    TEST_ASSERT_TRUE(count <= 2);
    int64_t epoch = 0;
    TEST_ASSERT_EQ_I(epoch_read(rows, &epoch), 0);
    TEST_ASSERT_EQ_I(epoch, 32);
    PASS();
}

/* A writer holds .write.lock for its whole write; scans do not wait for it,
 * in other processes or in its own. */
static test_result_t scan_during_write(void) {
    const int64_t rows = 3000;
    ray_block_store_options_t store = {4096, 1, false};
    ray_t* t = epoch_table(1, rows);
    TEST_ASSERT_EQ_I(ray_block_store_save(t, root, &store), RAY_OK);
    ray_release(t);
    ray_splay_write_t write;
    TEST_ASSERT_EQ_I(ray_splay_write_begin_staged(root, &write), RAY_OK);
    fflush(stdout); fflush(stderr);
    pid_t pid = fork();
    TEST_ASSERT_TRUE(pid >= 0);
    if (!pid) {
        int64_t e = 0;
        _exit(epoch_read(rows, &e) || e != 1 ? 1 : 0);
    }
    int status = 0;
    pid_t done = 0;
    for (int i = 0; i < 1000 && !(done = waitpid(pid, &status, WNOHANG)); i++) sleep_us(10000);
    if (!done) { kill(pid, SIGKILL); (void)waitpid(pid, &status, 0); }
    if (done == pid && WIFEXITED(status) && !WEXITSTATUS(status)) {
        int64_t e = 0;
        TEST_ASSERT_EQ_I(epoch_read(rows, &e), 0);
        TEST_ASSERT_EQ_I(e, 1);
    }
    /* Abandoned: nothing is published. */
    (void)ray_splay_write_finish(&write, RAY_ERR_IO, false);
    TEST_ASSERT_EQ_I(done, pid);
    TEST_ASSERT_TRUE(WIFEXITED(status));
    TEST_ASSERT_EQ_I(WEXITSTATUS(status), 0);
    int64_t e = 0;
    TEST_ASSERT_EQ_I(epoch_read(rows, &e), 0);
    TEST_ASSERT_EQ_I(e, 1);
    PASS();
}

static test_result_t shared_work_limit(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, 5, 5, 60, 64, 131};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_reserve(scan, 7), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);

    options.work_limit = 130;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_reserve(scan, 7), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit");
    ray_release(batch); ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);

    options.work_limit = 6;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_reserve(scan, 7), RAY_ERR_LIMIT);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t batches(void) {
    publish(1, 16, 16, 1);
    char pinned[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    int64_t baseline = mapped();
    const char* const projection[] = {"y", "x"};
    ray_block_scan_options_t options = {2, 12, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &options, &scan), RAY_OK);
    uint64_t offset = 2;
    ray_t* retained = NULL;
    for (unsigned batch = 0; batch < 3; batch++) {
        ray_t* t = ray_block_scan_next(scan);
        TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t));
        int64_t n = batch == 2 ? 2 : 5;
        TEST_ASSERT_EQ_I(ray_table_nrows(t), n); TEST_ASSERT_EQ_I(ray_table_ncols(t), 2);
        TEST_ASSERT_EQ_I(ray_table_col_name(t, 0), ray_sym_intern("y", 1));
        ray_t* y = ray_table_get_col_idx(t, 0); ray_t* x = ray_table_get_col_idx(t, 1);
        for (int64_t i = 0; i < n; i++) {
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(y))[i], 1000 + offset + i);
            TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[i], 100 + offset + i);
        }
        offset += n;
        if (!batch) {
            retained = t;
            for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
            TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
        } else ray_release(t);
    }
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL);
    ray_block_scan_close(&scan); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(5, 16, 16, 5);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[0], 102);
    ray_release(retained);
    PASS();
}

static test_result_t failures(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    const char* const invalid[] = {"x", "../y"};
    const char* const duplicates[] = {"x", "x"};
    const char* const missing[] = {"x", "missing"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, NULL), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, NULL, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 0, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1025, &options, &scan), RAY_ERR_LIMIT);
    options.batch_rows = 0;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_DOMAIN);
    options.batch_rows = 5;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, invalid, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, duplicates, 2, &options, &scan), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, missing, 2, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    for (unsigned kind = 0; kind < 3; kind++) {
        options.payload_limit = kind == 0 ? 59 : 60;
        options.scratch_limit = kind == 1 ? 63 : 64;
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        if (kind == 2) ray_block_scan_cancel(scan);
        ray_t* err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err));
        TEST_ASSERT_STR_EQ(ray_err_code(err), kind == 2 ? "cancel" : "limit"); ray_error_free(err);
        err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    publish(2, 16, 15, 2);
    char failed_generation[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, failed_generation, sizeof(failed_generation)), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_SCHEMA);
    publish(3, 16, 16, 4);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(4, 0, 0, 4);
    TEST_ASSERT_EQ_I(access(failed_generation, F_OK), -1); /* No leaked lease after open failure. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* schema = ray_block_scan_schema(scan);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); TEST_ASSERT_EQ_I(ray_table_ncols(schema), 2);
    TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_I64);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 1)->type, RAY_I32); ray_release(schema);
    options.start = 1;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t cancel_after_batch(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_DOMAIN);
    ray_t* t = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t));
    ray_block_scan_cancel(scan);
    ray_t* err = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "cancel");
    ray_error_free(err); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(t, 0)))[0], 100);
    ray_release(t);
    ray_block_scan_cancel(NULL); ray_block_scan_close(NULL);
    err = ray_block_scan_next(NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); ray_error_free(err);
    PASS();
}

static test_result_t projection(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    damage("y", 0); /* A corrupt unselected column must never be opened. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_t* t = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(t); TEST_ASSERT_FALSE(RAY_IS_ERR(t)); ray_release(t);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(2, 16, 16, 2);
    damage("y", RAY_COL_BLOCK_HEADER); /* Failure after x has been materialized. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    t = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(t)); TEST_ASSERT_STR_EQ(ray_err_code(t), "corrupt"); ray_error_free(t);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t subset_ranges(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    char pinned[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    ray_block_scan_options_t options = {2, 9, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    size_t y_only[] = {1};
    uint64_t start = UINT64_MAX;
    ray_t* filter = ray_block_scan_next_columns(scan, y_only, 1, &start);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(start, 2); TEST_ASSERT_EQ_I(ray_table_nrows(filter), 5);
    TEST_ASSERT_EQ_I(ray_table_ncols(filter), 1);
    TEST_ASSERT_EQ_I(ray_table_col_name(filter, 0), ray_sym_intern("y", 1));
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(filter, 0)))[4], 1006);
    ray_release(filter);
    for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
    size_t reverse[] = {1, 0};
    ray_t* retained = ray_block_scan_read_columns(scan, reverse, 2, 4, 3);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_nrows(retained), 3);
    TEST_ASSERT_EQ_I(ray_table_col_name(retained, 0), ray_sym_intern("y", 1));
    TEST_ASSERT_EQ_I(((int32_t*)ray_data(ray_table_get_col_idx(retained, 0)))[0], 1004);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[2], 106);
    filter = ray_block_scan_next_columns(scan, NULL, 1, &start);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(start, 7); TEST_ASSERT_EQ_I(ray_table_nrows(filter), 4);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(filter, 0)))[3], 110);
    ray_release(filter);
    start = UINT64_MAX;
    TEST_ASSERT_TRUE(ray_block_scan_next_columns(scan, NULL, 1, &start) == NULL);
    TEST_ASSERT_TRUE(start == UINT64_MAX);
    filter = ray_block_scan_read_columns(scan, NULL, 1, 2, 1);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(filter, 0)))[0], 102);
    ray_release(filter);
    filter = ray_block_scan_read_columns(scan, reverse, 2, 11, 0);
    TEST_ASSERT_NOT_NULL(filter); TEST_ASSERT_FALSE(RAY_IS_ERR(filter));
    TEST_ASSERT_EQ_I(ray_table_nrows(filter), 0);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(filter, 0)->type, RAY_I32);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(filter, 1)->type, RAY_I64);
    ray_release(filter);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(5, 16, 16, 5); TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(retained, 1)))[0], 104);
    ray_release(retained);
    PASS();
}

static test_result_t subset_failures(void) {
    publish(1, 16, 16, 1);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {2, 9, 5, 60, 64, 0};
    const size_t invalid[] = {SIZE_MAX}, duplicate[] = {1, 1};
    for (unsigned which = 0; which < 14; which++) {
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        uint64_t start = UINT64_MAX;
        ray_t* err = NULL;
        const char* code = "range";
        switch (which) {
        case 0: err = ray_block_scan_next_columns(scan, NULL, 0, &start); code = "domain"; break;
        case 1: err = ray_block_scan_next_columns(scan, NULL, 3, &start); code = "domain"; break;
        case 2: err = ray_block_scan_next_columns(scan, invalid, 1, &start); break;
        case 3: err = ray_block_scan_next_columns(scan, duplicate, 2, &start); code = "domain"; break;
        case 4: err = ray_block_scan_read_columns(scan, NULL, 1, 1, 1); break;
        case 5: err = ray_block_scan_read_columns(scan, NULL, 1, 12, 0); break;
        case 6: err = ray_block_scan_read_columns(scan, NULL, 1, 10, 2); break;
        case 7: err = ray_block_scan_read_columns(scan, NULL, 1, 2, 6); break;
        case 8: err = ray_block_scan_read_columns(scan, NULL, 1, UINT64_MAX, 1); break;
        case 9: err = ray_block_scan_read_columns(scan, NULL, 1, 2, UINT64_MAX); break;
        case 10: err = ray_block_scan_read_columns(scan, duplicate, 2, 2, 1); code = "domain"; break;
        case 11: err = ray_block_scan_read_columns(scan, invalid, 1, 2, 1); break;
        case 12:
            ray_block_scan_cancel(scan);
            err = ray_block_scan_read_columns(scan, NULL, 1, 2, 0); code = "cancel"; break;
        case 13:
            ray_block_scan_cancel(scan);
            err = ray_block_scan_next_columns(scan, NULL, 1, &start); code = "cancel"; break;
        }
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), code);
        TEST_ASSERT_TRUE(start == UINT64_MAX); ray_error_free(err);
        err = ray_block_scan_read_columns(scan, NULL, 1, 2, 1);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        err = ray_block_scan_next(scan);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    ray_t* err = ray_block_scan_read_columns(NULL, NULL, 1, 0, 0);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
    err = ray_block_scan_next_columns(NULL, NULL, 1, NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_error_free(err);
    PASS();
}

static test_result_t subset_payloads(void) {
    publish(1, 16, 16, 1);
    /* Open validates y's metadata, but filter-only x reads must not decode y. */
    damage("y", RAY_COL_BLOCK_HEADER);
    int64_t baseline = mapped();
    ray_block_scan_options_t options = {0, 5, 5, 40, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next_columns(scan, NULL, 1, NULL);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    size_t y_only[] = {1};
    batch = ray_block_scan_read_columns(scan, y_only, 1, 0, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    batch = ray_block_scan_read_columns(scan, y_only, 1, 0, 1);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt"); ray_error_free(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    publish(2, 16, 16, 2);
    for (unsigned which = 0; which < 3; which++) {
        options.payload_limit = which == 0 ? 39 : 40;
        options.scratch_limit = which == 1 ? 63 : 64;
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 2, &options, &scan), RAY_OK);
        batch = ray_block_scan_read_columns(scan, NULL, which == 2 ? 2 : 1, 0, 5);
        TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_error_free(batch);
        ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    PASS();
}

static void schema_path(char* path, size_t capacity) {
    char dir[1024];
    if (ray_splay_resolve_dir(root, dir, sizeof(dir))) ray_test_fatal("schema fixture resolve");
    int n = snprintf(path, capacity, "%s/.d", dir);
    if (n < 0 || (size_t)n >= capacity) ray_test_fatal("schema fixture path");
}
static void write_schema(ray_t* schema) {
    if (!schema || RAY_IS_ERR(schema)) ray_test_fatal("schema fixture allocation");
    char path[1100]; schema_path(path, sizeof(path));
    ray_err_t err = ray_col_save(schema, path);
    ray_release(schema);
    if (err) ray_test_fatal("schema fixture save");
}

static test_result_t schema_membership(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    int64_t baseline = mapped();
    char dir[1024], from[1100], to[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(from, sizeof(from), "%s/x", dir); snprintf(to, sizeof(to), "%s/rogue", dir);
    TEST_ASSERT_EQ_I(rename(from, to), 0);
    const char* const rogue[] = {"rogue"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, rogue, 1, &options, &scan), RAY_ERR_SCHEMA);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(rename(to, from), 0);
    /* Declaration without a selected data file is an I/O failure, not a schema failure. */
    const char* schema_names[] = {"x", "ghost"};
    const uint32_t lengths[] = {1, 5};
    write_schema(ray_str_vec_from_parts(schema_names, lengths, NULL, 2));
    const char* const ghost[] = {"ghost"};
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, ghost, 1, &options, &scan), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    /* No failed open retained a lease: this generation may be pruned. */
    publish(2, 16, 16, 2); publish(3, 16, 16, 3);
    TEST_ASSERT_EQ_I(access(dir, F_OK), -1);
    PASS();
}

static test_result_t schema_corruption(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    int64_t baseline = mapped();
    const char* bad[] = {"", ".hidden", "../y", "a/b", "a\\b", "y\0tail", "x"};
    const uint32_t sizes[] = {0, 7, 4, 3, 3, 6, 1};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); i++) {
        const char* entries[] = {"x", bad[i]};
        uint32_t lengths[] = {1, sizes[i]};
        write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
        TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    }
    write_schema(ray_vec_new(RAY_I64, 0));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    write_schema(ray_vec_new(RAY_STR, 0));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_SCHEMA);
    char path[1100]; schema_path(path, sizeof(path));
    TEST_ASSERT_EQ_I(unlink(path), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_IO);
    FILE* f = fopen(path, "wb"); TEST_ASSERT_NOT_NULL(f); TEST_ASSERT_EQ_I(fclose(f), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    f = fopen(path, "wb"); TEST_ASSERT_NOT_NULL(f);
    int truncated = ftruncate(fileno(f), RAY_BLOCK_SCAN_SCHEMA_MAX_BYTES + 1);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_LIMIT);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t schema_limits(void) {
    publish(1, 16, 16, 1);
    ray_block_scan_options_t options = {0, UINT64_MAX, 5, 60, 64, 0};
    int64_t baseline = mapped();
    ray_t* schema = ray_vec_new(RAY_STR, RAY_BLOCK_SCAN_MAX_COLUMNS);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    for (unsigned i = 0; i < RAY_BLOCK_SCAN_MAX_COLUMNS; i++) {
        char name[32];
        int n = i ? snprintf(name, sizeof(name), "column%u", i) : snprintf(name, sizeof(name), "x");
        schema = ray_str_vec_append(schema, name, (size_t)n);
        TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    }
    char path[1100]; schema_path(path, sizeof(path));
    TEST_ASSERT_EQ_I(ray_col_save(schema, path), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    schema = ray_str_vec_append(schema, "extra", 5);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); write_schema(schema);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_LIMIT);
    char long_name[256]; memset(long_name, 'a', sizeof(long_name));
    const char* entries[] = {"x", long_name};
    uint32_t lengths[] = {1, 255};
    write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    lengths[1] = 256;
    write_schema(ray_str_vec_from_parts(entries, lengths, NULL, 2));
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    /* Truncated serialized schema must fail in the existing column decoder. */
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    int truncated = ftruncate(fileno(f), 31);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &options, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static ray_t* store_table(unsigned epoch, unsigned rows) {
    ray_t* t = ray_table_new(2);
    for (unsigned c = 0; c < 2; c++) {
        ray_t* col = ray_vec_new(c ? RAY_I32 : RAY_I64, rows);
        if (!col || RAY_IS_ERR(col)) ray_test_fatal("store table allocation");
        for (unsigned i = 0; i < rows; i++) {
            if (c) ((int32_t*)ray_data(col))[i] = epoch * 1000 + i;
            else ((int64_t*)ray_data(col))[i] = i == 1 ? INT64_MIN : epoch * 100 + i;
        }
        col->len = rows;
        if (!c) col->attrs |= RAY_ATTR_HAS_NULLS;
        t = ray_table_add_col(t, ray_sym_intern(names[c], 1), col); ray_release(col);
        if (!t || RAY_IS_ERR(t)) ray_test_fatal("store table assembly");
    }
    return t;
}

static test_result_t store_roundtrip(void) {
    char target[128], path[1300], pinned[1024];
    snprintf(target, sizeof(target), "%s/new/table", root);
    ray_block_store_options_t options = {64, 1, true};
    ray_t* table = store_table(1, 16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    ray_release(table);
    snprintf(path, sizeof(path), "%s/.d", target); TEST_ASSERT_EQ_I(access(path, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, pinned, sizeof(pinned)), RAY_OK);
    TEST_ASSERT_TRUE(strcmp(pinned, target) != 0);
    ray_block_scan_options_t scan_options = {0, UINT64_MAX, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    ray_t* first = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(first); TEST_ASSERT_FALSE(RAY_IS_ERR(first));
    TEST_ASSERT_TRUE(ray_vec_is_null(ray_table_get_col_idx(first, 0), 1));
    for (unsigned e = 2; e <= 4; e++) {
        table = store_table(e, 16);
        options.codec = e % 2; options.durable = false;
        TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
        ray_release(table);
    }
    TEST_ASSERT_EQ_I(access(pinned, F_OK), 0);
    unsigned row = 5;
    ray_t* batch;
    while ((batch = ray_block_scan_next(scan))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
        ray_t* x = ray_table_get_col_idx(batch, 0);
        for (int64_t i = 0; i < x->len; i++)
            TEST_ASSERT_EQ_I(((int64_t*)ray_data(x))[i], 100 + row + i);
        row += (unsigned)x->len; ray_release(batch);
    }
    TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    TEST_ASSERT_EQ_I(((int64_t*)ray_data(ray_table_get_col_idx(first, 0)))[0], 100);
    ray_release(first);
    table = store_table(5, 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

/* Rows [lo, hi) of a BOOL/I64/STR/SYM table, as fresh runtime vectors. */
static ray_t* stream_slice(ray_t* table, int64_t lo, int64_t hi) {
    ray_t* out = ray_table_new(ray_table_ncols(table));
    for (int64_t c = 0; c < ray_table_ncols(table); c++) {
        ray_t* src = ray_table_get_col_idx(table, c);
        ray_t* col;
        if (src->type == RAY_STR) {
            col = ray_vec_new(RAY_STR, hi - lo);
            for (int64_t i = lo; i < hi; i++) {
                size_t n; const char* s = ray_str_vec_get(src, i, &n);
                col = ray_str_vec_append(col, s, n);
            }
        } else {
            size_t w = ray_type_sizes[(uint8_t)src->type];
            col = src->type == RAY_SYM ? ray_sym_vec_new(RAY_SYM_W64, hi - lo) :
                ray_vec_new(src->type, hi - lo);
            memcpy(ray_data(col), (const char*)ray_data(src) + lo * w, (size_t)(hi - lo) * w);
            col->len = hi - lo;
        }
        out = ray_table_add_col(out, ray_table_col_name(table, c), col);
        ray_release(col);
    }
    return out;
}

static ray_t* stream_table(int64_t rows) {
    ray_t* b = ray_vec_new(RAY_BOOL, rows);
    ray_t* a = ray_vec_new(RAY_I64, rows);
    ray_t* k = ray_sym_vec_new(RAY_SYM_W64, rows);
    ray_t* s = ray_vec_new(RAY_STR, rows);
    char text[64];
    for (int64_t i = 0; i < rows; i++) {
        ((uint8_t*)ray_data(b))[i] = (uint8_t)(i % 3 == 0);
        ((int64_t*)ray_data(a))[i] = i * 7 - 50;
        int n = snprintf(text, sizeof(text), "k%lld", (long long)(i % 13));
        ((int64_t*)ray_data(k))[i] = i % 11 ? ray_sym_intern(text, (size_t)n) : 0;
        n = snprintf(text, sizeof(text), "%.*s", (int)(i % 40), "a string of varying length, up to forty");
        s = ray_str_vec_append(s, text, (size_t)n);
    }
    b->len = a->len = k->len = rows;
    const char* const cols[] = {"b", "a", "k", "s"};
    ray_t* vecs[] = {b, a, k, s};
    ray_t* t = ray_table_new(4);
    for (unsigned c = 0; c < 4; c++) {
        t = ray_table_add_col(t, ray_sym_intern(cols[c], 1), vecs[c]);
        ray_release(vecs[c]);
    }
    return t;
}

static uint64_t stream_blocks(const char* target, const char* column) {
    char dir[1024], path[1100];
    if (ray_splay_resolve_dir(target, dir, sizeof(dir))) return 0;
    snprintf(path, sizeof(path), "%s/%s", dir, column);
    ray_col_block_file_t* file = NULL;
    if (ray_col_block_file_open(path, &file)) return 0;
    uint64_t blocks = ray_col_block_file_reader(file)->blocks;
    ray_col_block_file_close(&file);
    return blocks;
}

static test_result_t store_stream_chunks(void) {
    const int64_t rows = 1000;
    ray_t* table = stream_table(rows);
    char one[128], two[128];
    snprintf(one, sizeof(one), "%s/one", root);
    snprintf(two, sizeof(two), "%s/two", root);
    ray_block_store_options_t options = {96, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(table, one, &options), RAY_OK);
    ray_block_table_writer_t* w = NULL;
    TEST_ASSERT_EQ_I(ray_block_table_begin(two, table, &options, &w), RAY_OK);
    const int64_t sizes[] = {1, 7, 64, 333, 0};
    for (int64_t lo = 0, i = 0; lo < rows; i++) {
        int64_t hi = lo + sizes[i % 5];
        if (hi > rows) hi = rows;
        ray_t* chunk = stream_slice(table, lo, hi);
        TEST_ASSERT_EQ_I(ray_block_table_append(w, chunk), RAY_OK);
        ray_release(chunk);
        lo = hi;
    }
    TEST_ASSERT_EQ_I(ray_block_table_finish(&w), RAY_OK);
    TEST_ASSERT_TRUE(w == NULL);
    const char* const cols[] = {"b", "a", "k", "s"};
    for (unsigned c = 0; c < 4; c++)
        TEST_ASSERT_EQ_I(stream_blocks(two, cols[c]), stream_blocks(one, cols[c]));
    ray_block_scan_options_t read = {0, UINT64_MAX, 4096, 1u << 20, 1u << 20, 0};
    ray_block_scan_t* other = NULL;
    TEST_ASSERT_EQ_I(ray_block_scan_open(one, cols, 4, &read, &scan), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(two, cols, 4, &read, &other), RAY_OK);
    ray_t* x = ray_block_scan_next(scan);
    ray_t* y = ray_block_scan_next(other);
    ray_block_scan_close(&scan); ray_block_scan_close(&other);
    TEST_ASSERT_FALSE(RAY_IS_ERR(x)); TEST_ASSERT_FALSE(RAY_IS_ERR(y));
    TEST_ASSERT_EQ_I(ray_table_nrows(x), rows); TEST_ASSERT_EQ_I(ray_table_nrows(y), rows);
    for (unsigned c = 0; c < 4; c++) {
        ray_t* p = ray_table_get_col_idx(x, c); ray_t* q = ray_table_get_col_idx(y, c);
        ray_t* e = ray_table_get_col_idx(table, c);
        for (int64_t i = 0; i < rows; i++) {
            if (e->type == RAY_STR) {
                size_t n1, n2, n3;
                const char* s1 = ray_str_vec_get(p, i, &n1);
                const char* s2 = ray_str_vec_get(q, i, &n2);
                const char* s3 = ray_str_vec_get(e, i, &n3);
                TEST_ASSERT_TRUE(n1 == n3 && n2 == n3 && (!n3 || (!memcmp(s1, s3, n3) && !memcmp(s2, s3, n3))));
            } else if (e->type == RAY_SYM) {
                ray_t* s1 = ray_sym_vec_cell(p, i); ray_t* s2 = ray_sym_vec_cell(q, i);
                ray_t* s3 = ray_sym_vec_cell(e, i);
                TEST_ASSERT_EQ_I(ray_str_len(s1), ray_str_len(s3));
                TEST_ASSERT_EQ_I(ray_str_len(s2), ray_str_len(s3));
                TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(s1), ray_str_ptr(s3), ray_str_len(s3)));
                TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(s2), ray_str_ptr(s3), ray_str_len(s3)));
            } else {
                size_t sz = ray_type_sizes[(uint8_t)e->type];
                TEST_ASSERT_TRUE(!memcmp((char*)ray_data(p) + i * sz, (char*)ray_data(e) + i * sz, sz));
                TEST_ASSERT_TRUE(!memcmp((char*)ray_data(q) + i * sz, (char*)ray_data(e) + i * sz, sz));
            }
        }
    }
    ray_release(x); ray_release(y);

    /* A failed chunk is terminal and the published generation survives. */
    char before[1024], after[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(two, before, sizeof(before)), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_table_begin(two, table, &options, &w), RAY_OK);
    ray_t* chunk = stream_slice(table, 0, 10);
    TEST_ASSERT_EQ_I(ray_block_table_append(w, chunk), RAY_OK);
    ray_t* wrong = ray_table_new(1);
    wrong = ray_table_add_col(wrong, ray_sym_intern("b", 1), ray_table_get_col_idx(chunk, 0));
    TEST_ASSERT_EQ_I(ray_block_table_append(w, wrong), RAY_ERR_SCHEMA);
    TEST_ASSERT_EQ_I(ray_block_table_append(w, chunk), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_table_finish(&w), RAY_ERR_DOMAIN);
    TEST_ASSERT_TRUE(w == NULL);
    ray_release(wrong);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(two, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(after, before);
    /* An oversize string fails the append, abort keeps the old generation. */
    ray_block_store_options_t small = {16, 0, false};
    ray_t* long_strings = stream_slice(table, 0, 20); /* lengths up to 19 > 16 - 4 */
    TEST_ASSERT_EQ_I(ray_block_table_begin(two, table, &small, &w), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_table_append(w, long_strings), RAY_ERR_RANGE);
    ray_release(long_strings);
    ray_block_table_abort(&w);
    TEST_ASSERT_TRUE(w == NULL);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(two, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(after, before);
    /* No appends: an empty table with the schema. */
    TEST_ASSERT_EQ_I(ray_block_table_begin(two, chunk, &options, &w), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_table_finish(&w), RAY_OK);
    TEST_ASSERT_EQ_I(ray_block_scan_open(two, cols, 4, &read, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL);
    ray_block_scan_close(&scan);
    ray_release(chunk); ray_release(table);
    PASS();
}

static test_result_t store_preflight(void) {
    char target[128]; snprintf(target, sizeof(target), "%s/preflight", root);
    ray_block_store_options_t options = {64, 1, false};
    ray_t* table = store_table(1, 16);
    ray_t* x = ray_table_get_col_idx(table, 0);
    ray_t* y = ray_table_get_col_idx(table, 1);
    y->len--;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_LENGTH); y->len++;
    x->attrs |= RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_NYI);
    x->attrs &= (uint8_t)~RAY_ATTR_SORTED;
    ray_table_set_col_name(table, 1, ray_sym_intern("../bad", 6));
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    ray_table_set_col_name(table, 1, ray_sym_intern("x", 1));
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    ray_table_set_col_name(table, 1, ray_sym_intern("y", 1));
    options.block_bytes = 7;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_RANGE);
    options.block_bytes = 64; options.codec = 2;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_DOMAIN);
    options.codec = 1;
    TEST_ASSERT_EQ_I(ray_block_store_save(x, target, &options), RAY_ERR_TYPE);
    ray_t* unsupported = ray_table_new(1);
    ray_t* str = ray_vec_new(RAY_LIST, 0);
    unsupported = ray_table_add_col(unsupported, ray_sym_intern("s", 1), str); ray_release(str);
    TEST_ASSERT_EQ_I(ray_block_store_save(unsupported, target, &options), RAY_ERR_TYPE);
    ray_release(unsupported);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    char before[1024], after[1024];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, before, sizeof(before)), RAY_OK);
    x->attrs |= RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_NYI);
    x->attrs &= (uint8_t)~RAY_ATTR_SORTED;
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(before, after); ray_release(table);
    PASS();
}

static test_result_t store_rollback(void) {
    char target[128], manifest[140], staged[1024];
    snprintf(target, sizeof(target), "%s/rollback", root);
    snprintf(manifest, sizeof(manifest), "%s/.current", target);
    ray_splay_write_t write;
    TEST_ASSERT_EQ_I(ray_splay_write_begin_staged(target, &write), RAY_OK);
    TEST_ASSERT_TRUE(write.staged); snprintf(staged, sizeof(staged), "%s", write.dir);
    TEST_ASSERT_EQ_I(access(manifest, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_write_finish(&write, RAY_ERR_IO, false), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(access(staged, F_OK), -1); TEST_ASSERT_EQ_I(access(manifest, F_OK), -1);
    /* Obstruct the final rename, after the whole new generation is written. */
    TEST_ASSERT_EQ_I(ray_test_mkdir_p(manifest), 0);
    ray_t* table = store_table(1, 16);
    ray_block_store_options_t options = {64, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(rmdir(manifest), 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_OK);
    char before[1024], after[1024], column[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, before, sizeof(before)), RAY_OK);
    TEST_ASSERT_EQ_I(ray_splay_write_begin_staged(target, &write), RAY_OK);
    snprintf(staged, sizeof(staged), "%s", write.dir);
    snprintf(column, sizeof(column), "%s/x", write.dir);
    TEST_ASSERT_EQ_I(ray_col_save(ray_table_get_col_idx(table, 0), column), RAY_OK);
    TEST_ASSERT_EQ_I(ray_splay_write_finish(&write, RAY_ERR_IO, false), RAY_ERR_IO);
    TEST_ASSERT_EQ_I(access(staged, F_OK), -1);
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(target, after, sizeof(after)), RAY_OK);
    TEST_ASSERT_STR_EQ(before, after);
    ray_block_scan_options_t scan_options = {0, UINT64_MAX, 5, 60, 64, 0};
    TEST_ASSERT_EQ_I(ray_block_scan_open(target, names, 2, &scan_options, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); ray_release(table);
    PASS();
}

/* Reversed local dictionaries deliberately give equal strings different IDs. */
static ray_t* symbol_table(uint8_t width, unsigned rows) {
    ray_t* table = store_table(1, rows);
    ray_sym_domain_t* dom = ray_sym_domain_new();
    if (!dom || ray_sym_domain_intern(dom, "beta", 4) != 1 ||
        ray_sym_domain_intern(dom, "alpha", 5) != 2) ray_test_fatal("symbol fixture domain");
    const char* text[] = {"", "alpha", "beta"};
    for (unsigned c = 0; c < 2; c++) {
        ray_t* v = ray_sym_vec_new(width, rows);
        if (!v || RAY_IS_ERR(v)) ray_test_fatal("symbol fixture vector");
        if (c) { v->sym_domain = dom; ray_sym_domain_retain(dom); }
        for (unsigned i = 0; i < rows; i++) {
            unsigned which = i % 3;
            int64_t pos = c ? (which ? 3 - which : 0) : ray_sym_intern(text[which], strlen(text[which]));
            ray_write_sym(ray_data(v), i, (uint64_t)pos, RAY_SYM, width);
        }
        v->len = rows; v->attrs |= RAY_ATTR_HAS_NULLS;
        table = ray_table_add_col(table, ray_sym_intern(c ? "local" : "global", c ? 5 : 6), v);
        ray_release(v);
        if (!table || RAY_IS_ERR(table)) ray_test_fatal("symbol fixture table");
    }
    ray_sym_domain_release(dom);
    return table;
}

static test_result_t store_symbols(void) {
    const char* projection[] = {"global", "local", "x"};
    const char* text[] = {"", "alpha", "beta"};
    ray_block_store_options_t options = {64, 1, true};
    ray_block_scan_options_t read = {1, 15, 5, 120, 64, 0};
    for (uint8_t width = RAY_SYM_W8; width <= RAY_SYM_W64; width++) {
        ray_t* table = symbol_table(width, 16);
        options.codec = width % 2;
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
        TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
        ray_t* schema = ray_block_scan_schema(scan);
        TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
        TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_SYM);
        TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0); ray_release(schema);
        unsigned row = 1;
        ray_t* batch;
        while ((batch = ray_block_scan_next(scan))) {
            TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
            ray_t* a = ray_table_get_col_idx(batch, 0);
            ray_t* b = ray_table_get_col_idx(batch, 1);
            TEST_ASSERT_TRUE(ray_sym_vec_domain(a) == ray_sym_vec_domain(b));
            TEST_ASSERT_TRUE(ray_sym_vec_domain(a) != ray_sym_runtime_domain());
            for (int64_t j = 0; j < a->len; j++) {
                const char* expected = text[(row + j) % 3];
                ray_t* sa = ray_sym_vec_cell(a, j); ray_t* sb = ray_sym_vec_cell(b, j);
                TEST_ASSERT_NOT_NULL(sa); TEST_ASSERT_NOT_NULL(sb);
                TEST_ASSERT_EQ_I(ray_str_len(sa), strlen(expected));
                TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(sa), expected, strlen(expected)));
                TEST_ASSERT_TRUE(sa == sb);
                TEST_ASSERT_EQ_I(ray_vec_is_null(a, j), (row + j) % 3 == 0);
            }
            row += (unsigned)a->len; ray_release(batch);
        }
        TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    }
    /* Release all inputs, then rebuild runtime IDs in a different order. */
    ray_sym_destroy(); ray_sym_init();
    TEST_ASSERT_TRUE(ray_sym_intern("unrelated", 9) >= 0);
    TEST_ASSERT_TRUE(ray_sym_intern("beta", 4) >= 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
    ray_t* retained = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    ray_block_scan_close(&scan);
    char pinned[1024]; TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, pinned, sizeof(pinned)), RAY_OK);
    for (unsigned i = 0; i < 3; i++) {
        ray_t* replacement = store_table(i, 0);
        TEST_ASSERT_EQ_I(ray_block_store_save(replacement, root, &options), RAY_OK); ray_release(replacement);
    }
    TEST_ASSERT_EQ_I(access(pinned, F_OK), -1);
    ray_t* v = ray_table_get_col_idx(retained, 0);
    ray_t* s = ray_sym_vec_cell(v, 0);
    TEST_ASSERT_EQ_I(ray_str_len(s), 5); TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(s), "alpha", 5));
    char other[128]; snprintf(other, sizeof(other), "%s/resave", root);
    TEST_ASSERT_EQ_I(ray_block_store_save(retained, other, &options), RAY_OK);
    ray_release(retained);
    read.start = 0; read.count = UINT64_MAX;
    ray_t* empty = symbol_table(RAY_SYM_W8, 0);
    TEST_ASSERT_EQ_I(ray_block_store_save(empty, root, &options), RAY_OK); ray_release(empty);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 3, &read, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

static test_result_t symbol_failures(void) {
    const char* projection[] = {"global", "local"};
    ray_block_store_options_t options = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 80, 64, 0};
    ray_t* table = symbol_table(RAY_SYM_W64, 16);
    ray_t* col = ray_table_get_col_idx(table, 2);
    ((uint64_t*)ray_data(col))[0] = UINT64_MAX;
    char target[128]; snprintf(target, sizeof(target), "%s/invalid", root);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_CORRUPT);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    ((uint64_t*)ray_data(col))[0] = 0;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    int64_t baseline = mapped();
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    ray_t* error = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(error)); TEST_ASSERT_STR_EQ(ray_err_code(error), "limit"); ray_release(error);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline); read.payload_limit++;
    damage(".sym", 20);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_CORRUPT);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    /* A numeric projection is independent of an unselected symbol dictionary. */
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, names, 1, &read, &scan), RAY_OK);
    ray_block_scan_close(&scan);
    char dir[1024], path[1100];
    TEST_ASSERT_EQ_I(ray_splay_resolve_dir(root, dir, sizeof(dir)), RAY_OK);
    snprintf(path, sizeof(path), "%s/.sym", dir);
    FILE* f = fopen(path, "r+b"); TEST_ASSERT_NOT_NULL(f);
    TEST_ASSERT_EQ_I(fseek(f, 4, SEEK_SET), 0);
    uint64_t forged_count = UINT64_MAX, checked_count;
    uint32_t checked_crc;
    TEST_ASSERT_EQ_I(fwrite(&forged_count, 8, 1, f), 1);
    TEST_ASSERT_EQ_I(fflush(f), 0);
    TEST_ASSERT_EQ_I(ray_block_sym_checksum(path, &checked_count, &checked_crc), RAY_ERR_CORRUPT);
    int truncated = ftruncate(fileno(f), RAY_BLOCK_SYM_MAX_BYTES + 1);
    TEST_ASSERT_EQ_I(fclose(f), 0); TEST_ASSERT_EQ_I(truncated, 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_LIMIT);
    TEST_ASSERT_EQ_I(unlink(path), 0);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_ERR_IO);
    TEST_ASSERT_TRUE(scan == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static const char* string_parts[] = {
    "", "abcdefghijkl", "abcdefghijklm", "a\0b",
    "012345678901234567890123456789012345678901234567890123456789"
};
static const uint32_t string_lengths[] = {0, 12, 13, 3, 60};

static ray_t* string_table(unsigned rows) {
    const char* parts[16]; uint32_t lengths[16];
    for (unsigned i = 0; i < rows; i++) {
        parts[i] = string_parts[i % 5]; lengths[i] = string_lengths[i % 5];
    }
    ray_t* table = symbol_table(RAY_SYM_W8, rows);
    ray_t* col = ray_str_vec_from_parts(parts, lengths, NULL, rows);
    if (!col || RAY_IS_ERR(col)) ray_test_fatal("string fixture vector");
    table = ray_table_add_col(table, ray_sym_intern("s", 1), col);
    if (!table || RAY_IS_ERR(table)) ray_test_fatal("string fixture table");
    table = ray_table_add_col(table, ray_sym_intern("t", 1), col); ray_release(col);
    if (!table || RAY_IS_ERR(table)) ray_test_fatal("string fixture table");
    return table;
}

static test_result_t subset_variable_width(void) {
    const char* projection[] = {"s", "local", "x", "t"};
    const size_t numeric[] = {2}, result[] = {1, 0};
    ray_block_store_options_t options = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 109, 64, 0};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    /* Neither reading x nor materializing local/s should decode the unused t. */
    damage("t", RAY_COL_BLOCK_HEADER);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    uint64_t start = UINT64_MAX;
    ray_t* batch = ray_block_scan_next_columns(scan, numeric, 1, &start);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(start, 0); ray_release(batch);
    /* 4 symbol codes + 4 STR descriptors + 13 pooled bytes = 109 bytes. */
    ray_t* retained = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_ncols(retained), 2);
    batch = ray_block_scan_read_columns(scan, result, 2, 16, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(ray_table_nrows(batch), 0);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(batch, 0)->type, RAY_SYM);
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(batch, 1)->type, RAY_STR); ray_release(batch);
    for (unsigned i = 0; i < 3; i++) {
        table = string_table(0);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    }
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    const char* symbols[] = {"", "alpha", "beta", ""};
    for (int64_t i = 0; i < 4; i++) {
        ray_t* symbol = ray_sym_vec_cell(ray_table_get_col_idx(retained, 0), i);
        TEST_ASSERT_NOT_NULL(symbol);
        TEST_ASSERT_EQ_I(ray_str_len(symbol), strlen(symbols[i]));
        TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(symbol), symbols[i], strlen(symbols[i])));
        size_t len;
        const char* str = ray_str_vec_get(ray_table_get_col_idx(retained, 1), i, &len);
        TEST_ASSERT_EQ_I(len, string_lengths[i]);
        TEST_ASSERT_TRUE(!memcmp(str, string_parts[i], len));
    }
    ray_release(retained);
    table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    int64_t baseline = mapped();
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 4);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_error_free(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit = 0; read.scratch_limit = 0;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_read_columns(scan, result, 2, 0, 0);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t store_strings(void) {
    const char* projection[] = {"s", "local", "x", "t"};
    ray_block_store_options_t options = {64, 1, true};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 1024, 64, 0};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    ray_t* schema = ray_block_scan_schema(scan);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema));
    TEST_ASSERT_EQ_I(ray_table_get_col_idx(schema, 0)->type, RAY_STR); ray_release(schema);
    ray_t* first = NULL;
    unsigned row = 0;
    ray_t* batch;
    while ((batch = ray_block_scan_next(scan))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
        ray_t* col = ray_table_get_col_idx(batch, 0);
        ray_t* other = ray_table_get_col_idx(batch, 3);
        TEST_ASSERT_EQ_I(col->len, 4);
        for (int64_t j = 0; j < col->len; j++) {
            unsigned which = (row + j) % 5;
            size_t len; const char* s = ray_str_vec_get(col, j, &len);
            TEST_ASSERT_EQ_I(len, string_lengths[which]);
            TEST_ASSERT_TRUE(!memcmp(s, string_parts[which], len));
            size_t n; const char* t = ray_str_vec_get(other, j, &n);
            TEST_ASSERT_EQ_I(n, len); TEST_ASSERT_TRUE(!memcmp(s, t, len));
            TEST_ASSERT_EQ_I(ray_vec_is_null(col, j), which == 0);
        }
        if (!row) {
            first = batch;
            for (unsigned i = 0; i < 3; i++) {
                table = string_table(0); options.codec = 0; options.durable = false;
                TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
            }
        } else ray_release(batch);
        row += 4;
    }
    TEST_ASSERT_EQ_I(row, 16); ray_block_scan_close(&scan);
    size_t len; const char* s = ray_str_vec_get(ray_table_get_col_idx(first, 0), 2, &len);
    TEST_ASSERT_EQ_I(len, 13); TEST_ASSERT_TRUE(!memcmp(s, string_parts[2], len));
    TEST_ASSERT_EQ_I(ray_block_store_save(first, root, &options), RAY_OK); ray_release(first);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan);
    table = string_table(0);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 4, &read, &scan), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_scan_next(scan) == NULL); ray_block_scan_close(&scan);
    PASS();
}

static test_result_t string_limits(void) {
    const char* projection[] = {"s", "t"};
    ray_block_store_options_t options = {64, 0, false};
    ray_t* table = string_table(16);
    char target[128]; snprintf(target, sizeof(target), "%s/too-long", root);
    options.block_bytes = 56;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, target, &options), RAY_ERR_RANGE);
    TEST_ASSERT_EQ_I(access(target, F_OK), -1);
    options.block_bytes = 64;
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &options), RAY_OK); ray_release(table);
    ray_block_scan_options_t read = {0, 4, 4, 154, 44, 0};
    int64_t baseline = mapped();
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    ray_t* batch = ray_block_scan_next(scan);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_release(batch);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "domain"); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    read.payload_limit++; read.scratch_limit--;
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "limit"); ray_release(batch);
    ray_block_scan_close(&scan);
    read.scratch_limit++; damage("s", RAY_COL_BLOCK_HEADER);
    TEST_ASSERT_EQ_I(ray_block_scan_open(root, projection, 2, &read, &scan), RAY_OK);
    batch = ray_block_scan_next(scan);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt"); ray_release(batch);
    ray_block_scan_close(&scan); TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t query_late_materialization(void) {
    const char* output[] = {"s", "local"};
    ray_block_store_options_t write = {64, 1, false};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4, 256, 64, 0};
    ray_t* table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    damage("s", RAY_COL_BLOCK_HEADER);
    ray_t* value = ray_i64(INT64_MAX);
    ray_block_predicate_t pred = {"x", OP_EQ, value};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_t* schema = ray_block_query_schema(query);
    TEST_ASSERT_FALSE(RAY_IS_ERR(schema)); TEST_ASSERT_EQ_I(ray_table_nrows(schema), 0);
    TEST_ASSERT_EQ_I(ray_table_ncols(schema), 2); ray_release(schema);
    ray_block_query_close(&query);
    pred.opcode = OP_LT;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    ray_release(value);
    ray_t* err = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "corrupt"); ray_release(err);
    err = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_release(err);
    ray_block_query_close(&query);
    table = string_table(16);
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    value = ray_i64(INT64_MAX); pred.value = value;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 2, &pred, 1, &read, &query), RAY_OK);
    value->i64 = INT64_MIN; ray_release(value); /* Open owns a copy, not an alias. */
    ray_t* retained = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(retained); TEST_ASSERT_FALSE(RAY_IS_ERR(retained));
    TEST_ASSERT_EQ_I(ray_table_nrows(retained), 4);
    for (unsigned i = 0; i < 3; i++) {
        table = string_table(0);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK); ray_release(table);
    }
    unsigned rows = 4;
    ray_t* batch;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); rows += ray_table_nrows(batch); ray_release(batch);
    }
    TEST_ASSERT_EQ_I(rows, 16);
    ray_block_query_close(&query);
    size_t len;
    const char* str = ray_str_vec_get(ray_table_get_col_idx(retained, 0), 2, &len);
    TEST_ASSERT_EQ_I(len, 13); TEST_ASSERT_TRUE(!memcmp(str, string_parts[2], len));
    ray_t* sym = ray_sym_vec_cell(ray_table_get_col_idx(retained, 1), 1);
    TEST_ASSERT_EQ_I(ray_str_len(sym), 5); TEST_ASSERT_TRUE(!memcmp(ray_str_ptr(sym), "alpha", 5));
    ray_release(retained);
    PASS();
}

static test_result_t query_conjunction(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {2, 12, 5, 60, 64, 0};
    ray_t* lo = ray_i64(103); ray_t* hi = ray_i64(112); ray_t* hole = ray_i64(105);
    ray_t* y_lo = ray_i32(1002);
    ray_block_predicate_t predicates[RAY_BLOCK_QUERY_MAX_PREDICATES];
    for (unsigned i = 0; i < RAY_BLOCK_QUERY_MAX_PREDICATES; i++)
        predicates[i] = (ray_block_predicate_t){"x", OP_GE, lo};
    predicates[1] = (ray_block_predicate_t){"x", OP_LE, hi};
    predicates[2] = (ray_block_predicate_t){"x", OP_NE, hole};
    predicates[3] = (ray_block_predicate_t){"y", OP_GT, y_lo};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, predicates,
                     RAY_BLOCK_QUERY_MAX_PREDICATES, &read, &query), RAY_OK);
    ray_release(lo); ray_release(hi); ray_release(hole); ray_release(y_lo);
    unsigned expected = 1003, rows = 0;
    ray_t* batch;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); TEST_ASSERT_EQ_I(ray_table_ncols(batch), 1);
        ray_t* col = ray_table_get_col_idx(batch, 0);
        for (int64_t i = 0; i < col->len; i++) {
            if (expected == 1005) expected++;
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], expected++); rows++;
        }
        ray_release(batch);
        if (rows == 2) for (unsigned e = 2; e <= 4; e++) publish(e, 16, 16, e);
    }
    TEST_ASSERT_EQ_I(rows, 9); TEST_ASSERT_EQ_I(expected, 1013);
    PASS();
}

static test_result_t query_native_parity(void) {
    const int8_t types[] = {RAY_BOOL, RAY_U8, RAY_I16, RAY_I32, RAY_I64,
                           RAY_DATE, RAY_TIME, RAY_TIMESTAMP};
    const uint16_t ops[] = {OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE};
    const char* output[] = {"id"};
    ray_block_store_options_t write = {64, 1, false};
    ray_block_scan_options_t read = {1, 15, 5, 80, 64, 0};
    for (unsigned t = 0; t < sizeof(types); t++) {
        int8_t type = types[t];
        ray_t* col = ray_vec_new(type, 16); col->len = 16;
        int64_t ids[16];
        for (int64_t i = 0; i < 16; i++) {
            ids[i] = i;
            int64_t v = i % 3;
            switch (type) {
            case RAY_BOOL: ((uint8_t*)ray_data(col))[i] = v != 0; break;
            case RAY_U8: ((uint8_t*)ray_data(col))[i] = (uint8_t)v; break;
            case RAY_I16: ((int16_t*)ray_data(col))[i] = (int16_t)v; break;
            case RAY_I32: case RAY_DATE: case RAY_TIME: ((int32_t*)ray_data(col))[i] = (int32_t)v; break;
            default: ((int64_t*)ray_data(col))[i] = v; break;
            }
        }
        if (type != RAY_BOOL && type != RAY_U8) ray_vec_set_null(col, 7, true);
        ray_t* id = ray_vec_from_raw(RAY_I64, ids, 16);
        ray_t* table = ray_table_new(2);
        table = ray_table_add_col(table, ray_sym_intern("v", 1), col);
        table = ray_table_add_col(table, ray_sym_intern("id", 2), id);
        ray_release(col); ray_release(id);
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &write), RAY_OK);
        for (unsigned null = 0; null < 2; null++) {
            ray_t* atom = null ? ray_typed_null(-type) : ray_i64(1);
            if (!null) atom->type = -type;
            for (unsigned op = 0; op < sizeof(ops) / sizeof(*ops); op++) {
                ray_graph_t* g = ray_graph_new(table);
                ray_op_t* source = ray_scan(g, "v");
                ray_op_t* literal = ray_const_atom(g, atom);
                ray_t* reference = ray_execute(g, ray_binop(g, ops[op], source, literal));
                TEST_ASSERT_NOT_NULL(reference); TEST_ASSERT_FALSE(RAY_IS_ERR(reference));
                TEST_ASSERT_EQ_I(reference->type, RAY_BOOL);
                ray_graph_free(g);
                ray_block_predicate_t pred = {"v", ops[op], atom};
                TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
                unsigned expected = 1;
                ray_t* batch;
                while ((batch = ray_block_query_next(query))) {
                    TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
                    ray_t* values = ray_table_get_col_idx(batch, 0);
                    for (int64_t i = 0; i < values->len; i++) {
                        while (expected < 16 && !((uint8_t*)ray_data(reference))[expected]) expected++;
                        TEST_ASSERT_TRUE(expected < 16);
                        TEST_ASSERT_EQ_I(((int64_t*)ray_data(values))[i], expected++);
                    }
                    ray_release(batch);
                }
                while (expected < 16 && !((uint8_t*)ray_data(reference))[expected]) expected++;
                TEST_ASSERT_EQ_I(expected, 16);
                ray_release(reference); ray_block_query_close(&query);
            }
            ray_release(atom);
        }
        ray_release(table);
    }
    PASS();
}

static test_result_t query_zone_pruning(void) {
    publish(1, 16, 16, 1);
    damage("x", RAY_COL_BLOCK_HEADER);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {0, 16, 8, 64, 64, 0};
    ray_t* atom = ray_i64(108);
    ray_block_predicate_t pred = {"x", OP_GE, atom};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_t* batch = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch));
    TEST_ASSERT_EQ_I(ray_table_nrows(batch), 8);
    ray_t* y = ray_table_get_col_idx(batch, 0);
    for (int64_t i = 0; i < 8; i++)
        TEST_ASSERT_EQ_I(((int32_t*)ray_data(y))[i], 1008 + i);
    ray_release(batch);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_block_query_close(&query);

    pred.opcode = OP_LT;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(atom);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "corrupt");
    ray_release(batch);
    PASS();
}

/* Release gate 2: on a sorted column, a narrow range decodes only the blocks
 * it overlaps; every excluded block stays undecoded, in the filter column and
 * in the late-materialized output column. */
static test_result_t gate_range_pruning(void) {
    struct { uint32_t rows, block_bytes, batch_rows, lo, hi; uint64_t blocks; } cases[] = {
        {4096, 512, 64, 1000, 1064, 4},               /* batch == I64 block */
        {4096, 512, 4096, 1000, 1064, 4},             /* one read: trimmed to the range's blocks */
        /* Default batch, 64 KiB blocks: batches are finer than blocks, so the
         * next batch inside the matching t block decodes that block again. */
        {524288, 65536, 4096, 100000, 100100, 3}
    };
    for (unsigned c = 0; c < sizeof(cases) / sizeof(*cases); c++) {
        uint32_t rows = cases[c].rows;
        ray_t* t = ray_vec_new(RAY_I64, rows);
        ray_t* v = ray_vec_new(RAY_I32, rows);
        for (uint32_t i = 0; i < rows; i++) {
            ((int64_t*)ray_data(t))[i] = i;
            ((int32_t*)ray_data(v))[i] = (int32_t)(i * 3);
        }
        t->len = v->len = rows;
        ray_t* table = ray_table_new(2);
        table = ray_table_add_col(table, ray_sym_intern("t", 1), t);
        table = ray_table_add_col(table, ray_sym_intern("v", 1), v);
        ray_release(t); ray_release(v);
        ray_block_store_options_t store = {cases[c].block_bytes, 1, false};
        TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &store), RAY_OK);
        ray_release(table);
        uint64_t total_blocks = (rows * 8ull + cases[c].block_bytes - 1) / cases[c].block_bytes +
                                (rows * 4ull + cases[c].block_bytes - 1) / cases[c].block_bytes;
        const char* output[] = {"v"};
        ray_t* lo = ray_i64(cases[c].lo); ray_t* hi = ray_i64(cases[c].hi);
        ray_block_predicate_t pred[] = {{"t", OP_GE, lo}, {"t", OP_LT, hi}};
        ray_block_scan_options_t read = {0, UINT64_MAX, cases[c].batch_rows, 1u << 20, 1u << 20, 0};
        TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, pred, 2, &read, &query), RAY_OK);
        ray_release(lo); ray_release(hi);
        uint64_t matched = 0;
        for (ray_t* b; (b = ray_block_query_next(query)); ray_release(b)) {
            TEST_ASSERT_FALSE(RAY_IS_ERR(b));
            ray_t* col = ray_table_get_col_idx(b, 0);
            for (int64_t i = 0; i < col->len; i++, matched++)
                TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], (int32_t)((cases[c].lo + matched) * 3));
        }
        TEST_ASSERT_EQ_I(matched, cases[c].hi - cases[c].lo);
        ray_block_query_stats_t stats;
        ray_block_query_stats(query, &stats);
        TEST_ASSERT_EQ_I(stats.scan.blocks_decoded, cases[c].blocks);
        TEST_ASSERT_EQ_I(stats.scan.raw_rows, 0);
        uint64_t batches = (rows + cases[c].batch_rows - 1) / cases[c].batch_rows;
        TEST_ASSERT_TRUE(batches <= 2 || stats.batches_skipped >= batches - 2);
        TEST_ASSERT_TRUE(stats.scan.blocks_decoded * 10 <= total_blocks);
        ray_block_query_close(&query);
    }
    PASS();
}

/* Scattered matches: every output block is decoded once, not once per run. */
static test_result_t query_scattered_matches(void) {
    const uint32_t rows = 4096;
    ray_t* x = ray_vec_new(RAY_I64, rows);
    ray_t* v = ray_vec_new(RAY_I32, rows);
    for (uint32_t i = 0; i < rows; i++) {
        ((int64_t*)ray_data(x))[i] = i % 7;
        ((int32_t*)ray_data(v))[i] = (int32_t)i;
    }
    x->len = v->len = rows;
    ray_t* table = ray_table_new(2);
    table = ray_table_add_col(table, ray_sym_intern("x", 1), x);
    table = ray_table_add_col(table, ray_sym_intern("v", 1), v);
    ray_release(x); ray_release(v);
    ray_block_store_options_t store = {512, 1, false};
    TEST_ASSERT_EQ_I(ray_block_store_save(table, root, &store), RAY_OK);
    ray_release(table);
    const char* output[] = {"v"};
    ray_t* three = ray_i64(3);
    ray_block_predicate_t pred = {"x", OP_EQ, three};
    ray_block_scan_options_t read = {0, UINT64_MAX, 4096, 1u << 20, 1u << 20, 0};
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(three);
    int64_t seen = 0;
    for (ray_t* b; (b = ray_block_query_next(query)); ray_release(b)) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(b));
        ray_t* col = ray_table_get_col_idx(b, 0);
        for (int64_t i = 0; i < col->len; i++, seen++)
            TEST_ASSERT_EQ_I(((int32_t*)ray_data(col))[i], (int32_t)(3 + 7 * seen));
    }
    TEST_ASSERT_EQ_I(seen, (rows - 3 + 6) / 7);
    ray_block_query_stats_t stats;
    ray_block_query_stats(query, &stats);
    /* 64 x blocks for the filter, 32 v blocks for the output. */
    TEST_ASSERT_EQ_I(stats.scan.blocks_decoded, 64 + 32);
    PASS();
}

static test_result_t query_admission(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"y"};
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 40, 64, 0};
    ray_t* atom = ray_i64(101);
    ray_block_predicate_t pred = {"x", OP_EQ, atom};
    int64_t baseline = mapped();
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 1, &read, &query), RAY_ERR_DOMAIN);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 33, &read, &query), RAY_ERR_LIMIT);
    pred.opcode = OP_ADD;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_NYI);
    pred.opcode = OP_EQ; pred.column = "y";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_TYPE);
    pred.column = "missing";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_SCHEMA);
    pred.column = "../x";
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_DOMAIN);
    pred.column = "x"; atom->type = -RAY_F64;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_ERR_TYPE);
    atom->type = -RAY_I64;
    TEST_ASSERT_TRUE(query == NULL); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
    ray_release(atom);
    ray_t* batch = ray_block_query_next(query);
    TEST_ASSERT_NOT_NULL(batch); TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); ray_release(batch);
    ray_block_query_cancel(query);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "cancel"); ray_release(batch);
    batch = ray_block_query_next(query);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); TEST_ASSERT_STR_EQ(ray_err_code(batch), "domain"); ray_release(batch);
    ray_block_query_close(&query); TEST_ASSERT_EQ_I(mapped(), baseline);
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &query), RAY_OK);
    unsigned rows = 0;
    while ((batch = ray_block_query_next(query))) {
        TEST_ASSERT_FALSE(RAY_IS_ERR(batch)); rows += ray_table_nrows(batch); ray_release(batch);
    }
    TEST_ASSERT_EQ_I(rows, 16); ray_block_query_close(&query);
    read.count = 0;
    TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, NULL, 0, &read, &query), RAY_OK);
    TEST_ASSERT_TRUE(ray_block_query_next(query) == NULL);
    ray_block_query_close(&query); ray_block_query_close(&query);
    ray_block_query_close(NULL); ray_block_query_cancel(NULL);
    batch = ray_block_query_next(NULL);
    TEST_ASSERT_TRUE(RAY_IS_ERR(batch)); ray_release(batch);
    TEST_ASSERT_EQ_I(mapped(), baseline);
    PASS();
}

static test_result_t query_budgets_and_errors(void) {
    publish(1, 16, 16, 1);
    const char* output[] = {"x"};
    char filter_name[] = "y";
    ray_t* atom = ray_i32(999);
    ray_block_predicate_t pred = {filter_name, OP_GT, atom};
    int64_t baseline = mapped();
    ray_block_scan_options_t read = {0, UINT64_MAX, 5, 19, 64, 0};
    for (unsigned which = 0; which < 4; which++) {
        read.payload_limit = which == 0 ? 19 : which == 1 ? 39 : 40;
        read.scratch_limit = which == 2 ? 63 : 64;
        if (which == 3) damage("y", RAY_COL_BLOCK_HEADER);
        TEST_ASSERT_EQ_I(ray_block_query_open(root, output, 1, &pred, 1, &read, &query), RAY_OK);
        filter_name[0] = 'z';
        ray_t* err = ray_block_query_next(query);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err));
        TEST_ASSERT_STR_EQ(ray_err_code(err), which == 3 ? "corrupt" : "limit"); ray_release(err);
        err = ray_block_query_next(query);
        TEST_ASSERT_TRUE(RAY_IS_ERR(err)); TEST_ASSERT_STR_EQ(ray_err_code(err), "domain"); ray_release(err);
        ray_block_query_close(&query); TEST_ASSERT_EQ_I(mapped(), baseline);
        filter_name[0] = 'y';
    }
    ray_release(atom);
    PASS();
}

const test_entry_t block_scan_entries[] = {
    {"block_scan/mixed_raw_scan", mixed_raw_scan, setup, teardown},
    {"block_scan/shared_work_limit", shared_work_limit, setup, teardown},
    {"block_scan/crash_publication", crash_publication, setup, teardown},
    {"block_scan/concurrent_readers", concurrent_readers, setup, teardown},
    {"block_scan/scan_during_write", scan_during_write, setup, teardown},
    {"block_scan/auto_batch_rows", auto_batch_rows, setup, teardown},
    {"block_scan/auto_batch_raw_strings", auto_batch_raw_strings, setup, teardown},
    {"block_scan/flat_immutable", flat_immutable, setup, teardown},
    {"block_scan/query_budgets_and_errors", query_budgets_and_errors, setup, teardown},
    {"block_scan/query_late_materialization", query_late_materialization, setup, teardown},
    {"block_scan/query_conjunction", query_conjunction, setup, teardown},
    {"block_scan/query_native_parity", query_native_parity, setup, teardown},
    {"block_scan/query_zone_pruning", query_zone_pruning, setup, teardown},
    {"block_scan/gate_range_pruning", gate_range_pruning, setup, teardown},
    {"block_scan/query_scattered_matches", query_scattered_matches, setup, teardown},
    {"block_scan/query_admission", query_admission, setup, teardown},
    {"block_scan/subset_variable_width", subset_variable_width, setup, teardown},
    {"block_scan/subset_ranges", subset_ranges, setup, teardown},
    {"block_scan/subset_failures", subset_failures, setup, teardown},
    {"block_scan/subset_payloads", subset_payloads, setup, teardown},
    {"block_scan/store_strings", store_strings, setup, teardown},
    {"block_scan/string_limits", string_limits, setup, teardown},
    {"block_scan/store_symbols", store_symbols, setup, teardown},
    {"block_scan/symbol_failures", symbol_failures, setup, teardown},
    {"block_scan/batches", batches, setup, teardown},
    {"block_scan/failures", failures, setup, teardown},
    {"block_scan/projection", projection, setup, teardown},
    {"block_scan/cancel_after_batch", cancel_after_batch, setup, teardown},
    {"block_scan/schema_membership", schema_membership, setup, teardown},
    {"block_scan/schema_corruption", schema_corruption, setup, teardown},
    {"block_scan/schema_limits", schema_limits, setup, teardown},
    {"block_scan/store_roundtrip", store_roundtrip, setup, teardown},
    {"block_scan/store_stream_chunks", store_stream_chunks, setup, teardown},
    {"block_scan/store_preflight", store_preflight, setup, teardown},
    {"block_scan/store_rollback", store_rollback, setup, teardown},
    {NULL, NULL, NULL, NULL},
};
