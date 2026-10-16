# Compressed Block Tables

!!! warning "Experimental"
    Block tables, their builtins and the on-disk format (major version 2) are experimental. Names, defaults and the supported query subset may change.

A **block table** stores each column as a sequence of independently compressed blocks instead of one flat, memory-mapped vector. Each block carries CRC-32 checksums of its stored and decoded bytes, and integer and temporal blocks carry min/max statistics ("zone maps"). Tables are published atomically as generations, exactly like staged splayed writes, and readers hold a lease on the generation they read.

Compared with raw splayed and partitioned tables:

- **Smaller on disk.** Snappy typically stores market data at a fraction of its raw size (2.9× in the benchmark below).
- **Faster selective queries.** Zone maps let a filtered read skip whole blocks without reading or decoding them, and partition predicates skip whole partitions before any of their files are opened.
- **Slower full scans.** Every byte read is decoded and checksummed. Decoding and global aggregates run on the native pool, but a full scan still costs more CPU than reading mmapped columns.
- **Not a drop-in replacement.** Block tables are read through explicit scan descriptors that support a bounded `select` subset. `.db.splayed.get` and `.db.parted.get` refuse them with a `version` error that names these descriptors; `(.db.splayed.get "dir" {materialize: bytes})` loads a whole table into memory when that is really wanted.

## Writing

| Function | Description |
|---|---|
| `(.db.block.set "dir" table [options])` | Save an in-memory table as a new generation of `dir`. Returns the table. |
| `(.csv.block [names types] "file.csv" "dir" [options])` | Stream a CSV file into a block table; the same parse and schema arguments as `.csv.splayed`. Returns the row count. |
| `(.parquet.block [read-options] "file.parquet" "dir" [options])` | Stream a Parquet file into a block table; `read-options` are those of `.parquet.read`. Returns the row count. |
| `(.db.block.convert "src" "dest" [options [read-options]])` | Rewrite an existing raw or block table as a block table at `dest`. Returns a table of `rows` with the effective `codec` and `block_bytes`. |

`options` is a dictionary whose keys are all optional; without it, or with `{}`, blocks are 64 KiB Snappy and durable. These defaults won on the benchmark below against 256 KiB blocks on full, narrow and selective scans. The existing `.db.splayed.set` and CSV/Parquet splayed writers stay raw.

| Key | Type | Meaning |
|---|---|---|
| `block-bytes` | `I64` | Default 65536. Uncompressed bytes per block, at most 64 MiB. Fixed-width columns hold `block-bytes / width` rows per block; a string must fit in one block. |
| `codec` | `SYM` | Default `snappy`; or `none`. Snappy stores a block raw when compression does not shrink it. |
| `durable` | `BOOL` | Optional, default `true`: fsync files and directories before publishing. |

Supported column types are `B8`, `U8`, `I16`, `I32`, `I64`, `F32`, `F64`, `DATE`, `TIME`, `TIMESTAMP`, `GUID`, `SYM` and `STR`. Symbols are stored in a dictionary private to the generation. Column attributes are not stored: a column carrying `sorted` (or link and index attributes) is rejected with `nyi` rather than silently losing them.

Writes stream: CSV and Parquet imports and conversions hold one input batch plus one block per column, never the whole table. A failed write publishes nothing and leaves the previous generation readable.

```lisp
(set t (table [ts sym px size]
  (list (as 'TIMESTAMP [1000 2000 3000 4000])
        ['AAPL 'MSFT 'AAPL 'IBM]
        [101 102 103 104]
        [10 20 30 40])))
(.db.block.set "/tmp/rayforce-block-doc/2024.01.02/trades" t {block-bytes: 65536 codec: snappy})
(.db.block.set "/tmp/rayforce-block-doc/2024.01.03/trades" t {block-bytes: 65536 codec: snappy})
```

### Converting existing data

`.db.block.convert` reads its source through a block scan of every column and writes a new generation of `dest`. `dest` may not be the source, inside it, or contain it: the source is never modified.

A whole partitioned database converts with `(.db.block.convert "src-root" 'table "dest-root" options [read-options])`: every partition `src-root/<p>/table` becomes `dest-root/<p>/table`, in partition order, and the result lists each partition's row count with the effective codec and block size. Raw `SYM` columns default to `src-root/.sym`. A failure names its partition; partitions converted before it stay published, and running the command again republishes every partition.

`read-options` describe the source:

| Key | Type | Meaning |
|---|---|---|
| `immutable` | `BOOL` | Read a flat directory written by `.db.splayed.set` (no generations, no reader lease). You promise that nothing rewrites it while it is read. |
| `sym` | `STR` | Symbol file of raw `SYM` columns. For a partition path `root/<partition>/<table>` it defaults to `root/.sym`. |

```text
(.db.block.convert "/data/raw/2024.01.02/trades" "/data/block/2024.01.02/trades"
  {block-bytes: 65536 codec: snappy durable: true} {immutable: true})
```

## Loading into memory

`(.db.splayed.get "dir" [sym_path] {materialize: bytes})` reads every row of a block or raw table through a block scan into an ordinary in-memory table and fails with `limit` once its logical size would exceed `bytes`. Nothing is decompressed implicitly: without the option, compressed columns are refused. `(.db.splayed.set "dir" table [sym_path] options)` with an options dictionary writes a block table, as `.db.block.set`; its symbols go to the generation's own dictionary, not `sym_path`.

## Reading

Block tables are read with `select` over a scan descriptor:

| Function | Description |
|---|---|
| `(.db.block.scan "dir" result-bytes [options])` | One table. |
| `(.db.block.parted "root" 'table result-bytes [options])` | A partitioned database `root/<partition>/table`. Partitions may mix block generations and flat raw tables. |

`result-bytes` caps the logical size of the result. Scan `options`:

| Key | Default | Meaning |
|---|---|---|
| `batch-rows` | automatic | Rows decoded per read. By default half the payload limit over the projection's row width, no more rows than `scratch-bytes` holds of decoded strings, at most 1Mi. |
| `payload-bytes` | 64 MiB | Decoded bytes per read. |
| `scratch-bytes` | 8 MiB | Temporary decode memory per read. String columns decode a read's blocks once into it when they fit, otherwise twice through one block; results are the same. |
| `work-bytes` | none | Optional cap on retained result plus the current read, masks and aggregate state. |
| `immutable` | `false` | Admit flat raw directories, as for conversion. |

The supported `select` is deliberately narrow; anything else fails with `nyi` instead of materializing the source:

- Outputs are either bare columns (with aliases) or only global aggregates: `count` of any column, `sum` and `avg` of integers and `F64`, `min` and `max` of integers, `F64`, booleans and temporals, with the native engine's null and NaN semantics.
- `where` is a conjunction (`and`) of `==`, `!=`, `<`, `<=`, `>`, `>=` between a column and a literal of the same integer, boolean or temporal type, or `==` and `!=` between a `SYM` column and a symbol literal (`'AAPL`). Symbol filters read every block; only integer and temporal columns have zone maps.
- No `by`, `take`, `asc`, `desc`, string or float predicates, or ordering of symbols; they fail with `nyi` rather than materializing the source.
- As with `.db.parted.get` selects, a column missing from some partitions of a partitioned table fails (`schema`) instead of reading as nulls.

A partitioned scan adds the virtual key column `date` (for `YYYY.MM.DD` partitions) or `part`. It can be projected, aggregated and compared; comparisons on it skip partitions before their files are opened. Partitions are listed once and read one at a time, each under its own lease, so there is no snapshot across partitions. The first partition defines the column schema, as for `.db.parted.get`. Symbols from different partitions are returned in the runtime symbol table.

```lisp
(set trades (.db.block.parted "/tmp/rayforce-block-doc" 'trades 100000000))
(select {from: trades where: (and (== date 2024.01.03) (> px 102)) t: ts k: sym p: px})
(select {from: trades n: (count px) total: (sum size) hi: (max px)})
```

## Inspecting and verifying

| Function | Description |
|---|---|
| `(.db.storage.info "dir" [read-options])` | One row per column: `column`, `type`, `format` (`raw`/`block`), `codec` (`none`/`snappy`/`mixed`), `rows`, `blocks`, `block_bytes`, `stored`, `logical`, `ratio`. Reads metadata only. |
| `(.db.storage.verify "dir" [read-options])` | Decodes every block, checking both checksums, string framing, symbol bounds and that zone statistics describe the data. Returns blocks checked per column, or a `corrupt` error naming the column and block. |

Both also accept a partitioned database, `(.db.storage.info "root" 'table [read-options])`: they report every partition `root/<partition>/table` with a leading `partition` column, raw `SYM` columns default to the root's `.sym`, and a corrupt block is reported with its partition.

Ordinary reads check each block's checksums the first time it is decoded through an open file, and the directory and header checksums on open. `verify` is the full check to run after copying or restoring data.

```lisp
(.db.storage.info "/tmp/rayforce-block-doc/2024.01.02/trades")
(.db.storage.verify "/tmp/rayforce-block-doc/2024.01.02/trades")
```

## Performance

`bench/block_hdb/bench.py` compares a raw partitioned database with block tables (codec `none` and `snappy`, 64 KiB blocks) on 100 million trade-like rows over 20 days; see `bench/block_hdb/results/` for complete reports including cold-cache runs. Warm medians on a shared 2×Xeon E5-2690 server:

| Query | raw | block none | block snappy |
|---|---|---|---|
| One minute of one day | 244 ms | 13 ms | 15 ms |
| One minute across all days | 125 ms | 21 ms | 22 ms |
| One column of one day (5M rows) | 206 ms | 107 ms | 112 ms |
| 0.5% of one day, six columns | 236 ms | 200–440 ms | 480–490 ms |
| Full scan, four aggregates | 332 ms | 314 ms | 604 ms |
| Full scan, cold cache | 1529 ms | 2441 ms | 1136 ms |
| Disk | 7.36 GiB | 5.92 GiB | 2.56 GiB |

Selective and time-range queries benefit most. Full scans decode in parallel and keep up with raw scans when warm; cold, Snappy wins by reading a seventh of the bytes. Scattered matches across wide rows decode whole blocks of every projected column, though only matching rows are built; with Snappy that decoding keeps them behind raw, which reads only the rows it needs. Zone maps prune within a read at block granularity, so large reads keep narrow ranges cheap.

## Errors

| Code | Meaning |
|---|---|
| `nyi` | Unsupported query shape, a flat source without `immutable: true`, a column attribute blocks cannot keep, or a file using a codec or format feature newer than this reader. |
| `limit` | A read or the result exceeds its configured byte limit. |
| `schema` | Partitions or columns disagree in names or types. |
| `corrupt` | A checksum, structure or statistic does not match. |
| `access` | Called in restricted mode. |
