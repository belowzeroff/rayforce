# RFC: compressed block storage for splayed and partitioned tables

Status: implemented on `feat/hdb-block-compression` (62 commits from
`2f4605b1`), experimental, 2026-10-16. Not yet rebased on current `dev`.
User documentation: [storage/block.md](docs/storage/block.md).

## Goals

- Store native columns of splayed and partitioned tables in independently
  compressed blocks, chosen per write; raw files keep working unchanged.
- Decode only the blocks a supported query needs: projection, partition-key
  and zone-map pruning, exact row filtering, streaming global aggregates.
- Check integrity of everything read: metadata and per-block checksums on
  read, a separate full `verify`.
- Bound memory: scratch, payload and result budgets per scan; no implicit
  decompression of a whole HDB.
- Atomic publication with reader leases, so a query survives concurrent
  rewrites and generation GC.

Out of scope for v1: a transparent lazy compressed vector for every
`ray_data()` consumer, snapshots across tables, bounded joins/sorts, WAL/IPC
compression, LIST/TABLE columns, codecs other than Snappy.

## Format (column file, major 2)

All integers little-endian, written field by field; big-endian hosts get
`nyi`. A file is `header | block payloads | directory | footer`.

| Region | Bytes | Fields |
|---|---|---|
| Header | 128 | `[17]` major = 2, `[18]` type, `[24]` rows, `[32]` magic `RAYHDB2`, `[40]` header length, `[44]` block bytes (≤ 64 MiB), `[48]` blocks, `[56]` directory offset, `[64]` file size, `[72]` generation, `[80]` directory CRC, `[84]` header CRC, `[88]` SYM dictionary count, `[96]` dictionary CRC, `[100]` required-feature bits, `[104..127]` reserved zero |
| Directory entry | 64 per block | `[0]` first row, `[8]` row count, `[16]` file offset, `[24]` stored bytes, `[28]` decoded bytes, `[32]` codec (0 none, 1 Snappy), `[33]` encoding (1 = STR framing), `[34]` zone flags (present / has null / has value), `[40]` stored CRC, `[44]` decoded CRC, `[48]` min, `[56]` max |
| Footer | 32 | magic `RAYEND2`, blocks, directory offset, file size |

Invariants checked on open, before any allocation: bytes 0..16 zero (old
readers see major 2 and refuse it), row ranges contiguous and summing to the
column length, regions inside the file and not overlapping metadata, codec
none ⇒ stored = decoded and equal CRCs, Snappy ⇒ stored < decoded (a block that
does not shrink is stored raw). An unknown codec or required-feature bit is
`nyi` (newer file), not `corrupt`.

STR blocks hold `[u32 length, bytes]` records; a string never spans blocks.
SYM columns store I64 indexes into a dictionary private to the generation
(`.sym` in the generation directory), bounded and checksummed by the header.
Zone maps exist for integer, boolean and temporal types only.

A table is a directory of generations: `.generations/<g>/` holds the column
files, `.d` and `.sym`; `.current` names the published one and is replaced
atomically after fsync. Readers take a shared `.lease` on their generation;
GC removes only unleased, unpublished generations; writers serialize on
`.write.lock`.

## User contract

| Builtin | Behaviour |
|---|---|
| `.db.block.set dir t [opts]` | Write a generation. Options all optional: `block-bytes` (64 KiB), `codec` (`snappy`/`none`), `durable` (true) |
| `.csv.block`, `.parquet.block` | Streaming imports, one input batch plus one block per column in memory |
| `.db.block.convert src dest [opts [read]]` | Raw or block → block, single table or `root 'table` over all partitions; overlapping paths refused |
| `.db.block.scan dir bytes [opts]`, `.db.block.parted root 't bytes [opts]` | Scan descriptors for `select`; result, payload, scratch and work budgets |
| `.db.storage.info`, `.db.storage.verify` | Metadata report; full decode with zone-map recheck, failing with partition/column/block |
| `.db.splayed.set dir t [sym] opts` | Writes a block table when given options |
| `.db.splayed.get dir [sym] {materialize: bytes}` | Explicit bounded load; without it a block table is a `version` error naming the scan builtins |

Supported `select`: bare columns or global `count`/`sum`/`avg`/`min`/`max`;
`where` a conjunction of comparisons of a column with a literal (integers,
booleans, temporals; `==`/`!=` for symbols); the virtual `date`/`part` key on
partitioned scans. Everything else is `nyi` rather than a hidden full
materialization.

## Measurements

100M trade-like rows, 20 partitions, 64 KiB blocks, release build, warm
medians (`bench/block_hdb/results/2026-10-16-full-e9cd68da.json` and later
spot checks on 2×Xeon E5-2690, HDD RAID):

| Query | raw | none | Snappy |
|---|---|---|---|
| One minute of one day | 244 ms | 13 ms | 15 ms |
| One minute across all days | 125 ms | 21 ms | 22 ms |
| One column of one day | 206 ms | 107 ms | 112 ms |
| 0.5% of one day, six columns | 236 ms | 200–440 ms | 480–490 ms |
| Full scan, four aggregates | 332 ms | 314 ms | 604 ms |
| Full scan, cold | 1529 ms | 2441 ms | 1136 ms |
| Disk | 7.36 GiB | 5.92 GiB | 2.56 GiB |

Release gates: zone-map pruning decodes only candidate blocks (gate 2);
SIGKILL during publication and readers racing publication and GC
(gate 4, TSan clean); raw paths 1.02 / 1.01 of base over 45 runs
(gate 6, limit 1.05). 64 KiB beat 256 KiB on every query for 4% more disk.
A bit-packing codec failed its gate on realistic data (size 0.62, timestamps
0.67 of Snappy, threshold 0.5) and is deferred.

## Decisions

| Topic | Decided | Open for maintainers |
|---|---|---|
| Codec | Native Snappy, raw fallback per block, no dependencies | Default `snappy` vs `none` |
| Names | `.db.block.*`, `.db.storage.*` | Final names; whether scans belong under `.db.splayed`/`.db.parted` |
| Read API | Explicit descriptors, refusal of unsupported plans, opt-in bounded materialize | — |
| Format evolution | Major 2, required-feature bits, `nyi` for newer files | — |
| Lifetime | Leases, non-blocking GC, generation-local SYM dictionary | — |
| Types | Fixed width, GUID, SYM, STR | LIST/TABLE policy (refuse today) |
| Platforms | Linux x86-64 tested; big-endian refused | macOS run pending; Windows (locks, leases) |
| Migration | Per-partition convert, failure names the partition, rerun republishes | Checkpoint/resume beyond rerun |

## Proposed PR split

Each PR builds and passes the full suite on its own; later PRs only add.

1. **Snappy**: extract the Parquet decoder into `src/core/snappy.c`, add the
   encoder and fast loop, PCLMUL CRC-32; `test_snappy`, `fuzz_snappy`,
   reference oracle script. No format or behaviour change.
2. **Container**: `col_block*.c` reader/writer including STR framing and
   SYM dictionary fields (the format is complete before fixtures are fixed),
   `nyi` for newer files, raw loader `version` error; `test_col_block`,
   `fuzz_col_block` with seeds, nightly entries.
3. **Publication and leases**: `splay.c` leases and GC, `block_store.c`
   atomic streaming writer; crash and concurrent-reader tests.
4. **Scan and select**: `block_scan.c`, `block_query.c`, scan builtins,
   partitions, zone maps, budgets, parallel decode and aggregates.
5. **Writers and conversion**: CSV/Parquet block sinks, convert, `.db.block.set`,
   `.db.splayed.set/get` integration.
6. **Operations**: info/verify, user docs, benchmarks and results.

The test files (`test_block_scan.c`, `test_block_source.c`) are split along
the same lines when the PRs are cut. Cutting starts after the branch is
rebased on current `dev` and gate 6 is repeated there.
