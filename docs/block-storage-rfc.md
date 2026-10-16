# RFC: compressed block storage for splayed and partitioned tables

Status: implemented on `feat/hdb-block-compression` (branched from
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

## Non-goals for v1

- A transparent lazy compressed vector for every `ray_data()` consumer.
- `by`, `take`, sorting, joins; predicates on `STR` and `F64`; ordering of
  symbols. These are `nyi` on block scans.
- Zone maps for `SYM`, `STR` and `F64` columns.
- Per-column codec or block size; codecs other than Snappy (a bit-packing
  codec failed its gate, see Measurements).
- LIST and TABLE columns (refused at write).
- Checkpointed conversion beyond "rerun republishes every partition".
- Snapshots across tables or partitions; WAL/IPC compression.
- Windows and network file systems (see Leases).

## Compatibility

Nothing changes for existing data or calls. Only `.db.block.set`,
`.csv.block`, `.parquet.block`, `.db.block.convert` and `.db.splayed.set` *with
an options dictionary* create block generations. `.db.splayed.set` without
options, `.csv.splayed`, `.parquet.splayed` and every other writer stay flat
raw, byte for byte. Raw tables are read as before; the raw publication path is
untouched (gate 6 below).

The step is one way. A binary from before this work resolves the generation
layout, which already existed for staged writes, but refuses each block
column (`version: col <path>: bad format version`, checked with the base
binary on the benchmark database) instead of misreading it. Downgrade means
loading with `{materialize: bytes}` and saving raw.

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
`nyi` (newer file), not `corrupt`. Zone maps exist for integer, boolean and
temporal types only.

STR blocks hold `[u32 length, bytes]` records; a string never spans blocks.

SYM columns store 64-bit positions into a dictionary private to the
generation (`.sym` in the generation directory, an `STRL` file whose position
0 is the reserved empty symbol; the header's dictionary count includes it and
the header CRC covers the file). The root `.sym` is used only by raw
partitions. A partitioned scan resolves each partition through its own
dictionary and returns runtime symbols, so mixed raw and block partitions
compare by name, never by position.

## Generations and leases

A table is a directory of generations: `.generations/<g>/` holds the column
files, `.d`, `.sym` and `.lease`; `.current` names the published one.
Locks are advisory OS file locks (`flock`; `LockFileEx` on Windows), released
by the kernel when a process dies, so there are no stale leases to clean.

1. A writer takes `.write.lock` exclusively, writes a new generation, fsyncs
   files and directories, and renames `.current.tmp` over `.current`.
2. A reader takes `.write.lock` shared, reads `.current`, takes its
   generation's `.lease` shared, drops the root lock, and keeps the lease for
   the whole scan.
3. GC, after publishing, removes a generation only if it is not current and
   a non-blocking exclusive lock on its `.lease` succeeds.

Windows (rename over open or mapped files, lock semantics) and NFS (`flock`
is not reliable there) are untested and not claimed.

## User contract

| Builtin | Behaviour |
|---|---|
| `.db.block.set dir t [opts]` | Write a generation. Options all optional: `block-bytes` (64 KiB), `codec` (`snappy`/`none`), `durable` (true) |
| `.csv.block`, `.parquet.block` | Streaming imports, one input batch plus one block per column in memory |
| `.db.block.convert src dest [opts [read]]` | Raw or block → block, single table or `root 'table` over all partitions; overlapping paths refused |
| `.db.block.scan dir bytes [opts]`, `.db.block.parted root 't bytes [opts]` | Scan descriptors for `select`; result, payload, scratch and work budgets |
| `.db.storage.info`, `.db.storage.verify` | Metadata report; full decode with zone-map recheck, failing with partition/column/block |
| `.db.splayed.set dir t [sym] opts` | Writes a block table when given options |
| `.db.splayed.get dir [sym] {materialize: bytes}` | Explicit bounded load |

Supported `select`: bare columns or global `count`/`sum`/`avg`/`min`/`max`;
`where` a conjunction of comparisons of a column with a literal (integers,
booleans, temporals; `==`/`!=` for symbols); the virtual `date`/`part` key on
partitioned scans. Everything else fails rather than materializing.

### Errors

| Code | Cases |
|---|---|
| `nyi` | Unsupported `select` shape; file with a newer codec or feature bit; big-endian host; flat source without `immutable: true`; column attribute that blocks cannot keep |
| `version` | A raw loader (`.db.splayed.get`/`.db.parted.get` without `materialize`) meets a block column: `col <path>: compressed block column; read it with .db.block.scan or .db.block.parted, or load it with (.db.splayed.get dir {materialize: bytes})`. An old binary reports `bad format version` |
| `limit` | A read or the result exceeds its byte budget |
| `schema` | Partitions or columns disagree, including ragged partitions |
| `corrupt` | Checksum, structure or statistic mismatch; `verify` names partition, column and block |

`nyi` deliberately covers both "this reader is too old" and "this query is
not supported yet": in both cases the data is fine and a newer build may
accept it. Argument errors keep the usual `arity`/`type`/`domain` codes with
the builtin name first.

## Measurements

100M trade-like rows, 20 partitions, 64 KiB blocks, release build, warm
medians on a shared 2×Xeon E5-2690 with HDD RAID
(`bench/block_hdb/results/2026-10-16-full-e9cd68da.json`; the selective row is a
separate 35-run measurement after masked materialization):

| Query | raw | none | Snappy |
|---|---|---|---|
| One minute of one day | 244 ms | 13 ms | 15 ms |
| One minute across all days | 125 ms | 21 ms | 22 ms |
| One column of one day | 206 ms | 107 ms | 112 ms |
| 0.5% of one day, six columns | 152 ms | 145 ms | 170 ms |
| Full scan, four aggregates | 332 ms | 314 ms | 604 ms |
| Full scan, cold | 1529 ms | 2441 ms | 1136 ms |
| Disk | 7.36 GiB | 5.92 GiB | 2.56 GiB |

The selective query's 90th percentile reached 300–340 ms for block tables
(178 ms raw) under the server's background load (load average ≈ 4.5).

Unexplained: cold full scan with codec none is slower than raw although it
reads fewer bytes. The likely cause is the read pattern (workers fault 64 KiB
blocks of different regions at once on HDD, while a raw scan gets sequential
readahead), but it has not been measured; Snappy, reading a seventh of the
bytes, is faster than raw cold.

Release gates: zone-map pruning decodes only candidate blocks (gate 2);
SIGKILL during publication and readers racing publication and GC
(gate 4, TSan clean); raw paths 1.02 / 1.01 of base over 45 runs
(gate 6, limit 1.05). 64 KiB beat 256 KiB on every query for 4% more disk.
A bit-packing codec failed its gate on realistic data (size 0.62, timestamps
0.67 of Snappy, threshold 0.5) and is deferred.

## Decisions

| Topic | Implemented | Open for maintainers |
|---|---|---|
| Codec | Native Snappy, raw fallback per block, no dependencies | — |
| Defaults | 64 KiB, Snappy, durable | May change the default codec to `none` |
| Names | `.db.block.*`, `.db.storage.*` | Final names; whether scans belong under `.db.splayed`/`.db.parted` |
| Read API | Explicit descriptors, refusal of unsupported plans, opt-in bounded materialize | — |
| Format evolution | Major 2, required-feature bits, `nyi` for newer files | — |
| Lifetime | Advisory leases, non-blocking GC, generation-local SYM dictionary | — |
| Types | Fixed width, GUID, SYM, STR | LIST/TABLE policy (refused today) |
| Platforms | Linux x86-64 tested; big-endian refused | macOS run pending; Windows, NFS |
| Migration | Per-partition convert, failure names the partition, rerun republishes | Checkpoint/resume beyond rerun |

## Proposed PR split

Each PR builds and passes the full suite on its own; later PRs only add.

1. **Snappy**: extract the Parquet decoder into `src/core/snappy.c`, add the
   encoder and fast loop; `test_snappy`, `fuzz_snappy`, reference oracle
   script. No format or behaviour change.
2. **CRC-32 fold** (independent of 1): PCLMULQDQ folding behind
   `__builtin_cpu_supports("pclmul")`/`sse4.1`, table version as fallback and
   as `ray_crc32_portable`; cross-test against it. Touches every CRC user, so
   it is reviewed on its own.
3. **Container**: `col_block*.c` reader/writer including STR framing and
   SYM dictionary fields, `nyi` for newer files, raw loader `version` error;
   `test_col_block`, `fuzz_col_block` with seeds, nightly entries. Commits
   small golden major-2 files (each fixed-width type, STR, SYM, a zoned block,
   a file with a feature bit) with a test that they read back, that a raw
   major-0 fixture still loads, and that the feature-bit file is `nyi`.
4. **Publication and leases**: `splay.c` leases and GC, `block_store.c`
   atomic streaming writer; crash and concurrent-reader tests. The PR states
   that raw table publication does not change.
5. **Scan and select**: `block_scan.c`, `block_query.c`, scan builtins,
   partitions, zone maps, budgets, parallel decode and aggregates.
6. **Writers and conversion**: CSV/Parquet block sinks, convert, `.db.block.set`,
   `.db.splayed.set/get` integration.
7. **Operations**: info/verify, user docs, benchmarks and results.

The test files (`test_block_scan.c`, `test_block_source.c`) are split along
the same lines when the PRs are cut. Cutting starts after the branch is
rebased on current `dev` and gate 6 is repeated there.
