#!/usr/bin/env python3
"""Partitioned HDB: raw mmap versus compressed block tables (codec none, snappy).

    bench.py RAYFORCE DATA_DIR [--days N] [--rows N] [--block-bytes B]
             [--repeat R] [--out results.json]

RAYFORCE is a release build. DATA_DIR receives raw/ (flat .db.splayed.set
partitions sharing raw/.sym), none/ and snappy/ (.db.block.convert of every
partition); existing data of the same shape is reused. Data is deterministic:
glibc rand() with its default seed, one process per day.

Every query runs in a fresh process, R times warm (after one untimed run) and
R times cold. "Cold" evicts the variant's files with posix_fadvise(DONTNEED)
before the run: clean pages of these files only, not a system-wide drop, so
metadata and other caches stay warm. Timings are the query's own (timeit);
I/O is the child's block input count, RSS its peak. Results of every variant
must agree, or the run fails.
"""
import argparse, datetime, json, os, platform, re, resource, statistics, subprocess, sys, tempfile, time

DAY0 = (2024, 1, 2)
VARIANTS = ("raw", "none", "snappy")


def day(i):
    return datetime.date(*DAY0) + datetime.timedelta(days=i)


def day_name(i):
    return day(i).strftime("%Y.%m.%d")


def day_ns(i):
    """Nanoseconds from the 2000.01.01 epoch to midnight of day i."""
    return (day(i) - datetime.date(2000, 1, 1)).days * 86400 * 10**9


def timestamp(i, seconds):
    t = datetime.datetime.combine(day(i), datetime.time()) + datetime.timedelta(seconds=seconds)
    return t.strftime("%Y.%m.%dD%H:%M:%S.000000000")


def run_rfl(binary, script, cwd=None):
    with tempfile.NamedTemporaryFile("w", suffix=".rfl", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        p = subprocess.run([binary, path], capture_output=True, text=True, cwd=cwd)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
    finally:
        os.unlink(path)
    out = p.stdout + p.stderr
    if p.returncode != 0 or "error:" in out:
        raise RuntimeError(f"rayforce failed ({p.returncode}):\n{script}\n{out}")
    return out, (after.ru_inblock - before.ru_inblock) * 512, after.ru_maxrss


def files_under(root):
    for d, _, names in os.walk(root):
        for n in names:
            yield os.path.join(d, n)


def disk_bytes(root):
    return sum(os.lstat(p).st_size for p in files_under(root))


def evict(root):
    for p in files_under(root):
        try:
            fd = os.open(p, os.O_RDONLY)
        except OSError:
            continue
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


GEN = """
(set n {rows})
(set d {day})
(set ts (as 'TIMESTAMP (+ {day_ns} (+ (* {step} (til n)) (rand n {step})))))
(set syms [{symbols}])
(set t (table [ts sym px size noise note]
  (list ts
        (at syms (rand n {nsyms}))
        (+ 100000 (rand n 5000))
        (+ 1 (rand n 1000))
        (/ (as 'F64 (rand n 1000000000)) 7.0)
        (at (list "fill" "partial fill, venue routed" "cancel" "a considerably longer free text annotation") (rand n 4)))))
(.db.splayed.set "{root}/{name}/t/" t "{root}/.sym")
"""


def generate(args, binary):
    raw = os.path.join(args.data, "raw")
    shape = {"days": args.days, "rows": args.rows}
    marker = os.path.join(args.data, "shape.json")
    if os.path.exists(marker) and json.load(open(marker)) == shape:
        return None
    if os.path.exists(raw) and os.listdir(raw):
        sys.exit(f"{args.data} holds data of another shape; choose an empty DATA_DIR")
    symbols = " ".join(f"S{i:03d}" for i in range(500))
    t0 = time.time()
    for i in range(args.days):
        # Each day is a fresh process restarting rand() at its default seed:
        # advance it by a day-dependent count so days hold different values.
        script = f"(rand {i + 1} 2)\n" + GEN.format(
            rows=args.rows, day=i, day_ns=day_ns(i) + 9 * 3600 * 10**9,
            step=max(1, 23400 * 10**9 // args.rows),  # 09:00 to 15:30
            symbols=symbols, nsyms=500, root=raw, name=day_name(i))
        run_rfl(binary, script)
    gen_seconds = time.time() - t0
    os.sync()
    encode = {}
    for codec in ("none", "snappy"):
        t0 = time.time()
        for i in range(args.days):
            run_rfl(binary, f'(.db.block.convert "{raw}/{day_name(i)}/t" "{args.data}/{codec}/{day_name(i)}/t" '
                            f'{{block-bytes: {args.block_bytes} codec: {codec} durable: false}} {{immutable: true}})\n')
        os.sync()
        encode[codec] = time.time() - t0
    json.dump(shape, open(marker, "w"))
    return {"generate_seconds": gen_seconds, "convert_seconds": encode}


def source(args, variant):
    if variant == "raw":
        return f'(.db.parted.get "{args.data}/raw" (quote t))'
    return f'(.db.block.parted "{args.data}/{variant}" (quote t) 100000000000)'


def queries(args):
    # Block selects admit literals only: compare with timestamp literals.
    middle = args.days // 2
    mid = day_name(middle)
    lo, hi = timestamp(middle, 13 * 3600), timestamp(middle, 13 * 3600 + 60)
    return [
        ("full_aggregate", "n: (count px) total: (sum size) lo: (min px) hi: (max px)"),
        ("day_time_range", f"where: (and (== date {mid}) (and (>= ts {lo}) (< ts {hi}))) n: (count px) total: (sum px)"),
        ("time_range_all_days", f"where: (and (>= ts {lo}) (< ts {hi})) n: (count px) total: (sum px)"),
        ("one_day_one_column", f"where: (== date {mid}) p: px"),
        ("one_day_selective_wide", f"where: (and (== date {mid}) (> size 995)) t: ts k: sym p: px s: size x: noise c: note"),
        ("avg_all_days", "a: (avg size) n: (count size)"),
    ]


def summary_expr(name):
    # Aggregates print their single row; projections a row count and checksum.
    if name in ("one_day_one_column", "one_day_selective_wide"):
        return "(println (count r)) (println (sum (at r 'p)))"
    return "(show r)"


def bench(args, binary):
    results = []
    reference = {}
    for qname, clause in queries(args):
        for variant in VARIANTS:
            script = (f"(set src {source(args, variant)})\n"
                      f"(println (timeit (set r (select {{from: src {clause}}}))))\n"
                      f"{summary_expr(qname)}\n")
            run_rfl(binary, script)  # untimed: warm the page cache
            entry = {"query": qname, "variant": variant, "warm_ms": [], "cold_ms": [],
                     "cold_read_bytes": [], "max_rss_kib": 0}
            for mode in ("warm", "cold"):
                for _ in range(args.repeat):
                    if mode == "cold":
                        evict(os.path.join(args.data, variant))
                    out, read, rss = run_rfl(binary, script)
                    lines = out.strip().splitlines()
                    entry[f"{mode}_ms"].append(float(lines[0]))
                    if mode == "cold":
                        entry["cold_read_bytes"].append(read)
                    entry["max_rss_kib"] = max(entry["max_rss_kib"], rss)
                    answer = "\n".join(lines[1:])
                    if reference.setdefault(qname, answer) != answer:
                        raise RuntimeError(f"{qname}: {variant} differs\n{reference[qname]}\n---\n{answer}")
            results.append(entry)
            print(f"{qname:24s} {variant:7s} warm {statistics.median(entry['warm_ms']):9.1f} ms  "
                  f"cold {statistics.median(entry['cold_ms']):9.1f} ms  "
                  f"read {statistics.median(entry['cold_read_bytes']) / 2**20:8.1f} MiB", flush=True)
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("rayforce")
    ap.add_argument("data")
    ap.add_argument("--days", type=int, default=20)
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--block-bytes", type=int, default=65536)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--out")
    args = ap.parse_args()
    binary = os.path.abspath(args.rayforce)
    with open(binary, "rb") as f:
        if b"__asan_init" in f.read() or b"__tsan_init" in f.read():
            sys.exit(f"{binary} is a sanitizer build; benchmark a release build (make release)")
    args.data = os.path.abspath(args.data)
    os.makedirs(args.data, exist_ok=True)
    build = generate(args, binary)
    sizes = {v: disk_bytes(os.path.join(args.data, v)) for v in VARIANTS}
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                            cwd=os.path.dirname(os.path.abspath(__file__))).stdout.strip()
    report = {
        "commit": commit, "binary": binary, "cpu": platform.processor() or platform.machine(),
        "logical_cpus": os.cpu_count(), "days": args.days, "rows_per_day": args.rows,
        "block_bytes": args.block_bytes, "repeat": args.repeat,
        "page_cache": "warm: after one untimed run; cold: posix_fadvise(DONTNEED) on the variant's files",
        "timing": "in-process timeit of the select, fresh process per run",
        "disk_bytes": sizes, "build": build, "results": bench(args, binary),
    }
    for v in VARIANTS:
        print(f"{v:7s} {sizes[v] / 2**30:7.2f} GiB  ratio vs raw {sizes['raw'] / sizes[v]:5.2f}")
    if args.out:
        json.dump(report, open(args.out, "w"), indent=2)


if __name__ == "__main__":
    main()
