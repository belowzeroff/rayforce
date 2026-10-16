#!/usr/bin/env python3
"""Raw-path regression check: the same raw workloads timed with two builds.

    raw_regression.py BASE_BINARY HEAD_BINARY RAW_HDB CSV PARQUET [--repeat N]

Workloads: the six block benchmark queries over the raw partitioned HDB,
.csv.splayed of CSV into a fresh directory, and .parquet.read of PARQUET.
Builds alternate run by run, in alternating order, so drift on a shared host
hits both; each query process times three runs. Reported are medians of
in-process timeit and HEAD/BASE ratios. Gate: every ratio <= 1.05.
"""
import argparse, os, statistics, subprocess, tempfile

QUERIES = [
    ("full_aggregate", "n: (count px) total: (sum size) lo: (min px) hi: (max px)"),
    ("day_time_range", "where: (and (== date 2024.01.12) (and (>= ts 2024.01.12D13:00:00.000000000) (< ts 2024.01.12D13:01:00.000000000))) n: (count px) total: (sum px)"),
    ("time_range_all_days", "where: (and (>= ts 2024.01.12D13:00:00.000000000) (< ts 2024.01.12D13:01:00.000000000)) n: (count px)"),
    ("one_day_one_column", "where: (== date 2024.01.12) p: px"),
    ("one_day_selective_wide", "where: (and (== date 2024.01.12) (> size 995)) t: ts k: sym p: px s: size x: noise c: note"),
    ("avg_all_days", "a: (avg size) n: (count size)"),
]


def run(binary, script):
    with tempfile.NamedTemporaryFile("w", suffix=".rfl", delete=False) as f:
        f.write(script)
    try:
        out = subprocess.run([binary, f.name], capture_output=True, text=True)
    finally:
        os.unlink(f.name)
    if out.returncode or "error:" in out.stdout + out.stderr:
        raise RuntimeError(out.stdout + out.stderr)
    return [float(x) for x in out.stdout.split()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base"); ap.add_argument("head"); ap.add_argument("hdb")
    ap.add_argument("csv"); ap.add_argument("parquet")
    ap.add_argument("--repeat", type=int, default=15)
    args = ap.parse_args()
    work = tempfile.mkdtemp(prefix="raw-regression-")
    workloads = [(name, f'(set src (.db.parted.get "{args.hdb}" (quote t)))\n'
                        f'(select {{from: src {clause}}})\n'
                        + f'(println (timeit (select {{from: src {clause}}})))\n' * 3) for name, clause in QUERIES]
    workloads.append(("csv_splayed", None))
    workloads.append(("parquet_read",
                      f'(.parquet.read "{args.parquet}")\n(println (timeit (.parquet.read "{args.parquet}")))\n'))
    worst = 0.0
    for name, script in workloads:
        times = {"base": [], "head": []}
        for i in range(args.repeat):
            for label in ("base", "head") if i % 2 else ("head", "base"):
                binary = getattr(args, label)
                if name == "csv_splayed":
                    script = f'(println (timeit (.csv.splayed "{args.csv}" "{work}/{label}/")))\n'
                times[label] += run(binary, script)
        b, h = statistics.median(times["base"]), statistics.median(times["head"])
        worst = max(worst, h / b)
        print(f"{name:24s} base {b:9.1f} ms  head {h:9.1f} ms  head/base {h / b:5.2f}", flush=True)
    print(f"worst head/base {worst:.2f}", "PASS" if worst <= 1.05 else "FAIL")


if __name__ == "__main__":
    main()
