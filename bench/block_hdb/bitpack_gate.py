#!/usr/bin/env python3
"""Offline gate for an integer bit-packing block codec, without a format.

    bitpack_gate.py RAYFORCE WORK_DIR [--rows N] [--seed S]

Generates one realistic trading day (Python random, fixed seed):
  px    random walk of one-tick steps with rare jumps of hundreds of ticks
  size  lognormal lots of 100 with a Pareto tail
  ts    Poisson bursts (many equal timestamps) between pauses, nanoseconds
  sym   Zipf over 500 tickers, as dictionary positions
For each 128-value mini-block it measures frame-of-reference and delta bit
widths, and counts 8192-row blocks (64 KiB of I64) that a v1 packer without
exceptions would leave unpacked: any mini-block needing 64 bits. Snappy sizes
come from writing the same day with .csv.block and reading .db.storage.info.
The gate: per column the chosen mode's width fits the target in >= 90% of
mini-blocks, fallback blocks < 10%, and packed bytes <= 0.5x Snappy.
"""
import argparse, collections, math, os, random, re, subprocess, sys

TARGET = {"px": 16, "size": 16, "sym": 16, "ts": 24}
BLOCK_ROWS = 8192
MINI = 128


def generate(rows, seed):
    rnd = random.Random(seed)
    px, size, ts, sym = [], [], [], []
    price = 100000
    t = 9 * 3600 * 10**9
    weights = [1 / (k + 1) ** 1.1 for k in range(500)]
    total = sum(weights)
    cdf, acc = [], 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)
    burst = 0
    for _ in range(rows):
        price += rnd.choice((-1, 0, 0, 1))
        if rnd.random() < 1e-5:
            price += rnd.randint(-800, 800)
        px.append(price)
        lots = max(1, int(round(rnd.lognormvariate(1.0, 1.0))))
        if rnd.random() < 0.002:
            lots = int(rnd.paretovariate(1.2) * 50)
        size.append(lots * 100)
        if burst > 0:
            burst -= 1
            if rnd.random() < 0.7:
                t += 0
            else:
                t += rnd.randint(1, 2000)
        else:
            if rnd.random() < 0.001:
                t += int(rnd.expovariate(1 / 2e9))
            else:
                t += int(rnd.expovariate(1 / 2e5))
            if rnd.random() < 0.05:
                burst = rnd.randint(2, 60)
        ts.append(t)
        u = rnd.random()
        lo, hi = 0, len(cdf) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if cdf[mid] < u: lo = mid + 1
            else: hi = mid
        sym.append(lo + 1)  # position 0 is the empty symbol
    return {"px": px, "size": size, "ts": ts, "sym": sym}


def zigzag(d):
    return ((d << 1) ^ (d >> 63)) & ((1 << 64) - 1)


def widths(values):
    """Per mini-block (FOR width, delta width) in bits."""
    out = []
    for i in range(0, len(values), MINI):
        b = values[i:i + MINI]
        f = (max(b) - min(b)).bit_length()
        d = max((zigzag(b[j] - b[j - 1]) for j in range(1, len(b))), default=0).bit_length()
        out.append((f, d))
    return out


def packed_bytes(ws, mode):
    # mode byte + reserve + i64 reference per block, a width byte per mini-block.
    blocks = math.ceil(len(ws) * MINI / BLOCK_ROWS)
    return blocks * 10 + sum(1 + (w[mode] * MINI + 7) // 8 for w in ws)


def snappy_sizes(rayforce, work, columns, rows):
    csv = os.path.join(work, "day.csv")
    with open(csv, "w") as f:
        f.write("px,size,ts,sym\n")
        for i in range(rows):
            f.write(f"{columns['px'][i]},{columns['size'][i]},{columns['ts'][i]},S{columns['sym'][i]:03d}\n")
    script = os.path.join(work, "gate.rfl")
    root = os.path.join(work, "snappy")
    with open(script, "w") as f:
        f.write(f'(.csv.block [px size ts sym] [I64 I64 I64 SYM] "{csv}" "{root}" '
                '{block-bytes: 65536 codec: snappy durable: false})\n'
                f'(show (.db.storage.info "{root}"))\n')
    out = subprocess.run([rayforce, script], capture_output=True, text=True).stdout
    sizes = {}
    for line in out.splitlines():
        cells = [c.strip() for c in line.strip("│ ").split("│")]
        if len(cells) >= 8 and cells[0] in columns:
            sizes[cells[0]] = int(cells[7])  # stored
    return sizes


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("rayforce")
    ap.add_argument("work")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--seed", type=int, default=20261016)
    args = ap.parse_args()
    os.makedirs(args.work, exist_ok=True)
    columns = generate(args.rows, args.seed)
    snappy = snappy_sizes(os.path.abspath(args.rayforce), os.path.abspath(args.work), columns, args.rows)
    passed = True
    for name in ("px", "size", "sym", "ts"):
        ws = widths(columns[name])
        mode = 1 if name == "ts" else min((0, 1), key=lambda m: packed_bytes(ws, m))
        share = sum(1 for w in ws if w[mode] <= TARGET[name]) / len(ws)
        per_block = BLOCK_ROWS // MINI
        blocks = [ws[i:i + per_block] for i in range(0, len(ws), per_block)]
        fallback = sum(1 for b in blocks if any(w[mode] >= 64 for w in b)) / len(blocks)
        pack = packed_bytes(ws, mode)
        ratio = pack / snappy[name] if snappy.get(name) else float("nan")
        ok = share >= 0.9 and fallback < 0.1 and ratio <= 0.5
        passed &= ok
        hist = collections.Counter(w[mode] for w in ws)
        print(f"{name:5s} mode {'delta' if mode else 'FOR':5s} <= {TARGET[name]} bits: {share:6.1%}  "
              f"fallback blocks {fallback:5.1%}  pack {pack/2**20:6.2f} MiB  snappy "
              f"{snappy.get(name, 0)/2**20:6.2f} MiB  ratio {ratio:4.2f}  {'PASS' if ok else 'FAIL'}")
        print("      widths", dict(sorted(hist.items())))
    print("GATE", "PASS" if passed else "FAIL")


if __name__ == "__main__":
    main()
