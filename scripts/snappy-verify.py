#!/usr/bin/env python3
"""Cross-check the native raw Snappy codec with the system libsnappy oracle.

Development-only: libsnappy is never linked into Rayforce. Example (Linux):
  cc -std=c17 -O2 -fPIC -shared -Isrc src/core/snappy.c -o /tmp/ray-snappy.so
  python3 scripts/snappy-verify.py /tmp/ray-snappy.so --cases 1000
"""

import argparse
import ctypes as c
import ctypes.util
import json
import random


class Workspace(c.Structure):
    _fields_ = [("positions", c.c_uint32 * (1 << 14))]


def bind(lib, name, result, *args):
    fn = getattr(lib, name)
    fn.restype = result
    fn.argtypes = args
    return fn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("library", help="path to the native codec shared library")
    parser.add_argument("--oracle", help="libsnappy path (otherwise system discovery)")
    parser.add_argument("--cases", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20261002)
    args = parser.parse_args()
    if args.cases < 1:
        parser.error("--cases must be positive")
    oracle_path = args.oracle or ctypes.util.find_library("snappy")
    if not oracle_path:
        parser.error("install libsnappy for this development-only check")
    native, oracle = c.CDLL(args.library), c.CDLL(oracle_path)
    ptr, size = c.c_void_p, c.c_size_t
    bound = bind(native, "ray_snappy_compress_bound", size, size)
    encode = bind(native, "ray_snappy_compress", size, ptr, size, ptr, size,
                  c.POINTER(Workspace))
    decode = bind(native, "ray_snappy_decompress", c.c_bool, ptr, size, ptr, size)
    ref_bound = bind(oracle, "snappy_max_compressed_length", size, size)
    ref_encode = bind(oracle, "snappy_compress", c.c_int, ptr, size, ptr, c.POINTER(size))
    ref_decode = bind(oracle, "snappy_uncompress", c.c_int, ptr, size, ptr, c.POINTER(size))
    ref_validate = bind(oracle, "snappy_validate_compressed_buffer", c.c_int, ptr, size)
    workspace = Workspace()
    rng = random.Random(args.seed)
    edges = [0, 1, 2, 3, 4, 5, 59, 60, 61, 63, 64, 65, 127, 128,
             255, 256, 257, 16383, 16384, 65535, 65536, 65537, 262144, 1048576]
    totals = {"input_bytes": 0, "native_bytes": 0, "reference_bytes": 0}

    for case in range(args.cases):
        n = edges[case // 5] if case < len(edges) * 5 else rng.randrange(262145)
        pattern = case % 5
        if pattern == 0:
            data = bytes(n)
        elif pattern == 1:
            motif = rng.randbytes(7)
            data = (motif * ((n + 6) // 7))[:n]
        elif pattern == 2:
            data = rng.randbytes(n)
        elif pattern == 3:
            data = b"".join(i.to_bytes(8, "little") for i in range((n + 7) // 8))[:n]
        else:
            prefix = rng.randbytes(min(n, 65536))
            data = (prefix * ((n + len(prefix) - 1) // len(prefix)))[:n] if n else b""

        src = c.create_string_buffer(data, max(1, n))
        encoded = c.create_string_buffer(bound(n))
        decoded = c.create_string_buffer(max(1, n))
        written = encode(src, n, encoded, len(encoded), c.byref(workspace))
        if not written or ref_validate(encoded, written) != 0:
            raise RuntimeError(f"case {case}: reference rejected native output")
        actual = size(n)
        if ref_decode(encoded, written, decoded, c.byref(actual)) != 0 or actual.value != n:
            raise RuntimeError(f"case {case}: reference decode length/status mismatch")
        if decoded.raw[:n] != data:
            raise RuntimeError(f"case {case}: reference decoded different bytes")

        ref = c.create_string_buffer(ref_bound(n))
        ref_len = size(len(ref))
        if ref_encode(src, n, ref, c.byref(ref_len)) != 0:
            raise RuntimeError(f"case {case}: reference encoding failed")
        c.memset(decoded, 0xa5, len(decoded))
        if not decode(ref, ref_len.value, decoded, n) or decoded.raw[:n] != data:
            raise RuntimeError(f"case {case}: native decode of reference output failed")
        totals["input_bytes"] += n
        totals["native_bytes"] += written
        totals["reference_bytes"] += ref_len.value

    print(json.dumps({"cases": args.cases, "seed": args.seed,
                      "oracle": oracle_path, "bidirectional": "passed",
                      "synthetic_totals": totals}, indent=2))


if __name__ == "__main__":
    main()
