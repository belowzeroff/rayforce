#!/usr/bin/env python3
"""POSIX subprocess smoke test for fileio shared/nonblocking-exclusive locks.

Build: cc -std=c17 -fPIC -shared -Iinclude -Isrc src/store/fileio.c -o /tmp/ray-fileio.so
Run: python3 scripts/lease-lock-verify.py /tmp/ray-fileio.so
"""
import ctypes
import os
import pathlib
import select
import subprocess
import sys
import tempfile


def library(path):
    lib = ctypes.CDLL(path)
    lib.ray_file_open.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.ray_file_open.restype = ctypes.c_int
    lib.ray_file_lock_sh.argtypes = [ctypes.c_int]
    lib.ray_file_lock_sh.restype = ctypes.c_int
    lib.ray_file_try_lock_ex.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_bool)]
    lib.ray_file_try_lock_ex.restype = ctypes.c_int
    lib.ray_file_close.argtypes = [ctypes.c_int]
    lib.ray_file_close.restype = None
    return lib


def main():
    if os.name != "posix" or len(sys.argv) not in (2, 4):
        raise SystemExit("usage (POSIX): lease-lock-verify.py /path/to/ray-fileio.so")
    lib = library(sys.argv[1])
    if len(sys.argv) == 4 and sys.argv[2] == "--hold":
        fd = lib.ray_file_open(os.fsencode(sys.argv[3]), 1)
        if fd < 0 or lib.ray_file_lock_sh(fd):
            raise SystemExit("reader could not acquire lock")
        sys.stdout.write("ready\n")
        sys.stdout.flush()
        sys.stdin.buffer.read(1)
        os._exit(0)
    with tempfile.TemporaryDirectory(prefix="ray-lease-") as directory:
        path = pathlib.Path(directory) / ".lease"
        path.touch(mode=0o444)
        child = subprocess.Popen(
            [sys.executable, __file__, sys.argv[1], "--hold", str(path)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        fd = -1
        try:
            ready, _, _ = select.select([child.stdout], [], [], 10)
            if not ready or child.stdout.readline() != b"ready\n":
                raise RuntimeError("reader did not acquire lease within 10 seconds")
            fd = lib.ray_file_open(os.fsencode(path), 1)
            if fd < 0:
                raise RuntimeError("cannot open probe")
            acquired = ctypes.c_bool(True)
            if lib.ray_file_try_lock_ex(fd, ctypes.byref(acquired)) or acquired.value:
                raise RuntimeError("exclusive probe ignored live reader")
            child.kill()
            child.wait(timeout=10)
            if lib.ray_file_try_lock_ex(fd, ctypes.byref(acquired)) or not acquired.value:
                raise RuntimeError("lock did not release after reader process termination")
            print("PASS: cross-process exclusion and automatic release after reader termination")
        finally:
            if fd >= 0:
                lib.ray_file_close(fd)
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)
            child.stdin.close()
            child.stdout.close()
            child.stderr.close()


if __name__ == "__main__":
    main()
