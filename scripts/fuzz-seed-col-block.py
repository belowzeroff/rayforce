#!/usr/bin/env python3
"""Generate major-2 container seeds independently of the C writer (stdlib only)."""
import argparse
import pathlib
import struct
import zlib


def container(compressed=False, empty=False):
    header = bytearray(128)
    header[17:19] = bytes((2, 2))  # major 2, U8
    header[32:40] = b"RAYHDB2\0"
    payload = bytearray()
    directory = bytearray()
    for block in range(0 if empty else 4):
        raw = bytes(256)
        # Length 256, one literal zero, overlapping COPY_2 spans: 64,64,64,63.
        stored = b"\x80\x02\x00\x00" + b"\xfe\x01\x00" * 3 + b"\xfa\x01\x00" if compressed else raw
        entry = bytearray(64)
        struct.pack_into("<QQQII", entry, 0, block * 256, 256, 128 + len(payload), len(stored), 256)
        entry[32] = int(compressed)
        struct.pack_into("<II", entry, 40, zlib.crc32(stored), zlib.crc32(raw))
        directory.extend(entry)
        payload.extend(stored)
    blocks = len(directory) // 64
    offset = 128 + len(payload)
    size = offset + len(directory) + 32
    struct.pack_into("<Q", header, 24, blocks * 256)
    struct.pack_into("<IIQQQQI", header, 40, 128, 256, blocks, offset, size, 123, zlib.crc32(directory))
    struct.pack_into("<I", header, 84, zlib.crc32(header))
    return header + payload + directory + struct.pack("<8sQQQ", b"RAYEND2\0", blocks, offset, size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=pathlib.Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    for name, compressed, empty in (("raw", False, False), ("snappy", True, False), ("empty", False, True)):
        (args.directory / name).write_bytes(container(compressed, empty))


if __name__ == "__main__":
    main()
