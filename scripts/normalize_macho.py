#!/usr/bin/env python3
"""Normalize the LC_UUID command in a thin 64-bit Mach-O executable."""

from __future__ import annotations

import argparse
import hashlib
import struct
from pathlib import Path

MH_MAGIC_64 = 0xFEEDFACF
LC_UUID = 0x1B
MACH_HEADER_64_SIZE = 32
LOAD_COMMAND_SIZE = 8
UUID_COMMAND_SIZE = 24


def normalize_macho_uuid(path: Path) -> bytes:
    data = bytearray(path.read_bytes())
    if len(data) < MACH_HEADER_64_SIZE:
        raise ValueError("file is too small for a 64-bit Mach-O header")

    header = struct.unpack_from("<IiiIIIII", data)
    if header[0] != MH_MAGIC_64:
        raise ValueError("only thin little-endian 64-bit Mach-O files are supported")

    command_count = header[4]
    command_bytes = header[5]
    command_offset = MACH_HEADER_64_SIZE
    command_limit = command_offset + command_bytes
    if command_limit > len(data):
        raise ValueError("Mach-O load commands exceed the file boundary")

    uuid_offset: int | None = None
    for _ in range(command_count):
        if command_offset + LOAD_COMMAND_SIZE > command_limit:
            raise ValueError("Mach-O load command header is truncated")
        command, size = struct.unpack_from("<II", data, command_offset)
        if size < LOAD_COMMAND_SIZE or command_offset + size > command_limit:
            raise ValueError("Mach-O load command has an invalid size")
        if command == LC_UUID:
            if size != UUID_COMMAND_SIZE:
                raise ValueError("LC_UUID has an invalid size")
            if uuid_offset is not None:
                raise ValueError("Mach-O contains multiple LC_UUID commands")
            uuid_offset = command_offset + LOAD_COMMAND_SIZE
        command_offset += size

    if uuid_offset is None:
        raise ValueError("Mach-O does not contain LC_UUID")

    content_for_hash = bytearray(data)
    content_for_hash[uuid_offset : uuid_offset + 16] = bytes(16)
    normalized_uuid = bytearray(hashlib.sha256(content_for_hash).digest()[:16])
    normalized_uuid[6] = (normalized_uuid[6] & 0x0F) | 0x50
    normalized_uuid[8] = (normalized_uuid[8] & 0x3F) | 0x80

    data[uuid_offset : uuid_offset + 16] = normalized_uuid
    with path.open("r+b") as stream:
        stream.seek(uuid_offset)
        stream.write(normalized_uuid)
        stream.flush()

    return bytes(normalized_uuid)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    normalized = normalize_macho_uuid(args.binary)
    print(normalized.hex())


if __name__ == "__main__":
    main()
