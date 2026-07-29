from __future__ import annotations

import struct
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from normalize_macho import normalize_macho_uuid


class NormalizeMachoTests(unittest.TestCase):
    def write_macho(self, uuid_bytes: bytes, *, command: int = 0x1B) -> Path:
        temporary = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(Path(temporary.name).unlink, missing_ok=True)
        header = struct.pack(
            "<IiiIIIII",
            0xFEEDFACF,
            0x0100000C,
            0,
            2,
            1,
            24,
            0,
            0,
        )
        load_command = struct.pack("<II16s", command, 24, uuid_bytes)
        temporary.write(header + load_command + b"stable-payload")
        temporary.close()
        return Path(temporary.name)

    def test_normalization_is_content_derived_and_idempotent(self) -> None:
        original_uuid = bytes(range(16))
        path = self.write_macho(original_uuid)

        first = normalize_macho_uuid(path)
        first_bytes = path.read_bytes()
        second = normalize_macho_uuid(path)

        self.assertEqual(first, second)
        self.assertEqual(path.read_bytes(), first_bytes)
        self.assertNotEqual(first, original_uuid)
        self.assertEqual(first[6] >> 4, 5)
        self.assertEqual(first[8] >> 6, 2)
        self.assertTrue(first_bytes.endswith(b"stable-payload"))

    def test_missing_uuid_command_fails_closed(self) -> None:
        path = self.write_macho(bytes(16), command=0x19)

        with self.assertRaisesRegex(ValueError, "LC_UUID"):
            normalize_macho_uuid(path)


if __name__ == "__main__":
    unittest.main()
