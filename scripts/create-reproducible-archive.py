#!/usr/bin/env python3
"""Create a deterministic gzip-compressed tar archive for a staged package."""

from __future__ import annotations

import argparse
import gzip
import tarfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--mtime", required=True, type=int)
    return parser.parse_args()


def normalized(info: tarfile.TarInfo, mtime: int) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = mtime
    info.pax_headers = {}
    return info


def add_path(
    archive: tarfile.TarFile,
    source: Path,
    archive_name: str,
    mtime: int,
) -> None:
    if source.is_symlink():
        raise ValueError(f"release package must not contain symlinks: {source}")
    archive.add(
        source,
        arcname=archive_name,
        recursive=False,
        filter=lambda info: normalized(info, mtime),
    )


def create_archive(source_dir: Path, archive_path: Path, mtime: int) -> None:
    source_dir = source_dir.resolve(strict=True)
    if not source_dir.is_dir():
        raise ValueError("source-dir must be a directory")
    if mtime < 0:
        raise ValueError("mtime must be non-negative")

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("wb") as raw:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw,
            compresslevel=9,
            mtime=mtime,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.GNU_FORMAT,
            ) as archive:
                add_path(
                    archive,
                    source_dir,
                    source_dir.name,
                    mtime,
                )
                for path in sorted(
                    source_dir.rglob("*"),
                    key=lambda item: item.relative_to(source_dir).as_posix(),
                ):
                    relative = path.relative_to(source_dir).as_posix()
                    add_path(
                        archive,
                        path,
                        f"{source_dir.name}/{relative}",
                        mtime,
                    )


def main() -> None:
    args = parse_args()
    create_archive(args.source_dir, args.archive, args.mtime)


if __name__ == "__main__":
    main()
