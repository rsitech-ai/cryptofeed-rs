#!/usr/bin/env python3
"""Bounded public daemon capture; records evidence, never qualifies a dataset.

Build marketfeed first. The caller supplies a reviewed, public-only config and
fresh evidence directory. This runner does not recover history or place orders.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.error
import urllib.request


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def validate_public_paths(config, output):
    root = output.resolve()
    raw = config.get("recording", {}).get("raw", {})
    sinks = config.get("sinks", [])
    if not raw.get("enabled") or len(sinks) != 1 or sinks[0].get("type") != "file":
        raise ValueError("requires raw capture and exactly one local file sink")
    for value in (raw.get("directory", ""), sinks[0].get("path", "")):
        path = Path(value).absolute()
        if not value or not path.resolve().is_relative_to(root) or path.resolve() == root:
            raise ValueError("raw and sink destinations must be inside fresh output")
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError("symlink destinations are forbidden")


def validate_metrics(config, parsed_url):
    if parsed_url.netloc != config.get("telemetry", {}).get("bind") or parsed_url.path != "/metrics" or parsed_url.query:
        raise ValueError("metrics URL must match configured telemetry.bind and /metrics")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--max-bytes", type=int, default=128 * 1024 * 1024)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 180 or not 1 <= args.max_bytes <= 512 * 1024 * 1024:
        parser.error("caps: 1–180 seconds, 1–512 MiB")
    # Only a literal loopback metrics URL is accepted, with no credentials.
    from urllib.parse import urlsplit
    url = urlsplit(args.metrics_url)
    if url.scheme != "http" or url.hostname != "127.0.0.1" or url.username or url.password:
        parser.error("metrics URL must use http://127.0.0.1")
    import tomllib
    config_bytes = args.config.read_bytes()
    config = tomllib.loads(config_bytes.decode())
    if any(item.get("enabled") for item in config.get("private", {}).values()):
        parser.error("private sessions forbidden")
    if any(v.get("segment") == "intl" for v in config.get("venues", [])):
        parser.error("authenticated INTX is excluded from public evidence")
    try:
        validate_public_paths(config, args.output)
        validate_metrics(config, url)
    except ValueError as error:
        parser.error(str(error))
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "config.toml").write_bytes(config_bytes)
    result = {"schema_version": 1, "source_sha": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True).strip(), "start_utc": utc(),
        "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "git_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "command": [str(args.binary.resolve()), "run", "--config", str((args.output / "config.toml").resolve())],
        "caps": {"seconds": args.seconds, "storage_stop_threshold_bytes": args.max_bytes,
                 "storage_threshold_is_soft": True, "shutdown_seconds": 25},
        "qualification": "not-qualified", "continuity": "unknown", "snapshots": []}
    env = {k: v for k, v in os.environ.items() if not k.startswith(
        ("COINBASE_", "OKX_", "BYBIT_", "BINANCE_"))}
    start = time.monotonic()
    with (args.output / "daemon.log").open("wb") as log:
        process = subprocess.Popen(result["command"], stdout=log, stderr=log, env=env)
        try:
            result["stop_reason"] = "wall-time-cap"
            while time.monotonic() - start < args.seconds:
                if process.poll() is not None:
                    result["stop_reason"] = "daemon-exited"
                    break
                size = sum(p.stat().st_size for p in args.output.rglob("*") if p.is_file())
                if size >= args.max_bytes:
                    result["stop_reason"] = "storage-cap-observed"
                    break
                snap = {"utc": utc(), "bytes_observed": size}
                try:
                    with urllib.request.urlopen(args.metrics_url, timeout=0.5) as response:
                        payload = response.read(1024 * 1024 + 1)
                    if len(payload) > 1024 * 1024:
                        raise ValueError("metrics exceeds 1 MiB")
                    name = f"metrics-{len(result['snapshots']):03}.txt"
                    (args.output / name).write_bytes(payload)
                    snap["file"] = name
                except (OSError, ValueError, urllib.error.URLError) as error:
                    snap["error"] = str(error)
                result["snapshots"].append(snap)
                time.sleep(1)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                result["forced_kill"] = True
    result.update(end_utc=utc(), elapsed_seconds=time.monotonic() - start,
                  daemon_exit_code=process.returncode)
    result["files"] = [{"path": str(p.relative_to(args.output)), "bytes": p.stat().st_size,
                        "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                       for p in sorted(args.output.rglob("*")) if p.is_file()]
    (args.output / "capture-manifest.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ("start_utc", "end_utc", "stop_reason", "daemon_exit_code")}))
    return int(process.returncode != 0)


if __name__ == "__main__":
    raise SystemExit(main())
