#!/usr/bin/env python3
"""Collect and evaluate bounded, read-only release canary evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import signal
import statistics
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
from datetime import datetime, timezone


DEFAULT_THRESHOLDS = {
    "max_queue_ratio": 0.80,
    "max_reconnect_delta": 2,
    "max_rss_mib": 1536.0,
    "max_rss_growth_mib_per_hour": 64.0,
    "rss_warmup_seconds": 300.0,
    "min_rss_trend_seconds": 600.0,
    "max_api_latency_ms": 500.0,
    "max_cpu_percent": 150.0,
}

METRIC_TO_FIELD = {
    "marketfeed_venue_events_dropped_total": "events_dropped",
    "marketfeed_venue_reconnects_total": "reconnects",
    "marketfeed_venue_book_invalidations_total": "book_invalidations",
    "marketfeed_venue_valid_books": "valid_books",
    "marketfeed_venue_queue_occupancy": "queue_occupancy",
    "marketfeed_venue_batch_queue_capacity": "queue_capacity",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_prometheus(text: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    parsed: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    line_pattern = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)")
    label_pattern = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"')
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = line_pattern.match(line)
        if not match:
            continue
        name, raw_labels, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError:
            continue
        labels = tuple(sorted((key, bytes(value, "utf-8").decode("unicode_escape")) for key, value in label_pattern.findall(raw_labels or "")))
        parsed[(name, labels)] = value
    return parsed


def should_retry_smoke(exit_code: int, output: str) -> bool:
    """Retry only a live tape warmup miss; structural smoke failures stay fatal."""
    return exit_code != 0 and "FAIL  critical tape empty" in output


def analyze_daemon_log(text: str) -> dict:
    errors: list[str] = []
    warnings: list[str] = []
    malformed = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        level = str(entry.get("level", "")).upper()
        message = str(entry.get("fields", {}).get("message", "unknown log event"))
        if level == "ERROR":
            errors.append(message)
        elif level == "WARN":
            warnings.append(message)
    return {
        "error_count": len(errors),
        "warning_count": len(warnings),
        "malformed_count": malformed,
        "error_messages": sorted(set(errors))[:20],
        "warning_messages": sorted(set(warnings))[:20],
    }


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _counter_delta(samples: list[dict], venue_id: str, field: str) -> float:
    values = [
        float(venue.get(field, 0) or 0)
        for sample in samples
        for venue in sample.get("venues", [])
        if venue.get("id") == venue_id
    ]
    return max(values) - values[0] if values else 0.0


def _rss_growth_mib_per_hour(
    samples: list[dict], warmup_seconds: float, minimum_span_seconds: float
) -> float | None:
    points = [
        (float(sample["elapsed_seconds"]), float(sample["rss_kib"]) / 1024.0)
        for sample in samples
        if sample.get("rss_kib") is not None
        and float(sample.get("elapsed_seconds", 0)) >= warmup_seconds
    ]
    if (
        len(points) < 2
        or points[-1][0] <= points[0][0]
        or points[-1][0] - points[0][0] < minimum_span_seconds
    ):
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(ys)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    if denominator == 0:
        return None
    mib_per_second = sum((x - x_mean) * (y - y_mean) for x, y in points) / denominator
    return max(0.0, mib_per_second * 3600.0)


def analyze_samples(
    samples: list[dict], expectations: dict, overrides: dict | None = None
) -> dict:
    thresholds = dict(DEFAULT_THRESHOLDS)
    thresholds.update(overrides or {})
    holds: list[str] = []
    warnings: list[str] = []
    if not samples:
        return {"verdict": "HOLD", "holds": ["no canary samples were collected"], "warnings": []}

    configured = list(expectations.get("configured_venues", []))
    expected_books = dict(expectations.get("l2_books", {}))
    venue_maps = [{venue.get("id"): venue for venue in sample.get("venues", [])} for sample in samples]

    if any(sample.get("live_http") != 200 or sample.get("ready_http") != 200 for sample in samples):
        holds.append("daemon health/readiness was not continuously HTTP 200")
    if any(sample.get("ui_http") != 200 for sample in samples):
        holds.append("UI root was not continuously HTTP 200")
    if any(
        sample.get("status_http") != 200 or sample.get("metrics_http") != 200
        for sample in samples
    ):
        holds.append("status/metrics observability was not continuously HTTP 200")

    live_counts = [sum(bool(venues.get(venue_id, {}).get("live")) for venue_id in configured) for venues in venue_maps]
    for venue_id in configured:
        bad_indexes = [index for index, venues in enumerate(venue_maps) if not venues.get(venue_id, {}).get("live")]
        if bad_indexes:
            if bad_indexes[-1] == len(samples) - 1 or len(bad_indexes) > 2:
                holds.append(f"venue {venue_id} was not continuously live")
            else:
                warnings.append(f"venue {venue_id} had a transient offline sample and recovered")

    for venue_id, expected in expected_books.items():
        bad_indexes = [
            index
            for index, venues in enumerate(venue_maps)
            if float(venues.get(venue_id, {}).get("valid_books", 0) or 0) < float(expected)
        ]
        if bad_indexes:
            if bad_indexes[-1] == len(samples) - 1 or len(bad_indexes) > 2:
                holds.append(f"venue {venue_id} did not retain {expected} valid books")
            else:
                warnings.append(f"venue {venue_id} briefly fell below {expected} valid books and recovered")

    reconnect_delta = 0.0
    for venue_id in configured:
        dropped = _counter_delta(samples, venue_id, "events_dropped")
        invalidations = _counter_delta(samples, venue_id, "book_invalidations")
        reconnects = _counter_delta(samples, venue_id, "reconnects")
        reconnect_delta += reconnects
        if dropped > 0:
            holds.append(f"venue {venue_id} added {dropped:g} dropped events")
        if invalidations > 0:
            holds.append(f"venue {venue_id} added {invalidations:g} book invalidations")
        if reconnects > 0:
            warnings.append(f"venue {venue_id} added {reconnects:g} reconnect(s)")
        if reconnects > float(thresholds["max_reconnect_delta"]):
            holds.append(f"venue {venue_id} exceeded reconnect allowance ({reconnects:g})")

    max_queue_ratio = 0.0
    for venues in venue_maps:
        for venue_id in configured:
            venue_status = venues.get(venue_id, {})
            capacity = float(venue_status.get("queue_capacity", 0) or 0)
            occupancy = float(venue_status.get("queue_occupancy", 0) or 0)
            if capacity > 0:
                max_queue_ratio = max(max_queue_ratio, occupancy / capacity)
    if max_queue_ratio >= float(thresholds["max_queue_ratio"]):
        holds.append(f"venue queue reached {max_queue_ratio:.1%} capacity")

    rss_values = [float(sample["rss_kib"]) / 1024.0 for sample in samples if sample.get("rss_kib") is not None]
    rss_peak_mib = max(rss_values, default=0.0)
    rss_growth = _rss_growth_mib_per_hour(
        samples,
        float(thresholds["rss_warmup_seconds"]),
        float(thresholds["min_rss_trend_seconds"]),
    )
    if rss_peak_mib > float(thresholds["max_rss_mib"]):
        holds.append(f"RSS peak {rss_peak_mib:.1f} MiB exceeded limit")
    if rss_growth is None:
        warnings.append("RSS trend window was too short for post-warmup leak qualification")
    elif rss_growth > float(thresholds["max_rss_growth_mib_per_hour"]):
        holds.append(f"RSS growth {rss_growth:.1f} MiB/hour exceeded limit")

    api_p95 = _percentile([float(sample.get("api_latency_ms", 0) or 0) for sample in samples], 0.95)
    cpu_p95 = _percentile([float(sample.get("cpu_percent", 0) or 0) for sample in samples], 0.95)
    if api_p95 > float(thresholds["max_api_latency_ms"]):
        holds.append(f"API latency p95 {api_p95:.1f} ms exceeded limit")
    if cpu_p95 > float(thresholds["max_cpu_percent"]):
        holds.append(f"CPU p95 {cpu_p95:.1f}% exceeded limit")

    return {
        "verdict": "HOLD" if holds else "GO",
        "holds": sorted(set(holds)),
        "warnings": sorted(set(warnings)),
        "samples": len(samples),
        "duration_seconds": float(samples[-1].get("elapsed_seconds", 0)),
        "venues_expected": len(configured),
        "venues_live_min": min(live_counts, default=0),
        "reconnect_delta": reconnect_delta,
        "max_queue_ratio": max_queue_ratio,
        "rss_peak_mib": rss_peak_mib,
        "rss_growth_mib_per_hour": rss_growth,
        "api_latency_p95_ms": api_p95,
        "cpu_p95_percent": cpu_p95,
        "thresholds": thresholds,
    }


def _http(url: str, timeout: float = 5.0) -> tuple[int, bytes, float]:
    start = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read(), (time.monotonic() - start) * 1000.0
    except urllib.error.HTTPError as error:
        return error.code, error.read(), (time.monotonic() - start) * 1000.0
    except (OSError, urllib.error.URLError):
        return 0, b"", (time.monotonic() - start) * 1000.0


def _process_stats(pid: int) -> tuple[int | None, float | None]:
    result = subprocess.run(
        ["ps", "-o", "rss=,%cpu=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    parts = result.stdout.split()
    if len(parts) < 2:
        return None, None
    try:
        return int(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def _expectations(config_path: pathlib.Path) -> dict:
    with config_path.open("rb") as handle:
        config = tomllib.load(handle)
    configured: list[str] = []
    books: dict[str, int] = {}
    for venue_config in config.get("venues", []):
        venue_id = str(venue_config["id"])
        configured.append(venue_id)
        if "l2" in venue_config.get("channels", []):
            books[venue_id] = len(venue_config.get("symbols", []))
    return {"configured_venues": configured, "l2_books": books}


def _runtime_config(source: pathlib.Path, destination: pathlib.Path, telemetry_port: int, ui_port: int) -> None:
    text = source.read_text()
    text, telemetry_count = re.subn(
        r'(?m)^bind\s*=\s*"[^"]+"\s*$', f'bind = "127.0.0.1:{telemetry_port}"', text, count=1
    )
    text, ui_count = re.subn(
        r'(?m)^ui_bind\s*=\s*"[^"]+"\s*$', f'ui_bind = "127.0.0.1:{ui_port}"', text, count=1
    )
    if telemetry_count != 1 or ui_count != 1:
        raise ValueError("config must contain one telemetry bind and one ui_bind")
    destination.write_text(text)


def _sample(pid: int, telemetry_base: str, ui_base: str, elapsed: float) -> dict:
    live_code, _, _ = _http(f"{telemetry_base}/live", 2)
    ready_code, _, _ = _http(f"{telemetry_base}/ready", 2)
    ui_code, _, _ = _http(f"{ui_base}/", 2)
    status_code, status_body, status_latency = _http(f"{ui_base}/v1/status", 5)
    metrics_code, metrics_body, metrics_latency = _http(f"{telemetry_base}/metrics", 5)
    status = json.loads(status_body) if status_code == 200 else {"venues": []}
    metrics = parse_prometheus(metrics_body.decode("utf-8", "replace")) if metrics_code == 200 else {}
    venues = []
    for venue_status in status.get("venues", []):
        current = dict(venue_status)
        venue_id = str(current.get("id", ""))
        for metric, field in METRIC_TO_FIELD.items():
            current[field] = metrics.get((metric, (("id", venue_id),)), current.get(field, 0))
        venues.append(current)
    rss_kib, cpu_percent = _process_stats(pid)
    return {
        "utc": utc_now(),
        "elapsed_seconds": round(elapsed, 3),
        "live_http": live_code,
        "ready_http": ready_code,
        "ui_http": ui_code,
        "status_http": status_code,
        "metrics_http": metrics_code,
        "api_latency_ms": round(max(status_latency, metrics_latency), 3),
        "rss_kib": rss_kib,
        "cpu_percent": cpu_percent,
        "venues": venues,
    }


def _qualified(sample: dict, expectations: dict) -> bool:
    if any(sample.get(key) != 200 for key in ("live_http", "ready_http", "ui_http", "status_http", "metrics_http")):
        return False
    venues = {venue.get("id"): venue for venue in sample.get("venues", [])}
    if any(not venues.get(venue_id, {}).get("live") for venue_id in expectations["configured_venues"]):
        return False
    return all(
        float(venues.get(venue_id, {}).get("valid_books", 0) or 0) >= expected
        for venue_id, expected in expectations["l2_books"].items()
    )


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_report(path: pathlib.Path, summary: dict, metadata: dict) -> None:
    holds = "\n".join(f"- {item}" for item in summary["holds"]) or "- None"
    warnings = "\n".join(f"- {item}" for item in summary["warnings"]) or "- None"
    rss_growth = summary["rss_growth_mib_per_hour"]
    rss_growth_label = f"{rss_growth:.1f} MiB/hour" if rss_growth is not None else "not qualified"
    path.write_text(
        f"""# Release canary result

**Verdict:** {summary['verdict']} for the next 24-hour beta qualification; this run does not promote maturity.

- Commit: `{metadata['git_commit']}`
- Binary SHA-256: `{metadata['binary_sha256']}`
- Started: {metadata['started_utc']}
- Duration sampled: {summary['duration_seconds']:.1f} seconds across {summary['samples']} samples
- Minimum live venues: {summary['venues_live_min']}/{summary['venues_expected']}
- RSS peak / fitted post-warmup growth: {summary['rss_peak_mib']:.1f} MiB / {rss_growth_label}
- CPU p95: {summary['cpu_p95_percent']:.1f}%
- API latency p95: {summary['api_latency_p95_ms']:.1f} ms
- Maximum venue queue occupancy: {summary['max_queue_ratio']:.1%}

## Hold reasons

{holds}

## Warnings

{warnings}

## Boundary

This is a bounded laptop run over public, read-only feeds. It does not prove scheduled operation, authenticated feeds, external sink delivery, 24-hour stability, or multi-day stability. Audio, trading, and order placement are outside this product gate.
"""
    )


def run_canary(args: argparse.Namespace) -> int:
    binary = pathlib.Path(args.binary).resolve()
    config = pathlib.Path(args.config).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise SystemExit(f"binary is missing or not executable: {binary}")
    if not config.is_file():
        raise SystemExit(f"config is missing: {config}")
    if args.duration_seconds < 60 and not args.allow_short:
        raise SystemExit("duration must be at least 60 seconds")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = pathlib.Path(args.output_dir) if args.output_dir else pathlib.Path(".local/evidence/release-canary/runs") / stamp
    output.mkdir(parents=True, exist_ok=False)
    runtime_config = output / "config.runtime.toml"
    _runtime_config(config, runtime_config, args.telemetry_port, args.ui_port)
    expectations = _expectations(runtime_config)
    telemetry_base = f"http://127.0.0.1:{args.telemetry_port}"
    ui_base = f"http://127.0.0.1:{args.ui_port}"
    metadata = {
        "started_utc": utc_now(),
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], text=True),
        "binary": str(binary),
        "binary_sha256": _sha256(binary),
        "config_source": str(config),
        "config_sha256": _sha256(config),
        "duration_seconds": args.duration_seconds,
        "sample_interval_seconds": args.sample_interval_seconds,
        "expectations": expectations,
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    log_handle = (output / "daemon.log").open("wb")
    process = subprocess.Popen([str(binary), "run", "--config", str(runtime_config)], stdout=log_handle, stderr=subprocess.STDOUT)
    forced_stop = False
    samples: list[dict] = []

    def stop_child(*_unused: object) -> None:
        if process.poll() is None:
            process.terminate()

    previous_handlers = {sig: signal.signal(sig, stop_child) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        ready_deadline = time.monotonic() + args.ready_timeout_seconds
        while time.monotonic() < ready_deadline:
            if process.poll() is not None:
                raise RuntimeError(f"daemon exited before qualification with code {process.returncode}")
            candidate = _sample(process.pid, telemetry_base, ui_base, 0.0)
            if _qualified(candidate, expectations):
                samples.append(candidate)
                break
            time.sleep(1)
        else:
            raise RuntimeError("daemon did not reach all-venue/L2 qualification before timeout")

        if args.smoke_script:
            smoke_env = dict(os.environ)
            smoke_env["BASE"] = ui_base
            smoke_env["OUT_DIR"] = str(output / "ui-smoke")
            smoke_deadline = time.monotonic() + args.smoke_warmup_timeout_seconds
            smoke_attempts: list[str] = []
            while True:
                smoke = subprocess.run([args.smoke_script], env=smoke_env, text=True, capture_output=True, check=False)
                smoke_output = smoke.stdout + smoke.stderr
                smoke_attempts.append(
                    f"=== attempt {len(smoke_attempts) + 1} exit={smoke.returncode} ===\n{smoke_output}"
                )
                if smoke.returncode == 0:
                    break
                if not should_retry_smoke(smoke.returncode, smoke_output) or time.monotonic() >= smoke_deadline:
                    break
                time.sleep(5)
            (output / "ui-smoke.log").write_text("\n".join(smoke_attempts))
            metadata["ui_smoke_attempts"] = len(smoke_attempts)
            metadata["ui_smoke_exit_code"] = smoke.returncode
            if smoke.returncode != 0:
                raise RuntimeError(f"live UI smoke failed with code {smoke.returncode}")

        start = time.monotonic()
        next_sample = start + args.sample_interval_seconds
        print(f"release canary qualified: pid={process.pid} venues={len(expectations['configured_venues'])}; sampling {args.duration_seconds}s", flush=True)
        while time.monotonic() - start < args.duration_seconds:
            if process.poll() is not None:
                raise RuntimeError(f"daemon exited during canary with code {process.returncode}")
            sleep_for = min(1.0, max(0.0, next_sample - time.monotonic()))
            time.sleep(sleep_for)
            if time.monotonic() >= next_sample:
                current = _sample(process.pid, telemetry_base, ui_base, time.monotonic() - start)
                samples.append(current)
                print(
                    f"sample t={current['elapsed_seconds']:.0f}s live={sum(bool(v.get('live')) for v in current['venues'])}/{len(expectations['configured_venues'])} rss_kib={current['rss_kib']} cpu={current['cpu_percent']}",
                    flush=True,
                )
                next_sample += args.sample_interval_seconds
        samples.append(_sample(process.pid, telemetry_base, ui_base, time.monotonic() - start))
    except Exception as error:
        metadata["runner_error"] = str(error)
        print(f"release canary runner error: {error}", file=sys.stderr, flush=True)
    finally:
        stop_child()
        try:
            process.wait(timeout=args.shutdown_timeout_seconds)
        except subprocess.TimeoutExpired:
            forced_stop = True
            process.kill()
            process.wait(timeout=5)
        log_handle.close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    with (output / "samples.jsonl").open("w") as handle:
        for item in samples:
            handle.write(json.dumps(item, separators=(",", ":")) + "\n")
    summary = analyze_samples(samples, expectations)
    log_result = analyze_daemon_log((output / "daemon.log").read_text(errors="replace"))
    summary["daemon_log"] = log_result
    if log_result["error_count"]:
        summary["holds"].append(
            f"daemon emitted {log_result['error_count']} ERROR log event(s): "
            + ", ".join(log_result["error_messages"])
        )
    if log_result["warning_count"]:
        summary["warnings"].append(
            f"daemon emitted {log_result['warning_count']} WARN log event(s): "
            + ", ".join(log_result["warning_messages"])
        )
    if log_result["malformed_count"]:
        summary["warnings"].append(
            f"daemon log contained {log_result['malformed_count']} non-JSON line(s)"
        )
    if metadata.get("runner_error"):
        summary["holds"].append(f"runner error: {metadata['runner_error']}")
    if metadata.get("ui_smoke_exit_code", 0) != 0:
        summary["holds"].append("live UI smoke did not pass")
    if forced_stop:
        summary["holds"].append("daemon required forced termination")
    if process.returncode != 0:
        summary["holds"].append(f"daemon exited with nonzero code {process.returncode}")
    summary["holds"] = sorted(set(summary["holds"]))
    summary["warnings"] = sorted(set(summary["warnings"]))
    summary["verdict"] = "HOLD" if summary["holds"] else "GO"
    metadata["stopped_utc"] = utc_now()
    metadata["daemon_exit_code"] = process.returncode
    metadata["forced_stop"] = forced_stop
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    _write_report(output / "report.md", summary, metadata)
    print(f"release canary {summary['verdict']}: {output}", flush=True)
    return 0 if summary["verdict"] == "GO" else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/release/marketfeed")
    parser.add_argument("--config", default=".local/live-ui/config.live.ui.toml")
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--sample-interval-seconds", type=int, default=15)
    parser.add_argument("--ready-timeout-seconds", type=int, default=180)
    parser.add_argument("--shutdown-timeout-seconds", type=int, default=30)
    parser.add_argument("--smoke-warmup-timeout-seconds", type=int, default=60)
    parser.add_argument("--telemetry-port", type=int, default=19208)
    parser.add_argument("--ui-port", type=int, default=19209)
    parser.add_argument("--output-dir")
    parser.add_argument("--smoke-script", default="./scripts/live_ui_smoke.sh")
    parser.add_argument("--allow-short", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run_canary(parse_args()))
