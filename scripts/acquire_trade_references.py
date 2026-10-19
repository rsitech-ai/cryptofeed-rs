#!/usr/bin/env python3
"""Repeat at most ten public GETs from an audit reference manifest, unqualified.

This reconstructs the acquisition procedure, not the original changing venue
responses. Fresh output is required. No pagination, retries or corpus writes.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import parse_qsl, urlsplit
import urllib.request

HOSTS = {"www.okx.com", "api.binance.com", "api.bybit.com", "api.kraken.com",
         "www.bitstamp.net", "api.gemini.com", "api.exchange.coinbase.com",
         "api-pub.bitfinex.com", "www.deribit.com"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    if not isinstance(plan, list) or not 1 <= len(plan) <= 10:
        parser.error("expected 1–10 public request entries")
    for row in plan:
        url = urlsplit(row["url"])
        if url.scheme != "https" or url.hostname not in HOSTS or url.username or url.password:
            parser.error("only allowlisted anonymous public HTTPS hosts")
        if any(any(secret in key.lower() for secret in ("key", "sig", "token", "secret", "auth", "pass"))
               for key, _ in parse_qsl(url.query)):
            parser.error("authenticated/signed URLs forbidden")
        if not row["name"].replace("-", "").isalnum():
            parser.error("invalid object name")
    if len({row["name"] for row in plan}) != len(plan):
        parser.error("duplicate object names")
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    deadline = time.monotonic() + 100
    for row in plan:
        if time.monotonic() >= deadline:
            break
        result = dict(name=row["name"], url=row["url"], attempts=1,
            retrieval_start_utc=datetime.now(timezone.utc).isoformat(),
            limits=dict(response_bytes=1048576, timeout_seconds=8, retries=0),
            status="blocked", qualification="not-qualified")
        try:
            request = urllib.request.Request(row["url"], headers={"User-Agent": "cryptofeed-rs-public-audit/1"})
            with urllib.request.urlopen(request, timeout=8) as response:
                body = response.read(1048577)
                result["http_status"] = response.status
                result["headers"] = {key: response.headers[key] for key in
                    ("Date", "Content-Type", "ETag", "Last-Modified") if key in response.headers}
            if len(body) > 1048576:
                raise ValueError("response exceeds 1 MiB")
            target = args.output / (row["name"] + ".json")
            target.write_bytes(body)
            result.update(status="acquired-unqualified", bytes=len(body), file=target.name,
                          sha256=hashlib.sha256(body).hexdigest())
        except (OSError, ValueError) as error:
            result["error"] = str(error)
        result["retrieval_end_utc"] = datetime.now(timezone.utc).isoformat()
        results.append(result)
        (args.output / "manifest.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(dict(requests=len(results), status="unqualified"), indent=2))


if __name__ == "__main__":
    main()
