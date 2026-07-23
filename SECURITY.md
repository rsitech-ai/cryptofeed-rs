# Security Policy

## Supported versions

Security fixes are applied to the default branch (`main`) of
[rsitech-ai/cryptofeed-rs](https://github.com/rsitech-ai/cryptofeed-rs). There are no
long-term supported release trains yet.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Prefer one of:

1. GitHub **private vulnerability reporting** on this repository (Security →
   Report a vulnerability).
2. Email RSI Tech confidentially at [info@rsitech.ai](mailto:info@rsitech.ai).

Include as much detail as you can: affected crate/component, reproduction
steps, impact, and whether a fix is already known.

We aim to acknowledge reports within a few business days. Please give a
reasonable window before public disclosure so a fix or mitigation can ship.

## Scope notes

- This project is a **market-data** engine (library-first). Do not send live
  API keys, session cookies, or private keys in reports; redact credentials.
- Supply-chain policy lives in `deny.toml` (licenses, advisories, banned
  sources). Dependency advisories are checked with `cargo deny`.
