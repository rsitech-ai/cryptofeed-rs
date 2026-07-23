# Governance

cryptofeed-rs is an open-source project maintained by
[RSI Tech](https://rsitech.ai).

## Roles

- **Copyright owner:** Rafal Sikora.
- **Maintainer:** RSI Tech.
- **Current code reviewer:** the account listed in [`CODEOWNERS`](CODEOWNERS).
- **Contributors:** anyone whose accepted work appears in the repository.

Maintainers set technical direction, review and merge changes, manage releases,
and respond to security reports. Decisions prioritize correctness, deterministic
behavior, operational safety, compatibility, and evidence over schedule.

## Changes

Behavior changes should be proposed through a focused issue or pull request.
Important architecture decisions belong in `docs/adr/`. Pull requests require
maintainer review, green local release gates, and resolution of actionable
feedback before merge.

Maintainers may expedite a security or incident fix. Any bypassed checks must be
documented and completed promptly after containment.

## Releases

Only maintainers publish releases. A release tag must point at reviewed `main`,
match the Cargo workspace version, and include the checksums and evidence
described in [`RELEASING.md`](RELEASING.md).

## Contact

Public and confidential project contact:
[info@rsitech.ai](mailto:info@rsitech.ai).
