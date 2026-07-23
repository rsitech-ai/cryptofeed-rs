## Outcome

<!-- What changes for users or operators? -->

## Risk and compatibility

<!-- Protocol, schema, config, performance, security, or operational impact. -->

## Verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-targets --all-features`
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps`
- [ ] `cargo deny check`
- [ ] `./scripts/check-oss-readiness.sh`
- [ ] Behavior-changing code includes regression coverage
- [ ] Public behavior or configuration changes include documentation

## Security and data handling

- [ ] No secrets, private recordings, customer data, or workstation paths
- [ ] Security-sensitive details were reported privately
