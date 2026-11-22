# Dependency audit policy

CI runs `cargo audit --deny unsound` and fails on all non-ignored vulnerability advisories and all unsoundness advisories. Four unmaintained-but-not-vulnerable dependencies (`chrono`, `humantime`, `paste`, `unsafe-libyaml`) remain visible as warnings; track upstream replacements rather than suppressing them.

Two narrowly scoped exceptions are currently required:

- `RUSTSEC-2023-0071` (`rsa` 0.9.x Marvin timing side-channel): pulled by SQLx 0.8's MySQL driver, which constrains RSA to the 0.9 major line. The advisory's fix is 0.10, which SQLx 0.8 cannot consume. Remove this exception when SQLx's supported MySQL driver no longer requires the vulnerable RSA line. Use TLS for MySQL connections.
- `RUSTSEC-2026-0459` (`libsqlite3-sys`): present in Cargo.lock as an optional SQLx dependency, but SQLite support is not enabled in Dataspool. `cargo tree --all-features -i libsqlite3-sys` reports no active dependency path. Keep SQLite disabled and remove this exception if SQLx's lock resolution stops including the optional crate.

Do not add further ignores without a written rationale and review. Do not enable SQLx SQLite or non-TLS MySQL RSA authentication under this policy.
