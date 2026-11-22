# Release Process

Dataspool is not yet a production-ready general-purpose migration platform. The current executable starts the DLQ administration API; several planned cluster-control, gRPC, Raft, and local Lucene-reader paths remain incomplete. Until those capabilities are implemented and validated end to end, releases must remain draft/pre-release artifacts and must not be presented as stable production releases.

## Creating a preview release

1. Update `[package].version` in `Cargo.toml` and `Cargo.lock` as appropriate.
2. Update `CHANGELOG.md` with the scope and known limitations of the preview.
3. Commit and merge the release changes to `main`.
4. Create and push a matching version tag, for example `v0.1.0-alpha.1`:

   ```bash
   git tag v0.1.0-alpha.1
   git push origin v0.1.0-alpha.1
   ```

5. The Release workflow verifies that the tag matches the Cargo package version, builds the locked release binary on Ubuntu 24.04 x86_64, packages it with the license and project documentation, and publishes a **draft pre-release** with a SHA-256 checksum.
6. Review the workflow result, artifact contents, release notes, and known limitations before manually publishing the GitHub Release.

The initial release workflow only produces `linux-x86_64` artifacts. Linux ARM64 and macOS artifacts are not currently provided.

## Release gates before stable 1.0

- Implement and integrate real multi-node Raft consensus and durable state-machine application.
- Replace placeholder gRPC handlers with working cluster and task APIs.
- Complete or explicitly remove unsupported drivers and local Lucene segment reading.
- Provide an actual Control/Agent startup and task execution path; document the supported runtime configuration.
- Add end-to-end tests for task lifecycle, restart/recovery, multi-node failover, authentication, and supported source/sink combinations.
- Verify installation, upgrade, backup/restore, and operational procedures on supported Linux architectures.
- Publish a support matrix and accurate feature documentation; remove claims that describe unimplemented capabilities as available.
