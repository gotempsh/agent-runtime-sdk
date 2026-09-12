# Open-source readiness

This is a source-readiness assessment, not authorization to publish, a legal
opinion, or a claim that every provider integration has been tested live.

## Code-side changes

- Root-anchored Cargo package inclusion excludes dependency trees and local
  applications. `scripts/check-package.py` validates Cargo's actual archive
  listing, with regression tests for nested README matches, local artifacts,
  credentials, and missing source files. This is a path-boundary check, not a
  substitute for secret scanning of file contents.
- Rust 1.88 is the advertised and CI-checked minimum, matching locked dependencies.
- CI checks independent feature builds, all-feature tests, strict Clippy and
  Rustdoc, package boundaries, and compilation of the standalone crate archive.
- Codex model relay credentials default to HTTPS except on loopback. Trusted
  embedders may explicitly select one isolated HTTP origin on the adapter;
  requests cannot grant themselves this exception. Host-side authorization of
  relay destinations and token references remains mandatory.
- Historical protocol fixes isolate slow consumers and stop disposed event
  pumps before clearing journals. MCP management rejects credential-bearing
  argv, Agent Relay rejects remote plaintext HTTP, and OpenCode emits session
  startup events. Each has focused regression coverage.
- The daemon guide and security policy explicitly describe carrier authentication,
  per-principal host isolation, trusted configuration, and lifecycle ownership.

## Reproduce the local checks

Run from the repository root:

```sh
cargo fmt --all -- --check
cargo check --locked --lib
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo +1.88.0 check --locked --all-targets --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps
python3 scripts/check-package.py --self-test
python3 scripts/check-package.py
cargo package --locked --allow-dirty
gitleaks git --redact --no-banner --log-opts=--all .
```

`--allow-dirty` verifies the current working tree without committing or publishing.
Remove it for the release checkout. CI also checks no-default and each individual
feature with warnings denied. The live Nono test is separately opt-in and requires
the Nono executable; mock-provider tests do not require credentials.

## Audit scorecard (2026-09-12)

Scores reflect repository evidence, not hosted settings or distribution readiness.

| Area | /10 | Evidence / remaining gap |
| --- | ---: | --- |
| README | 9 | Purpose, boundaries, quickstart, features, license links |
| Installation | 8 | Independent crate and verified compiler minimum; publication remains pending |
| Documentation | 9 | Tutorials, guides, protocol references, daemon security boundaries |
| Examples | 8 | CLI examples and full-stack app; live-provider matrix not rerun in this audit |
| Contributing | 9 | Local checks, regression-test rules, PR template |
| CI | 8 | OS/feature/MSRV/package checks defined; hosted run of these changes still required |
| Issue templates | 9 | Bug and feature forms request reproduction and security context |
| Licensing | 8 | Existing MIT/Apache files, NOTICE, and manifest agree; owner confirms provenance |
| Security | 8 | Policy, bounded stream tests, relay hardening, local history scan; hosting settings unverified |
| Governance | 6 | Contribution/conduct policies exist; maintainer contacts and release ownership need confirmation |
| **Total** | **82** | **Code verification is distinct from public-release approval** |

## Owner-controlled release checklist

1. Confirm ownership/provenance and the existing MIT OR Apache-2.0 licensing choice.
   Review third-party attribution; the crate invokes provider CLIs rather than
   bundling them. No license terms were changed in this audit.
2. Choose repository visibility, maintainers, reporting contacts, and support policy.
   Enable private vulnerability reporting before relying on the SECURITY.md channel.
3. Configure branch protection and require a green hosted CI run, including Windows
   and Linux. Local macOS results do not replace those platform checks.
4. Decide package/version ownership, registry credentials, GitHub Pages, release
   tags, and image distribution. Existing release/docs workflows can publish on
   their configured triggers; review permissions and environments before enabling
   them. This audit does not run those workflows or publish any artifacts.
5. Run a fresh dependency-advisory and secret scan on the exact release commit.
   The local history scan covered 61 commits without findings; the local advisory
   scan used cached data, so neither is a perpetual guarantee.
6. Verify consuming applications against the selected SDK revision and images.
   In particular, remote HTTP model relays now need TLS, a loopback tunnel, or
   the explicitly configured isolated-origin host policy described in SECURITY.md.
SDK readiness does not certify Temps/Fleet authorization, preview routing,
   runtime-image compatibility, or live provider behavior.

Do not include unrelated local files or credentials when staging a release.
