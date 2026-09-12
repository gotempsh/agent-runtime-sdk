#!/usr/bin/env python3
"""Validate Cargo's actual archive file list without publishing anything."""

import subprocess
import sys
import unittest
from pathlib import Path, PurePosixPath


ROOT_FILES = {
    ".cargo_vcs_info.json", "Cargo.toml", "Cargo.toml.orig", "Cargo.lock",
    "README.md", "CHANGELOG.md", "LICENSE-MIT", "LICENSE-APACHE", "NOTICE",
    "CONTRIBUTING.md", "SECURITY.md", "CODE_OF_CONDUCT.md",
}
REQUIRED = {"Cargo.toml", "src/lib.rs", "README.md", "LICENSE-MIT", "LICENSE-APACHE"}


def violations(files):
    """Only source, crate examples, tests, and documentation belong in the crate."""
    errors = []
    for name in files:
        path = PurePosixPath(name)
        parts = path.parts
        unsafe = path.is_absolute() or ".." in parts or any(
            part in {"node_modules", "target", ".git", ".data", "auth.json"}
            or part.startswith(".env") for part in parts
        )
        allowed = (
            name in ROOT_FILES
            or (len(parts) > 1 and parts[0] in {"src", "docs", "tests"})
            or (len(parts) == 2 and parts[0] == "examples" and path.suffix == ".rs")
        )
        if unsafe or not allowed:
            errors.append(f"Unexpected archive file: {name}")
    errors.extend(f"Missing required archive file: {name}" for name in sorted(REQUIRED - set(files)))
    return errors


class PackagePolicyTests(unittest.TestCase):
    def test_required_source_is_accepted(self):
        self.assertEqual(violations(REQUIRED | {"examples/run_turn.rs", "docs/daemon-stream.md"}), [])

    def test_nested_readme_does_not_bypass_root_allowlist(self):
        self.assertTrue(violations(REQUIRED | {"site/node_modules/vendor/README.md"}))

    def test_secrets_and_build_output_are_rejected(self):
        for name in ["docs/.env", "tests/auth.json", "src/target/cache", "foo.txt", "../README.md"]:
            with self.subTest(name=name):
                self.assertTrue(violations(REQUIRED | {name}))

    def test_missing_source_is_rejected(self):
        self.assertTrue(violations(REQUIRED - {"src/lib.rs"}))


def main():
    result = subprocess.run(
        ["cargo", "package", "--list", "--locked", "--allow-dirty"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        return result.returncode
    files = result.stdout.splitlines()
    errors = violations(files)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Package boundaries verified ({len(files)} files); nothing published.")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        unittest.main(argv=[sys.argv[0]])
    else:
        sys.exit(main())
