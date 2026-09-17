#!/usr/bin/env python3
"""Run all local all-feature contracts; report the external suite separately."""
import json
import subprocess


def run(*args):
    subprocess.run(args, check=True)


def main():
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"], text=True
    ))
    package = next(p for p in metadata["packages"] if p["name"] == "im-bridge")
    targets = sorted(t["name"] for t in package["targets"] if "test" in t["kind"])
    assert "e2e_sidecar" in targets, "external suite was unexpectedly removed"
    run("cargo", "test", "--workspace", "--all-features", "--locked", "--no-run")
    run("cargo", "test", "--workspace", "--all-features", "--locked", "--lib", "--bins")
    args = ["cargo", "test", "--all-features", "--locked"]
    for target in targets:
        if target != "e2e_sidecar":
            args.extend(["--test", target])
    run(*args)
    print("NOT EXECUTED: e2e_sidecar requires the approved external isolated stack.", flush=True)
    print("Compiled external scenarios (discovery is not execution):", flush=True)
    run("cargo", "test", "--all-features", "--locked", "--test", "e2e_sidecar", "--", "--list")


if __name__ == "__main__":
    main()
