"""Read-only repository probe. Candidate changes stay in an ephemeral worktree."""
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

root = Path(sys.argv[1]).resolve()


def run(*args):
    result = subprocess.run(args, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        print(result.stderr)
        raise RuntimeError(f"probe command failed: {args!r}")
    return result.stdout


def tree(all_features=False):
    args = ["cargo", "tree", "--locked", "--target", "x86_64-unknown-linux-gnu", "--edges", "normal,build", "--prefix", "none", "--format", "{p}|{f}"]
    if all_features:
        args.append("--all-features")
    lines = run(*args).splitlines()
    return sorted(set(lines))


def package_set(lock):
    return {(p["name"], p["version"], p.get("source", ""), p.get("checksum", "")) for p in lock["package"]}


references = []
for name in run("git", "ls-files").splitlines():
    path = root / name
    if path.suffix == ".rs":
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if re.search(r"\bteloxide\b", line):
                references.append({"path": name, "line": number, "text": line.strip()})
print("RUST_REFERENCE_SCAN=" + json.dumps(references))
before_meta = json.loads(run("cargo", "metadata", "--locked", "--all-features", "--format-version", "1"))
before_lock = tomllib.loads((root / "Cargo.lock").read_text())
before_tree = tree()
before_all = tree(True)
for package in before_meta["packages"]:
    if package["name"] in {"teloxide", "teloxide-core", "reqwest", "sqlx", "sqlx-macros", "sqlx-macros-core"}:
        print("PACKAGE_FEATURE_DECLARATION=" + json.dumps({"name": package["name"], "version": package["version"], "features": package["features"], "dependencies": package["dependencies"]}))
manifest = root / "Cargo.toml"
text = manifest.read_text()
lines = text.splitlines(keepends=True)
removed = [line for line in lines if line.startswith("teloxide = ")]
if len(removed) != 1:
    raise RuntimeError("expected exactly one direct teloxide declaration")
manifest.write_text("".join(line for line in lines if not line.startswith("teloxide = ")))
after_meta = json.loads(run("cargo", "metadata", "--all-features", "--format-version", "1"))
after_lock = tomllib.loads((root / "Cargo.lock").read_text())
after_tree = tree()
after_all = tree(True)
old = package_set(before_lock)
new = package_set(after_lock)
print("LOCK_PACKAGE_DIFF=" + json.dumps({"before": len(old), "after": len(new), "removed": sorted(old-new), "added_or_changed": sorted(new-old)}))
for label, first, second in [("default", before_tree, after_tree), ("all_features", before_all, after_all)]:
    focus = {"reqwest", "rustls", "hyper", "hyper-util", "tokio", "tokio-rustls", "rustls-native-certs", "webpki-roots", "tower-http"}
    a = [line for line in first if line.split()[0] in focus]
    b = [line for line in second if line.split()[0] in focus]
    print("RESOLVED_NETWORK_FEATURES=" + json.dumps({"configuration": label, "before": a, "after": b}))
print("MANIFEST_DIFF=" + run("git", "diff", "--", "Cargo.toml"))
print("Probe only: no candidate committed, pushed or approved; test/build/feature-preservation decisions remain separate.")
