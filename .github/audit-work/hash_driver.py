"""Temporary pinned-base validation; this runner is not part of the final PR."""
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

BASE = "01d4fb24503adc74508c8a6e3b35facf05cc204b"
ROOT = Path(__file__).resolve().parent
SOURCES = {
    "src/domain/mod.rs": "1d41e5465251da4e78abc17b1a6ef540973ba305",
    "src/modules/bridge/st_ops.rs": "96ce7c76583b165062288b4ea9d1e57c62b32d39",
    "src/modules/bridge/operation_payload.rs": "0e1c69a696a6494e4fb362973071e021e14480e1",
}
NEW = {"src/domain/locator.rs", "tests/locator_hash_contract.rs", "docs/locator-hash-compatibility.md"}


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f"ambiguous patch anchor: {old!r}")
    return text.replace(old, new, 1)


def measure(target):
    binary = Path(os.environ["CARGO_TARGET_DIR"]) / "release/im-bridge"
    raw = binary.read_bytes()
    source_paths = sorted((target / "src").rglob("*.rs"))
    return {
        "binary_bytes": len(raw),
        "gzip9_mtime0_bytes": len(gzip.compress(raw, compresslevel=9, mtime=0)),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "src_rs_physical_lines": sum(len(path.read_bytes().splitlines()) for path in source_paths),
        "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=target, text=True),
        "profile": "repository release profile; no overrides",
    }


def main():
    mode, directory = sys.argv[1:]
    target = Path(directory).resolve()
    if mode == "prepare":
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
        if head != BASE:
            raise RuntimeError("wrong audited base")
        for name, expected in SOURCES.items():
            raw = (target / name).read_bytes()
            actual = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
            if actual != expected:
                raise RuntimeError(f"unexpected source blob: {name}")
        if (target / "tests/locator_hash_contract.rs").exists():
            raise RuntimeError("test path already exists")
        shutil.copyfile(ROOT / "locator_hash_contract.rs", target / "tests/locator_hash_contract.rs")
    elif mode == "fix":
        declaration = target / "src/domain/mod.rs"
        declaration.write_text(replace_once(declaration.read_text(), "pub mod identity;\n", "pub mod identity;\npub(crate) mod locator;\n"))
        for name in ["src/modules/bridge/st_ops.rs", "src/modules/bridge/operation_payload.rs"]:
            path = target / name
            text = path.read_text()
            start = text.index("pub fn locator_hash(")
            end = text.index("\n}\n", start) + len("\n}\n")
            text = text[:start] + text[end:]
            if name.endswith("operation_payload.rs"):
                text = replace_once(text, "use sha2::{Digest, Sha256};\n", "")
                anchor = "const KEY_LEN: usize = 32;"
            else:
                anchor = "const RECENT_LIMIT: usize = 24;"
            text = replace_once(text, anchor, "pub use crate::domain::locator::locator_hash;\n\n" + anchor)
            path.write_text(text)
        for name, destination in [
            ("locator.rs", "src/domain/locator.rs"),
            ("locator-hash-compatibility.md", "docs/locator-hash-compatibility.md"),
        ]:
            if (target / destination).exists():
                raise RuntimeError("new candidate path already exists")
            shutil.copyfile(ROOT / name, target / destination)
    elif mode == "mutations":
        path = target / "src/domain/locator.rs"
        original = path.read_text()
        try:
            for name, old, new in [
                ("unicode-codepoint-length", "bytes.len() as u64", "part.chars().count() as u64"),
                ("little-endian-length", ".to_be_bytes()", ".to_le_bytes()"),
            ]:
                path.write_text(replace_once(original, old, new))
                result = subprocess.run(
                    ["cargo", "test", "--locked", "--test", "locator_hash_contract"],
                    cwd=target, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
                print(f"Mutation: {name}\n{result.stdout}")
                if result.returncode == 0 or "test result: FAILED" not in result.stdout or "public_paths_match_fixed_vectors ... FAILED" not in result.stdout:
                    raise RuntimeError("mutation was not rejected by the intended runtime assertions")
        finally:
            path.write_text(original)
    elif mode == "allowlist":
        tracked = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=target, text=True).splitlines()
        untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], cwd=target, text=True).splitlines()
        if set(tracked + untracked) != set(SOURCES) | NEW:
            raise RuntimeError(f"unexpected candidate paths: {tracked + untracked!r}")
    elif mode in ("measure-baseline", "measure-candidate"):
        current = measure(target)
        result_path = Path(os.environ["RUNNER_TEMP"]) / "locator-hash-baseline.json"
        if mode == "measure-baseline":
            result_path.write_text(json.dumps(current, indent=2))
            print("BASELINE_MEASUREMENT=" + json.dumps(current))
        else:
            before = json.loads(result_path.read_text())
            report = {"before": before, "after": current, "delta": {
                key: current[key] - before[key]
                for key in ("binary_bytes", "gzip9_mtime0_bytes", "src_rs_physical_lines")
            }}
            print("COMPARISON_MEASUREMENT=" + json.dumps(report))
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
                summary.write("## Same-runner measurement\n\n```json\n" + json.dumps(report, indent=2) + "\n```\n")
    else:
        raise RuntimeError("unsupported validation mode")


if __name__ == "__main__":
    main()
