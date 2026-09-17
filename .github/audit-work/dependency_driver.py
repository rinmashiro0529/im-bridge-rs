"""Ephemeral validation for D01. This driver is excluded from the final PR."""
import gzip
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

BASE = "01d4fb24503adc74508c8a6e3b35facf05cc204b"
ROOT = Path(__file__).resolve().parent
STATE = Path(os.environ["RUNNER_TEMP"]) / "dependency-validation-state.json"
FOCUS = {"reqwest", "rustls", "hyper", "hyper-util", "hyper-rustls", "tokio", "tokio-rustls", "rustls-native-certs", "webpki-roots", "tower-http"}
TARGETS = ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "aarch64-apple-darwin"]


def run(root, *args):
    result = subprocess.run(args, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        print(result.stderr)
        raise RuntimeError(f"validation command failed: {args!r}")
    return result.stdout


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f"unexpected patch anchor: {old!r}")
    return text.replace(old, new, 1)


def graph(root):
    result = {}
    for target in TARGETS:
        for all_features in [False, True]:
            args = ["cargo", "tree", "--locked", "--target", target, "--edges", "normal,build", "--prefix", "none", "--format", "{p}|{f}"]
            if all_features:
                args.append("--all-features")
            nodes = {}
            for line in run(root, *args).splitlines():
                package, features = line.split("|", 1)
                name = package.split()[0]
                if name in FOCUS:
                    nodes.setdefault(package, set()).update(filter(None, features.removesuffix(" (*)").split(",")))
            result[f"{target}:{'all' if all_features else 'default'}"] = {key: sorted(value) for key, value in sorted(nodes.items())}
    return result


def packages(root):
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    return sorted([p["name"], p["version"], p.get("source", ""), p.get("checksum", "")] for p in lock["package"])


def smoke(root, binary):
    # Clear project settings inherited from a runner. No project or API secrets
    # are needed; the publish token exists only in the later publish step.
    env = {key: value for key, value in os.environ.items() if not key.startswith("IMBRIDGE_")}
    help_output = {}
    for command in [[], ["serve"], ["doctor"], ["bootstrap-admin"], ["import-st"], ["export-st"], ["backup"], ["rotate-master-key"]]:
        result = subprocess.run([str(binary), *command, "--help"], cwd=root, env=env, text=True, capture_output=True, timeout=10)
        if result.returncode:
            raise RuntimeError(f"help command failed: {command}")
        help_output[" ".join(command) or "root"] = [result.returncode, result.stdout, result.stderr]
    assets = {}
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for signum in [signal.SIGTERM, signal.SIGINT]:
        with tempfile.TemporaryDirectory(prefix="imbridge-validation-") as directory:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            current_env = dict(env, IMBRIDGE_LISTEN=f"127.0.0.1:{port}", IMBRIDGE_DATA_DIR=directory, IMBRIDGE_MASTER_KEY_PATH=str(Path(directory) / "master.key"), IMBRIDGE_COOKIE_SECURE="false", IMBRIDGE_ST_MODE="disabled", RUST_LOG="error")
            process = subprocess.Popen([str(binary), "serve"], cwd=root, env=current_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            try:
                deadline = time.monotonic() + 30
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("isolated server exited before becoming live")
                    try:
                        with opener.open(f"http://127.0.0.1:{port}/health/live", timeout=1) as response:
                            if response.status == 200:
                                break
                    except (urllib.error.URLError, TimeoutError):
                        pass
                    if time.monotonic() > deadline:
                        raise RuntimeError("isolated server did not become live")
                    time.sleep(0.1)
                observed = {}
                for path in ["/health/live", "/health/ready", "/", "/app.js", "/style.css"]:
                    with opener.open(f"http://127.0.0.1:{port}{path}", timeout=3) as response:
                        if response.status != 200:
                            raise RuntimeError(f"unexpected local response: {path}")
                        observed[path] = {"sha256": hashlib.sha256(response.read()).hexdigest(), "type": response.headers.get("content-type"), "nosniff": response.headers.get("x-content-type-options")}
                try:
                    opener.open(f"http://127.0.0.1:{port}/api/v1/auth/me", timeout=3)
                    raise RuntimeError("anonymous administration unexpectedly succeeded")
                except urllib.error.HTTPError as error:
                    if error.code != 401:
                        raise
                    error.close()
                process.send_signal(signum)
                output, _ = process.communicate(timeout=10)
                if process.returncode != 0:
                    raise RuntimeError(f"signal shutdown failed: {signum}; {process.returncode}; {output}")
                assets[str(signum)] = observed
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=5)
    return {"help": help_output, "assets_by_signal": assets}


def measurement(root):
    binary = Path(os.environ["CARGO_TARGET_DIR"]) / "release/im-bridge"
    raw = binary.read_bytes()
    return {"binary_bytes": len(raw), "gzip9_mtime0_bytes": len(gzip.compress(raw, compresslevel=9, mtime=0)), "sha256": hashlib.sha256(raw).hexdigest(), "rustc": run(root, "rustc", "-Vv"), "smoke": smoke(root, binary)}


def main():
    mode, directory = sys.argv[1:]
    root = Path(directory).resolve()
    if mode == "prepare":
        if run(root, "git", "rev-parse", "HEAD").strip() != BASE:
            raise RuntimeError("wrong audited base")
        expected = "3f6f44a6f03fac3d91407c6dac13589a2899925a"
        if run(root, "git", "hash-object", "Cargo.toml").strip() != expected:
            raise RuntimeError("manifest changed")
        references = []
        for name in run(root, "git", "ls-files").splitlines():
            if name.endswith(".rs"):
                for number, line in enumerate((root / name).read_text().splitlines(), 1):
                    if re.search(r"\bteloxide\b", line):
                        references.append(f"{name}:{number}")
        if references:
            raise RuntimeError(f"potential crate references require review: {references!r}")
        run(root, "cargo", "metadata", "--locked", "--all-features", "--format-version", "1")
        state = {"base": BASE, "before_packages": packages(root), "before_graph": graph(root), "rust_reference_scan": references}
        STATE.write_text(json.dumps(state, indent=2))
        destination = root / "tests/transport_dependency_contract.rs"
        if destination.exists():
            raise RuntimeError("test path already exists")
        shutil.copyfile(ROOT / destination.name, destination)
        print("Baseline reference scan and eight target/feature graph views recorded.")
    elif mode == "baseline":
        state = json.loads(STATE.read_text())
        state["baseline"] = measurement(root)
        STATE.write_text(json.dumps(state, indent=2))
        print("BASELINE_RELEASE=" + json.dumps({k: v for k, v in state["baseline"].items() if k != "smoke"}))
        print("Baseline smoke: eight CLI help pages, local API/assets, anonymous 401, SIGTERM and SIGINT passed.")
    elif mode == "fix":
        manifest = root / "Cargo.toml"
        text = replace_once(manifest.read_text(), 'teloxide = { version = "0.17", default-features = false, features = ["ctrlc_handler", "rustls"] }\n', "")
        text = replace_once(text,
            'reqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls-native-roots"] }',
            '# Preserve the TLS root sources and multipart support previously unified through teloxide.\nreqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls-native-roots", "rustls-tls", "multipart"] }')
        manifest.write_text(text)
        # Cargo, not hand editing, updates the dependency closure. All retained
        # registry versions/checksums must remain identical to the baseline.
        run(root, "cargo", "metadata", "--all-features", "--format-version", "1")
        state = json.loads(STATE.read_text())
        after = packages(root)
        old = {tuple(item) for item in state["before_packages"]}
        new = {tuple(item) for item in after}
        if new-old:
            raise RuntimeError(f"unexpected dependency additions/upgrades: {sorted(new-old)}")
        if {"teloxide", "teloxide-core", "proc-macro-error2"} & {item[0] for item in after}:
            raise RuntimeError("removed dependency closure is still present")
        new_graph = graph(root)
        if new_graph != state["before_graph"]:
            print("BEFORE_GRAPH=" + json.dumps(state["before_graph"]))
            print("AFTER_GRAPH=" + json.dumps(new_graph))
            raise RuntimeError("resolved transport capabilities changed")
        policy = root / "deny.toml"
        policy.write_text(replace_once(policy.read_text(), '# Transitive via teloxide -> aquamarine. RUSTSEC marks it unmaintained, not\n# vulnerable, and provides no safe upgrade. Revisit when teloxide removes it.\nignore = ["RUSTSEC-2026-0173"]\n', ""))
        state["after_packages"] = after
        state["removed_packages"] = [list(item) for item in sorted(old-new)]
        state["network_graph_preserved"] = True
        STATE.write_text(json.dumps(state, indent=2))
        shutil.copyfile(ROOT / "unused-telegram-dependency.md", root / "docs/unused-telegram-dependency.md")
        print("DEPENDENCY_RESULT=" + json.dumps({"before": len(old), "after": len(new), "removed": state["removed_packages"], "network_graph_preserved": True}))
    elif mode == "candidate":
        state = json.loads(STATE.read_text())
        current = measurement(root)
        if current["smoke"] != state["baseline"]["smoke"]:
            raise RuntimeError("release CLI, local API/assets or shutdown smoke differs from baseline")
        report = {"base": BASE, "before": {k: v for k, v in state["baseline"].items() if k != "smoke"}, "after": {k: v for k, v in current.items() if k != "smoke"}, "delta": {k: current[k]-state["baseline"][k] for k in ["binary_bytes", "gzip9_mtime0_bytes"]}, "lock_packages_before": len(state["before_packages"]), "lock_packages_after": len(state["after_packages"]), "network_graph_preserved": state["network_graph_preserved"], "release_smoke_identical": True}
        print("DEPENDENCY_RELEASE_COMPARISON=" + json.dumps(report))
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write("## Dependency and release evidence\n\n```json\n" + json.dumps(report, indent=2) + "\n```\n")
    elif mode == "allowlist":
        names = run(root, "git", "diff", "--name-only", "HEAD").splitlines() + run(root, "git", "ls-files", "--others", "--exclude-standard").splitlines()
        if set(names) != {"Cargo.toml", "Cargo.lock", "deny.toml", "tests/transport_dependency_contract.rs", "docs/unused-telegram-dependency.md"}:
            raise RuntimeError(f"unexpected candidate paths: {names!r}")
    else:
        raise RuntimeError("unsupported mode")


if __name__ == "__main__":
    main()
