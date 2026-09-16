#!/usr/bin/env python3
"""Fail closed when a public release candidate contains private or unsafe artifacts."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
SKIP_PARTS = {".git", ".codex", "target", "data", "tmp", "__pycache__"}
TEXT_SUFFIXES = {
    "", ".css", ".html", ".js", ".json", ".jsonl", ".lock", ".md",
    ".rs", ".service", ".sh", ".sql", ".toml", ".txt", ".yaml", ".yml",
}
FORBIDDEN_NAMES = re.compile(
    r"(?i)(?:^|\.)(?:db|sqlite3?|bak|backup|log|pem|p12|pfx|jks)$"
)
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "JWT": re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    "Telegram bot token": re.compile(r"\b\d{5,16}:A[A-Za-z0-9_-]{30,}\b"),
}
# Split internal markers so this checker does not self-match.
PRIVATE_MARKERS = [
    "192" + ".168.",
    "@new_" + "rin",
    "@rin_" + "st",
    "sillytavern" + "-jg",
    "C:" + "\\Users\\Mashiro",
    "Z:" + "\\",
]
MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")


def candidate_files() -> list[Path]:
    return sorted(
        (
            path
            for path in ROOT.rglob("*")
            if path.is_file()
            and not any(part in SKIP_PARTS for part in path.relative_to(ROOT).parts)
        ),
        key=lambda path: path.relative_to(ROOT).as_posix(),
    )


def read_text(path: Path) -> str | None:
    if path.suffix.lower() not in TEXT_SUFFIXES and path.name not in {".gitignore", ".env.example"}:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def check_markdown_link(path: Path, target: str) -> str | None:
    target = target.strip().split(maxsplit=1)[0].strip("<>")
    if not target or target.startswith(("http://", "https://", "mailto:", "#")):
        return None
    relative = unquote(target.split("#", 1)[0].split("?", 1)[0])
    if not relative:
        return None
    resolved = (path.parent / relative).resolve()
    try:
        resolved.relative_to(ROOT)
    except ValueError:
        return f"link escapes repository: {target}"
    if not resolved.exists():
        return f"missing link target: {target}"
    return None


def main() -> int:
    failures: list[str] = []
    files = candidate_files()
    for path in files:
        rel = path.relative_to(ROOT).as_posix()
        lower_name = path.name.lower()
        if lower_name == ".env" or (lower_name.startswith(".env.") and lower_name != ".env.example"):
            failures.append(f"{rel}: local environment file is not publishable")
        if lower_name.startswith("master.key") or FORBIDDEN_NAMES.search(lower_name):
            failures.append(f"{rel}: forbidden runtime/secret artifact")
        if path.stat().st_size > 2 * 1024 * 1024:
            failures.append(f"{rel}: file exceeds the 2 MiB publication limit")
        text = read_text(path)
        if text is None:
            continue
        if path.suffix == ".sh" and "\r" in text:
            failures.append(f"{rel}: shell script uses CRLF/CR line endings")
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                failures.append(f"{rel}: matched {label}")
        for marker in PRIVATE_MARKERS:
            if marker.casefold() in text.casefold():
                failures.append(f"{rel}: matched private topology marker")
        if path.suffix.lower() == ".md":
            for match in MARKDOWN_LINK.finditer(text):
                error = check_markdown_link(path, match.group(1))
                if error:
                    failures.append(f"{rel}: {error}")

    if failures:
        print("publication gate: FAILED", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    manifest = hashlib.sha256()
    for path in files:
        manifest.update(path.relative_to(ROOT).as_posix().encode())
        manifest.update(b"\0")
        manifest.update(hashlib.sha256(path.read_bytes()).digest())
    print(f"publication gate: PASS ({len(files)} files, manifest {manifest.hexdigest()[:16]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
