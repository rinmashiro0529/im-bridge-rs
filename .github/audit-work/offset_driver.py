"""Temporary, pinned-base patch preparation; never included in the final PR."""
import hashlib
import re
import shutil
import subprocess
import sys
from pathlib import Path

BASE = "01d4fb24503adc74508c8a6e3b35facf05cc204b"
SOURCE_BLOB = "2c5a1d3fbe9f7599194a2fe76d030a46427974cb"
MODULE = "src/modules/telegram/mod.rs"
TESTS = "src/modules/telegram/inbox_offset_tests.rs"
NOTE = "docs/inbox-offset-atomicity.md"
ROOT = Path(__file__).resolve().parent


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f"expected one matching patch anchor: {old!r}")
    return text.replace(old, new, 1)


def main():
    mode, directory = sys.argv[1:]
    target = Path(directory).resolve()
    source = target / MODULE
    if mode == "prepare":
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
        if head != BASE:
            raise RuntimeError("candidate does not start at the audited commit")
        raw = source.read_bytes()
        digest = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
        if digest != SOURCE_BLOB:
            raise RuntimeError("base Telegram module blob changed")
        text = replace_once(raw.decode(), "pub mod stream;\n", "pub mod stream;\n\n#[cfg(test)]\nmod inbox_offset_tests;\n")
        source.write_text(text)
        for name, destination in [("inbox_offset_tests.rs", TESTS), ("inbox-offset-atomicity.md", NOTE)]:
            if (target / destination).exists():
                raise RuntimeError("candidate would overwrite a pre-existing file")
            shutil.copyfile(ROOT / name, target / destination)
    elif mode == "red":
        result = subprocess.run(
            ["cargo", "test", "--locked", "--lib", "modules::telegram::inbox_offset_tests", "--", "--nocapture"],
            cwd=target, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        print(result.stdout)
        prefix = "modules::telegram::inbox_offset_tests::"
        failures = set(re.findall(r"^test (\S+) \.\.\. FAILED$", result.stdout, re.MULTILINE))
        expected = {prefix + name for name in [
            "rollback_on_identity_conflict", "rollback_on_offset_write_failure",
            "rollback_on_deferred_commit_failure", "rollback_on_invalid_update_id",
        ]}
        if result.returncode == 0 or failures != expected or "4 passed; 4 failed" not in result.stdout:
            raise RuntimeError("baseline did not fail exactly the intended regression assertions")
        print("Baseline: all four intended regression cases failed; all four compatibility cases passed.")
    elif mode == "fix":
        text = source.read_text()
        start = text.index("async fn persist_update_batch(\n")
        end = text.index("\nasync fn telegram_api_request(\n", start)
        body = text[start:end]
        body = replace_once(body, "    let mut persisted = Vec::new();\n", "    let mut persisted = Vec::new();\n    let mut candidate_offset = *offset;\n")
        body = replace_once(body, "        *offset = (*offset).max(next_offset);", "        candidate_offset = candidate_offset.max(next_offset);")
        body = replace_once(body, "    .bind(*offset)", "    .bind(candidate_offset)")
        body = replace_once(body, "    tx.commit().await?;\n    Ok(persisted)", "    tx.commit().await?;\n    // Publish only after both inbox rows and the durable offset have committed.\n    *offset = candidate_offset;\n    Ok(persisted)")
        source.write_text(text[:start] + body + text[end:])
    elif mode == "allowlist":
        tracked = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=target, text=True).splitlines()
        untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], cwd=target, text=True).splitlines()
        if set(tracked + untracked) != {MODULE, TESTS, NOTE}:
            raise RuntimeError(f"unexpected candidate paths: {tracked + untracked!r}")
    else:
        raise RuntimeError("unsupported preparation mode")


if __name__ == "__main__":
    main()
