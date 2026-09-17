"""Execute auditable C03 old/new and negative-mutation checks in an isolated worktree."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone

BASE = '01d4fb24503adc74508c8a6e3b35facf05cc204b'
BRANCH = 'audit/recovery-payload-validation'
SOURCE = Path('src/modules/bridge/operation_coordinator.rs')
TEST = Path('tests/recovery_payload_contract.rs')
NOTE = Path('docs/recovery-payload-validation.md')
DATA = Path(__file__).resolve().parent
WORK = Path(os.environ['RUNNER_TEMP']) / 'c03-project'
REPORT = Path(os.environ['RUNNER_TEMP']) / 'c03-report.json'
records = []

def note(step, result, detail):
    entry = dict(time=datetime.now(timezone.utc).isoformat(), step=step, result=result, detail=detail)
    records.append(entry)
    REPORT.write_text(json.dumps(records, indent=2) + '\n')
    print('AUDIT ' + json.dumps(entry), flush=True)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as f:
            f.write(f'### {step}: {result}\n\n{detail}\n\n')

def run(*args, capture=False):
    print('$ ' + ' '.join(map(str, args)), flush=True)
    p = subprocess.run(list(map(str, args)), cwd=WORK, text=True,
                       stdout=subprocess.PIPE if capture else None,
                       stderr=subprocess.STDOUT if capture else None)
    if p.returncode:
        if capture: print(p.stdout, flush=True)
        raise RuntimeError(f'command failed ({p.returncode}): {args[0:3]}')
    return p.stdout if capture else ''

def mutation(name, old, replacement, exact_test):
    clean = (WORK / SOURCE).read_text()
    assert clean.count(old) == 1, f'{name}: ambiguous mutation'
    try:
        (WORK / SOURCE).write_text(clean.replace(old, replacement, 1))
        run('cargo', 'test', '--locked', '--test', TEST.stem, '--no-run')
        args = ['cargo', 'test', '--locked', '--test', TEST.stem, exact_test, '--', '--exact']
        print('$ ' + ' '.join(args), flush=True)
        p = subprocess.run(args, cwd=WORK, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(p.stdout, flush=True)
        if p.returncode == 0 or f'test {exact_test} ... FAILED' not in p.stdout or '0 passed; 1 failed;' not in p.stdout:
            raise RuntimeError(f'{name}: expected one compiled, failing regression test')
        note('S05/' + name, 'CAUGHT', f'Compiled successfully; exact test {exact_test} failed as required.')
    finally:
        (WORK / SOURCE).write_text(clean)

def verify():
    subprocess.run(['git', 'worktree', 'add', '--detach', str(WORK), BASE], check=True)
    assert run('git', 'rev-parse', 'HEAD', capture=True).strip() == BASE
    assert not run('git', 'status', '--porcelain', capture=True).strip()
    note('S03', 'PASS', f'Clean isolated worktree at {BASE}; no product branch modified.')
    shutil.copyfile(DATA / TEST.name, WORK / TEST)
    run('cargo', 'fmt', '--all')
    assert not run('git', 'diff', '--name-only', capture=True).strip(), 'formatter changed tracked baseline'
    run('cargo', 'test', '--locked', '--test', TEST.stem)
    note('S04', 'PASS', 'All 10 new contract tests pass on the unmodified production implementation.')
    run(sys.executable, DATA / 'refactor.py')
    shutil.copyfile(DATA / NOTE.name, WORK / NOTE)
    run('cargo', 'fmt', '--all')
    run('cargo', 'test', '--locked', '--test', TEST.stem)
    note('S05/equivalence', 'PASS', 'All 10 contract tests pass after factoring the same reviewed validation blocks.')
    mutation('digest-check', '            if actual != expected_digest {',
             '            if false && actual != expected_digest {',
             'mismatched_digest_is_rejected_for_every_variant')
    mutation('domain-check', '        payload.validate()?;\n        if let Some(expected_digest)',
             '        let _ = &payload;\n        if let Some(expected_digest)',
             'invalid_payload_is_rejected_before_digest')
    mutation('restore-not-create', 'return provider.restore_for_operation(operation_id).await;',
             'return provider.for_operation(operation_id).await;',
             'missing_reference_never_creates_a_key_and_precedes_payload_validation')
    run('cargo', 'fmt', '--all', '--check')
    run('git', 'diff', '--check')
    run('python3', 'scripts/publication_gate.py')
    run('cargo', 'clippy', '--all-targets', '--all-features', '--locked', '--', '-D', 'warnings')
    run('cargo', 'test', '--all', '--locked')
    run('cargo', 'build', '--release', '--locked')
    run('git', 'add', '--', SOURCE, TEST, NOTE)
    changed = set(run('git', 'diff', '--cached', '--name-only', capture=True).splitlines())
    assert changed == {str(SOURCE), str(TEST), str(NOTE)}, changed
    assert not run('git', 'diff', '--name-only', capture=True).strip()
    assert not run('git', 'ls-files', '--others', '--exclude-standard', capture=True).strip()
    run('git', 'diff', '--cached', '--check')
    stat = run('git', 'diff', '--cached', '--numstat', capture=True).strip()
    tree = run('git', 'write-tree', capture=True).strip()
    artifact = WORK / 'target/release/im-bridge'
    note('S06', 'PASS', 'Format, diff, publication, all-feature Clippy, full default tests and release build passed. '
         + 'Only three allowed files staged. Numstat:\n' + stat
         + '\nRelease SHA256 (not a size comparison): ' + hashlib.sha256(artifact.read_bytes()).hexdigest())
    (Path(os.environ['RUNNER_TEMP']) / 'c03-publish.json').write_text(json.dumps(dict(base=BASE, tree=tree, branch=BRANCH, numstat=stat)))

def publish():
    import base64
    data = json.loads((Path(os.environ['RUNNER_TEMP']) / 'c03-publish.json').read_text())
    assert data['base'] == BASE and data['branch'] == BRANCH
    head = run('git', 'ls-remote', 'origin', 'refs/heads/main', capture=True).split()[0]
    assert head == BASE, 'main moved; stop for rebase/review'
    assert not run('git', 'ls-remote', 'origin', 'refs/heads/' + BRANCH, capture=True).strip(), 'candidate already exists'
    env = os.environ.copy()
    env.update(GIT_AUTHOR_NAME='github-actions[bot]', GIT_COMMITTER_NAME='github-actions[bot]',
               GIT_AUTHOR_EMAIL='41898282+github-actions[bot]@users.noreply.github.com',
               GIT_COMMITTER_EMAIL='41898282+github-actions[bot]@users.noreply.github.com')
    sha = subprocess.check_output(['git', 'commit-tree', data['tree'], '-p', BASE, '-m',
                                  'refactor(bridge): share recovery payload validation without changing entry contracts'],
                                 cwd=WORK, text=True, env=env).strip()
    token = env.pop('PUBLISH_TOKEN')
    header = base64.b64encode(('x-access-token:' + token).encode()).decode()
    env.update(GIT_CONFIG_COUNT='1', GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',
               GIT_CONFIG_VALUE_0='AUTHORIZATION: basic ' + header)
    subprocess.run(['git', 'push', 'origin', sha + ':refs/heads/' + BRANCH], cwd=WORK, env=env, check=True)
    print('PUBLISHED_COMMIT=' + sha, flush=True)
    if REPORT.exists(): records.extend(json.loads(REPORT.read_text()))
    note('S07/candidate', 'PUBLISHED', f'Published {BRANCH} at {sha}; parent {BASE}. PR creation and routine checks are separate.')

if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['publish']:
            publish()
        else:
            verify()
    except Exception as exc:
        note('EXECUTION', 'FAILED', str(exc))
        raise
