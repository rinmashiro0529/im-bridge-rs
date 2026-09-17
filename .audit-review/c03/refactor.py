"""C03: extract identical recovery validation blocks, preserving entry order."""
from pathlib import Path
import hashlib

BASE_BLOB = '2a82f12a10ad6ae96eb470184950f2558053af73'
PATH = Path('src/modules/bridge/operation_coordinator.rs')

def transform(source: str) -> str:
    start = source.index('    pub fn decrypt_recovery_payload(')
    middle = source.index('    pub async fn decrypt_recovery_payload_for_operation(', start)
    end = source.index('    pub async fn replay_generated(', middle)
    sync, asynchronous = source[start:middle], source[middle:end]
    payload_marker = '        let Some(encrypted) = record.payload.as_ref() else {'
    scope_marker = '        if !fence.is_valid()'
    aad_marker = '        let aad = OperationAad::new_with_fence('
    def parts(block):
        p, s, a = (block.index(x) for x in (payload_marker, scope_marker, aad_marker))
        return block[:p], block[p:s], block[s:a], block[a:]
    sp, shared, ss, core = parts(sync)
    ap, check, ass, other_core = parts(asynchronous)
    assert shared == check, 'prechecks diverged; stop rather than infer equivalence'
    assert core == other_core, 'decode blocks diverged; stop rather than infer equivalence'
    def entry(prefix, scope):
        return (prefix + '        let encrypted = Self::recovery_payload(record)?;\n' + scope
                + '        Self::decode_and_validate_recovery_payload(encryptor, record, encrypted, fence)\n'
                + '    }\n\n')
    async_entry = entry(ap, ass).replace('(encryptor, record, encrypted, fence)',
                                          '(&encryptor, record, encrypted, fence)')
    precheck = ('    fn recovery_payload(\n'
                '        record: &BridgeOperationRecord,\n'
                '    ) -> AppResult<&EncryptedOperationPayload> {\n'
                + shared + '        Ok(encrypted)\n    }\n\n')
    decoder = ('    // Entry points retain key-source and scope checks before this shared core.\n'
               '    fn decode_and_validate_recovery_payload(\n'
               '        encryptor: &OperationPayloadEncryptor,\n'
               '        record: &BridgeOperationRecord,\n'
               '        encrypted: &EncryptedOperationPayload,\n'
               '        fence: &PollerRuntimeFence,\n'
               '    ) -> AppResult<OperationRecoveryPayload> {\n' + core)
    return source[:start] + entry(sp, ss) + async_entry + precheck + decoder + source[end:]

if __name__ == '__main__':
    raw = PATH.read_bytes()
    blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
    if blob != BASE_BLOB:
        raise SystemExit('STOP: coordinator blob differs from reviewed baseline: ' + blob)
    changed = transform(raw.decode())
    PATH.write_text(changed)
    print('C03 coordinator lines:', len(raw.splitlines()), '->', len(changed.splitlines()))
