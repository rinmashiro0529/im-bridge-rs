#!/usr/bin/env bash
set -euo pipefail
python3 - "${ST_CHATS_DIR:-${ST_ROOT:-/srv/sillytavern}/data/example-user/chats}" <<'PY'
import json,os,sys
root=sys.argv[1]
total=with_integrity=missing=invalid=0
for dp,_,files in os.walk(root):
    for name in files:
        if not name.lower().endswith('.jsonl'):
            continue
        total+=1
        path=os.path.join(dp,name)
        try:
            with open(path,encoding='utf-8-sig') as f:
                line=f.readline()
            header=json.loads(line)
        except Exception:
            invalid+=1
            continue
        value=(header.get('chat_metadata') or {}).get('integrity') if isinstance(header,dict) else None
        if isinstance(value,str) and value:
            with_integrity+=1
        else:
            missing+=1
print('total='+str(total))
print('with_integrity='+str(with_integrity))
print('missing_integrity='+str(missing))
print('invalid_header='+str(invalid))
PY
