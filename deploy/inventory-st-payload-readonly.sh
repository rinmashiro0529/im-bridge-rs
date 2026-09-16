#!/usr/bin/env bash
set -euo pipefail
python3 - "${ST_CHATS_DIR:-${ST_ROOT:-/srv/sillytavern}/data/example-user/chats}" <<'PY'
import os,sys
root=sys.argv[1]
rows=[]
for dp,_,files in os.walk(root):
    for name in files:
        if name.lower().endswith('.jsonl'):
            path=os.path.join(dp,name)
            size=os.path.getsize(path)
            with open(path,'rb') as f:
                lines=sum(1 for _ in f)
            rel=os.path.relpath(path,root)
            rows.append((size,lines,rel))
rows.sort(reverse=True)
print('jsonl_count='+str(len(rows)))
if rows:
    size,lines,rel=rows[0]
    print('max_jsonl_bytes='+str(size))
    print('max_jsonl_lines='+str(lines))
    import hashlib
    print('max_jsonl_path_sha256='+hashlib.sha256(rel.encode()).hexdigest())
for i,(size,lines,rel) in enumerate(rows[:5],1):
    print(f'top{i}_bytes={size},lines={lines}')
PY

echo '== body parser references =='
docker exec "$container" sh -lc "grep -R 'express.json' -n server.js src 2>/dev/null | head -n 20 || true"
docker exec "$container" sh -lc "grep -R 'body.*limit\|json.*limit\|requestOverrides' -n server.js src/configuration.js src 2>/dev/null | head -n 40 || true"
