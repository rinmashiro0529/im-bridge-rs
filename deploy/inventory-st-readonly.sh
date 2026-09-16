#!/usr/bin/env bash
set -euo pipefail
root="${ST_ROOT:-/srv/sillytavern}"
plugin="$root/plugins/st-im-bridge"
data="$root/data"
base_url="${ST_BASE_URL:-http://127.0.0.1:18000}"

echo '== repositories =='
for repo in "$plugin" "$root/public/scripts/extensions/third-party/SillyTavern-IM-Bridge-UI"; do
  if [ -d "$repo/.git" ]; then
    printf 'repo=%s\n' "$repo"
    git -C "$repo" status --short --branch
    printf 'head='; git -C "$repo" rev-parse HEAD
  else
    printf 'repo_missing=%s\n' "$repo"
  fi
done

echo '== ST data inventory =='
python3 - "$data" <<'PY'
import os,sys
root=sys.argv[1]
handles=[]
for name in sorted(os.listdir(root)):
    path=os.path.join(root,name)
    if os.path.isdir(path) and (os.path.exists(os.path.join(path,'settings.json')) or os.path.isdir(os.path.join(path,'characters'))):
        handles.append(name)
print('handles_count='+str(len(handles)))
for handle in handles:
    base=os.path.join(root,handle)
    chars=os.path.join(base,'characters')
    chats=os.path.join(base,'chats')
    extensions=os.path.join(base,'extensions')
    char_files=[]
    chat_files=[]
    chat_bytes=0
    if os.path.isdir(chars):
        char_files=[os.path.join(dp,f) for dp,_,fs in os.walk(chars) for f in fs if f.lower().endswith(('.png','.json'))]
    if os.path.isdir(chats):
        chat_files=[os.path.join(dp,f) for dp,_,fs in os.walk(chats) for f in fs if f.lower().endswith('.jsonl')]
        chat_bytes=sum(os.path.getsize(p) for p in chat_files)
    ui=os.path.join(extensions,'SillyTavern-IM-Bridge-UI')
    print('handle='+handle)
    print('character_files='+str(len(char_files)))
    print('chat_files='+str(len(chat_files)))
    print('chat_bytes='+str(chat_bytes))
    print('settings_present='+str(os.path.isfile(os.path.join(base,'settings.json'))).lower())
    print('ui_extension_present='+str(os.path.isdir(ui)).lower())
PY

echo '== plugin database =='
python3 - "$plugin/data/app.db" <<'PY'
import json,sqlite3,sys
path=sys.argv[1]
con=sqlite3.connect('file:'+path+'?mode=ro', uri=True)
con.row_factory=sqlite3.Row
print('quick_check='+str(con.execute('PRAGMA quick_check').fetchone()[0]))
print('foreign_key_violations='+str(len(con.execute('PRAGMA foreign_key_check').fetchall())))
tables=[r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
print('tables='+','.join(tables))
for table in tables:
    count=con.execute('SELECT COUNT(*) FROM "'+table.replace('"','""')+'"').fetchone()[0]
    print('table_count.'+table+'='+str(count))
if 'accounts' in tables:
    for row in con.execute('SELECT account_id, st_user_handle, role FROM accounts ORDER BY account_id'):
        print('account='+str(row['account_id'])+',handle='+str(row['st_user_handle'])+',role='+str(row['role']))
if 'account_configs' in tables:
    rows=con.execute('SELECT telegram_bot_token, telegram_allowed_user_ids, bot_enabled FROM account_configs').fetchall()
    configured=sum(1 for r in rows if r['telegram_bot_token'])
    enabled=sum(1 for r in rows if r['bot_enabled'])
    allowed=0
    for r in rows:
        try:
            values=json.loads(r['telegram_allowed_user_ids'] or '[]')
            allowed += len(values) if isinstance(values,list) else 0
        except Exception:
            pass
    print('bot_tokens_configured='+str(configured))
    print('bots_desired_enabled='+str(enabled))
    print('allowed_telegram_ids='+str(allowed))
if 'active_sessions' in tables:
    rows=con.execute('SELECT active_character_avatar, active_chat_file, active_model_override, compression_model_override FROM active_sessions').fetchall()
    print('active_sessions_with_character='+str(sum(1 for r in rows if r['active_character_avatar'])))
    print('active_sessions_with_chat='+str(sum(1 for r in rows if r['active_chat_file'])))
    print('active_sessions_with_model_override='+str(sum(1 for r in rows if r['active_model_override'])))
    print('active_sessions_with_compression_override='+str(sum(1 for r in rows if r['compression_model_override'])))
if 'turn_records' in tables:
    for row in con.execute('SELECT operation, status, COUNT(*) AS n FROM turn_records GROUP BY operation,status ORDER BY operation,status'):
        print('turn_records.'+str(row['operation'])+'.'+str(row['status'])+'='+str(row['n']))
PY

echo '== runtime bot state =='
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
curl -fsS -c "$tmp_dir/cookies" -o "$tmp_dir/csrf" "$base_url/csrf-token"
token="$(python3 - "$tmp_dir/csrf" <<'PY'
import json,sys
print(json.load(open(sys.argv[1])).get('token',''))
PY
)"
status="$(curl -sS -b "$tmp_dir/cookies" -o "$tmp_dir/bots" -w '%{http_code}' "$base_url/api/plugins/st-im-bridge/admin/bots")"
printf 'admin_bots_http=%s\n' "$status"
python3 - "$tmp_dir/bots" <<'PY'
import json,sys
try: payload=json.load(open(sys.argv[1]))
except Exception:
    print('admin_bots_json=false'); raise SystemExit
items=payload.get('items',[]) if isinstance(payload,dict) else []
print('runtime_bots_count='+str(len(items)))
for item in items:
    print('runtime_bot='+','.join([
        'handle:'+str(item.get('handle')),
        'status:'+str(item.get('status')),
        'username:'+str(item.get('username')),
    ]))
PY
