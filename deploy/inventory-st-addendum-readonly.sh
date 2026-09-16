#!/usr/bin/env bash
set -euo pipefail
base_url="${ST_BASE_URL:-http://127.0.0.1:18000}"
ui="${ST_UI_DIR:-${ST_ROOT:-/srv/sillytavern}/data/example-user/extensions/SillyTavern-IM-Bridge-UI}"

echo '== UI repository =='
if [ -d "$ui/.git" ]; then
  git -c safe.directory="$ui" -C "$ui" status --short --branch
  printf 'ui_head='; git -c safe.directory="$ui" -C "$ui" rev-parse HEAD
else
  echo ui_git_repo=false
fi

echo '== container env names =='
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "${ST_CONTAINER:-sillytavern}" | sed 's/=.*//' | sort | sed 's/^/env_name=/'

echo '== UFW ST exposure =='
ufw status | grep -E '(^|[[:space:]])18000(/tcp)?([[:space:]]|$)' || echo 'ufw_18000_allow_rule=false'

echo '== character and chat classification =='
python3 - "${ST_DATA_DIR:-${ST_ROOT:-/srv/sillytavern}/data/example-user}" <<'PY'
import os,re,sys
root=sys.argv[1]
chars=os.path.join(root,'characters')
chats=os.path.join(root,'chats')
root_png=[]; root_json=[]; expression_png=[]; other=[]
for dp,_,files in os.walk(chars):
    for f in files:
        p=os.path.join(dp,f)
        rel=os.path.relpath(p,chars)
        ext=os.path.splitext(f)[1].lower()
        if dp==chars and ext=='.png': root_png.append(rel)
        elif dp==chars and ext=='.json': root_json.append(rel)
        elif dp!=chars and ext=='.png': expression_png.append(rel)
        else: other.append(rel)
all_jsonl=[]
for dp,_,files in os.walk(chats):
    for f in files:
        if f.lower().endswith('.jsonl'):
            all_jsonl.append(os.path.join(dp,f))
def is_aux(path):
    rel=os.path.relpath(path,chats).lower()
    name=os.path.basename(rel)
    return any(x in rel for x in ['/_stwm_test','_backup/']) or any(x in name for x in ['pre_compress','backup','snapshot','compressed_draft'])
primary=[p for p in all_jsonl if not is_aux(p)]
aux=[p for p in all_jsonl if is_aux(p)]
print('root_character_png='+str(len(root_png)))
print('root_character_json='+str(len(root_json)))
print('expression_png='+str(len(expression_png)))
print('other_character_files='+str(len(other)))
print('all_jsonl='+str(len(all_jsonl)))
print('primary_jsonl_estimate='+str(len(primary)))
print('auxiliary_jsonl_estimate='+str(len(aux)))
print('all_jsonl_bytes='+str(sum(os.path.getsize(p) for p in all_jsonl)))
print('primary_jsonl_bytes='+str(sum(os.path.getsize(p) for p in primary)))
PY

echo '== selected generation settings =='
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
curl -fsS -c "$tmp_dir/cookies" -o "$tmp_dir/csrf" "$base_url/csrf-token"
token="$(python3 - "$tmp_dir/csrf" <<'PY'
import json,sys
print(json.load(open(sys.argv[1])).get('token',''))
PY
)"
curl -fsS -b "$tmp_dir/cookies" -H "x-csrf-token: $token" -H 'content-type: application/json' -X POST --data '{}' -o "$tmp_dir/settings" "$base_url/api/settings/get"
python3 - "$tmp_dir/settings" <<'PY'
import json,sys,urllib.parse
payload=json.load(open(sys.argv[1]))
settings=json.loads(payload.get('settings','{}'))
oai=settings.get('oai_settings') or {}
source=str(oai.get('chat_completion_source') or '')
model=str(oai.get('custom_model') or oai.get('openai_model') or '')
custom_url=str(oai.get('custom_url') or '')
parsed=urllib.parse.urlsplit(custom_url)
print('username_configured='+str(bool(str(settings.get('username') or '').strip())).lower())
print('chat_completion_source='+source)
print('model='+model)
print('custom_url_configured='+str(bool(custom_url)).lower())
print('custom_url_scheme='+parsed.scheme if custom_url else 'none')
print('custom_url_host='+parsed.hostname if custom_url else 'none')
print('custom_prompt_post_processing='+str(oai.get('custom_prompt_post_processing') or ''))
print('temperature='+str(oai.get('temp_openai')))
print('top_p='+str(oai.get('top_p_openai')))
print('max_tokens='+str(oai.get('openai_max_tokens')))
PY
