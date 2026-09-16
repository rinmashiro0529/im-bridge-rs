#!/usr/bin/env bash
set -euo pipefail
container="${ST_CONTAINER:-sillytavern}"
base_url="http://127.0.0.1:18000"

echo '== container =='
docker inspect -f 'name={{.Name}} image={{.Config.Image}} working_dir={{.Config.WorkingDir}} status={{.State.Status}} started={{.State.StartedAt}}' "$container"
docker inspect -f '{{range .Mounts}}mount type={{.Type}} source={{.Source}} destination={{.Destination}} rw={{.RW}}{{println}}{{end}}' "$container"
docker inspect -f '{{range $key, $_ := .Config.Env}}{{$key}}{{println}}{{end}}' "$container" | sed 's/=.*//' | sort | sed 's/^/env_name=/'

echo '== SillyTavern version =='
docker exec "$container" node -e "const p=require('./package.json'); console.log('name='+p.name); console.log('version='+(p.version||'unknown'))"

echo '== selected config flags =='
docker exec "$container" sh -lc "grep -E '^(port|listen|whitelistMode|basicAuthMode|enableUserAccounts|enableServerPlugins|dataRoot|autorun|securityOverride):' config.yaml 2>/dev/null || true"

echo '== plugin installation =='
docker exec "$container" sh -lc 'if [ -f plugins/st-im-bridge/package.json ]; then node -e "const p=require(\"./plugins/st-im-bridge/package.json\"); console.log(\"plugin_present=true\"); console.log(\"plugin_name=\"+p.name); console.log(\"plugin_version=\"+(p.version||\"unknown\")); console.log(\"plugin_main=\"+p.main)"; else echo plugin_present=false; fi'
docker exec "$container" sh -lc 'if [ -f plugins/st-im-bridge/data/app.db ]; then echo plugin_db_container=plugins/st-im-bridge/data/app.db; ls -ln plugins/st-im-bridge/data/app.db*; else echo plugin_db_container=missing; fi'

echo '== ST API read-only probe =='
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
csrf_status="$(curl -sS -c "$tmp_dir/cookies" -D "$tmp_dir/csrf-headers" -o "$tmp_dir/csrf-body" -w '%{http_code}' "$base_url/csrf-token" || true)"
printf 'csrf_http=%s\n' "$csrf_status"
token="$(python3 - "$tmp_dir/csrf-body" <<'PY'
import json,sys
try:
    payload=json.load(open(sys.argv[1]))
    print(payload.get('token','') if isinstance(payload,dict) else '')
except Exception:
    print('')
PY
)"
if [ -n "$token" ]; then echo csrf_token_present=true; else echo csrf_token_present=false; fi
if [ -s "$tmp_dir/cookies" ]; then echo cookie_present=true; else echo cookie_present=false; fi

probe_post() {
  local name="$1"
  local path="$2"
  local data="$3"
  local output="$tmp_dir/$name.json"
  local status
  status="$(curl -sS -b "$tmp_dir/cookies" -H "x-csrf-token: $token" -H 'content-type: application/json' -o "$output" -w '%{http_code}' -X POST "$base_url$path" --data "$data" || true)"
  printf '%s_http=%s\n' "$name" "$status"
  python3 - "$name" "$output" <<'PY'
import hashlib,json,sys
name,path=sys.argv[1:]
try:
    payload=json.load(open(path))
except Exception:
    print(name+'_json=false')
    raise SystemExit
print(name+'_json=true')
if isinstance(payload,list):
    print(name+'_type=list')
    print(name+'_count='+str(len(payload)))
    if name=='characters' and payload and isinstance(payload[0],dict):
        avatar=str(payload[0].get('avatar',''))
        print('first_avatar_sha256='+hashlib.sha256(avatar.encode()).hexdigest() if avatar else 'first_avatar_sha256=missing')
elif isinstance(payload,dict):
    print(name+'_type=dict')
    print(name+'_keys='+','.join(sorted(payload.keys())[:30]))
    if 'characters' in payload and isinstance(payload['characters'],list):
        print(name+'_characters_count='+str(len(payload['characters'])))
else:
    print(name+'_type='+type(payload).__name__)
PY
}

if [ -n "$token" ]; then
  probe_post settings /api/settings/get '{}'
  probe_post characters /api/characters/all '{}'
else
  echo settings_http=skipped
  echo characters_http=skipped
fi
plugin_probe_status="$(curl -sS -b "$tmp_dir/cookies" -o /dev/null -w '%{http_code}' "$base_url/api/plugins/st-im-bridge/probe" || true)"
printf 'plugin_probe_http=%s\n' "$plugin_probe_status"
