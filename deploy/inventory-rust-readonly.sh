#!/usr/bin/env bash
set -euo pipefail
python3 - "${IMBRIDGE_DB:-/var/lib/im-bridge/app.db}" <<'PY'
import sqlite3,sys
path=sys.argv[1]
con=sqlite3.connect('file:'+path+'?mode=ro',uri=True)
for table in ['accounts','telegram_bots','external_identities','characters','conversations','messages','provider_profiles','model_presets','telegram_updates','channel_deliveries']:
    try:
        count=con.execute('SELECT COUNT(*) FROM "'+table+'"').fetchone()[0]
    except Exception:
        count='missing'
    print(table+'='+str(count))
PY
systemctl is-active im-bridge.service
systemctl is-enabled im-bridge.service
