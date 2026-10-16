#!/bin/bash
# Run ON the server as lighthouse (sudo). Only after 15:10 — touches nothing but rquant-wuhu + one nginx location (graceful reload).
set -euo pipefail
cd ~/rquant-wuhu
if ! sudo test -f /etc/rquant-wuhu.env; then
  T=$(/usr/bin/python3 -c 'import secrets;print(secrets.token_urlsafe(32))')
  printf 'WUHU_TOKEN=%s\nWUHU_PORT=8775\n' "$T" | sudo install -m 600 -o root -g root /dev/stdin /etc/rquant-wuhu.env
  printf '%s\n' "$T" | install -m 600 /dev/stdin ~/rquant-wuhu/client_token   # for handing to the Mac; delete after
  unset T
fi
sudo install -m 644 rquant-wuhu.service /etc/systemd/system/rquant-wuhu.service
sudo systemctl daemon-reload && sudo systemctl enable --now rquant-wuhu.service
C=/www/server/panel/vhost/nginx/rquant-backup.conf
if ! sudo grep -q "/api/v1/wuhu/" $C; then
  sudo cp -a $C /home/lighthouse/rquant-wuhu/rquant-backup.conf.bak-$(date +%Y%m%d%H%M%S)
  sudo python3 - "$C" <<'PY'
import sys;p=sys.argv[1];s=open(p).read()
blk='''    # rQuant 五虎 notch widget (Bearer token checked by rquant-wuhu :8775)
    location ^~ /api/v1/wuhu/ {
        proxy_pass http://127.0.0.1:8775;
        proxy_http_version 1.1;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 10s;
        gzip on; gzip_types application/json;
    }

    location = /app {'''
assert s.count('    location = /app {')==1
open(p,'w').write(s.replace('    location = /app {',blk,1))
PY
  sudo nginx -t && sudo nginx -s reload
fi
