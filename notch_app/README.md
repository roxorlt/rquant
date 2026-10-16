# rQuant 五虎 notch widget

Server: `server/wuhu_live.py` (systemd `rquant-wuhu`, 127.0.0.1:8775, nginx `:8081/api/v1/wuhu/` → it).
`GET http://82.156.0.68:8081/api/v1/wuhu/live` with `Authorization: Bearer <token>`; token lives in `/etc/rquant-wuhu.env` (root 600).
Data: pools/auction/<date>.json picks (top 5 by rank) · quotes/part-*.parquet (open + minute spark backfill)
· stream/d-*.parquet (~3s per-round deltas, polled every 1s) · alerts/<date>.jsonl (bias derived from type).
Rollback: `sudo systemctl disable --now rquant-wuhu` and restore the nginx backup in ~/rquant-wuhu/.

Mac (macOS 14+): `./build.sh` (checks Xcode CLT, SwiftPM release build, wraps .app, ad-hoc sign) → `./install.sh`
(copies to /Applications, stores token in Keychain service `rquant-wuhu` account `token`, launches; app registers
itself as a login item via SMAppService). Alternative token source: `~/.config/rquant-notch/config.json`
`{"token":"…","endpoint":"…"}` chmod 600. Polls every 3s Mon–Fri 09:14–15:05 CST, every 60s otherwise (only if empty).
Hover = expand (440pt), new 五虎 alert = 3s peek, click row = rQuant `/app/#/stock/<code>`. Menu-bar "虎" item: refresh / quit.
Collapsed hit area is the physical notch only (`safeAreaInsets.top` × `auxiliaryTopLeftArea`/`Right`); transparent window chrome passes mouse events through so browser tab strips under the notch stay clickable.
