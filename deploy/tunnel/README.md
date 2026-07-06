# 全景页手机访问（SSH 反向隧道 + nginx 密码）

> **⚠️ 已被云端直跑取代（2026-07-06，feat/panorama-cloud）。** 办公网 DPI 掐 SSH
> 后本方案死亡。现方案：全景页迁到云端常驻（`rquant-panorama.service`，读同机
> surge feed + 自拉兜底），nginx 28080 直接反代云端 127.0.0.1:8506，零隧道、零 Mac
> 依赖。见 `docs/deploy/2026-07-06-panorama-cloud-deploy.md` 与
> `deploy/nginx/rquant-panorama-cloud.conf`。本目录**留档备用**（不删），万一云端直跑
> 需回退可参考。

拓扑（旧）：手机（任意网络）→ 云 nginx:28080（basic auth）→ 云 127.0.0.1:18506
（sshd 反向端口）→ SSH 隧道 → Mac Streamlit 8506。

- Mac 端：launchd `com.roxor.rquant-tunnel` 常驻 `ssh -N -R`（本目录 plist），
  KeepAlive 断线自动重连，走既有 SSH 免密 key
- 云端：nginx 配置在 `/www/server/panel/vhost/nginx/panorama.conf`
  （listen 28080，密码文件 `/www/server/nginx/conf/.htpasswd-panorama`）
- 安全组：仅需放行 TCP 28080
- 为什么不用 frp（deploy/frp/ 保留备用）：办公网出口 DPI 在 TCP 握手后掐掉
  frp 协议数据包（TLS 伪装也被掐、frps 侧零日志），SSH 流量天然放行。
  frps 已装在云端可停用：`sudo systemctl disable --now frps`
- Mac 合盖睡眠即断流（页面依赖本机东财/新浪源），看盘时段保持供电唤醒
