# 盘中全景页手机访问（frp 穿透 + nginx 密码）

拓扑：手机（任意网络）→ 云 nginx:18080（basic auth）→ 云 frps:18506（仅回环）
→ frp 隧道 → Mac frpc → 本机 Streamlit 8506。

- Mac 端：frpc 配置在 `~/.config/rquant-frp/frpc.toml`（含 token，不进 git），
  launchd `com.roxor.rquant-frpc` 常驻自动重连
- 云端安装见本目录三个文件 + 项目 DEPLOY.md 命令清单
- 腾讯云安全组需放行 TCP 7100（frp 控制）与 18080（nginx 入口）；
  18506 不对外（frps proxyBindAddr=127.0.0.1）
- Mac 合盖睡眠即断流（页面依赖本机东财/新浪源），看盘时段保持供电唤醒
