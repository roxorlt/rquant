"""rQuant Preview 多页入口（独立 Streamlit 进程）。

用 ``st.navigation`` 把两个既有的只读页面挂到同一个 Streamlit session：

- 健康看板（``app.py``）
- 运行控制台（``runtime_console.py``）

两个子页面各自在自己的脚本体里调用 ``st.set_page_config``；已验证当前
Streamlit 版本（见 ``.venv`` 里的 streamlit==1.57.0）允许每个页面各调用一次
``set_page_config``——该调用现在是"仅追加式"更新（写 ``page_config_changed``
proto 消息），既不要求必须是脚本的第一条命令，也不会在第二次调用时报错，
所以两个页面都能在同一 session 里正常切换渲染，不需要把 config 提到本文件。

Strategy Lab（``lab/app.py``）**没有**挂进来：它通过
``PageControlClient``（默认 POST 到 ``http://127.0.0.1:8767/v1/commands``，
即生产环境常驻的 ``rquant-page-control.service``）提交任务/暂停/恢复/取消/
导出等写命令，且没有"未配置凭据 / 未配置 page-control 时拒绝写入"的开关——
唯一现在能拦住写入的是 ``_job_center_runtime()`` 要求 ``RQUANT_RUNTIME_ROOT``
被设置且 Job Center 权威 manifest 验证通过，这是环境配置的副作用而不是代码里
设计好的只读模式，一旦该环境变量在部署侧被设置（比如未来和其它服务共用同一份
``.env``），写路径就会打开并直接命中生产 Job Center。详见本次改动的 PR 说明
（写路径逐行证据：``src/rquant/dashboard/lab/app.py`` 的 124 / 299 / 607 / 1450 /
1475 行，``src/rquant/page_control.py`` 的 2545 行）。在把这一判断证伪之前，
Lab 页面不挂进 preview。

健康看板独立跑时（生产 8501 就是这样）用 30 秒 ``<meta http-equiv="refresh">``
整页刷新；这个 tag 一旦渲染进 DOM，浏览器计时器不会因为 ``st.navigation`` 之后
切到另一个子页面（同一个 SPA session，没有真正的文档级导航）就被取消，到点会把
标签页整页拉回健康看板，用户体验上等于"看着看着自动跳走"。所以本文件在调用
``pages.run()`` 之前，先在 ``st.session_state`` 写一个明确的挂载标记
（``rquant.dashboard.preview_state.PREVIEW_MOUNTED_SESSION_KEY``）；``app.py``
只认这个标记来决定要不要注入那个 meta refresh tag，不去猜 URL 或者猜自己是不是
被谁 import 的。``st.session_state`` 是 ``st.navigation`` 在切页时唯一保证跨页
保留的状态，所以这个检测在预览会话内是可靠的，也不能被直接深链接到子页面绕过
（深链接同样先经过本文件的顶层脚本体，标记照样会被设置）。

启动方式::

    RQUANT_SERVING_ROOT=data/runtime/serving uv run streamlit run \\
        src/rquant/dashboard/preview_app.py --server.port 8501
"""

from __future__ import annotations

import streamlit as st

from rquant.dashboard.preview_state import PREVIEW_MOUNTED_SESSION_KEY

st.session_state[PREVIEW_MOUNTED_SESSION_KEY] = True

_pages = st.navigation(
    [
        st.Page("app.py", title="健康看板", icon="📈", default=True),
        st.Page("runtime_console.py", title="运行控制台", icon="🖥️"),
    ]
)
_pages.run()
