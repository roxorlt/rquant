# SDK-R01 定向补修

执行者为 Codex 原生 `/root/research_sdk_impl`，角色 implementer，父任务 `/root`。只补原 reviewer `/root/research_sdk_review` 提出的可执行示例权限恢复问题。开始时本树干净，HEAD 为 `b16d4ae6fe4c62360395a51c31a1f2e8e71a517b`。

从该完整 HEAD 使用 `git -c tar.umask=0022 archive --format=tar` 还原 SDK 源码、示例、旧说明和已提交的 demo 到本树专用独立目录。两份产物 JSON 实际恢复为 `0644`；没有复制原树已经为 `0600` 的文件。按原文档代码块执行后退出 1，错误为 `factor artifact is not the owned single-link regular file`。完整命令、Git archive 字节校验值和当时权限见 `failure.json`，原始 stdout/stderr 分别保留。

修正 `docs/research-sdk.md` 的说明与命令，明确要求原 reader 的当前用户、普通单链接、`0600` 文件合同，并只对两份具名合成产物执行 `chmod 600`。在同一全新还原目录按更新后的文档代码块执行，相同示例退出 0、stderr 为空，实际文件权限为 `0600`，完整与展示产物均读取成功。更新说明的 Git blob 为 `d677939afd661b43eaf12143811ab4830eafb2e6`；最终补修提交的父提交须等于上述 HEAD。

本次使用原只读环境 Python 3.13.12，`-I -B` 与还原目录中显式 `src`，子进程只给定禁用 dotenv 和 dummy token 的最小环境。失败耗时 0.452 秒，成功耗时 0.448 秒。失败和成功后 reader scratch 都为 0；父进程 FD/Python 线程始终为 `4/1`，所有子进程都已等待退出。产物字节及选定源文件 hash 未变，独立还原目录已清理。证据见 `success.json` 及原始日志。

SDK、reader、统计与测试代码均未改动；原 `3 passed in 2.83s` 及 native resource 证据仍有效，未重跑 smoke 或全仓。保留原终审报告及所有失败记录。原 reviewer 的定向复核由 root 安排，M2 整体仍为部分。
