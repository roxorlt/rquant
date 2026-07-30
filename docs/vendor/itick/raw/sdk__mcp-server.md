<!-- source: https://docs.itick.org/zh-cn/sdk/mcp-server.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: sdk/mcp-server -->

---
title: MCP Server
description: iTick 官方MCP Server，提供基础、股票、指数、期货、基金、外汇、加密货币数据的 REST API 查询和 WebSocket 实时数据订阅功能。
keywords: MCP Server, iTick 官方MCP, REST API, WebSocket
---

## MCP Server 介绍

iTick 官方 MCP Server 是 iTick 金融数据平台官方推出的标准化协议服务器，基于 [Model Context Protocol (MCP)](https://modelcontextprotocol.io/){:target="\_blank"} 构建，为 AI 助手（如 Claude Desktop、Cursor 等）提供统一的金融数据访问入口。该服务器封装了 iTick 在全球外汇、股票、指数、期货、基金及加密货币等领域的高质量数据能力，支持 REST API 批量查询与 WebSocket 实时推送，开发者仅需配置 API Key 即可在 AI 对话环境中即时调用专业金融数据，大幅降低数据集成门槛

### 特点

- **开发者友好**：标准易用接口、简明文档与丰富示例，便于快速接入。
- **产品线丰富**：多市场股票、外汇、指数、加密货币等实时与历史数据。
- **多场景适用**：量化团队、金融科技与专业分析等场景。
- **服务与基础设施**：专业数据源、多地区加速与链路热备份，侧重实时与稳定。
- **定制化**：机构与专业用户可洽谈定制数据方案。

### iTick API 类型与本项目范围

| 类型          | 说明                                                                  | 本 MCP                                     |
| ------------- | --------------------------------------------------------------------- | ------------------------------------------ |
| **REST**      | 通过市场数据端点获取报价、K 线等；请求需按文档携带 token 等鉴权信息。 | **已实现**（仅 HTTP GET 覆盖的 REST 能力） |
| **Websocket** | 发布/订阅，推送订单、成交、行情等，减少轮询。                         | **未实现**                                 |
| **FIX**       | 高吞吐、机构级连接；当前主要面向机构客户。                            | **未实现**（非 REST）                      |

FIX 仅机构开放时，可联系官方客服：[Telegram @iticksupport](https://t.me/iticksupport){:target="_blank"}、[WhatsApp +852 59046663](https://wa.me/85259046663){:target="_blank"}。

### 技术支持（摘要）

- **邮件**：[support@itick.org](mailto:support@itick.org)（建议主题中注明环境、身份与问题描述）
- **营业时间**：周一至周五 9:00–18:00（香港时间）；紧急生产问题以官方说明为准。
- **非办公时间**：可登录 [itick.org](https://itick.org/zh-cn) 通过站内即时消息联系在线客服。

---

## 部署

### uv build + upx（唯一推荐方式）

使用 [`uv`](https://github.com/astral-sh/uv){:target="\_blank"} 构建，并用 [`upx`](https://upx.github.io/){:target="\_blank"} 压缩，生成轻量、可分发的 MCP stdio 可执行文件。

```bash
# 安装 uv（若未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装 upx（若未安装）
# macOS: brew install upx
# Linux: sudo apt install upx

# 构建 + 压缩
uv build
```

构建产物位于 `dist/` 目录，可直接用于 MCP stdio 模式（Cursor / Claude Desktop / OpenCode）：


### 快速配置

支持配置的平台：
- ✨ Cursor
- ✨ Claude Desktop  
- ✨ OpenCode

### 手动配置环境变量

| 变量 | 说明 |
|------|------|
| `TOKEN` | 必填（实际调用时）：请求头 `token`，见 [REST 文档](https://docs.itick.org/zh-cn) |
| `ITICK_API_BASE` | 可选，默认 `https://{baseHost}`（与文档示例一致）。若你环境使用 `https://api.itick.io`，可设为该地址 |

```bash
export TOKEN="your_token"
# export ITICK_API_BASE="https://api.itick.io"
```

## Run (stdio)

```bash
itick-mcp
# 或
# http方式启动
python3 -m itick_mcp_server  --transport http
# sse方式启动
python3 -m itick_mcp_server  --transport sse
# stdio方式启动
python3 -m itick_mcp_server
```

## Cursor / Claude Desktop

```json
{
  "mcpServers": {
    "itick": {
      "command": "uvx",
      "args": [
        "itick-mcp"
      ],
      "env": {
        "TOKEN": "your_token"
      }
    }
  }
}
```

## OpenCode


OpenCode 使用 `opencode.json` / `opencode.jsonc` 的 `mcp` 键，本地服务需将 `type` 设为 `local`，`command` 为字符串数组，环境变量放在 `environment` 中（与 Cursor 的 `mcpServers` / `env` 不同）。最小示例：

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "itick": {
      "type": "local",
      "enabled": true,
      "command": ["uvx", "itick-mcp"],
      "environment": {
        "TOKEN": "your_token"
      }
    }
  }
}
```

## MCP 工具一览

与 [REST 文档](https://docs.itick.org/zh-cn/) 路径对应：

**基础**

- `symbolList` → `GET /symbol/list`
- `symbolHolidays` → `GET /symbol/v2/holidays`

**股票（额外）**

- `stockInfo` → `GET /stock/info`
- `stockIpo` → `GET /stock/ipo`
- `stockSplit` → `GET /stock/split`

**各产品线**（`stock` / `crypto` / `forex` / `indices` / `future` / `fund`）

| 工具名           | REST                   |
| ---------------- | ---------------------- |
| `{prefix}Tick`   | `GET /{prefix}/tick`   |
| `{prefix}Quote`  | `GET /{prefix}/quote`  |
| `{prefix}Depth`  | `GET /{prefix}/depth`  |
| `{prefix}Kline`  | `GET /{prefix}/kline`  |
| `{prefix}Ticks`  | `GET /{prefix}/ticks`  |
| `{prefix}Quotes` | `GET /{prefix}/quotes` |
| `{prefix}Depths` | `GET /{prefix}/depths` |
| `{prefix}Klines` | `GET /{prefix}/klines` |

批量接口中 `codes` 为英文逗号分隔。K 线参数在工具中为 `k_type`，请求中会编码为 **`kType`**；可选 `et`（毫秒时间戳）、`limit`。

与早期 Java 示例一致的股票单笔 K 线工具名仍为 **`stockKline`**。

## 代码结构

- `client.py` — HTTP GET、`ITICK_API_BASE` / `TOKEN`
- `tools_register.py` — 注册全部 REST 工具
- `server.py` — FastMCP 入口
