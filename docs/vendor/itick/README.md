# iTick 平台文档本地镜像

供 rQuant 离线查阅的 iTick（https://itick.org）官方文档快照。**只读参考资料，不参与构建。**

- **抓取日期**：2026-07-28
- **语言**：优先中文（zh-cn），仅 `faq.md` 一页站点强制回落到英文
- **文件数**：78（`raw/` 目录，每页一个文件）
- **索引**：见 [`_index.md`](_index.md)

## 覆盖了什么

| 分组 | 页数 | 说明 |
|------|------|------|
| 入门/参考 | 7 | readme、getting-started、api-url、product-list、error-code、kline、faq |
| FAQ | 1 | faq/renewal（开通与续费流程） |
| REST 基础数据 | 3 | symbol-list、symbol-holidays、market-status |
| REST 股票 | 11 | tick / ticks / quote / quotes / depth / depths / kline / klines / info / ipo / split |
| REST 其他市场 | 32 | crypto、forex、indices、future、fund 各 8 个端点（tick/quote/depth/kline 的单个 + 批量） |
| REST 新闻 | 2 | news/list、news/detail |
| WebSocket | 6 | stocks、crypto、forex、future、fund、indices |
| SDK | 6 | python / java / go / node / javascript / mcp-server |
| 定价（itick.org 主站） | 2 | 定价页 + 首页，文件名前缀 `_pricing__` |

REST 端点已**全部逐页抓取**，包括外汇/加密货币/美股等非核心市场——总量只有 76 页，远低于任务给的 120 页阈值，因此没有触发"只存索引"的降级策略。

## 抓取方式

站点是 Nuxt SPA，但对每个文档页额外暴露了 `.md` 源文件：

```
https://docs.itick.org/zh-cn/<slug>.md
```

页面清单来自两处并集：
1. `https://docs.itick.org/llms.txt`（**不完整**，缺 stock-info / stock-ipo / stock-split / news / sdk / market-status）
2. zh-cn 首页 SSR HTML 里的侧边栏 `href`（这才是完整清单）

76 页中 71 页拿到了干净的 `.md`；剩下 5 页（`faq`、`market-status`、`news/list`、`news/detail`、`stock-split`）`.md` 返回 404，改从 SSR HTML 的 `<main>` 用 markdownify 转换，内容完整（参数表、字段说明都在），但排版不如原生 markdown。文件头注释里的 `mode: md|html` 标明了来源方式。

`llms-full.txt` 和 `sitemap.xml` **不存在**——两个路径都返回 SPA 首页 HTML（同为 123556 字节），不要被 HTTP 200 骗了。

## 跳过 / 抓不到的部分

| 项 | 原因 |
|---|---|
| 定价页的另外 4 个市场 tab | 定价页有 5 个 tab（综合/股票/期货/基金/企业自定义），SSR 只渲染默认的"综合市场"。其余 tab 的数字走 `/api/order/plan`，需登录态，未授权返回 404。**已存的档位数字只对「综合市场」成立。** |
| 图片资源 | 只保留原站 URL（`https://docs.itick.org/images/...`），未下载 |
| blog.itick.org 教程文章 | 不属于 API 文档，且为营销内容 |
| 产品清单明细 | 官方放在外部 Google Sheets，非文档站内容 |
| FIX 协议文档 | **站上不存在**。`/fix`、`/fix-api`、`/rest-api/fix` 等路径均无 `.md`，侧边栏也无入口。详见下方 |
| 限频 / 配额文档 | **站上不存在独立页面**。唯一的定量口径在定价页 |
| changelog | **站上不存在** |

## 怎么更新

```bash
# 1. 取最新页面清单（以侧边栏为准，llms.txt 不全）
curl -sL -H 'Cookie: i18n_redirected=zh-cn' https://docs.itick.org/zh-cn \
  | grep -o 'href="/zh-cn/[^"#?]*"' | sed 's|href="/zh-cn/||;s|"||' | sort -u

# 2. 逐页抓 .md，404 的回落到 HTML
curl -sL https://docs.itick.org/zh-cn/<slug>.md -o raw/<slug with / -> __>.md

# 3. 定价（主站，非文档站）
curl -sL -H 'Cookie: i18n_redirected=zh-cn' https://itick.org/zh-cn/pricing
```

抓完记得给每个文件补回头部两行来源注释，并重新生成 `_index.md`。

## 阅读时的重要提醒

**frontmatter 里的 `description` / `keywords` 是 SEO 文案，不是接口契约。** 这些字段大量出现"逐笔成交""买卖方向""Level-2 深度""十档行情""历史 Tick 数据"等说法，但**正文的请求/响应参数表里并没有对应字段**。判断接口能力时**只看正文的参数表**，不要采信 frontmatter。这是本次抓取最容易踩的坑。
