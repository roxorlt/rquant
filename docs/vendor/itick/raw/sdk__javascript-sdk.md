<!-- source: https://docs.itick.org/zh-cn/sdk/javascript-sdk.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: sdk/javascript-sdk -->

---
title: Javascript SDK
description: iTick 官方 Javascript SDK，提供基础、股票、指数、期货、基金、外汇、加密货币数据的 REST API 查询和 WebSocket 实时数据订阅功能。
keywords: Javascript SDK, iTick 官方SDK, REST API, WebSocket
---

## Javascript SDK 文档

Javascript 版本的 iTick API SDK，提供基础、股票、指数、期货、基金、外汇、加密货币数据的 REST API 查询和 WebSocket 实时数据订阅功能。用于访问 iTick API 的实时金融市场数据。

## 特性

- **全面的市场覆盖**：访问全球金融市场，包括股票、加密货币、外汇、指数、期货和基金
- **实时数据**：基于 WebSocket 的实时数据流，支持自动重连
- **RESTful API**：简洁直观的 REST API，用于获取历史数据和快照
- **类型安全**：完整的 TypeScript 支持和全面的类型定义
- **自动重连**：内置自动重连机制（5 秒间隔，可设置无限次尝试）
- **心跳保活**：自动 ping/pong 机制（30 秒间隔）保持连接稳定
- **模块化设计**：按资产类型划分独立模块，结构更清晰
- **灵活订阅**：支持报价、盘口、成交和 K 线数据订阅

## 支持的浏览器

|                                                     Chrome                                                     |                                                      Firefox                                                      |                                                     Safari                                                     |                                                    Opera                                                    |                                                   Edge                                                   |
| :------------------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------------: | :---------------------------------------------------------------------------------------------------------: | :------------------------------------------------------------------------------------------------------: |
| ![Chrome browser logo](https://raw.githubusercontent.com/alrra/browser-logos/main/src/chrome/chrome_48x48.png) | ![Firefox browser logo](https://raw.githubusercontent.com/alrra/browser-logos/main/src/firefox/firefox_48x48.png) | ![Safari browser logo](https://raw.githubusercontent.com/alrra/browser-logos/main/src/safari/safari_48x48.png) | ![Opera browser logo](https://raw.githubusercontent.com/alrra/browser-logos/main/src/opera/opera_48x48.png) | ![Edge browser logo](https://raw.githubusercontent.com/alrra/browser-logos/main/src/edge/edge_48x48.png) |
|                                                    Latest ✔                                                    |                                                     Latest ✔                                                      |                                                    Latest ✔                                                    |                                                  Latest ✔                                                   |                                                 Latest ✔                                                 |

## 安装

### 项目安装

使用 npm:

```bash
npm install @itick/browser-sdk
```

使用 yarn:

```bash
yarn add @itick/browser-sdk
```

使用 pnpm:

```bash
pnpm add @itick/browser-sdk
```

### CDN

```html
<!-- Using unpkg CDN -->
<script src="https://unpkg.com/@itick/browser-sdk@latest/dist/itick-sdk.min.js"></script>

<!-- Or using jsDelivr CDN -->
<script src="https://cdn.jsdelivr.net/npm/@itick/browser-sdk@latest/dist/itick-sdk.min.js"></script>

<script>
  const { BaseClient, StockClient, CryptoClient, ForexClient, IndicesClient, FutureClient, FundClient } = window.iTickSDK;
</script>
```

## 快速开始

### 基础用法

```javascript
import { StockClient } from "@itick/browser-sdk";

// 可选配置 （默认host {baseHost}）其他模块配置参照此配置
const options = {
  baseURL: "https://api-free.itick.org",
  wssURL: "wss://api-free.itick.org",
};
// API Token
const token = process.env.ITICK_TOKEN;
// 初始化客户端
const client = new StockClient(token, options);

// 获取股票报价
async function getQuote() {
  try {
    const response = await client.getQuote({ region: "US", code: "AAPL" });

    if (response.code === 0 && response.data) {
      console.log("最新价格:", response.data.ld);
      console.log("涨跌幅:", response.data.chp);
    }
  } catch (error) {
    console.error("错误:", error.message);
  }
}

getQuote();
```

### 通过 WebSocket 获取实时数据

```javascript
import { CryptoClient } from "@itick/browser-sdk";
const client = new CryptoClient(token);

// 创建带订阅数据的 WebSocket 连接 SDK内部已处理连接和重连后直接订阅 subscribeData 数据 无需再发送订阅数据
const socket = client.createSocket({
  maxReconnectTimes: 10, // 最大重连次数 不传则使用默认值 0 无限制重连
  pingInterval: 30000, // ping 间隔 默认30 秒
  reconnectInterval: 5000, // 重连间隔 默认5 秒
  subscribeData: {
    codes: ["BTCUSDT$BA", "ETHUSDT$BA"],
    types: ["quote", "tick"],
  },
});

// 创建自定义 WebSocket 连接
const socket = client.createSocket();

// 连接成功或者重连连成功后发送订阅数据
socket.onSocketOpen(() => {
  socket.subscribeData({
    codes: ["BTCUSDT$BA", "ETHUSDT$BA"],
    types: ["quote", "tick"],
  });
});

// 处理接收到的消息
socket.onSocketMessage((res) => {
  console.log("收到数据:", res);
});

// 处理错误
socket.onSocketError((error) => {
  console.error("WebSocket 错误:", error);
});

// 完成后断开连接
// socket.disconnectSocket();
```

## API 参考

### 基础模块

金融产品清单和市场节假日信息以及开市休市时间信息。

```typescript
import { BaseClient } from "@itick/browser-sdk";

const client = new BaseClient(token);

// 获取品种列表
await client.getSymbolList({ type: "stock", region: "US" });
await client.getSymbolList({ type: "crypto", region: "BA" });
await client.getSymbolList({ type: "forex", region: "GB" });

// 获取市场假期
await client.getSymbolHolidays("US");
await client.getSymbolHolidays("HK");
```

#### BaseClient 方法参考表

| 方法名              | 参数说明                                                                                                                                                       | 返回值类型                               | 描述                                               | 详细参数                                                                 |
| :------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------- | :------------------------------------------------- | :----------------------------------------------------------------------- |
| `getSymbolList`     | `options`: `Object`<br>- `type`:`enum` (产品类型，如 `stock`,`forex`,`fund`,`future`,`indices`) <br>- `region`:`string` (市场区域代码，如 `US`, `BA`, `GB` 等) | `Promise<APIResponse<SymbolListData[]>>` | 获取指定市场和资产类型的金融产品清单（品种列表）。 | [iTick 产品清单](https://docs.itick.org/rest-api/basics/symbol-list)     |
| `getSymbolHolidays` | region: `string` (市场区域代码，如 `US`, `HK` 等)                                                                                                              | `Promise<APIResponse<HolidayData[]>>`    | 获取指定市场的节假日信息，包括开市和休市时间安排。 | [iTick 市场假期](https://docs.itick.org/rest-api/basics/symbol-holidays) |

### 股票模块

访问美股、港股等全球股票市场数据。

```javascript
import { StockClient } from "@itick/browser-sdk";

const client = new StockClient(token);

// 获取单只股票信息
await client.getInfo({ region: "US", code: "AAPL" });

// 获取实时报价
await client.getQuote({ region: "US", code: "AAPL" });

// 获取盘口深度
await client.getDepth({ region: "US", code: "AAPL" });

// 获取最新成交
await client.getTick({ region: "US", code: "AAPL" });

// 获取 K 线数据
await client.getKline({
  region: "US",
  code: "AAPL",
  interval: "5m",
  limit: 100,
});

// 批量查询
await client.getQuotes({ region: "US", codes: ["AAPL", "MSFT", "GOOGL"] });
await client.getDepths({ region: "US", codes: ["AAPL", "MSFT"] });
await client.getTicks({ region: "US", codes: ["AAPL", "MSFT"] });
await client.getKlines({
  region: "US",
  codes: ["AAPL", "MSFT"],
  interval: "1d",
  limit: 50,
});

// IPO 信息
await client.getIPO({ region: "US", code: "RIVN" });

// 除权因子	
await client.getSplit({ region: "US", code: "AAPL" });
```

#### StockClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getInfo` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`)<br>-`exchange`?:`string` (可选, 上市交易所, 如 `NYSE`, `NASDAQ`等) | `Promise<APIResponse<StockInfo>>` | 获取股票基本信息 | [iTick 股票信息](https://docs.itick.org/rest-api/stocks/stock-info) |
| `getIPO` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`) | `Promise<APIResponse<StockIPO>>` | 获取股票 IPO 信息 | [iTick 股票 IPO](https://docs.itick.org/rest-api/stocks/stock-ipo) |
| `getSplit` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`) | `Promise<APIResponse<StockSplit>>` | 获取股票除权除息信息 | [iTick 股票除权因子](https://docs.itick.org/rest-api/stocks/stock-split) |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`) | `Promise<APIResponse<TickData>>` | 获取单个股票最新成交行情 | [iTick 股票实时成交](https://docs.itick.org/rest-api/stocks/stock-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`) | `Promise<APIResponse<QuoteData>>` | 获取单个股票最新报价 | [iTick 股票实时报价](https://docs.itick.org/rest-api/stocks/stock-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (股票代码，如 `AAPL`) | `Promise<APIResponse<DepthData>>` | 获取单个股票最新盘口 | [iTick 股票实时盘口](https://docs.itick.org/rest-api/stocks/stock-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (股票代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个股票 K 线数据 | [iTick 股票 K 线](https://docs.itick.org/rest-api/stocks/stock-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (股票代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个股票最新成交行情 | [iTick 股票批量成交](https://docs.itick.org/rest-api/stocks/stock-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (股票代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个股票最新报价 | [iTick 股票批量报价](https://docs.itick.org/rest-api/stocks/stock-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (股票代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个股票最新盘口 | [iTick 股票批量盘口](https://docs.itick.org/rest-api/stocks/stock-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (股票代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个股票 K 线数据 | [iTick 股票批量 K 线](https://docs.itick.org/rest-api/stocks/stock-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 股票](https://docs.itick.org/websocket/stocks) |

### 加密货币模块

访问多个交易所的加密货币市场数据。

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const client = new CryptoClient(token);

// 获取实时数据
await client.getQuote({ region: "BA", code: "BTCUSDT" });
await client.getDepth({ region: "BA", code: "ETHUSDT" });
await client.getTick({ region: "BA", code: "BTCUSDT" });

// 获取 K 线数据
await client.getKline({
  region: "BA",
  code: "BTCUSDT",
  interval: "1h",
  limit: 100,
});

// 批量查询
await client.getQuotes({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
```

#### CryptoClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `BA`, `BT`, `PB` 等)<br>- `code`: `string` (标的代码，如 `BTCUSDT`) | `Promise<APIResponse<TickData>>` | 获取单个加密货币最新成交行情 | [iTick 加密货币实时成交](https://docs.itick.org/rest-api/crypto/crypto-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `BA`, `BT`, `PB` 等)<br>- `code`: `string` (标的代码，如 `BTCUSDT`) | `Promise<APIResponse<QuoteData>>` | 获取单个加密货币最新报价 | [iTick 加密货币实时报价](https://docs.itick.org/rest-api/crypto/crypto-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `BA`, `BT`, `PB` 等)<br>- `code`: `string` (标的代码，如 `BTCUSDT`) | `Promise<APIResponse<DepthData>>` | 获取单个加密货币最新盘口 | [iTick 加密货币实时盘口](https://docs.itick.org/rest-api/crypto/crypto-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (标的代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个加密货币 K 线数据 | [iTick 加密货币 K 线](https://docs.itick.org/rest-api/crypto/crypto-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个加密货币最新成交行情 | [iTick 加密货币批量成交](https://docs.itick.org/rest-api/crypto/crypto-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个加密货币最新报价 | [iTick 加密货币批量报价](https://docs.itick.org/rest-api/crypto/crypto-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个加密货币最新盘口 | [iTick 加密货币批量盘口](https://docs.itick.org/rest-api/crypto/crypto-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个加密货币 K 线数据 | [iTick 加密货币批量 K 线](https://docs.itick.org/rest-api/crypto/crypto-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 加密货币](https://docs.itick.org/websocket/crypto) |

### 外汇模块

访问外汇市场数据。

```javascript
import { ForexClient } from "@itick/browser-sdk";

const client = new ForexClient(token);

await client.getQuote({ region: "GB", code: "EURUSD" });
await client.getDepth({ region: "GB", code: "GBPUSD" });
await client.getTick({ region: "GB", code: "USDJPY" });
await client.getKline({ region: "GB", code: "EURUSD", interval: "1d", limit: 50 });
```

#### ForexClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `GB` 等)<br>- `code`: `string` (标的代码，如 `EURUSD`) | `Promise<APIResponse<TickData>>` | 获取单个外汇对最新成交行情 | [iTick 外汇实时成交](https://docs.itick.org/rest-api/forex/forex-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `GB` 等)<br>- `code`: `string` (标的代码，如 `EURUSD`) | `Promise<APIResponse<QuoteData>>` | 获取单个外汇对最新报价 | [iTick 外汇实时报价](https://docs.itick.org/rest-api/forex/forex-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `GB` 等)<br>- `code`: `string` (标的代码，如 `EURUSD`) | `Promise<APIResponse<DepthData>>` | 获取单个外汇对最新盘口 | [iTick 外汇实时盘口](https://docs.itick.org/rest-api/forex/forex-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (标的代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个外汇对 K 线数据 | [iTick 外汇 K 线](https://docs.itick.org/rest-api/forex/forex-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个外汇对最新成交行情 | [iTick 外汇批量成交](https://docs.itick.org/rest-api/forex/forex-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个外汇对最新报价 | [iTick 外汇批量报价](https://docs.itick.org/rest-api/forex/forex-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个外汇对最新盘口 | [iTick 外汇批量盘口](https://docs.itick.org/rest-api/forex/forex-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个外汇对 K 线数据 | [iTick 外汇批量 K 线](https://docs.itick.org/rest-api/forex/forex-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 外汇](https://docs.itick.org/websocket/forex) |

### 指数模块

访问全球股票指数数据。

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const client = new IndicesClient(token);

await client.getQuote({ region: "US", code: "SPX" });
await client.getDepth({ region: "US", code: "NDX" });
await client.getKline({ region: "US", code: "DJI", interval: "1w", limit: 20 });
```

#### IndicesClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `GB` 等)<br>- `code`: `string` (标的代码，如 `DJI`, `SPX`) | `Promise<APIResponse<TickData>>` | 获取单个指数最新成交行情 | [iTick 指数实时成交](https://docs.itick.org/rest-api/indices/indices-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `GB` 等)<br>- `code`: `string` (标的代码，如 `DJI`, `SPX`) | `Promise<APIResponse<QuoteData>>` | 获取单个指数最新报价 | [iTick 指数实时报价](https://docs.itick.org/rest-api/indices/indices-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `GB` 等)<br>- `code`: `string` (标的代码，如 `DJI`, `SPX`) | `Promise<APIResponse<DepthData>>` | 获取单个指数最新盘口 | [iTick 指数实时盘口](https://docs.itick.org/rest-api/indices/indices-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (标的代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个指数 K 线数据 | [iTick 指数 K 线](https://docs.itick.org/rest-api/indices/indices-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个指数最新成交行情 | [iTick 指数批量成交](https://docs.itick.org/rest-api/indices/indices-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个指数最新报价 | [iTick 指数批量报价](https://docs.itick.org/rest-api/indices/indices-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个指数最新盘口 | [iTick 指数批量盘口](https://docs.itick.org/rest-api/indices/indices-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个指数 K 线数据 | [iTick 指数批量 K 线](https://docs.itick.org/rest-api/indices/indices-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 指数](https://docs.itick.org/websocket/indices) |

### 期货模块

访问期货市场数据。

```javascript
import { FutureClient } from "@itick/browser-sdk";

const client = new FutureClient(token);

await client.getQuote({ region: "US", code: "ES" });
await client.getDepth({ region: "US", code: "NQ" });
await client.getKline({ region: "US", code: "CL", interval: "5m", limit: 100 });
```

#### FutureClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `CN`, `HK` 等)<br>- `code`: `string` (标的代码，如 `CL`, `GC`) | `Promise<APIResponse<TickData>>` | 获取单个期货合约最新成交行情 | [iTick 期货实时成交](https://docs.itick.org/rest-api/future/future-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `CN`, `HK` 等)<br>- `code`: `string` (标的代码，如 `CL`, `GC`) | `Promise<APIResponse<QuoteData>>` | 获取单个期货合约最新报价 | [iTick 期货实时报价](https://docs.itick.org/rest-api/future/future-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `CN`, `HK` 等)<br>- `code`: `string` (标的代码，如 `CL`, `GC`) | `Promise<APIResponse<DepthData>>` | 获取单个期货合约最新盘口 | [iTick 期货实时盘口](https://docs.itick.org/rest-api/future/future-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (标的代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个期货合约 K 线数据 | [iTick 期货 K 线](https://docs.itick.org/rest-api/future/future-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个期货合约最新成交行情 | [iTick 期货批量成交](https://docs.itick.org/rest-api/future/future-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个期货合约最新报价 | [iTick 期货批量报价](https://docs.itick.org/rest-api/future/future-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个期货合约最新盘口 | [iTick 期货批量盘口](https://docs.itick.org/rest-api/future/future-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个期货合约 K 线数据 | [iTick 期货批量 K 线](https://docs.itick.org/rest-api/future/future-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 期货](https://docs.itick.org/websocket/future) |

### 基金模块

访问共同基金和 ETF 数据。

```javascript
import { FundClient } from "@itick/browser-sdk";

const client = new FundClient(token);

await client.getQuote({ region: "US", code: "VOO" });
await client.getDepth({ region: "US", code: "QQQ" });
await client.getKline({ region: "US", code: "SPY", interval: "1d", limit: 100 });
```

#### FundClient 方法参考表

| 方法名 | 参数说明 | 返回值类型 | 描述 | 详细参数 |
| :--- | :--- | :--- | :--- | :--- |
| `getTick` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (标的代码，如 `SPY`, `QQQ`) | `Promise<APIResponse<TickData>>` | 获取单个基金最新成交行情 | [iTick 基金实时成交](https://docs.itick.org/rest-api/fund/fund-tick) |
| `getQuote` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (标的代码，如 `SPY`, `QQQ`) | `Promise<APIResponse<QuoteData>>` | 获取单个基金最新报价 | [iTick 基金实时报价](https://docs.itick.org/rest-api/fund/fund-quote) |
| `getDepth` | `params`: `Object`<br>- `region`: `string` (市场编码，如 `US`, `HK` 等)<br>- `code`: `string` (标的代码，如 `SPY`, `QQQ`) | `Promise<APIResponse<DepthData>>` | 获取单个基金最新盘口 | [iTick 基金实时盘口](https://docs.itick.org/rest-api/fund/fund-depth) |
| `getKline`   | `options`: `GetKlineOptions`<br>- `region`: `string` (市场编码)<br>- `code`: `string` (标的代码)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineData[]>>` | 获取单个基金 K 线数据 | [iTick 基金 K 线](https://docs.itick.org/rest-api/fund/fund-kline) |
| `getTicks` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<TickDataMap>>` | 获取多个基金最新成交行情 | [iTick 基金批量成交](https://docs.itick.org/rest-api/fund/fund-ticks) |
| `getQuotes` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<QuoteDataMap>>` | 获取多个基金最新报价 | [iTick 基金批量报价](https://docs.itick.org/rest-api/fund/fund-quotes) |
| `getDepths` | `params`: `Object`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表) | `Promise<APIResponse<DepthDataMap>>` | 获取多个基金最新盘口 | [iTick 基金批量盘口](https://docs.itick.org/rest-api/fund/fund-depths) |
| `getKlines` | `options`: `GetKlinesOptions`<br>- `region`: `string` (市场编码)<br>- `codes`: `string[] \| string` (标的代码列表)<br>- `interval`: `KlineType` (K 线周期类型)<br>- `limit`: `number` (返回数据条数，最大 500)<br>- `et`?: `string \| number` (可选，结束时间戳) | `Promise<APIResponse<KlineDataMap>>` | 获取多个基金 K 线数据 | [iTick 基金批量 K 线](https://docs.itick.org/rest-api/fund/fund-klines) |
| `createSocket` | `options`?: `CreateSocketOptions` (可选，WebSocket [连接选项](#连接选项)) | `SocketClient` | 创建 WebSocket 连接用于实时数据订阅 | [iTick WebSocket 基金](https://docs.itick.org/websocket/fund) |

## WebSocket 实时数据

### 支持的数据类型

- `quote`：实时报价
- `depth`：盘口深度
- `tick`：最新成交
- `kline@1m` 或 `kline@1`：1 分钟 K 线
- `kline@5m` 或 `kline@2`：5 分钟 K 线
- `kline@15m` 或 `kline@3`：15 分钟 K 线
- `kline@30m` 或 `kline@4`：30 分钟 K 线
- `kline@1h` 或 `kline@5`：1 小时 K 线
- `kline@2h` 或 `kline@6`：2 小时 K 线（仅加密货币）
- `kline@4h` 或 `kline@7`：4 小时 K 线（仅加密货币）
- `kline@1d` 或 `kline@8`：日线
- `kline@1w` 或 `kline@9`：周线
- `kline@1M` 或 `kline@10`：月线

### 连接选项

```typescript
const socket = client.createSocket({
  maxReconnectTimes: 10, // 最大重连次数（0 = 无限）
  reconnectInterval: 5000, // 重连间隔（毫秒）
  pingInterval: 30000, // Ping 间隔（毫秒）
  subscribeData: {
    codes: ["AAPL$US", "MSFT$US"],
    types: ["quote", "tick", "kline@1m"],
  },
});
```

### 事件处理器

```javascript
// 连接打开
socket.onSocketOpen(() => {
  console.log("已连接！");
});

// 接收消息
socket.onSocketMessage((data) => {
  console.log("收到数据:", data);
});

// 发生错误
socket.onSocketError((error) => {
  console.error("错误:", error);
});

// 连接关闭
socket.onSocketClose(() => {
  console.log("已断开");
});

// 检查连接状态
const isConnected = socket.checkSocketConnected();

// 断开连接
socket.disconnectSocket();
```

### 动态订阅

```javascript
// 连接后订阅
socket.subscribeSocket({
  ac: "subscribe",
  types: ["quote", "depth"],
  codes: ["TSLA$US", "NVDA$US"],
});

// 取消订阅
socket.subscribeSocket({
  ac: "unsubscribe",
  types: ["tick"],
  codes: ["AAPL$US"],
});
```

## 错误处理

```javascript
try {
  const response = await client.getQuote({ region: "US", code: "AAPL" });

  if (response.code !== 0) {
    console.error("API 错误:", response.msg);
    return;
  }

  // 处理数据
  console.log(response.data);
} catch (error) {
  if (error instanceof Error) {
    console.error("网络错误:", error.message);
  }
}
```

## TypeScript 支持

完整的 TypeScript 支持和全面的类型定义：

```javascript
import type {
  APIResponse,
  QuoteData,
  SocketKlineData,
  SocketTickData,
  SocketDepthData,
  SocketQuoteData,
} from "@itick/browser-sdk";

// 类型安全的响应
const response: APIResponse<QuoteData> = await client.getQuote({
  region: "US",
  code: "AAPL",
});

// 类型安全的 WebSocket 消息
socket.onSocketMessage((response) => {
  const { code, data, msg, resAc } = response;
  if (data?.type === "quote") {
    const quoteData: SocketQuoteData = data;
  }
  if (data?.type === "kline@1") {
    const klineData: SocketKlineData = data;
  }
  if (data?.type === "tick") {
    const tickData: SocketTickData = data;
  }
  if (data?.type === "depth") {
    const depthData: SocketDepthData = data;
  }
});
```
