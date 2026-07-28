<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-info.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-info -->

---
title: 股票信息
description: 提供稳定可靠的股票信息API接口，覆盖A股、美股、港股等多个市场。免费及付费接口支持获取实时行情、历史数据、分时K线、财务数据、公司基本面等。快速集成，免费试用。
keywords: 股票API, 股票数据接口, 股票历史数据, 公司基本面, 分时K线, 股票基本信息, 证券交易所行情, 实时行情, 高频交易接口
---

# 股票 API - 股票信息

GET /stock/info?type={type}&region={region}&code={code}

## 请求参数

| 参数名称 | 描述           | 必填 |
| -------- | -------------- | ---- |
| type     | 产品类型 stock | true |
| region   | 市场代码 HK    | true |
| code     | 产品代码       | true |

## 响应参数

| 响应参数 | 参数类型 | 描述          |
| -------- | -------- | ------------- |
| c        | string   | 股票代码      |
| n        | string   | 股票名称      |
| t        | string   | 类型          |
| e        | string   | 交易所        |
| s        | string   | 所属板块      |
| i        | string   | 所属行业      |
| l        | string   | logo          |
| r        | string   | 区域/国家代码 |
| bd       | string   | 公司简介      |
| wu       | string   | 公司网站URL   |
| mcb      | number   | 总市值        |
| tso      | number   | 总股本        |
| pet      | number   | 市盈率        |
| fcc      | string   | 货币代码      |

## 代码示例

```python
import requests

url = "https://api.itick.org/stock/info?type=stock&region=HK&code=700"

headers = {
"accept": "application/json"
"token": "your_token"
}

response = requests.get(url, headers=headers)

print(response.text)

```

```java
OkHttpClient client = new OkHttpClient();

Request request = new Request.Builder()
.url("https://api.itick.org/stock/info?type=stock&region=HK&code=700")
.get()
.addHeader("accept", "application/json")
.addHeader("token", "your_token")
.build();

Response response = client.newCall(request).execute();
```

```go
package main

import (
	"fmt"
	"net/http"
	"io"
)

func main() {

	url := "https://api.itick.org/stock/info?type=stock&region=HK&code=700"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("token", "your_token")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	fmt.Println(string(body))

}
```

```javascript
const https = require('https');

const options = {
  method: 'GET',
  hostname: 'api.itick.org',
  path: '/stock/info?type=stock&region=HK&code=700',
  headers: {
    'accept': 'application/json',
    'token': '{token}'
  }
};

const req = https.request(options, function (res) {
  const chunks = [];

  res.on('data', function (chunk) {
    chunks.push(chunk);
  });

  res.on('end', function () {
    const body = Buffer.concat(chunks);
    console.log(body.toString());
  });
});

req.end();
```

```bash
curl --location 'https://api.itick.org/stock/info?type=stock&region=HK&code=700' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "c": "AAPL",
    "n": "Apple Inc.",
    "t": "stock",
    "e": "NASDAQ",
    "s": "Electronic Technology",
    "i": "Telecommunications Equipment",
    "l": "Apple Inc.",
    "r": "USD",
    "bd": "Apple, Inc. engages in the design, manufacture, and sale of smartphones, personal computers, tablets, wearables and accessories, and other varieties of related services. It operates through the following geographical segments: Americas, Europe, Greater China, Japan, and Rest of Asia Pacific. The Americas segment includes North and South America. The Europe segment consists of European countries, as well as India, the Middle East, and Africa. The Greater China segment comprises China, Hong Kong, and Taiwan. The Rest of Asia Pacific segment includes Australia and Asian countries. Its products and services include iPhone, Mac, iPad, AirPods, Apple TV, Apple Watch, Beats products, AppleCare, iCloud, digital content stores, streaming, and licensing services. The company was founded by Steven Paul Jobs, Ronald Gerald Wayne, and Stephen G. Wozniak in April 1976 and is headquartered in Cupertino, CA.",
    "wu": "http://www.apple.com",
    "mcb": 3436885784335,
    "tso": 14840389413,
    "pet": 35.3865799154784,
    "fcc": "USD"
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

info = client.get_stock_info("HK", "700")
print(info)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var info = client.getStockInfo("HK", "700");
            System.out.println(info);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Go SDK

```go
package main

import (
	"fmt"
	"log"

	"io.github.itick/sdk"
)

func main() {
	token := "your_api_token"
	client := sdk.NewClient(token)

	info, err := client.GetStockInfo("HK", "700")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Info: %+v\n", info)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getInfo({ region: "HK", code: "700" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getInfo({ region: "HK", code: "700" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票信息 API：

```python
# 使用 MCP 工具 stockInfo
# 对应 REST API: GET /stock/info
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
