<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-quote.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-quote -->

---
title: 实时报价
description: 全球主流外汇货币对的实时报价数据，包含EUR、GBP、JPY、CHF等主要货币对的实时买入价、卖出价、中间价及点差。数据源来自多家流动性提供商，更新频率高。
keywords: 外汇实时报价API, 外汇实时Quote数据, 外汇报价接口, EUR/USD实时报价, 外汇基准报价, 货币对买卖价格, 金融数据分析
---

# 外汇 API - 实时报价

GET /forex/quote?region={region}&code={code}

## 请求参数

| 参数名称 | 描述     | 必填 |
| -------- | -------- | ---- |
| region   | 市场代码 | true |
| code     | 产品代码 | true |

## 响应参数

| 响应参数 | 参数类型 | 描述         |
| -------- | -------- | ------------ |
| s        | string   | 产品代码     |
| ld       | number   | 最新价       |
| l        | number   | 最低价       |
| o        | number   | 开盘价       |
| h        | number   | 最高价       |
| t        | number   | 时间戳       |
| v        | number   | 成交数量     |
| tu       | number   | 成交额       |
| ts       | number   | 标的交易状态 |
| ch       | number   | 涨跌额       |
| chp      | number   | 涨跌幅百分比 |

## 代码示例

```python
import requests

url = "https://api.itick.org/forex/quote?region=GB&code=EURUSD"

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
.url("https://api.itick.org/forex/quote?region=GB&code=EURUSD")
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

	url := "https://api.itick.org/forex/quote?region=GB&code=EURUSD"

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
  path: '/forex/quote?region=GB&code=EURUSD',
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
curl --location 'https://api.itick.org/forex/quote?region=GB&code=EURUSD' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "EURUSD",
    "ld": 1.17376,
    "o": 1.1741,
    "h": 1.17497,
    "l": 1.17194,
    "t": 1765576744049,
    "v": 1193417.3,
    "tu": 1400561.87122,
    "ch": 0.00001,
    "chp": 0.02,
    "ts": 0
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

quote = client.get_forex_quote("GB", "EURUSD")
print(quote)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quote = client.getForexQuote("GB", "EURUSD");
            System.out.println(quote);
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

	quote, err := client.GetForexQuote("GB", "EURUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Quote: %+v\n", quote)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getQuote({ region: "GB", code: "EURUSD" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getQuote({ region: "GB", code: "EURUSD" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇实时报价 API：

```python
# 使用 MCP 工具 forexQuote
# 对应 REST API: GET /forex/quote
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
