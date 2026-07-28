<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-quote.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-quote -->

---
title: 实时报价
description: 加密货币的实时聚合报价数据。单次请求返回最新成交价、24小时最高/最低价、涨跌幅、总成交量以及完整的买卖一档或多档盘口深度。数据源聚合自多家主流交易所。
keywords: 实时报价, 加密货币报价API, 实时Quote数据, BTC/USD 盘口数据, 加密货币买卖盘深度, 加密货币最新价格
---

# 加密货币 API - 实时报价

GET /crypto/quote?region={symbol}&code={code}

## 请求参数

| 参数名称 | 描述             | 必填 |
| -------- | ---------------- | ---- |
| region   | 市场代码 BA,BT   | true |
| code     | 产品代码 BTCUSDT | true |

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

## 代码示例

```python
import requests

url = "https://api.itick.org/crypto/quote?region=BA&code=BTCUSDT"

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
.url("https://api.itick.org/crypto/quote?region=BA&code=BTCUSDT")
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

	url := "https://api.itick.org/crypto/quote?region=BA&code=BTCUSDT"

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
  path: '/crypto/quote?region=BA&code=BTCUSDT',
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
curl --location 'https://api.itick.org/crypto/quote?region=BA&code=BTCUSDT' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "BTCUSDT",
    "ld": 116211.33,
    "o": 114953.48,
    "h": 116828.93,
    "l": 114259,
    "t": 1754585059019,
    "v": 11296.78228,
    "tu": 1307158285.566175,
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

quote = client.get_crypto_quote("BA", "BTCUSDT")
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

            var quote = client.getCryptoQuote("BA", "BTCUSDT");
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

	quote, err := client.GetCryptoQuote("BA", "BTCUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Quote: %+v\n", quote)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getQuote({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getQuote({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币实时报价 API：

```python
# 使用 MCP 工具 cryptoQuote
# 对应 REST API: GET /crypto/quote
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
