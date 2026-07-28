<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-quotes -->

---
title: 批量实时报价
description: 批量获取多个加密货币交易对的实时报价数据。返回各交易对的最新价、24小时涨跌幅、成交量、买卖盘口等完整市场快照。专为投资组合监控、多币种行情看板及策略决策设计，极大提升数据获取效率，节省API调用次数。
keywords: 加密货币批量报价API, 批量数字货币行情, 多币种实时报价, 多个交易对实时行情, 批量获取加密货币价格, 多币种行情看板数据
---

# 加密货币 API - 批量实时报价

GET /crypto/quotes?region={symbol}&codes={codes}

## 请求参数

| 参数名称 | 描述                                               | 必填 |
| -------- | -------------------------------------------------- | ---- |
| region   | 市场代码 BA,BT                                     | true |
| codes    | 货币代码，多个用英文逗号隔开，如： BTCUSDT,ETHUSDT | true |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 产品代码         |
| ld       | number   | 最新价           |
| o        | number   | 开盘价           |
| h        | number   | 最高价           |
| l        | number   | 最低价           |
| t        | number   | 最新成交的时间戳 |
| v        | number   | 成交数量         |
| tu       | number   | 成交金额         |
| ts       | number   | 标的交易状态     |

## 代码示例

```python
import requests

url = "https://api.itick.org/crypto/quotes?region=BA&codes=BTCUSDT,ETHUSDT"

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
.url("https://api.itick.org/crypto/quotes?region=BA&codes=BTCUSDT,ETHUSDT")
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

	url := "https://api.itick.org/crypto/quotes?region=BA&codes=BTCUSDT,ETHUSDT"

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
  path: '/crypto/quotes?region=BA&codes=BTCUSDT,ETHUSDT',
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
curl --location 'https://api.itick.org/crypto/quotes?region=BA&codes=BTCUSDT,ETHUSDT' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "BTCUSDT": {
      "s": "BTCUSDT",
      "ld": 116216.87,
      "o": 115131.02,
      "h": 116828.93,
      "l": 114259,
      "t": 1754585538020,
      "v": 11497.05829,
      "tu": 1330432358.619943,
      "ts": 0
    },
    "ETHUSDT": {
      "s": "ETHUSDT",
      "ld": 3812.5,
      "o": 3649.44,
      "h": 3865.64,
      "l": 3638.56,
      "t": 1754585538028,
      "v": 553868.7467,
      "tu": 2085284600.019927,
      "ts": 0
    }
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

quotes = client.get_crypto_quotes("BA", "BTCUSDT,ETHUSDT")
print(quotes)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quotes = client.getCryptoQuotes("BA", "BTCUSDT,ETHUSDT");
            System.out.println(quotes);
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

	quotes, err := client.GetCryptoQuotes("BA", "BTCUSDT,ETHUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getQuotes({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getQuotes({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币批量实时报价 API：

```python
# 使用 MCP 工具 cryptoQuotes
# 对应 REST API: GET /crypto/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
